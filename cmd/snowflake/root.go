package snowflake

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/snowsql"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap-inc/tidb2dw/replicate"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cdcv2 "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/pingcap/tiflow/pkg/logutil"
	putil "github.com/pingcap/tiflow/pkg/util"
	"github.com/spf13/cobra"
	"github.com/thediveo/enumflag"
	"go.uber.org/zap"
)

func genSinkURI(storagePath string, flushInterval time.Duration, fileSize int64) (*url.URL, error) {
	sinkUri, err := url.Parse(storagePath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	values := sinkUri.Query()
	values.Add("flush-interval", flushInterval.String())
	values.Add("file-size", fmt.Sprint(fileSize))
	values.Add("protocol", "csv")
	if sinkUri.Scheme == "s3" {
		creds := credentials.NewEnvCredentials()
		credValue, err := creds.Get()
		if err != nil {
			return nil, errors.Annotate(err, "Failed to resolve AWS credential")
		}
		values.Add("access-key", credValue.AccessKeyID)
		values.Add("secret-access-key", credValue.SecretAccessKey)
		if credValue.SessionToken != "" {
			values.Add("session-token", credValue.SessionToken)
		}
	} else if sinkUri.Scheme == "gcs" {
		credValue, found := syscall.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
		if !found {
			return nil, errors.New("Failed to resolve GCS credential")
		} else {
			values.Add("credentials-file", credValue)
		}
	} else {
		return nil, errors.Errorf("get sink uri failed, unsupported uri schema: %s", sinkUri.Scheme)
	}
	sinkUri.RawQuery = values.Encode()
	return sinkUri, nil
}

func createChangefeed(cdcServer string, sinkURI *url.URL, tableFQN string, startTSO uint64) error {
	client := &http.Client{}
	cfCfg := &cdcv2.ChangefeedConfig{
		SinkURI: sinkURI.String(),
		ReplicaConfig: &cdcv2.ReplicaConfig{
			Filter: &cdcv2.FilterConfig{Rules: []string{tableFQN}},
			Sink: &cdcv2.SinkConfig{
				CSVConfig:          &cdcv2.CSVConfig{IncludeCommitTs: true, Delimiter: ","},
				CloudStorageConfig: &cdcv2.CloudStorageConfig{OutputColumnID: putil.AddressOf(true)},
			},
			EnableOldValue: false,
		},
		StartTs: 0,
	}
	if startTSO != 0 {
		cfCfg.StartTs = startTSO
	}
	bytesData, _ := json.Marshal(cfCfg)
	url, err := url.JoinPath(cdcServer, "api/v2/changefeeds")
	if err != nil {
		return errors.Annotate(err, "join url failed")
	}
	httpReq, _ := http.NewRequest("POST", url, bytes.NewReader(bytesData))
	resp, err := client.Do(httpReq)
	if err != nil {
		return errors.Trace(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("create changefeed failed, status code: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Trace(err)
	}
	respData := make(map[string]interface{})
	if err = json.Unmarshal(body, &respData); err != nil {
		return errors.Trace(err)
	}
	changefeedID := respData["id"].(string)
	replicateConfig := respData["config"].(map[string]interface{})
	log.Info("create changefeed success", zap.String("changefeed-id", changefeedID), zap.Any("replica-config", replicateConfig))

	return nil
}

type RunMode enumflag.Flag

const (
	RunModeFull RunMode = iota
	RunModeSnapshotOnly
	RunModeIncrementalOnly
)

var RunModeIds = map[RunMode][]string{
	RunModeFull:            {"full"},
	RunModeSnapshotOnly:    {"snapshot-only"},
	RunModeIncrementalOnly: {"incremental-only"},
}

func NewSnowflakeCmd() *cobra.Command {
	var (
		tidbConfigFromCli      tidbsql.TiDBConfig
		snowflakeConfigFromCli snowsql.SnowflakeConfig
		tableFQN               string
		snapshotConcurrency    int
		storagePath            string
		cdcHost                string
		cdcPort                int
		cdcFlushInterval       time.Duration
		cdcFileSize            int64
		timezone               string
		logFile                string
		logLevel               string
		credValue              credentials.Value
		sindURIStr             string

		mode RunMode
	)

	run := func() error {
		// 0. check status
		ctx := context.Background()
		storage, err := putil.GetExternalStorageFromURI(ctx, storagePath)
		if err != nil {
			return errors.Trace(err)
		}
		metadataExist, err := storage.FileExists(ctx, "increment/metadata")
		if err != nil {
			return errors.Trace(err)
		}
		loadinfoExist, err := storage.FileExists(ctx, "snapshot/loadinfo")
		if err != nil {
			return errors.Trace(err)
		}

		// 1. get current tso
		startTSO := uint64(0)
		if !loadinfoExist {
			startTSO, err = tidbsql.GetCurrentTSO(&tidbConfigFromCli)
			if err != nil {
				return errors.Annotate(err, "Failed to get current TSO")
			}
		} else {
			log.Info("Snapshot data is all loaded, skip get current TSO")
		}

		var sinkURI *url.URL

		// 2. create changefeed
		if mode == RunModeFull || (mode == RunModeIncrementalOnly && sindURIStr == "") {
			increStoragePath, err := url.JoinPath(storagePath, "increment")
			if err != nil {
				return errors.Trace(err)
			}
			sinkURI, err = genSinkURI(increStoragePath, cdcFlushInterval, cdcFileSize)
			if err != nil {
				return errors.Trace(err)
			}
			if !loadinfoExist || !metadataExist {
				if err = createChangefeed(fmt.Sprintf("http://%s:%d", cdcHost, cdcPort), sinkURI, tableFQN, startTSO); err != nil {
					return errors.Annotate(err, "Failed to create changefeed")
				}
			} else {
				log.Info("Snapshot has been loaded, Changefeed has been created, skip create changefeed")
			}
		} else if mode == RunModeIncrementalOnly && sindURIStr != "" {
			uri, err := url.Parse(sindURIStr)
			if err != nil {
				return errors.Trace(err)
			}
			sinkURI = uri
		}

		var sourceDatabase, sourceTable string
		if tableFQN != "" {
			parts := strings.SplitN(tableFQN, ".", 2)
			if len(parts) != 2 {
				return errors.Errorf("table must be a full-qualified name like mydb.mytable")
			}
			sourceDatabase, sourceTable = parts[0], parts[1]
		}

		// 3. run replicate snapshot
		if (mode == RunModeFull || mode == RunModeSnapshotOnly) && !loadinfoExist {
			snapStoragePath, err := url.JoinPath(storagePath, "snapshot")
			if err != nil {
				return errors.Trace(err)
			}
			db, err := snowflakeConfigFromCli.OpenDB()
			if err != nil {
				return errors.Trace(err)
			}
			snapshotURI, err := url.Parse(snapStoragePath)
			if err != nil {
				return errors.Annotate(err, "Failed to parse workspace path")
			}
			connector, err := snowsql.NewSnowflakeConnector(
				db,
				fmt.Sprintf("snapshot_stage_%s", sourceTable),
				snapshotURI,
				&credValue,
			)
			if err != nil {
				return errors.Trace(err)
			}
			if err = replicate.StartReplicateSnapshot(connector, &tidbConfigFromCli, sourceDatabase, sourceTable, snapshotConcurrency, snapshotURI, fmt.Sprint(startTSO), &credValue); err != nil {
				return errors.Annotate(err, "Failed to replicate snapshot")
			}
		} else if !loadinfoExist {
			log.Info("Snapshot has been loaded, skip replicate snapshot")
		}

		// 4. run replicate increment
		if mode == RunModeFull || mode == RunModeIncrementalOnly {
			db, err := snowflakeConfigFromCli.OpenDB()
			if err != nil {
				return errors.Trace(err)
			}
			connector, err := snowsql.NewSnowflakeConnector(
				db,
				fmt.Sprintf("increment_stage_%s", sourceTable),
				sinkURI,
				&credValue,
			)
			if err != nil {
				return errors.Trace(err)
			}
			if err = replicate.StartReplicateIncrement(connector, sinkURI, cdcFlushInterval/5, "", timezone, &credValue); err != nil {
				return errors.Annotate(err, "Failed to replicate incremental")
			}
		}

		return nil
	}

	cmd := &cobra.Command{
		Use:   "snowflake",
		Short: "Replicate snapshot and incremental data from TiDB to Snowflake",
		Run: func(_ *cobra.Command, _ []string) {
			// init logger
			err := logutil.InitLogger(&logutil.Config{
				Level: logLevel,
				File:  logFile,
			})
			if err != nil {
				panic(err)
			}

			uri, err := url.Parse(storagePath)
			if err != nil {
				panic(err)
			}
			if uri.Scheme == "s3" {
				// resolve aws credential
				creds := credentials.NewEnvCredentials()
				credValue, err = creds.Get()
				if err != nil {
					panic(err)
				}
			}

			if err = run(); err != nil {
				log.Error("Error running snowflake replication", zap.Error(err))
			}
		},
	}

	cmd.PersistentFlags().BoolP("help", "", false, "help for this command")
	cmd.Flags().Var(enumflag.New(&mode, "mode", RunModeIds, enumflag.EnumCaseInsensitive), "full", "replication mode: full, snapshot-only, incremental-only")
	cmd.Flags().StringVarP(&tidbConfigFromCli.Host, "tidb.host", "h", "127.0.0.1", "TiDB host")
	cmd.Flags().IntVarP(&tidbConfigFromCli.Port, "tidb.port", "P", 4000, "TiDB port")
	cmd.Flags().StringVarP(&tidbConfigFromCli.User, "tidb.user", "u", "root", "TiDB user")
	cmd.Flags().StringVarP(&tidbConfigFromCli.Pass, "tidb.pass", "p", "", "TiDB password")
	cmd.Flags().StringVar(&tidbConfigFromCli.SSLCA, "tidb.ssl-ca", "", "TiDB SSL CA")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.AccountId, "snowflake.account-id", "", "snowflake accound id: <organization>-<account>")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.Warehouse, "snowflake.warehouse", "COMPUTE_WH", "")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.User, "snowflake.user", "", "snowflake user")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.Pass, "snowflake.pass", "", "snowflake password")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.Database, "snowflake.database", "", "snowflake database")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.Schema, "snowflake.schema", "", "snowflake schema")
	cmd.Flags().StringVarP(&tableFQN, "table", "t", "", "table full qualified name: <database>.<table>")
	cmd.Flags().IntVar(&snapshotConcurrency, "snapshot-concurrency", 8, "the number of concurrent snapshot workers")
	cmd.Flags().StringVarP(&storagePath, "storage", "s", "", "storage path: s3://<bucket>/<path> or gcs://<bucket>/<path>")
	cmd.Flags().StringVar(&cdcHost, "cdc.host", "127.0.0.1", "TiCDC server host")
	cmd.Flags().IntVar(&cdcPort, "cdc.port", 8300, "TiCDC server port")
	cmd.Flags().DurationVar(&cdcFlushInterval, "cdc.flush-interval", 60*time.Second, "")
	cmd.Flags().Int64Var(&cdcFileSize, "cdc.file-size", 64*1024*1024, "")
	cmd.Flags().StringVar(&timezone, "tz", "System", "specify time zone of storage consumer")
	cmd.Flags().StringVar(&logFile, "log.file", "", "log file path")
	cmd.Flags().StringVar(&logLevel, "log.level", "info", "log level")
	cmd.Flags().StringVar(&sindURIStr, "sink-uri", "", "sink uri, only needed under incremental-only mode")

	return cmd
}
