package snowflake

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/pingcap-inc/tidb2dw/snowsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/snowflakedb/gosnowflake"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type ReplicateSession struct {
	ID string

	SFConfig            *SnowflakeConfig
	TiDBConfig          *TiDBConfig
	TableFQN            string
	SnapshotConcurrency int
	ResolvedS3Region    string
	ResolvedTSO         string // Available after buildDumper()

	AWSSession    *session.Session
	AWSCredential credentials.Value // The resolved credential from current env
	SnowflakePool *sql.DB
	TiDBPool      *sql.DB

	SourceDatabase string
	SourceTable    string

	StorageWorkspacePath string
}

func NewReplicateSession(
	sfConfigFromCli *SnowflakeConfig,
	tidbConfigFromCli *TiDBConfig,
	tableFQN string,
	snapshotConcurrency int,
	s3StoragePath string) (*ReplicateSession, error) {
	sess := &ReplicateSession{
		ID:                  uuid.New().String(),
		SFConfig:            sfConfigFromCli,
		TiDBConfig:          tidbConfigFromCli,
		TableFQN:            tableFQN,
		SnapshotConcurrency: snapshotConcurrency,
	}
	sess.StorageWorkspacePath = fmt.Sprintf("%s/%s", s3StoragePath, sess.ID)
	{
		parts := strings.SplitN(tableFQN, ".", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("table must be a full-qualified name like mydb.mytable")
		}
		sess.SourceDatabase = parts[0]
		sess.SourceTable = parts[1]
	}
	log.Info("Creating replicate session",
		zap.String("id", sess.ID),
		zap.String("storage", sess.StorageWorkspacePath),
		zap.String("source", tableFQN))
	{
		awsSession, err := session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		})
		if err != nil {
			return nil, errors.Annotate(err, "Failed to establish AWS session")
		}
		sess.AWSSession = awsSession

		creds := credentials.NewEnvCredentials()
		credValue, err := creds.Get()
		if err != nil {
			return nil, errors.Annotate(err, "Failed to resolve AWS credential")
		}
		sess.AWSCredential = credValue
	}
	{
		// Parse s3StoragePath like s3://wenxuan-snowflake-test/dump20230601
		parsed, err := url.Parse(s3StoragePath)
		if err != nil {
			return nil, errors.Annotate(err, "Failed to parse --storage value")
		}
		if parsed.Scheme != "s3" {
			return nil, errors.Errorf("storage must be like s3://...")
		}

		bucket := parsed.Host
		log.Debug("Resolving storage region")
		s3Region, err := s3manager.GetBucketRegion(context.Background(), sess.AWSSession, bucket, "us-west-2")
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
				return nil, fmt.Errorf("unable to find bucket %s's region not found", bucket)
			}
			return nil, errors.Annotate(err, "Failed to resolve --storage region")
		}
		sess.ResolvedS3Region = s3Region
		log.Info("Resolved storage region", zap.String("region", s3Region))
	}
	{
		sfConfig := gosnowflake.Config{}
		sfConfig.Account = sfConfigFromCli.SnowflakeAccountId
		sfConfig.User = sfConfigFromCli.SnowflakeUser
		sfConfig.Password = sfConfigFromCli.SnowflakePass
		sfConfig.Database = sfConfigFromCli.SnowflakeDatabase
		sfConfig.Schema = sfConfigFromCli.SnowflakeSchema
		sfConfig.Warehouse = sfConfigFromCli.SnowflakeWarehouse
		dsn, err := gosnowflake.DSN(&sfConfig)
		if err != nil {
			return nil, errors.Annotate(err, "Failed to generate Snowflake DSN")
		}
		db, err := sql.Open("snowflake", dsn)
		if err != nil {
			return nil, errors.Annotate(err, "Failed to open Snowflake connection")
		}
		sess.SnowflakePool = db
	}
	{
		tidbConfig := mysql.NewConfig()
		tidbConfig.User = tidbConfigFromCli.TiDBUser
		tidbConfig.Passwd = tidbConfigFromCli.TiDBPass
		tidbConfig.Net = "tcp"
		tidbConfig.Addr = fmt.Sprintf("%s:%d", tidbConfigFromCli.TiDBHost, tidbConfigFromCli.TiDBPort)
		if tidbConfigFromCli.TiDBSSLCA != "" {
			rootCertPool := x509.NewCertPool()
			pem, err := os.ReadFile(tidbConfigFromCli.TiDBSSLCA)
			if err != nil {
				log.Fatal(err.Error())
			}
			if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
				log.Fatal("Failed to append PEM.")
			}
			mysql.RegisterTLSConfig("tidb", &tls.Config{
				RootCAs:    rootCertPool,
				MinVersion: tls.VersionTLS12,
				ServerName: tidbConfigFromCli.TiDBHost,
			})
			tidbConfig.TLSConfig = "tidb"
		}
		db, err := sql.Open("mysql", tidbConfig.FormatDSN())
		if err != nil {
			return nil, errors.Annotate(err, "Failed to open TiDB connection")
		}
		sess.TiDBPool = db
	}

	return sess, nil
}

func (sess *ReplicateSession) Close() {
	sess.SnowflakePool.Close()
	sess.TiDBPool.Close()
}

func (sess *ReplicateSession) Run() error {
	var err error

	log.Info("Testing connections with Snowflake")
	err = sess.SnowflakePool.Ping()
	if err != nil {
		return errors.Annotate(err, "Failed to connect to Snowflake")
	}
	log.Info("Connected with Snowflake")

	log.Info("Testing connections with TiDB")
	err = sess.TiDBPool.Ping()
	if err != nil {
		return errors.Annotate(err, "Failed to connect to TiDB")
	}
	log.Info("Connected with TiDB")

	dumper, err := sess.buildDumper()
	if err != nil {
		return errors.Trace(err)
	}

	err = sess.dumpPrepareTargetTable()
	if err != nil {
		return errors.Trace(err)
	}

	err = dumper.Dump()
	_ = dumper.Close()
	if err != nil {
		return errors.Annotate(err, "Failed to dump table from TiDB")
	}

	log.Info("Successfully dumped table from TiDB, starting to load into Snowflake")

	err = sess.loadSnapshotDataIntoSnowflake()
	if err != nil {
		return errors.Annotate(err, "Failed to load snapshot data into Snowflake")
	}

	return nil
}

func (sess *ReplicateSession) buildDumper() (*export.Dumper, error) {
	conf, err := sess.buildDumperConfig()
	if err != nil {
		return nil, errors.Annotate(err, "Failed to build dumpling config")
	}
	dumper, err := export.NewDumper(context.Background(), conf)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to create dumpling instance")
	}

	sess.ResolvedTSO = conf.Snapshot
	if len(sess.ResolvedTSO) == 0 {
		return nil, errors.Errorf("Snapshot is not available")
	}
	// FIXME: This might cause a bug, because the underlying is a pool?
	_, err = sess.TiDBPool.ExecContext(context.Background(), "SET SESSION tidb_snapshot = ?", conf.Snapshot)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("Using snapshot", zap.String("snapshot", sess.ResolvedTSO))

	return dumper, nil
}

func (sess *ReplicateSession) buildDumperConfig() (*export.Config, error) {
	conf := export.DefaultConfig()
	conf.Logger = log.L()
	conf.User = sess.TiDBConfig.TiDBUser
	conf.Password = sess.TiDBConfig.TiDBPass
	conf.Host = sess.TiDBConfig.TiDBHost
	conf.Port = sess.TiDBConfig.TiDBPort
	conf.Threads = sess.SnapshotConcurrency
	conf.NoHeader = true
	conf.FileType = "csv"
	conf.CsvSeparator = ","
	conf.CsvDelimiter = "\""
	conf.EscapeBackslash = true
	conf.TransactionalConsistency = true
	conf.OutputDirPath = fmt.Sprintf("%s/snapshot", sess.StorageWorkspacePath)
	conf.S3.Region = sess.ResolvedS3Region

	conf.SpecifiedTables = true
	tables, err := export.GetConfTables([]string{sess.TableFQN})
	if err != nil {
		return nil, errors.Trace(err) // Should not happen
	}
	conf.Tables = tables

	return conf, nil
}

func (sess *ReplicateSession) dumpPrepareTargetTable() error {
	sql, err := snowsql.GenCreateSchema(sess.SourceDatabase, sess.SourceTable, sess.TiDBPool)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("Creating table in Snowflake", zap.String("sql", sql))
	_, err = sess.SnowflakePool.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (sess *ReplicateSession) loadSnapshotDataIntoSnowflake() error {
	stageName := fmt.Sprintf("snapshot_stage_%s", sess.SourceTable)
	log.Info("Creating stage for loading snapshot data", zap.String("stageName", stageName))
	err := snowsql.CreateExternalStage(
		sess.SnowflakePool,
		stageName,
		sess.StorageWorkspacePath,
		sess.AWSCredential)
	if err != nil {
		return errors.Annotate(err, "Failed to create stage")
	}

	// List all available files
	parsedWorkspace, err := url.Parse(sess.StorageWorkspacePath)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("List objects",
		zap.String("bucket", parsedWorkspace.Host),
		zap.String("prefix", fmt.Sprintf("%s/snapshot/", parsedWorkspace.Path)))

	workspacePrefix := strings.TrimPrefix(parsedWorkspace.Path, "/")
	snapshotPrefix := fmt.Sprintf("%s/snapshot/", workspacePrefix)
	dumpFilePrefix := fmt.Sprintf("%s%s.%s.", snapshotPrefix, sess.SourceDatabase, sess.SourceTable)

	s3Client := s3.New(sess.AWSSession, aws.NewConfig().WithRegion(sess.ResolvedS3Region))
	result, err := s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(parsedWorkspace.Host),
		Prefix: aws.String(workspacePrefix),
	})
	if err != nil {
		return errors.Trace(err)
	}
	if len(result.Contents) == 0 {
		return errors.Errorf("No snapshot files found")
	}

	dumpedSnapshots := make([]string, 0, 1)
	for _, item := range result.Contents {
		if strings.HasPrefix(*item.Key, dumpFilePrefix) && strings.HasSuffix(*item.Key, ".csv") {
			filePathToWorkspace := strings.TrimPrefix(*item.Key, workspacePrefix)
			dumpedSnapshots = append(dumpedSnapshots, filePathToWorkspace)
			log.Info("Found snapshot file", zap.String("key", filePathToWorkspace))
		}
	}

	for _, dumpedSnapshot := range dumpedSnapshots {
		log.Info("Loading snapshot data", zap.String("snapshot", dumpedSnapshot))
		sql := snowsql.GenLoadSnapshotFromStage(sess.SourceTable, stageName, dumpedSnapshot)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("Executing SQL", zap.String("sql", sql))
		_, err = sess.SnowflakePool.Exec(sql)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("Snapshot data load finished", zap.String("snapshot", dumpedSnapshot))
	}

	err = snowsql.DropStage(sess.SnowflakePool, stageName)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func newSnapshotCmd() *cobra.Command {
	var (
		tidbConfigFromCli   TiDBConfig
		tableFQN            string
		snapshotConcurrency int
		s3StoragePath       string
	)

	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "Replicate snapshot from TiDB to Snowflake",
		Run: func(_ *cobra.Command, _ []string) {
			session, err := NewReplicateSession(&snowflakeConfigFromCli, &tidbConfigFromCli, tableFQN, snapshotConcurrency, s3StoragePath)
			if err != nil {
				panic(err)
			}
			defer session.Close()

			err = session.Run()
			if err != nil {
				panic(err)
			}
		},
	}

	cmd.PersistentFlags().BoolP("help", "", false, "help for this command")
	cmd.Flags().StringVarP(&tidbConfigFromCli.TiDBHost, "host", "h", "127.0.0.1", "TiDB host")
	cmd.Flags().IntVarP(&tidbConfigFromCli.TiDBPort, "port", "P", 4000, "TiDB port")
	cmd.Flags().StringVarP(&tidbConfigFromCli.TiDBUser, "user", "u", "root", "TiDB user")
	cmd.Flags().StringVarP(&tidbConfigFromCli.TiDBPass, "pass", "p", "", "TiDB password")
	cmd.Flags().StringVar(&tidbConfigFromCli.TiDBSSLCA, "ssl-ca", "", "TiDB SSL CA")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakeAccountId, "snowflake.account-id", "", "snowflake accound id: <organization>-<account>")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakeWarehouse, "snowflake.warehouse", "COMPUTE_WH", "")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakeUser, "snowflake.user", "", "snowflake user")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakePass, "snowflake.pass", "", "snowflake password")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakeDatabase, "snowflake.database", "", "snowflake database")
	cmd.Flags().StringVar(&snowflakeConfigFromCli.SnowflakeSchema, "snowflake.schema", "", "snowflake schema")
	cmd.Flags().StringVarP(&tableFQN, "table", "t", "", "table fully qualified name: <database>.<table>")
	cmd.Flags().IntVar(&snapshotConcurrency, "snapshot-concurrency", 8, "the number of concurrent snapshot workers")
	cmd.Flags().StringVarP(&s3StoragePath, "storage", "s", "", "S3 storage path: s3://<bucket>/<path>")
	cmd.MarkFlagRequired("storage")
	cmd.MarkFlagRequired("table")

	return cmd
}
