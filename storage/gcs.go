package storage

import (
	"context"
	"database/sql"

	"cloud.google.com/go/iam"
	"cloud.google.com/go/storage"
	"github.com/pingcap-inc/tidb2dw/snowsql"
	"github.com/snowflakedb/gosnowflake"
)

// TODO(lcui2): still can not add permission, but no error.

func GCSTest() {
	storagePath := "gcs://tidb-bucket"
	sfConfig := &gosnowflake.Config{
		Account:  "ksplloe-nu29250",
		User:     "larry",
		Password: "Qazwsx147",
		Database: "snowpipe_db",
		Schema:   "s3",
	}
	dns, _ := gosnowflake.DSN(sfConfig)
	db, _ := sql.Open("snowflake", dns)
	name := "inter_warehouse"
	_ = snowsql.CreateStorageIntegration(db, name, storagePath, "GCS")
	server, _ := snowsql.GetGCSServiceAccount(db, name)
	addBucketIAMMember("tidb-bucket", server, "tidb-snowflake")
}

// addBucketIAMMember adds the bucket IAM member to permission role.
func addBucketIAMMember(bucketName, identity, roleName string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)
	policy, err := bucket.IAM().Policy(ctx)
	if err != nil {
		return err
	}
	// like "projects/tribal-pillar-384718/roles/CustomRole"
	var role iam.RoleName = iam.RoleName(roleName)
	// like "serviceAccount:kjq200000@awsuseast2-a5c7.iam.gserviceaccount.com"
	identity = "serviceAccount:" + identity
	policy.Add(identity, role)
	if err := bucket.IAM().SetPolicy(ctx, policy); err != nil {
		return err
	}
	return nil
}
