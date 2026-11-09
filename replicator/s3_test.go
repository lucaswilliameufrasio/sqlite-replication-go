package replicator_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/spool"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/replicator"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storages3"
)

func TestFlociPublicS3Restore(t *testing.T) {
	endpoint := os.Getenv("SQLITE_REPLICATION_FLOCI_ENDPOINT")
	runID := os.Getenv("SQLITE_REPLICATION_FLOCI_RUN_ID")
	if endpoint == "" || runID == "" {
		t.Skip("requires the isolated Floci service from make test-floci")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	awsConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load local S3 SDK config: %v", err)
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
	store, err := storages3.New(client, "sqlite-replication-m4-"+runID)
	if err != nil {
		t.Fatalf("initialize S3 adapter: %v", err)
	}
	destination := filepath.Join(t.TempDir(), "restored.sqlite")
	if err := replicator.RestoreFromS3(ctx, store, "rust-recovery-test", destination, replicator.RestoreTarget{TXID: 8}); err != nil {
		t.Fatalf("restore public API from S3: %v", err)
	}
	database, err := sql.Open("sqlite", destination)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var value string
	if err := database.QueryRow("SELECT value FROM records WHERE id = 1").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "updated" {
		t.Fatalf("restored value = %q, want updated", value)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	crossEngineDestination := filepath.Join(t.TempDir(), "rust-synced.sqlite")
	if err := replicator.RestoreFromS3(ctx, store, "public-rust-sync-test", crossEngineDestination, replicator.RestoreTarget{TXID: 2}); err != nil {
		t.Fatalf("restore Rust-synced repository through Go API: %v", err)
	}
	crossEngineDatabase, err := sql.Open("sqlite", crossEngineDestination)
	if err != nil {
		t.Fatal(err)
	}
	defer crossEngineDatabase.Close()
	if err := crossEngineDatabase.QueryRow("SELECT value FROM records WHERE id = 1").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "uploaded" {
		t.Fatalf("Rust-synced restored value = %q, want uploaded", value)
	}
}

func TestFlociPublicS3Sync(t *testing.T) {
	endpoint := os.Getenv("SQLITE_REPLICATION_FLOCI_ENDPOINT")
	runID := os.Getenv("SQLITE_REPLICATION_FLOCI_RUN_ID")
	if endpoint == "" || runID == "" {
		t.Skip("requires the isolated Floci service from make test-floci")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	awsConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load local S3 SDK config: %v", err)
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
	bucketName := "sqlite-replication-sync-" + runID
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)}); err != nil {
		t.Fatalf("create isolated local-test bucket: %v", err)
	}
	store, err := storages3.New(client, bucketName)
	if err != nil {
		t.Fatalf("initialize S3 adapter: %v", err)
	}
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("create initial snapshot: %v", err)
	}
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, "UPDATE records SET value = ? WHERE id = 1", "uploaded"); err != nil {
		t.Fatalf("commit source change: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := replicator.CaptureOnce(ctx, databasePath, repositoryRoot, func(context.Context) (func() error, error) {
		return func() error { return nil }, nil
	}); err != nil {
		t.Fatalf("capture local WAL: %v", err)
	}

	result, err := replicator.SyncToS3(ctx, repositoryRoot, store, "public-sync-test", "floci-public-sync-"+runID)
	if err != nil {
		t.Fatalf("sync local LTX chain to Floci: %v", err)
	}
	if result.TXID != 2 || result.ObjectsConfirmed != 3 || result.BytesConfirmed <= 0 {
		t.Fatalf("sync result = %+v, want TXID 2 and 3 confirmed objects", result)
	}
	ack, err := spool.ReadRemoteAck(repositoryRoot)
	if err != nil {
		t.Fatalf("read durable remote ack: %v", err)
	}
	if ack.RemoteID != "floci-public-sync-"+runID || ack.Prefix != "public-sync-test" || ack.TXID != result.TXID {
		t.Fatalf("remote ack = %+v, does not match successful sync", ack)
	}
	coordinator, err := replicator.NewCoordinator(replicator.CoordinatorConfig{
		RepositoryRoot: repositoryRoot,
		DatabasePath:   databasePath,
		ObserveTXID: func(context.Context) (uint64, bool, error) {
			return result.TXID, true, nil
		},
		Capture: func(context.Context) (replicator.CaptureResult, error) {
			return replicator.CaptureResult{TXID: result.TXID}, nil
		},
		SetAdmission: func(context.Context, bool, replicator.Limits) error { return nil },
		DrainWrites:  func(context.Context) error { return nil },
		Limits: replicator.Limits{
			MaxSpoolBytes:      1 << 30,
			MaxWALBytes:        64 << 20,
			MaxUnprotectedAge:  time.Hour,
			MaxUploadsInFlight: 1,
		},
		Remote: &replicator.RemoteConfig{
			Store:    store,
			Prefix:   "public-sync-test",
			RemoteID: "floci-public-sync-" + runID,
		},
	})
	if err != nil {
		t.Fatalf("construct remote coordinator: %v", err)
	}
	if err := coordinator.Flush(ctx); err != nil {
		t.Fatalf("flush remote target through coordinator: %v", err)
	}
	health := coordinator.Health(ctx)
	if health.Local != replicator.ComponentHealthy || health.Remote != replicator.ComponentHealthy {
		t.Fatalf("coordinator component health = %+v", health)
	}

	retry, err := replicator.SyncToS3(ctx, repositoryRoot, store, "public-sync-test", "floci-public-sync-"+runID)
	if err != nil {
		t.Fatalf("idempotent remote sync retry: %v", err)
	}
	if retry.TXID != result.TXID {
		t.Fatalf("retry TXID = %d, want %d", retry.TXID, result.TXID)
	}

	conflictRoot := filepath.Join(directory, "conflict-repository")
	if _, err := replication.Snapshot(ctx, databasePath, conflictRoot); err != nil {
		t.Fatalf("create repository for conflict test: %v", err)
	}
	conflictingKey := "public-sync-conflict/0000/0000000000000001-0000000000000001.ltx"
	if _, err := store.PutImmutable(ctx, conflictingKey, strings.NewReader("not the local snapshot")); err != nil {
		t.Fatalf("prepare isolated immutable-key conflict: %v", err)
	}
	if _, err := replicator.SyncToS3(ctx, conflictRoot, store, "public-sync-conflict", "floci-public-conflict-"+runID); err == nil {
		t.Fatal("sync accepted a divergent immutable object")
	}
	if _, err := spool.ReadRemoteAck(conflictRoot); err == nil {
		t.Fatal("failed sync advanced the remote acknowledgement")
	}
}
