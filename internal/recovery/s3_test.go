package recovery

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/ltx"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storages3"
	_ "modernc.org/sqlite"
)

func TestS3RecoveryKeyMappingAndPrefixValidation(t *testing.T) {
	localKey, err := s3KeyToLocalKey("tenant/db", "tenant/db/000a/0000000000000001-0000000000000007.ltx")
	if err != nil {
		t.Fatalf("s3KeyToLocalKey: %v", err)
	}
	if localKey != "ltx/10/0000000000000001-0000000000000007.ltx" {
		t.Fatalf("local key = %q", localKey)
	}
	remoteKey, err := localKeyToS3Key("tenant/db", localKey)
	if err != nil {
		t.Fatalf("localKeyToS3Key: %v", err)
	}
	if remoteKey != "tenant/db/000a/0000000000000001-0000000000000007.ltx" {
		t.Fatalf("remote key = %q", remoteKey)
	}
	for _, prefix := range []string{"/absolute", "trailing/", "a//b", "a/../b", "a/./b", "nul\x00byte"} {
		if err := validateS3Prefix(prefix); err == nil {
			t.Errorf("validateS3Prefix(%q) succeeded", prefix)
		}
	}
	for _, key := range []string{
		"tenant/db/../000a/0000000000000001-0000000000000007.ltx",
		"tenant/db/000a/../../outside.ltx",
		"tenant/db/000a/not-an-ltx-name.ltx",
	} {
		if _, err := s3KeyToLocalKey("tenant/db", key); err == nil {
			t.Errorf("s3KeyToLocalKey(%q) succeeded", key)
		}
	}
}

func TestFlociS3RecoveryRestoreCompactionAndGC(t *testing.T) {
	endpoint := os.Getenv("SQLITE_REPLICATION_FLOCI_ENDPOINT")
	runID := os.Getenv("SQLITE_REPLICATION_FLOCI_RUN_ID")
	if endpoint == "" || runID == "" {
		t.Skip("requires the isolated Floci service from make test-floci")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
	bucket := "sqlite-replication-m4-" + runID
	sharedFloci := os.Getenv("SQLITE_REPLICATION_FLOCI_CROSS_ENGINE") == "1"
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create isolated recovery bucket: %v", err)
	}
	store, err := storages3.New(client, bucket)
	if err != nil {
		t.Fatalf("initialize S3 adapter: %v", err)
	}
	prefix := "recovery-test"
	remoteKeys := []string{
		joinS3Key(prefix, "_meta/repository.json"),
		joinS3Key(prefix, "0000/0000000000000001-0000000000000007.ltx"),
		joinS3Key(prefix, "0000/0000000000000008-0000000000000008.ltx"),
	}
	var compactedKeys []string
	defer func() {
		if sharedFloci {
			return
		}
		cleanupKeys := append(append([]string(nil), remoteKeys...), compactedKeys...)
		_ = store.DeleteBatch(context.Background(), cleanupKeys)
		_, _ = client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	}()

	localRepository := t.TempDir()
	if err := storagefs.EnsureRepository(localRepository); err != nil {
		t.Fatalf("initialize fixture repository: %v", err)
	}
	databasePath := filepath.Join(t.TempDir(), "source.sqlite")
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec("CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT NOT NULL); INSERT INTO records VALUES (1, 'source')"); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	info, err := source.Stat()
	if err != nil {
		_ = source.Close()
		t.Fatal(err)
	}
	baseState, err := ltx.LoadDatabaseState(source, info.Size(), 7)
	if err != nil {
		_ = source.Close()
		t.Fatalf("load source state for S3 delta: %v", err)
	}
	localSnapshot := storagefs.LTXPath(localRepository, 1, 7)
	if err := os.MkdirAll(filepath.Dir(localSnapshot), 0o750); err != nil {
		_ = source.Close()
		t.Fatal(err)
	}
	staged, err := os.CreateTemp(filepath.Dir(localSnapshot), ".snapshot-*.ltx")
	if err != nil {
		_ = source.Close()
		t.Fatal(err)
	}
	if _, err := ltx.EncodeSnapshotAt(source, info.Size(), staged, 7, 1000); err != nil {
		_ = source.Close()
		_ = staged.Close()
		t.Fatalf("encode source snapshot: %v", err)
	}
	if err := staged.Sync(); err != nil {
		_ = source.Close()
		_ = staged.Close()
		t.Fatal(err)
	}
	stagedPath := staged.Name()
	if err := staged.Close(); err != nil {
		_ = source.Close()
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := storagefs.Publish(stagedPath, localSnapshot); err != nil {
		t.Fatalf("publish fixture snapshot: %v", err)
	}
	manifest, err := os.Open(filepath.Join(localRepository, "_meta", "repository.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutImmutable(ctx, remoteKeys[0], manifest); err != nil {
		_ = manifest.Close()
		t.Fatalf("upload S3 repository metadata: %v", err)
	}
	if err := manifest.Close(); err != nil {
		t.Fatal(err)
	}
	base, err := os.Open(localSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutImmutable(ctx, remoteKeys[1], base); err != nil {
		_ = base.Close()
		t.Fatalf("upload initial S3 snapshot: %v", err)
	}
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec("UPDATE records SET value = 'updated' WHERE id = 1"); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	target, err := os.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	targetInfo, err := target.Stat()
	if err != nil {
		_ = target.Close()
		t.Fatal(err)
	}
	deltaHeader, pages, targetState, err := baseState.DeltaToDatabaseImage(target, targetInfo.Size(), 2000)
	if err != nil {
		_ = target.Close()
		t.Fatalf("create S3 delta fixture: %v", err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	deltaPath := storagefs.LTXPath(localRepository, deltaHeader.MinTXID, deltaHeader.MaxTXID)
	if err := os.MkdirAll(filepath.Dir(deltaPath), 0o750); err != nil {
		t.Fatal(err)
	}
	deltaStage, err := os.CreateTemp(filepath.Dir(deltaPath), ".delta-*.ltx")
	if err != nil {
		t.Fatal(err)
	}
	if err := ltx.EncodeDelta(*deltaHeader, pages, targetState.Checksum, deltaStage); err != nil {
		_ = deltaStage.Close()
		t.Fatalf("encode S3 delta fixture: %v", err)
	}
	if err := deltaStage.Sync(); err != nil {
		_ = deltaStage.Close()
		t.Fatal(err)
	}
	deltaStagePath := deltaStage.Name()
	if err := deltaStage.Close(); err != nil {
		t.Fatal(err)
	}
	if err := storagefs.Publish(deltaStagePath, deltaPath); err != nil {
		t.Fatalf("publish S3 delta fixture: %v", err)
	}
	delta, err := os.Open(deltaPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutImmutable(ctx, remoteKeys[2], delta); err != nil {
		_ = delta.Close()
		t.Fatalf("upload S3 delta: %v", err)
	}
	if err := delta.Close(); err != nil {
		t.Fatal(err)
	}

	restoredPath := filepath.Join(t.TempDir(), "restored.sqlite")
	if err := RestoreS3ToTarget(ctx, store, prefix, restoredPath, Target{TXID: 7}); err != nil {
		t.Fatalf("restore TXID from S3: %v", err)
	}
	assertRecoveryRow(t, restoredPath, "source")
	beforeDelta := int64(1500)
	restoredByTime := filepath.Join(t.TempDir(), "restored-by-time.sqlite")
	if err := RestoreS3ToTarget(ctx, store, prefix, restoredByTime, Target{TimestampMillis: &beforeDelta}); err != nil {
		t.Fatalf("restore timestamp from S3: %v", err)
	}
	assertRecoveryRow(t, restoredByTime, "source")
	afterDelta := int64(2500)
	restoredDeltaByTime := filepath.Join(t.TempDir(), "restored-delta-by-time.sqlite")
	if err := RestoreS3ToTarget(ctx, store, prefix, restoredDeltaByTime, Target{TimestampMillis: &afterDelta}); err != nil {
		t.Fatalf("restore timestamp through S3 delta: %v", err)
	}
	assertRecoveryRow(t, restoredDeltaByTime, "updated")

	crashedCompactionKey := crashAfterS3Compaction(t, ctx, prefix)
	compactedKeyAfterRetry, err := CompactS3ToSnapshot(ctx, store, prefix, Target{TXID: 8})
	if err != nil {
		t.Fatalf("retry S3 compaction after caller crash: %v", err)
	}
	if !strings.HasPrefix(compactedKeyAfterRetry, prefix+"/") {
		t.Fatalf("retried compaction returned invalid S3 key %q", compactedKeyAfterRetry)
	}
	if _, err := store.Head(ctx, compactedKeyAfterRetry); err != nil {
		t.Fatalf("retried compacted snapshot is not present: %v", err)
	}
	compactedKeys = append(compactedKeys, crashedCompactionKey, compactedKeyAfterRetry)

	type compactResult struct {
		key string
		err error
	}
	compactions := make(chan compactResult, 2)
	for range 2 {
		go func() {
			key, err := CompactS3ToSnapshot(ctx, store, prefix, Target{TXID: 8})
			compactions <- compactResult{key: key, err: err}
		}()
	}
	first := <-compactions
	second := <-compactions
	if first.err != nil || second.err != nil {
		t.Fatalf("concurrent S3 compaction errors: %v / %v", first.err, second.err)
	}
	for _, key := range []string{first.key, second.key} {
		if !strings.HasPrefix(key, prefix+"/") || !strings.HasSuffix(key, ".ltx") {
			t.Fatalf("invalid concurrent compaction key %q", key)
		}
		compactedKeys = append(compactedKeys, key)
	}
	plan, err := BuildS3GCPlan(ctx, store, prefix, []Target{{TXID: 8}})
	if err != nil {
		t.Fatalf("build S3 GC dry-run: %v", err)
	}
	if containsString(plan.CandidateObjectKeys, remoteKeys[1]) || containsString(plan.CandidateObjectKeys, remoteKeys[2]) || len(plan.CandidateObjectKeys) == 0 {
		t.Fatalf("unexpected S3 GC candidates: %+v", plan.CandidateObjectKeys)
	}
	extraKey := joinS3Key(prefix, "0002/0000000000000001-0000000000000007.ltx")
	validExtraSnapshot, err := os.Open(localSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutImmutable(ctx, extraKey, validExtraSnapshot); err != nil {
		_ = validExtraSnapshot.Close()
		t.Fatalf("add object to stale the S3 GC plan: %v", err)
	}
	if err := validExtraSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ApplyS3GCPlan(ctx, store, prefix, plan); err != ErrStaleGCPlan {
		t.Fatalf("stale S3 GC plan error = %v, want ErrStaleGCPlan", err)
	}
	if err := store.DeleteBatch(ctx, []string{extraKey}); err != nil {
		t.Fatalf("remove stale-plan fixture: %v", err)
	}
	crashAfterS3Delete(t, ctx, plan.CandidateObjectKeys[0])
	if err := ApplyS3GCPlan(ctx, store, prefix, plan); err != ErrStaleGCPlan {
		t.Fatalf("partially applied S3 GC plan error = %v, want ErrStaleGCPlan", err)
	}
	plan, err = BuildS3GCPlan(ctx, store, prefix, []Target{{TXID: 8}})
	if err != nil {
		t.Fatalf("rebuild S3 GC plan after partial collection: %v", err)
	}
	if err := ApplyS3GCPlan(ctx, store, prefix, plan); err != nil {
		t.Fatalf("apply revalidated S3 GC plan: %v", err)
	}
	for _, candidate := range plan.Plan.Candidates {
		key, err := localKeyToS3Key(prefix, candidate.Key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Head(ctx, key); err == nil {
			t.Fatalf("collected S3 candidate %q still exists", key)
		}
	}
	for _, key := range remoteKeys[1:] {
		if _, err := store.Head(ctx, key); err != nil {
			t.Fatalf("level-zero object %q missing after GC: %v", key, err)
		}
	}
	restoredAfterGC := filepath.Join(t.TempDir(), "restored-after-gc.sqlite")
	if err := RestoreS3ToTarget(ctx, store, prefix, restoredAfterGC, Target{TXID: 8}); err != nil {
		t.Fatalf("restore retained S3 point after GC: %v", err)
	}
	assertRecoveryRow(t, restoredAfterGC, "updated")
	retainedTimestamp := int64(2500)
	restoredByTimeAfterGC := filepath.Join(t.TempDir(), "restored-by-time-after-gc.sqlite")
	if err := RestoreS3ToTarget(ctx, store, prefix, restoredByTimeAfterGC, Target{TimestampMillis: &retainedTimestamp}); err != nil {
		t.Fatalf("restore retained timestamp after S3 GC: %v", err)
	}
	assertRecoveryRow(t, restoredByTimeAfterGC, "updated")
}

func TestFlociS3DeleteCrashChild(t *testing.T) {
	if os.Getenv("SQLITE_REPLICATION_FLOCI_DELETE_CRASH_CHILD") != "1" {
		t.Skip("child process for the isolated Floci GC crash test")
	}
	endpoint := os.Getenv("SQLITE_REPLICATION_FLOCI_ENDPOINT")
	runID := os.Getenv("SQLITE_REPLICATION_FLOCI_RUN_ID")
	key := os.Getenv("SQLITE_REPLICATION_FLOCI_DELETE_KEY")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	awsConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load local S3 SDK config in crash child: %v", err)
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
	store, err := storages3.New(client, "sqlite-replication-m4-"+runID)
	if err != nil {
		t.Fatalf("initialize S3 adapter in crash child: %v", err)
	}
	if err := store.DeleteBatch(ctx, []string{key}); err != nil {
		t.Fatalf("delete one GC candidate in crash child: %v", err)
	}
	if _, err := fmt.Fprintln(os.Stdout, "s3-delete-applied"); err != nil {
		t.Fatalf("signal completed S3 delete to parent: %v", err)
	}
	select {}
}

func TestFlociS3CompactCrashChild(t *testing.T) {
	if os.Getenv("SQLITE_REPLICATION_FLOCI_COMPACT_CRASH_CHILD") != "1" {
		t.Skip("child process for the isolated Floci compaction crash test")
	}
	endpoint := os.Getenv("SQLITE_REPLICATION_FLOCI_ENDPOINT")
	runID := os.Getenv("SQLITE_REPLICATION_FLOCI_RUN_ID")
	prefix := os.Getenv("SQLITE_REPLICATION_FLOCI_COMPACT_PREFIX")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	awsConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load local S3 SDK config in compaction crash child: %v", err)
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
	store, err := storages3.New(client, "sqlite-replication-m4-"+runID)
	if err != nil {
		t.Fatalf("initialize S3 adapter in compaction crash child: %v", err)
	}
	key, err := CompactS3ToSnapshot(ctx, store, prefix, Target{TXID: 8})
	if err != nil {
		t.Fatalf("publish S3 compaction in crash child: %v", err)
	}
	if _, err := fmt.Fprintln(os.Stdout, key); err != nil {
		t.Fatalf("signal published compacted key to parent: %v", err)
	}
	select {}
}

func crashAfterS3Delete(t *testing.T, ctx context.Context, key string) {
	t.Helper()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFlociS3DeleteCrashChild$")
	command.Env = append(os.Environ(),
		"SQLITE_REPLICATION_FLOCI_DELETE_CRASH_CHILD=1",
		"SQLITE_REPLICATION_FLOCI_DELETE_KEY="+key,
	)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("open S3 delete crash-child stdout: %v", err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start S3 delete crash child: %v", err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "s3-delete-applied\n" {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("S3 delete crash child signal = %q, error = %v", line, err)
	}
	if err := command.Process.Kill(); err != nil {
		_ = command.Wait()
		t.Fatalf("kill S3 delete crash child after partial GC: %v", err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("S3 delete crash child exited successfully; expected abrupt termination")
	}
}

func crashAfterS3Compaction(t *testing.T, ctx context.Context, prefix string) string {
	t.Helper()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFlociS3CompactCrashChild$")
	command.Env = append(os.Environ(),
		"SQLITE_REPLICATION_FLOCI_COMPACT_CRASH_CHILD=1",
		"SQLITE_REPLICATION_FLOCI_COMPACT_PREFIX="+prefix,
	)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("open S3 compaction crash-child stdout: %v", err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start S3 compaction crash child: %v", err)
	}
	key, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || !strings.HasPrefix(key, prefix+"/") {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("compaction crash child key = %q, error = %v", key, err)
	}
	if err := command.Process.Kill(); err != nil {
		_ = command.Wait()
		t.Fatalf("kill S3 compaction crash child after publish: %v", err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("S3 compaction crash child exited successfully; expected abrupt termination")
	}
	return strings.TrimSpace(key)
}

func TestFlociS3CrossEngineRestore(t *testing.T) {
	endpoint := os.Getenv("SQLITE_REPLICATION_FLOCI_ENDPOINT")
	runID := os.Getenv("SQLITE_REPLICATION_FLOCI_RUN_ID")
	if endpoint == "" || runID == "" || os.Getenv("SQLITE_REPLICATION_FLOCI_CROSS_ENGINE") != "1" {
		t.Skip("requires the shared Floci bucket from make test-floci")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
		t.Fatalf("initialize S3 adapter for cross-engine restore: %v", err)
	}
	destination := filepath.Join(t.TempDir(), "rust-restored.sqlite")
	if err := RestoreS3ToTarget(ctx, store, "rust-recovery-test", destination, Target{TXID: 8}); err != nil {
		t.Fatalf("restore Rust-published S3 repository with Go: %v", err)
	}
	assertRecoveryRow(t, destination, "updated")
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func assertRecoveryRow(t *testing.T, path, expected string) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	var value string
	if err := database.QueryRow("SELECT value FROM records WHERE id = 1").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != expected {
		t.Fatalf("restored row = %q, want %q", value, expected)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}
