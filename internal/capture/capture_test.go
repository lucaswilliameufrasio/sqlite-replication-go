package capture

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"modernc.org/sqlite"

	"github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/recovery"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/spool"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
)

func TestCaptureCyclePublishesAndRecoversCheckpointedChanges(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	repositoryRoot := filepath.Join(directory, "repository")
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open source SQLite database: %v", err)
	}
	database.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close source SQLite database: %v", err)
		}
	})
	for _, statement := range []string{
		"PRAGMA page_size=512",
		"PRAGMA journal_mode=WAL",
		"PRAGMA wal_autocheckpoint=0",
		"PRAGMA busy_timeout=0",
		"CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT)",
		"INSERT INTO records VALUES (1, 'base')",
	} {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			t.Fatalf("initialize SQLite database with %q: %v", statement, err)
		}
	}
	var busy, logFrames, checkpointed int
	if err := database.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed); err != nil {
		t.Fatalf("checkpoint baseline database: %v", err)
	}
	if busy != 0 || (logFrames >= 0 && checkpointed != logFrames) {
		t.Fatalf("baseline checkpoint incomplete: busy=%d log=%d checkpointed=%d", busy, logFrames, checkpointed)
	}
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("create base snapshot: %v", err)
	}

	if _, err := database.ExecContext(ctx, "UPDATE records SET value='first' WHERE id=1"); err != nil {
		t.Fatalf("commit first change: %v", err)
	}
	first, err := CaptureOnce(ctx, databasePath, repositoryRoot, 100, nil)
	if err != nil {
		t.Fatalf("capture first WAL commit: %v", err)
	}
	if len(first.Transactions) != 1 || first.State.Database.TXID != 2 {
		t.Fatalf("unexpected first capture result: txs=%d state=%+v", len(first.Transactions), first.State.Database)
	}
	if _, err := os.Stat(storagefs.LTXPath(repositoryRoot, 2, 2)); err != nil {
		t.Fatalf("first LTX delta was not durably published: %v", err)
	}

	repeated, err := CaptureOnce(ctx, databasePath, repositoryRoot, 101, nil)
	if err != nil {
		t.Fatalf("repeat capture without writes: %v", err)
	}
	if len(repeated.Transactions) != 0 || repeated.State.Database.TXID != first.State.Database.TXID {
		t.Fatalf("unchanged database produced duplicate transactions: %+v", repeated)
	}

	if _, err := database.ExecContext(ctx, "UPDATE records SET value='checkpointed' WHERE id=1"); err != nil {
		t.Fatalf("commit change before external checkpoint: %v", err)
	}
	if err := database.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed); err != nil {
		t.Fatalf("checkpoint between capture cycles: %v", err)
	}
	if busy != 0 || (logFrames >= 0 && checkpointed != logFrames) {
		t.Fatalf("external checkpoint incomplete: busy=%d log=%d checkpointed=%d", busy, logFrames, checkpointed)
	}
	checkpointedCapture, err := CaptureOnce(ctx, databasePath, repositoryRoot, 102, nil)
	if err != nil {
		t.Fatalf("capture after WAL reset/checkpoint: %v", err)
	}
	if len(checkpointedCapture.Transactions) != 1 || checkpointedCapture.State.Database.TXID != 3 {
		t.Fatalf("checkpointed source change was not reconciled: %+v", checkpointedCapture)
	}
	if checkpointedCapture.Transactions[0].Header.WALOffset != 0 {
		t.Fatalf("checkpoint reconciliation unexpectedly claims WAL provenance: %+v", checkpointedCapture.Transactions[0].Header)
	}

	if _, err := database.ExecContext(ctx, "UPDATE records SET value='orphan-stage' WHERE id=1"); err != nil {
		t.Fatalf("commit change for crash recovery: %v", err)
	}
	crashErr := errors.New("simulated crash after immutable LTX publication")
	_, err = CaptureOnce(ctx, databasePath, repositoryRoot, 103, func(point FaultPoint, _ int) error {
		if string(point) == string(spool.AfterLTXPublish) {
			return crashErr
		}
		return nil
	})
	if !errors.Is(err, crashErr) {
		t.Fatalf("expected injected post-publication failure, got %v", err)
	}

	recovered, err := CaptureOnce(ctx, databasePath, repositoryRoot, 104, nil)
	if err != nil {
		t.Fatalf("recover after LTX publication before watermark: %v", err)
	}
	if len(recovered.Transactions) != 0 || recovered.State.Database.TXID != 4 {
		t.Fatalf("recovery duplicated the already-published commit: %+v", recovered)
	}
	watermark, err := spool.ReadWatermark(repositoryRoot)
	if err != nil {
		t.Fatalf("read final capture watermark: %v", err)
	}
	if watermark.TXID != 4 || watermark.DatabaseChecksum != recovered.State.Database.Checksum {
		t.Fatalf("final watermark differs from recovered chain: %+v", watermark)
	}

	for _, value := range []string{"batch-a", "batch-b", "batch-c"} {
		if _, err := database.ExecContext(ctx, "UPDATE records SET value=? WHERE id=1", value); err != nil {
			t.Fatalf("commit batched change %q: %v", value, err)
		}
	}
	partialPublicationErr := errors.New("simulated crash after first of several LTX publications")
	_, err = CaptureOnce(ctx, databasePath, repositoryRoot, 105, func(point FaultPoint, index int) error {
		if string(point) == string(spool.AfterLTXPublish) && index == 0 {
			return partialPublicationErr
		}
		return nil
	})
	if !errors.Is(err, partialPublicationErr) {
		t.Fatalf("expected partial-batch publication failure, got %v", err)
	}

	remaining, err := CaptureOnce(ctx, databasePath, repositoryRoot, 106, nil)
	if err != nil {
		t.Fatalf("resume after partial-batch publication: %v", err)
	}
	if len(remaining.Transactions) != 2 || remaining.State.Database.TXID != 7 {
		t.Fatalf("recovery skipped or duplicated remaining WAL commits: transactions=%d state=%+v", len(remaining.Transactions), remaining.State.Database)
	}
	watermark, err = spool.ReadWatermark(repositoryRoot)
	if err != nil {
		t.Fatalf("read final batched watermark: %v", err)
	}
	if watermark.TXID != 7 {
		t.Fatalf("final watermark TXID = %d, want 7", watermark.TXID)
	}
}

func TestCaptureBatchLimitRejectsBeforePublicationAndRetries(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "source.sqlite")
	repository := filepath.Join(dir, "repository")
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE data(value TEXT); INSERT INTO data VALUES ('base')"); err != nil {
		t.Fatal(err)
	}
	if _, err := replication.Snapshot(ctx, source, repository); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE data SET value='after limit'"); err != nil {
		t.Fatal(err)
	}
	if _, err := CaptureOnceWithBatchLimit(ctx, source, repository, 100, nil, 1<<20, 1<<20, 1); err == nil {
		t.Fatal("expected LTX batch limit failure")
	}
	watermark, err := spool.ReadWatermark(repository)
	if err != nil || watermark.TXID != 1 {
		t.Fatalf("watermark after failure: %+v, %v", watermark, err)
	}
	result, err := CaptureOnceWithBatchLimit(ctx, source, repository, 100, nil, 1<<20, 1<<20, 1<<20)
	if err != nil || result.State.Database.TXID != 2 {
		t.Fatalf("retry: %+v, %v", result.State.Database, err)
	}
	restored := filepath.Join(dir, "restored.sqlite")
	if err := recovery.RestoreToTarget(ctx, repository, restored, recovery.Target{TXID: 2}); err != nil {
		t.Fatalf("restore after retry: %v", err)
	}
	reader, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var value string
	if err := reader.QueryRowContext(ctx, "SELECT value FROM data").Scan(&value); err != nil || value != "after limit" {
		t.Fatalf("restored value: %q, %v", value, err)
	}
}

func TestCaptureSpoolLimitRejectsAndRestoresAfterRetry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "source.sqlite")
	repository := filepath.Join(dir, "repository")
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE data(value TEXT); INSERT INTO data VALUES ('base')"); err != nil {
		t.Fatal(err)
	}
	if _, err := replication.Snapshot(ctx, source, repository); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE data SET value='spool retry'"); err != nil {
		t.Fatal(err)
	}
	if _, err := CaptureOnceWithSpoolLimit(ctx, source, repository, 100, nil, 1); err == nil {
		t.Fatal("expected spool limit failure")
	}
	watermark, err := spool.ReadWatermark(repository)
	if err != nil || watermark.TXID != 1 {
		t.Fatalf("watermark after failure: %+v, %v", watermark, err)
	}
	result, err := CaptureOnceWithSpoolLimit(ctx, source, repository, 100, nil, 1<<20)
	if err != nil || result.State.Database.TXID != 2 {
		t.Fatalf("retry: %+v, %v", result.State.Database, err)
	}
	restored := filepath.Join(dir, "restored.sqlite")
	if err := recovery.RestoreToTarget(ctx, repository, restored, recovery.Target{TXID: 2}); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var value string
	if err := reader.QueryRowContext(ctx, "SELECT value FROM data").Scan(&value); err != nil || value != "spool retry" {
		t.Fatalf("restored value: %q, %v", value, err)
	}
}

func TestCaptureAfterSnapshotWithUncheckpointedWALIsNoOp(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	repositoryRoot := filepath.Join(directory, "repository")
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = database.Close() })
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA wal_autocheckpoint=0",
		"CREATE TABLE records (value TEXT)",
		"INSERT INTO records VALUES ('baseline')",
	} {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			t.Fatalf("initialize %q: %v", statement, err)
		}
	}
	info, err := os.Stat(databasePath + "-wal")
	if err != nil || info.Size() == 0 {
		t.Fatalf("expected uncheckpointed WAL: info=%v err=%v", info, err)
	}
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	for cycle := int64(0); cycle < 2; cycle++ {
		captured, err := CaptureOnce(ctx, databasePath, repositoryRoot, 100+cycle, nil)
		if err != nil {
			t.Fatalf("no-op capture %d: %v", cycle, err)
		}
		if len(captured.Transactions) != 0 || captured.State.Database.TXID != 1 {
			t.Fatalf("no-op capture %d: txid=%d transactions=%d", cycle, captured.State.Database.TXID, len(captured.Transactions))
		}
	}
	if _, err := database.ExecContext(ctx, "INSERT INTO records VALUES ('new')"); err != nil {
		t.Fatalf("new commit: %v", err)
	}
	captured, err := CaptureOnce(ctx, databasePath, repositoryRoot, 102, nil)
	if err != nil {
		t.Fatalf("capture new commit: %v", err)
	}
	if len(captured.Transactions) != 1 || captured.State.Database.TXID != 2 {
		t.Fatalf("new capture: txid=%d transactions=%d", captured.State.Database.TXID, len(captured.Transactions))
	}
	restoredPath := filepath.Join(directory, "restored.sqlite")
	if err := recovery.RestoreToTarget(ctx, repositoryRoot, restoredPath, recovery.Target{TXID: 2}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	restored, err := sql.Open("sqlite", restoredPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restored.Close() })
	var count int
	if err := restored.QueryRowContext(ctx, "SELECT COUNT(*) FROM records WHERE value IN ('baseline', 'new')").Scan(&count); err != nil || count != 2 {
		t.Fatalf("restored records=%d error=%v", count, err)
	}
}

func TestCaptureReopensAfterBusyWriterLeaseFailure(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	repositoryRoot := filepath.Join(directory, "repository")
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open source SQLite database: %v", err)
	}
	database.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close source SQLite database: %v", err)
		}
	})
	for _, statement := range []string{
		"PRAGMA page_size=512",
		"PRAGMA journal_mode=WAL",
		"PRAGMA wal_autocheckpoint=0",
		"PRAGMA busy_timeout=0",
		"CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT)",
		"INSERT INTO records VALUES (1, 'base')",
	} {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			t.Fatalf("initialize SQLite database with %q: %v", statement, err)
		}
	}
	if _, err := database.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatalf("checkpoint baseline database: %v", err)
	}
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("create base snapshot: %v", err)
	}
	if _, err := database.ExecContext(ctx, "UPDATE records SET value='captured-before-busy' WHERE id=1"); err != nil {
		t.Fatalf("commit source value before busy lease attempt: %v", err)
	}
	beforeBusy, err := CaptureOnce(ctx, databasePath, repositoryRoot, 599, nil)
	if err != nil {
		t.Fatalf("create watermark before busy lease attempt: %v", err)
	}
	if len(beforeBusy.Transactions) != 1 || beforeBusy.State.Database.TXID != 2 {
		t.Fatalf("initial capture before busy attempt = transactions:%d txid:%d, want 1 and 2", len(beforeBusy.Transactions), beforeBusy.State.Database.TXID)
	}
	if _, err := database.ExecContext(ctx, "UPDATE records SET value='pending-capture' WHERE id=1"); err != nil {
		t.Fatalf("commit source value for capture: %v", err)
	}

	external := openCaptureTestConnection(t, databasePath)
	if _, err := external.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("acquire external writer lock: %v", err)
	}
	_, err = CaptureOnce(ctx, databasePath, repositoryRoot, 600, nil)
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) || sqliteErr.Code()&0xff != 5 {
		t.Fatalf("capture under external writer lock returned %v, want SQLITE_BUSY", err)
	}
	watermark, err := spool.ReadWatermark(repositoryRoot)
	if err != nil {
		t.Fatalf("read watermark after busy lease failure: %v", err)
	}
	if watermark.TXID != beforeBusy.State.Database.TXID || watermark.DatabaseChecksum != beforeBusy.State.Database.Checksum {
		t.Fatalf("busy lease failure changed the existing watermark: %+v", watermark)
	}
	if _, err := external.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("release external writer lock: %v", err)
	}

	retried, err := CaptureOnce(ctx, databasePath, repositoryRoot, 601, nil)
	if err != nil {
		t.Fatalf("retry capture after releasing external writer: %v", err)
	}
	if len(retried.Transactions) != 1 || retried.State.Database.TXID != 3 {
		t.Fatalf("retry did not capture the pending commit: txs=%d txid=%d", len(retried.Transactions), retried.State.Database.TXID)
	}
	watermark, err = spool.ReadWatermark(repositoryRoot)
	if err != nil {
		t.Fatalf("read watermark after successful retry: %v", err)
	}
	if watermark.TXID != 3 || watermark.DatabaseChecksum != retried.State.Database.Checksum {
		t.Fatalf("watermark differs after successful retry: %+v", watermark)
	}
}

func TestCaptureCancellationAfterPartialPublicationReleasesLeaseAndRecovers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	repositoryRoot := filepath.Join(directory, "repository")
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open source SQLite database: %v", err)
	}
	database.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close source SQLite database: %v", err)
		}
	})
	for _, statement := range []string{
		"PRAGMA page_size=512",
		"PRAGMA journal_mode=WAL",
		"PRAGMA wal_autocheckpoint=0",
		"PRAGMA busy_timeout=0",
		"CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT)",
		"INSERT INTO records VALUES (1, 'base')",
	} {
		if _, err := database.ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("initialize SQLite database with %q: %v", statement, err)
		}
	}
	var busy, logFrames, checkpointed int
	if err := database.QueryRowContext(context.Background(), "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed); err != nil {
		t.Fatalf("checkpoint baseline database: %v", err)
	}
	if busy != 0 || (logFrames >= 0 && checkpointed != logFrames) {
		t.Fatalf("baseline checkpoint incomplete: busy=%d log=%d checkpointed=%d", busy, logFrames, checkpointed)
	}
	if _, err := replication.Snapshot(context.Background(), databasePath, repositoryRoot); err != nil {
		t.Fatalf("create base snapshot: %v", err)
	}
	for _, value := range []string{"first", "second", "third"} {
		if _, err := database.ExecContext(context.Background(), "UPDATE records SET value=? WHERE id=1", value); err != nil {
			t.Fatalf("commit source value %q: %v", value, err)
		}
	}

	writer := openCaptureTestConnection(t, databasePath)
	cancelledAfterPublication := false
	_, err = CaptureOnce(ctx, databasePath, repositoryRoot, 400, func(point FaultPoint, index int) error {
		if string(point) == string(spool.AfterLTXPublish) && index == 0 {
			cancelledAfterPublication = true
			cancel()
		}
		return nil
	})
	if !cancelledAfterPublication {
		t.Fatal("capture did not reach the selected publication cancellation boundary")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("capture error = %v, want context cancellation", err)
	}
	firstPublished := storagefs.LTXPath(repositoryRoot, 2, 2)
	if _, err := os.Stat(firstPublished); err != nil {
		t.Fatalf("first immutable LTX was not retained after cancellation: %v", err)
	}
	secondPublished := storagefs.LTXPath(repositoryRoot, 3, 3)
	if _, err := os.Stat(secondPublished); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("second LTX exists after cancellation at first publication: stat error=%v", err)
	}
	watermark, err := spool.ReadWatermark(repositoryRoot)
	if err != nil {
		t.Fatalf("read watermark after cancellation: %v", err)
	}
	if watermark.TXID != 1 {
		t.Fatalf("watermark advanced to TXID %d after partial cancellation; want 1", watermark.TXID)
	}
	if _, err := writer.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("capture did not release writer guard after cancellation: %v", err)
	}
	if _, err := writer.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		t.Fatalf("release writer acquired after cancellation: %v", err)
	}

	recovered, err := CaptureOnce(context.Background(), databasePath, repositoryRoot, 401, nil)
	if err != nil {
		t.Fatalf("recover partial publication after cancellation: %v", err)
	}
	if recovered.State.Database.TXID != 4 || len(recovered.Transactions) != 2 {
		t.Fatalf("recovery skipped or duplicated WAL commits: txid=%d transactions=%d", recovered.State.Database.TXID, len(recovered.Transactions))
	}
	var latestValue string
	if err := database.QueryRowContext(context.Background(), "SELECT value FROM records WHERE id=1").Scan(&latestValue); err != nil {
		t.Fatalf("read final source value: %v", err)
	}
	if latestValue != "third" {
		t.Fatalf("final source value = %q, want %q", latestValue, "third")
	}
	watermark, err = spool.ReadWatermark(repositoryRoot)
	if err != nil {
		t.Fatalf("read recovered watermark: %v", err)
	}
	if watermark.TXID != 4 || watermark.DatabaseChecksum != recovered.State.Database.Checksum {
		t.Fatalf("recovered watermark differs from source state: %+v", watermark)
	}

	if _, err := database.ExecContext(context.Background(), "UPDATE records SET value='fourth' WHERE id=1"); err != nil {
		t.Fatalf("commit source value before watermark cancellation: %v", err)
	}
	watermarkContext, cancelBeforeWatermark := context.WithCancel(context.Background())
	defer cancelBeforeWatermark()
	_, err = CaptureOnce(watermarkContext, databasePath, repositoryRoot, 402, func(point FaultPoint, _ int) error {
		if string(point) == string(spool.AfterWatermarkSync) {
			cancelBeforeWatermark()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("capture error at watermark promotion boundary = %v, want context cancellation", err)
	}
	watermark, err = spool.ReadWatermark(repositoryRoot)
	if err != nil {
		t.Fatalf("read watermark after pre-promotion cancellation: %v", err)
	}
	if watermark.TXID != 4 {
		t.Fatalf("watermark advanced to TXID %d before promotion; want 4", watermark.TXID)
	}
	finalLTX := storagefs.LTXPath(repositoryRoot, 5, 5)
	if _, err := os.Stat(finalLTX); err != nil {
		t.Fatalf("final immutable LTX was not retained before watermark promotion: %v", err)
	}
	if _, err := writer.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("capture did not release writer guard at watermark cancellation: %v", err)
	}
	if _, err := writer.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		t.Fatalf("release writer acquired after watermark cancellation: %v", err)
	}
	resumed, err := CaptureOnce(context.Background(), databasePath, repositoryRoot, 403, nil)
	if err != nil {
		t.Fatalf("recover after cancellation before watermark promotion: %v", err)
	}
	if resumed.State.Database.TXID != 5 || len(resumed.Transactions) != 0 {
		t.Fatalf("recovery did not adopt already-published LTX: txid=%d transactions=%d", resumed.State.Database.TXID, len(resumed.Transactions))
	}
	watermark, err = spool.ReadWatermark(repositoryRoot)
	if err != nil {
		t.Fatalf("read watermark after promotion recovery: %v", err)
	}
	if watermark.TXID != 5 || watermark.DatabaseChecksum != resumed.State.Database.Checksum {
		t.Fatalf("recovered watermark differs from source state: %+v", watermark)
	}

	finalContext, cancelAfterWatermark := context.WithCancel(context.Background())
	defer cancelAfterWatermark()
	completed, err := CaptureOnce(finalContext, databasePath, repositoryRoot, 404, func(point FaultPoint, _ int) error {
		if string(point) == string(spool.AfterWatermark) {
			cancelAfterWatermark()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("capture returned cancellation after durable watermark: %v", err)
	}
	if completed.State.Database.TXID != 5 {
		t.Fatalf("post-watermark capture TXID = %d, want 5", completed.State.Database.TXID)
	}
}

func TestCaptureSerializesUnmanagedWriterAndReconcilesAutocheckpoint(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	repositoryRoot := filepath.Join(directory, "repository")
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open source SQLite database: %v", err)
	}
	database.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close source SQLite database: %v", err)
		}
	})
	for _, statement := range []string{
		"PRAGMA page_size=512",
		"PRAGMA journal_mode=WAL",
		"PRAGMA wal_autocheckpoint=0",
		"CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT)",
		"INSERT INTO records VALUES (1, 'base')",
	} {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			t.Fatalf("initialize SQLite database with %q: %v", statement, err)
		}
	}
	var busy, logFrames, checkpointed int
	if err := database.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed); err != nil {
		t.Fatalf("checkpoint baseline database: %v", err)
	}
	if busy != 0 || (logFrames >= 0 && checkpointed != logFrames) {
		t.Fatalf("baseline checkpoint incomplete: busy=%d log=%d checkpointed=%d", busy, logFrames, checkpointed)
	}
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("create base snapshot: %v", err)
	}
	if _, err := database.ExecContext(ctx, "UPDATE records SET value='managed-capture' WHERE id=1"); err != nil {
		t.Fatalf("commit source value for capture: %v", err)
	}

	externalDB, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open unmanaged application connection: %v", err)
	}
	externalDB.SetMaxOpenConns(1)
	externalDB.SetMaxIdleConns(1)
	t.Cleanup(func() {
		if err := externalDB.Close(); err != nil {
			t.Errorf("close unmanaged application connection: %v", err)
		}
	})
	external, err := externalDB.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire unmanaged application connection: %v", err)
	}
	t.Cleanup(func() {
		if err := external.Close(); err != nil {
			t.Errorf("release unmanaged application connection: %v", err)
		}
	})
	if _, err := external.ExecContext(ctx, "PRAGMA busy_timeout=250"); err != nil {
		t.Fatalf("configure external busy timeout: %v", err)
	}
	if _, err := external.ExecContext(ctx, "PRAGMA wal_autocheckpoint=1"); err != nil {
		t.Fatalf("enable external autocheckpoint: %v", err)
	}
	var busyTimeout, autoCheckpoint int
	if err := external.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		t.Fatalf("read external busy timeout: %v", err)
	}
	if err := external.QueryRowContext(ctx, "PRAGMA wal_autocheckpoint").Scan(&autoCheckpoint); err != nil {
		t.Fatalf("read external autocheckpoint: %v", err)
	}
	if busyTimeout != 250 || autoCheckpoint != 1 {
		t.Fatalf("unmanaged connection settings = busy_timeout:%d autocheckpoint:%d", busyTimeout, autoCheckpoint)
	}

	observedBusy := false
	captured, err := CaptureOnce(ctx, databasePath, repositoryRoot, 500, func(point FaultPoint, _ int) error {
		if point != AfterWALRead {
			return nil
		}
		_, beginErr := external.ExecContext(ctx, "BEGIN IMMEDIATE")
		if beginErr == nil {
			_, _ = external.ExecContext(ctx, "ROLLBACK")
			return errors.New("unmanaged writer acquired lock during capture")
		}
		var sqliteErr *sqlite.Error
		if !errors.As(beginErr, &sqliteErr) || sqliteErr.Code() != 5 {
			return fmt.Errorf("unmanaged writer returned %v, want SQLITE_BUSY", beginErr)
		}
		observedBusy = true
		return nil
	})
	if err != nil {
		t.Fatalf("capture with unmanaged writer attempt: %v", err)
	}
	if !observedBusy || captured.State.Database.TXID != 2 {
		t.Fatalf("unmanaged writer barrier/capture state unexpected: busy=%t txid=%d", observedBusy, captured.State.Database.TXID)
	}

	if _, err := external.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("unmanaged writer could not begin after capture: %v", err)
	}
	if _, err := external.ExecContext(ctx, "UPDATE records SET value='external-after-capture' WHERE id=1"); err != nil {
		t.Fatalf("unmanaged writer update after capture: %v", err)
	}
	if _, err := external.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatalf("unmanaged writer commit after capture: %v", err)
	}
	if err := external.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed); err != nil {
		t.Fatalf("truncate WAL from unmanaged connection: %v", err)
	}
	if busy != 0 || (logFrames >= 0 && checkpointed != logFrames) {
		t.Fatalf("external TRUNCATE incomplete: busy=%d log=%d checkpointed=%d", busy, logFrames, checkpointed)
	}

	reconciled, err := CaptureOnce(ctx, databasePath, repositoryRoot, 501, nil)
	if err != nil {
		t.Fatalf("reconcile unmanaged checkpointed commit: %v", err)
	}
	if len(reconciled.Transactions) != 1 || reconciled.State.Database.TXID != 3 {
		t.Fatalf("external checkpoint commit not reconciled: txid=%d transactions=%d", reconciled.State.Database.TXID, len(reconciled.Transactions))
	}
	if reconciled.Transactions[0].Header.WALOffset != 0 {
		t.Fatalf("reconciled external commit falsely claims WAL provenance: %+v", reconciled.Transactions[0].Header)
	}
	watermark, err := spool.ReadWatermark(repositoryRoot)
	if err != nil {
		t.Fatalf("read final watermark: %v", err)
	}
	if watermark.TXID != 3 || watermark.DatabaseChecksum != reconciled.State.Database.Checksum {
		t.Fatalf("final watermark differs from reconciled database: %+v", watermark)
	}
}

func TestCaptureReleaseWindowSurvivesWriterAndCheckpoint(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	repositoryRoot := filepath.Join(directory, "repository")
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open source SQLite database: %v", err)
	}
	database.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close source SQLite database: %v", err)
		}
	})
	for _, statement := range []string{
		"PRAGMA page_size=512",
		"PRAGMA journal_mode=WAL",
		"PRAGMA wal_autocheckpoint=0",
		"PRAGMA busy_timeout=0",
		"CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT)",
		"INSERT INTO records VALUES (1, 'base')",
	} {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			t.Fatalf("initialize SQLite database with %q: %v", statement, err)
		}
	}
	var busy, logFrames, checkpointed int
	if err := database.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed); err != nil {
		t.Fatalf("checkpoint baseline database: %v", err)
	}
	if busy != 0 || (logFrames >= 0 && checkpointed != logFrames) {
		t.Fatalf("baseline checkpoint incomplete: busy=%d log=%d checkpointed=%d", busy, logFrames, checkpointed)
	}
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("create base snapshot: %v", err)
	}
	if _, err := database.ExecContext(ctx, "UPDATE records SET value='before-capture' WHERE id=1"); err != nil {
		t.Fatalf("commit change before capture: %v", err)
	}

	writer := openCaptureTestConnection(t, databasePath)
	boundaries := []FaultPoint{AfterWALRead, AfterSnapshot, AfterTranslation, AfterPublication}
	observedBoundaries := make(map[FaultPoint]bool, len(boundaries))
	captured, err := CaptureOnce(ctx, databasePath, repositoryRoot, 200, func(point FaultPoint, _ int) error {
		for _, boundary := range boundaries {
			if point != boundary {
				continue
			}
			_, beginErr := writer.ExecContext(ctx, "BEGIN IMMEDIATE")
			if beginErr == nil {
				_, _ = writer.ExecContext(ctx, "ROLLBACK")
				return fmt.Errorf("writer acquired BEGIN IMMEDIATE at capture boundary %q", boundary)
			}
			var sqliteErr *sqlite.Error
			if !errors.As(beginErr, &sqliteErr) || sqliteErr.Code() != 5 {
				return fmt.Errorf("writer at capture boundary %q returned %v, want SQLITE_BUSY", boundary, beginErr)
			}
			observedBoundaries[boundary] = true
			if boundary == AfterPublication {
				watermark, err := spool.ReadWatermark(repositoryRoot)
				if err != nil {
					return fmt.Errorf("read watermark before checkpoint boundary: %w", err)
				}
				if watermark.TXID != 2 {
					return fmt.Errorf("published watermark TXID = %d before checkpoint, want 2", watermark.TXID)
				}
				var checkpointBusy, checkpointFrames, checkpointedFrames int
				if err := writer.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&checkpointBusy, &checkpointFrames, &checkpointedFrames); err != nil {
					return fmt.Errorf("attempt checkpoint before releasing capture lease: %w", err)
				}
				if checkpointBusy == 0 {
					return fmt.Errorf("TRUNCATE completed while capture lease was active: busy=%d log=%d checkpointed=%d", checkpointBusy, checkpointFrames, checkpointedFrames)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("capture under deterministic writer/checkpoint barriers: %v", err)
	}
	if captured.State.Database.TXID != 2 || len(captured.Transactions) != 1 {
		t.Fatalf("unexpected first capture result: %+v", captured)
	}
	for _, boundary := range boundaries {
		if !observedBoundaries[boundary] {
			t.Errorf("writer was not tested at capture boundary %q", boundary)
		}
	}

	if _, err := writer.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("writer could not acquire lock immediately after capture release: %v", err)
	}
	if _, err := writer.ExecContext(ctx, "INSERT INTO records VALUES (2, 'after-release')"); err != nil {
		t.Fatalf("write after capture release: %v", err)
	}
	if _, err := writer.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatalf("commit after capture release: %v", err)
	}
	if err := writer.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed); err != nil {
		t.Fatalf("checkpoint durable capture followed by the post-release writer: %v", err)
	}
	if busy != 0 || (logFrames >= 0 && checkpointed != logFrames) {
		t.Fatalf("post-release checkpoint incomplete: busy=%d log=%d checkpointed=%d", busy, logFrames, checkpointed)
	}

	reconciled, err := CaptureOnce(ctx, databasePath, repositoryRoot, 201, nil)
	if err != nil {
		t.Fatalf("capture commit written after lease release and checkpointed before the next pin: %v", err)
	}
	if len(reconciled.Transactions) != 1 || reconciled.State.Database.TXID != 3 {
		t.Fatalf("post-release commit was lost or duplicated: %+v", reconciled)
	}
	if reconciled.Transactions[0].Header.WALOffset != 0 {
		t.Fatalf("checkpoint reconciliation unexpectedly claims WAL provenance: %+v", reconciled.Transactions[0].Header)
	}
	var value string
	if err := database.QueryRowContext(ctx, "SELECT value FROM records WHERE id=2").Scan(&value); err != nil {
		t.Fatalf("read post-release committed row: %v", err)
	}
	if value != "after-release" {
		t.Fatalf("post-release row value = %q, want after-release", value)
	}
	watermark, err := spool.ReadWatermark(repositoryRoot)
	if err != nil {
		t.Fatalf("read final capture watermark: %v", err)
	}
	if watermark.TXID != 3 || watermark.DatabaseChecksum != reconciled.State.Database.Checksum {
		t.Fatalf("final watermark differs from reconciled source state: %+v", watermark)
	}
}

func openCaptureTestConnection(t *testing.T, databasePath string) *sql.Conn {
	t.Helper()

	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open capture test connection: %v", err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close capture test database: %v", err)
		}
	})
	connection, err := database.Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire capture test connection: %v", err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close capture test connection: %v", err)
		}
	})
	if _, err := connection.ExecContext(context.Background(), "PRAGMA busy_timeout=0; PRAGMA wal_autocheckpoint=0;"); err != nil {
		t.Fatalf("configure capture test connection: %v", err)
	}
	return connection
}
