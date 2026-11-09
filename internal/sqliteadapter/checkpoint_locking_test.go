package sqliteadapter

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"modernc.org/sqlite"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/wal"
)

type checkpointResult struct {
	busy         int
	logFrames    int
	checkpointed int
}

func TestCaptureLeaseHoldsLocksAndReadsStableWAL(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "capture.sqlite")
	database := openCheckpointTestConnection(t, databasePath)
	if _, err := database.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("enable WAL mode: %v", err)
	}
	if _, err := database.ExecContext(ctx, "CREATE TABLE records (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create records table: %v", err)
	}
	if _, err := database.ExecContext(ctx, "INSERT INTO records VALUES (1)"); err != nil {
		t.Fatalf("insert record: %v", err)
	}
	busyWriter := openCheckpointTestConnection(t, databasePath)
	if _, err := busyWriter.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("prepare external writer lock: %v", err)
	}
	if lease, err := BeginCaptureLease(ctx, databasePath); err == nil {
		_ = lease.Close()
		_, _ = busyWriter.ExecContext(ctx, "ROLLBACK")
		t.Fatal("capture lease succeeded while an external writer held BEGIN IMMEDIATE")
	}
	if _, err := busyWriter.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("release prepared external writer: %v", err)
	}
	lease, err := BeginCaptureLease(ctx, databasePath)
	if err != nil {
		t.Fatalf("begin capture lease: %v", err)
	}
	defer func() {
		if err := lease.Close(); err != nil {
			t.Errorf("close capture lease: %v", err)
		}
	}()
	stable, err := lease.ReadStableWAL(ctx)
	if err != nil {
		t.Fatalf("read stable WAL: %v", err)
	}
	scanned, err := wal.Scan(stable.Bytes)
	if err != nil {
		t.Fatalf("validate stable WAL: %v", err)
	}
	if len(scanned.Frames) != stable.Observation.LogFrames {
		t.Fatalf("stable WAL has %d frames, NOOP reports %d", len(scanned.Frames), stable.Observation.LogFrames)
	}
	walPath := databasePath + "-wal"
	walBeforeCheckpoint, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read WAL before checkpoint: %v", err)
	}

	writer := openCheckpointTestConnection(t, databasePath)
	if _, err := writer.ExecContext(ctx, "BEGIN IMMEDIATE"); err == nil {
		t.Fatal("external writer acquired the writer lock during capture")
	}
	checkpointer := openCheckpointTestConnection(t, databasePath)
	status := queryCheckpointStatus(t, ctx, checkpointer, "TRUNCATE")
	if status.busy == 0 {
		t.Fatalf("TRUNCATE unexpectedly completed under capture locks: %+v", status)
	}
	walAfterCheckpoint, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read WAL after checkpoint: %v", err)
	}
	if !bytes.Equal(walBeforeCheckpoint, walAfterCheckpoint) {
		t.Fatal("TRUNCATE changed WAL bytes while capture locks were held")
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("release capture lease: %v", err)
	}
	if _, err := writer.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("external writer did not acquire lock after lease release: %v", err)
	}
	if _, err := writer.ExecContext(ctx, "INSERT INTO records VALUES (2)"); err != nil {
		t.Fatalf("external writer could not write after lease release: %v", err)
	}
	if _, err := writer.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatalf("external writer could not commit after lease release: %v", err)
	}
}

func TestCaptureLeaseKeepsWriterGuardAfterReaderPinRelease(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "capture.sqlite")
	initializer := openCheckpointTestConnection(t, databasePath)
	if _, err := initializer.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("enable WAL mode: %v", err)
	}
	if _, err := initializer.ExecContext(ctx, "CREATE TABLE records (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create records table: %v", err)
	}
	if _, err := initializer.ExecContext(ctx, "INSERT INTO records VALUES (1)"); err != nil {
		t.Fatalf("insert record: %v", err)
	}

	lease, err := BeginCaptureLease(ctx, databasePath)
	if err != nil {
		t.Fatalf("begin capture lease: %v", err)
	}
	writer := openCheckpointTestConnection(t, databasePath)
	readerPinReleased := make(chan struct{})
	continueRelease := make(chan struct{})
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- lease.closeWithHook(func() {
			close(readerPinReleased)
			<-continueRelease
		})
	}()

	readerReleased := false
	select {
	case <-readerPinReleased:
		readerReleased = true
	case <-time.After(5 * time.Second):
	}
	var writerBeginErr error
	var checkpoint checkpointResult
	var checkpointErr error
	if readerReleased {
		_, writerBeginErr = writer.ExecContext(ctx, "BEGIN IMMEDIATE")
		if writerBeginErr == nil {
			_, _ = writer.ExecContext(ctx, "ROLLBACK")
		}
		if err := writer.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&checkpoint.busy, &checkpoint.logFrames, &checkpoint.checkpointed); err != nil {
			checkpointErr = err
		}
	}
	close(continueRelease)
	var closeErr error
	select {
	case closeErr = <-closeResult:
	case <-time.After(5 * time.Second):
		t.Fatal("capture lease did not finish release after the test barrier opened")
	}
	if closeErr != nil {
		t.Fatalf("close capture lease: %v", closeErr)
	}
	if !readerReleased {
		t.Fatal("capture lease did not reach the reader-pin release barrier")
	}
	var sqliteErr *sqlite.Error
	if !errors.As(writerBeginErr, &sqliteErr) || sqliteErr.Code()&0xff != 5 {
		t.Fatalf("writer between reader-pin and guard release returned %v, want SQLITE_BUSY", writerBeginErr)
	}
	if checkpointErr != nil {
		t.Fatalf("checkpoint between reader-pin and guard release: %v", checkpointErr)
	}
	if checkpoint.busy == 0 {
		t.Fatalf("TRUNCATE completed after reader-pin release but before writer-guard release: %+v", checkpoint)
	}
	if _, err := writer.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("writer could not acquire lock after writer-guard release: %v", err)
	}
	if _, err := writer.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("release writer acquired after capture lease: %v", err)
	}
}

func TestImmediateWriterLockAndNoopCheckpointBoundary(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "source.sqlite")
	initializer := openCheckpointTestConnection(t, databasePath)
	var runtimeVersion string
	if err := initializer.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&runtimeVersion); err != nil {
		t.Fatalf("read SQLite runtime version: %v", err)
	}
	if !versionAtLeast(runtimeVersion, minimumSQLiteVersion) {
		t.Fatalf("SQLite runtime %s is below supported version %s", runtimeVersion, minimumSQLiteVersion)
	}
	t.Logf("CP-01 lock experiment SQLite runtime: %s", runtimeVersion)

	var journalMode string
	if err := initializer.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journalMode); err != nil {
		t.Fatalf("enable WAL mode: %v", err)
	}
	if journalMode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", journalMode)
	}
	if _, err := initializer.ExecContext(ctx, "CREATE TABLE records (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create records table: %v", err)
	}
	if _, err := initializer.ExecContext(ctx, "INSERT INTO records(id) VALUES (1)"); err != nil {
		t.Fatalf("insert baseline row: %v", err)
	}

	reader := openCheckpointTestConnection(t, databasePath)
	if _, err := reader.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatalf("begin reader transaction: %v", err)
	}
	var readerCount int
	if err := reader.QueryRowContext(ctx, "SELECT count(*) FROM records").Scan(&readerCount); err != nil {
		t.Fatalf("pin reader snapshot: %v", err)
	}
	if readerCount != 1 {
		t.Fatalf("reader baseline count = %d, want 1", readerCount)
	}

	guard := openCheckpointTestConnection(t, databasePath)
	if _, err := guard.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("acquire immediate writer lock: %v", err)
	}
	backupPath := filepath.Join(t.TempDir(), "guarded-backup.sqlite")
	if err := CreateOnlineBackup(ctx, databasePath, backupPath); err != nil {
		t.Fatalf("create online backup while guard holds writer lock: %v", err)
	}
	if err := VerifyDatabase(ctx, backupPath); err != nil {
		t.Fatalf("verify online backup while guard holds writer lock: %v", err)
	}

	checkpointer := openCheckpointTestConnection(t, databasePath)
	baselineStatus := queryCheckpointStatus(t, ctx, checkpointer, "NOOP")
	if baselineStatus.logFrames <= 0 {
		t.Fatalf("NOOP reported %d log frames; expected the committed WAL frames", baselineStatus.logFrames)
	}
	walPath := databasePath + "-wal"
	walBeforeCheckpoints, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read WAL before checkpoint attempts: %v", err)
	}
	if len(walBeforeCheckpoints) < 32 {
		t.Fatalf("WAL has only %d bytes", len(walBeforeCheckpoints))
	}

	for _, mode := range []string{"PASSIVE", "FULL", "RESTART", "TRUNCATE"} {
		t.Run(mode, func(t *testing.T) {
			status := queryCheckpointStatus(t, ctx, checkpointer, mode)
			if mode != "PASSIVE" && status.busy == 0 {
				t.Fatalf("%s checkpoint did not report that the writer lock prevented completion: %+v", mode, status)
			}
			walAfterCheckpoint, err := os.ReadFile(walPath)
			if err != nil {
				t.Fatalf("read WAL after %s checkpoint attempt: %v", mode, err)
			}
			if !bytes.Equal(walBeforeCheckpoints, walAfterCheckpoint) {
				t.Fatalf("%s checkpoint changed WAL bytes while the immediate writer lock was held", mode)
			}
			statusAfterCheckpoint := queryCheckpointStatus(t, ctx, checkpointer, "NOOP")
			if statusAfterCheckpoint.logFrames != baselineStatus.logFrames {
				t.Fatalf("NOOP boundary changed during %s checkpoint attempt: before=%d after=%d", mode, baselineStatus.logFrames, statusAfterCheckpoint.logFrames)
			}
		})
	}

	writer := openCheckpointTestConnection(t, databasePath)
	if _, err := writer.ExecContext(ctx, "BEGIN IMMEDIATE"); err == nil {
		_, _ = writer.ExecContext(ctx, "ROLLBACK")
		t.Fatal("second writer acquired BEGIN IMMEDIATE while the guard held the writer lock")
	}

	if _, err := guard.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("release immediate writer lock: %v", err)
	}
	if _, err := writer.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("writer could not acquire lock after guard release: %v", err)
	}
	if _, err := writer.ExecContext(ctx, "INSERT INTO records(id) VALUES (2)"); err != nil {
		t.Fatalf("insert concurrent row: %v", err)
	}
	if _, err := writer.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatalf("commit concurrent row: %v", err)
	}

	if err := reader.QueryRowContext(ctx, "SELECT count(*) FROM records").Scan(&readerCount); err != nil {
		t.Fatalf("read from pinned snapshot after writer commit: %v", err)
	}
	if readerCount != 1 {
		t.Fatalf("pinned reader count after writer commit = %d, want 1", readerCount)
	}

	statusAfterWriter := queryCheckpointStatus(t, ctx, checkpointer, "NOOP")
	if statusAfterWriter.logFrames <= baselineStatus.logFrames {
		t.Fatalf("NOOP log boundary did not advance after writer commit: before=%d after=%d", baselineStatus.logFrames, statusAfterWriter.logFrames)
	}
	for _, mode := range []string{"PASSIVE", "FULL", "RESTART", "TRUNCATE"} {
		t.Run("pinned-reader/"+mode, func(t *testing.T) {
			status := queryCheckpointStatus(t, ctx, checkpointer, mode)
			if mode != "PASSIVE" && status.busy == 0 {
				t.Fatalf("%s reported completion while an older reader was pinned: %+v", mode, status)
			}
			if status.logFrames != statusAfterWriter.logFrames || status.checkpointed >= status.logFrames {
				t.Fatalf("%s did not report an incomplete checkpoint behind the pinned reader: %+v", mode, status)
			}
			walAfterCheckpoint, err := os.ReadFile(walPath)
			if err != nil {
				t.Fatalf("read WAL after blocked %s: %v", mode, err)
			}
			if len(walAfterCheckpoint) == 0 {
				t.Fatalf("%s removed WAL bytes newer than the pinned reader", mode)
			}
		})
	}

	if _, err := reader.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("release reader snapshot: %v", err)
	}
	truncateAfterReaderRelease := queryCheckpointStatus(t, ctx, checkpointer, "TRUNCATE")
	if truncateAfterReaderRelease.busy != 0 || truncateAfterReaderRelease.checkpointed < truncateAfterReaderRelease.logFrames {
		t.Fatalf("TRUNCATE did not complete after releasing the pinned reader: %+v", truncateAfterReaderRelease)
	}
}

func openCheckpointTestConnection(t *testing.T, databasePath string) *sql.Conn {
	t.Helper()

	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open SQLite database: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close SQLite database: %v", err)
		}
	})

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire SQLite connection: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close SQLite connection: %v", err)
		}
	})
	for _, pragma := range []string{"PRAGMA busy_timeout=0", "PRAGMA wal_autocheckpoint=0"} {
		if _, err := conn.ExecContext(ctx, pragma); err != nil {
			t.Fatalf("configure test connection with %q: %v", pragma, err)
		}
	}
	return conn
}

func queryCheckpointStatus(t *testing.T, ctx context.Context, conn *sql.Conn, mode string) checkpointResult {
	t.Helper()
	var result checkpointResult
	query := fmt.Sprintf("PRAGMA wal_checkpoint(%s)", mode)
	if err := conn.QueryRowContext(ctx, query).Scan(&result.busy, &result.logFrames, &result.checkpointed); err != nil {
		t.Fatalf("query %s: %v", query, err)
	}
	return result
}
