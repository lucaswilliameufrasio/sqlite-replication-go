package replication_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/sqliteadapter"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/replicator"
	_ "modernc.org/sqlite"
)

func TestSnapshotAndOfflineRestore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "source.db")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(dir, "repository")
	ltxPath, err := replication.Snapshot(ctx, databasePath, repositoryRoot)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if _, err := os.Stat(ltxPath); err != nil {
		t.Fatalf("snapshot file missing: %v", err)
	}

	restoredPath := filepath.Join(dir, "restored.db")
	if err := replication.RestoreSnapshot(ctx, repositoryRoot, restoredPath); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	if err := sqliteadapter.VerifyDatabase(ctx, restoredPath); err != nil {
		t.Fatalf("verify restored database: %v", err)
	}
	if got := rowCount(t, databasePath); got != rowCount(t, restoredPath) {
		t.Fatalf("row count changed: source=%d restored=%d", got, rowCount(t, restoredPath))
	}
	if err := replication.RestoreSnapshot(ctx, repositoryRoot, restoredPath); err == nil {
		t.Fatal("restore replaced an existing destination")
	}
}

func TestRestoreToTargetReconstructsIncrementalPointWithoutOverwrite(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("public incremental capture is intentionally limited to Linux amd64")
	}
	ctx := context.Background()
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "source.db")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(dir, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("create initial snapshot: %v", err)
	}

	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO records(value) VALUES (?)", "incremental"); err != nil {
		t.Fatalf("commit incremental row: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := replicator.CaptureOnce(ctx, databasePath, repositoryRoot, func(context.Context) (func() error, error) {
		return func() error { return nil }, nil
	}); err != nil {
		t.Fatalf("capture local WAL: %v", err)
	}

	for _, test := range []struct {
		name   string
		target replicator.RestoreTarget
		rows   int
	}{
		{name: "snapshot TXID", target: replicator.RestoreTarget{TXID: 1}, rows: 100},
		{name: "incremental TXID", target: replicator.RestoreTarget{TXID: 2}, rows: 101},
		{name: "timestamp", target: replicator.RestoreTarget{TimestampMillis: int64Pointer(1<<62 - 1)}, rows: 101},
	} {
		t.Run(test.name, func(t *testing.T) {
			destination := filepath.Join(dir, test.name+".db")
			if err := replicator.RestoreToTarget(ctx, repositoryRoot, destination, test.target); err != nil {
				t.Fatalf("restore target: %v", err)
			}
			if got := rowCount(t, destination); got != test.rows {
				t.Fatalf("restored row count = %d, want %d", got, test.rows)
			}
			if err := os.WriteFile(destination, []byte("preserve"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := replicator.RestoreToTarget(ctx, repositoryRoot, destination, test.target); err == nil {
				t.Fatal("restore overwrote an existing destination")
			}
			contents, err := os.ReadFile(destination)
			if err != nil {
				t.Fatal(err)
			}
			if string(contents) != "preserve" {
				t.Fatalf("existing destination changed to %q", contents)
			}
		})
	}
}

func int64Pointer(value int64) *int64 {
	return &value
}

func TestRestoreRejectsCorruptLTXWithoutPromoting(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "source.db")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(dir, "repository")
	ltxPath, err := replication.Snapshot(ctx, databasePath, repositoryRoot)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	content, err := os.ReadFile(ltxPath)
	if err != nil {
		t.Fatal(err)
	}
	content[len(content)-1] ^= 0x01
	if err := os.WriteFile(ltxPath, content, 0o600); err != nil {
		t.Fatal(err)
	}

	destination := filepath.Join(dir, "must-not-exist.db")
	if err := replication.RestoreSnapshot(ctx, repositoryRoot, destination); err == nil {
		t.Fatal("corrupt LTX snapshot restored successfully")
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination was promoted after corruption: %v", err)
	}
}

func TestRestoreRejectsStaleDestinationSidecars(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "source.db")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(dir, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	destination := filepath.Join(dir, "restored.db")
	staleWAL := destination + "-wal"
	if err := os.WriteFile(staleWAL, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := replication.RestoreSnapshot(ctx, repositoryRoot, destination); err == nil {
		t.Fatal("restore ignored a stale WAL sidecar")
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restore promoted a database alongside stale sidecar: %v", err)
	}
	content, err := os.ReadFile(staleWAL)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "stale" {
		t.Fatal("restore changed the stale sidecar")
	}
}

func createSourceDatabase(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA synchronous=FULL"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := db.Exec("INSERT INTO records(value) VALUES (?)", "row"); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func rowCount(t *testing.T, path string) int {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow("SELECT count(*) FROM records").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
