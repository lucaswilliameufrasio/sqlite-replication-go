package walcapture

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/sqliteadapter"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/ltx"
)

func TestTranslateWALProducedBySQLiteRuntime(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open SQLite database: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close SQLite database: %v", err)
		}
	})
	database.SetMaxOpenConns(1)

	var journalMode string
	if _, err := database.ExecContext(ctx, "PRAGMA page_size=512"); err != nil {
		t.Fatalf("set SQLite page size: %v", err)
	}
	if err := database.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journalMode); err != nil {
		t.Fatalf("enable WAL mode: %v", err)
	}
	if journalMode != "wal" {
		t.Fatalf("journal mode = %q, want wal", journalMode)
	}
	if _, err := database.ExecContext(ctx, "PRAGMA wal_autocheckpoint=0; PRAGMA busy_timeout=0;"); err != nil {
		t.Fatalf("configure SQLite connection: %v", err)
	}
	if _, err := database.ExecContext(ctx, "CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatalf("create records table: %v", err)
	}
	if _, err := database.ExecContext(ctx, "INSERT INTO records VALUES (1, 'base')"); err != nil {
		t.Fatalf("insert base record: %v", err)
	}
	var checkpointBusy, logFrames, checkpointedFrames int
	if err := database.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(
		&checkpointBusy,
		&logFrames,
		&checkpointedFrames,
	); err != nil {
		t.Fatalf("checkpoint initial database: %v", err)
	}
	if checkpointBusy != 0 || (logFrames >= 0 && checkpointedFrames != logFrames) {
		t.Fatalf("initial checkpoint incomplete: busy=%d log=%d checkpointed=%d", checkpointBusy, logFrames, checkpointedFrames)
	}

	basePath := filepath.Join(directory, "base.sqlite")
	if err := sqliteadapter.CreateOnlineBackup(ctx, databasePath, basePath); err != nil {
		t.Fatalf("create initial consistent database image: %v", err)
	}
	baseBytes, err := os.ReadFile(basePath)
	if err != nil {
		t.Fatalf("read initial database image: %v", err)
	}
	baseState, err := ltx.LoadDatabaseState(bytes.NewReader(baseBytes), int64(len(baseBytes)), 1)
	if err != nil {
		t.Fatalf("load initial LTX state: %v", err)
	}

	transactions := []string{
		"UPDATE records SET value='first' WHERE id=1",
		"INSERT INTO records VALUES (2, 'second')",
		"UPDATE records SET value='third' WHERE id=2",
	}
	for _, statement := range transactions {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			t.Fatalf("execute SQLite commit %q: %v", statement, err)
		}
	}

	walBytes, err := os.ReadFile(databasePath + "-wal")
	if err != nil {
		t.Fatalf("read SQLite WAL: %v", err)
	}
	translated, state, err := TranslateWAL(walBytes, State{Database: baseState}, 200)
	if err != nil {
		t.Fatalf("translate SQLite WAL: %v", err)
	}
	if len(translated) != len(transactions) {
		t.Fatalf("translated %d WAL transactions, want %d", len(translated), len(transactions))
	}

	finalPath := filepath.Join(directory, "final.sqlite")
	if err := sqliteadapter.CreateOnlineBackup(ctx, databasePath, finalPath); err != nil {
		t.Fatalf("create final consistent database image: %v", err)
	}
	finalBytes, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("read final database image: %v", err)
	}
	finalState, err := ltx.LoadDatabaseState(bytes.NewReader(finalBytes), int64(len(finalBytes)), state.Database.TXID)
	if err != nil {
		t.Fatalf("load final database state: %v", err)
	}
	if state.Database.Checksum != finalState.Checksum || state.Database.Commit != finalState.Commit {
		t.Fatalf("translated WAL state differs from SQLite backup: translated=%+v backup=%+v", state.Database, finalState)
	}
}
