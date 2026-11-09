//go:build linux

package capture

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/spool"
)

func TestCaptureRecoversAfterRealENOSPCOnBoundedTmpfs(t *testing.T) {
	tmpfsRoot := os.Getenv("SQLITE_REPLICATION_ENOSPC_ROOT")
	if tmpfsRoot == "" {
		t.Skip("run the bounded tmpfs drill with make test-cp01-enospc")
	}
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(tmpfsRoot, &filesystem); err != nil {
		t.Fatalf("inspect bounded ENOSPC filesystem: %v", err)
	}
	if uint64(filesystem.Type) != 0x01021994 {
		t.Fatalf("ENOSPC root filesystem type = %#x, want Linux tmpfs", filesystem.Type)
	}
	capacity := uint64(filesystem.Blocks) * uint64(filesystem.Bsize)
	if capacity == 0 || capacity > 16<<20 {
		t.Fatalf("ENOSPC tmpfs capacity = %d bytes, want a bounded filesystem no larger than 16 MiB", capacity)
	}

	repositoryRoot, err := os.MkdirTemp(tmpfsRoot, "go-cp01-enospc-")
	if err != nil {
		t.Fatalf("create isolated repository on bounded tmpfs: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(repositoryRoot); err != nil {
			t.Errorf("remove isolated bounded-tmpfs repository: %v", err)
		}
	})
	databasePath := filepath.Join(t.TempDir(), "source.sqlite")
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
	ctx := context.Background()
	for _, statement := range []string{
		"PRAGMA page_size=512",
		"PRAGMA journal_mode=WAL",
		"PRAGMA wal_autocheckpoint=0",
		"PRAGMA busy_timeout=0",
		"CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT)",
		"INSERT INTO records VALUES (1, 'base')",
	} {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			t.Fatalf("initialize source database with %q: %v", statement, err)
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
		t.Fatalf("create base snapshot on bounded tmpfs: %v", err)
	}

	const commitCount = 1000
	for index := 0; index < commitCount; index++ {
		value := fmt.Sprintf("value-%05d", index)
		if _, err := database.ExecContext(ctx, "UPDATE records SET value=? WHERE id=1", value); err != nil {
			t.Fatalf("commit WAL transaction %d: %v", index+1, err)
		}
	}
	fillerPath := fillTmpfsExcept(t, tmpfsRoot, 128<<10)

	writer := openCaptureTestConnection(t, databasePath)
	_, captureErr := CaptureOnce(ctx, databasePath, repositoryRoot, 300, nil)
	if !errors.Is(captureErr, syscall.ENOSPC) {
		t.Fatalf("capture error = %v, want real ENOSPC from the bounded tmpfs", captureErr)
	}
	if published := countPublishedLTX(t, repositoryRoot); published <= 1 || published >= commitCount+1 {
		t.Fatalf("bounded tmpfs failure published %d LTX files; want a partial chain after the base snapshot", published)
	}
	watermark, err := spool.ReadWatermark(repositoryRoot)
	if err != nil {
		t.Fatalf("read watermark after real ENOSPC: %v", err)
	}
	if watermark.TXID != 1 {
		t.Fatalf("watermark advanced to TXID %d after incomplete publication; want 1", watermark.TXID)
	}
	if _, err := writer.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("capture did not release the SQLite writer guard after ENOSPC: %v", err)
	}
	if _, err := writer.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("release post-ENOSPC writer transaction: %v", err)
	}
	if err := os.Remove(fillerPath); err != nil {
		t.Fatalf("free bounded tmpfs after ENOSPC: %v", err)
	}

	recovered, err := CaptureOnce(ctx, databasePath, repositoryRoot, 301, nil)
	if err != nil {
		t.Fatalf("recover partial chain after freeing tmpfs: %v", err)
	}
	if recovered.State.Database.TXID != commitCount+1 {
		t.Fatalf("recovered TXID = %d, want %d", recovered.State.Database.TXID, commitCount+1)
	}
	var latestValue string
	if err := database.QueryRowContext(ctx, "SELECT value FROM records WHERE id=1").Scan(&latestValue); err != nil {
		t.Fatalf("read final source value: %v", err)
	}
	if latestValue != fmt.Sprintf("value-%05d", commitCount-1) {
		t.Fatalf("final source value = %q, want final commit", latestValue)
	}
	watermark, err = spool.ReadWatermark(repositoryRoot)
	if err != nil {
		t.Fatalf("read recovered watermark: %v", err)
	}
	if watermark.TXID != commitCount+1 || watermark.DatabaseChecksum != recovered.State.Database.Checksum {
		t.Fatalf("recovered watermark does not match the final chain: %+v", watermark)
	}
}

func fillTmpfsExcept(t *testing.T, tmpfsRoot string, reserve uint64) string {
	t.Helper()
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(tmpfsRoot, &filesystem); err != nil {
		t.Fatalf("inspect tmpfs before filling: %v", err)
	}
	available := uint64(filesystem.Bavail) * uint64(filesystem.Bsize)
	if available <= reserve {
		t.Fatalf("tmpfs has %d bytes available, cannot preserve %d bytes for capture setup", available, reserve)
	}
	fillerPath := filepath.Join(tmpfsRoot, "go-cp01-enospc-filler")
	filler, err := os.OpenFile(fillerPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("create tmpfs filler: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Remove(fillerPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Errorf("remove tmpfs filler: %v", err)
		}
	})
	buffer := bytes.Repeat([]byte{0xa5}, 64<<10)
	remaining := available - reserve
	for remaining > 0 {
		writeSize := uint64(len(buffer))
		if remaining < writeSize {
			writeSize = remaining
		}
		written, err := filler.Write(buffer[:int(writeSize)])
		if err != nil {
			_ = filler.Close()
			t.Fatalf("fill bounded tmpfs while preserving reserve: %v", err)
		}
		if written == 0 {
			_ = filler.Close()
			t.Fatal("tmpfs filler write returned no progress")
		}
		remaining -= uint64(written)
	}
	if err := filler.Sync(); err != nil {
		_ = filler.Close()
		t.Fatalf("sync tmpfs filler: %v", err)
	}
	if err := filler.Close(); err != nil {
		t.Fatalf("close tmpfs filler: %v", err)
	}
	return fillerPath
}

func countPublishedLTX(t *testing.T, repositoryRoot string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(repositoryRoot, "ltx", "0"))
	if err != nil {
		t.Fatalf("list published LTX files: %v", err)
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".ltx") {
			count++
		}
	}
	return count
}
