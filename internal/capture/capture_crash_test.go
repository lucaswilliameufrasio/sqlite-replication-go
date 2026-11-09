package capture

import (
	"context"
	"database/sql"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/lucaswilliameufrasio/sqlite-replication-go"
)

func TestCaptureRecoversAfterSIGKILLAtDurabilityBoundaries(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("os.Process.Kill is not SIGKILL on Windows")
	}
	for _, point := range []string{
		"before-stage-sync",
		"after-stage-sync",
		"before-ltx-publish",
		"after-ltx-publish",
		"before-watermark-write",
		"before-watermark-sync",
		"after-watermark-sync",
		"after-watermark",
	} {
		t.Run(point, func(t *testing.T) {
			ctx := context.Background()
			directory := t.TempDir()
			databasePath := filepath.Join(directory, "source.sqlite")
			repositoryRoot := filepath.Join(directory, "repository")
			database, err := sql.Open("sqlite", databasePath)
			if err != nil {
				t.Fatalf("open source database: %v", err)
			}
			database.SetMaxOpenConns(1)
			for _, statement := range []string{
				"PRAGMA page_size=512",
				"PRAGMA journal_mode=WAL",
				"PRAGMA wal_autocheckpoint=0",
				"CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT)",
				"INSERT INTO records VALUES (1, 'base')",
			} {
				if _, err := database.ExecContext(ctx, statement); err != nil {
					t.Fatalf("initialize database with %q: %v", statement, err)
				}
			}
			if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
				t.Fatalf("create base snapshot: %v", err)
			}
			if _, err := database.ExecContext(ctx, "UPDATE records SET value='survives-kill' WHERE id=1"); err != nil {
				t.Fatalf("commit WAL change: %v", err)
			}
			if err := database.Close(); err != nil {
				t.Fatalf("close source before child capture: %v", err)
			}

			markerPath := filepath.Join(directory, "reached-boundary")
			command := exec.Command(os.Args[0], "-test.run=^TestCaptureCrashChild$")
			command.Env = append(os.Environ(),
				"SQLITE_REPLICATION_CAPTURE_CRASH_CHILD=1",
				"SQLITE_REPLICATION_CAPTURE_CRASH_POINT="+point,
				"SQLITE_REPLICATION_CAPTURE_SOURCE="+databasePath,
				"SQLITE_REPLICATION_CAPTURE_REPOSITORY="+repositoryRoot,
				"SQLITE_REPLICATION_CAPTURE_MARKER="+markerPath,
			)
			command.Stdout = io.Discard
			command.Stderr = io.Discard
			if err := command.Start(); err != nil {
				t.Fatalf("start child capture: %v", err)
			}
			deadline := time.Now().Add(15 * time.Second)
			for {
				if _, err := os.Stat(markerPath); err == nil {
					break
				}
				if time.Now().After(deadline) {
					_ = command.Process.Kill()
					_ = command.Wait()
					t.Fatalf("child did not reach durability boundary %q", point)
				}
				time.Sleep(5 * time.Millisecond)
			}
			if err := command.Process.Kill(); err != nil {
				t.Fatalf("send SIGKILL to child at %q: %v", point, err)
			}
			if err := command.Wait(); err == nil {
				t.Fatal("child unexpectedly exited cleanly after SIGKILL")
			}

			recovered, err := CaptureOnce(ctx, databasePath, repositoryRoot, 200, nil)
			if err != nil {
				t.Fatalf("recover after SIGKILL at %q: %v", point, err)
			}
			if recovered.State.Database.TXID != 2 {
				t.Fatalf("recovered TXID = %d after %q, want 2", recovered.State.Database.TXID, point)
			}
			ltxAlreadyPublished := point == "after-ltx-publish" || point == "before-watermark-write" || point == "before-watermark-sync" ||
				point == "after-watermark-sync" || point == "after-watermark"
			if !ltxAlreadyPublished && len(recovered.Transactions) != 1 {
				t.Fatalf("staged but unpublished transaction was not replayed after kill: %d", len(recovered.Transactions))
			}
			if ltxAlreadyPublished && len(recovered.Transactions) != 0 {
				t.Fatalf("already-published transaction was duplicated after kill at %q", point)
			}
		})
	}
}

func TestCaptureCrashChild(t *testing.T) {
	if os.Getenv("SQLITE_REPLICATION_CAPTURE_CRASH_CHILD") != "1" {
		return
	}
	sourcePath := os.Getenv("SQLITE_REPLICATION_CAPTURE_SOURCE")
	repositoryRoot := os.Getenv("SQLITE_REPLICATION_CAPTURE_REPOSITORY")
	markerPath := os.Getenv("SQLITE_REPLICATION_CAPTURE_MARKER")
	wantPoint := os.Getenv("SQLITE_REPLICATION_CAPTURE_CRASH_POINT")
	_, err := CaptureOnce(context.Background(), sourcePath, repositoryRoot, 199, func(point FaultPoint, _ int) error {
		if string(point) == wantPoint {
			marker, err := os.Create(markerPath)
			if err != nil {
				return err
			}
			if err := marker.Close(); err != nil {
				return err
			}
			select {}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("child capture failed before crash boundary: %v", err)
	}
	t.Fatal("child capture completed without reaching requested crash boundary")
}
