//go:build linux

package recovery

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/capture"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

func TestCaptureWhileGCIsApplyingAcrossProcesses(t *testing.T) {
	if root := os.Getenv("SQLITE_REPLICATION_GC_HOLDER_ROOT"); root != "" {
		plan, err := BuildGCPlan(root, []Target{{TXID: 1}})
		if err != nil {
			t.Fatal(err)
		}
		err = applyGCPlanLocked(root, plan, func() error {
			if _, err := fmt.Fprintln(os.Stdout, "GC_VALIDATED"); err != nil {
				return err
			}
			_, err := bufio.NewReader(os.Stdin).ReadString('\n')
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	root := t.TempDir()
	source := filepath.Join(root, "source.sqlite")
	repo := filepath.Join(root, "repository")
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, statement := range []string{"PRAGMA journal_mode=WAL", "PRAGMA wal_autocheckpoint=0", "CREATE TABLE items (value TEXT)", "INSERT INTO items VALUES ('base')"} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := replication.Snapshot(context.Background(), source, repo); err != nil {
		t.Fatal(err)
	}
	if _, err := CompactToSnapshot(repo, Target{TXID: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := CompactToSnapshot(repo, Target{TXID: 1}); err != nil {
		t.Fatal(err)
	}
	plan, err := BuildGCPlan(repo, []Target{{TXID: 1}})
	if err != nil || len(plan.Candidates) == 0 {
		t.Fatalf("GC plan: %+v, %v", plan, err)
	}
	for _, object := range plan.Candidates {
		if object.Key == "ltx/0/0000000000000001-0000000000000001.ltx" {
			t.Fatal("GC must retain the base needed by capture")
		}
	}
	candidate := filepath.Join(repo, filepath.FromSlash(plan.Candidates[0].Key))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCaptureWhileGCIsApplyingAcrossProcesses$")
	child.Env = append(os.Environ(), "SQLITE_REPLICATION_GC_HOLDER_ROOT="+repo)
	stdin, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stdin.Close(); _ = child.Wait() })
	reader := bufio.NewReader(stdout)
	line, err := reader.ReadString('\n')
	if err != nil || line != "GC_VALIDATED\n" {
		t.Fatalf("GC readiness %q: %v", line, err)
	}
	if _, err := capture.CaptureOnce(context.Background(), source, repo, 100, nil); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("capture during GC: %v", err)
	}
	if _, err := os.Stat(candidate); err != nil {
		t.Fatalf("candidate before GC release: %v", err)
	}
	if _, err := fmt.Fprintln(stdin, "release"); err != nil {
		t.Fatal(err)
	}
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err != nil {
		t.Fatalf("GC process: %v", err)
	}
	if _, err := os.Stat(candidate); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate after GC: %v", err)
	}
	if _, err := BuildRestorePlanFromFilesystem(repo, Target{TXID: 1}); err != nil {
		t.Fatalf("retained point: %v", err)
	}
	if _, err := capture.CaptureOnce(context.Background(), source, repo, 101, nil); err != nil {
		t.Fatalf("capture after GC: %v", err)
	}
}
