//go:build linux

package capture

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
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/recovery"
	"golang.org/x/sys/unix"
)

func TestGCFromRustCapture(t *testing.T) {
	root := os.Getenv("SQLITE_REPLICATION_RUST_CAPTURE_GC_ROOT")
	if root == "" {
		t.Skip("helper for Rust capture and Go GC interoperability")
	}
	plan, err := recovery.BuildGCPlan(root, []recovery.Target{{TXID: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Candidates) == 0 {
		t.Fatal("expected GC candidate")
	}
	candidate := filepath.Join(root, filepath.FromSlash(plan.Candidates[0].Key))
	err = recovery.ApplyGCPlan(root, plan)
	if os.Getenv("SQLITE_REPLICATION_RUST_CAPTURE_GC_CONTENDED") == "1" {
		if !errors.Is(err, unix.EWOULDBLOCK) {
			t.Fatalf("GC during Rust capture = %v", err)
		}
		if _, err := os.Stat(candidate); err != nil {
			t.Fatalf("candidate under contention: %v", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("GC after capture: %v", err)
	}
	if _, err := os.Stat(candidate); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate after GC: %v", err)
	}
	if _, err := recovery.BuildRestorePlanFromFilesystem(root, recovery.Target{TXID: 1}); err != nil {
		t.Fatalf("retained point: %v", err)
	}
}

func TestCaptureGCSeparateProcesses(t *testing.T) {
	if root := os.Getenv("SQLITE_REPLICATION_CAPTURE_GC_CHILD"); root != "" {
		injected := errors.New("injected capture failure")
		_, err := CaptureOnce(context.Background(), filepath.Join(root, "source.sqlite"), filepath.Join(root, "repository"), 100, func(point FaultPoint, _ int) error {
			if point == AfterLeaseAcquire {
				fmt.Fprintln(os.Stdout, "CAPTURE_LOCKED")
				if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
					return err
				}
				if os.Getenv("SQLITE_REPLICATION_CAPTURE_GC_FAIL") == "1" {
					return injected
				}
			}
			return nil
		})
		if os.Getenv("SQLITE_REPLICATION_CAPTURE_GC_FAIL") == "1" {
			if !errors.Is(err, injected) {
				t.Fatalf("capture failure = %v", err)
			}
		} else if err != nil {
			t.Fatalf("capture: %v", err)
		}
		return
	}
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("failure=%t", fail), func(t *testing.T) {
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
			if _, err := recovery.CompactToSnapshot(repo, recovery.Target{TXID: 1}); err != nil {
				t.Fatal(err)
			}
			if _, err := recovery.CompactToSnapshot(repo, recovery.Target{TXID: 1}); err != nil {
				t.Fatal(err)
			}
			plan, err := recovery.BuildGCPlan(repo, []recovery.Target{{TXID: 1}})
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Candidates) == 0 {
				t.Fatal("expected a GC candidate")
			}
			candidate := filepath.Join(repo, filepath.FromSlash(plan.Candidates[0].Key))
			if _, err := db.Exec("INSERT INTO items VALUES ('captured')"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCaptureGCSeparateProcesses$")
			child.Env = append(os.Environ(), "SQLITE_REPLICATION_CAPTURE_GC_CHILD="+root)
			if fail {
				child.Env = append(child.Env, "SQLITE_REPLICATION_CAPTURE_GC_FAIL=1")
			}
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
			if err != nil || line != "CAPTURE_LOCKED\n" {
				t.Fatalf("child readiness %q: %v", line, err)
			}
			if err := recovery.ApplyGCPlan(repo, plan); !errors.Is(err, unix.EWOULDBLOCK) {
				t.Fatalf("GC during capture = %v", err)
			}
			if _, err := os.Stat(candidate); err != nil {
				t.Fatalf("GC removed candidate under contention: %v", err)
			}
			if _, err := fmt.Fprintln(stdin, "release"); err != nil {
				t.Fatal(err)
			}
			if err := stdin.Close(); err != nil {
				t.Fatal(err)
			}
			if err := child.Wait(); err != nil {
				t.Fatalf("capture child: %v", err)
			}
			newPlan, err := recovery.BuildGCPlan(repo, []recovery.Target{{TXID: 1}})
			if err != nil {
				t.Fatal(err)
			}
			if err := recovery.ApplyGCPlan(repo, newPlan); err != nil {
				t.Fatalf("GC after capture: %v", err)
			}
			if _, err := os.Stat(candidate); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("candidate after GC: %v", err)
			}
			if _, err := recovery.BuildRestorePlanFromFilesystem(repo, recovery.Target{TXID: 1}); err != nil {
				t.Fatalf("retained point: %v", err)
			}
		})
	}
}
