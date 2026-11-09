//go:build linux

package repositorylock

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"golang.org/x/sys/unix"
)

func TestExclusiveLockAcrossProcesses(t *testing.T) {
	if root := os.Getenv("SQLITE_REPLICATION_LOCK_TEST_ROOT"); root != "" {
		file, err := AcquireExclusive(root)
		if !errors.Is(err, unix.EWOULDBLOCK) {
			if file != nil {
				_ = file.Close()
			}
			t.Fatalf("expected lock contention, got %v", err)
		}
		return
	}
	root := t.TempDir()
	file, err := AcquireExclusive(root)
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestExclusiveLockAcrossProcesses$")
	child.Env = append(os.Environ(), "SQLITE_REPLICATION_LOCK_TEST_ROOT="+root)
	if output, err := child.CombinedOutput(); err != nil {
		_ = file.Close()
		t.Fatalf("child failed: %v: %s", err, output)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reacquired, err := AcquireExclusive(root)
	if err != nil {
		t.Fatalf("reacquire after close: %v", err)
	}
	if err := reacquired.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHoldRepositoryLockForInterop(t *testing.T) {
	root := os.Getenv("SQLITE_REPLICATION_INTEROP_HOLD_ROOT")
	if root == "" {
		t.Skip("helper for cross-language lock test")
	}
	file, err := AcquireExclusive(root)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := fmt.Fprintln(os.Stdout, "LOCKED"); err != nil {
		t.Fatal(err)
	}
	if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
		t.Fatalf("wait for release: %v", err)
	}
}
