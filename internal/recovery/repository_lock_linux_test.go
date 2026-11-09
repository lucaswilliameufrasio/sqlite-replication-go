//go:build linux

package recovery

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/repositorylock"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storages3"
	"golang.org/x/sys/unix"
)

func TestGCRejectsHeldRepositoryLock(t *testing.T) {
	root := t.TempDir()
	lock, err := repositorylock.AcquireExclusive(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	err = ApplyGCPlan(root, GCPlan{RequiredTargets: []Target{{TXID: 1}}})
	if !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("GC with held lock = %v, want contention", err)
	}
}

func TestS3SyncRejectsHeldLocalRepositoryLock(t *testing.T) {
	root := t.TempDir()
	lock, err := repositorylock.AcquireExclusive(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	// No remote request can be reached before the local repository is read.
	_, err = SyncFilesystemToS3(context.Background(), root, &storages3.Store{}, "test", "test")
	if !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("sync with held local lock = %v, want contention", err)
	}
}

func TestS3SyncReleasesLocalLockOnRecoveryFailure(t *testing.T) {
	root := t.TempDir()
	_, err := SyncFilesystemToS3(context.Background(), root, &storages3.Store{}, "test", "test")
	if err == nil {
		t.Fatal("sync unexpectedly succeeded with no repository metadata")
	}
	lock, err := repositorylock.AcquireExclusive(root)
	if err != nil {
		t.Fatalf("lock retained after failed sync: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreAndCompactionRejectHeldRepositoryLock(t *testing.T) {
	root := t.TempDir()
	lock, err := repositorylock.AcquireExclusive(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	if err := RestoreToTarget(context.Background(), root, filepath.Join(t.TempDir(), "restore.sqlite"), Target{TXID: 1}); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("restore with held lock = %v, want contention", err)
	}
	if _, err := CompactToSnapshot(root, Target{TXID: 1}); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("compaction with held lock = %v, want contention", err)
	}
}
