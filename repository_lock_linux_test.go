//go:build linux

package replication_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/repositorylock"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
	"golang.org/x/sys/unix"
)

func TestSnapshotAndRestoreRejectHeldRepositoryLock(t *testing.T) {
	root := t.TempDir()
	if err := storagefs.EnsureRepository(root); err != nil {
		t.Fatal(err)
	}
	lock, err := repositorylock.AcquireExclusive(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	if _, err := replication.Snapshot(context.Background(), filepath.Join(root, "source.sqlite"), root); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("snapshot with held lock = %v, want contention", err)
	}
	if err := replication.RestoreSnapshot(context.Background(), root, filepath.Join(t.TempDir(), "restored.sqlite")); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("snapshot restore with held lock = %v, want contention", err)
	}
}

func TestSnapshotInitializesMetadataBeforeLockContention(t *testing.T) {
	root := t.TempDir()
	lock, err := repositorylock.AcquireExclusive(root)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	_, err = replication.Snapshot(context.Background(), filepath.Join(root, "missing.sqlite"), root)
	if !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("snapshot with held lock = %v", err)
	}
	if _, err := storagefs.ReadRepository(root); err != nil {
		t.Fatalf("metadata should exist before contention: %v", err)
	}
	if _, err := os.Stat(storagefs.LTXPath(root, 1, 1)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("snapshot object unexpectedly present: %v", err)
	}
}
