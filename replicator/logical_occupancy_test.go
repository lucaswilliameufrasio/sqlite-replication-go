package replicator_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/repositorylock"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/replicator"
)

func TestMeasureRepositoryLogicalBytesFailsClosed(t *testing.T) {
	root := t.TempDir()
	if _, err := replicator.MeasureRepositoryLogicalBytes(root); err == nil {
		t.Fatal("missing ltx directory accepted")
	}
	if err := os.Mkdir(filepath.Join(root, "ltx"), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "ltx", "data"), []byte("1234"), 0600); err != nil {
		t.Fatal(err)
	}
	if total, err := replicator.MeasureRepositoryLogicalBytes(root); err != nil || total != 4 {
		t.Fatalf("missing optional staging: %d %v", total, err)
	}
	if err := os.Symlink("missing", filepath.Join(root, "ltx", "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := replicator.MeasureRepositoryLogicalBytes(root); err == nil {
		t.Fatal("symlink accepted")
	}
}

func TestMeasureRepositoryLogicalBytesRejectsHeldLock(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "ltx"), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "ltx", "data"), []byte("1234"), 0600); err != nil {
		t.Fatal(err)
	}
	lock, err := repositorylock.AcquireExclusive(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replicator.MeasureRepositoryLogicalBytes(root); err == nil {
		t.Fatal("measurement ignored held repository lock")
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	if total, err := replicator.MeasureRepositoryLogicalBytes(root); err != nil || total != 4 {
		t.Fatalf("measurement after unlock: %d %v", total, err)
	}
}
