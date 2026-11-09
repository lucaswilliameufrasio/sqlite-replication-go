//go:build linux

package replicator_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	replication "github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/repositorylock"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/replicator"
	"golang.org/x/sys/unix"
)

func TestCoordinatorRecoveryRejectsHeldRepositoryLock(t *testing.T) {
	root := t.TempDir()
	database := filepath.Join(root, "source.sqlite")
	repo := filepath.Join(root, "repository")
	createSourceDatabase(t, database)
	if _, err := replication.Snapshot(context.Background(), database, repo); err != nil {
		t.Fatal(err)
	}
	cfg := coordinatorConfig(repo, database, 1)
	captureCalled := false
	cfg.Capture = func(context.Context) (replicator.CaptureResult, error) {
		captureCalled = true
		return replicator.CaptureResult{TXID: 1}, nil
	}
	coordinator, err := replicator.NewCoordinator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := repositorylock.AcquireExclusive(repo)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if _, err := coordinator.Status(context.Background()); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("status with held lock = %v", err)
	}
	if err := coordinator.Flush(context.Background()); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("flush with held lock = %v", err)
	}
	if captureCalled {
		t.Fatal("flush invoked capture while repository was locked")
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Status(context.Background()); err != nil {
		t.Fatalf("status after release: %v", err)
	}
}
