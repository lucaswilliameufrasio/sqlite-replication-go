//go:build linux

package capture

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/repositorylock"
	"golang.org/x/sys/unix"
)

func TestCaptureRejectsHeldRepositoryLock(t *testing.T) {
	root := t.TempDir()
	lock, err := repositorylock.AcquireExclusive(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	_, err = CaptureOnce(context.Background(), filepath.Join(root, "source.sqlite"), root, 1, nil)
	if !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("capture with held lock = %v, want contention", err)
	}
}
