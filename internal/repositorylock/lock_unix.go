//go:build linux || darwin

// Package repositorylock provides an experimental cooperative local lock.
// It does not coordinate remote storage or uncooperative processes.
package repositorylock

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// AcquireExclusive obtains a nonblocking exclusive lock in an existing repository.
// The lock file must never be removed while any participant might hold it.
func AcquireExclusive(root string) (*os.File, error) {
	path := filepath.Join(root, ".repository-coordination.lock")
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open repository coordination lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("acquire repository coordination lock: %w", err)
	}
	return file, nil
}
