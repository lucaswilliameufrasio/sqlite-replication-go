package replicator

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/repositorylock"
)

// MeasureRepositoryLogicalBytes measures regular files under ltx and .staging
// while holding the cooperative local repository lock. A missing ltx directory,
// unsupported entry, overflow, or read error fails closed. A missing .staging
// directory contributes zero. It does not count journals, metadata, files outside
// these directories, or physical blocks, and cannot exclude uncooperative
// writers. Call within Budget.Reconcile while all other producers are excluded.
func MeasureRepositoryLogicalBytes(root string) (total uint64, returnErr error) {
	lock, err := repositorylock.AcquireExclusive(root)
	if err != nil {
		return 0, fmt.Errorf("lock repository for measurement: %w", err)
	}
	defer func() {
		if err := lock.Close(); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("release repository measurement lock: %w", err))
		}
	}()
	return measureRepositoryLogicalBytesLocked(root)
}

// Caller must retain the repository's exclusive coordination lock.
func measureRepositoryLogicalBytesLocked(root string) (total uint64, returnErr error) {
	for index, dir := range []string{filepath.Join(root, "ltx"), filepath.Join(root, ".staging")} {
		err := filepath.Walk(dir, func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				if index == 1 && path == dir && errors.Is(walkErr, os.ErrNotExist) {
					return nil
				}
				return walkErr
			}
			if info.IsDir() {
				return nil
			}
			if !info.Mode().IsRegular() || info.Size() < 0 {
				return fmt.Errorf("unsupported repository entry: %s", path)
			}
			size := uint64(info.Size())
			if size > ^uint64(0)-total {
				return errors.New("logical occupancy overflow")
			}
			total += size
			return nil
		})
		if err != nil {
			return 0, fmt.Errorf("measure repository: %w", err)
		}
	}
	return total, nil
}
