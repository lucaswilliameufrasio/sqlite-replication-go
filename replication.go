// Package replication exposes initial snapshot and offline restore operations.
package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/repositorylock"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/sqliteadapter"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/ltx"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
)

// Snapshot creates an LTX v2 snapshot of a WAL-mode SQLite database and
// publishes it immutably under the filesystem repository root.
func Snapshot(ctx context.Context, databasePath, repositoryRoot string) (string, error) {
	if err := storagefs.EnsureRepository(repositoryRoot); err != nil {
		return "", err
	}
	lock, err := repositorylock.AcquireExclusive(repositoryRoot)
	if err != nil {
		return "", fmt.Errorf("lock repository for snapshot: %w", err)
	}
	defer lock.Close()
	stagingDir := filepath.Join(repositoryRoot, ".staging")
	if err := os.MkdirAll(stagingDir, 0o750); err != nil {
		return "", fmt.Errorf("create snapshot staging directory: %w", err)
	}
	databaseStage, err := os.CreateTemp(stagingDir, ".snapshot-*.sqlite")
	if err != nil {
		return "", fmt.Errorf("create temporary SQLite snapshot: %w", err)
	}
	databaseStagePath := databaseStage.Name()
	if err := databaseStage.Close(); err != nil {
		_ = os.Remove(databaseStagePath)
		return "", fmt.Errorf("close temporary SQLite snapshot: %w", err)
	}
	defer os.Remove(databaseStagePath)

	if err := sqliteadapter.CreateOnlineBackup(ctx, databasePath, databaseStagePath); err != nil {
		return "", err
	}
	if err := sqliteadapter.SyncFile(databaseStagePath); err != nil {
		return "", fmt.Errorf("sync SQLite snapshot: %w", err)
	}
	databaseFile, err := os.Open(databaseStagePath)
	if err != nil {
		return "", fmt.Errorf("open SQLite snapshot image: %w", err)
	}
	defer databaseFile.Close()
	databaseInfo, err := databaseFile.Stat()
	if err != nil {
		return "", fmt.Errorf("stat SQLite snapshot image: %w", err)
	}

	finalPath := storagefs.LTXPath(repositoryRoot, 1, 1)
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o750); err != nil {
		return "", fmt.Errorf("create LTX level directory: %w", err)
	}
	ltxStage, err := os.CreateTemp(filepath.Dir(finalPath), ".snapshot-*.ltx")
	if err != nil {
		return "", fmt.Errorf("create temporary LTX file: %w", err)
	}
	ltxStagePath := ltxStage.Name()
	defer os.Remove(ltxStagePath)
	if _, err := ltx.EncodeSnapshot(databaseFile, databaseInfo.Size(), ltxStage, time.Now().UnixMilli()); err != nil {
		_ = ltxStage.Close()
		return "", fmt.Errorf("encode LTX v2 snapshot: %w", err)
	}
	if err := ltxStage.Sync(); err != nil {
		_ = ltxStage.Close()
		return "", fmt.Errorf("sync staged LTX snapshot: %w", err)
	}
	if err := ltxStage.Close(); err != nil {
		return "", fmt.Errorf("close staged LTX snapshot: %w", err)
	}
	if err := storagefs.Publish(ltxStagePath, finalPath); err != nil {
		return "", err
	}
	return finalPath, nil
}

// RestoreSnapshot restores the repository's initial snapshot to a new path.
// Existing destination files are never replaced.
func RestoreSnapshot(ctx context.Context, repositoryRoot, destinationPath string) error {
	lock, err := repositorylock.AcquireExclusive(repositoryRoot)
	if err != nil {
		return fmt.Errorf("lock repository for snapshot restore: %w", err)
	}
	defer lock.Close()
	if _, err := storagefs.ReadRepository(repositoryRoot); err != nil {
		return err
	}
	ltxPath := storagefs.LTXPath(repositoryRoot, 1, 1)
	ltxFile, err := os.Open(ltxPath)
	if err != nil {
		return fmt.Errorf("open LTX v2 snapshot: %w", err)
	}
	defer ltxFile.Close()
	destinationPath, err = filepath.Abs(destinationPath)
	if err != nil {
		return fmt.Errorf("resolve restore destination: %w", err)
	}
	parent := filepath.Dir(destinationPath)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return fmt.Errorf("create restore destination directory: %w", err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Lstat(destinationPath + suffix); err == nil {
			return fmt.Errorf("restore destination has a stale SQLite %s sidecar", suffix)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect restore destination sidecar: %w", err)
		}
	}
	restored, err := os.CreateTemp(parent, ".restore-*.sqlite")
	if err != nil {
		return fmt.Errorf("create temporary restore file: %w", err)
	}
	restoredPath := restored.Name()
	defer os.Remove(restoredPath)
	header, decodeErr := ltx.DecodeSnapshot(ltxFile, restored)
	if decodeErr != nil {
		_ = restored.Close()
		return fmt.Errorf("decode LTX v2 snapshot: %w", decodeErr)
	}
	if err := restored.Sync(); err != nil {
		_ = restored.Close()
		return fmt.Errorf("sync restored SQLite image: %w", err)
	}
	if err := restored.Close(); err != nil {
		return fmt.Errorf("close restored SQLite image: %w", err)
	}
	info, err := os.Stat(restoredPath)
	if err != nil {
		return fmt.Errorf("stat restored SQLite image: %w", err)
	}
	if info.Size() != int64(header.PageSize)*int64(header.Commit) {
		return fmt.Errorf("restored SQLite image has an invalid size")
	}
	if err := sqliteadapter.VerifyDatabase(ctx, restoredPath); err != nil {
		return err
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := removeIfPresent(restoredPath + suffix); err != nil {
			return err
		}
	}
	if err := os.Link(restoredPath, destinationPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("restore destination already exists: %s", destinationPath)
		}
		return fmt.Errorf("promote verified restore without overwrite: %w", err)
	}
	if err := storagefs.SyncDirectory(parent); err != nil {
		return err
	}
	if err := os.Remove(restoredPath); err != nil {
		return fmt.Errorf("remove restore staging link: %w", err)
	}
	if err := storagefs.SyncDirectory(parent); err != nil {
		return err
	}
	return nil
}

func removeIfPresent(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove temporary SQLite sidecar: %w", err)
	}
	return nil
}

var _ io.ReaderAt = (*os.File)(nil)
