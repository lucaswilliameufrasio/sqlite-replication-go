package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/repositorylock"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/sqliteadapter"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/ltx"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
)

// RestoreToTarget reconstructs a verified repository point into a new file.
// It is internal and remains unwired to the public API while CP-01 is pending.
func RestoreToTarget(ctx context.Context, root, destinationPath string, target Target) error {
	lock, err := repositorylock.AcquireExclusive(root)
	if err != nil {
		return fmt.Errorf("lock repository for restore: %w", err)
	}
	defer lock.Close()
	plan, err := BuildRestorePlanFromFilesystem(root, target)
	if err != nil {
		return err
	}
	return restorePlan(ctx, root, destinationPath, plan)
}

func restorePlan(ctx context.Context, root, destinationPath string, plan Plan) (returnErr error) {
	if len(plan.Segments) == 0 || !plan.Segments[0].Snapshot {
		return fmt.Errorf("invalid restore plan: missing full snapshot base")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	destinationPath, err := filepath.Abs(destinationPath)
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
	defer func() {
		if cleanupErr := os.Remove(restoredPath); cleanupErr != nil && !errors.Is(cleanupErr, os.ErrNotExist) && returnErr == nil {
			returnErr = fmt.Errorf("remove restore staging file: %w", cleanupErr)
		}
	}()

	basePath := filepath.Join(root, filepath.FromSlash(plan.Segments[0].Key))
	base, err := os.Open(basePath)
	if err != nil {
		_ = restored.Close()
		return fmt.Errorf("open restore base: %w", err)
	}
	header, decodeErr := ltx.DecodeSnapshot(base, restored)
	closeErr := base.Close()
	if decodeErr != nil {
		_ = restored.Close()
		return fmt.Errorf("decode restore base: %w", decodeErr)
	}
	if closeErr != nil {
		_ = restored.Close()
		return fmt.Errorf("close restore base: %w", closeErr)
	}
	if header.MaxTXID != plan.Segments[0].MaxTXID || header.PageSize != plan.Segments[0].PageSize || header.Commit != plan.Segments[0].Commit {
		_ = restored.Close()
		return fmt.Errorf("restore base changed after planning")
	}
	info, err := restored.Stat()
	if err != nil {
		_ = restored.Close()
		return err
	}
	state, err := ltx.LoadDatabaseState(restored, info.Size(), header.MaxTXID)
	if err != nil {
		_ = restored.Close()
		return fmt.Errorf("load restore base state: %w", err)
	}

	for _, segment := range plan.Segments[1:] {
		if err := ctx.Err(); err != nil {
			_ = restored.Close()
			return err
		}
		deltaPath := filepath.Join(root, filepath.FromSlash(segment.Key))
		delta, err := os.Open(deltaPath)
		if err != nil {
			_ = restored.Close()
			return fmt.Errorf("open restore delta %q: %w", segment.Key, err)
		}
		deltaHeader, pages, postChecksum, decodeErr := ltx.DecodeDelta(delta)
		closeErr := delta.Close()
		if decodeErr != nil {
			_ = restored.Close()
			return fmt.Errorf("decode restore delta %q: %w", segment.Key, decodeErr)
		}
		if closeErr != nil {
			_ = restored.Close()
			return fmt.Errorf("close restore delta %q: %w", segment.Key, closeErr)
		}
		if deltaHeader.MinTXID != segment.MinTXID || deltaHeader.MaxTXID != segment.MaxTXID || postChecksum != segment.PostChecksum {
			_ = restored.Close()
			return fmt.Errorf("restore delta %q changed after planning", segment.Key)
		}
		updated, err := state.ApplyDelta(deltaHeader, pages, postChecksum)
		if err != nil {
			_ = restored.Close()
			return fmt.Errorf("validate restore delta %q: %w", segment.Key, err)
		}
		for _, page := range pages {
			if err := ctx.Err(); err != nil {
				_ = restored.Close()
				return err
			}
			written, err := restored.WriteAt(page.Data, int64(page.PageNumber-1)*int64(deltaHeader.PageSize))
			if err != nil {
				_ = restored.Close()
				return fmt.Errorf("write restored page %d: %w", page.PageNumber, err)
			}
			if written != len(page.Data) {
				_ = restored.Close()
				return fmt.Errorf("write restored page %d: %w", page.PageNumber, io.ErrShortWrite)
			}
		}
		if err := restored.Truncate(int64(deltaHeader.Commit) * int64(deltaHeader.PageSize)); err != nil {
			_ = restored.Close()
			return fmt.Errorf("truncate restored SQLite image: %w", err)
		}
		actual, err := ltx.LoadDatabaseState(restored, int64(deltaHeader.Commit)*int64(deltaHeader.PageSize), updated.TXID)
		if err != nil {
			_ = restored.Close()
			return fmt.Errorf("verify applied delta state: %w", err)
		}
		if actual.PageSize != updated.PageSize || actual.Commit != updated.Commit || actual.TXID != updated.TXID || actual.Checksum != updated.Checksum {
			_ = restored.Close()
			return fmt.Errorf("applied delta differs from validated LTX state")
		}
		state = updated
	}

	if state.TXID != plan.TargetTXID {
		_ = restored.Close()
		return fmt.Errorf("restore plan target TXID differs from reconstructed state")
	}
	if err := restored.Sync(); err != nil {
		_ = restored.Close()
		return fmt.Errorf("sync restored SQLite image: %w", err)
	}
	if err := restored.Close(); err != nil {
		return fmt.Errorf("close restored SQLite image: %w", err)
	}
	if err := sqliteadapter.VerifyDatabase(ctx, restoredPath); err != nil {
		return err
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := removeRestoreArtifact(restoredPath + suffix); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Link(restoredPath, destinationPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("restore destination already exists")
		}
		return fmt.Errorf("promote verified restore without overwrite: %w", err)
	}
	if err := storagefs.SyncDirectory(parent); err != nil {
		return err
	}
	if err := os.Remove(restoredPath); err != nil {
		return fmt.Errorf("remove restore staging link: %w", err)
	}
	return storagefs.SyncDirectory(parent)
}

func removeRestoreArtifact(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove restore staging sidecar: %w", err)
	}
	return nil
}

// CompactToSnapshot writes and verifies an immutable higher-level full
// snapshot for the selected point. Source segments are deliberately retained.
func CompactToSnapshot(root string, target Target) (string, error) {
	lock, err := repositorylock.AcquireExclusive(root)
	if err != nil {
		return "", fmt.Errorf("lock repository for compaction: %w", err)
	}
	defer lock.Close()
	plan, err := BuildRestorePlanFromFilesystem(root, target)
	if err != nil {
		return "", err
	}
	levels, err := os.ReadDir(filepath.Join(root, "ltx"))
	if err != nil {
		return "", fmt.Errorf("list LTX levels for compaction: %w", err)
	}
	var maximumLevel uint64
	for _, entry := range levels {
		if !entry.IsDir() {
			continue
		}
		level, err := strconv.ParseUint(entry.Name(), 10, 32)
		if err != nil {
			return "", fmt.Errorf("invalid LTX level directory %q", entry.Name())
		}
		if level > maximumLevel {
			maximumLevel = level
		}
	}
	if maximumLevel == uint64(^uint32(0)) {
		return "", fmt.Errorf("LTX level exhausted")
	}
	targetLevel := uint32(maximumLevel + 1)
	finalPath := filepath.Join(root, "ltx", strconv.FormatUint(uint64(targetLevel), 10), fmt.Sprintf("%016x-%016x.ltx", 1, plan.TargetTXID))
	parent := filepath.Dir(finalPath)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return "", fmt.Errorf("create compacted snapshot directory: %w", err)
	}
	restoreDirectory, err := os.MkdirTemp("", "sqlite-replication-compact-*")
	if err != nil {
		return "", fmt.Errorf("create compaction restore directory: %w", err)
	}
	defer os.RemoveAll(restoreDirectory)
	restoredPath := filepath.Join(restoreDirectory, "database.sqlite")
	if err := restorePlan(context.Background(), root, restoredPath, plan); err != nil {
		return "", fmt.Errorf("reconstruct compaction target: %w", err)
	}
	database, err := os.Open(restoredPath)
	if err != nil {
		return "", fmt.Errorf("open compacted SQLite image: %w", err)
	}
	defer database.Close()
	info, err := database.Stat()
	if err != nil {
		return "", err
	}
	staged, err := os.CreateTemp(parent, ".compact-*.ltx")
	if err != nil {
		return "", fmt.Errorf("create compacted LTX stage: %w", err)
	}
	stagedPath := staged.Name()
	defer os.Remove(stagedPath)
	if _, err := ltx.EncodeSnapshotAt(database, info.Size(), staged, plan.TargetTXID, plan.TimestampMillis); err != nil {
		_ = staged.Close()
		return "", fmt.Errorf("encode compacted snapshot: %w", err)
	}
	if err := staged.Sync(); err != nil {
		_ = staged.Close()
		return "", fmt.Errorf("sync compacted snapshot: %w", err)
	}
	if _, err := staged.Seek(0, io.SeekStart); err != nil {
		_ = staged.Close()
		return "", fmt.Errorf("rewind compacted snapshot for verification: %w", err)
	}
	decoded, err := os.CreateTemp("", "sqlite-replication-compact-verify-*.sqlite")
	if err != nil {
		_ = staged.Close()
		return "", fmt.Errorf("create compacted snapshot verification file: %w", err)
	}
	decodedPath := decoded.Name()
	defer os.Remove(decodedPath)
	compactedHeader, err := ltx.DecodeSnapshot(staged, decoded)
	if err != nil {
		_ = decoded.Close()
		_ = staged.Close()
		return "", fmt.Errorf("verify compacted snapshot encoding: %w", err)
	}
	compactedState, err := ltx.LoadDatabaseState(decoded, info.Size(), plan.TargetTXID)
	if err != nil {
		_ = decoded.Close()
		_ = staged.Close()
		return "", fmt.Errorf("verify compacted snapshot state: %w", err)
	}
	if compactedHeader.MaxTXID != plan.TargetTXID || compactedHeader.Timestamp != plan.TimestampMillis || compactedState.Commit != plan.Segments[len(plan.Segments)-1].Commit || compactedState.PageSize != plan.Segments[len(plan.Segments)-1].PageSize || compactedState.Checksum != plan.Segments[len(plan.Segments)-1].PostChecksum {
		_ = decoded.Close()
		_ = staged.Close()
		return "", fmt.Errorf("compacted snapshot failed verification")
	}
	if err := decoded.Close(); err != nil {
		_ = staged.Close()
		return "", fmt.Errorf("close compacted snapshot verification file: %w", err)
	}
	if err := staged.Close(); err != nil {
		return "", fmt.Errorf("close compacted snapshot: %w", err)
	}
	if err := storagefs.Publish(stagedPath, finalPath); err != nil {
		return "", fmt.Errorf("publish compacted snapshot: %w", err)
	}
	return finalPath, nil
}
