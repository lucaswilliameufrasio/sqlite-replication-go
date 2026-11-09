package spool

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/wal"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/walcapture"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/ltx"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
)

type deltaFile struct {
	minTXID uint64
	maxTXID uint64
	path    string
}

type databasePosition struct {
	pageSize uint32
	commit   uint32
	checksum uint64
}

// Recover rebuilds the verified LTX state from the immutable chain, reconciles
// a published LTX that may be ahead of its watermark, and validates a retained
// WAL boundary against the current generation when those bytes are available.
func Recover(root string, walBytes []byte) (walcapture.State, error) {
	return recoverWithImageLimit(root, walBytes, 0)
}

// RecoverWithImageLimit bounds the logical bytes written to the temporary
// snapshot recovery image. It does not bound other files or reserve disk space.
func RecoverWithImageLimit(root string, walBytes []byte, maxImageBytes uint64) (walcapture.State, error) {
	if maxImageBytes == 0 {
		return walcapture.State{}, fmt.Errorf("recovery image limit must be nonzero")
	}
	return recoverWithImageLimit(root, walBytes, maxImageBytes)
}

func recoverWithImageLimit(root string, walBytes []byte, maxImageBytes uint64) (walcapture.State, error) {
	if _, err := storagefs.ReadRepository(root); err != nil {
		return walcapture.State{}, fmt.Errorf("read capture repository: %w", err)
	}
	database, err := loadSnapshotState(root, maxImageBytes)
	if err != nil {
		return walcapture.State{}, err
	}
	knownPositions := map[uint64]databasePosition{
		database.TXID: {pageSize: database.PageSize, commit: database.Commit, checksum: database.Checksum},
	}
	deltas, err := listDeltaFiles(root)
	if err != nil {
		return walcapture.State{}, err
	}
	var lastHeader ltx.Header
	for _, delta := range deltas {
		file, err := os.Open(delta.path)
		if err != nil {
			return walcapture.State{}, fmt.Errorf("open LTX delta %s: %w", filepath.Base(delta.path), err)
		}
		header, pages, postChecksum, decodeErr := ltx.DecodeDelta(file)
		closeErr := file.Close()
		if decodeErr != nil {
			return walcapture.State{}, fmt.Errorf("decode LTX delta %s: %w", filepath.Base(delta.path), decodeErr)
		}
		if closeErr != nil {
			return walcapture.State{}, fmt.Errorf("close LTX delta %s: %w", filepath.Base(delta.path), closeErr)
		}
		if header.MinTXID != delta.minTXID || header.MaxTXID != delta.maxTXID {
			return walcapture.State{}, fmt.Errorf("LTX filename and header positions differ: %s", filepath.Base(delta.path))
		}
		if database.TXID == ^uint64(0) || header.MinTXID != database.TXID+1 {
			return walcapture.State{}, fmt.Errorf("LTX chain gap or overlap at %s", filepath.Base(delta.path))
		}
		database, err = database.ApplyDelta(header, pages, postChecksum)
		if err != nil {
			return walcapture.State{}, fmt.Errorf("apply LTX delta %s: %w", filepath.Base(delta.path), err)
		}
		knownPositions[database.TXID] = databasePosition{
			pageSize: database.PageSize,
			commit:   database.Commit,
			checksum: database.Checksum,
		}
		lastHeader = header
	}

	watermark, watermarkErr := ReadWatermark(root)
	watermarkExists := watermarkErr == nil
	if watermarkErr != nil && !errors.Is(watermarkErr, os.ErrNotExist) {
		return walcapture.State{}, watermarkErr
	}
	watermarkTXID := uint64(1)
	position := walcapture.Position{}
	if watermarkExists {
		watermarkTXID = watermark.TXID
		known, exists := knownPositions[watermark.TXID]
		if !exists || known.pageSize != watermark.PageSize || known.commit != watermark.Commit || known.checksum != watermark.DatabaseChecksum {
			return walcapture.State{}, fmt.Errorf("capture watermark does not match a verified LTX chain position")
		}
		if watermark.TXID == database.TXID {
			position = watermark.WAL
		}
	} else {
		known := knownPositions[1]
		if known.pageSize != database.PageSize || known.commit == 0 || known.checksum == 0 {
			return walcapture.State{}, fmt.Errorf("base snapshot state is invalid")
		}
	}

	if watermarkTXID < database.TXID {
		position, err = positionAfterPublishedDelta(lastHeader, walBytes)
		if err != nil {
			return walcapture.State{}, err
		}
	}
	position, err = validateCurrentWALPosition(position, walBytes, database.PageSize)
	if err != nil {
		return walcapture.State{}, err
	}
	state := walcapture.State{Database: database, WAL: position}
	if !watermarkExists || watermarkTXID < database.TXID || watermark.WAL != position {
		if err := PublishTransactions(root, nil, state, nil); err != nil {
			return walcapture.State{}, fmt.Errorf("persist recovered capture watermark: %w", err)
		}
	}
	return state, nil
}

func loadSnapshotState(root string, maxImageBytes uint64) (ltx.DatabaseState, error) {
	snapshotPath := storagefs.LTXPath(root, 1, 1)
	snapshot, err := os.Open(snapshotPath)
	if err != nil {
		return ltx.DatabaseState{}, fmt.Errorf("open base LTX snapshot: %w", err)
	}
	defer snapshot.Close()
	stage, err := os.CreateTemp(filepath.Join(root, "_meta"), ".recover-snapshot-*.sqlite")
	if err != nil {
		return ltx.DatabaseState{}, fmt.Errorf("create snapshot recovery image: %w", err)
	}
	stagePath := stage.Name()
	defer os.Remove(stagePath)
	var target io.Writer = stage
	if maxImageBytes != 0 {
		target = &limitedRecoveryWriter{writer: stage, remaining: maxImageBytes}
	}
	header, decodeErr := ltx.DecodeSnapshot(snapshot, target)
	if decodeErr != nil {
		_ = stage.Close()
		return ltx.DatabaseState{}, fmt.Errorf("decode base LTX snapshot: %w", decodeErr)
	}
	if err := stage.Sync(); err != nil {
		_ = stage.Close()
		return ltx.DatabaseState{}, fmt.Errorf("sync recovery image: %w", err)
	}
	info, err := stage.Stat()
	if err != nil {
		_ = stage.Close()
		return ltx.DatabaseState{}, fmt.Errorf("stat recovery image: %w", err)
	}
	state, stateErr := ltx.LoadDatabaseState(stage, info.Size(), 1)
	closeErr := stage.Close()
	if stateErr != nil {
		return ltx.DatabaseState{}, fmt.Errorf("load base SQLite state: %w", stateErr)
	}
	if closeErr != nil {
		return ltx.DatabaseState{}, fmt.Errorf("close recovery image: %w", closeErr)
	}
	if state.PageSize != header.PageSize || state.Commit != header.Commit {
		return ltx.DatabaseState{}, fmt.Errorf("base LTX header differs from restored SQLite image")
	}
	return state, nil
}

type limitedRecoveryWriter struct {
	writer    io.Writer
	remaining uint64
}

func (w *limitedRecoveryWriter) Write(data []byte) (int, error) {
	if uint64(len(data)) > w.remaining {
		return 0, fmt.Errorf("recovery image size limit exceeded")
	}
	n, err := w.writer.Write(data)
	w.remaining -= uint64(n)
	return n, err
}

func listDeltaFiles(root string) ([]deltaFile, error) {
	directory := filepath.Join(root, "ltx", "0")
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("list LTX chain: %w", err)
	}
	deltas := make([]deltaFile, 0)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".ltx" {
			continue
		}
		minimum, maximum, err := parseLTXFilename(entry.Name())
		if err != nil {
			return nil, err
		}
		if minimum == 1 && maximum == 1 {
			continue
		}
		if minimum == 1 || maximum < minimum {
			return nil, fmt.Errorf("invalid LTX chain filename %q", entry.Name())
		}
		deltas = append(deltas, deltaFile{
			minTXID: minimum,
			maxTXID: maximum,
			path:    filepath.Join(directory, entry.Name()),
		})
	}
	sort.Slice(deltas, func(i, j int) bool {
		if deltas[i].minTXID != deltas[j].minTXID {
			return deltas[i].minTXID < deltas[j].minTXID
		}
		return deltas[i].maxTXID < deltas[j].maxTXID
	})
	return deltas, nil
}

func parseLTXFilename(name string) (uint64, uint64, error) {
	if filepath.Ext(name) != ".ltx" {
		return 0, 0, fmt.Errorf("invalid LTX chain filename %q", name)
	}
	parts := strings.Split(strings.TrimSuffix(name, ".ltx"), "-")
	if len(parts) != 2 || len(parts[0]) != 16 || len(parts[1]) != 16 {
		return 0, 0, fmt.Errorf("invalid LTX chain filename %q", name)
	}
	minimum, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid LTX chain filename %q: %w", name, err)
	}
	maximum, err := strconv.ParseUint(parts[1], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid LTX chain filename %q: %w", name, err)
	}
	return minimum, maximum, nil
}

func positionAfterPublishedDelta(header ltx.Header, walBytes []byte) (walcapture.Position, error) {
	if header.WALSize == 0 {
		return walcapture.Position{}, nil
	}
	if header.WALSize < 32 || header.WALSize > int64(len(walBytes)) {
		return walcapture.Position{}, nil
	}
	scanned, err := wal.Scan(walBytes)
	if err != nil {
		return walcapture.Position{}, fmt.Errorf("scan WAL during spool recovery: %w", err)
	}
	if scanned.Header.Salt1 != header.WALSalt1 || scanned.Header.Salt2 != header.WALSalt2 {
		return walcapture.Position{}, nil
	}
	position, err := walcapture.PositionFromWALPrefix(walBytes, header.WALSize)
	if err != nil {
		return walcapture.Position{}, fmt.Errorf("recover published WAL position: %w", err)
	}
	return position, nil
}

func validateCurrentWALPosition(position walcapture.Position, walBytes []byte, pageSize uint32) (walcapture.Position, error) {
	if len(walBytes) == 0 {
		return walcapture.Position{}, nil
	}
	scanned, err := wal.Scan(walBytes)
	if err != nil {
		return walcapture.Position{}, fmt.Errorf("scan current WAL during recovery: %w", err)
	}
	if !position.Initialized || position.Salt1 != scanned.Header.Salt1 || position.Salt2 != scanned.Header.Salt2 || position.CheckpointSequence != scanned.Header.CheckpointSequence {
		return walcapture.Position{}, nil
	}
	frameSize := int64(24 + pageSize)
	prefixSize := int64(32) + int64(position.FrameCount)*frameSize
	if prefixSize > int64(len(walBytes)) {
		return walcapture.Position{}, fmt.Errorf("current WAL is shorter than the persisted capture boundary")
	}
	observed, err := walcapture.PositionFromWALPrefix(walBytes, prefixSize)
	if err != nil {
		return walcapture.Position{}, fmt.Errorf("validate persisted WAL boundary: %w", err)
	}
	if observed.FrameCount != position.FrameCount || observed.Checksum1 != position.Checksum1 || observed.Checksum2 != position.Checksum2 {
		return walcapture.Position{}, fmt.Errorf("current WAL prefix differs from persisted capture boundary")
	}
	return position, nil
}

var _ io.ReaderAt = (*os.File)(nil)
