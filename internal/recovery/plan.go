// Package recovery plans verified restores from immutable LTX segments.
// It does not read storage, mutate repositories, or expose WAL capture.
package recovery

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/ltx"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
)

const checksumFlag uint64 = 1 << 63

var (
	ErrInvalidInventory = errors.New("invalid recovery inventory")
	ErrNoRestorePoint   = errors.New("no verified restore point matches the target")
	ErrAmbiguousChain   = errors.New("recovery inventory contains an ambiguous chain")
)

type Segment struct {
	Key             string
	Level           uint32
	MinTXID         uint64
	MaxTXID         uint64
	TimestampMillis int64
	PageSize        uint32
	Commit          uint32
	PreChecksum     uint64
	PostChecksum    uint64
	Snapshot        bool
}

type Target struct {
	TXID            uint64
	TimestampMillis *int64
}

type Plan struct {
	Segments        []Segment
	TargetTXID      uint64
	TimestampMillis int64
}

type candidatePlan struct {
	plan      Plan
	baseLevel uint32
}

func BuildRestorePlan(inventory []Segment, target Target) (Plan, error) {
	if (target.TXID == 0) == (target.TimestampMillis == nil) {
		return Plan{}, fmt.Errorf("%w: specify exactly one of TXID or timestamp", ErrInvalidInventory)
	}
	if len(inventory) == 0 {
		return Plan{}, ErrNoRestorePoint
	}

	seenKeys := make(map[string]struct{}, len(inventory))
	snapshots := make([]Segment, 0)
	deltas := make([]Segment, 0)
	for _, segment := range inventory {
		if err := validateSegment(segment); err != nil {
			return Plan{}, err
		}
		if _, exists := seenKeys[segment.Key]; exists {
			return Plan{}, fmt.Errorf("%w: duplicate key %q", ErrInvalidInventory, segment.Key)
		}
		seenKeys[segment.Key] = struct{}{}
		if segment.Snapshot {
			snapshots = append(snapshots, segment)
		} else {
			deltas = append(deltas, segment)
		}
	}
	if len(snapshots) == 0 {
		return Plan{}, ErrNoRestorePoint
	}
	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].MaxTXID != snapshots[j].MaxTXID {
			return snapshots[i].MaxTXID > snapshots[j].MaxTXID
		}
		return snapshots[i].Level > snapshots[j].Level
	})
	sort.Slice(deltas, func(i, j int) bool {
		if deltas[i].MinTXID != deltas[j].MinTXID {
			return deltas[i].MinTXID < deltas[j].MinTXID
		}
		if deltas[i].MaxTXID != deltas[j].MaxTXID {
			return deltas[i].MaxTXID < deltas[j].MaxTXID
		}
		return deltas[i].Key < deltas[j].Key
	})

	var candidates []candidatePlan
	for _, snapshot := range snapshots {
		segments := []Segment{snapshot}
		currentTXID := snapshot.MaxTXID
		currentChecksum := snapshot.PostChecksum
		points := []Segment{snapshot}
		for currentTXID < ^uint64(0) {
			var next *Segment
			for index := range deltas {
				delta := &deltas[index]
				if delta.MinTXID != currentTXID+1 || delta.PreChecksum != currentChecksum {
					continue
				}
				if next != nil {
					return Plan{}, fmt.Errorf("%w after TXID %d", ErrAmbiguousChain, currentTXID)
				}
				next = delta
			}
			if next == nil {
				break
			}
			segments = append(segments, *next)
			currentTXID = next.MaxTXID
			currentChecksum = next.PostChecksum
			points = append(points, *next)
		}

		if target.TimestampMillis == nil {
			for pointIndex, point := range points {
				if point.MaxTXID == target.TXID {
					candidates = append(candidates, candidatePlan{
						plan:      Plan{Segments: append([]Segment(nil), segments[:pointIndex+1]...), TargetTXID: point.MaxTXID, TimestampMillis: point.TimestampMillis},
						baseLevel: snapshot.Level,
					})
				}
			}
			continue
		}

		var chosen *Segment
		chosenIndex := 0
		for index := range points {
			point := &points[index]
			if point.TimestampMillis <= *target.TimestampMillis && (chosen == nil || point.MaxTXID > chosen.MaxTXID) {
				chosen = point
				chosenIndex = index
			}
		}
		if chosen != nil {
			candidates = append(candidates, candidatePlan{
				plan:      Plan{Segments: append([]Segment(nil), segments[:chosenIndex+1]...), TargetTXID: chosen.MaxTXID, TimestampMillis: chosen.TimestampMillis},
				baseLevel: snapshot.Level,
			})
		}
	}
	if len(candidates) == 0 {
		return Plan{}, ErrNoRestorePoint
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].plan.TargetTXID != candidates[j].plan.TargetTXID {
			return candidates[i].plan.TargetTXID > candidates[j].plan.TargetTXID
		}
		return candidates[i].baseLevel > candidates[j].baseLevel
	})
	if len(candidates) > 1 && candidates[0].plan.TargetTXID == candidates[1].plan.TargetTXID && candidates[0].baseLevel == candidates[1].baseLevel {
		return Plan{}, ErrAmbiguousChain
	}
	return candidates[0].plan, nil
}

// BuildRestorePlanFromFilesystem reads and validates every LTX object in a
// filesystem repository before selecting a restore path. It never mutates the
// repository; the temporary SQLite image is created outside the repository.
func BuildRestorePlanFromFilesystem(root string, target Target) (Plan, error) {
	if _, err := storagefs.ReadRepository(root); err != nil {
		return Plan{}, fmt.Errorf("read repository metadata: %w", err)
	}
	levels, err := os.ReadDir(filepath.Join(root, "ltx"))
	if err != nil {
		return Plan{}, fmt.Errorf("list LTX levels: %w", err)
	}
	inventory := make([]Segment, 0)
	for _, levelEntry := range levels {
		if !levelEntry.IsDir() {
			continue
		}
		levelValue, err := strconv.ParseUint(levelEntry.Name(), 10, 32)
		if err != nil {
			return Plan{}, fmt.Errorf("invalid LTX level directory %q", levelEntry.Name())
		}
		level := uint32(levelValue)
		levelPath := filepath.Join(root, "ltx", levelEntry.Name())
		files, err := os.ReadDir(levelPath)
		if err != nil {
			return Plan{}, fmt.Errorf("list LTX level %d: %w", level, err)
		}
		for _, entry := range files {
			if filepath.Ext(entry.Name()) != ".ltx" {
				continue
			}
			if !entry.Type().IsRegular() {
				return Plan{}, fmt.Errorf("LTX object %q is not a regular file", entry.Name())
			}
			segment, err := inspectFile(filepath.Join(levelPath, entry.Name()), level, entry.Name())
			if err != nil {
				return Plan{}, fmt.Errorf("validate LTX object %q: %w", entry.Name(), err)
			}
			inventory = append(inventory, segment)
		}
	}
	return BuildRestorePlan(inventory, target)
}

func inspectFile(path string, level uint32, name string) (Segment, error) {
	minimum, maximum, err := parseFilename(name)
	if err != nil {
		return Segment{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return Segment{}, err
	}
	defer file.Close()
	var header ltx.Header
	var postChecksum uint64
	if minimum == 1 {
		stage, stageErr := os.CreateTemp("", "sqlite-replication-plan-*.sqlite")
		if stageErr != nil {
			return Segment{}, stageErr
		}
		stagePath := stage.Name()
		defer os.Remove(stagePath)
		header, err = ltx.DecodeSnapshot(file, stage)
		if err != nil {
			_ = stage.Close()
			return Segment{}, err
		}
		info, statErr := stage.Stat()
		if statErr != nil {
			_ = stage.Close()
			return Segment{}, statErr
		}
		state, stateErr := ltx.LoadDatabaseState(stage, info.Size(), header.MaxTXID)
		closeErr := stage.Close()
		if stateErr != nil {
			return Segment{}, stateErr
		}
		if closeErr != nil {
			return Segment{}, closeErr
		}
		if state.PageSize != header.PageSize || state.Commit != header.Commit {
			return Segment{}, fmt.Errorf("snapshot header differs from decoded image")
		}
		postChecksum = state.Checksum
	} else {
		header, _, postChecksum, err = ltx.DecodeDelta(file)
		if err != nil {
			return Segment{}, err
		}
	}
	if header.MinTXID != minimum || header.MaxTXID != maximum {
		return Segment{}, fmt.Errorf("filename TXID range differs from LTX header")
	}
	return Segment{
		Key:             filepath.ToSlash(filepath.Join("ltx", strconv.FormatUint(uint64(level), 10), name)),
		Level:           level,
		MinTXID:         minimum,
		MaxTXID:         maximum,
		TimestampMillis: header.Timestamp,
		PageSize:        header.PageSize,
		Commit:          header.Commit,
		PreChecksum:     header.PreApplyChecksum,
		PostChecksum:    postChecksum,
		Snapshot:        minimum == 1,
	}, nil
}

func parseFilename(name string) (uint64, uint64, error) {
	if filepath.Ext(name) != ".ltx" {
		return 0, 0, fmt.Errorf("invalid LTX filename %q", name)
	}
	parts := strings.Split(strings.TrimSuffix(name, ".ltx"), "-")
	if len(parts) != 2 || len(parts[0]) != 16 || len(parts[1]) != 16 {
		return 0, 0, fmt.Errorf("invalid LTX filename %q", name)
	}
	minimum, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid LTX filename %q", name)
	}
	maximum, err := strconv.ParseUint(parts[1], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid LTX filename %q", name)
	}
	return minimum, maximum, nil
}

func validateSegment(segment Segment) error {
	if segment.Key == "" || segment.MaxTXID < segment.MinTXID || !validPageSize(segment.PageSize) || segment.Commit == 0 || segment.PostChecksum&checksumFlag == 0 {
		return fmt.Errorf("%w: malformed segment %q", ErrInvalidInventory, segment.Key)
	}
	if segment.Snapshot {
		if segment.MinTXID != 1 || segment.PreChecksum != 0 {
			return fmt.Errorf("%w: malformed snapshot %q", ErrInvalidInventory, segment.Key)
		}
		return nil
	}
	if segment.MinTXID <= 1 || segment.PreChecksum&checksumFlag == 0 {
		return fmt.Errorf("%w: malformed delta %q", ErrInvalidInventory, segment.Key)
	}
	return nil
}

func validPageSize(size uint32) bool {
	return size >= 512 && size <= 65536 && size&(size-1) == 0
}
