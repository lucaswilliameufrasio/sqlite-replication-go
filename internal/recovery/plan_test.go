package recovery

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/sqliteadapter"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/ltx"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
	_ "modernc.org/sqlite"
)

func TestBuildRestorePlanByInclusiveTXID(t *testing.T) {
	inventory := []Segment{
		delta("delta-2", 2, 3, 10, 12, 200),
		snapshot("base", 1, 0, 10),
		delta("delta-4", 4, 4, 12, 13, 400),
	}
	plan, err := BuildRestorePlan(inventory, Target{TXID: 3})
	if err != nil {
		t.Fatalf("BuildRestorePlan: %v", err)
	}
	wantKeys := []string{"base", "delta-2"}
	gotKeys := make([]string, len(plan.Segments))
	for index, segment := range plan.Segments {
		gotKeys[index] = segment.Key
	}
	if !reflect.DeepEqual(gotKeys, wantKeys) || plan.TargetTXID != 3 {
		t.Fatalf("restore plan = %+v, want keys %v at TXID 3", plan, wantKeys)
	}
}

func TestBuildRestorePlanTimestampIsInclusiveAndSelectsLatestTXID(t *testing.T) {
	inventory := []Segment{
		snapshot("base", 1, 0, 10),
		delta("delta-2", 2, 2, 10, 11, 200),
		delta("delta-3", 3, 4, 11, 12, 300),
	}
	timestamp := int64(300)
	plan, err := BuildRestorePlan(inventory, Target{TimestampMillis: &timestamp})
	if err != nil {
		t.Fatalf("BuildRestorePlan: %v", err)
	}
	if plan.TargetTXID != 4 || plan.TimestampMillis != timestamp {
		t.Fatalf("timestamp restore point = TXID %d at %d, want TXID 4 at %d", plan.TargetTXID, plan.TimestampMillis, timestamp)
	}
}

func TestBuildRestorePlanRejectsUnavailableInteriorTXIDAndGaps(t *testing.T) {
	tests := []struct {
		name      string
		inventory []Segment
		target    uint64
	}{
		{
			name:      "inside indivisible segment",
			inventory: []Segment{snapshot("base", 1, 0, 10), delta("delta", 2, 4, 10, 11, 400)},
			target:    3,
		},
		{
			name:      "gap",
			inventory: []Segment{snapshot("base", 1, 0, 10), delta("delta", 3, 3, 10, 11, 300)},
			target:    3,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := BuildRestorePlan(test.inventory, Target{TXID: test.target}); !errors.Is(err, ErrNoRestorePoint) {
				t.Fatalf("BuildRestorePlan error = %v, want ErrNoRestorePoint", err)
			}
		})
	}
}

func TestBuildRestorePlanRejectsForksAndMismatchedChecksums(t *testing.T) {
	base := snapshot("base", 1, 0, 10)
	t.Run("fork", func(t *testing.T) {
		inventory := []Segment{base, delta("fork-a", 2, 2, 10, 11, 200), delta("fork-b", 2, 2, 10, 12, 200)}
		if _, err := BuildRestorePlan(inventory, Target{TXID: 2}); !errors.Is(err, ErrAmbiguousChain) {
			t.Fatalf("BuildRestorePlan error = %v, want ErrAmbiguousChain", err)
		}
	})
	t.Run("mismatched previous checksum", func(t *testing.T) {
		inventory := []Segment{base, delta("delta", 2, 2, 99, 11, 200)}
		if _, err := BuildRestorePlan(inventory, Target{TXID: 2}); !errors.Is(err, ErrNoRestorePoint) {
			t.Fatalf("BuildRestorePlan error = %v, want ErrNoRestorePoint", err)
		}
	})
}

func TestBuildRestorePlanFromFilesystemVerifiesLTXAndDoesNotMutateRepository(t *testing.T) {
	root := t.TempDir()
	if err := storagefs.EnsureRepository(root); err != nil {
		t.Fatalf("initialize repository: %v", err)
	}
	database := make([]byte, 4096)
	copy(database[:16], "SQLite format 3\x00")
	binary.BigEndian.PutUint16(database[16:18], 4096)
	binary.BigEndian.PutUint32(database[28:32], 1)
	snapshotPath := storagefs.LTXPath(root, 1, 7)
	if err := os.MkdirAll(filepath.Dir(snapshotPath), 0o750); err != nil {
		t.Fatal(err)
	}
	staged, err := os.CreateTemp(filepath.Dir(snapshotPath), ".snapshot-*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ltx.EncodeSnapshotAt(bytes.NewReader(database), int64(len(database)), staged, 7, 1234); err != nil {
		_ = staged.Close()
		t.Fatalf("encode full snapshot at TXID 7: %v", err)
	}
	if err := staged.Sync(); err != nil {
		_ = staged.Close()
		t.Fatal(err)
	}
	stagedPath := staged.Name()
	if err := staged.Close(); err != nil {
		t.Fatal(err)
	}
	if err := storagefs.Publish(stagedPath, snapshotPath); err != nil {
		t.Fatalf("publish full snapshot: %v", err)
	}
	before, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildRestorePlanFromFilesystem(root, Target{TXID: 7})
	if err != nil {
		t.Fatalf("BuildRestorePlanFromFilesystem: %v", err)
	}
	if plan.TargetTXID != 7 || len(plan.Segments) != 1 || !plan.Segments[0].Snapshot {
		t.Fatalf("unexpected restore plan: %+v", plan)
	}
	after, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("read-only restore planning modified the source LTX object")
	}
	if _, err := os.Stat(filepath.Join(root, "_meta", ".recover-snapshot-*.sqlite")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected repository staging artifact: %v", err)
	}
}

func TestBuildRestorePlanFromFilesystemRejectsHeaderFilenameMismatch(t *testing.T) {
	root := t.TempDir()
	if err := storagefs.EnsureRepository(root); err != nil {
		t.Fatal(err)
	}
	database := make([]byte, 4096)
	copy(database[:16], "SQLite format 3\x00")
	binary.BigEndian.PutUint16(database[16:18], 4096)
	binary.BigEndian.PutUint32(database[28:32], 1)
	actualPath := storagefs.LTXPath(root, 1, 2)
	wrongPath := storagefs.LTXPath(root, 1, 3)
	if err := os.MkdirAll(filepath.Dir(actualPath), 0o750); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(actualPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ltx.EncodeSnapshotAt(bytes.NewReader(database), int64(len(database)), file, 2, 100); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(actualPath, wrongPath); err != nil {
		t.Fatal(err)
	}
	if _, err := BuildRestorePlanFromFilesystem(root, Target{TXID: 3}); err == nil {
		t.Fatal("planner accepted an LTX filename/header TXID mismatch")
	}
}

func TestRestoreToTargetReconstructsAndVerifiesLTXChain(t *testing.T) {
	root := t.TempDir()
	if err := storagefs.EnsureRepository(root); err != nil {
		t.Fatalf("initialize repository: %v", err)
	}
	databasePath := filepath.Join(t.TempDir(), "source.sqlite")
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec("CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec("INSERT INTO records VALUES (1, 'base')"); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	baseFile, err := os.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	baseInfo, err := baseFile.Stat()
	if err != nil {
		_ = baseFile.Close()
		t.Fatal(err)
	}
	baseState, err := ltx.LoadDatabaseState(baseFile, baseInfo.Size(), 1)
	if err != nil {
		_ = baseFile.Close()
		t.Fatal(err)
	}
	basePath := storagefs.LTXPath(root, 1, 1)
	if err := os.MkdirAll(filepath.Dir(basePath), 0o750); err != nil {
		_ = baseFile.Close()
		t.Fatal(err)
	}
	baseStage, err := os.CreateTemp(filepath.Dir(basePath), ".base-*.tmp")
	if err != nil {
		_ = baseFile.Close()
		t.Fatal(err)
	}
	if _, err := ltx.EncodeSnapshot(baseFile, baseInfo.Size(), baseStage, 100); err != nil {
		_ = baseFile.Close()
		_ = baseStage.Close()
		t.Fatalf("encode base snapshot: %v", err)
	}
	if err := baseStage.Sync(); err != nil {
		_ = baseFile.Close()
		_ = baseStage.Close()
		t.Fatal(err)
	}
	baseStagePath := baseStage.Name()
	if err := baseStage.Close(); err != nil {
		_ = baseFile.Close()
		t.Fatal(err)
	}
	if err := baseFile.Close(); err != nil {
		t.Fatal(err)
	}
	if err := storagefs.Publish(baseStagePath, basePath); err != nil {
		t.Fatalf("publish base snapshot: %v", err)
	}

	database, err = sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec("INSERT INTO records VALUES (2, 'delta')"); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	targetFile, err := os.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	targetInfo, err := targetFile.Stat()
	if err != nil {
		_ = targetFile.Close()
		t.Fatal(err)
	}
	deltaHeader, pages, targetState, err := baseState.DeltaToDatabaseImage(targetFile, targetInfo.Size(), 200)
	if err != nil {
		_ = targetFile.Close()
		t.Fatalf("create delta fixture: %v", err)
	}
	if err := targetFile.Close(); err != nil {
		t.Fatal(err)
	}
	deltaPath := storagefs.LTXPath(root, deltaHeader.MinTXID, deltaHeader.MaxTXID)
	deltaStage, err := os.CreateTemp(filepath.Dir(deltaPath), ".delta-*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if err := ltx.EncodeDelta(*deltaHeader, pages, targetState.Checksum, deltaStage); err != nil {
		_ = deltaStage.Close()
		t.Fatalf("encode delta fixture: %v", err)
	}
	if err := deltaStage.Sync(); err != nil {
		_ = deltaStage.Close()
		t.Fatal(err)
	}
	deltaStagePath := deltaStage.Name()
	if err := deltaStage.Close(); err != nil {
		t.Fatal(err)
	}
	if err := storagefs.Publish(deltaStagePath, deltaPath); err != nil {
		t.Fatalf("publish delta: %v", err)
	}

	destination := filepath.Join(t.TempDir(), "restored.sqlite")
	if err := RestoreToTarget(context.Background(), root, destination, Target{TXID: 2}); err != nil {
		t.Fatalf("RestoreToTarget: %v", err)
	}
	if err := sqliteadapter.VerifyDatabase(context.Background(), destination); err != nil {
		t.Fatalf("verify restored database: %v", err)
	}
	restored, err := sql.Open("sqlite", destination)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var count int
	if err := restored.QueryRow("SELECT count(*) FROM records").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("restored row count = %d, want 2", count)
	}
	if compactedPath, due, err := CompactIfDue(root, 1, 2, SnapshotPolicy{EveryTXIDs: 2}); err != nil || due || compactedPath != "" {
		t.Fatalf("CompactIfDue before cadence = %q, %v, %v; want empty, false, nil", compactedPath, due, err)
	}
	compactedPath, due, err := CompactIfDue(root, 1, 2, SnapshotPolicy{EveryTXIDs: 1})
	if err != nil {
		t.Fatalf("CompactIfDue: %v", err)
	}
	if !due {
		t.Fatal("CompactIfDue did not publish at the configured cadence")
	}
	compactedPlan, err := BuildRestorePlanFromFilesystem(root, Target{TXID: 2})
	if err != nil {
		t.Fatalf("plan compacted restore: %v", err)
	}
	if len(compactedPlan.Segments) != 1 || compactedPlan.Segments[0].Key != filepath.ToSlash(filepath.Join("ltx", "1", filepath.Base(compactedPath))) {
		t.Fatalf("compacted snapshot did not become preferred base: %+v", compactedPlan)
	}
	if _, err := os.Stat(basePath); err != nil {
		t.Fatalf("compaction removed original base: %v", err)
	}
	if _, err := os.Stat(deltaPath); err != nil {
		t.Fatalf("compaction removed original delta: %v", err)
	}
}

func TestGCPlanIsDryRunExplicitAndRevalidated(t *testing.T) {
	root := t.TempDir()
	if err := storagefs.EnsureRepository(root); err != nil {
		t.Fatal(err)
	}
	database := make([]byte, 4096)
	copy(database[:16], "SQLite format 3\x00")
	binary.BigEndian.PutUint16(database[16:18], 4096)
	binary.BigEndian.PutUint32(database[28:32], 1)
	for _, item := range []struct {
		level uint32
		txid  uint64
	}{
		{level: 0, txid: 1},
		{level: 1, txid: 1},
		{level: 2, txid: 2},
	} {
		finalPath := filepath.Join(root, "ltx", fmt.Sprint(item.level), fmt.Sprintf("%016x-%016x.ltx", 1, item.txid))
		if err := os.MkdirAll(filepath.Dir(finalPath), 0o750); err != nil {
			t.Fatal(err)
		}
		staged, err := os.CreateTemp(filepath.Dir(finalPath), ".snapshot-*.ltx")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ltx.EncodeSnapshotAt(bytes.NewReader(database), int64(len(database)), staged, item.txid, int64(item.txid*100)); err != nil {
			_ = staged.Close()
			t.Fatal(err)
		}
		if err := staged.Close(); err != nil {
			t.Fatal(err)
		}
		if err := storagefs.Publish(staged.Name(), finalPath); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := BuildGCPlan(root, nil); err == nil {
		t.Fatal("GC accepted an implicit empty retention policy")
	}
	reviewed, err := BuildGCPlan(root, []Target{{TXID: 2}})
	if err != nil {
		t.Fatalf("BuildGCPlan: %v", err)
	}
	if len(reviewed.Candidates) != 1 || !strings.HasPrefix(reviewed.Candidates[0].Key, "ltx/1/") {
		t.Fatalf("unexpected dry-run candidates: %+v", reviewed.Candidates)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(reviewed.Candidates[0].Key))); err != nil {
		t.Fatalf("dry-run deleted candidate: %v", err)
	}
	tampered := reviewed
	tampered.Candidates = append([]GCObject(nil), reviewed.Candidates...)
	tampered.Candidates[0].Size++
	if err := ApplyGCPlan(root, tampered); !errors.Is(err, ErrStaleGCPlan) {
		t.Fatalf("tampered plan error = %v, want ErrStaleGCPlan", err)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(reviewed.Candidates[0].Key))); err != nil {
		t.Fatalf("rejected plan removed candidate: %v", err)
	}
	if _, err := BuildRestorePlanFromFilesystem(root, Target{TXID: 2}); err != nil {
		t.Fatalf("rejected plan damaged retained restore point: %v", err)
	}
	// Characterize the uncoordinated window: a new immutable snapshot can be
	// published after revalidation and GC still removes its old candidate.
	concurrentPath := filepath.Join(root, "ltx", "3", "0000000000000001-0000000000000003.ltx")
	publication := func() error {
		if err := os.MkdirAll(filepath.Dir(concurrentPath), 0o750); err != nil {
			return err
		}
		staged, err := os.CreateTemp(filepath.Dir(concurrentPath), ".snapshot-*.ltx")
		if err != nil {
			return err
		}
		if _, err := ltx.EncodeSnapshotAt(bytes.NewReader(database), int64(len(database)), staged, 3, 300); err != nil {
			_ = staged.Close()
			return err
		}
		if err := staged.Close(); err != nil {
			return err
		}
		return storagefs.Publish(staged.Name(), concurrentPath)
	}
	ready := make(chan struct{})
	resume := make(chan struct{})
	gcResult := make(chan error, 1)
	go func() {
		gcResult <- applyGCPlan(root, reviewed, func() error {
			close(ready)
			<-resume
			return nil
		})
	}()
	<-ready
	publicationErr := publication()
	close(resume)
	err = <-gcResult
	if publicationErr != nil {
		t.Fatalf("publish while GC is paused: %v", publicationErr)
	}
	if err != nil {
		t.Fatalf("characterize concurrent publication: %v", err)
	}
	if _, err := os.Stat(concurrentPath); err != nil {
		t.Fatalf("concurrent publication missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(reviewed.Candidates[0].Key))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("GC candidate still exists: %v", err)
	}
	if _, err := BuildRestorePlanFromFilesystem(root, Target{TXID: 2}); err != nil {
		t.Fatalf("retained restore point unavailable after GC: %v", err)
	}
	stale, err := BuildGCPlan(root, []Target{{TXID: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ApplyGCPlan(root, stale); err != nil {
		t.Fatalf("no-op GC apply: %v", err)
	}
	changed, err := BuildGCPlan(root, []Target{{TXID: 2}})
	if err != nil {
		t.Fatal(err)
	}
	newObject := filepath.Join(root, "ltx", "2", "0000000000000001-0000000000000003.ltx")
	if err := os.MkdirAll(filepath.Dir(newObject), 0o750); err != nil {
		t.Fatal(err)
	}
	newFile, err := os.Create(newObject)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ltx.EncodeSnapshotAt(bytes.NewReader(database), int64(len(database)), newFile, 3, 300); err != nil {
		_ = newFile.Close()
		t.Fatal(err)
	}
	if err := newFile.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ApplyGCPlan(root, changed); !errors.Is(err, ErrStaleGCPlan) {
		t.Fatalf("stale plan error = %v, want ErrStaleGCPlan", err)
	}
}

func TestSnapshotCadenceRequiresExplicitPositiveInterval(t *testing.T) {
	if due, err := ShouldCompact(10, 15, SnapshotPolicy{EveryTXIDs: 5}); err != nil || !due {
		t.Fatalf("ShouldCompact at cadence boundary = %v, %v; want true, nil", due, err)
	}
	if due, err := ShouldCompact(10, 14, SnapshotPolicy{EveryTXIDs: 5}); err != nil || due {
		t.Fatalf("ShouldCompact before cadence = %v, %v; want false, nil", due, err)
	}
	if _, err := ShouldCompact(10, 15, SnapshotPolicy{}); err == nil {
		t.Fatal("zero snapshot cadence was accepted")
	}
	if _, err := ShouldCompact(15, 10, SnapshotPolicy{EveryTXIDs: 5}); err == nil {
		t.Fatal("current TXID before last snapshot was accepted")
	}
}

func snapshot(key string, maxTXID uint64, level uint32, checksum uint64) Segment {
	return Segment{
		Key:             key,
		Level:           level,
		MinTXID:         1,
		MaxTXID:         maxTXID,
		TimestampMillis: int64(maxTXID * 100),
		PageSize:        4096,
		Commit:          20,
		PostChecksum:    checksumFlag | checksum,
		Snapshot:        true,
	}
}

func formatLTXPath(root string, level uint32, minimum, maximum uint64) string {
	return filepath.Join(root, "ltx", fmt.Sprint(level), fmt.Sprintf("%016x-%016x.ltx", minimum, maximum))
}

func delta(key string, minTXID, maxTXID uint64, preChecksum, postChecksum uint64, timestampMillis int64) Segment {
	return Segment{
		Key:             key,
		Level:           0,
		MinTXID:         minTXID,
		MaxTXID:         maxTXID,
		TimestampMillis: timestampMillis,
		PageSize:        4096,
		Commit:          20,
		PreChecksum:     checksumFlag | preChecksum,
		PostChecksum:    checksumFlag | postChecksum,
	}
}
