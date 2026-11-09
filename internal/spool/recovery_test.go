package spool

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/walcapture"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/ltx"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
)

func TestRecoverPublishedDeltaAheadOfWatermark(t *testing.T) {
	root := filepath.Join(t.TempDir(), "repository")
	baseState := initializeRecoveryRepository(t, root)
	page := bytes.Repeat([]byte{0x6a}, int(baseState.PageSize))
	header := ltx.Header{
		PageSize:         baseState.PageSize,
		Commit:           baseState.Commit,
		MinTXID:          2,
		MaxTXID:          2,
		Timestamp:        123,
		PreApplyChecksum: baseState.Checksum,
	}
	pages := []ltx.Page{{PageNumber: 2, Data: page}}
	updated, err := baseState.ApplyPages(header, pages)
	if err != nil {
		t.Fatalf("calculate recovery delta: %v", err)
	}
	var encoded bytes.Buffer
	if err := ltx.EncodeDelta(header, pages, updated.Checksum, &encoded); err != nil {
		t.Fatalf("encode recovery delta: %v", err)
	}
	transaction := walcapture.Transaction{
		StartFrame:   1,
		EndFrame:     1,
		Header:       header,
		Pages:        pages,
		PostChecksum: updated.Checksum,
		Encoded:      encoded.Bytes(),
	}
	state := walcapture.State{Database: updated}
	syncCall := 0
	injectedSyncErr := errors.New("injected EIO from watermark File.Sync operation")
	err = publishTransactionsWithSync(
		root,
		[]walcapture.Transaction{transaction},
		state,
		nil,
		func(file *os.File) error {
			syncCall++
			if syncCall == 2 {
				return injectedSyncErr
			}
			return file.Sync()
		},
		storagefs.SyncDirectory,
	)
	if !errors.Is(err, injectedSyncErr) {
		t.Fatalf("publish returned %v, want injected watermark sync EIO", err)
	}
	watermark, err := ReadWatermark(root)
	if err != nil {
		t.Fatalf("read old watermark: %v", err)
	}
	if watermark.TXID != 1 {
		t.Fatalf("watermark advanced before recovery: %+v", watermark)
	}

	recovered, err := Recover(root, nil)
	if err != nil {
		t.Fatalf("recover published delta: %v", err)
	}
	if recovered.Database.TXID != 2 || recovered.Database.Checksum != updated.Checksum {
		t.Fatalf("recovered database state differs from published delta: %+v", recovered.Database)
	}
	recoveredWatermark, err := ReadWatermark(root)
	if err != nil {
		t.Fatalf("read reconciled watermark: %v", err)
	}
	if recoveredWatermark.TXID != 2 || recoveredWatermark.DatabaseChecksum != updated.Checksum {
		t.Fatalf("recovery did not advance watermark to published chain: %+v", recoveredWatermark)
	}
	repeated, err := Recover(root, nil)
	if err != nil {
		t.Fatalf("repeat recovery: %v", err)
	}
	if repeated.Database.TXID != recovered.Database.TXID || repeated.Database.Checksum != recovered.Database.Checksum {
		t.Fatal("recovery was not idempotent")
	}
}

func TestPublishInterruptedDuringSync(t *testing.T) {
	root := os.Getenv("SQLITE_REPLICATION_CP01_FUSE_ROOT")
	if root == "" {
		t.Skip("requires the opt-in CP-01 FUSE fsync interruption drill")
	}
	transaction, state := sampleCapture()
	if os.Getenv("SQLITE_REPLICATION_CP01_FUSE_CHILD") == "1" {
		if err := PublishTransactions(root, []walcapture.Transaction{transaction}, state, nil); err != nil {
			t.Fatalf("publish in child after release: %v", err)
		}
		return
	}

	barrierPath := os.Getenv("CP01_FUSE_BARRIER")
	releasePath := os.Getenv("CP01_FUSE_RELEASE")
	completedPath := os.Getenv("CP01_FUSE_COMPLETED")
	if barrierPath == "" || releasePath == "" || completedPath == "" {
		t.Fatal("FUSE barrier environment is incomplete")
	}
	baseState := initializeRecoveryRepository(t, root)
	if baseState.TXID != 1 {
		t.Fatalf("fixture base TXID = %d, want 1", baseState.TXID)
	}

	command := exec.Command(os.Args[0], "-test.run=^TestPublishInterruptedDuringSync$")
	command.Env = append(os.Environ(), "SQLITE_REPLICATION_CP01_FUSE_CHILD=1")
	var childOutput bytes.Buffer
	command.Stdout = &childOutput
	command.Stderr = &childOutput
	if err := command.Start(); err != nil {
		t.Fatalf("start publishing child: %v", err)
	}
	childDone := make(chan error, 1)
	go func() {
		childDone <- command.Wait()
	}()
	defer func() {
		if command.Process != nil {
			_ = command.Process.Kill()
			_ = os.WriteFile(releasePath, []byte("release\n"), 0o600)
			<-childDone
		}
	}()

	barrierDeadline := time.Now().Add(15 * time.Second)
	for {
		select {
		case childErr := <-childDone:
			command.Process = nil
			t.Fatalf("publishing child exited before reaching blocked fsync: %v; output: %s", childErr, childOutput.String())
		default:
		}
		if _, err := os.Stat(barrierPath); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("inspect FUSE fsync barrier: %v", err)
		}
		if time.Now().After(barrierDeadline) {
			t.Fatal("child did not enter the FUSE fsync callback before the timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := command.Process.Kill(); err != nil {
		t.Fatalf("kill child during blocked fsync: %v", err)
	}
	if err := os.WriteFile(releasePath, []byte("release\n"), 0o600); err != nil {
		t.Fatalf("release FUSE fsync callback after process kill: %v", err)
	}
	if err := <-childDone; err == nil {
		t.Fatal("publishing child exited successfully instead of being killed during fsync")
	}
	command.Process = nil
	completedDeadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(completedPath); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("inspect FUSE completion marker: %v", err)
		}
		if time.Now().After(completedDeadline) {
			t.Fatal("FUSE fsync callback did not finish after releasing its barrier")
		}
		time.Sleep(10 * time.Millisecond)
	}

	recovered, err := Recover(root, nil)
	if err != nil {
		t.Fatalf("recover base after interrupted fsync: %v", err)
	}
	if recovered.Database.TXID != 1 {
		t.Fatalf("interrupted publication advanced recovery to TXID %d, want 1", recovered.Database.TXID)
	}
	watermark, err := ReadWatermark(root)
	if err != nil || watermark.TXID != 1 {
		t.Fatalf("interrupted publication changed watermark: %+v, %v", watermark, err)
	}

	if err := PublishTransactions(root, []walcapture.Transaction{transaction}, state, nil); err != nil {
		t.Fatalf("retry capture after interrupted fsync: %v", err)
	}
	recovered, err = Recover(root, nil)
	if err != nil {
		t.Fatalf("recover retried capture: %v", err)
	}
	if recovered.Database.TXID != 2 || recovered.Database.Checksum != state.Database.Checksum {
		t.Fatalf("recovered capture differs from expected transaction: %+v", recovered.Database)
	}
	repeated, err := Recover(root, nil)
	if err != nil || !reflect.DeepEqual(repeated.Database, recovered.Database) {
		t.Fatalf("recovery after interrupted sync is not idempotent: %+v, %v", repeated.Database, err)
	}
}

func TestRecoverAfterAbruptQEMUExit(t *testing.T) {
	root := os.Getenv("SQLITE_REPLICATION_CP01_QEMU_ROOT")
	mode := os.Getenv("SQLITE_REPLICATION_CP01_QEMU_MODE")
	if root == "" || mode == "" {
		t.Skip("requires the opt-in CP-01 QEMU power-off drill")
	}

	transaction, state := sampleCapture()
	switch mode {
	case "prepare":
		baseState := initializeRecoveryRepository(t, root)
		if baseState.TXID != 1 {
			t.Fatalf("fixture base TXID = %d, want 1", baseState.TXID)
		}
		if err := PublishTransactions(root, []walcapture.Transaction{transaction}, state, nil); err != nil {
			t.Fatalf("publish durable capture before abrupt QEMU exit: %v", err)
		}
		recovered, err := Recover(root, nil)
		if err != nil {
			t.Fatalf("verify capture before abrupt QEMU exit: %v", err)
		}
		if recovered.Database.TXID != state.Database.TXID || recovered.Database.Checksum != state.Database.Checksum {
			t.Fatalf("pre-exit recovery differs from published capture: %+v", recovered.Database)
		}
	case "recover":
		recovered, err := Recover(root, nil)
		if err != nil {
			t.Fatalf("recover after abrupt QEMU exit: %v", err)
		}
		if recovered.Database.TXID != state.Database.TXID || recovered.Database.Checksum != state.Database.Checksum {
			t.Fatalf("post-exit recovery differs from published capture: %+v", recovered.Database)
		}
		repeated, err := Recover(root, nil)
		if err != nil {
			t.Fatalf("repeat post-exit recovery: %v", err)
		}
		if !reflect.DeepEqual(repeated.Database, recovered.Database) {
			t.Fatal("post-exit recovery was not idempotent")
		}
	default:
		t.Fatalf("unknown QEMU drill mode %q", mode)
	}
}

func TestRecoverRejectsGapOrDivergentWatermark(t *testing.T) {
	root := filepath.Join(t.TempDir(), "repository")
	baseState := initializeRecoveryRepository(t, root)
	watermark, err := ReadWatermark(root)
	if err != nil {
		t.Fatalf("read initial watermark: %v", err)
	}
	watermark.DatabaseChecksum ^= 1
	watermarkPath := filepath.Join(root, "_meta", "capture-watermark.json")
	watermarkBytes, err := json.Marshal(watermark)
	if err != nil {
		t.Fatalf("encode divergent watermark fixture: %v", err)
	}
	if err := os.WriteFile(watermarkPath, watermarkBytes, 0o600); err != nil {
		t.Fatalf("write divergent watermark fixture: %v", err)
	}
	if _, err := Recover(root, nil); err == nil {
		t.Fatal("accepted a watermark that differs from the verified snapshot")
	}
	if baseState.TXID != 1 {
		t.Fatalf("unexpected fixture base TXID: %d", baseState.TXID)
	}
}

func initializeRecoveryRepository(t *testing.T, root string) ltx.DatabaseState {
	t.Helper()
	if err := storagefs.EnsureRepository(root); err != nil {
		t.Fatalf("initialize repository: %v", err)
	}
	image := make([]byte, 2*512)
	copy(image[:16], "SQLite format 3\x00")
	binary.BigEndian.PutUint16(image[16:18], 512)
	binary.BigEndian.PutUint32(image[28:32], 2)
	baseState, err := ltx.LoadDatabaseState(bytes.NewReader(image), int64(len(image)), 1)
	if err != nil {
		t.Fatalf("load base state: %v", err)
	}
	snapshotPath := storagefs.LTXPath(root, 1, 1)
	if err := os.MkdirAll(filepath.Dir(snapshotPath), 0o750); err != nil {
		t.Fatalf("create snapshot directory: %v", err)
	}
	snapshotStage, err := os.CreateTemp(filepath.Dir(snapshotPath), ".snapshot-*.stage")
	if err != nil {
		t.Fatalf("create snapshot stage: %v", err)
	}
	if _, err := ltx.EncodeSnapshot(bytes.NewReader(image), int64(len(image)), snapshotStage, 1); err != nil {
		_ = snapshotStage.Close()
		t.Fatalf("encode base snapshot: %v", err)
	}
	if err := snapshotStage.Sync(); err != nil {
		_ = snapshotStage.Close()
		t.Fatalf("sync base snapshot: %v", err)
	}
	stagePath := snapshotStage.Name()
	if err := snapshotStage.Close(); err != nil {
		t.Fatalf("close base snapshot: %v", err)
	}
	if err := storagefs.Publish(stagePath, snapshotPath); err != nil {
		t.Fatalf("publish base snapshot: %v", err)
	}
	if err := PublishTransactions(root, nil, walcapture.State{Database: baseState}, nil); err != nil {
		t.Fatalf("publish base watermark: %v", err)
	}
	return baseState
}

func TestRecoverWithImageLimitRejectsAndRecoversWithoutChangingWatermark(t *testing.T) {
	root := filepath.Join(t.TempDir(), "repository")
	state := initializeRecoveryRepository(t, root)
	before, err := os.ReadFile(filepath.Join(root, "_meta", "capture-watermark.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RecoverWithImageLimit(root, nil, 512); err == nil {
		t.Fatal("expected bounded recovery to reject image larger than limit")
	}
	after, err := os.ReadFile(filepath.Join(root, "_meta", "capture-watermark.json"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("watermark changed after failed recovery: %v", err)
	}
	stages, err := filepath.Glob(filepath.Join(root, "_meta", ".recover-snapshot-*.sqlite"))
	if err != nil || len(stages) != 0 {
		t.Fatalf("recovery stages after failure: %v, error %v", stages, err)
	}
	recovered, err := RecoverWithImageLimit(root, nil, 1024)
	if err != nil {
		t.Fatalf("retry at exact image size: %v", err)
	}
	if recovered.Database.TXID != state.TXID || recovered.Database.Checksum != state.Checksum {
		t.Fatalf("recovered state = %+v", recovered.Database)
	}
}

func TestLimitedRecoveryWriterRejectsBeforeOversizeWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "image")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	writer := &limitedRecoveryWriter{writer: file, remaining: 4}
	if _, err := writer.Write([]byte("1234")); err != nil {
		t.Fatal(err)
	}
	if n, err := writer.Write([]byte("5")); n != 0 || err == nil {
		t.Fatalf("oversize write = %d, %v", n, err)
	}
	info, err := file.Stat()
	if err != nil || info.Size() != 4 {
		t.Fatalf("image size = %v, error = %v", info, err)
	}
}
