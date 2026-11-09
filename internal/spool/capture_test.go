package spool

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/walcapture"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/ltx"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
)

func TestPublishTransactionsDurabilityAndFaultBoundaries(t *testing.T) {
	root := filepath.Join(t.TempDir(), "repository")
	transaction, state := sampleCapture()

	if err := PublishTransactions(root, []walcapture.Transaction{transaction}, state, nil); err != nil {
		t.Fatalf("publish LTX transaction: %v", err)
	}
	watermark, err := ReadWatermark(root)
	if err != nil {
		t.Fatalf("read published watermark: %v", err)
	}
	if watermark.TXID != state.Database.TXID || watermark.DatabaseChecksum != state.Database.Checksum || watermark.WAL != state.WAL {
		t.Fatalf("watermark differs from durable state: %+v", watermark)
	}
	ltxPath := storagefs.LTXPath(root, transaction.Header.MinTXID, transaction.Header.MaxTXID)
	encoded, err := os.ReadFile(ltxPath)
	if err != nil {
		t.Fatalf("read published LTX file: %v", err)
	}
	if !bytes.Equal(encoded, transaction.Encoded) {
		t.Fatal("published LTX bytes differ from staged transaction")
	}
	if err := PublishTransactions(root, []walcapture.Transaction{transaction}, state, nil); err != nil {
		t.Fatalf("retry identical published transaction: %v", err)
	}

	for _, point := range []FaultPoint{AfterStageSync, BeforeLTXPublish} {
		faultRoot := filepath.Join(t.TempDir(), "fault-before-publish")
		err := PublishTransactions(faultRoot, []walcapture.Transaction{transaction}, state, func(got FaultPoint, _ int) error {
			if got == point {
				return errors.New("injected I/O failure")
			}
			return nil
		})
		if err == nil {
			t.Fatalf("fault at %s did not fail publication", point)
		}
		if _, err := os.Stat(storagefs.LTXPath(faultRoot, transaction.Header.MinTXID, transaction.Header.MaxTXID)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("fault at %s published an LTX file: %v", point, err)
		}
		if _, err := ReadWatermark(faultRoot); err == nil {
			t.Fatalf("fault at %s advanced a watermark", point)
		}
	}

	for _, point := range []FaultPoint{AfterLTXPublish, BeforeWatermark, AfterWatermarkSync} {
		faultRoot := filepath.Join(t.TempDir(), "fault-after-publish")
		err := PublishTransactions(faultRoot, []walcapture.Transaction{transaction}, state, func(got FaultPoint, _ int) error {
			if got == point {
				return errors.New("injected crash boundary")
			}
			return nil
		})
		if err == nil {
			t.Fatalf("fault at %s did not fail publication", point)
		}
		if _, err := os.Stat(storagefs.LTXPath(faultRoot, transaction.Header.MinTXID, transaction.Header.MaxTXID)); err != nil {
			t.Fatalf("fault at %s lost already-published LTX file: %v", point, err)
		}
		if _, err := ReadWatermark(faultRoot); err == nil {
			t.Fatalf("fault at %s advanced an unpublished watermark", point)
		}
	}

	for _, test := range []struct {
		point             FaultPoint
		publishedLTX      bool
		injectedOperation string
	}{
		{point: BeforeStageWrite, injectedOperation: "write: no space left on device"},
		{point: AfterStageWrite, injectedOperation: "fsync: input/output error"},
		{point: BeforeStageSync, injectedOperation: "fsync: input/output error"},
		{point: BeforeWatermarkWrite, publishedLTX: true, injectedOperation: "write: no space left on device"},
		{point: BeforeWatermarkSync, publishedLTX: true, injectedOperation: "fsync: input/output error"},
	} {
		faultRoot := filepath.Join(t.TempDir(), "syscall-boundary-failure")
		faultErr := errors.New(test.injectedOperation)
		err := PublishTransactions(faultRoot, []walcapture.Transaction{transaction}, state, func(got FaultPoint, _ int) error {
			if got == test.point {
				return faultErr
			}
			return nil
		})
		if !errors.Is(err, faultErr) {
			t.Fatalf("injected %s at %s returned %v", test.injectedOperation, test.point, err)
		}
		_, statErr := os.Stat(storagefs.LTXPath(faultRoot, transaction.Header.MinTXID, transaction.Header.MaxTXID))
		if test.publishedLTX && statErr != nil {
			t.Fatalf("failure at %s lost already-published LTX: %v", test.point, statErr)
		}
		if !test.publishedLTX && !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("failure at %s unexpectedly published LTX: %v", test.point, statErr)
		}
		if _, err := ReadWatermark(faultRoot); err == nil {
			t.Fatalf("failure at %s advanced watermark", test.point)
		}
	}

	faultRoot := filepath.Join(t.TempDir(), "fault-after-watermark")
	err = PublishTransactions(faultRoot, []walcapture.Transaction{transaction}, state, func(got FaultPoint, _ int) error {
		if got == AfterWatermark {
			return errors.New("simulated acknowledgement loss")
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected post-watermark hook error")
	}
	if watermark, readErr := ReadWatermark(faultRoot); readErr != nil || watermark.TXID != state.Database.TXID {
		t.Fatalf("durable watermark not recoverable after acknowledgement loss: %+v, %v", watermark, readErr)
	}
}

func TestPublishTransactionsBatchLimitRejectsBeforeStagingAndRetries(t *testing.T) {
	root := filepath.Join(t.TempDir(), "repository")
	transaction, state := sampleCapture()
	limit := uint64(len(transaction.Encoded))
	if limit < 2 {
		t.Fatal("invalid fixture")
	}
	called := false
	err := PublishTransactionsWithBatchLimit(root, []walcapture.Transaction{transaction}, state, func(FaultPoint, int) error {
		called = true
		return nil
	}, limit-1)
	if err == nil || called {
		t.Fatalf("oversize batch: error %v, hook called %t", err, called)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected batch created repository: %v", err)
	}
	if err := PublishTransactionsWithBatchLimit(root, []walcapture.Transaction{transaction}, state, nil, limit); err != nil {
		t.Fatalf("retry at exact limit: %v", err)
	}
	watermark, err := ReadWatermark(root)
	if err != nil || watermark.TXID != state.Database.TXID {
		t.Fatalf("watermark: %+v, %v", watermark, err)
	}
}

func TestPublishTransactionsBatchLimitCountsWholeBatch(t *testing.T) {
	root := filepath.Join(t.TempDir(), "repository")
	transaction, state := sampleCapture()
	// Duplicate transactions intentionally: the aggregate check must reject the
	// entire input before batch validation or any filesystem mutation.
	batch := []walcapture.Transaction{transaction, transaction}
	limit := uint64(len(transaction.Encoded))
	err := PublishTransactionsWithBatchLimit(root, batch, state, nil, limit)
	if err == nil || !strings.Contains(err.Error(), "LTX batch size limit exceeded") {
		t.Fatalf("expected aggregate limit error, got %v", err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oversize batch created repository: %v", err)
	}
}

func TestSpoolLimitRejectsSymlinkAndMissingDirectory(t *testing.T) {
	root := filepath.Join(t.TempDir(), "repository")
	tx, state := sampleCapture()
	batch := []walcapture.Transaction{tx}
	if err := PublishTransactionsWithSpoolLimit(root, batch, state, nil, 1<<20); err == nil {
		t.Fatal("missing spool accepted as empty")
	}
	ltxDir := filepath.Join(root, "ltx")
	if err := os.MkdirAll(ltxDir, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "outside"), filepath.Join(ltxDir, "unexpected")); err != nil {
		t.Fatal(err)
	}
	if err := PublishTransactionsWithSpoolLimit(root, batch, state, nil, 1<<20); err == nil {
		t.Fatal("symlink accepted during spool measurement")
	}
	if _, err := ReadWatermark(root); err == nil {
		t.Fatal("watermark advanced after failed measurement")
	}
}

func TestSpoolLimitCountsExistingStagingAndRetry(t *testing.T) {
	root := filepath.Join(t.TempDir(), "repository")
	ltxDir := filepath.Join(root, "ltx")
	if err := os.MkdirAll(ltxDir, 0750); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(ltxDir, ".old.stage")
	if err := os.WriteFile(stale, []byte("12345"), 0600); err != nil {
		t.Fatal(err)
	}
	tx, state := sampleCapture()
	size := uint64(len(tx.Encoded))
	batch := []walcapture.Transaction{tx}
	if err := PublishTransactionsWithSpoolLimit(root, batch, state, nil, size+4); err == nil {
		t.Fatal("existing staging was not counted")
	}
	injected := errors.New("published")
	err := PublishTransactionsWithSpoolLimit(root, batch, state, func(point FaultPoint, _ int) error {
		if point == AfterLTXPublish {
			return injected
		}
		return nil
	}, size+5)
	if !errors.Is(err, injected) {
		t.Fatalf("publication: %v", err)
	}
	if err := PublishTransactionsWithSpoolLimit(root, batch, state, nil, size+5); err == nil {
		t.Fatal("retry staging was not counted")
	}
	if err := PublishTransactionsWithSpoolLimit(root, batch, state, nil, size*2+5); err != nil {
		t.Fatalf("retry with headroom: %v", err)
	}
	watermark, err := ReadWatermark(root)
	if err != nil || watermark.TXID != state.Database.TXID {
		t.Fatalf("watermark: %+v %v", watermark, err)
	}
}

func TestPublishTransactionsBatchLimitRetriesAfterPublicationFailure(t *testing.T) {
	root := filepath.Join(t.TempDir(), "repository")
	transaction, state := sampleCapture()
	limit := uint64(len(transaction.Encoded))
	injected := errors.New("failure after LTX publication")
	err := PublishTransactionsWithBatchLimit(root, []walcapture.Transaction{transaction}, state, func(point FaultPoint, _ int) error {
		if point == AfterLTXPublish {
			return injected
		}
		return nil
	}, limit)
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error: %v", err)
	}
	if _, err := os.Stat(storagefs.LTXPath(root, transaction.Header.MinTXID, transaction.Header.MaxTXID)); err != nil {
		t.Fatalf("published LTX missing: %v", err)
	}
	if _, err := ReadWatermark(root); err == nil {
		t.Fatal("watermark advanced before retry")
	}
	if err := PublishTransactionsWithBatchLimit(root, []walcapture.Transaction{transaction}, state, nil, limit); err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	watermark, err := ReadWatermark(root)
	if err != nil || watermark.TXID != state.Database.TXID {
		t.Fatalf("watermark after retry: %+v, %v", watermark, err)
	}
}

func TestPublishTransactionsPropagatesSyncOperationErrors(t *testing.T) {
	transaction, state := sampleCapture()

	for _, test := range []struct {
		name         string
		failSyncCall int
		wantLTX      bool
	}{
		{name: "LTX file sync", failSyncCall: 1},
		{name: "watermark file sync", failSyncCall: 2, wantLTX: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "repository")
			injectedErr := errors.New("injected EIO from File.Sync operation")
			syncCall := 0
			err := publishTransactionsWithSync(
				root,
				[]walcapture.Transaction{transaction},
				state,
				nil,
				func(file *os.File) error {
					syncCall++
					if syncCall == test.failSyncCall {
						return injectedErr
					}
					return file.Sync()
				},
				storagefs.SyncDirectory,
			)
			if !errors.Is(err, injectedErr) {
				t.Fatalf("sync error = %v, want injected EIO", err)
			}

			ltxPath := storagefs.LTXPath(root, transaction.Header.MinTXID, transaction.Header.MaxTXID)
			_, statErr := os.Stat(ltxPath)
			if test.wantLTX && statErr != nil {
				t.Fatalf("failed watermark sync lost published LTX: %v", statErr)
			}
			if !test.wantLTX && !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("failed LTX sync unexpectedly published LTX: %v", statErr)
			}
			if _, readErr := ReadWatermark(root); readErr == nil {
				t.Fatal("file sync error advanced watermark")
			}

			if retryErr := PublishTransactions(root, []walcapture.Transaction{transaction}, state, nil); retryErr != nil {
				t.Fatalf("retry after file sync error: %v", retryErr)
			}
			if retryErr := PublishTransactions(root, []walcapture.Transaction{transaction}, state, nil); retryErr != nil {
				t.Fatalf("repeat retry after file sync error: %v", retryErr)
			}
		})
	}

	t.Run("metadata directory sync after watermark rename", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "repository")
		injectedErr := errors.New("injected EIO from directory Sync operation")
		err := publishTransactionsWithSync(
			root,
			[]walcapture.Transaction{transaction},
			state,
			nil,
			func(file *os.File) error { return file.Sync() },
			func(string) error { return injectedErr },
		)
		if !errors.Is(err, injectedErr) {
			t.Fatalf("directory sync error = %v, want injected EIO", err)
		}
		watermark, readErr := ReadWatermark(root)
		if readErr != nil || watermark.TXID != state.Database.TXID {
			t.Fatalf("renamed watermark not visible after directory sync error: %+v, %v", watermark, readErr)
		}
		if retryErr := PublishTransactions(root, []walcapture.Transaction{transaction}, state, nil); retryErr != nil {
			t.Fatalf("retry after directory sync error: %v", retryErr)
		}
		if retryErr := PublishTransactions(root, []walcapture.Transaction{transaction}, state, nil); retryErr != nil {
			t.Fatalf("repeat retry after directory sync error: %v", retryErr)
		}
	})
}

func sampleCapture() (walcapture.Transaction, walcapture.State) {
	pageSize := uint32(512)
	image := make([]byte, 2*int(pageSize))
	copy(image[:16], "SQLite format 3\x00")
	binary.BigEndian.PutUint16(image[16:18], uint16(pageSize))
	binary.BigEndian.PutUint32(image[28:32], 2)
	baseState, err := ltx.LoadDatabaseState(bytes.NewReader(image), int64(len(image)), 1)
	if err != nil {
		panic(err)
	}
	page := bytes.Repeat([]byte{0x56}, int(pageSize))
	header := ltx.Header{
		PageSize:         pageSize,
		Commit:           2,
		MinTXID:          2,
		MaxTXID:          2,
		PreApplyChecksum: baseState.Checksum,
		WALOffset:        32,
		WALSize:          32 + 24 + int64(pageSize),
		WALSalt1:         1,
		WALSalt2:         2,
	}
	pages := []ltx.Page{{PageNumber: 2, Data: page}}
	postState, err := baseState.ApplyPages(header, pages)
	if err != nil {
		panic(err)
	}
	var encoded bytes.Buffer
	if err := ltx.EncodeDelta(header, pages, postState.Checksum, &encoded); err != nil {
		panic(err)
	}
	transaction := walcapture.Transaction{
		StartFrame:   1,
		EndFrame:     1,
		Header:       header,
		Pages:        pages,
		PostChecksum: postState.Checksum,
		Encoded:      encoded.Bytes(),
	}
	state := walcapture.State{
		Database: postState,
		WAL: walcapture.Position{
			Initialized:        true,
			CheckpointSequence: 1,
			Salt1:              1,
			Salt2:              2,
			FrameCount:         1,
			Checksum1:          3,
			Checksum2:          4,
		},
	}
	return transaction, state
}
