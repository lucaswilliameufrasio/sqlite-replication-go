package spool

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/walcapture"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/ltx"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
)

const captureWatermarkVersion = 1

type FaultPoint string

const (
	BeforeStageWrite     FaultPoint = "before-stage-write"
	AfterStageWrite      FaultPoint = "after-stage-write"
	BeforeStageSync      FaultPoint = "before-stage-sync"
	AfterStageSync       FaultPoint = "after-stage-sync"
	BeforeLTXPublish     FaultPoint = "before-ltx-publish"
	AfterLTXPublish      FaultPoint = "after-ltx-publish"
	BeforeWatermark      FaultPoint = "before-watermark"
	BeforeWatermarkWrite FaultPoint = "before-watermark-write"
	BeforeWatermarkSync  FaultPoint = "before-watermark-sync"
	AfterWatermarkSync   FaultPoint = "after-watermark-sync"
	AfterWatermark       FaultPoint = "after-watermark"
)

type FaultHook func(point FaultPoint, transactionIndex int) error

type Watermark struct {
	Version          int                 `json:"version"`
	PageSize         uint32              `json:"page_size"`
	Commit           uint32              `json:"commit"`
	TXID             uint64              `json:"txid"`
	DatabaseChecksum uint64              `json:"database_checksum"`
	WAL              walcapture.Position `json:"wal"`
}

// PublishTransactions syncs and immutably publishes every LTX before atomically
// advancing the local capture watermark. The caller must retain its SQLite
// writer guard and reader pin until this function returns successfully.
func PublishTransactions(root string, transactions []walcapture.Transaction, state walcapture.State, hook FaultHook) error {
	return publishTransactionsWithSync(root, transactions, state, hook, func(file *os.File) error {
		return file.Sync()
	}, storagefs.SyncDirectory)
}

// PublishTransactionsWithBatchLimit rejects a batch whose encoded LTX bytes
// exceed limit before creating staging files. It does not count existing spool,
// metadata, encoding allocations, other producers, or physical disk blocks.
func PublishTransactionsWithBatchLimit(root string, transactions []walcapture.Transaction, state walcapture.State, hook FaultHook, limit uint64) error {
	if limit == 0 {
		return fmt.Errorf("LTX batch limit must be nonzero")
	}
	var total uint64
	for _, transaction := range transactions {
		size := uint64(len(transaction.Encoded))
		if size > limit-total {
			return fmt.Errorf("LTX batch size limit exceeded")
		}
		total += size
	}
	return PublishTransactions(root, transactions, state, hook)
}

// PublishTransactionsWithSpoolLimit bounds logical bytes under ltx plus the
// entire encoded batch. The caller must hold the repository lock and exclude all
// other mutations until return. Retries count their staging bytes even when the
// destination already exists. This is not a physical quota and excludes files
// outside ltx, encoding memory and non-cooperating writers.
func PublishTransactionsWithSpoolLimit(root string, transactions []walcapture.Transaction, state walcapture.State, hook FaultHook, limit uint64) error {
	if limit == 0 {
		return fmt.Errorf("spool limit must be nonzero")
	}
	remaining := limit
	err := filepath.Walk(filepath.Join(root, "ltx"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() || info.Size() < 0 {
			return fmt.Errorf("unsupported spool entry: %s", path)
		}
		size := uint64(info.Size())
		if size > remaining {
			return fmt.Errorf("spool size limit exceeded")
		}
		remaining -= size
		return nil
	})
	if err != nil {
		return fmt.Errorf("measure spool: %w", err)
	}
	for _, transaction := range transactions {
		size := uint64(len(transaction.Encoded))
		if size > remaining {
			return fmt.Errorf("spool size limit exceeded")
		}
		remaining -= size
	}
	return PublishTransactions(root, transactions, state, hook)
}

func publishTransactionsWithSync(
	root string,
	transactions []walcapture.Transaction,
	state walcapture.State,
	hook FaultHook,
	syncFile func(*os.File) error,
	syncMetadataDirectory func(string) error,
) error {
	if err := storagefs.EnsureRepository(root); err != nil {
		return fmt.Errorf("ensure capture repository: %w", err)
	}
	if err := validateBatch(transactions, state); err != nil {
		return err
	}

	for index, transaction := range transactions {
		finalPath := storagefs.LTXPath(root, transaction.Header.MinTXID, transaction.Header.MaxTXID)
		if err := os.MkdirAll(filepath.Dir(finalPath), 0o750); err != nil {
			return fmt.Errorf("create LTX destination directory: %w", err)
		}
		staged, err := os.CreateTemp(filepath.Dir(finalPath), ".capture-*.stage")
		if err != nil {
			return fmt.Errorf("create LTX staging file: %w", err)
		}
		stagedPath := staged.Name()
		published := false
		defer func() {
			if !published {
				_ = os.Remove(stagedPath)
			}
		}()
		if err := invokeHook(hook, BeforeStageWrite, index); err != nil {
			_ = staged.Close()
			return err
		}
		if err := writeFull(staged, transaction.Encoded); err != nil {
			_ = staged.Close()
			return fmt.Errorf("write staged LTX %d: %w", transaction.Header.MinTXID, err)
		}
		if err := invokeHook(hook, AfterStageWrite, index); err != nil {
			_ = staged.Close()
			return err
		}
		if err := invokeHook(hook, BeforeStageSync, index); err != nil {
			_ = staged.Close()
			return err
		}
		if err := syncFile(staged); err != nil {
			_ = staged.Close()
			return fmt.Errorf("sync staged LTX %d: %w", transaction.Header.MinTXID, err)
		}
		if err := staged.Close(); err != nil {
			return fmt.Errorf("close staged LTX %d: %w", transaction.Header.MinTXID, err)
		}
		if err := invokeHook(hook, AfterStageSync, index); err != nil {
			return err
		}
		if err := invokeHook(hook, BeforeLTXPublish, index); err != nil {
			return err
		}
		if err := storagefs.Publish(stagedPath, finalPath); err != nil {
			return fmt.Errorf("publish LTX %d: %w", transaction.Header.MinTXID, err)
		}
		published = true
		if err := invokeHook(hook, AfterLTXPublish, index); err != nil {
			return err
		}
	}

	if err := invokeHook(hook, BeforeWatermark, len(transactions)); err != nil {
		return err
	}
	watermark := Watermark{
		Version:          captureWatermarkVersion,
		PageSize:         state.Database.PageSize,
		Commit:           state.Database.Commit,
		TXID:             state.Database.TXID,
		DatabaseChecksum: state.Database.Checksum,
		WAL:              state.WAL,
	}
	if err := writeWatermark(root, watermark, hook, len(transactions), syncFile, syncMetadataDirectory); err != nil {
		return err
	}
	return invokeHook(hook, AfterWatermark, len(transactions))
}

func ReadWatermark(root string) (Watermark, error) {
	path := filepath.Join(root, "_meta", "capture-watermark.json")
	file, err := os.Open(path)
	if err != nil {
		return Watermark{}, fmt.Errorf("open capture watermark: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var watermark Watermark
	if err := decoder.Decode(&watermark); err != nil {
		return Watermark{}, fmt.Errorf("decode capture watermark: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Watermark{}, fmt.Errorf("capture watermark contains trailing data")
	}
	if watermark.Version != captureWatermarkVersion || watermark.PageSize == 0 || watermark.TXID == 0 {
		return Watermark{}, fmt.Errorf("capture watermark is incompatible")
	}
	if watermark.DatabaseChecksum&ltx.ChecksumFlag == 0 {
		return Watermark{}, fmt.Errorf("capture watermark has invalid database checksum")
	}
	if watermark.WAL.FrameCount > 0 && !watermark.WAL.Initialized {
		return Watermark{}, fmt.Errorf("capture watermark WAL frame count has no generation")
	}
	return watermark, nil
}

func writeWatermark(
	root string,
	watermark Watermark,
	hook FaultHook,
	transactionIndex int,
	syncFile func(*os.File) error,
	syncMetadataDirectory func(string) error,
) error {
	metadataDirectory := filepath.Join(root, "_meta")
	if err := os.MkdirAll(metadataDirectory, 0o750); err != nil {
		return fmt.Errorf("create capture metadata directory: %w", err)
	}
	staged, err := os.CreateTemp(metadataDirectory, ".capture-watermark-*.tmp")
	if err != nil {
		return fmt.Errorf("create capture watermark staging file: %w", err)
	}
	stagedPath := staged.Name()
	defer os.Remove(stagedPath)
	encoder := json.NewEncoder(staged)
	encoder.SetIndent("", "  ")
	if err := invokeHook(hook, BeforeWatermarkWrite, transactionIndex); err != nil {
		_ = staged.Close()
		return err
	}
	if err := encoder.Encode(watermark); err != nil {
		_ = staged.Close()
		return fmt.Errorf("encode capture watermark: %w", err)
	}
	if err := invokeHook(hook, BeforeWatermarkSync, transactionIndex); err != nil {
		_ = staged.Close()
		return err
	}
	if err := syncFile(staged); err != nil {
		_ = staged.Close()
		return fmt.Errorf("sync capture watermark: %w", err)
	}
	if err := staged.Close(); err != nil {
		return fmt.Errorf("close capture watermark: %w", err)
	}
	if err := invokeHook(hook, AfterWatermarkSync, transactionIndex); err != nil {
		return err
	}
	finalPath := filepath.Join(metadataDirectory, "capture-watermark.json")
	if err := os.Rename(stagedPath, finalPath); err != nil {
		return fmt.Errorf("atomically replace capture watermark: %w", err)
	}
	if err := syncMetadataDirectory(metadataDirectory); err != nil {
		return fmt.Errorf("sync capture metadata directory: %w", err)
	}
	return nil
}

func validateBatch(transactions []walcapture.Transaction, state walcapture.State) error {
	previousTXID := uint64(0)
	for index, transaction := range transactions {
		if transaction.Header.MinTXID == 0 || transaction.Header.MaxTXID < transaction.Header.MinTXID {
			return fmt.Errorf("invalid transaction ID range at batch index %d", index)
		}
		if index > 0 && transaction.Header.MinTXID != previousTXID+1 {
			return fmt.Errorf("non-contiguous LTX transaction IDs at batch index %d", index)
		}
		header, pages, postChecksum, err := ltx.DecodeDelta(bytes.NewReader(transaction.Encoded))
		if err != nil {
			return fmt.Errorf("validate encoded LTX at batch index %d: %w", index, err)
		}
		if header != transaction.Header || postChecksum != transaction.PostChecksum || len(pages) != len(transaction.Pages) {
			return fmt.Errorf("encoded LTX differs from transaction metadata at batch index %d", index)
		}
		for pageIndex := range pages {
			if pages[pageIndex].PageNumber != transaction.Pages[pageIndex].PageNumber ||
				!bytes.Equal(pages[pageIndex].Data, transaction.Pages[pageIndex].Data) {
				return fmt.Errorf("encoded LTX page differs at batch index %d page index %d", index, pageIndex)
			}
		}
		previousTXID = transaction.Header.MaxTXID
	}
	if len(transactions) > 0 {
		last := transactions[len(transactions)-1]
		if last.Header.MaxTXID != state.Database.TXID || last.PostChecksum != state.Database.Checksum || last.Header.Commit != state.Database.Commit {
			return fmt.Errorf("capture watermark does not match the final LTX transaction")
		}
	}
	if state.Database.PageSize == 0 || state.Database.TXID == 0 || state.Database.Checksum&ltx.ChecksumFlag == 0 {
		return fmt.Errorf("capture state is incomplete")
	}
	return nil
}

func invokeHook(hook FaultHook, point FaultPoint, transactionIndex int) error {
	if hook == nil {
		return nil
	}
	if err := hook(point, transactionIndex); err != nil {
		return fmt.Errorf("injected capture failure at %s: %w", point, err)
	}
	return nil
}

func writeFull(destination io.Writer, content []byte) error {
	for len(content) > 0 {
		written, err := destination.Write(content)
		if written > 0 {
			content = content[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
