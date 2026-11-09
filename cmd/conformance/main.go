package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/capture"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/sqliteadapter"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/ltx"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
	_ "modernc.org/sqlite"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 2 {
		return usageError()
	}
	switch args[0] {
	case "create":
		if len(args) < 2 || len(args) > 3 {
			return usageError()
		}
		pageSize := 4096
		if len(args) == 3 {
			parsed, err := strconv.Atoi(args[2])
			if err != nil {
				return fmt.Errorf("parse page size: %w", err)
			}
			pageSize = parsed
		}
		return createDatabase(args[1], pageSize)
	case "snapshot":
		if len(args) != 3 {
			return usageError()
		}
		_, err := replication.Snapshot(context.Background(), args[1], args[2])
		return err
	case "encode-snapshot-at":
		if len(args) != 5 {
			return usageError()
		}
		txid, err := strconv.ParseUint(args[3], 10, 64)
		if err != nil {
			return fmt.Errorf("parse snapshot TXID: %w", err)
		}
		timestamp, err := strconv.ParseInt(args[4], 10, 64)
		if err != nil {
			return fmt.Errorf("parse snapshot timestamp: %w", err)
		}
		return encodeSnapshotAt(args[1], args[2], txid, timestamp)
	case "create-delta":
		if len(args) != 2 {
			return usageError()
		}
		return createDelta(args[1])
	case "create-capture-chain":
		if len(args) != 3 {
			return usageError()
		}
		return createCaptureChain(args[1], args[2])
	case "restore":
		if len(args) != 3 {
			return usageError()
		}
		return replication.RestoreSnapshot(context.Background(), args[1], args[2])
	case "decode-ltx":
		if len(args) != 3 {
			return usageError()
		}
		return decodeLTX(args[1], args[2])
	case "verify":
		if len(args) != 2 {
			return usageError()
		}
		return verifyDatabase(args[1])
	case "verify-capture-chain":
		if len(args) != 2 {
			return usageError()
		}
		return verifyCaptureChain(args[1])
	case "compare-capture-chain":
		if len(args) != 3 {
			return usageError()
		}
		return compareCaptureChain(args[1], args[2])
	default:
		return usageError()
	}
}

func encodeSnapshotAt(databasePath, ltxPath string, txid uint64, timestamp int64) error {
	database, err := os.Open(databasePath)
	if err != nil {
		return fmt.Errorf("open SQLite image: %w", err)
	}
	defer database.Close()
	info, err := database.Stat()
	if err != nil {
		return fmt.Errorf("stat SQLite image: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(ltxPath), 0o750); err != nil {
		return fmt.Errorf("create LTX output directory: %w", err)
	}
	output, err := os.OpenFile(ltxPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create LTX output: %w", err)
	}
	if _, err := ltx.EncodeSnapshotAt(database, info.Size(), output, txid, timestamp); err != nil {
		_ = output.Close()
		return fmt.Errorf("encode snapshot at TXID %d: %w", txid, err)
	}
	if err := output.Sync(); err != nil {
		_ = output.Close()
		return fmt.Errorf("sync LTX output: %w", err)
	}
	if err := output.Close(); err != nil {
		return fmt.Errorf("close LTX output: %w", err)
	}
	return nil
}

func createDelta(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create LTX delta directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create LTX delta: %w", err)
	}
	defer file.Close()

	pageOne := bytes.Repeat([]byte{0x11}, 512)
	copy(pageOne[:16], "SQLite format 3\x00")
	pageOne[16] = 2
	header := ltx.Header{
		PageSize:         512,
		Commit:           3,
		MinTXID:          2,
		MaxTXID:          2,
		Timestamp:        1_700_000_000_000,
		PreApplyChecksum: ltx.ChecksumFlag | 0x1234,
		WALOffset:        32,
		WALSize:          32 + 2*(24+512),
		WALSalt1:         0x11223344,
		WALSalt2:         0xaabbccdd,
	}
	pages := []ltx.Page{
		{PageNumber: 1, Data: pageOne},
		{PageNumber: 3, Data: bytes.Repeat([]byte{0x33}, 512)},
	}
	if err := ltx.EncodeDelta(header, pages, ltx.ChecksumFlag|0x9876, file); err != nil {
		return fmt.Errorf("encode LTX delta: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync LTX delta: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close LTX delta: %w", err)
	}
	return nil
}

func decodeLTX(sourcePath, destinationPath string) error {
	if err := os.MkdirAll(filepath.Dir(destinationPath), 0o750); err != nil {
		return fmt.Errorf("create restore directory: %w", err)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open LTX file: %w", err)
	}
	defer source.Close()
	stage, err := os.CreateTemp(filepath.Dir(destinationPath), ".conformance-restore-*.sqlite")
	if err != nil {
		return fmt.Errorf("create staged restore: %w", err)
	}
	stagePath := stage.Name()
	defer os.Remove(stagePath)
	header, err := ltx.DecodeSnapshot(source, stage)
	if err != nil {
		_ = stage.Close()
		return fmt.Errorf("decode LTX snapshot: %w", err)
	}
	if err := stage.Sync(); err != nil {
		_ = stage.Close()
		return fmt.Errorf("sync staged restore: %w", err)
	}
	if err := stage.Close(); err != nil {
		return fmt.Errorf("close staged restore: %w", err)
	}
	info, err := os.Stat(stagePath)
	if err != nil {
		return fmt.Errorf("stat staged restore: %w", err)
	}
	if info.Size() != int64(header.PageSize)*int64(header.Commit) {
		return fmt.Errorf("restored SQLite image has an invalid size")
	}
	if err := sqliteadapter.VerifyDatabase(context.Background(), stagePath); err != nil {
		return err
	}
	return storagefs.Publish(stagePath, destinationPath)
}

func createDatabase(path string, pageSize int) error {
	if pageSize < 512 || pageSize > 65536 || pageSize&(pageSize-1) != 0 {
		return fmt.Errorf("invalid SQLite page size: %d", pageSize)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create database directory: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("open conformance database: %w", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	if _, err := db.Exec(fmt.Sprintf("PRAGMA page_size = %d", pageSize)); err != nil {
		return fmt.Errorf("set SQLite page size: %w", err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("enable WAL mode: %w", err)
	}
	if _, err := db.Exec("CREATE TABLE records (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"); err != nil {
		return fmt.Errorf("create conformance table: %w", err)
	}
	transaction, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin conformance transaction: %w", err)
	}
	for id := 1; id <= 128; id++ {
		payload := fmt.Sprintf("value-%04d-%s", id, strings.Repeat("x", 1024))
		if _, err := transaction.Exec("INSERT INTO records(id, payload) VALUES (?, ?)", id, payload); err != nil {
			_ = transaction.Rollback()
			return fmt.Errorf("insert conformance row: %w", err)
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit conformance rows: %w", err)
	}
	return nil
}

func createCaptureChain(sourcePath, repositoryPath string) error {
	if err := createDatabase(sourcePath, 4096); err != nil {
		return err
	}
	if _, err := replication.Snapshot(context.Background(), sourcePath, repositoryPath); err != nil {
		return fmt.Errorf("publish capture-chain base snapshot: %w", err)
	}
	database, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		return fmt.Errorf("open capture-chain source: %w", err)
	}
	database.SetMaxOpenConns(1)
	defer database.Close()
	if _, err := database.Exec("PRAGMA wal_autocheckpoint=0"); err != nil {
		return fmt.Errorf("disable capture-chain autocheckpoint: %w", err)
	}
	if _, err := database.Exec("PRAGMA synchronous=FULL"); err != nil {
		return fmt.Errorf("set capture-chain synchronous mode: %w", err)
	}
	for index, id := range []int{1, 2} {
		result, err := database.Exec("UPDATE records SET payload = substr(payload, 1, length(payload) - 1) || 'y' WHERE id = ?", id)
		if err != nil {
			return fmt.Errorf("commit capture-chain source change %d: %w", id, err)
		}
		updatedRows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect capture-chain change %d: %w", id, err)
		}
		if updatedRows != 1 {
			return fmt.Errorf("capture-chain change updated %d rows, want 1", updatedRows)
		}
		captured, err := capture.CaptureOnce(context.Background(), sourcePath, repositoryPath, 1_700_000_000_001+int64(index), nil)
		if err != nil {
			return fmt.Errorf("capture source transaction %d: %w", index+1, err)
		}
		if captured.WALFrames == 0 || len(captured.Transactions) != 1 || captured.State.Database.TXID != uint64(index+2) {
			return fmt.Errorf("capture transaction %d observed %d WAL frames, produced %d LTX files at TXID %d", index+1, captured.WALFrames, len(captured.Transactions), captured.State.Database.TXID)
		}
	}
	var busy, logFrames, checkpointed int
	if err := database.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed); err != nil {
		return fmt.Errorf("checkpoint capture-chain source after publication: %w", err)
	}
	if busy != 0 || (logFrames >= 0 && checkpointed != logFrames) {
		return fmt.Errorf("capture-chain source checkpoint incomplete: busy=%d log=%d checkpointed=%d", busy, logFrames, checkpointed)
	}
	return verifyCaptureChain(sourcePath)
}

func verifyCaptureChain(path string) error {
	if err := sqliteadapter.VerifyDatabase(context.Background(), path); err != nil {
		return fmt.Errorf("verify capture-chain SQLite image: %w", err)
	}
	database, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?mode=ro")
	if err != nil {
		return fmt.Errorf("open capture-chain image: %w", err)
	}
	defer database.Close()
	var count, idSum, payloadBytes, updatedRows, unchangedRows int
	if err := database.QueryRow(`
		SELECT count(*), sum(id), sum(length(payload)),
		       sum(CASE WHEN id IN (1, 2) AND substr(payload, -1, 1) = 'y' THEN 1 ELSE 0 END),
		       sum(CASE WHEN id NOT IN (1, 2) AND substr(payload, -1, 1) = 'x' THEN 1 ELSE 0 END)
		FROM records
	`).Scan(&count, &idSum, &payloadBytes, &updatedRows, &unchangedRows); err != nil {
		return fmt.Errorf("read capture-chain logical contents: %w", err)
	}
	if count != 128 || idSum != 8256 || payloadBytes != 132480 || updatedRows != 2 || unchangedRows != 126 {
		return fmt.Errorf("unexpected capture-chain contents: count=%d id_sum=%d payload_bytes=%d updated_rows=%d unchanged_rows=%d", count, idSum, payloadBytes, updatedRows, unchangedRows)
	}
	return nil
}

func compareCaptureChain(sourcePath, restoredPath string) error {
	if err := verifyCaptureChain(sourcePath); err != nil {
		return fmt.Errorf("verify capture-chain source: %w", err)
	}
	if err := verifyCaptureChain(restoredPath); err != nil {
		return fmt.Errorf("verify capture-chain restore: %w", err)
	}
	source, err := sql.Open("sqlite", "file:"+filepath.ToSlash(sourcePath)+"?mode=ro")
	if err != nil {
		return fmt.Errorf("open capture-chain source for comparison: %w", err)
	}
	defer source.Close()
	restored, err := sql.Open("sqlite", "file:"+filepath.ToSlash(restoredPath)+"?mode=ro")
	if err != nil {
		return fmt.Errorf("open restored capture-chain image: %w", err)
	}
	defer restored.Close()
	sourceRows, err := source.Query("SELECT id, payload FROM records ORDER BY id")
	if err != nil {
		return fmt.Errorf("read capture-chain source rows: %w", err)
	}
	defer sourceRows.Close()
	restoredRows, err := restored.Query("SELECT id, payload FROM records ORDER BY id")
	if err != nil {
		return fmt.Errorf("read restored capture-chain rows: %w", err)
	}
	defer restoredRows.Close()
	for rowNumber := 1; ; rowNumber++ {
		sourceHasRow := sourceRows.Next()
		restoredHasRow := restoredRows.Next()
		if sourceHasRow != restoredHasRow {
			return fmt.Errorf("capture-chain row count differs at row %d", rowNumber)
		}
		if !sourceHasRow {
			break
		}
		var sourceID, restoredID int
		var sourcePayload, restoredPayload string
		if err := sourceRows.Scan(&sourceID, &sourcePayload); err != nil {
			return fmt.Errorf("scan capture-chain source row %d: %w", rowNumber, err)
		}
		if err := restoredRows.Scan(&restoredID, &restoredPayload); err != nil {
			return fmt.Errorf("scan restored capture-chain row %d: %w", rowNumber, err)
		}
		if sourceID != restoredID || sourcePayload != restoredPayload {
			return fmt.Errorf("capture-chain content differs at row %d (source id=%d, restored id=%d)", rowNumber, sourceID, restoredID)
		}
	}
	if err := sourceRows.Err(); err != nil {
		return fmt.Errorf("iterate capture-chain source rows: %w", err)
	}
	if err := restoredRows.Err(); err != nil {
		return fmt.Errorf("iterate restored capture-chain rows: %w", err)
	}
	return nil
}

func verifyDatabase(path string) error {
	if err := sqliteadapter.VerifyDatabase(context.Background(), path); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?mode=ro")
	if err != nil {
		return fmt.Errorf("open conformance database: %w", err)
	}
	defer db.Close()
	var count, idSum, payloadBytes int
	if err := db.QueryRow("SELECT count(*), sum(id), sum(length(payload)) FROM records").Scan(&count, &idSum, &payloadBytes); err != nil {
		return fmt.Errorf("read conformance database: %w", err)
	}
	if count != 128 || idSum != 8256 || payloadBytes != 132480 {
		return fmt.Errorf("unexpected restored logical contents: count=%d id_sum=%d payload_bytes=%d", count, idSum, payloadBytes)
	}
	return nil
}

func usageError() error {
	return errors.New("usage: conformance <create DATABASE [PAGE_SIZE] | snapshot DATABASE REPOSITORY | encode-snapshot-at DATABASE LTX TXID TIMESTAMP_MILLIS | create-delta LTX | create-capture-chain DATABASE REPOSITORY | restore REPOSITORY DESTINATION | decode-ltx LTX DESTINATION | verify DATABASE | verify-capture-chain DATABASE | compare-capture-chain SOURCE RESTORED>")
}
