package sqliteadapter

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	sqlite "modernc.org/sqlite"
)

const minimumSQLiteVersion = "3.51.3"

type onlineBackupper interface {
	NewBackup(dstURI string) (*sqlite.Backup, error)
}

type CaptureObservation struct {
	Busy         int
	LogFrames    int
	Checkpointed int
}

type StableWAL struct {
	Bytes       []byte
	Observation CaptureObservation
}

// CaptureLease pins one read snapshot and holds SQLite's writer reservation.
// It does not make arbitrary external checkpoint behavior safe by itself.
type CaptureLease struct {
	sourcePath string
	writerDB   *sql.DB
	writer     *sql.Conn
	readerDB   *sql.DB
	reader     *sql.Conn
	closed     bool
}

// BeginCaptureLease opens dedicated, explicitly configured SQLite connections,
// obtains BEGIN IMMEDIATE, then pins a read transaction on a separate connection.
func BeginCaptureLease(ctx context.Context, sourcePath string) (*CaptureLease, error) {
	writerDB, writer, err := openManagedCaptureConnection(ctx, sourcePath)
	if err != nil {
		return nil, fmt.Errorf("open capture writer connection: %w", err)
	}
	lease := &CaptureLease{sourcePath: sourcePath, writerDB: writerDB, writer: writer}
	if _, err := writer.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		_ = lease.Close()
		return nil, fmt.Errorf("acquire SQLite writer guard: %w", err)
	}

	readerDB, reader, err := openManagedCaptureConnection(ctx, sourcePath)
	if err != nil {
		_ = lease.Close()
		return nil, fmt.Errorf("open capture reader connection: %w", err)
	}
	lease.readerDB = readerDB
	lease.reader = reader
	if _, err := reader.ExecContext(ctx, "BEGIN"); err != nil {
		_ = lease.Close()
		return nil, fmt.Errorf("begin SQLite capture read transaction: %w", err)
	}
	var schemaEntries int64
	if err := reader.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema").Scan(&schemaEntries); err != nil {
		_ = lease.Close()
		return nil, fmt.Errorf("pin SQLite capture read snapshot: %w", err)
	}
	_ = schemaEntries
	return lease, nil
}

// ReadStableWAL copies the sidecar while writer/read locks remain held and
// brackets the file read with NOOP observations. NOOP is not the locking proof.
func (lease *CaptureLease) ReadStableWAL(ctx context.Context) (StableWAL, error) {
	if lease == nil || lease.closed {
		return StableWAL{}, fmt.Errorf("SQLite capture lease is closed")
	}
	before, err := observeCheckpoint(ctx, lease.sourcePath)
	if err != nil {
		return StableWAL{}, err
	}
	walPath := lease.sourcePath + "-wal"
	content, walFileInfo, walExists, err := readStableWALFile(walPath, nil)
	if err != nil {
		return StableWAL{}, fmt.Errorf("read SQLite WAL for capture: %w", err)
	}
	after, err := observeCheckpoint(ctx, lease.sourcePath)
	if err != nil {
		return StableWAL{}, err
	}
	if before.LogFrames != after.LogFrames || before.Checkpointed != after.Checkpointed {
		return StableWAL{}, fmt.Errorf("SQLite WAL checkpoint observation changed during capture")
	}
	// The ctime comparison is scoped to the file read; recheck only path identity
	// after the second SQLite observation.
	if err := verifyStableWALPath(walPath, walFileInfo, walExists, false); err != nil {
		return StableWAL{}, err
	}
	if after.LogFrames > 0 && !walExists {
		return StableWAL{}, fmt.Errorf("SQLite reports WAL frames but the WAL sidecar is absent")
	}
	return StableWAL{Bytes: content, Observation: after}, nil
}

func readStableWALFile(walPath string, afterOpen func() error) ([]byte, os.FileInfo, bool, error) {
	file, err := os.Open(walPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}

	beforeInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, false, fmt.Errorf("stat SQLite WAL before capture: %w", err)
	}
	beforeChangeTime, err := fileChangeTime(beforeInfo)
	if err != nil {
		_ = file.Close()
		return nil, nil, false, fmt.Errorf("read SQLite WAL change time before capture: %w", err)
	}
	if afterOpen != nil {
		if err := afterOpen(); err != nil {
			_ = file.Close()
			return nil, nil, false, err
		}
	}

	content, readErr := io.ReadAll(file)
	afterInfo, statErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil {
		return nil, nil, false, fmt.Errorf("read SQLite WAL: %w", readErr)
	}
	if statErr != nil {
		return nil, nil, false, fmt.Errorf("stat SQLite WAL after capture: %w", statErr)
	}
	if closeErr != nil {
		return nil, nil, false, fmt.Errorf("close captured SQLite WAL: %w", closeErr)
	}
	afterChangeTime, err := fileChangeTime(afterInfo)
	if err != nil {
		return nil, nil, false, fmt.Errorf("read SQLite WAL change time after capture: %w", err)
	}
	if beforeInfo.Size() != afterInfo.Size() ||
		!beforeInfo.ModTime().Equal(afterInfo.ModTime()) ||
		int64(len(content)) != afterInfo.Size() {
		return nil, nil, false, fmt.Errorf("SQLite WAL changed while it was being read")
	}
	if beforeChangeTime != afterChangeTime {
		return nil, nil, false, fmt.Errorf("SQLite WAL change time changed while it was being read")
	}
	if err := verifyStableWALPath(walPath, beforeInfo, true, true); err != nil {
		return nil, nil, false, err
	}
	return content, beforeInfo, true, nil
}

func verifyStableWALPath(walPath string, expectedInfo os.FileInfo, expectedPresent, verifyChangeTime bool) error {
	currentInfo, err := os.Stat(walPath)
	if !expectedPresent {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("stat SQLite WAL after capture: %w", err)
		}
		return fmt.Errorf("SQLite WAL appeared while it was being read")
	}
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("SQLite WAL path disappeared while it was being read")
	}
	if err != nil {
		return fmt.Errorf("stat SQLite WAL after capture: %w", err)
	}
	if !os.SameFile(expectedInfo, currentInfo) {
		return fmt.Errorf("SQLite WAL path changed while it was being read")
	}
	if !verifyChangeTime {
		return nil
	}
	expectedChangeTime, err := fileChangeTime(expectedInfo)
	if err != nil {
		return fmt.Errorf("read SQLite WAL change time before path verification: %w", err)
	}
	currentChangeTime, err := fileChangeTime(currentInfo)
	if err != nil {
		return fmt.Errorf("read SQLite WAL change time after path verification: %w", err)
	}
	if expectedChangeTime != currentChangeTime {
		return fmt.Errorf("SQLite WAL change time changed after it was read")
	}
	return nil
}

// Close releases the reader pin before the writer guard and closes both pools.
func (lease *CaptureLease) Close() error {
	return lease.closeWithHook(nil)
}

func (lease *CaptureLease) closeWithHook(afterReaderRelease func()) error {
	if lease == nil || lease.closed {
		return nil
	}
	lease.closed = true
	var failures []error
	if lease.reader != nil {
		if _, err := lease.reader.ExecContext(context.Background(), "ROLLBACK"); err != nil {
			failures = append(failures, fmt.Errorf("release SQLite read pin: %w", err))
		}
		if err := lease.reader.Close(); err != nil {
			failures = append(failures, fmt.Errorf("close SQLite reader connection: %w", err))
		}
	}
	if afterReaderRelease != nil {
		afterReaderRelease()
	}
	if lease.writer != nil {
		if _, err := lease.writer.ExecContext(context.Background(), "ROLLBACK"); err != nil {
			failures = append(failures, fmt.Errorf("release SQLite writer guard: %w", err))
		}
		if err := lease.writer.Close(); err != nil {
			failures = append(failures, fmt.Errorf("close SQLite writer connection: %w", err))
		}
	}
	if lease.readerDB != nil {
		if err := lease.readerDB.Close(); err != nil {
			failures = append(failures, fmt.Errorf("close SQLite reader pool: %w", err))
		}
	}
	if lease.writerDB != nil {
		if err := lease.writerDB.Close(); err != nil {
			failures = append(failures, fmt.Errorf("close SQLite writer pool: %w", err))
		}
	}
	return errors.Join(failures...)
}

func openManagedCaptureConnection(ctx context.Context, path string) (*sql.DB, *sql.Conn, error) {
	db, err := sql.Open("sqlite", sqliteURI(path))
	if err != nil {
		return nil, nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	cleanup := func(cause error) (*sql.DB, *sql.Conn, error) {
		_ = conn.Close()
		_ = db.Close()
		return nil, nil, cause
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA busy_timeout=0; PRAGMA wal_autocheckpoint=0;"); err != nil {
		return cleanup(fmt.Errorf("configure managed SQLite connection: %w", err))
	}
	var autocheckpoint int
	if err := conn.QueryRowContext(ctx, "PRAGMA wal_autocheckpoint").Scan(&autocheckpoint); err != nil {
		return cleanup(fmt.Errorf("verify WAL autocheckpoint configuration: %w", err))
	}
	if autocheckpoint != 0 {
		return cleanup(fmt.Errorf("managed SQLite connection has autocheckpoint=%d", autocheckpoint))
	}
	if err := validateRuntimeAndJournalMode(ctx, conn); err != nil {
		return cleanup(err)
	}
	return db, conn, nil
}

func observeCheckpoint(ctx context.Context, path string) (CaptureObservation, error) {
	db, connection, err := openManagedCaptureConnection(ctx, path)
	if err != nil {
		return CaptureObservation{}, fmt.Errorf("open SQLite checkpoint observer: %w", err)
	}
	defer db.Close()
	defer connection.Close()
	var observation CaptureObservation
	if err := connection.QueryRowContext(ctx, "PRAGMA wal_checkpoint(NOOP)").Scan(
		&observation.Busy,
		&observation.LogFrames,
		&observation.Checkpointed,
	); err != nil {
		return CaptureObservation{}, fmt.Errorf("observe SQLite WAL checkpoint boundary: %w", err)
	}
	if observation.Busy != 0 {
		return CaptureObservation{}, fmt.Errorf("SQLite NOOP checkpoint reported busy=%d", observation.Busy)
	}
	return observation, nil
}

// CreateOnlineBackup creates a consistent SQLite image using the runtime's
// Online Backup API. The source must be in WAL mode and use the modernc adapter.
func CreateOnlineBackup(ctx context.Context, sourcePath, destinationPath string) error {
	return createOnlineBackup(ctx, sourcePath, destinationPath, 0)
}

// CreateOnlineBackupWithImageLimit bounds the SQLite destination main image
// using max_page_count. It does not bound journals or reserve physical blocks.
// The destination must be new or an empty regular file; callers remove it on error.
func CreateOnlineBackupWithImageLimit(ctx context.Context, sourcePath, destinationPath string, maxImageBytes uint64) error {
	if maxImageBytes == 0 {
		return fmt.Errorf("backup image limit must be nonzero")
	}
	return createOnlineBackup(ctx, sourcePath, destinationPath, maxImageBytes)
}

func createOnlineBackup(ctx context.Context, sourcePath, destinationPath string, maxImageBytes uint64) error {
	db, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		return fmt.Errorf("open source database: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire dedicated SQLite connection: %w", err)
	}
	defer conn.Close()
	if err := validateRuntimeAndJournalMode(ctx, conn); err != nil {
		return err
	}
	destinationURI := destinationPath
	if maxImageBytes != 0 {
		info, err := os.Lstat(destinationPath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err == nil && (!info.Mode().IsRegular() || info.Size() != 0) {
			return fmt.Errorf("bounded backup destination must be an empty regular file")
		}
		var pageSize uint64
		if err := conn.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize); err != nil {
			return fmt.Errorf("read SQLite page size: %w", err)
		}
		if pageSize == 0 || maxImageBytes/pageSize == 0 {
			return fmt.Errorf("backup image limit smaller than page size")
		}
		pages := maxImageBytes / pageSize
		if pages > 4294967294 {
			pages = 4294967294
		}
		absolute, err := filepath.Abs(destinationPath)
		if err != nil {
			return err
		}
		destination := url.URL{Scheme: "file", Path: filepath.ToSlash(absolute)}
		query := destination.Query()
		query.Add("_pragma", fmt.Sprintf("page_size(%d)", pageSize))
		query.Add("_pragma", fmt.Sprintf("max_page_count(%d)", pages))
		destination.RawQuery = query.Encode()
		destinationURI = destination.String()
	}

	return conn.Raw(func(raw any) error {
		source, ok := raw.(onlineBackupper)
		if !ok {
			return fmt.Errorf("SQLite driver does not expose the approved Online Backup API")
		}
		backup, err := source.NewBackup(destinationURI)
		if err != nil {
			return fmt.Errorf("initialize SQLite Online Backup API: %w", err)
		}
		finished := false
		defer func() {
			if !finished {
				_ = backup.Finish()
			}
		}()

		backoff := 10 * time.Millisecond
		stepPages := int32(1)
		for more := true; more; {
			if err := ctx.Err(); err != nil {
				return err
			}
			more, err = backup.Step(stepPages)
			if err != nil {
				if !isBusyOrLocked(err) {
					return fmt.Errorf("copy SQLite snapshot pages: %w", err)
				}
				if err := sleepContext(ctx, backoff); err != nil {
					return err
				}
				if backoff < 250*time.Millisecond {
					backoff *= 2
				}
				continue
			}
			backoff = 10 * time.Millisecond
			remaining := backup.Remaining()
			if remaining > 0 {
				if remaining > 256 {
					remaining = 256
				}
				stepPages = int32(remaining)
			} else {
				stepPages = 1
			}
		}
		if err := backup.Finish(); err != nil {
			finished = true
			return fmt.Errorf("finish SQLite Online Backup API: %w", err)
		}
		finished = true
		return nil
	})
}

// VerifyDatabase checks the SQLite runtime version and runs quick_check and
// integrity_check against an offline database image.
func VerifyDatabase(ctx context.Context, path string) error {
	db, err := sql.Open("sqlite", sqliteURI(path)+"?mode=ro")
	if err != nil {
		return fmt.Errorf("open restored database: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire restored database connection: %w", err)
	}
	defer conn.Close()
	var version string
	if err := conn.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
		return fmt.Errorf("read SQLite runtime version: %w", err)
	}
	if !versionAtLeast(version, minimumSQLiteVersion) {
		return fmt.Errorf("SQLite %s is unsupported; require >= %s", version, minimumSQLiteVersion)
	}
	if err := checkPragma(ctx, conn, "quick_check"); err != nil {
		return err
	}
	if err := checkPragma(ctx, conn, "integrity_check"); err != nil {
		return err
	}
	return nil
}

func validateRuntimeAndJournalMode(ctx context.Context, conn *sql.Conn) error {
	var version string
	if err := conn.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
		return fmt.Errorf("read SQLite runtime version: %w", err)
	}
	if !versionAtLeast(version, minimumSQLiteVersion) {
		return fmt.Errorf("SQLite %s is unsupported; require >= %s", version, minimumSQLiteVersion)
	}
	var journalMode string
	if err := conn.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journalMode); err != nil {
		return fmt.Errorf("read SQLite journal mode: %w", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		return fmt.Errorf("SQLite journal_mode must be WAL, got %q", journalMode)
	}
	return nil
}

func checkPragma(ctx context.Context, conn *sql.Conn, pragma string) error {
	rows, err := conn.QueryContext(ctx, "PRAGMA "+pragma)
	if err != nil {
		return fmt.Errorf("run SQLite %s: %w", pragma, err)
	}
	defer rows.Close()
	seen := false
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return fmt.Errorf("read SQLite %s result: %w", pragma, err)
		}
		seen = true
		if !strings.EqualFold(result, "ok") {
			return fmt.Errorf("SQLite %s failed: %s", pragma, result)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read SQLite %s results: %w", pragma, err)
	}
	if !seen {
		return fmt.Errorf("SQLite %s returned no result", pragma)
	}
	return nil
}

func isBusyOrLocked(err error) bool {
	var coded interface{ Code() int }
	if !errors.As(err, &coded) {
		return false
	}
	baseCode := coded.Code() & 0xff
	return baseCode == 5 || baseCode == 6
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func versionAtLeast(version, minimum string) bool {
	current, err := parseVersion(version)
	if err != nil {
		return false
	}
	required, _ := parseVersion(minimum)
	for i := range current {
		if current[i] > required[i] {
			return true
		}
		if current[i] < required[i] {
			return false
		}
	}
	return true
}

func parseVersion(version string) ([3]int, error) {
	var parsed [3]int
	parts := strings.Split(version, ".")
	if len(parts) != len(parsed) {
		return parsed, fmt.Errorf("invalid SQLite version %q", version)
	}
	for i, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 {
			return parsed, fmt.Errorf("invalid SQLite version %q", version)
		}
		parsed[i] = value
	}
	return parsed, nil
}

func sqliteURI(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return "file:" + filepath.ToSlash(abs)
}

func SyncFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
