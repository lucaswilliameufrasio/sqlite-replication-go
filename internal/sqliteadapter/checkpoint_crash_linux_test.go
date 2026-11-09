//go:build linux && (amd64 || arm64)

package sqliteadapter

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

// These Linux-only tests trace a child through a real FULL checkpoint and kill
// it after a successful write or sync of the main database file. They test
// process-crash recovery, not power loss or storage-device durability.
const checkpointCrashChildReady = "SQLITE_REPLICATION_CHECKPOINT_CHILD_READY"
const checkpointCrashBoundaryEnv = "SQLITE_REPLICATION_CHECKPOINT_CRASH_BOUNDARY"

type ptraceSyscallInfo struct {
	operation uint8
	padding   [3]byte
	arch      uint32
	pc        uint64
	sp        uint64
	data      [8]uint64
}

func TestCheckpointCrashDuringRealCheckpointIoRecovers(t *testing.T) {
	for _, boundary := range []string{"write", "sync"} {
		t.Run(boundary, func(t *testing.T) {
			runCheckpointCrashRecovery(t, boundary)
		})
	}
}

func TestCheckpointCrashTracerSurvivesConcurrentScheduling(t *testing.T) {
	for attempt := 0; attempt < 8; attempt++ {
		t.Run(strconv.Itoa(attempt), func(t *testing.T) {
			t.Parallel()
			runCheckpointCrashRecovery(t, "sync")
		})
	}
}

func runCheckpointCrashRecovery(t *testing.T, boundary string) {
	databasePath, err := filepath.Abs(filepath.Join(t.TempDir(), "checkpoint-crash.sqlite"))
	if err != nil {
		t.Fatalf("resolve checkpoint crash database path: %v", err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestCheckpointCrashChild$", "-test.v")
	command.Env = append(os.Environ(),
		"SQLITE_REPLICATION_CHECKPOINT_CRASH_CHILD=1",
		"SQLITE_REPLICATION_CHECKPOINT_CRASH_DATABASE="+databasePath,
		checkpointCrashBoundaryEnv+"="+boundary,
	)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatalf("create checkpoint child input pipe: %v", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("create checkpoint child output pipe: %v", err)
	}
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		t.Fatalf("start checkpoint child: %v", err)
	}
	defer stdin.Close()
	reaped := false
	t.Cleanup(func() {
		if !reaped {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	traceTID, err := waitForCheckpointChildReady(stdout)
	if err != nil {
		t.Fatalf("wait for checkpoint child readiness: %v", err)
	}

	status, err := killAfterCheckpointIoBoundary(traceTID, command.Process.Pid, databasePath, boundary, stdin)
	if err != nil {
		t.Fatalf("trace and interrupt checkpoint child: %v", err)
	}
	reaped = true
	if err := command.Process.Release(); err != nil {
		t.Fatalf("release checkpoint child process handle: %v", err)
	}
	if !status.Signaled() || status.Signal() != unix.SIGKILL {
		t.Fatalf("checkpoint child status = %v, want SIGKILL after database %s", status, boundary)
	}

	verifyCheckpointRecovery(t, databasePath)
}

func TestCheckpointCrashChild(t *testing.T) {
	if os.Getenv("SQLITE_REPLICATION_CHECKPOINT_CRASH_CHILD") != "1" {
		return
	}
	databasePath := os.Getenv("SQLITE_REPLICATION_CHECKPOINT_CRASH_DATABASE")
	boundary := os.Getenv(checkpointCrashBoundaryEnv)
	if boundary != "write" && boundary != "sync" {
		t.Fatalf("checkpoint crash boundary = %q, want write or sync", boundary)
	}
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open checkpoint child database: %v", err)
	}
	database.SetMaxOpenConns(1)
	for _, statement := range []string{
		"PRAGMA page_size=4096",
		"PRAGMA journal_mode=WAL",
		"PRAGMA wal_autocheckpoint=0",
		"PRAGMA synchronous=FULL",
		"CREATE TABLE records (id INTEGER PRIMARY KEY, payload BLOB NOT NULL)",
	} {
		if _, err := database.ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("initialize checkpoint child database with %q: %v", statement, err)
		}
	}
	transaction, err := database.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin checkpoint child data transaction: %v", err)
	}
	statement, err := transaction.PrepareContext(context.Background(), "INSERT INTO records(id, payload) VALUES (?, ?)")
	if err != nil {
		_ = transaction.Rollback()
		t.Fatalf("prepare checkpoint child data transaction: %v", err)
	}
	payload := make([]byte, 3000)
	for id := 1; id <= 512; id++ {
		if _, err := statement.ExecContext(context.Background(), id, payload); err != nil {
			_ = statement.Close()
			_ = transaction.Rollback()
			t.Fatalf("insert checkpoint child row %d: %v", id, err)
		}
	}
	if err := statement.Close(); err != nil {
		_ = transaction.Rollback()
		t.Fatalf("close checkpoint child insert statement: %v", err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit checkpoint child data: %v", err)
	}
	walInfo, err := os.Stat(databasePath + "-wal")
	if err != nil {
		t.Fatalf("stat checkpoint child WAL: %v", err)
	}
	if walInfo.Size() <= 32 {
		t.Fatalf("checkpoint child WAL size = %d, want committed frames", walInfo.Size())
	}
	runtime.LockOSThread()
	traceTID := unix.Gettid()
	if _, err := fmt.Fprintf(os.Stdout, "%s %d\n", checkpointCrashChildReady, traceTID); err != nil {
		t.Fatalf("signal checkpoint child readiness: %v", err)
	}
	var release [1]byte
	if _, err := io.ReadFull(os.Stdin, release[:]); err != nil {
		t.Fatalf("wait for parent to start syscall tracing: %v", err)
	}
	var busy, logFrames, checkpointed int
	if err := database.QueryRowContext(context.Background(), "PRAGMA wal_checkpoint(FULL)").Scan(&busy, &logFrames, &checkpointed); err != nil {
		t.Fatalf("run real FULL checkpoint in child: %v", err)
	}
	t.Fatalf("checkpoint completed before the parent interrupted database %s: busy=%d log=%d checkpointed=%d", boundary, busy, logFrames, checkpointed)
}

func waitForCheckpointChildReady(stdout io.Reader) (int, error) {
	ready := make(chan struct {
		tid int
		err error
	}, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) == 2 && fields[0] == checkpointCrashChildReady {
				tid, err := strconv.Atoi(fields[1])
				ready <- struct {
					tid int
					err error
				}{tid: tid, err: err}
				return
			}
		}
		if err := scanner.Err(); err != nil {
			ready <- struct {
				tid int
				err error
			}{err: err}
			return
		}
		ready <- struct {
			tid int
			err error
		}{err: io.EOF}
	}()
	select {
	case result := <-ready:
		return result.tid, result.err
	case <-time.After(15 * time.Second):
		return 0, fmt.Errorf("timed out waiting for readiness marker")
	}
}

func killAfterCheckpointIoBoundary(traceTID, processID int, databasePath, boundary string, stdin io.Writer) (unix.WaitStatus, error) {
	// Linux associates the tracee with the attaching OS thread, not merely the
	// Go process. Wait4 can park this goroutine, so pin the tracer too (the child
	// already pins its checkpoint thread). Otherwise migration causes ESRCH at
	// a valid syscall stop and invalidates the crash-boundary evidence.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := unix.PtraceAttach(traceTID); err != nil {
		return 0, fmt.Errorf("attach to checkpoint child thread: %w", err)
	}
	status, err := waitForTraceStatus(traceTID)
	if err != nil {
		return 0, err
	}
	if !status.Stopped() || status.StopSignal() != unix.SIGSTOP {
		return status, fmt.Errorf("checkpoint child initial status = %v, want SIGSTOP", status)
	}
	if err := unix.PtraceSetOptions(traceTID, unix.PTRACE_O_TRACESYSGOOD); err != nil {
		return status, fmt.Errorf("enable syscall-stop markers at status %v (signal %s): %w", status, status.StopSignal(), err)
	}
	if _, err := stdin.Write([]byte{1}); err != nil {
		return status, fmt.Errorf("release checkpoint child into FULL checkpoint: %w", err)
	}

	databaseWritePending := false
	signalToDeliver := 0
	for {
		if err := unix.PtraceSyscall(traceTID, signalToDeliver); err != nil {
			return status, fmt.Errorf("resume checkpoint child under syscall tracing: %w", err)
		}
		signalToDeliver = 0
		status, err = waitForTraceStatus(traceTID)
		if err != nil {
			return status, err
		}
		if status.Exited() || status.Signaled() {
			return status, fmt.Errorf("checkpoint child exited before a traced database write: %v", status)
		}
		if !status.Stopped() {
			return status, fmt.Errorf("unexpected checkpoint child status: %v", status)
		}
		stopSignal := status.StopSignal()
		if stopSignal == unix.Signal(int(unix.SIGTRAP)|0x80) {
			info, err := readPtraceSyscallInfo(traceTID)
			if err != nil {
				return status, err
			}
			switch info.operation {
			case 1:
				databaseWritePending = isCheckpointDatabaseIo(processID, databasePath, boundary, info)
			case 2:
				completed := int64(info.data[0]) > 0
				if boundary == "sync" {
					completed = int64(info.data[0]) == 0
				}
				if databaseWritePending && completed {
					if err := unix.PtraceSyscall(traceTID, int(unix.SIGKILL)); err != nil {
						return status, fmt.Errorf("kill checkpoint child after database write: %w", err)
					}
					return waitForTraceStatus(traceTID)
				}
				databaseWritePending = false
			}
			continue
		}
		signalToDeliver = int(stopSignal)
	}
}

func waitForTraceStatus(pid int) (unix.WaitStatus, error) {
	for {
		var status unix.WaitStatus
		waitedPID, err := unix.Wait4(pid, &status, unix.WALL, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("wait for checkpoint child: %w", err)
		}
		if waitedPID != pid {
			return status, fmt.Errorf("wait returned process %d, want %d", waitedPID, pid)
		}
		return status, nil
	}
}

func readPtraceSyscallInfo(pid int) (ptraceSyscallInfo, error) {
	var info ptraceSyscallInfo
	_, _, errno := unix.Syscall6(
		unix.SYS_PTRACE,
		uintptr(unix.PTRACE_GET_SYSCALL_INFO),
		uintptr(pid),
		unsafe.Sizeof(info),
		uintptr(unsafe.Pointer(&info)),
		0,
		0,
	)
	if errno != 0 {
		return info, fmt.Errorf("read checkpoint child syscall details: %w", errno)
	}
	return info, nil
}

func isCheckpointDatabaseIo(pid int, databasePath, boundary string, info ptraceSyscallInfo) bool {
	syscallNumber := info.data[0]
	switch boundary {
	case "write":
		if syscallNumber != uint64(unix.SYS_WRITE) && syscallNumber != uint64(unix.SYS_PWRITE64) {
			return false
		}
	case "sync":
		if syscallNumber != uint64(unix.SYS_FSYNC) && syscallNumber != uint64(unix.SYS_FDATASYNC) {
			return false
		}
	default:
		return false
	}
	fileDescriptorPath := filepath.Join("/proc", strconv.Itoa(pid), "fd", strconv.FormatUint(info.data[1], 10))
	openedPath, err := os.Readlink(fileDescriptorPath)
	if err != nil {
		return false
	}
	return filepath.Clean(strings.TrimSuffix(openedPath, " (deleted)")) == databasePath
}

func verifyCheckpointRecovery(t *testing.T, databasePath string) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("reopen database after checkpoint crash: %v", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close recovered checkpoint database: %v", err)
		}
	}()
	database.SetMaxOpenConns(1)
	var integrity string
	if err := database.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil {
		t.Fatalf("integrity_check after checkpoint crash: %v", err)
	}
	if integrity != "ok" {
		t.Fatalf("integrity_check after checkpoint crash = %q, want ok", integrity)
	}
	var rows, invalidPayloads int
	if err := database.QueryRow("SELECT count(*), count(*) FILTER (WHERE length(payload) != 3000) FROM records").Scan(&rows, &invalidPayloads); err != nil {
		t.Fatalf("read recovered checkpoint rows: %v", err)
	}
	if rows != 512 || invalidPayloads != 0 {
		t.Fatalf("recovered checkpoint rows=%d invalid_payloads=%d, want 512 and 0", rows, invalidPayloads)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close recovery verification database before checkpoint: %v", err)
	}

	// Do not let the pooled connection used by integrity_check or the row scan
	// retain a read mark while verifying that recovery can truncate the WAL.
	checkpointDatabase, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open database for recovery checkpoint: %v", err)
	}
	defer func() {
		if err := checkpointDatabase.Close(); err != nil {
			t.Errorf("close recovery checkpoint database: %v", err)
		}
	}()
	checkpointDatabase.SetMaxOpenConns(1)
	var busy, logFrames, checkpointed int
	if err := checkpointDatabase.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed); err != nil {
		t.Fatalf("finish checkpoint after crash recovery: %v", err)
	}
	if busy != 0 || (logFrames >= 0 && checkpointed != logFrames) {
		t.Fatalf("recovery checkpoint incomplete: busy=%d log=%d checkpointed=%d", busy, logFrames, checkpointed)
	}
}
