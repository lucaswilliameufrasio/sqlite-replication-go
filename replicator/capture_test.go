package replicator_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/repositorylock"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/spool"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/replicator"
	_ "modernc.org/sqlite"
)

func TestCaptureOnceRequiresStrictWriteGateAndSpoolsBeforeRelease(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "source.db")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(dir, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("create initial snapshot: %v", err)
	}

	if _, err := replicator.CaptureOnce(ctx, databasePath, repositoryRoot, nil); err == nil {
		t.Fatal("CaptureOnce accepted a missing strict write gate")
	}
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		if _, err := replicator.CaptureOnce(ctx, databasePath, repositoryRoot, func(context.Context) (func() error, error) {
			t.Fatal("unsupported platform acquired the write gate")
			return nil, nil
		}); err == nil {
			t.Fatal("CaptureOnce accepted an unsupported platform")
		}
		return
	}

	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "PRAGMA wal_autocheckpoint=0"); err != nil {
		t.Fatalf("disable test autocheckpoint: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO records(value) VALUES (?)", "captured"); err != nil {
		t.Fatalf("commit source change: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	gateAcquired := false
	gateReleased := false
	result, err := replicator.CaptureOnce(ctx, databasePath, repositoryRoot, func(context.Context) (func() error, error) {
		gateAcquired = true
		return func() error {
			watermark, err := spool.ReadWatermark(repositoryRoot)
			if err != nil {
				return err
			}
			if watermark.TXID != 2 {
				t.Errorf("write gate released at TXID %d, want 2", watermark.TXID)
			}
			gateReleased = true
			return nil
		}, nil
	})
	if err != nil {
		t.Fatalf("capture local WAL: %v", err)
	}
	if !gateAcquired || !gateReleased {
		t.Fatalf("write gate lifecycle: acquired=%t released=%t", gateAcquired, gateReleased)
	}
	if result.TXID != 2 || result.Transactions == 0 {
		t.Fatalf("capture result = %+v, want durable TXID 2 and a transaction", result)
	}
}

func TestCaptureOnceWithBudgetInvalidatesAndRestoresData(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("capture execution is intentionally limited to Linux amd64")
	}
	ctx := context.Background()
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "source.db")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(dir, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "INSERT INTO records(value) VALUES ('budgeted')"); err != nil {
		t.Fatal(err)
	}
	admission := replicator.NewAdmissionGate()
	if err := admission.Open(); err != nil {
		t.Fatal(err)
	}
	budget, err := replicator.NewConservativeBudget(admission, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.Reconcile(func() (uint64, error) { return 0, nil }); err != nil {
		t.Fatal(err)
	}
	result, err := replicator.CaptureOnceWithBudget(ctx, databasePath, repositoryRoot, func(context.Context) (func() error, error) {
		return func() error { return nil }, nil
	}, budget)
	if err != nil || result.TXID != 2 {
		t.Fatalf("capture result = %+v, error = %v", result, err)
	}
	if _, err := budget.TryReserve(1); err == nil {
		t.Fatal("capture did not invalidate occupancy")
	}
	restored := filepath.Join(dir, "restored.db")
	if err := replicator.RestoreToTarget(ctx, repositoryRoot, restored, replicator.RestoreTarget{TXID: 2}); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var count int
	if err := reader.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE value='budgeted'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("restored rows = %d, error = %v", count, err)
	}
}

func TestCaptureOnceWithBudgetFailureInvalidatesAfterRelease(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("capture execution is intentionally limited to Linux amd64")
	}
	admission := replicator.NewAdmissionGate()
	if err := admission.Open(); err != nil {
		t.Fatal(err)
	}
	budget, err := replicator.NewConservativeBudget(admission, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.Reconcile(func() (uint64, error) { return 0, nil }); err != nil {
		t.Fatal(err)
	}
	_, err = replicator.CaptureOnceWithBudget(context.Background(), "missing", filepath.Join(t.TempDir(), "missing"), func(context.Context) (func() error, error) {
		return func() error {
			return budget.Reconcile(func() (uint64, error) { return 0, nil })
		}, nil
	}, budget)
	if err == nil {
		t.Fatal("capture should fail")
	}
	if _, err := budget.TryReserve(1); err == nil {
		t.Fatal("failed capture did not invalidate release callback's measurement")
	}
}

func TestCaptureOnceWithBudgetRejectsActiveWriterThenRecovers(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("capture execution is intentionally limited to Linux amd64")
	}
	ctx := context.Background()
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "source.db")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(dir, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatal(err)
	}
	admission := replicator.NewAdmissionGate()
	if err := admission.Open(); err != nil {
		t.Fatal(err)
	}
	budget, err := replicator.NewConservativeBudget(admission, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.Reconcile(func() (uint64, error) { return 0, nil }); err != nil {
		t.Fatal(err)
	}
	reservation, err := budget.TryReserve(1)
	if err != nil {
		t.Fatal(err)
	}
	gate := func(context.Context) (func() error, error) { return func() error { return nil }, nil }
	if _, err := replicator.CaptureOnceWithBudget(ctx, databasePath, repositoryRoot, gate, budget); !errors.Is(err, replicator.ErrBackpressure) {
		t.Fatalf("capture with active reservation: %v", err)
	}
	if err := reservation.Settle(0, true); err == nil {
		t.Fatal("failed capture should leave occupancy unknown")
	}
	if err := budget.Reconcile(func() (uint64, error) { return 0, nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := replicator.CaptureOnceWithBudget(ctx, databasePath, repositoryRoot, gate, budget); err != nil {
		t.Fatalf("capture after drain: %v", err)
	}
	if _, err := budget.TryReserve(1); err == nil {
		t.Fatal("successful capture must invalidate accounting")
	}
}

func TestReservedBudgetRejectsBeforeCaptureAndRetries(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("capture execution is intentionally limited to Linux amd64")
	}
	ctx := context.Background()
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "source.db")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(dir, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "INSERT INTO records(value) VALUES ('reserved')"); err != nil {
		t.Fatal(err)
	}
	admission := replicator.NewAdmissionGate()
	budget, err := replicator.NewConservativeBudget(admission, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.Reconcile(func() (uint64, error) { return 5, nil }); err != nil {
		t.Fatal(err)
	}
	gate := func(context.Context) (func() error, error) { return func() error { return nil }, nil }
	if _, err := replicator.CaptureOnceWithReservedBudget(ctx, databasePath, repositoryRoot, gate, budget, 6); !errors.Is(err, replicator.ErrBackpressure) {
		t.Fatalf("capture without headroom = %v", err)
	}
	if _, err := spool.ReadWatermark(repositoryRoot); err == nil {
		t.Fatal("rejected capture advanced watermark")
	}
	if err := budget.Reconcile(func() (uint64, error) { return 4, nil }); err != nil {
		t.Fatal(err)
	}
	result, err := replicator.CaptureOnceWithReservedBudget(ctx, databasePath, repositoryRoot, gate, budget, 6)
	if err != nil || result.TXID != 2 {
		t.Fatalf("retry result = %+v, error = %v", result, err)
	}
	if err := admission.Open(); err != nil {
		t.Fatal(err)
	}
	if _, err := budget.TryReserve(1); err == nil {
		t.Fatal("capture should invalidate occupancy")
	}
	restored := filepath.Join(dir, "restored.db")
	if err := replicator.RestoreToTarget(ctx, repositoryRoot, restored, replicator.RestoreTarget{TXID: 2}); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var count int
	if err := reader.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE value='reserved'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("restored rows = %d, error = %v", count, err)
	}
}

func TestCaptureWithRecoveryImageLimitFailsAndRetries(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("incremental capture requires Linux amd64")
	}
	ctx := context.Background()
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "source.db")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(dir, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "INSERT INTO records(value) VALUES ('limited')"); err != nil {
		t.Fatal(err)
	}
	budget, err := replicator.NewConservativeBudget(replicator.NewAdmissionGate(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.Reconcile(func() (uint64, error) { return 0, nil }); err != nil {
		t.Fatal(err)
	}
	gate := func(context.Context) (func() error, error) { return func() error { return nil }, nil }
	if _, err := replicator.CaptureOnceWithRecoveryImageLimit(ctx, databasePath, repositoryRoot, gate, budget, 1, 1); err == nil {
		t.Fatal("expected recovery image limit failure")
	}
	if _, err := spool.ReadWatermark(repositoryRoot); err == nil {
		t.Fatal("failed capture advanced watermark")
	}
	if err := budget.Reconcile(func() (uint64, error) { return 0, nil }); err != nil {
		t.Fatal(err)
	}
	result, err := replicator.CaptureOnceWithRecoveryImageLimit(ctx, databasePath, repositoryRoot, gate, budget, 1, 1<<20)
	if err != nil || result.TXID != 2 {
		t.Fatalf("retry result = %+v, error = %v", result, err)
	}
	restored := filepath.Join(dir, "restored.db")
	if err := replicator.RestoreToTarget(ctx, repositoryRoot, restored, replicator.RestoreTarget{TXID: 2}); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var count int
	if err := reader.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE value='limited'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("restored rows = %d, error = %v", count, err)
	}
}

func TestCaptureWithImageLimitsBackupFailureAndRetry(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("incremental capture requires Linux amd64")
	}
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "source.sqlite")
	createSourceDatabase(t, source)
	repository := filepath.Join(dir, "repository")
	if _, err := replication.Snapshot(ctx, source, repository); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "INSERT INTO records(value) VALUES (zeroblob(131072))"); err != nil {
		t.Fatal(err)
	}
	budget, err := replicator.NewConservativeBudget(replicator.NewAdmissionGate(), 10)
	if err != nil {
		t.Fatal(err)
	}
	measure := func() (uint64, error) { return 0, nil }
	if err := budget.Reconcile(measure); err != nil {
		t.Fatal(err)
	}
	gate := func(context.Context) (func() error, error) { return func() error { return nil }, nil }
	_, err = replicator.CaptureOnceWithImageLimits(ctx, source, repository, gate, budget, 1, 1<<20, 8192)
	if err == nil || !strings.Contains(err.Error(), "database or disk is full") {
		t.Fatalf("expected SQLite full error: %v", err)
	}
	watermark, err := spool.ReadWatermark(repository)
	if err != nil || watermark.TXID != 1 {
		t.Fatalf("watermark after failure: %+v, %v", watermark, err)
	}
	stages, err := filepath.Glob(filepath.Join(repository, ".staging", ".capture-source-*.sqlite*"))
	if err != nil || len(stages) != 0 {
		t.Fatalf("staging after failure: %v, %v", stages, err)
	}
	if _, err := budget.TryReserve(1); err == nil {
		t.Fatal("failure did not invalidate budget")
	}
	if err := budget.Reconcile(measure); err != nil {
		t.Fatal(err)
	}
	result, err := replicator.CaptureOnceWithImageLimits(ctx, source, repository, gate, budget, 1, 1<<20, 1<<20)
	if err != nil || result.TXID != 2 {
		t.Fatalf("retry: %+v, %v", result, err)
	}
	restored := filepath.Join(dir, "restored.sqlite")
	if err := replicator.RestoreToTarget(ctx, repository, restored, replicator.RestoreTarget{TXID: 2}); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var count int
	if err := reader.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE length(value)=131072").Scan(&count); err != nil || count != 1 {
		t.Fatalf("restored rows: %d, %v", count, err)
	}
}

func TestDerivedReservationWithMeasuredSpoolAndRetry(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("incremental capture requires Linux amd64")
	}
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "source.sqlite")
	createSourceDatabase(t, source)
	repository := filepath.Join(dir, "repository")
	if _, err := replication.Snapshot(ctx, source, repository); err != nil {
		t.Fatal(err)
	}
	staging := filepath.Join(repository, ".staging")
	if err := os.MkdirAll(staging, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "residual-test"), []byte("backup residue"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, "ltx", "residual.stage"), []byte("ltx residue"), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "INSERT INTO records(value) VALUES ('derived retry')"); err != nil {
		t.Fatal(err)
	}
	measure := func() (uint64, error) {
		return replicator.MeasureRepositoryLogicalBytes(repository)
	}
	baseline, err := measure()
	if err != nil || baseline == 0 {
		t.Fatalf("measure baseline: %d, %v", baseline, err)
	}
	limits := replicator.CaptureLimits{RecoveryImage: 1 << 20, BackupImage: 1 << 20, LTXBatch: 1 << 20, Spool: 4 << 20}
	gate := func(context.Context) (func() error, error) { return func() error { return nil }, nil }
	held, err := repositorylock.AcquireExclusive(repository)
	if err != nil {
		t.Fatal(err)
	}
	blockedBudget, err := replicator.NewConservativeBudget(replicator.NewAdmissionGate(), baseline+(3<<20))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replicator.CaptureOnceWithMeasuredReservation(ctx, source, repository, gate, blockedBudget, limits); err == nil {
		t.Fatal("capture ignored held repository lock")
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	budget, err := replicator.NewConservativeBudget(replicator.NewAdmissionGate(), baseline+(3<<20)-1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replicator.CaptureOnceWithMeasuredReservation(ctx, source, repository, gate, budget, limits); !errors.Is(err, replicator.ErrBackpressure) {
		t.Fatalf("expected rejection: %v", err)
	}
	if _, err := spool.ReadWatermark(repository); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("watermark created before capture: %v", err)
	}
	budget, err = replicator.NewConservativeBudget(replicator.NewAdmissionGate(), baseline+(3<<20))
	if err != nil {
		t.Fatal(err)
	}
	result, err := replicator.CaptureOnceWithMeasuredReservation(ctx, source, repository, gate, budget, limits)
	if err != nil || result.TXID != 2 {
		t.Fatalf("retry: %+v, %v", result, err)
	}
	restored := filepath.Join(dir, "restored.sqlite")
	if err := replicator.RestoreToTarget(ctx, repository, restored, replicator.RestoreTarget{TXID: 2}); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var count int
	if err := reader.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE value='derived retry'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("restore: %d, %v", count, err)
	}
}

func TestDerivedReservationRejectsOverflowAndInsufficientBudget(t *testing.T) {
	gateCalled := false
	gate := func(context.Context) (func() error, error) {
		gateCalled = true
		return func() error { return nil }, nil
	}
	budget, err := replicator.NewConservativeBudget(replicator.NewAdmissionGate(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.Reconcile(func() (uint64, error) { return 5, nil }); err != nil {
		t.Fatal(err)
	}
	limits := replicator.CaptureLimits{RecoveryImage: ^uint64(0), BackupImage: 1, LTXBatch: 1, Spool: 1}
	if _, err := replicator.CaptureOnceWithDerivedReservation(context.Background(), "missing", "missing", gate, budget, limits); err == nil || !strings.Contains(err.Error(), "overflow") || gateCalled {
		t.Fatalf("overflow: %v, gate called: %t", err, gateCalled)
	}
	limits = replicator.CaptureLimits{RecoveryImage: 2, BackupImage: 2, LTXBatch: 2, Spool: 10}
	if _, err := replicator.CaptureOnceWithDerivedReservation(context.Background(), "missing", "missing", gate, budget, limits); !errors.Is(err, replicator.ErrBackpressure) {
		t.Fatalf("expected insufficient budget: %v", err)
	}
}

func TestCaptureWithSpoolLimitFailureAndRetry(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("incremental capture requires Linux amd64")
	}
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "source.sqlite")
	createSourceDatabase(t, source)
	repository := filepath.Join(dir, "repository")
	if _, err := replication.Snapshot(ctx, source, repository); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "INSERT INTO records(value) VALUES ('spool retry')"); err != nil {
		t.Fatal(err)
	}
	budget, err := replicator.NewConservativeBudget(replicator.NewAdmissionGate(), 10)
	if err != nil {
		t.Fatal(err)
	}
	measure := func() (uint64, error) { return 0, nil }
	if err := budget.Reconcile(measure); err != nil {
		t.Fatal(err)
	}
	gate := func(context.Context) (func() error, error) { return func() error { return nil }, nil }
	limits := replicator.CaptureLimits{RecoveryImage: 1 << 20, BackupImage: 1 << 20, LTXBatch: 1 << 20, Spool: 1}
	batchLimits := limits
	batchLimits.LTXBatch = 1
	batchLimits.Spool = 1 << 20
	if _, err := replicator.CaptureOnceWithSpoolLimit(ctx, source, repository, gate, budget, 1, batchLimits); err == nil || !strings.Contains(err.Error(), "LTX batch size limit exceeded") {
		t.Fatalf("expected independent batch limit error: %v", err)
	}
	if err := budget.Reconcile(measure); err != nil {
		t.Fatal(err)
	}
	_, err = replicator.CaptureOnceWithSpoolLimit(ctx, source, repository, gate, budget, 1, limits)
	if err == nil || !strings.Contains(err.Error(), "spool size limit exceeded") {
		t.Fatalf("expected spool limit error: %v", err)
	}
	watermark, err := spool.ReadWatermark(repository)
	if err != nil || watermark.TXID != 1 {
		t.Fatalf("watermark: %+v, %v", watermark, err)
	}
	if err := budget.Reconcile(measure); err != nil {
		t.Fatal(err)
	}
	limits.Spool = 1 << 20
	result, err := replicator.CaptureOnceWithSpoolLimit(ctx, source, repository, gate, budget, 1, limits)
	if err != nil || result.TXID != 2 {
		t.Fatalf("retry: %+v, %v", result, err)
	}
	restored := filepath.Join(dir, "restored.sqlite")
	if err := replicator.RestoreToTarget(ctx, repository, restored, replicator.RestoreTarget{TXID: 2}); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var count int
	if err := reader.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE value='spool retry'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("restore: %d, %v", count, err)
	}
}

func TestCaptureWithBatchLimitFailureAndRetry(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("incremental capture requires Linux amd64")
	}
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "source.sqlite")
	createSourceDatabase(t, source)
	repository := filepath.Join(dir, "repository")
	if _, err := replication.Snapshot(ctx, source, repository); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "INSERT INTO records(value) VALUES ('batch retry')"); err != nil {
		t.Fatal(err)
	}
	budget, err := replicator.NewConservativeBudget(replicator.NewAdmissionGate(), 10)
	if err != nil {
		t.Fatal(err)
	}
	measure := func() (uint64, error) { return 0, nil }
	if err := budget.Reconcile(measure); err != nil {
		t.Fatal(err)
	}
	gate := func(context.Context) (func() error, error) { return func() error { return nil }, nil }
	_, err = replicator.CaptureOnceWithBatchLimit(ctx, source, repository, gate, budget, 1, 1<<20, 1<<20, 1)
	if err == nil || !strings.Contains(err.Error(), "LTX batch size limit exceeded") {
		t.Fatalf("expected batch limit error: %v", err)
	}
	watermark, err := spool.ReadWatermark(repository)
	if err != nil || watermark.TXID != 1 {
		t.Fatalf("watermark after failure: %+v, %v", watermark, err)
	}
	if err := budget.Reconcile(measure); err != nil {
		t.Fatal(err)
	}
	result, err := replicator.CaptureOnceWithBatchLimit(ctx, source, repository, gate, budget, 1, 1<<20, 1<<20, 1<<20)
	if err != nil || result.TXID != 2 {
		t.Fatalf("retry: %+v, %v", result, err)
	}
	restored := filepath.Join(dir, "restored.sqlite")
	if err := replicator.RestoreToTarget(ctx, repository, restored, replicator.RestoreTarget{TXID: 2}); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var count int
	if err := reader.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE value='batch retry'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("restore: %d, %v", count, err)
	}
}

func TestCaptureOnceReleasesWriteGateWhenCaptureFails(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("capture execution is intentionally limited to Linux amd64")
	}
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "missing.db")
	repositoryRoot := filepath.Join(dir, "missing-repository")
	gateReleased := false
	_, err := replicator.CaptureOnce(context.Background(), databasePath, repositoryRoot, func(context.Context) (func() error, error) {
		return func() error {
			gateReleased = true
			return nil
		}, nil
	})
	if err == nil {
		t.Fatal("capture succeeded without an initialized repository")
	}
	if !gateReleased {
		t.Fatal("write gate was not released after capture failure")
	}
}

func TestCaptureOnceCombinesCaptureAndGateReleaseErrors(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("capture execution is intentionally limited to Linux amd64")
	}
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "missing.db")
	repositoryRoot := filepath.Join(dir, "missing-repository")
	gateErr := errors.New("gate release failed")
	_, err := replicator.CaptureOnce(context.Background(), databasePath, repositoryRoot, func(context.Context) (func() error, error) {
		return func() error {
			return gateErr
		}, nil
	})
	if !errors.Is(err, gateErr) {
		t.Fatalf("CaptureOnce error = %v, want gate release error", err)
	}
}

func TestCaptureOnceReportsGateReleaseErrorAfterDurableCapture(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("capture execution is intentionally limited to Linux amd64")
	}
	ctx := context.Background()
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "source.db")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(dir, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("create initial snapshot: %v", err)
	}
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO records(value) VALUES (?)", "captured"); err != nil {
		t.Fatalf("commit source change: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	releaseErr := errors.New("release failed after local durability")
	_, err = replicator.CaptureOnce(ctx, databasePath, repositoryRoot, func(context.Context) (func() error, error) {
		return func() error {
			return releaseErr
		}, nil
	})
	if !errors.Is(err, releaseErr) {
		t.Fatalf("CaptureOnce error = %v, want release error", err)
	}
	watermark, err := spool.ReadWatermark(repositoryRoot)
	if err != nil {
		t.Fatalf("read durable watermark after gate release error: %v", err)
	}
	if watermark.TXID != 2 {
		t.Fatalf("durable watermark TXID = %d, want 2", watermark.TXID)
	}
}

func createSourceDatabase(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}
	if _, err := db.Exec("INSERT INTO records(value) VALUES (?)", "base"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
