package replicator_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	replication "github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/replicator"
)

// This single-writer gate is a test reference, not a production byte quota.
type cooperativeTestGate struct {
	mu           sync.Mutex
	gate         *replicator.AdmissionGate
	closed       chan struct{}
	cancelOnOpen bool
}

func (g *cooperativeTestGate) setAdmission(ctx context.Context, enabled bool, _ replicator.Limits) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if enabled {
		if err := ctx.Err(); err != nil {
			return err
		}
		if g.cancelOnOpen {
			g.gate.Cancel()
		}
		if err := g.gate.Open(); err != nil {
			return err
		}
		g.closed = make(chan struct{})
	} else {
		g.gate.Close()
		if g.closed != nil {
			close(g.closed)
			g.closed = nil
		}
	}
	return nil
}

func (g *cooperativeTestGate) accept() (func(), error) {
	return g.gate.TryAccept()
}

func (g *cooperativeTestGate) drain(ctx context.Context) error {
	return g.gate.Drain(ctx)
}

func TestCooperativeBudgetCoordinatorRealCaptureAndFailedMeasurement(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("incremental capture requires Linux amd64")
	}
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repository := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repository); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	gate := &cooperativeTestGate{gate: replicator.NewAdmissionGate()}
	budget, err := replicator.NewConservativeBudget(gate.gate, 10)
	if err != nil {
		t.Fatal(err)
	}
	var failMeasurement atomic.Bool
	config := coordinatorConfig(repository, databasePath, 1)
	config.ObserveTXID = func(context.Context) (uint64, bool, error) { return 0, false, nil }
	config.SetAdmission = func(ctx context.Context, enabled bool, limits replicator.Limits) error {
		if enabled {
			if err := budget.Reconcile(func() (uint64, error) {
				if failMeasurement.Load() {
					return 0, errors.New("injected measurement failure")
				}
				return 0, nil // A logical test cost, not a WAL or disk measurement.
			}); err != nil {
				return err
			}
		}
		return gate.setAdmission(ctx, enabled, limits)
	}
	config.DrainWrites = gate.drain
	config.Capture = func(ctx context.Context) (replicator.CaptureResult, error) {
		return replicator.CaptureOnceWithBudget(ctx, databasePath, repository, func(context.Context) (func() error, error) {
			return func() error { return nil }, nil
		}, budget)
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Flush(ctx); err != nil {
		t.Fatalf("initial flush: %v", err)
	}
	reservation, err := budget.TryReserve(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE records SET value = 'budgeted coordinator' WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	if err := reservation.Settle(0, true); err != nil {
		t.Fatal(err)
	}
	failMeasurement.Store(true)
	if err := coordinator.Flush(ctx); err == nil {
		t.Fatal("flush should fail when reconciliation fails")
	}
	if _, err := budget.TryReserve(1); err == nil {
		t.Fatal("failed measurement reopened budget")
	}
	if _, err := gate.accept(); err == nil {
		t.Fatal("failed measurement reopened admission")
	}
	restored := filepath.Join(directory, "restored.sqlite")
	if err := replicator.RestoreToTarget(ctx, repository, restored, replicator.RestoreTarget{TXID: 2}); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var value string
	if err := reader.QueryRowContext(ctx, "SELECT value FROM records WHERE id = 1").Scan(&value); err != nil || value != "budgeted coordinator" {
		t.Fatalf("restored value = %q, error = %v", value, err)
	}
	failMeasurement.Store(false)
	if err := coordinator.Flush(ctx); err != nil {
		t.Fatalf("retry: %v", err)
	}
	reservation, err = budget.TryReserve(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.Settle(0, true); err != nil {
		t.Fatal(err)
	}
}

func TestCooperativeBarrierDrainsRealSQLiteWriter(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("incremental capture requires Linux amd64")
	}
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repository := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repository); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	gate := &cooperativeTestGate{gate: replicator.NewAdmissionGate()}
	var failCapture atomic.Bool
	config := coordinatorConfig(repository, databasePath, 1)
	config.ObserveTXID = func(context.Context) (uint64, bool, error) { return 0, false, nil }
	config.SetAdmission = gate.setAdmission
	config.DrainWrites = gate.drain
	config.Capture = func(ctx context.Context) (replicator.CaptureResult, error) {
		if failCapture.Swap(false) {
			return replicator.CaptureResult{}, errors.New("injected capture failure")
		}
		return replicator.CaptureOnce(ctx, databasePath, repository, func(context.Context) (func() error, error) {
			// The coordinator has already closed and drained this test gate.
			return func() error { return nil }, nil
		})
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Flush(ctx); err != nil {
		t.Fatalf("initial barrier: %v", err)
	}
	release, err := gate.accept()
	if err != nil {
		t.Fatal(err)
	}
	gate.mu.Lock()
	closed := gate.closed
	gate.mu.Unlock()
	result := make(chan error, 1)
	go func() { result <- coordinator.Flush(ctx) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("admission did not close")
	}
	if _, err := gate.accept(); err == nil {
		t.Fatal("accepted a new writer after closing admission")
	}
	select {
	case err := <-result:
		t.Fatalf("flush finished before writer: %v", err)
	default:
	}
	if _, err := db.ExecContext(ctx, "UPDATE records SET value = ? WHERE id = 1", "accepted-before-close"); err != nil {
		t.Fatal(err)
	}
	release()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("flush: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not finish")
	}
	restored := filepath.Join(directory, "restored.sqlite")
	if err := replicator.RestoreToTarget(ctx, repository, restored, replicator.RestoreTarget{TXID: 2}); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var value string
	if err := reader.QueryRowContext(ctx, "SELECT value FROM records WHERE id = 1").Scan(&value); err != nil || value != "accepted-before-close" {
		t.Fatalf("restored value=%q error=%v", value, err)
	}

	release, err = gate.accept()
	if err != nil {
		t.Fatal(err)
	}
	gate.mu.Lock()
	closed = gate.closed
	gate.mu.Unlock()
	cancelCtx, cancel := context.WithCancel(ctx)
	canceled := make(chan error, 1)
	go func() { canceled <- coordinator.Flush(cancelCtx) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("admission did not close for cancellation")
	}
	cancel()
	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("drain cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled flush did not finish")
	}
	if _, err := gate.accept(); err == nil {
		t.Fatal("admission reopened after cancellation")
	}
	release()
	failCapture.Store(true)
	if err := coordinator.Flush(ctx); err == nil {
		t.Fatal("injected capture failure was ignored")
	}
	if _, err := gate.accept(); err == nil {
		t.Fatal("admission reopened after capture failure")
	}
	if err := coordinator.Flush(ctx); err != nil {
		t.Fatalf("retry after capture failure: %v", err)
	}
	finalRelease, err := gate.accept()
	if err != nil {
		t.Fatalf("admission after verified retry: %v", err)
	}
	finalRelease()
	gate.mu.Lock()
	gate.cancelOnOpen = true
	gate.mu.Unlock()
	if err := coordinator.Flush(ctx); !errors.Is(err, replicator.ErrAdmissionClosed) {
		t.Fatalf("coordinated cancellation during reopen: %v", err)
	}
	if _, err := gate.accept(); !errors.Is(err, replicator.ErrAdmissionClosed) {
		t.Fatalf("accept after coordinated cancellation: %v", err)
	}
}
