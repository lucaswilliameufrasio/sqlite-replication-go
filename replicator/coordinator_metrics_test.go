package replicator_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	replication "github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/replicator"
)

type flushObservation struct {
	duration time.Duration
	outcome  replicator.FlushOutcome
}

type flushMetrics struct {
	mu           sync.Mutex
	observations []flushObservation
	onObserve    func()
}

func (m *flushMetrics) ObserveFlush(duration time.Duration, outcome replicator.FlushOutcome) {
	m.mu.Lock()
	m.observations = append(m.observations, flushObservation{duration: duration, outcome: outcome})
	m.mu.Unlock()
	if m.onObserve != nil {
		m.onObserve()
	}
}

func TestCoordinatorFlushMetricsOutcomesAndReentrancy(t *testing.T) {
	directory := t.TempDir()
	database := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, database)
	repository := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(context.Background(), database, repository); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	metrics := &flushMetrics{}
	config := coordinatorConfig(repository, database, 1)
	config.Metrics = metrics
	var available atomic.Bool
	available.Store(true)
	config.ObserveTXID = func(context.Context) (uint64, bool, error) {
		return 1, available.Load(), nil
	}
	config.Capture = func(context.Context) (replicator.CaptureResult, error) {
		if !available.Load() {
			return replicator.CaptureResult{}, errors.New("injected capture failure")
		}
		return replicator.CaptureResult{TXID: 1}, nil
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatalf("construct coordinator: %v", err)
	}
	var calls atomic.Int32
	var nestedErr error
	metrics.onObserve = func() {
		if calls.Add(1) == 1 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			nestedErr = coordinator.Flush(ctx)
		}
	}
	if err := coordinator.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if nestedErr != nil {
		t.Fatalf("reentrant flush after metric observation: %v", nestedErr)
	}
	available.Store(false)
	if err := coordinator.Flush(context.Background()); err == nil {
		t.Fatal("flush with failing capture succeeded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := coordinator.Flush(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled flush error = %v", err)
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	want := []replicator.FlushOutcome{replicator.FlushSucceeded, replicator.FlushSucceeded, replicator.FlushFailed, replicator.FlushCanceled}
	if len(metrics.observations) != len(want) {
		t.Fatalf("got %d observations, want %d", len(metrics.observations), len(want))
	}
	for i, got := range metrics.observations {
		if got.outcome != want[i] || got.duration < 0 {
			t.Errorf("observation %d = %+v, want %s and nonnegative duration", i, got, want[i])
		}
	}
}
