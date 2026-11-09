package replicator_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	replication "github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/replicator"
)

func TestCoordinatorStatusSeparatesObservedAndDurablePositions(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("create initial snapshot: %v", err)
	}
	coordinator, err := replicator.NewCoordinator(coordinatorConfig(repositoryRoot, databasePath, 2))
	if err != nil {
		t.Fatalf("construct coordinator: %v", err)
	}
	status, err := coordinator.Status(ctx)
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status.ObservedTXID == nil || *status.ObservedTXID != 2 {
		t.Fatalf("observed position = %v, want 2", status.ObservedTXID)
	}
	if status.LocalDurable == nil || status.LocalDurable.TXID != 1 {
		t.Fatalf("local durable position = %+v, want TXID 1", status.LocalDurable)
	}
	if status.RemoteConfirmed != nil || status.RemoteAckMatches {
		t.Fatalf("local-only coordinator reported remote confirmation: %+v", status)
	}
	if status.UnprotectedAge < 0 {
		t.Fatalf("unprotected age is negative: %s", status.UnprotectedAge)
	}
	health := coordinator.Health(ctx)
	if health.Local != replicator.ComponentHealthy || health.Remote != replicator.ComponentUnknown {
		t.Fatalf("component health = %+v", health)
	}
}

func TestCoordinatorStatusTreatsUnavailableObservationConservatively(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("create initial snapshot: %v", err)
	}
	config := coordinatorConfig(repositoryRoot, databasePath, 1)
	config.ObserveTXID = func(context.Context) (uint64, bool, error) {
		return 0, false, nil
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatalf("construct coordinator: %v", err)
	}
	status, err := coordinator.Status(ctx)
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status.ObservedTXID != nil {
		t.Fatalf("unavailable observed TXID was represented as %d", *status.ObservedTXID)
	}
	if status.UnprotectedAge < config.Limits.MaxUnprotectedAge {
		t.Fatalf("unknown observed position has insufficiently conservative age: %s", status.UnprotectedAge)
	}
}

func TestCoordinatorBarrierCapturesWhenObservedTXIDUnavailable(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	config := coordinatorConfig(repositoryRoot, databasePath, 1)
	config.ObserveTXID = func(context.Context) (uint64, bool, error) { return 0, false, nil }
	captured := false
	config.Capture = func(context.Context) (replicator.CaptureResult, error) {
		captured = true
		return replicator.CaptureResult{TXID: 1}, nil
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Flush(ctx); err != nil {
		t.Fatalf("barrier: %v", err)
	}
	if !captured {
		t.Fatal("barrier skipped capture")
	}
}

func TestCoordinatorBarrierDrainsBeforeCapture(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	config := coordinatorConfig(repositoryRoot, databasePath, 1)
	drainStarted := make(chan struct{})
	release := make(chan struct{})
	captureStarted := make(chan struct{}, 1)
	config.DrainWrites = func(ctx context.Context) error {
		close(drainStarted)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	config.Capture = func(context.Context) (replicator.CaptureResult, error) {
		captureStarted <- struct{}{}
		return replicator.CaptureResult{TXID: 1}, nil
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- coordinator.Flush(ctx) }()
	select {
	case <-drainStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not start")
	}
	select {
	case <-captureStarted:
		t.Fatal("capture started before drain completed")
	default:
	}
	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("barrier: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("barrier did not finish")
	}
	select {
	case <-captureStarted:
	default:
		t.Fatal("barrier did not capture")
	}
}

func TestCoordinatorDrainFailureSkipsCaptureAndKeepsAdmissionClosed(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	gate := replicator.NewAdmissionGate()
	if err := gate.Open(); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected drain failure")
	var captures int
	config := coordinatorConfig(repositoryRoot, databasePath, 1)
	config.SetAdmission = func(_ context.Context, enabled bool, _ replicator.Limits) error {
		if enabled {
			return gate.Open()
		}
		gate.Close()
		return nil
	}
	config.DrainWrites = func(context.Context) error { return injected }
	config.Capture = func(context.Context) (replicator.CaptureResult, error) {
		captures++
		return replicator.CaptureResult{TXID: 1}, nil
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatalf("coordinator: %v", err)
	}
	if err := coordinator.Flush(ctx); !errors.Is(err, injected) {
		t.Fatalf("flush error = %v, want drain failure", err)
	}
	if captures != 0 {
		t.Fatalf("capture invoked %d times after drain failure", captures)
	}
	if _, err := gate.TryAccept(); !errors.Is(err, replicator.ErrAdmissionClosed) {
		t.Fatalf("admission after drain failure = %v", err)
	}
}

func TestCoordinatorBarrierCapturesCheckpointedSQLiteChanges(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("incremental capture requires Linux amd64")
	}
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.ExecContext(ctx, "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE records(value TEXT)"); err != nil {
		t.Fatal(err)
	}
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"first", "second"} {
		if _, err := database.ExecContext(ctx, "INSERT INTO records VALUES (?)", value); err != nil {
			t.Fatal(err)
		}
	}
	var busy, logFrames, checkpointed int
	if err := database.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed); err != nil || busy != 0 {
		t.Fatalf("checkpoint: busy=%d error=%v", busy, err)
	}
	config := coordinatorConfig(repositoryRoot, databasePath, 1)
	config.ObserveTXID = func(context.Context) (uint64, bool, error) { return 0, false, nil }
	config.Capture = func(ctx context.Context) (replicator.CaptureResult, error) {
		return replicator.CaptureOnce(ctx, databasePath, repositoryRoot, func(context.Context) (func() error, error) {
			return func() error { return nil }, nil
		})
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Flush(ctx); err != nil {
		t.Fatalf("barrier: %v", err)
	}
	restored := filepath.Join(directory, "restored.sqlite")
	if err := replicator.RestoreToTarget(ctx, repositoryRoot, restored, replicator.RestoreTarget{TXID: 2}); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var count int
	if err := reader.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 2 {
		t.Fatalf("restored rows=%d error=%v", count, err)
	}
}

func TestCoordinatorFlushSerializesAdmissionAroundTarget(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("create initial snapshot: %v", err)
	}
	var admission []bool
	var appliedLimits []replicator.Limits
	config := coordinatorConfig(repositoryRoot, databasePath, 1)
	config.SetAdmission = func(_ context.Context, enabled bool, limits replicator.Limits) error {
		admission = append(admission, enabled)
		appliedLimits = append(appliedLimits, limits)
		return nil
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatalf("construct coordinator: %v", err)
	}
	if err := coordinator.Flush(ctx); err != nil {
		t.Fatalf("flush already-durable target: %v", err)
	}
	if !reflect.DeepEqual(admission, []bool{false, true}) {
		t.Fatalf("admission transitions = %v, want [false true]", admission)
	}
	if len(appliedLimits) != 2 || appliedLimits[0] != config.Limits || appliedLimits[1] != config.Limits {
		t.Fatalf("admission limits = %+v, want configured limits %+v", appliedLimits, config.Limits)
	}
}

func TestCoordinatorSerializesConcurrentFlushOperations(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("create initial snapshot: %v", err)
	}
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open source database: %v", err)
	}
	if _, err := database.ExecContext(ctx, "UPDATE records SET value = ? WHERE id = 1", "flush-target"); err != nil {
		t.Fatalf("commit Flush target: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close source database: %v", err)
	}
	captureEntered := make(chan struct{}, 2)
	releaseCapture := make(chan struct{})
	var admission []bool
	config := coordinatorConfig(repositoryRoot, databasePath, 2)
	config.SetAdmission = func(_ context.Context, enabled bool, _ replicator.Limits) error {
		admission = append(admission, enabled)
		return nil
	}
	config.Capture = func(context.Context) (replicator.CaptureResult, error) {
		captureEntered <- struct{}{}
		select {
		case <-releaseCapture:
		case <-time.After(2 * time.Second):
			return replicator.CaptureResult{}, errors.New("test capture release timed out")
		}
		result, err := replicator.CaptureOnce(ctx, databasePath, repositoryRoot, func(context.Context) (func() error, error) {
			return func() error { return nil }, nil
		})
		return result, err
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatalf("construct coordinator: %v", err)
	}
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- coordinator.Flush(ctx)
	}()
	select {
	case <-captureEntered:
	case <-time.After(time.Second):
		t.Fatal("first Flush did not start capture")
	}
	secondResult := make(chan error, 1)
	go func() {
		secondResult <- coordinator.Flush(ctx)
	}()
	select {
	case <-captureEntered:
		t.Fatal("second Flush overlapped the first capture")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseCapture)
	if err := <-firstResult; err != nil {
		t.Fatalf("first Flush: %v", err)
	}
	if err := <-secondResult; err != nil {
		t.Fatalf("second Flush: %v", err)
	}
	if !reflect.DeepEqual(admission, []bool{false, true, false, true}) {
		t.Fatalf("serialized admission transitions = %v", admission)
	}
}

func TestCoordinatorRequiresExplicitPositiveLimitsAndStrictAdapter(t *testing.T) {
	directory := t.TempDir()
	valid := coordinatorConfig(filepath.Join(directory, "repository"), filepath.Join(directory, "source.sqlite"), 1)
	invalidLimits := valid
	invalidLimits.Limits.MaxWALBytes = 0
	if _, err := replicator.NewCoordinator(invalidLimits); err == nil {
		t.Fatal("coordinator accepted a zero WAL limit")
	}
	missingAdmission := valid
	missingAdmission.SetAdmission = nil
	if _, err := replicator.NewCoordinator(missingAdmission); err == nil {
		t.Fatal("strict coordinator accepted a missing admission controller")
	}
	missingCapture := valid
	missingCapture.Capture = nil
	if _, err := replicator.NewCoordinator(missingCapture); err == nil {
		t.Fatal("coordinator accepted a missing capture operation")
	}
	missingDrain := valid
	missingDrain.DrainWrites = nil
	if _, err := replicator.NewCoordinator(missingDrain); err == nil {
		t.Fatal("coordinator accepted a missing write drain")
	}
}

func TestCoordinatorFlushLeavesWritesBlockedWhenSpoolLimitIsExceeded(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("create initial snapshot: %v", err)
	}
	var admission []bool
	config := coordinatorConfig(repositoryRoot, databasePath, 1)
	config.Limits.MaxSpoolBytes = 1
	config.SetAdmission = func(_ context.Context, enabled bool, _ replicator.Limits) error {
		admission = append(admission, enabled)
		return nil
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatalf("construct coordinator: %v", err)
	}
	if err := coordinator.Flush(ctx); !errors.Is(err, replicator.ErrBackpressure) {
		t.Fatalf("flush error = %v, want backpressure", err)
	}
	if !reflect.DeepEqual(admission, []bool{false}) {
		t.Fatalf("admission transitions = %v, want writes to remain blocked", admission)
	}
}

func TestCoordinatorSpoolLimitAtCapacityAndRecovery(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("create initial snapshot: %v", err)
	}
	config := coordinatorConfig(repositoryRoot, databasePath, 1)
	probe, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	status, err := probe.Status(ctx)
	if err != nil || status.SpoolBytes == 0 {
		t.Fatalf("spool status = %+v, error = %v", status, err)
	}
	var admission []bool
	config.Limits.MaxSpoolBytes = status.SpoolBytes
	config.SetAdmission = func(_ context.Context, enabled bool, _ replicator.Limits) error {
		admission = append(admission, enabled)
		return nil
	}
	limited, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	if health := limited.Health(ctx); health.Local != replicator.ComponentDegraded || health.LocalError != replicator.ErrorBackpressure {
		t.Fatalf("health at capacity = %+v", health)
	}
	if err := limited.Flush(ctx); !errors.Is(err, replicator.ErrBackpressure) {
		t.Fatalf("flush at capacity = %v, want backpressure", err)
	}
	if !reflect.DeepEqual(admission, []bool{false}) {
		t.Fatalf("admission at capacity = %v", admission)
	}
	config.Limits.MaxSpoolBytes++
	recovered, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Flush(ctx); err != nil {
		t.Fatalf("flush below limit: %v", err)
	}
	if !reflect.DeepEqual(admission, []bool{false, false, true}) {
		t.Fatalf("admission after recovery = %v", admission)
	}
}

func TestCoordinatorWALLimitAtCapacityAndRecovery(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE boundary_test(value INTEGER); INSERT INTO boundary_test VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	config := coordinatorConfig(repositoryRoot, databasePath, 1)
	probe, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	status, err := probe.Status(ctx)
	if err != nil || status.WALBytes == 0 {
		t.Fatalf("WAL status = %+v, error = %v", status, err)
	}
	var admission []bool
	config.Limits.MaxWALBytes = status.WALBytes
	config.SetAdmission = func(_ context.Context, enabled bool, _ replicator.Limits) error {
		admission = append(admission, enabled)
		return nil
	}
	limited, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	if health := limited.Health(ctx); health.Local != replicator.ComponentDegraded || health.LocalError != replicator.ErrorBackpressure {
		t.Fatalf("health at capacity = %+v", health)
	}
	if err := limited.Flush(ctx); !errors.Is(err, replicator.ErrBackpressure) {
		t.Fatalf("flush at capacity = %v, want backpressure", err)
	}
	if !reflect.DeepEqual(admission, []bool{false}) {
		t.Fatalf("admission at capacity = %v", admission)
	}
	config.Limits.MaxWALBytes++
	recovered, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Flush(ctx); err != nil {
		t.Fatalf("flush below limit: %v", err)
	}
	if !reflect.DeepEqual(admission, []bool{false, false, true}) {
		t.Fatalf("admission after recovery = %v", admission)
	}
}

func TestCoordinatorRealCaptureExceedsSpoolLimitAndRestores(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("incremental capture requires Linux amd64")
	}
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.ExecContext(ctx, "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE records(value TEXT); INSERT INTO records VALUES ('base')"); err != nil {
		t.Fatal(err)
	}
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatal(err)
	}
	config := coordinatorConfig(repositoryRoot, databasePath, 1)
	probe, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	status, err := probe.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	baseline := status.SpoolBytes
	config.Limits.MaxSpoolBytes = baseline + 1
	config.ObserveTXID = func(context.Context) (uint64, bool, error) { return 2, true, nil }
	config.Capture = func(ctx context.Context) (replicator.CaptureResult, error) {
		return replicator.CaptureOnce(ctx, databasePath, repositoryRoot, func(context.Context) (func() error, error) {
			return func() error { return nil }, nil
		})
	}
	var admission []bool
	config.SetAdmission = func(_ context.Context, enabled bool, _ replicator.Limits) error {
		admission = append(admission, enabled)
		return nil
	}
	limited, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, "UPDATE records SET value='durable despite saturation'"); err != nil {
		t.Fatal(err)
	}
	if err := limited.Flush(ctx); !errors.Is(err, replicator.ErrBackpressure) {
		t.Fatalf("flush = %v, want backpressure", err)
	}
	status, err = limited.Status(ctx)
	if err != nil || status.SpoolBytes <= baseline || status.LocalDurable == nil || status.LocalDurable.TXID != 2 {
		t.Fatalf("saturated status = %+v, error = %v", status, err)
	}
	if !reflect.DeepEqual(admission, []bool{false}) {
		t.Fatalf("admission = %v, want closed", admission)
	}
	restored := filepath.Join(directory, "restored.sqlite")
	if err := replicator.RestoreToTarget(ctx, repositoryRoot, restored, replicator.RestoreTarget{TXID: 2}); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", restored)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var value string
	if err := reader.QueryRowContext(ctx, "SELECT value FROM records").Scan(&value); err != nil || value != "durable despite saturation" {
		t.Fatalf("restored value = %q, error = %v", value, err)
	}
	config.Limits.MaxSpoolBytes = status.SpoolBytes + 1
	recovered, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Flush(ctx); err != nil {
		t.Fatalf("retry with headroom: %v", err)
	}
	if !reflect.DeepEqual(admission, []bool{false, false, true}) {
		t.Fatalf("admission after retry = %v", admission)
	}
}

func TestCoordinatorRejectsCapturePositionWithoutDurableSpool(t *testing.T) {
	for _, test := range []struct {
		name string
		txid uint64
	}{
		{name: "zero", txid: 0},
		{name: "ahead of spool", txid: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			directory := t.TempDir()
			databasePath := filepath.Join(directory, "source.sqlite")
			createSourceDatabase(t, databasePath)
			repositoryRoot := filepath.Join(directory, "repository")
			if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
				t.Fatalf("create initial snapshot: %v", err)
			}
			var admission []bool
			config := coordinatorConfig(repositoryRoot, databasePath, 1)
			config.Capture = func(context.Context) (replicator.CaptureResult, error) {
				return replicator.CaptureResult{TXID: test.txid}, nil
			}
			config.SetAdmission = func(_ context.Context, enabled bool, _ replicator.Limits) error {
				admission = append(admission, enabled)
				return nil
			}
			coordinator, err := replicator.NewCoordinator(config)
			if err != nil {
				t.Fatalf("construct coordinator: %v", err)
			}
			if err := coordinator.Flush(ctx); !errors.Is(err, replicator.ErrNoProgress) {
				t.Fatalf("flush error = %v, want ErrNoProgress", err)
			}
			if !reflect.DeepEqual(admission, []bool{false}) {
				t.Fatalf("admission transitions = %v, want closed only", admission)
			}
		})
	}
}

func TestCoordinatorFlushLeavesWritesBlockedAfterCaptureErrorOrCancellation(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("create initial snapshot: %v", err)
	}
	var admission []bool
	config := coordinatorConfig(repositoryRoot, databasePath, 2)
	config.Capture = func(context.Context) (replicator.CaptureResult, error) {
		return replicator.CaptureResult{}, errors.New("injected capture failure")
	}
	config.SetAdmission = func(_ context.Context, enabled bool, _ replicator.Limits) error {
		admission = append(admission, enabled)
		return nil
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatalf("construct coordinator: %v", err)
	}
	if err := coordinator.Flush(ctx); err == nil {
		t.Fatal("Flush succeeded after capture failure")
	}
	if !reflect.DeepEqual(admission, []bool{false}) {
		t.Fatalf("admission transitions after capture failure = %v", admission)
	}

	admission = nil
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := coordinator.Flush(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Flush error = %v, want context cancellation", err)
	}
	if !reflect.DeepEqual(admission, []bool{false}) {
		t.Fatalf("admission transitions after cancellation = %v", admission)
	}
}

func TestCoordinatorRunPerformsInitialFlushAndClosesAdmissionOnExit(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(ctx, databasePath, repositoryRoot); err != nil {
		t.Fatalf("create initial snapshot: %v", err)
	}
	var admission []bool
	config := coordinatorConfig(repositoryRoot, databasePath, 1)
	config.SetAdmission = func(_ context.Context, enabled bool, _ replicator.Limits) error {
		admission = append(admission, enabled)
		return nil
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatalf("construct coordinator: %v", err)
	}
	events := make(chan struct{})
	close(events)
	if err := coordinator.Run(ctx, events); err != nil {
		t.Fatalf("run coordinator: %v", err)
	}
	if !reflect.DeepEqual(admission, []bool{false, false, true, false}) {
		t.Fatalf("admission transitions = %v, want [false false true false]", admission)
	}
}

func TestCoordinatorRunCapturesAfterCoalescibleCommitEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(context.Background(), databasePath, repositoryRoot); err != nil {
		t.Fatalf("create initial snapshot: %v", err)
	}
	var observed atomic.Uint64
	observed.Store(1)
	admissionEvents := make(chan bool, 8)
	config := coordinatorConfig(repositoryRoot, databasePath, 1)
	config.ObserveTXID = func(context.Context) (uint64, bool, error) {
		return observed.Load(), true, nil
	}
	config.Capture = func(ctx context.Context) (replicator.CaptureResult, error) {
		return replicator.CaptureOnce(ctx, databasePath, repositoryRoot, func(context.Context) (func() error, error) {
			return func() error { return nil }, nil
		})
	}
	config.SetAdmission = func(_ context.Context, enabled bool, _ replicator.Limits) error {
		admissionEvents <- enabled
		return nil
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatalf("construct coordinator: %v", err)
	}
	events := make(chan struct{}, 1)
	runResult := make(chan error, 1)
	go func() {
		runResult <- coordinator.Run(ctx, events)
	}()
	awaitAdmission(t, admissionEvents, false)
	awaitAdmission(t, admissionEvents, false)
	awaitAdmission(t, admissionEvents, true)

	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open source database: %v", err)
	}
	if _, err := database.ExecContext(context.Background(), "UPDATE records SET value = ? WHERE id = 1", "event-target"); err != nil {
		t.Fatalf("commit event target: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close source database: %v", err)
	}
	observed.Store(2)
	events <- struct{}{}
	awaitAdmission(t, admissionEvents, false)
	awaitAdmission(t, admissionEvents, true)
	cancel()
	if err := <-runResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run result = %v, want context cancellation", err)
	}
	awaitAdmission(t, admissionEvents, false)
	status, err := coordinator.Status(context.Background())
	if err != nil {
		t.Fatalf("read status after event capture: %v", err)
	}
	if status.LocalDurable == nil || status.LocalDurable.TXID != 2 {
		t.Fatalf("local durable position after event = %+v, want TXID 2", status.LocalDurable)
	}
}

func awaitAdmission(t *testing.T, events <-chan bool, want bool) {
	t.Helper()
	select {
	case got := <-events:
		if got != want {
			t.Fatalf("admission state = %t, want %t", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for admission state %t", want)
	}
}

func coordinatorConfig(repositoryRoot, databasePath string, observed uint64) replicator.CoordinatorConfig {
	return replicator.CoordinatorConfig{
		RepositoryRoot: repositoryRoot,
		DatabasePath:   databasePath,
		ObserveTXID: func(context.Context) (uint64, bool, error) {
			return observed, true, nil
		},
		Capture: func(context.Context) (replicator.CaptureResult, error) {
			return replicator.CaptureResult{TXID: observed}, nil
		},
		SetAdmission: func(context.Context, bool, replicator.Limits) error { return nil },
		DrainWrites:  func(context.Context) error { return nil },
		Limits: replicator.Limits{
			MaxSpoolBytes:      1 << 30,
			MaxWALBytes:        64 << 20,
			MaxUnprotectedAge:  time.Hour,
			MaxUploadsInFlight: 1,
		},
	}
}
