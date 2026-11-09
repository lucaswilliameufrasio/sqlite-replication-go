// Package replicator exposes synchronous incremental capture operations.
package replicator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/capture"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/repositorylock"
)

// WriteGate pauses every application writer coordinated by the caller. The
// returned release function must restore writes and may report a release error.
// A nil gate is rejected: strict admission is mandatory for incremental capture.
type WriteGate func(context.Context) (release func() error, err error)

// CaptureResult reports the durable local position produced by CaptureOnce.
type CaptureResult struct {
	TXID         uint64
	Transactions int
	WALFrames    int
}

// CaptureOnce captures and durably spools the currently observable SQLite WAL
// and reconciliation delta, then releases the application's write gate.
//
// The repository must already contain an initial snapshot created by
// replication.Snapshot. The caller must route all application writes through
// gate and keep them paused until the returned release function runs. This API
// is supported only on Linux amd64 with the SQLite runtimes validated by CP-01.
// It never requests a checkpoint and does not upload the local spool remotely.
func CaptureOnce(ctx context.Context, databasePath, repositoryRoot string, gate WriteGate) (CaptureResult, error) {
	return captureOnce(ctx, databasePath, repositoryRoot, gate, nil, 0, 0, 0, 0, 0)
}

// CaptureOnceWithBudget is experimental cooperative accounting, not a disk
// quota. The gate must stop all cooperating writers before returning. Capture
// invalidates occupancy. A successful release callback can reconcile it again;
// callers must coordinate reconciliation before accepting new reservations.
// Independent budgets and writers bypassing this budget are not coordinated.
// Gate callbacks run outside the budget lock and must not independently reopen
// accounting after a failed capture or release.
func CaptureOnceWithBudget(ctx context.Context, databasePath, repositoryRoot string, gate WriteGate, budget *ConservativeBudget) (CaptureResult, error) {
	if budget == nil {
		return CaptureResult{}, errors.New("cooperative capture requires a budget")
	}
	return captureOnce(ctx, databasePath, repositoryRoot, gate, budget, 0, 0, 0, 0, 0)
}

// CaptureOnceWithReservedBudget checks a caller-supplied conservative cost
// before capture, including all temporary recovery, staging and spool growth.
// A successful check does not reserve filesystem space or enforce the estimate.
// All writers and mutations must cooperate; the gate must close and drain them.
func CaptureOnceWithReservedBudget(ctx context.Context, databasePath, repositoryRoot string, gate WriteGate, budget *ConservativeBudget, cost uint64) (CaptureResult, error) {
	if budget == nil || cost == 0 {
		return CaptureResult{}, errors.New("cooperative capture requires a budget and nonzero cost")
	}
	return captureOnce(ctx, databasePath, repositoryRoot, gate, budget, cost, 0, 0, 0, 0)
}

// CaptureOnceWithRecoveryImageLimit additionally bounds logical bytes written
// to the decoded base-snapshot temporary file. It does not enforce the budget
// estimate or bound the source backup, spool, metadata or filesystem blocks.
func CaptureOnceWithRecoveryImageLimit(ctx context.Context, databasePath, repositoryRoot string, gate WriteGate, budget *ConservativeBudget, cost, maxImageBytes uint64) (CaptureResult, error) {
	if budget == nil || cost == 0 || maxImageBytes == 0 {
		return CaptureResult{}, errors.New("capture requires budget, nonzero cost, and recovery image limit")
	}
	return captureOnce(ctx, databasePath, repositoryRoot, gate, budget, cost, maxImageBytes, 0, 0, 0)
}

// CaptureOnceWithImageLimits limits the logical recovery and SQLite backup images.
// It does not limit LTX, journals, metadata, other operations or physical blocks.
func CaptureOnceWithImageLimits(ctx context.Context, databasePath, repositoryRoot string, gate WriteGate, budget *ConservativeBudget, cost, recoveryLimit, backupLimit uint64) (CaptureResult, error) {
	if budget == nil || cost == 0 || recoveryLimit == 0 || backupLimit == 0 {
		return CaptureResult{}, errors.New("capture requires budget, nonzero cost, and image limits")
	}
	return captureOnce(ctx, databasePath, repositoryRoot, gate, budget, cost, recoveryLimit, backupLimit, 0, 0)
}

// CaptureOnceWithBatchLimit additionally rejects encoded LTX batches exceeding
// batchLimit before staging. It does not bound encoding memory, existing spool,
// other producers, journals, metadata or physical disk consumption.
func CaptureOnceWithBatchLimit(ctx context.Context, databasePath, repositoryRoot string, gate WriteGate, budget *ConservativeBudget, cost, recoveryLimit, backupLimit, batchLimit uint64) (CaptureResult, error) {
	if budget == nil || cost == 0 || recoveryLimit == 0 || backupLimit == 0 || batchLimit == 0 {
		return CaptureResult{}, errors.New("capture requires budget, nonzero cost, and limits")
	}
	return captureOnce(ctx, databasePath, repositoryRoot, gate, budget, cost, recoveryLimit, backupLimit, batchLimit, 0)
}

// CaptureLimits specifies separate logical limits, not a combined disk quota.
type CaptureLimits struct {
	RecoveryImage uint64
	BackupImage   uint64
	LTXBatch      uint64
	Spool         uint64
}

// CaptureOnceWithDerivedReservation reserves the sum of the image and encoded
// batch ceilings against the shared budget. The caller must reconcile that
// budget with all existing relevant logical bytes and coordinate all producers.
// This does not cover journals, metadata, filesystem blocks or enforce the
// measurement supplied by the caller; it is not a physical quota.
func CaptureOnceWithDerivedReservation(ctx context.Context, databasePath, repositoryRoot string, gate WriteGate, budget *ConservativeBudget, limits CaptureLimits) (CaptureResult, error) {
	if limits.RecoveryImage == 0 || limits.BackupImage == 0 || limits.LTXBatch == 0 || limits.Spool == 0 {
		return CaptureResult{}, errors.New("capture limits must be nonzero")
	}
	if limits.BackupImage > ^uint64(0)-limits.RecoveryImage {
		return CaptureResult{}, errors.New("capture reservation overflow")
	}
	cost := limits.RecoveryImage + limits.BackupImage
	if limits.LTXBatch > ^uint64(0)-cost {
		return CaptureResult{}, errors.New("capture reservation overflow")
	}
	cost += limits.LTXBatch
	return CaptureOnceWithSpoolLimit(ctx, databasePath, repositoryRoot, gate, budget, cost, limits)
}

// CaptureOnceWithMeasuredReservation measures ltx and .staging and reserves
// the sum of the logical ceilings under one budget mutex. The caller's gate
// must drain all producers and keep them excluded through the call. Independent
// budgets must use the same repository lock; non-cooperating operations remain outside the bound;
// journals, metadata and physical blocks are not included.
func CaptureOnceWithMeasuredReservation(ctx context.Context, databasePath, repositoryRoot string, gate WriteGate, budget *ConservativeBudget, limits CaptureLimits) (CaptureResult, error) {
	return captureOnceWithMeasuredReservation(ctx, databasePath, repositoryRoot, gate, budget, limits, nil)
}

func captureOnceWithMeasuredReservation(ctx context.Context, databasePath, repositoryRoot string, gate WriteGate, budget *ConservativeBudget, limits CaptureLimits, hook capture.FaultHook) (CaptureResult, error) {
	if budget == nil || gate == nil || limits.RecoveryImage == 0 || limits.BackupImage == 0 || limits.LTXBatch == 0 || limits.Spool == 0 {
		return CaptureResult{}, errors.New("capture requires gate, budget and nonzero limits")
	}
	if limits.BackupImage > ^uint64(0)-limits.RecoveryImage {
		return CaptureResult{}, errors.New("capture reservation overflow")
	}
	cost := limits.RecoveryImage + limits.BackupImage
	if limits.LTXBatch > ^uint64(0)-cost {
		return CaptureResult{}, errors.New("capture reservation overflow")
	}
	cost += limits.LTXBatch
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return CaptureResult{}, fmt.Errorf("incremental capture is supported only on Linux amd64")
	}
	if err := ctx.Err(); err != nil {
		return CaptureResult{}, err
	}
	release, err := gate(ctx)
	if err != nil || release == nil {
		budget.Invalidate()
		if err == nil {
			err = errors.New("application write gate returned no release function")
		}
		return CaptureResult{}, fmt.Errorf("acquire application write gate: %w", err)
	}
	var result capture.Result
	var repositoryLock *os.File
	defer func() {
		if repositoryLock != nil {
			_ = repositoryLock.Close()
		}
	}()
	captureErr := budget.WithMeasuredReservedMutation(cost, func() (uint64, error) {
		var err error
		repositoryLock, err = repositorylock.AcquireExclusive(repositoryRoot)
		if err != nil {
			return 0, fmt.Errorf("lock repository for measured capture: %w", err)
		}
		return measureRepositoryLogicalBytesLocked(repositoryRoot)
	}, func() error {
		var err error
		result, err = capture.CaptureOnceWithAllLimitsLocked(ctx, databasePath, repositoryRoot, time.Now().UnixMilli(), hook, limits.RecoveryImage, limits.BackupImage, limits.LTXBatch, limits.Spool)
		return err
	})
	if repositoryLock != nil {
		captureErr = errors.Join(captureErr, repositoryLock.Close())
		repositoryLock = nil
	}
	releaseErr := release()
	if captureErr != nil || releaseErr != nil {
		budget.Invalidate()
		return CaptureResult{}, errors.Join(captureErr, wrapReleaseError(releaseErr))
	}
	return CaptureResult{TXID: result.State.Database.TXID, Transactions: len(result.Transactions), WALFrames: int(result.WALFrames)}, nil
}

// CaptureOnceWithSpoolLimit applies separate limits under cooperative local
// coordination. It does not bound metadata, journals or physical disk usage.
func CaptureOnceWithSpoolLimit(ctx context.Context, databasePath, repositoryRoot string, gate WriteGate, budget *ConservativeBudget, cost uint64, limits CaptureLimits) (CaptureResult, error) {
	if budget == nil || cost == 0 || limits.RecoveryImage == 0 || limits.BackupImage == 0 || limits.LTXBatch == 0 || limits.Spool == 0 {
		return CaptureResult{}, errors.New("capture requires budget, nonzero cost, and limits")
	}
	return captureOnce(ctx, databasePath, repositoryRoot, gate, budget, cost, limits.RecoveryImage, limits.BackupImage, limits.LTXBatch, limits.Spool)
}

func captureOnce(ctx context.Context, databasePath, repositoryRoot string, gate WriteGate, budget *ConservativeBudget, cost, maxImageBytes, backupLimit, batchLimit, spoolLimit uint64) (CaptureResult, error) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return CaptureResult{}, fmt.Errorf("incremental capture is supported only on Linux amd64")
	}
	if gate == nil {
		return CaptureResult{}, fmt.Errorf("strict incremental capture requires an application write gate")
	}
	if err := ctx.Err(); err != nil {
		return CaptureResult{}, err
	}
	release, err := gate(ctx)
	if err != nil {
		if budget != nil {
			budget.Invalidate()
		}
		return CaptureResult{}, fmt.Errorf("acquire application write gate: %w", err)
	}
	if release == nil {
		if budget != nil {
			budget.Invalidate()
		}
		return CaptureResult{}, fmt.Errorf("application write gate returned no release function")
	}

	var result capture.Result
	runCapture := func() error {
		var captureErr error
		if spoolLimit != 0 {
			result, captureErr = capture.CaptureOnceWithAllLimits(ctx, databasePath, repositoryRoot, time.Now().UnixMilli(), nil, maxImageBytes, backupLimit, batchLimit, spoolLimit)
		} else if batchLimit != 0 {
			result, captureErr = capture.CaptureOnceWithBatchLimit(ctx, databasePath, repositoryRoot, time.Now().UnixMilli(), nil, maxImageBytes, backupLimit, batchLimit)
		} else if backupLimit != 0 {
			result, captureErr = capture.CaptureOnceWithImageLimits(ctx, databasePath, repositoryRoot, time.Now().UnixMilli(), nil, maxImageBytes, backupLimit)
		} else if maxImageBytes == 0 {
			result, captureErr = capture.CaptureOnce(ctx, databasePath, repositoryRoot, time.Now().UnixMilli(), nil)
		} else {
			result, captureErr = capture.CaptureOnceWithRecoveryImageLimit(ctx, databasePath, repositoryRoot, time.Now().UnixMilli(), nil, maxImageBytes)
		}
		return captureErr
	}
	var captureErr error
	if budget != nil && cost != 0 {
		captureErr = budget.WithReservedMutation(cost, runCapture)
	} else if budget != nil {
		captureErr = budget.WithOccupancyMutation(runCapture)
	} else {
		captureErr = runCapture()
	}
	releaseErr := release()
	if budget != nil && (captureErr != nil || releaseErr != nil) {
		budget.Invalidate()
	}
	if captureErr != nil || releaseErr != nil {
		return CaptureResult{}, errors.Join(captureErr, wrapReleaseError(releaseErr))
	}
	return CaptureResult{
		TXID:         result.State.Database.TXID,
		Transactions: len(result.Transactions),
		WALFrames:    result.WALFrames,
	}, nil
}

func wrapReleaseError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("release application write gate: %w", err)
}
