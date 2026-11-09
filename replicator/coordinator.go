package replicator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/recovery"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/repositorylock"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/spool"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storages3"
)

var (
	ErrBackpressure = errors.New("replication limits exceeded; writes remain blocked")
	ErrNoProgress   = errors.New("barrier position does not match durable or confirmed progress")
	errAdapter      = errors.New("replication adapter failure")
	errLocalStorage = errors.New("replication local storage failure")
)

type ObserveTXID func(context.Context) (txid uint64, available bool, err error)

// CaptureOperation durably captures the state of the drained cooperative
// writers. A no-op returns the current nonzero durable TXID. A matching spool
// TXID alone cannot detect an adapter that fails to capture accepted changes.
type CaptureOperation func(context.Context) (CaptureResult, error)

// DrainWrites waits for every write accepted before admission closed to finish.
// It must honor cancellation without claiming unfinished writes were drained.
type DrainWrites func(context.Context) error

// SetWriteAdmission applies a strict admission policy. Enabling must atomically
// reserve enough capacity to keep writes within limits and leave admission
// closed if it returns an error; disabling must stop new writes before it
// returns successfully.
type SetWriteAdmission func(context.Context, bool, Limits) error

type Limits struct {
	MaxSpoolBytes      uint64
	MaxWALBytes        uint64
	MaxUnprotectedAge  time.Duration
	MaxUploadsInFlight int
}

type RemoteConfig struct {
	Store    *storages3.Store
	Prefix   string
	RemoteID string
}

// FlushOutcome is a bounded label; it never includes errors, keys, or paths.
type FlushOutcome string

const (
	FlushSucceeded FlushOutcome = "success"
	FlushFailed    FlushOutcome = "failure"
	FlushCanceled  FlushOutcome = "canceled"
)

// CoordinatorMetrics is invoked synchronously after the flush lock is released.
// Implementations must be concurrency-safe, quick, and must not panic. Duration
// includes lock wait and excludes the callback. If cancellation is joined with
// another error, the outcome is canceled; inspect the returned error separately.
type CoordinatorMetrics interface {
	ObserveFlush(duration time.Duration, outcome FlushOutcome)
}

type CoordinatorConfig struct {
	RepositoryRoot string
	DatabasePath   string
	ObserveTXID    ObserveTXID
	Capture        CaptureOperation
	SetAdmission   SetWriteAdmission
	DrainWrites    DrainWrites
	Limits         Limits
	Remote         *RemoteConfig
	Metrics        CoordinatorMetrics
}

type Coordinator struct {
	config  CoordinatorConfig
	flushMu chan struct{}
}

type Position struct {
	TXID             uint64
	DatabaseChecksum uint64
	TimestampMillis  *int64
}

type Status struct {
	ObservedTXID     *uint64
	LocalDurable     *Position
	RemoteConfirmed  *Position
	RemoteAckMatches bool
	SpoolBytes       uint64
	WALBytes         uint64
	UnprotectedAge   time.Duration
}

type ErrorCategory string

const (
	ErrorNone          ErrorCategory = "none"
	ErrorAdapter       ErrorCategory = "adapter"
	ErrorLocalStorage  ErrorCategory = "local-storage"
	ErrorRemoteStorage ErrorCategory = "remote-storage"
	ErrorBackpressure  ErrorCategory = "backpressure"
	ErrorCancelled     ErrorCategory = "cancelled"
)

type ComponentState string

const (
	ComponentUnknown       ComponentState = "unknown"
	ComponentHealthy       ComponentState = "healthy"
	ComponentDegraded      ComponentState = "degraded"
	ComponentNeedsOperator ComponentState = "needs-operator"
)

type Health struct {
	Local       ComponentState
	Remote      ComponentState
	LocalError  ErrorCategory
	RemoteError ErrorCategory
}

func NewCoordinator(config CoordinatorConfig) (*Coordinator, error) {
	if config.RepositoryRoot == "" || config.DatabasePath == "" {
		return nil, fmt.Errorf("repository root and database path are required")
	}
	if config.ObserveTXID == nil || config.Capture == nil || config.SetAdmission == nil || config.DrainWrites == nil {
		return nil, fmt.Errorf("TXID observer, capture operation, write admission, and write drain are required")
	}
	if config.Limits.MaxSpoolBytes == 0 || config.Limits.MaxWALBytes == 0 ||
		config.Limits.MaxUnprotectedAge <= 0 || config.Limits.MaxUploadsInFlight <= 0 {
		return nil, fmt.Errorf("all replication limits must be explicitly positive")
	}
	if config.Remote != nil {
		if config.Remote.Store == nil || config.Remote.RemoteID == "" {
			return nil, fmt.Errorf("remote store and stable remote ID are required")
		}
	}
	coordinator := &Coordinator{config: config, flushMu: make(chan struct{}, 1)}
	coordinator.flushMu <- struct{}{}
	return coordinator, nil
}

// Status returns observed, local-durable, and destination-matching remote
// positions independently. It never treats an absent position as TXID zero.
func (c *Coordinator) Status(ctx context.Context) (Status, error) {
	status := Status{}
	observed, available, err := c.config.ObserveTXID(ctx)
	if err != nil {
		return status, fmt.Errorf("%w: observe SQLite TXID: %w", errAdapter, err)
	}
	if available {
		status.ObservedTXID = uint64Pointer(observed)
	}
	lock, err := repositorylock.AcquireExclusive(c.config.RepositoryRoot)
	if err != nil {
		return status, fmt.Errorf("%w: lock repository for status: %w", errLocalStorage, err)
	}
	defer lock.Close()
	state, err := spool.Recover(c.config.RepositoryRoot, nil)
	if err != nil {
		return status, fmt.Errorf("%w: recover local durable position: %w", errLocalStorage, err)
	}
	plan, err := recovery.BuildRestorePlanFromFilesystem(c.config.RepositoryRoot, recovery.Target{TXID: state.Database.TXID})
	if err != nil {
		return status, fmt.Errorf("%w: validate local durable position: %w", errLocalStorage, err)
	}
	status.LocalDurable = &Position{
		TXID:             state.Database.TXID,
		DatabaseChecksum: state.Database.Checksum,
		TimestampMillis:  int64Pointer(plan.TimestampMillis),
	}
	status.SpoolBytes, err = localSpoolBytes(filepath.Join(c.config.RepositoryRoot, "ltx"))
	if err != nil {
		return status, fmt.Errorf("%w: measure local spool: %w", errLocalStorage, err)
	}
	status.WALBytes, err = fileSizeIfPresent(c.config.DatabasePath + "-wal")
	if err != nil {
		return status, fmt.Errorf("%w: measure SQLite WAL: %w", errLocalStorage, err)
	}
	if status.LocalDurable != nil {
		if !available {
			status.UnprotectedAge = time.Duration(1<<63 - 1)
		} else if observed > status.LocalDurable.TXID {
			if status.LocalDurable.TimestampMillis == nil {
				status.UnprotectedAge = time.Duration(1<<63 - 1)
			} else {
				status.UnprotectedAge = time.Since(time.UnixMilli(*status.LocalDurable.TimestampMillis))
				if status.UnprotectedAge < 0 {
					status.UnprotectedAge = 0
				}
			}
		}
	}
	if c.config.Remote != nil {
		ack, ackErr := spool.ReadRemoteAck(c.config.RepositoryRoot)
		if ackErr == nil && ack.RemoteID == c.config.Remote.RemoteID && ack.Prefix == c.config.Remote.Prefix {
			status.RemoteAckMatches = true
			status.RemoteConfirmed = &Position{TXID: ack.TXID, DatabaseChecksum: ack.DatabaseChecksum}
		}
	}
	return status, nil
}

// Health reports local and remote component availability separately. Remote
// availability is not process liveness; ACK freshness remains in Status.
func (c *Coordinator) Health(ctx context.Context) Health {
	health := Health{Local: ComponentUnknown, Remote: ComponentUnknown, LocalError: ErrorNone, RemoteError: ErrorNone}
	status, err := c.Status(ctx)
	if err != nil {
		health.LocalError = categorizeError(err)
		switch health.LocalError {
		case ErrorAdapter:
			health.Local = ComponentDegraded
		case ErrorCancelled:
			health.Local = ComponentUnknown
		default:
			health.Local = ComponentNeedsOperator
		}
		return health
	}
	if status.LocalDurable != nil {
		health.Local = ComponentHealthy
		if status.SpoolBytes >= c.config.Limits.MaxSpoolBytes || status.WALBytes >= c.config.Limits.MaxWALBytes || status.UnprotectedAge > c.config.Limits.MaxUnprotectedAge {
			health.Local = ComponentDegraded
			health.LocalError = ErrorBackpressure
		}
	}
	if c.config.Remote != nil {
		key := joinRemoteKey(c.config.Remote.Prefix, "_meta/repository.json")
		if _, err := c.config.Remote.Store.Head(ctx, key); err != nil {
			health.Remote = ComponentDegraded
			health.RemoteError = ErrorRemoteStorage
		} else {
			health.Remote = ComponentHealthy
		}
	}
	return health
}

// Flush closes admission, drains accepted writers, captures a consistent state,
// and optionally confirms the resulting durable position remotely. On any
// error the coordinator remains fail-closed; successful completion reopens
// admission only if every configured capacity limit is satisfied.
func (c *Coordinator) Flush(ctx context.Context) (returnErr error) {
	if c.config.Metrics != nil {
		started := time.Now()
		defer func() {
			outcome := FlushSucceeded
			if errors.Is(returnErr, context.Canceled) || errors.Is(returnErr, context.DeadlineExceeded) {
				outcome = FlushCanceled
			} else if returnErr != nil {
				outcome = FlushFailed
			}
			c.config.Metrics.ObserveFlush(time.Since(started), outcome)
		}()
	}
	select {
	case <-ctx.Done():
		return errors.Join(ctx.Err(), c.closeAdmission())
	case <-c.flushMu:
	}
	defer func() { c.flushMu <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return errors.Join(err, c.closeAdmission())
	}
	if err := c.closeAdmission(); err != nil {
		return fmt.Errorf("close write admission before Flush: %w", err)
	}
	if err := c.config.DrainWrites(ctx); err != nil {
		return fmt.Errorf("drain accepted writes before Flush: %w", err)
	}
	lock, err := repositorylock.AcquireExclusive(c.config.RepositoryRoot)
	if err != nil {
		return fmt.Errorf("lock repository before Flush: %w", err)
	}
	previous, recoverErr := spool.Recover(c.config.RepositoryRoot, nil)
	closeErr := lock.Close()
	err = errors.Join(recoverErr, closeErr)
	if err != nil {
		return fmt.Errorf("recover durable position before Flush: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	captured, err := c.config.Capture(ctx)
	if err != nil {
		return fmt.Errorf("capture Flush barrier locally: %w", err)
	}
	target := captured.TXID
	status, err := c.Status(ctx)
	if err != nil {
		return fmt.Errorf("verify captured position: %w", err)
	}
	if target == 0 || target < previous.Database.TXID || status.LocalDurable == nil || status.LocalDurable.TXID != target {
		return ErrNoProgress
	}
	if c.config.Remote != nil {
		result, err := SyncToS3(ctx, c.config.RepositoryRoot, c.config.Remote.Store, c.config.Remote.Prefix, c.config.Remote.RemoteID)
		if err != nil {
			return fmt.Errorf("confirm Flush target remotely: %w", err)
		}
		if result.TXID < target {
			return ErrNoProgress
		}
	}
	status, err = c.Status(ctx)
	if err != nil {
		return fmt.Errorf("verify Flush completion: %w", err)
	}
	if status.LocalDurable == nil || status.LocalDurable.TXID < target {
		return ErrNoProgress
	}
	if status.SpoolBytes >= c.config.Limits.MaxSpoolBytes || status.WALBytes >= c.config.Limits.MaxWALBytes || (status.ObservedTXID != nil && status.UnprotectedAge > c.config.Limits.MaxUnprotectedAge) {
		return ErrBackpressure
	}
	if c.config.Remote != nil && (!status.RemoteAckMatches || status.RemoteConfirmed == nil || status.RemoteConfirmed.TXID < target) {
		return ErrNoProgress
	}
	if err := c.config.SetAdmission(ctx, true, c.config.Limits); err != nil {
		return fmt.Errorf("reopen write admission after Flush: %w", err)
	}
	return nil
}

// Run performs an initial reconciliation, then flushes after each coalescible
// adapter notification. The application must keep admission closed until this
// initial flush succeeds. Closing events or canceling ctx leaves writes closed.
func (c *Coordinator) Run(ctx context.Context, events <-chan struct{}) error {
	if events == nil {
		return fmt.Errorf("coordinator event channel is required")
	}
	if err := c.closeAdmission(); err != nil {
		return fmt.Errorf("close write admission before coordinator startup: %w", err)
	}
	if err := c.Flush(ctx); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			closeErr := c.closeAdmission()
			return errors.Join(ctx.Err(), closeErr)
		case _, open := <-events:
			if !open {
				if err := c.closeAdmission(); err != nil {
					return fmt.Errorf("close write admission after coordinator events ended: %w", err)
				}
				return nil
			}
			if err := c.Flush(ctx); err != nil {
				return err
			}
		}
	}
}

func (c *Coordinator) closeAdmission() error {
	return c.config.SetAdmission(context.Background(), false, c.config.Limits)
}

func localSpoolBytes(root string) (uint64, error) {
	if _, err := os.Stat(root); errors.Is(err, os.ErrNotExist) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	var total uint64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("spool contains a non-regular file")
		}
		size := uint64(info.Size())
		if ^uint64(0)-total < size {
			return fmt.Errorf("spool byte count overflow")
		}
		total += size
		return nil
	})
	return total, err
}

func fileSizeIfPresent(path string) (uint64, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() {
		return 0, fmt.Errorf("SQLite WAL path is not a regular file")
	}
	return uint64(info.Size()), nil
}

func uint64Pointer(value uint64) *uint64 {
	return &value
}

func int64Pointer(value int64) *int64 {
	return &value
}

func joinRemoteKey(prefix, suffix string) string {
	if prefix == "" {
		return suffix
	}
	return prefix + "/" + suffix
}

func categorizeError(err error) ErrorCategory {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ErrorCancelled
	}
	if errors.Is(err, ErrBackpressure) {
		return ErrorBackpressure
	}
	if errors.Is(err, errAdapter) {
		return ErrorAdapter
	}
	if errors.Is(err, errLocalStorage) {
		return ErrorLocalStorage
	}
	return ErrorLocalStorage
}
