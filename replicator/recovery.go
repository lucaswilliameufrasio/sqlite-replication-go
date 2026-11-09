package replicator

import (
	"context"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/recovery"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storages3"
)

// RestoreTarget selects exactly one verified recovery point. Set TXID for an
// exact transaction, or TimestampMillis for the latest point at or before that
// UTC Unix timestamp in milliseconds.
type RestoreTarget struct {
	TXID            uint64
	TimestampMillis *int64
}

// RestoreToTarget rebuilds and verifies a snapshot-plus-delta chain into a new
// database path. It never overwrites a database or stale SQLite sidecar.
func RestoreToTarget(ctx context.Context, repositoryRoot, destinationPath string, target RestoreTarget) error {
	return recovery.RestoreToTarget(ctx, repositoryRoot, destinationPath, recovery.Target{
		TXID:            target.TXID,
		TimestampMillis: target.TimestampMillis,
	})
}

// RestoreFromS3 stages and verifies an S3-compatible repository, then restores
// the requested point without overwriting an existing database. Provider
// compatibility remains the caller's responsibility; Floci is the only tested
// implementation and remote restore requires local temporary space proportional
// to the repository size.
func RestoreFromS3(ctx context.Context, store *storages3.Store, prefix, destinationPath string, target RestoreTarget) error {
	return recovery.RestoreS3ToTarget(ctx, store, prefix, destinationPath, recovery.Target{
		TXID:            target.TXID,
		TimestampMillis: target.TimestampMillis,
	})
}

// SyncToS3 immutably uploads the validated local LTX chain and persists a
// destination-scoped acknowledgement only after every object is confirmed.
// remoteID must be stable, non-secret, and unique to the configured endpoint;
// retries resend the complete chain and never delete local or remote data.
func SyncToS3(ctx context.Context, repositoryRoot string, store *storages3.Store, prefix, remoteID string) (SyncResult, error) {
	result, err := recovery.SyncFilesystemToS3(ctx, repositoryRoot, store, prefix, remoteID)
	if err != nil {
		return SyncResult{}, err
	}
	return SyncResult{
		TXID:             result.TXID,
		DatabaseChecksum: result.DatabaseChecksum,
		ObjectsConfirmed: result.ObjectsConfirmed,
		BytesConfirmed:   result.BytesConfirmed,
	}, nil
}

// SyncResult reports the validated chain position and objects confirmed by
// immutable put operations. Counts include matching objects already present.
type SyncResult struct {
	TXID             uint64
	DatabaseChecksum uint64
	ObjectsConfirmed int
	BytesConfirmed   int64
}
