package replicator

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/capture"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/repositorylock"
	_ "modernc.org/sqlite"
)

func TestMeasuredCaptureExcludesOtherRepositoryOperations(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		t.Skip("incremental capture requires Linux amd64")
	}
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "source.sqlite")
	repository := filepath.Join(dir, "repository")
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE data(value TEXT); INSERT INTO data VALUES ('base')"); err != nil {
		t.Fatal(err)
	}
	if _, err := replication.Snapshot(ctx, source, repository); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE data SET value='retry'"); err != nil {
		t.Fatal(err)
	}
	budget, err := NewConservativeBudget(NewAdmissionGate(), 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	gate := func(context.Context) (func() error, error) { return func() error { return nil }, nil }
	limits := CaptureLimits{RecoveryImage: 1 << 20, BackupImage: 1 << 20, LTXBatch: 1 << 20, Spool: 4 << 20}
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	injected := errors.New("injected capture failure")
	go func() {
		_, err := captureOnceWithMeasuredReservation(ctx, source, repository, gate, budget, limits, func(point capture.FaultPoint, _ int) error {
			if point == capture.AfterLeaseAcquire {
				close(entered)
				<-release
				return injected
			}
			return nil
		})
		done <- err
	}()
	<-entered
	contender, err := repositorylock.AcquireExclusive(repository)
	if err == nil {
		_ = contender.Close()
		close(release)
		t.Fatal("second repository operation acquired lock during capture")
	}
	close(release)
	if err := <-done; !errors.Is(err, injected) {
		t.Fatalf("capture failure: %v", err)
	}
	result, err := CaptureOnceWithMeasuredReservation(ctx, source, repository, gate, budget, limits)
	if err != nil || result.TXID != 2 {
		t.Fatalf("retry after lock release: %+v %v", result, err)
	}
}
