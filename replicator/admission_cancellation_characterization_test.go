package replicator_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	_ "modernc.org/sqlite"

	replication "github.com/lucaswilliameufrasio/sqlite-replication-go"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/replicator"
)

// This characterizes a non-cooperative callback, not a safe admission contract.
func TestCharacterizeCancellationInsideAdmissionCallbackCanAdmitWrite(t *testing.T) {
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.sqlite")
	createSourceDatabase(t, databasePath)
	repositoryRoot := filepath.Join(directory, "repository")
	if _, err := replication.Snapshot(context.Background(), databasePath, repositoryRoot); err != nil {
		t.Fatalf("create initial snapshot: %v", err)
	}
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open source database: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close source database: %v", err)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var transitions []bool
	admissionOpen := false
	writeAfterCancel := false
	config := coordinatorConfig(repositoryRoot, databasePath, 1)
	config.SetAdmission = func(callbackCtx context.Context, enabled bool, _ replicator.Limits) error {
		if enabled {
			cancel()
			admissionOpen = true
			transitions = append(transitions, true)
			if callbackCtx.Err() != nil && admissionOpen {
				if _, err := database.ExecContext(context.Background(), "INSERT INTO records(value) VALUES (?)", "after-cancel"); err != nil {
					return err
				}
				writeAfterCancel = true
			}
			return nil
		}
		admissionOpen = false
		transitions = append(transitions, false)
		return nil
	}
	coordinator, err := replicator.NewCoordinator(config)
	if err != nil {
		t.Fatalf("construct coordinator: %v", err)
	}
	if err := coordinator.Run(ctx, make(chan struct{})); !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context canceled", err)
	}
	if !writeAfterCancel {
		t.Fatal("expected synthetic writer to commit after cancellation")
	}
	if admissionOpen {
		t.Fatal("expected Run to close admission after observing cancellation")
	}
	if !reflect.DeepEqual(transitions, []bool{false, false, true, false}) {
		t.Fatalf("admission transitions = %v, want [false false true false]", transitions)
	}
	var count int
	if err := database.QueryRow("SELECT COUNT(*) FROM records WHERE value = ?", "after-cancel").Scan(&count); err != nil {
		t.Fatalf("query committed write: %v", err)
	}
	if count != 1 {
		t.Fatalf("writes after cancellation = %d, want 1", count)
	}
}
