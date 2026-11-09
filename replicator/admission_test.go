package replicator_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/replicator"
)

func TestAdmissionGateDrainRequiresCloseAndAllReleases(t *testing.T) {
	gate := replicator.NewAdmissionGate()
	if err := gate.Open(); err != nil {
		t.Fatal(err)
	}
	first, err := gate.TryAccept()
	if err != nil {
		t.Fatal(err)
	}
	second, err := gate.TryAccept()
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.Drain(context.Background()); err == nil {
		t.Fatal("draining an open gate must fail")
	}
	gate.Close()
	first()
	first()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := gate.Drain(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("drain with second writer active: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- gate.Drain(context.Background())
	}()
	second()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("drain after both releases: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not complete after both releases")
	}
}

func TestAdmissionGateCancellationAndDrain(t *testing.T) {
	gate := replicator.NewAdmissionGate()
	if _, err := gate.TryAccept(); !errors.Is(err, replicator.ErrAdmissionClosed) {
		t.Fatalf("initial acceptance: %v", err)
	}
	if err := gate.Open(); err != nil {
		t.Fatal(err)
	}
	release, err := gate.TryAccept()
	if err != nil {
		t.Fatal(err)
	}
	gate.Cancel()
	if _, err := gate.TryAccept(); !errors.Is(err, replicator.ErrAdmissionClosed) {
		t.Fatalf("acceptance after cancel: %v", err)
	}
	if err := gate.Open(); !errors.Is(err, replicator.ErrAdmissionClosed) {
		t.Fatalf("reopen after cancel: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := gate.Drain(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("drain before release: %v", err)
	}
	release()
	release()
	if err := gate.Drain(context.Background()); err != nil {
		t.Fatalf("drain after release: %v", err)
	}
}

func TestAdmissionGateConcurrentCancelAndAcceptance(t *testing.T) {
	gate := replicator.NewAdmissionGate()
	if err := gate.Open(); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for range 32 {
		workers.Go(func() {
			for range 100 {
				release, err := gate.TryAccept()
				if err == nil {
					release()
				}
			}
		})
	}
	gate.Cancel()
	workers.Wait()
	if _, err := gate.TryAccept(); !errors.Is(err, replicator.ErrAdmissionClosed) {
		t.Fatalf("acceptance after cancel returned: %v", err)
	}
	if err := gate.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}
