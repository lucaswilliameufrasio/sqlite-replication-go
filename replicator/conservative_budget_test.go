package replicator

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestConservativeBudgetSaturationFailureAndRecovery(t *testing.T) {
	gate := NewAdmissionGate()
	if err := gate.Open(); err != nil {
		t.Fatal(err)
	}
	budget, err := NewConservativeBudget(gate, 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := budget.TryReserve(1); err == nil {
		t.Fatal("unknown occupancy accepted")
	}
	if err := budget.Reconcile(func() (uint64, error) { return 4, nil }); err != nil {
		t.Fatal(err)
	}
	reservation, err := budget.TryReserve(6)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := budget.TryReserve(1); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("saturated reservation: %v", err)
	}
	if err := budget.Reconcile(func() (uint64, error) { t.Fatal("active measurement"); return 0, nil }); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("active reconciliation: %v", err)
	}
	if err := reservation.Settle(7, true); err == nil {
		t.Fatal("underestimated growth accepted")
	}
	if _, err := budget.TryReserve(1); err == nil {
		t.Fatal("unknown occupancy accepted after underestimate")
	}
	measurementError := errors.New("measurement failed")
	if err := budget.Reconcile(func() (uint64, error) { return 0, measurementError }); !errors.Is(err, measurementError) {
		t.Fatalf("measurement error: %v", err)
	}
	if _, err := budget.TryReserve(1); err == nil {
		t.Fatal("failed measurement reopened budget")
	}
	if err := budget.Reconcile(func() (uint64, error) { return 3, nil }); err != nil {
		t.Fatal(err)
	}
	retry, err := budget.TryReserve(7)
	if err != nil {
		t.Fatal(err)
	}
	if err := retry.Settle(0, true); err != nil {
		t.Fatal(err)
	}
	if err := retry.Settle(0, true); err == nil {
		t.Fatal("double settlement accepted")
	}
}

func TestMeasuredReservedMutationExcludesReservationsAndRecovers(t *testing.T) {
	gate := NewAdmissionGate()
	if err := gate.Open(); err != nil {
		t.Fatal(err)
	}
	budget, err := NewConservativeBudget(gate, 10)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	finish := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- budget.WithMeasuredReservedMutation(6, func() (uint64, error) { return 4, nil }, func() error {
			close(entered)
			<-finish
			return nil
		})
	}()
	<-entered
	reserve := make(chan error, 1)
	go func() {
		_, err := budget.TryReserve(1)
		reserve <- err
	}()
	close(finish)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-reserve; err == nil {
		t.Fatal("reservation succeeded with unknown occupancy after mutation")
	}
	if err := budget.Reconcile(func() (uint64, error) { return 5, nil }); err != nil {
		t.Fatal(err)
	}
	reservation, err := budget.TryReserve(5)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.Settle(0, true); err != nil {
		t.Fatal(err)
	}
	called := false
	if err := budget.WithMeasuredReservedMutation(7, func() (uint64, error) { return 4, nil }, func() error { called = true; return nil }); !errors.Is(err, ErrBackpressure) || called {
		t.Fatalf("expected measured rejection: %v, called=%t", err, called)
	}
}

func TestConservativeBudgetReconcilesRealStagingAfterFailureAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stage")
	gate := NewAdmissionGate()
	if err := gate.Open(); err != nil {
		t.Fatal(err)
	}
	budget, err := NewConservativeBudget(gate, 8)
	if err != nil {
		t.Fatal(err)
	}
	measure := func() (uint64, error) {
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		return uint64(info.Size()), nil
	}
	if err := budget.Reconcile(measure); err != nil {
		t.Fatal(err)
	}
	failed := errors.New("injected failure after staging")
	if err := budget.WithOccupancyMutation(func() error {
		if err := os.WriteFile(path, []byte("12345"), 0o600); err != nil {
			return err
		}
		return failed
	}); !errors.Is(err, failed) {
		t.Fatalf("mutation: %v", err)
	}
	if _, err := budget.TryReserve(1); err == nil {
		t.Fatal("failed mutation should leave accounting unknown")
	}
	newGate := NewAdmissionGate()
	if err := newGate.Open(); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewConservativeBudget(newGate, 8)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.TryReserve(1); err == nil {
		t.Fatal("restart must begin unknown")
	}
	if err := restarted.Reconcile(measure); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.TryReserve(4); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("expected saturation: %v", err)
	}
	reservation, err := restarted.TryReserve(3)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.Settle(0, true); err != nil {
		t.Fatal(err)
	}
}

func TestConservativeBudgetConcurrentReservationsAndAbandonment(t *testing.T) {
	gate := NewAdmissionGate()
	if err := gate.Open(); err != nil {
		t.Fatal(err)
	}
	budget, err := NewConservativeBudget(gate, 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.Reconcile(func() (uint64, error) { return 2, nil }); err != nil {
		t.Fatal(err)
	}
	const writers = 12
	results := make(chan *BudgetReservation, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reservation, _ := budget.TryReserve(1)
			results <- reservation
		}()
	}
	wg.Wait()
	close(results)
	var accepted []*BudgetReservation
	for reservation := range results {
		if reservation != nil {
			accepted = append(accepted, reservation)
		}
	}
	if len(accepted) != 3 {
		t.Fatalf("accepted %d reservations, want 3", len(accepted))
	}
	if err := accepted[0].Abandon(); err == nil {
		t.Fatal("abandoned reservation must require reconciliation")
	}
	for _, reservation := range accepted[1:] {
		if err := reservation.Settle(1, true); err == nil {
			t.Fatal("settlement must not restore unknown occupancy")
		}
	}
	if _, err := budget.TryReserve(1); err == nil {
		t.Fatal("abandonment did not close accounting")
	}
	if err := budget.Reconcile(func() (uint64, error) { return 4, nil }); err != nil {
		t.Fatal(err)
	}
	gate.Cancel()
	if _, err := budget.TryReserve(1); !errors.Is(err, ErrAdmissionClosed) {
		t.Fatalf("cancelled admission: %v", err)
	}
}

func TestConservativeBudgetMutationFailureRequiresReconciliation(t *testing.T) {
	gate := NewAdmissionGate()
	if err := gate.Open(); err != nil {
		t.Fatal(err)
	}
	budget, err := NewConservativeBudget(gate, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.Reconcile(func() (uint64, error) { return 4, nil }); err != nil {
		t.Fatal(err)
	}
	reservation, err := budget.TryReserve(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.WithOccupancyMutation(func() error { t.Fatal("mutation with active writer"); return nil }); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("active mutation: %v", err)
	}
	if err := reservation.Settle(0, true); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("capture failed")
	if err := budget.WithOccupancyMutation(func() error { return failure }); !errors.Is(err, failure) {
		t.Fatalf("mutation error: %v", err)
	}
	if _, err := budget.TryReserve(1); err == nil {
		t.Fatal("failed mutation did not invalidate occupancy")
	}
	if err := budget.Reconcile(func() (uint64, error) { return 5, nil }); err != nil {
		t.Fatal(err)
	}
	retry, err := budget.TryReserve(5)
	if err != nil {
		t.Fatal(err)
	}
	if err := retry.Settle(0, true); err != nil {
		t.Fatal(err)
	}
}

func TestConservativeBudgetCopiedReservationCannotSettleTwice(t *testing.T) {
	gate := NewAdmissionGate()
	if err := gate.Open(); err != nil {
		t.Fatal(err)
	}
	budget, err := NewConservativeBudget(gate, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.Reconcile(func() (uint64, error) { return 0, nil }); err != nil {
		t.Fatal(err)
	}
	reservation, err := budget.TryReserve(6)
	if err != nil {
		t.Fatal(err)
	}
	copyOfReservation := *reservation
	if err := reservation.Settle(3, true); err != nil {
		t.Fatal(err)
	}
	if err := copyOfReservation.Settle(3, true); err == nil {
		t.Fatal("copied reservation settled twice")
	}
	retry, err := budget.TryReserve(7)
	if err != nil {
		t.Fatalf("budget corrupted by duplicate settlement: %v", err)
	}
	if err := retry.Settle(0, true); err != nil {
		t.Fatal(err)
	}
}

func TestReservedMutationRejectsBeforeOperationAndRecovers(t *testing.T) {
	budget, err := NewConservativeBudget(NewAdmissionGate(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.Reconcile(func() (uint64, error) { return 7, nil }); err != nil {
		t.Fatal(err)
	}
	called := false
	if err := budget.WithReservedMutation(4, func() error { called = true; return nil }); !errors.Is(err, ErrBackpressure) || called {
		t.Fatalf("saturated mutation = %v, called = %t", err, called)
	}
	failure := errors.New("staging failed")
	if err := budget.WithReservedMutation(3, func() error { return failure }); !errors.Is(err, failure) {
		t.Fatalf("mutation failure = %v", err)
	}
	if err := budget.WithReservedMutation(1, func() error { return nil }); err == nil {
		t.Fatal("unknown occupancy permitted mutation")
	}
	if err := budget.Reconcile(func() (uint64, error) { return 5, nil }); err != nil {
		t.Fatal(err)
	}
	if err := budget.WithReservedMutation(5, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
}
