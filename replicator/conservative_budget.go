package replicator

import (
	"errors"
	"sync"
)

// ConservativeBudget is experimental, process-local accounting for one
// resource. It is not a filesystem quota. All producers must cooperate, costs
// must be conservative, and occupancy must be measured again after restart.
// Do not copy this value. Callbacks must be bounded and must not reenter it.
type ConservativeBudget struct {
	mu       sync.Mutex
	gate     *AdmissionGate
	limit    uint64
	occupied uint64
	known    bool
	reserved uint64
	active   uint64
}

// BudgetReservation must be settled or abandoned exactly once, including on
// failed or cancelled writes. Unlike Rust, Go has no deterministic destructor.
type BudgetReservation struct {
	state *budgetReservationState
}

type budgetReservationState struct {
	budget   *ConservativeBudget
	cost     uint64
	release  func()
	finished bool
}

// NewConservativeBudget starts with unknown occupancy and denies reservations.
func NewConservativeBudget(gate *AdmissionGate, limit uint64) (*ConservativeBudget, error) {
	if gate == nil || limit == 0 {
		return nil, errors.New("budget requires a gate and nonzero limit")
	}
	return &ConservativeBudget{gate: gate, limit: limit}, nil
}

// Invalidate closes accounting even while reservations are active.
func (b *ConservativeBudget) Invalidate() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.known = false
}

// Reconcile measures while reservations through this budget are excluded.
// The caller must separately exclude capture and all other producers. An
// error leaves occupancy unknown; active reservations prevent measurement.
func (b *ConservativeBudget) Reconcile(measure func() (uint64, error)) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active != 0 {
		return ErrBackpressure
	}
	b.known = false
	if measure == nil {
		return errors.New("budget measurement is required")
	}
	value, err := measure()
	if err != nil {
		return err
	}
	b.occupied = value
	b.known = true
	return nil
}

// WithOccupancyMutation invalidates accounting before an operation such as
// capture and excludes reservations and reconciliation using this budget.
// It does not coordinate independent budgets or non-cooperating writers.
// The callback must not reenter this budget or wait for a writer that needs it.
func (b *ConservativeBudget) WithOccupancyMutation(operation func() error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active != 0 {
		return ErrBackpressure
	}
	b.known = false
	if operation == nil {
		return errors.New("budget operation is required")
	}
	return operation()
}

// WithReservedMutation checks a caller-supplied conservative upper bound before
// starting an exclusive mutation. Occupancy is invalidated before the callback,
// on success or failure. This does not measure actual allocation, reserve disk
// blocks, coordinate independent budgets, or prevent uncooperative writes.
// The caller must close and drain admission first and include all transient
// costs in the bound. The callback must not reenter or wait on this budget.
func (b *ConservativeBudget) WithReservedMutation(cost uint64, operation func() error) error {
	if cost == 0 || operation == nil {
		return errors.New("budget mutation requires a nonzero cost and operation")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active != 0 {
		return ErrBackpressure
	}
	if !b.known {
		return errors.New("budget occupancy unknown")
	}
	if b.occupied > b.limit || cost > b.limit-b.occupied {
		return ErrBackpressure
	}
	b.known = false
	return operation()
}

// WithMeasuredReservedMutation measures and starts a reserved mutation without
// releasing the budget mutex between them. Callers must first close and drain
// admission and must exclude mutations that bypass this budget. Both callbacks
// run under the mutex and must not reenter or wait for this budget. The measure
// must include every existing resource charged to this budget. This does not
// reserve physical space or hold a repository lock between the callbacks.
func (b *ConservativeBudget) WithMeasuredReservedMutation(cost uint64, measure func() (uint64, error), operation func() error) error {
	if cost == 0 || measure == nil || operation == nil {
		return errors.New("budget mutation requires nonzero cost, measurement and operation")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active != 0 {
		return ErrBackpressure
	}
	b.known = false
	occupied, err := measure()
	if err != nil {
		return err
	}
	b.occupied = occupied
	b.known = true
	if occupied > b.limit || cost > b.limit-occupied {
		return ErrBackpressure
	}
	b.known = false
	return operation()
}

// TryReserve rejects zero or unavailable capacity. It may wait on the mutex;
// it is not a bounded-latency or cancellation-aware operation.
func (b *ConservativeBudget) TryReserve(cost uint64) (*BudgetReservation, error) {
	if cost == 0 {
		return nil, errors.New("budget cost must be nonzero")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.known {
		return nil, errors.New("budget occupancy unknown")
	}
	if b.occupied > b.limit || b.reserved > b.limit-b.occupied || cost > b.limit-b.occupied-b.reserved {
		return nil, ErrBackpressure
	}
	release, err := b.gate.TryAccept()
	if err != nil {
		return nil, err
	}
	b.reserved += cost
	b.active++
	return &BudgetReservation{state: &budgetReservationState{budget: b, cost: cost, release: release}}, nil
}

// Settle accounts for a conservative increase; an underestimate or unknown
// increase invalidates occupancy. It releases admission even on error.
func (r *BudgetReservation) Settle(increase uint64, known bool) error {
	if r == nil || r.state == nil {
		return errors.New("invalid budget reservation")
	}
	s := r.state
	b := s.budget
	b.mu.Lock()
	if s.finished {
		b.mu.Unlock()
		return errors.New("reservation already finished")
	}
	s.finished = true
	if !known || increase > s.cost || !b.known || increase > ^uint64(0)-b.occupied {
		b.known = false
	} else {
		b.occupied += increase
	}
	b.reserved -= s.cost
	b.active--
	valid := b.known
	b.mu.Unlock()
	s.release()
	if !valid {
		return errors.New("budget occupancy requires reconciliation")
	}
	return nil
}

// Abandon invalidates the budget when the outcome cannot be measured.
func (r *BudgetReservation) Abandon() error {
	return r.Settle(0, false)
}
