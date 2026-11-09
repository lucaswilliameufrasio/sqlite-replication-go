package replicator

import (
	"context"
	"errors"
	"sync"
)

var ErrAdmissionClosed = errors.New("write admission is closed or cancelled")

// AdmissionGate coordinates cooperating writers within one process. It is not
// a durable queue, a byte quota, or protection against writers that bypass it.
// Construct it with NewAdmissionGate; do not copy it after first use.
type AdmissionGate struct {
	mu        sync.Mutex
	open      bool
	cancelled bool
	active    uint64
	drained   chan struct{}
}

// NewAdmissionGate constructs a gate that initially denies writes.
func NewAdmissionGate() *AdmissionGate {
	drained := make(chan struct{})
	close(drained)
	return &AdmissionGate{drained: drained}
}

// Open permits new writers unless Cancel has permanently closed the gate.
// Callers must enforce capacity limits separately before opening.
func (g *AdmissionGate) Open() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.cancelled {
		return ErrAdmissionClosed
	}
	g.open = true
	return nil
}

// Close stops new writers without cancelling those already accepted.
func (g *AdmissionGate) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.open = false
}

// Cancel permanently prevents both reopening and new acceptance. A write
// already accepted may complete after Cancel returns.
func (g *AdmissionGate) Cancel() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.cancelled = true
	g.open = false
}

// TryAccept returns a release function that must be called when the write
// finishes. Acceptance is atomic with Close and Cancel; release is idempotent.
func (g *AdmissionGate) TryAccept() (func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.open || g.cancelled {
		return nil, ErrAdmissionClosed
	}
	if g.active == ^uint64(0) {
		return nil, errors.New("write admission count exhausted")
	}
	if g.active == 0 {
		g.drained = make(chan struct{})
	}
	g.active++
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			g.active--
			if g.active == 0 {
				close(g.drained)
			}
		})
	}, nil
}

// Drain waits for accepted writers to finish; callers must close admission
// first and keep it closed until the operation requiring drainage completes.
func (g *AdmissionGate) Drain(ctx context.Context) error {
	g.mu.Lock()
	if g.open {
		g.mu.Unlock()
		return errors.New("cannot drain open admission")
	}
	drained := g.drained
	g.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-drained:
		return nil
	}
}
