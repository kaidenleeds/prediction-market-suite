// Package killswitch provides a process-wide trading halt. Tripping it is the
// single most important safety primitive: when set, the trading loops (added in
// later phases) must stop opening positions and cancel resting orders.
package killswitch

import (
	"sync"
	"time"
)

type State struct {
	Tripped bool      `json:"tripped"`
	Reason  string    `json:"reason,omitempty"`
	At      time.Time `json:"at,omitempty"`
}

type Switch struct {
	mu      sync.RWMutex
	tripped bool
	reason  string
	at      time.Time
	onTrip  func(reason string) // optional side effect, fired once per trip
}

// New returns a kill switch. onTrip (may be nil) fires the first time the switch
// transitions from clear to tripped — use it to log/audit and cancel orders.
func New(onTrip func(reason string)) *Switch {
	return &Switch{onTrip: onTrip}
}

// Trip halts trading. Idempotent: the onTrip side effect fires only on the
// clear -> tripped transition.
func (s *Switch) Trip(reason string) {
	s.mu.Lock()
	already := s.tripped
	s.tripped = true
	s.reason = reason
	s.at = time.Now()
	cb := s.onTrip
	s.mu.Unlock()

	if !already && cb != nil {
		cb(reason)
	}
}

// Reset clears the switch (manual re-arm).
func (s *Switch) Reset() {
	s.mu.Lock()
	s.tripped = false
	s.reason = ""
	s.at = time.Time{}
	s.mu.Unlock()
}

// Restore reinstates a persisted trip without firing onTrip. The owner calls its halt/cancel
// recovery after all venue clients are attached; restoring state must never masquerade as a new
// operator trip or launch network work from a half-built server.
func (s *Switch) Restore(state State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tripped = state.Tripped
	s.reason = state.Reason
	s.at = state.At
	if s.tripped && s.at.IsZero() {
		s.at = time.Now()
	}
}

func (s *Switch) Tripped() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tripped
}

func (s *Switch) State() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return State{Tripped: s.tripped, Reason: s.reason, At: s.at}
}
