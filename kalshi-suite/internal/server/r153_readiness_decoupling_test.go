package server

import (
	"testing"
	"time"
)

func TestR153DisabledPolyUSDestinationIsNotReadinessAuthority(t *testing.T) {
	s := testServer(t)
	s.pusAuthState = "ok" // configured collector/authentication alone is not money authority
	r149EnableLiveDestinations(s, true, false)
	if s.livePolyUSDestinationEnabled() {
		t.Fatal("disabled PolyUS destination was treated as LIVE authority")
	}
	r149EnableLiveDestinations(s, true, true)
	if !s.livePolyUSDestinationEnabled() {
		t.Fatal("enabled PolyUS Systems destination was not treated as LIVE authority")
	}
}

func TestR153ResearchRefreshCannotBlockExecutionProducerClock(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	executionRuns := 0

	runSignalLogTick(func() { executionRuns++ }, func() {
		close(started)
		<-release
		close(done)
	})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("research refresh did not start")
	}
	// The first research refresh remains blocked, but the next execution tick must still finish.
	runSignalLogTick(func() { executionRuns++ }, nil)
	if executionRuns != 2 {
		t.Fatalf("execution producer ran %d times; want 2 while research remained blocked", executionRuns)
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("research refresh did not finish after release")
	}
}

func TestR153ProducerTimeoutCoversDurableBatchButStaysInsideCadence(t *testing.T) {
	if signalProducerScanTimeout <= 30*time.Second || signalProducerScanTimeout >= 60*time.Second {
		t.Fatalf("signal producer timeout=%s; want >30s and <60s", signalProducerScanTimeout)
	}
}
