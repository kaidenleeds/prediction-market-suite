package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestR163ExecutionShadowEndpointPollingAndWriterContentionLoseNoRecords(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)

	// Hold the worker lock before its cold start. This is stricter than the observed runtime
	// collision: the producer has no unlocked common path available, while the diagnostic endpoint
	// must remain responsive without touching the queue mutex.
	s.executionShadowMu.Lock()
	firstPoll := make(chan int, 1)
	go func() {
		response := httptest.NewRecorder()
		s.handleExecutionShadow(response,
			httptest.NewRequest(http.MethodGet, "/api/execution-shadow?limit=1", nil))
		firstPoll <- response.Code
	}()
	select {
	case code := <-firstPoll:
		if code != http.StatusOK {
			s.executionShadowMu.Unlock()
			t.Fatalf("diagnostic endpoint status=%d while producer mutex held", code)
		}
	case <-time.After(time.Second):
		s.executionShadowMu.Unlock()
		t.Fatal("diagnostic endpoint waited on the producer mutex")
	}

	pollStop := make(chan struct{})
	pollErr := make(chan error, 1)
	var pollWG sync.WaitGroup
	pollWG.Add(1)
	go func() {
		defer pollWG.Done()
		for {
			select {
			case <-pollStop:
				return
			default:
			}
			response := httptest.NewRecorder()
			s.handleExecutionShadow(response,
				httptest.NewRequest(http.MethodGet, "/api/execution-shadow?limit=1", nil))
			if response.Code != http.StatusOK {
				select {
				case pollErr <- fmt.Errorf("diagnostic endpoint status=%d body=%s",
					response.Code, response.Body.String()):
				default:
				}
				return
			}
		}
	}()

	const groups = 1000
	started := time.Now()
	for i := 0; i < groups; i++ {
		at := time.Now().UTC()
		candidate := liveMirrorCandidate{
			Platform: "kalshi", Ticker: fmt.Sprintf("KXR163-CONTENTION-%04d", i),
			Title: "contention stress", Side: "YES", Action: "BUY",
			Family: "kalshi-flow", Source: "stress", Price: .40, At: at,
		}
		candidate, attempt, ok := s.executionShadowBuildCandidate(candidate,
			"contention-stress", "")
		if !ok {
			s.executionShadowMu.Unlock()
			t.Fatalf("could not build stress attempt %d", i)
		}
		event := s.executionShadowEvent(candidate.ShadowAttemptID,
			"detector-branch", "qualified", "stress", at)
		if !s.enqueueExecutionShadowWriteGroup([]executionShadowWrite{
			{attempt: &attempt}, {event: &event},
		}, false) {
			s.executionShadowMu.Unlock()
			t.Fatalf("lock-free stress admission %d was refused", i)
		}
	}
	producerElapsed := time.Since(started)
	s.executionShadowMu.Unlock()

	if producerElapsed > 2*time.Second {
		t.Fatalf("nonblocking producer took %v for %d two-record groups", producerElapsed, groups)
	}
	if got := s.executionShadowRerouted.Load(); got != groups*2 {
		t.Fatalf("rerouted records=%d want=%d", got, groups*2)
	}
	if got := s.executionShadowDropContention.Load(); got != 0 {
		t.Fatalf("contention drops=%d want=0", got)
	}
	if got := s.executionShadowDropped.Load(); got != 0 {
		t.Fatalf("total drops=%d want=0", got)
	}

	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 30*time.Second)
	err := s.stopExecutionShadowWriter(drainCtx)
	cancelDrain()
	close(pollStop)
	pollWG.Wait()
	select {
	case err := <-pollErr:
		t.Fatal(err)
	default:
	}
	if err != nil {
		t.Fatalf("drain failed: %v", err)
	}

	var attempts, events int
	if err := st.ExecutionShadowDBForTest().QueryRow(
		`SELECT COUNT(*) FROM execution_shadow_attempts`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := st.ExecutionShadowDBForTest().QueryRow(
		`SELECT COUNT(*) FROM execution_shadow_events`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if attempts != groups || events != groups {
		t.Fatalf("persisted attempts/events=%d/%d want=%d/%d",
			attempts, events, groups, groups)
	}
	if s.executionShadowOccupancy.Load() != 0 ||
		s.executionShadowInFlightCount.Load() != 0 {
		t.Fatalf("drain left occupancy/inflight=%d/%d",
			s.executionShadowOccupancy.Load(), s.executionShadowInFlightCount.Load())
	}

	// 2,000 records in at most two seconds is at least 1,000 records/s, comfortably above the
	// measured ~42 records/s runtime load.
}

func TestR163ExecutionShadowColdNoWakeThenExplicitWakeCannotBeLost(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	at := time.Now().UTC()
	candidate := liveMirrorCandidate{
		Platform: "kalshi", Ticker: "KXR163-COLD-WAKE", Title: "cold wake",
		Side: "YES", Action: "BUY", Family: "kalshi-flow", Source: "stress",
		Price: .40, At: at,
	}
	_, attempt, ok := s.executionShadowBuildCandidate(candidate, "cold-wake-race", "")
	if !ok {
		t.Fatal("could not build cold-wake attempt")
	}

	// Force the no-wake write onto the lock-free ingress, then issue the one explicit wake while
	// the cold-start goroutine is still unable to publish its channel.
	s.executionShadowMu.Lock()
	if !s.enqueueExecutionShadowWriteGroup(
		[]executionShadowWrite{{attempt: &attempt}}, false) {
		s.executionShadowMu.Unlock()
		t.Fatal("cold no-wake group was refused")
	}
	s.wakeExecutionShadowWriter()
	s.executionShadowMu.Unlock()

	deadline := time.Now().Add(3 * time.Second)
	for {
		var persisted int
		err := st.ExecutionShadowDBForTest().QueryRow(
			`SELECT COUNT(*) FROM execution_shadow_attempts WHERE attempt_id=?`,
			attempt.AttemptID).Scan(&persisted)
		if err != nil {
			t.Fatal(err)
		}
		if persisted == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the explicit wake was lost during cold lock contention")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestR163ExecutionShadowEmptyWorkerStopClosesDirectSession(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	at := time.Now().UTC()
	candidate := liveMirrorCandidate{
		Platform: "kalshi", Ticker: "KXR163-DIRECT-SESSION", Title: "direct session",
		Side: "YES", Action: "BUY", Family: "kalshi-flow", Source: "test",
		Price: .40, At: at,
	}
	_, attempt, ok := s.executionShadowBuildCandidate(candidate, "direct-session", "")
	if !ok {
		t.Fatal("could not build direct-session attempt")
	}
	if err := s.persistExecutionShadowWrite(context.Background(),
		executionShadowWrite{attempt: &attempt}); err != nil {
		t.Fatal(err)
	}
	if s.executionShadowWake != nil {
		t.Fatal("direct isolated-store fixture unexpectedly started the queue worker")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(stopCtx); err != nil {
		t.Fatal(err)
	}
	status := st.ExecutionShadowStatus(context.Background())
	if !status.LatestClean || status.LatestEndedAt == "" ||
		status.LatestCommitted != 1 {
		t.Fatalf("direct session was not closed cleanly: %+v", status)
	}
	if err := s.persistExecutionShadowWrite(context.Background(),
		executionShadowWrite{attempt: &attempt}); err == nil {
		t.Fatal("direct isolated-store helper bypassed the permanent shutdown fence")
	}
}
