package server

import (
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPolyWhaleRefreshSingleFlight pins the production crash from R137: the 200ms monitor and
// onLiveTradesPush used to enter refreshPolyWhales concurrently and write pWhaleSigSeen at the
// same time. The first refresh is deliberately held open while every competing caller attempts
// to enter; all competitors must return without running the refresh body.
func TestPolyWhaleRefreshSingleFlight(t *testing.T) {
	s := &Server{log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	const callers = 64
	start := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{}, callers)
	var entered atomic.Int32
	var active atomic.Int32
	var maxActive atomic.Int32

	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			<-start
			s.runPolyWhaleRefresh(func() {
				entered.Add(1)
				n := active.Add(1)
				for {
					old := maxActive.Load()
					if n <= old || maxActive.CompareAndSwap(old, n) {
						break
					}
				}
				<-release
				active.Add(-1)
			})
			done <- struct{}{}
		}()
	}
	close(start)

	deadline := time.After(2 * time.Second)
	for i := 0; i < callers-1; i++ {
		select {
		case <-done:
		case <-deadline:
			t.Fatalf("only %d/%d competing callers returned while the winner was held", i, callers-1)
		}
	}
	if got := entered.Load(); got != 1 {
		t.Fatalf("refresh bodies entered while one was already running: got %d, want 1", got)
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("maximum concurrent refresh bodies = %d, want 1", got)
	}

	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("winning refresh did not finish")
	}
	wg.Wait()

	// The latch must release after completion; otherwise one slow refresh would permanently kill
	// the feed even though it no longer crashes the process.
	s.runPolyWhaleRefresh(func() { entered.Add(1) })
	if got := entered.Load(); got != 2 {
		t.Fatalf("refresh latch did not reopen after completion: bodies=%d, want 2", got)
	}

	// runGuarded recovers refresh panics. The single-flight defer must still release the latch,
	// otherwise one malformed upstream payload would silently disable this feed forever.
	s.runPolyWhaleRefresh(func() { panic("synthetic refresh failure") })
	s.runPolyWhaleRefresh(func() { entered.Add(1) })
	if got := entered.Load(); got != 3 {
		t.Fatalf("refresh latch did not reopen after recovered panic: bodies=%d, want 3", got)
	}
}
