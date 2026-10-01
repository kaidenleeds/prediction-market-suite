package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r156LatencyIntent(ticker string, point float64, at time.Time) liveSignalIntent {
	return liveSignalIntent{Signal: storage.Signal{
		Platform: "kalshi", Ticker: ticker, Side: "YES", SignalType: "kalshi-flow",
	}, Point: point, At: at}
}

func TestR156SpotlagDetectorCadenceStaysInsideFiveSeconds(t *testing.T) {
	if spotlagDetectorInterval <= 0 || spotlagDetectorInterval > 5*time.Second {
		t.Fatalf("Spot-lag detector cadence=%s, want positive and no slower than 5s", spotlagDetectorInterval)
	}
}

func TestR156SignalIntakeSubmitsLaterBurstWithoutWaitingForEarlierPreflight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slowStarted := make(chan struct{})
	slowRelease := make(chan struct{})
	acceptedDispatch := make(chan string, 2)
	var slowRuns, urgentRuns atomic.Int32
	scheduler := newLiveSignalPreflightScheduler(nil, ctx, 2, 8,
		func(runCtx context.Context, intent liveSignalIntent) bool {
			switch intent.Signal.Ticker {
			case "SLOW-EARLIER":
				slowRuns.Add(1)
				close(slowStarted)
				select {
				case <-slowRelease:
				case <-runCtx.Done():
				}
				return false
			case "URGENT-LATER":
				urgentRuns.Add(1)
				acceptedDispatch <- intent.Signal.Ticker
				return true
			default:
				return false
			}
		}, nil)
	s := &Server{}
	if _, loaded := liveSignalPreflightSchedulers.LoadOrStore(s, scheduler); loaded {
		scheduler.stop()
		t.Fatal("test server unexpectedly already had a preflight scheduler")
	}
	defer func() {
		liveSignalPreflightSchedulers.CompareAndDelete(s, scheduler)
		scheduler.stop()
	}()

	now := time.Now()
	firstReturned := make(chan *liveSignalPreflightBatch, 1)
	go func() {
		firstReturned <- s.submitLiveSignalIntentBatch(ctx, []liveSignalIntent{
			r156LatencyIntent("SLOW-EARLIER", .80, now),
		})
	}()
	var first *liveSignalPreflightBatch
	select {
	case first = <-firstReturned:
	case <-time.After(250 * time.Millisecond):
		close(slowRelease)
		t.Fatal("raw-intent submission waited for the earlier preflight to finish")
	}
	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		close(slowRelease)
		t.Fatal("earlier preflight did not start")
	}

	later := s.submitLiveSignalIntentBatch(ctx, []liveSignalIntent{
		r156LatencyIntent("URGENT-LATER", .95, time.Now()),
	})
	select {
	case got := <-acceptedDispatch:
		if got != "URGENT-LATER" {
			t.Fatalf("accepted dispatch=%q, want URGENT-LATER", got)
		}
	case <-time.After(250 * time.Millisecond):
		close(slowRelease)
		t.Fatal("later urgent burst did not enter a free worker while the earlier batch was running")
	}
	select {
	case <-first.ready:
		close(slowRelease)
		t.Fatal("earlier blocked batch finished before the later accepted candidate dispatched")
	default:
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, time.Second)
	if accepted := later.wait(waitCtx); accepted != 1 {
		waitCancel()
		close(slowRelease)
		t.Fatalf("later accepted count=%d, want 1", accepted)
	}
	waitCancel()

	close(slowRelease)
	waitCtx, waitCancel = context.WithTimeout(ctx, time.Second)
	if accepted := first.wait(waitCtx); accepted != 0 {
		waitCancel()
		t.Fatalf("earlier accepted count=%d, want 0", accepted)
	}
	waitCancel()
	if got := slowRuns.Load(); got != 1 {
		t.Fatalf("earlier runner calls=%d, want exactly 1", got)
	}
	if got := urgentRuns.Load(); got != 1 {
		t.Fatalf("urgent runner calls=%d, want exactly 1", got)
	}
	select {
	case duplicate := <-acceptedDispatch:
		t.Fatalf("accepted candidate dispatched twice: %q", duplicate)
	default:
	}
}

func TestR156RouteProofCacheCoalescesConcurrentSameKeyColdReads(t *testing.T) {
	var cache r154LiveRouteProofCache
	now := time.Now()
	key := r154LiveRouteProofKey("kalshi-flow", "kalshi", "strategy", "YES")
	const readers = 16

	start := make(chan struct{})
	loadStarted := make(chan struct{})
	releaseLoad := make(chan struct{})
	results := make(chan storage.UnitTrialLeaderboardStat, readers)
	errs := make(chan error, readers)
	var calls atomic.Int32
	var startOnce sync.Once
	var ready sync.WaitGroup
	ready.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			ready.Done()
			<-start
			stat, found, err := cache.get(context.Background(), now, key, func() (
				storage.UnitTrialLeaderboardStat, bool, error) {
				calls.Add(1)
				startOnce.Do(func() { close(loadStarted) })
				<-releaseLoad
				return storage.UnitTrialLeaderboardStat{SettledMarkets: 91, MeanPC: .04}, true, nil
			})
			if err == nil && !found {
				err = context.Canceled
			}
			results <- stat
			errs <- err
		}()
	}
	ready.Wait()
	close(start)
	select {
	case <-loadStarted:
	case <-time.After(time.Second):
		t.Fatal("cold route-proof load did not start")
	}
	if got := calls.Load(); got != 1 {
		close(releaseLoad)
		t.Fatalf("concurrent cold loads started %d storage reads, want 1", got)
	}
	select {
	case <-results:
		close(releaseLoad)
		t.Fatal("a cold proof reader returned before the shared load completed")
	default:
	}
	close(releaseLoad)

	for i := 0; i < readers; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("reader %d error=%v", i, err)
		}
		if got := <-results; got.SettledMarkets != 91 || got.MeanPC != .04 {
			t.Fatalf("reader %d proof=%+v", i, got)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("concurrent same-key cold reads called storage %d times, want 1", got)
	}
}

func TestR156RouteProofCacheSharedReadWaiterHonorsOwnContext(t *testing.T) {
	var cache r154LiveRouteProofCache
	now := time.Now()
	key := r154LiveRouteProofKey("spotlag", "kalshi", "strategy", "NO")
	loadStarted := make(chan struct{})
	releaseLoad := make(chan struct{})
	leaderDone := make(chan error, 1)
	var loads atomic.Int32

	go func() {
		_, _, err := cache.get(context.Background(), now, key, func() (
			storage.UnitTrialLeaderboardStat, bool, error) {
			loads.Add(1)
			close(loadStarted)
			<-releaseLoad
			return storage.UnitTrialLeaderboardStat{SettledMarkets: 88}, true, nil
		})
		leaderDone <- err
	}()
	select {
	case <-loadStarted:
	case <-time.After(time.Second):
		t.Fatal("leader proof read did not start")
	}

	waiterCtx, waiterCancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer waiterCancel()
	waiterDone := make(chan error, 1)
	go func() {
		_, _, err := cache.get(waiterCtx, now, key, func() (
			storage.UnitTrialLeaderboardStat, bool, error) {
			loads.Add(1)
			return storage.UnitTrialLeaderboardStat{}, false, nil
		})
		waiterDone <- err
	}()
	select {
	case err := <-waiterDone:
		if err != context.DeadlineExceeded {
			close(releaseLoad)
			<-leaderDone
			t.Fatalf("shared-read waiter error=%v, want context deadline exceeded", err)
		}
	case <-time.After(250 * time.Millisecond):
		close(releaseLoad)
		<-leaderDone
		t.Fatal("shared-read waiter remained stranded behind the leader past its own deadline")
	}
	if got := loads.Load(); got != 1 {
		close(releaseLoad)
		<-leaderDone
		t.Fatalf("deadline waiter started an extra proof read; loads=%d, want 1", got)
	}
	close(releaseLoad)
	if err := <-leaderDone; err != nil {
		t.Fatalf("leader proof read error=%v", err)
	}
}
