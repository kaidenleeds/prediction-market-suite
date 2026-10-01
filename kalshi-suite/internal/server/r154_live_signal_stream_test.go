package server

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r154StreamIntent(ticker string, point float64, at time.Time) liveSignalIntent {
	return liveSignalIntent{Signal: storage.Signal{
		Platform: "kalshi", Ticker: ticker, Side: "YES", SignalType: "kalshi-flow",
	}, Point: point, At: at}
}

func TestR154StreamingPreflightAcceptedCandidateDoesNotWaitForSlowSibling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slowRelease := make(chan struct{})
	slowDone := make(chan struct{})
	fastDone := make(chan struct{})
	scheduler := newLiveSignalPreflightScheduler(nil, ctx, 2, 8,
		func(runCtx context.Context, intent liveSignalIntent) bool {
			switch intent.Signal.Ticker {
			case "SLOW-STRONG":
				defer close(slowDone)
				select {
				case <-slowRelease:
				case <-runCtx.Done():
				}
				return false
			case "FAST-ACCEPT":
				close(fastDone)
				return true
			default:
				return false
			}
		}, nil)

	now := time.Now()
	started := time.Now()
	batch := scheduler.submit(ctx, []liveSignalIntent{
		r154StreamIntent("FAST-ACCEPT", .80, now),
		r154StreamIntent("SLOW-STRONG", .90, now),
	})
	waitCtx, waitCancel := context.WithTimeout(ctx, time.Second)
	defer waitCancel()
	if accepted := batch.wait(waitCtx); accepted != 1 {
		t.Fatalf("accepted=%d, want first ready accepted candidate", accepted)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("fast accepted sibling waited %s for slow preflight", elapsed)
	}
	select {
	case <-fastDone:
	default:
		t.Fatal("batch returned before the accepted candidate completed")
	}
	select {
	case <-slowDone:
		t.Fatal("batch waited for the deliberately blocked sibling")
	default:
	}
	close(slowRelease)
	select {
	case <-slowDone:
	case <-time.After(time.Second):
		t.Fatal("slow sibling did not finish after release")
	}
}

func TestR154StreamingPreflightStartsStrongestMeasuredPointFirst(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		mu      sync.Mutex
		started []string
	)
	scheduler := newLiveSignalPreflightScheduler(nil, ctx, 1, 8,
		func(_ context.Context, intent liveSignalIntent) bool {
			mu.Lock()
			started = append(started, intent.Signal.Ticker)
			mu.Unlock()
			return false
		}, nil)

	now := time.Now()
	batch := scheduler.submit(ctx, []liveSignalIntent{
		r154StreamIntent("WEAK", .10, now),
		r154StreamIntent("STRONG", .90, now),
		r154StreamIntent("MIDDLE", .40, now),
	})
	waitCtx, waitCancel := context.WithTimeout(ctx, time.Second)
	defer waitCancel()
	if accepted := batch.wait(waitCtx); accepted != 0 {
		t.Fatalf("accepted=%d, want zero from rejecting test runner", accepted)
	}
	mu.Lock()
	got := append([]string(nil), started...)
	mu.Unlock()
	want := []string{"STRONG", "MIDDLE", "WEAK"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("preflight start order=%v, want measured-point order %v", got, want)
	}
}

func TestR154StreamingPreflightPreservesTTLAndClockSkewDrops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var runs atomic.Int32
	var (
		mu      sync.Mutex
		reasons []string
	)
	scheduler := newLiveSignalPreflightScheduler(nil, ctx, 2, 8,
		func(context.Context, liveSignalIntent) bool {
			runs.Add(1)
			return true
		},
		func(_ liveSignalIntent, _, reason string, _ map[string]any) {
			mu.Lock()
			reasons = append(reasons, reason)
			mu.Unlock()
		})

	now := time.Now()
	batch := scheduler.submit(ctx, []liveSignalIntent{
		r154StreamIntent("EXPIRED", .9, now.Add(-liveMirrorTTL-time.Second)),
		r154StreamIntent("FUTURE", .8, now.Add(time.Second)),
	})
	waitCtx, waitCancel := context.WithTimeout(ctx, time.Second)
	defer waitCancel()
	if accepted := batch.wait(waitCtx); accepted != 0 {
		t.Fatalf("accepted=%d, want stale/skewed candidates rejected", accepted)
	}
	if got := runs.Load(); got != 0 {
		t.Fatalf("preflight runner called %d times for stale/skewed candidates", got)
	}
	mu.Lock()
	got := append([]string(nil), reasons...)
	mu.Unlock()
	want := []string{"expired-before-live-first-preflight", "signal-intent-clock-skew"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("durable drop reasons=%v, want %v", got, want)
	}
}

func TestR154StreamingPreflightBoundedQueueEvictsWeakestPendingPoint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blockerRelease := make(chan struct{})
	blockerStarted := make(chan struct{})
	var (
		mu      sync.Mutex
		started []string
		dropped []string
	)
	scheduler := newLiveSignalPreflightScheduler(nil, ctx, 1, 2,
		func(runCtx context.Context, intent liveSignalIntent) bool {
			if intent.Signal.Ticker == "BLOCKER" {
				close(blockerStarted)
				select {
				case <-blockerRelease:
				case <-runCtx.Done():
				}
				return false
			}
			mu.Lock()
			started = append(started, intent.Signal.Ticker)
			mu.Unlock()
			return false
		},
		func(intent liveSignalIntent, _, reason string, _ map[string]any) {
			if reason == "signal-preflight-queue-cap-weaker-proxy" {
				mu.Lock()
				dropped = append(dropped, intent.Signal.Ticker)
				mu.Unlock()
			}
		})

	now := time.Now()
	blockerBatch := scheduler.submit(ctx, []liveSignalIntent{r154StreamIntent("BLOCKER", 1, now)})
	select {
	case <-blockerStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not start blocker")
	}
	weakBatch := scheduler.submit(ctx, []liveSignalIntent{
		r154StreamIntent("WEAK", .10, now),
		r154StreamIntent("MIDDLE", .40, now),
	})
	strongBatch := scheduler.submit(ctx, []liveSignalIntent{
		r154StreamIntent("STRONG", .90, now),
	})
	close(blockerRelease)

	for _, batch := range []*liveSignalPreflightBatch{blockerBatch, weakBatch, strongBatch} {
		waitCtx, waitCancel := context.WithTimeout(ctx, time.Second)
		_ = batch.wait(waitCtx)
		waitCancel()
	}
	mu.Lock()
	gotStarted := append([]string(nil), started...)
	gotDropped := append([]string(nil), dropped...)
	mu.Unlock()
	if want := []string{"STRONG", "MIDDLE"}; !reflect.DeepEqual(gotStarted, want) {
		t.Fatalf("pending start order=%v, want %v", gotStarted, want)
	}
	if want := []string{"WEAK"}; !reflect.DeepEqual(gotDropped, want) {
		t.Fatalf("queue-cap drops=%v, want weakest pending %v", gotDropped, want)
	}
}
