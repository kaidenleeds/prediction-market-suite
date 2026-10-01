package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func r159ArbitrationCandidate(ticker string, lower float64) liveMirrorCandidate {
	return liveMirrorCandidate{
		Platform:         "kalshi",
		Ticker:           ticker,
		Side:             "YES",
		At:               time.Now(),
		ArbitrationQuote: liveMirrorQuote{Price: .50, Depth: 10},
		ArbitrationMean:  lower + .01,
		ArbitrationLower: lower,
	}
}

func r159ArbitrationResult(candidate liveMirrorCandidate) liveMirrorRankedCandidate {
	return liveMirrorRankedCandidate{
		Candidate:   candidate,
		Quote:       candidate.ArbitrationQuote,
		Mean:        candidate.ArbitrationMean,
		Lower:       candidate.ArbitrationLower,
		LowerPerDay: candidate.ArbitrationLower,
		MeanPerDay:  candidate.ArbitrationMean,
	}
}

func TestR159ReadyGraceJoinsThenRequeuesEveryUnfinishedCandidate(t *testing.T) {
	const extra = 4
	batch := make([]liveMirrorCandidate, 0, liveSignalPreflightWorkers+extra)
	for i := 0; i < liveSignalPreflightWorkers+extra; i++ {
		batch = append(batch, r159ArbitrationCandidate(
			fmt.Sprintf("KXR159UNRELATED%02d-CONTRACT", i),
			.40-float64(i)/1000,
		))
	}

	var active atomic.Int32
	var callsMu sync.Mutex
	calls := make(map[string]int, len(batch))
	startedSlow := make(chan struct{}, len(batch))
	preflight := func(ctx context.Context, candidate liveMirrorCandidate) (
		liveMirrorRankedCandidate, string) {
		callsMu.Lock()
		calls[candidate.Ticker]++
		callsMu.Unlock()
		if candidate.Ticker == batch[0].Ticker {
			return r159ArbitrationResult(candidate), ""
		}
		active.Add(1)
		startedSlow <- struct{}{}
		<-ctx.Done()
		active.Add(-1)
		return liveMirrorRankedCandidate{}, ctx.Err().Error()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results, requeue := (&Server{}).preflightLiveMirrorBatchWith(ctx, batch, preflight)

	if len(results) != 1 || results[0].candidate.Ticker != batch[0].Ticker ||
		results[0].why != "" {
		t.Fatalf("ready results=%+v, want only first eligible candidate", results)
	}
	if len(requeue) != len(batch)-1 {
		t.Fatalf("requeue=%d, want %d", len(requeue), len(batch)-1)
	}
	if active.Load() != 0 {
		t.Fatalf("%d old preflights were still running when candidates were requeued", active.Load())
	}
	if len(startedSlow) < liveSignalPreflightWorkers-1 {
		t.Fatalf("only %d slow preflights started; test did not exercise a saturated worker set",
			len(startedSlow))
	}

	accounted := map[string]int{results[0].candidate.Ticker: 1}
	for _, candidate := range requeue {
		accounted[candidate.Ticker]++
	}
	for _, candidate := range batch {
		if accounted[candidate.Ticker] != 1 {
			t.Fatalf("%s was accounted for %d times, want exactly once",
				candidate.Ticker, accounted[candidate.Ticker])
		}
	}
	callsMu.Lock()
	defer callsMu.Unlock()
	for ticker, count := range calls {
		if count != 1 {
			t.Fatalf("%s preflight ran %d times before the requeue fence", ticker, count)
		}
	}
}

func TestR159SlowStrongerSiblingRequeuesBothAfterJoin(t *testing.T) {
	strong := r159ArbitrationCandidate("KXR159SAMEEVENT-STRONG", .40)
	weak := r159ArbitrationCandidate("KXR159SAMEEVENT-WEAK", .20)
	batch := []liveMirrorCandidate{strong, weak}

	strongStarted := make(chan struct{})
	var startOnce sync.Once
	var active atomic.Int32
	var calls atomic.Int32
	preflight := func(ctx context.Context, candidate liveMirrorCandidate) (
		liveMirrorRankedCandidate, string) {
		calls.Add(1)
		if candidate.Ticker == strong.Ticker {
			active.Add(1)
			startOnce.Do(func() { close(strongStarted) })
			<-ctx.Done()
			active.Add(-1)
			return liveMirrorRankedCandidate{}, ctx.Err().Error()
		}
		<-strongStarted
		return r159ArbitrationResult(candidate), ""
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results, requeue := (&Server{}).preflightLiveMirrorBatchWith(ctx, batch, preflight)

	if len(results) != 0 {
		t.Fatalf("weaker sibling escaped while its stronger preflight was pending: %+v", results)
	}
	if len(requeue) != 2 || requeue[0].Ticker != strong.Ticker ||
		requeue[1].Ticker != weak.Ticker {
		t.Fatalf("same-event requeue=%+v, want both original candidates", requeue)
	}
	if active.Load() != 0 {
		t.Fatal("stronger sibling was requeued before its old preflight joined")
	}
	if calls.Load() != 2 {
		t.Fatalf("preflight calls=%d, want one per candidate", calls.Load())
	}
}

func TestR159ArbitrationRankMarksKalshiResidentBeforeHorizonCheck(t *testing.T) {
	raw, err := os.ReadFile("live_arbitration.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	start := strings.Index(source, "func (s *Server) preflightLiveMirrorRank")
	if start < 0 {
		t.Fatal("preflightLiveMirrorRank source block unavailable")
	}
	end := strings.Index(source[start+1:], "\nfunc ")
	if end < 0 {
		t.Fatal("preflightLiveMirrorRank source block unavailable")
	}
	source = source[start : start+1+end]
	residentAt := strings.Index(source, "ctx = r159KalshiResidentAdmissionContext(ctx)")
	horizonAt := strings.Index(source, "s.paperEntryHorizonNow(ctx")
	if residentAt < 0 || horizonAt <= residentAt {
		t.Fatalf("resident-only marker must precede horizon lookup: resident=%d horizon=%d",
			residentAt, horizonAt)
	}
}

func TestR159DisabledAutoComboBeltReturnsBeforeVenueWork(t *testing.T) {
	s := &Server{liveArmed: true, liveAuto: true}
	code, out := s.selfPOST(s.handleLiveComboPlace, "/api/live/combo/place", map[string]any{
		"collection_ticker": "KXR159-COMBO-COLLECTION",
		"auto":              true,
		"legs": []map[string]any{
			{"ticker": "KXR159-A", "event": "KXR159-EVENT-A", "side": "yes"},
			{"ticker": "KXR159-B", "event": "KXR159-EVENT-B", "side": "no"},
		},
	})
	if code != http.StatusForbidden || out["error"] != "AUTO combo belt is disabled" {
		t.Fatalf("disabled AUTO combo response=%d %v", code, out)
	}

	raw, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	start := strings.Index(source, "func (s *Server) handleLiveComboPlace")
	if start < 0 {
		t.Fatal("handleLiveComboPlace source block unavailable")
	}
	end := strings.Index(source[start+1:], "\nfunc ")
	if end < 0 {
		t.Fatal("handleLiveComboPlace source block unavailable")
	}
	source = source[start : start+1+end]
	gateAt := strings.Index(source, "if body.Auto && !s.liveAutoCombosEnabled()")
	riskAt := strings.Index(source, "s.liveVenueBudgetCheckForBundle")
	venueAt := strings.Index(source, "s.kal.AcceptQuoteRFQ")
	if gateAt < 0 || riskAt <= gateAt || venueAt <= gateAt {
		t.Fatalf("disabled AUTO gate must precede risk and venue work: gate=%d risk=%d venue=%d",
			gateAt, riskAt, venueAt)
	}
}
