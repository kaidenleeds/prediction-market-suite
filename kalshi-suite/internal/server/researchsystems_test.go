package server

import (
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func lifecycleEvent(ticker, kind, close string) kalshi.MarketLifecycleEvent {
	return kalshi.MarketLifecycleEvent{Ticker: ticker, EventType: kind, CloseTime: close, Observed: time.Now()}
}

func TestLifecycleWSFilterRejectsFullBoardCloseDateBaselines(t *testing.T) {
	f := &lifecycleWSFilter{}
	for i := 0; i < 10_000; i++ {
		if f.accept(lifecycleEvent(fmt.Sprintf("KX%05d", i), "close_date_updated", "2026-07-12T00:00:00Z")) {
			t.Fatalf("first close date for ticker %d was treated as a change", i)
		}
	}
	if len(f.byTicker) != 10_000 {
		t.Fatalf("baseline coverage=%d", len(f.byTicker))
	}
}

func TestLifecycleWSFilterAdmitsOnlyGenuineReopenSequences(t *testing.T) {
	f := &lifecycleWSFilter{}
	if f.accept(lifecycleEvent("KXBASE", "activated", "")) {
		t.Fatal("initial activation became a reopen")
	}
	if !f.accept(lifecycleEvent("KXBASE", "deactivated", "")) {
		t.Fatal("deactivation was not retained")
	}
	if f.accept(lifecycleEvent("KXBASE", "deactivated", "")) {
		t.Fatal("duplicate deactivation was retained")
	}
	if !f.accept(lifecycleEvent("KXBASE", "activated", "")) {
		t.Fatal("activation after deactivation was not retained")
	}
	if f.accept(lifecycleEvent("KXBASE", "activated", "")) {
		t.Fatal("duplicate activation became a reopen")
	}

	if f.accept(lifecycleEvent("KXCLOSE", "close_date_updated", "2026-07-12T00:00:00Z")) {
		t.Fatal("first close date became a change")
	}
	if f.accept(lifecycleEvent("KXCLOSE", "close_date_updated", "2026-07-12T00:00:00Z")) {
		t.Fatal("identical close date became a change")
	}
	if !f.accept(lifecycleEvent("KXCLOSE", "close_date_updated", "2026-07-13T00:00:00Z")) {
		t.Fatal("actual close-date change was not retained")
	}
	if !f.accept(lifecycleEvent("KXCLOSE", "activated", "")) {
		t.Fatal("activation after a real close-date change was not retained")
	}
}

func TestLifecycleWSFilterClassifiesBaselinesDuplicatesAndReopens(t *testing.T) {
	f := &lifecycleWSFilter{}
	d := f.classify(lifecycleEvent("KXCLOSE", "close_date_updated", "2026-07-12T00:00:00Z"))
	if !d.Baseline || d.Accepted || d.Exclusion != "first_close_date_baseline" {
		t.Fatalf("first close=%+v", d)
	}
	d = f.classify(lifecycleEvent("KXCLOSE", "close_date_updated", "2026-07-12T00:00:00Z"))
	if d.Accepted || d.Exclusion != "unchanged_close_date" {
		t.Fatalf("duplicate close=%+v", d)
	}
	d = f.classify(lifecycleEvent("KXCLOSE", "close_date_updated", "2026-07-13T00:00:00Z"))
	if !d.Accepted || d.Classification != "close_date_change" {
		t.Fatalf("changed close=%+v", d)
	}
	d = f.classify(lifecycleEvent("KXCLOSE", "activated", ""))
	if !d.Accepted || !d.GenuineReopen || d.Classification != "genuine_reopen" {
		t.Fatalf("reopen=%+v", d)
	}
	d = f.classify(lifecycleEvent("KXINITIAL", "activated", ""))
	if !d.Baseline || d.Accepted || d.Exclusion != "initial_activation_without_prior_transition" {
		t.Fatalf("initial activation=%+v", d)
	}
}

func TestLifecycleCollectorCountsRawBaselineReopenAndHorizonsSeparately(t *testing.T) {
	a := newLifecycleCollectorAccumulator()
	a.noteFrame(lifecycleFrameDecision{Baseline: true, Classification: "baseline", Exclusion: "first_close_date_baseline"})
	a.noteFrame(lifecycleFrameDecision{Accepted: true, GenuineReopen: true, Classification: "genuine_reopen"})
	a.noteTransition(true, true, nil)
	a.noteHorizonDueAttempt()
	a.noteHorizonSample(true, nil)
	_, counts, ok := a.drain(time.Now())
	if !ok || counts.RawFrames != 2 || counts.BaselineFrames != 1 || counts.AcceptedTransitions != 1 ||
		counts.GenuineReopens != 1 || counts.TransitionInserted != 1 || counts.HorizonDue != 1 ||
		counts.HorizonAttempts != 1 || counts.HorizonSamples != 1 || counts.Exclusions["first_close_date_baseline"] != 1 {
		t.Fatalf("counts=%+v ok=%v", counts, ok)
	}
}

func TestLifecycleCollectorConcurrentWSAndHorizonAccounting(t *testing.T) {
	a := newLifecycleCollectorAccumulator()
	const workers = 200
	var wg sync.WaitGroup
	wg.Add(workers * 2)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			a.noteFrame(lifecycleFrameDecision{Baseline: true, Exclusion: "first_close_date_baseline"})
		}()
		go func() {
			defer wg.Done()
			a.noteHorizonDueAttempt()
			a.noteHorizonExclusion("fresh_executable_book_unavailable")
		}()
	}
	wg.Wait()
	_, counts, ok := a.drain(time.Now())
	if !ok || counts.RawFrames != workers || counts.BaselineFrames != workers || counts.HorizonDue != workers ||
		counts.HorizonAttempts != workers ||
		counts.Exclusions["first_close_date_baseline"] != workers ||
		counts.Exclusions["fresh_executable_book_unavailable"] != workers {
		t.Fatalf("counts=%+v ok=%v", counts, ok)
	}
}

func TestLifecycleCollectorExpiredHorizonMissHasAnAttempt(t *testing.T) {
	a := newLifecycleCollectorAccumulator()
	a.noteHorizonDueAttempt()
	a.noteHorizonMiss(true, "fixed_horizon_expired", nil)
	_, counts, ok := a.drain(time.Now())
	if !ok || counts.HorizonDue != 1 || counts.HorizonAttempts != 1 || counts.HorizonMisses != 1 {
		t.Fatalf("expired horizon funnel=%+v ok=%v", counts, ok)
	}
}

func researchOrder(t *testing.T, body string) kalshi.Order {
	t.Helper()
	var o kalshi.Order
	if err := json.Unmarshal([]byte(body), &o); err != nil {
		t.Fatal(err)
	}
	return o
}

func TestQueueLadderRefMapsAllOfficialOrderSides(t *testing.T) {
	tests := []struct {
		name, body string
		no         bool
		price      float64
	}{
		{"yes bid", `{"outcome_side":"yes","book_side":"bid","yes_price_dollars":"0.42"}`, false, .42},
		{"no ask", `{"outcome_side":"no","book_side":"ask","no_price_dollars":"0.58"}`, true, .58},
		{"legacy buy yes", `{"side":"yes","action":"buy","yes_price_dollars":"0.42"}`, false, .42},
		{"legacy sell no", `{"side":"no","action":"sell","yes_price_dollars":"0.42"}`, false, .42},
		{"legacy buy no", `{"side":"no","action":"buy","no_price_dollars":"0.58"}`, true, .58},
		{"legacy sell yes", `{"side":"yes","action":"sell","no_price_dollars":"0.58"}`, true, .58},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			no, px, ok := queueLadderRef(researchOrder(t, tc.body))
			if !ok || no != tc.no || math.Abs(px-tc.price) > 1e-9 {
				t.Fatalf("no=%v px=%v ok=%v", no, px, ok)
			}
		})
	}
}

func TestQueueLadderRefRejectsContradictoryCanonicalBits(t *testing.T) {
	for _, body := range []string{
		`{"outcome_side":"no","book_side":"bid","no_price_dollars":"0.58"}`,
		`{"outcome_side":"yes","book_side":"ask","yes_price_dollars":"0.42"}`,
	} {
		if _, _, ok := queueLadderRef(researchOrder(t, body)); ok {
			t.Fatalf("accepted contradictory canonical direction: %s", body)
		}
	}
}

func TestKalshiNoAskTouchUsesOfficialYesBidSize(t *testing.T) {
	var m kalshi.Market
	if err := json.Unmarshal([]byte(`{"yes_bid_dollars":"0.59","yes_bid_size_fp":"12.5","no_ask_dollars":"0.41"}`), &m); err != nil {
		t.Fatal(err)
	}
	ask, depth, ok := kalshiNoAskTouch(m)
	if !ok || math.Abs(ask-.41) > 1e-9 || depth != 12.5 {
		t.Fatalf("no ask touch ask=%v depth=%v ok=%v", ask, depth, ok)
	}
	if err := json.Unmarshal([]byte(`{"yes_bid_dollars":"0.59","yes_bid_size_fp":"12.5","no_ask_dollars":"0.40"}`), &m); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := kalshiNoAskTouch(m); ok {
		t.Fatal("accepted contradictory NO ask")
	}
}

func TestNoSideBookPreservesExecutableDepth(t *testing.T) {
	b := noSideBook(storage.LifecycleBook{Bid: .40, Ask: .43, BidDepth: 12, AskDepth: 7, Source: "ws", Age: .2})
	if math.Abs(b.Bid-.57) > 1e-9 || math.Abs(b.Ask-.60) > 1e-9 || b.BidDepth != 7 || b.AskDepth != 12 {
		t.Fatalf("NO book=%+v", b)
	}
}

func TestNestedGreaterPayoffTruth(t *testing.T) {
	low, high := 10.0, 20.0
	for _, tc := range []struct{ value, want float64 }{{5, 1}, {10, 1}, {15, 2}, {20, 2}, {25, 1}} {
		if got := nestedGreaterPayout(tc.value, low, high); got != tc.want {
			t.Fatalf("value=%v payout=%v want=%v", tc.value, got, tc.want)
		}
	}
}

func TestResearchBasketRotationEventuallyPassesFirstThirty(t *testing.T) {
	keys := make([]string, 50)
	for i := range keys {
		keys[i] = fmt.Sprintf("E%02d", i)
	}
	seen := map[string]bool{}
	cursor := 0
	for pass := 0; pass < 5; pass++ {
		var chosen []string
		chosen, cursor = rotateEligibleResearchKeys(keys, cursor, 10, func(k string) bool { return !seen[k] })
		if len(chosen) != 10 {
			t.Fatalf("pass %d chose %d", pass, len(chosen))
		}
		for _, k := range chosen {
			if seen[k] {
				t.Fatalf("duplicate %s", k)
			}
			seen[k] = true
		}
	}
	if len(seen) != 50 || !seen["E49"] {
		t.Fatalf("rotation starved tail: n=%d cursor=%d", len(seen), cursor)
	}
}

func TestNestedLadderRejectsMissingOrZeroFloorStrike(t *testing.T) {
	var snap kalshi.EventSnapshot
	err := json.Unmarshal([]byte(`{"event_ticker":"E","markets":[
{"ticker":"MISSING","event_ticker":"E","status":"active","strike_type":"greater","yes_bid_dollars":"0.40","yes_ask_dollars":"0.42","yes_bid_size_fp":"5","yes_ask_size_fp":"5"},
{"ticker":"ZERO","event_ticker":"E","status":"active","strike_type":"greater","floor_strike":0,"yes_bid_dollars":"0.40","yes_ask_dollars":"0.42","yes_bid_size_fp":"5","yes_ask_size_fp":"5"},
{"ticker":"TEN","event_ticker":"E","status":"active","strike_type":"greater","floor_strike":10,"yes_bid_dollars":"0.40","yes_ask_dollars":"0.42","yes_bid_size_fp":"5","yes_ask_size_fp":"5"},
{"ticker":"TWENTY","event_ticker":"E","status":"active","strike_type":"greater","floor_strike":20,"yes_bid_dollars":"0.20","yes_ask_dollars":"0.22","yes_bid_size_fp":"5","yes_ask_size_fp":"5"}]}`), &snap)
	if err != nil {
		t.Fatal(err)
	}
	rows := nestedLadderRungs(snap)
	if len(rows) != 2 || rows[0].ticker != "TEN" || rows[1].ticker != "TWENTY" {
		t.Fatalf("absent/zero strike entered identity: %+v", rows)
	}
}
