package server

import (
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR154LiveArbitrationWindowIsExecutionScale(t *testing.T) {
	if liveMirrorArbitrationWindow > 10*time.Millisecond {
		t.Fatalf("LIVE burst window = %s; expected at most 10ms", liveMirrorArbitrationWindow)
	}
}

func TestR154PostTimerUrgentProbeStillChoosesMoneyWork(t *testing.T) {
	intents := make(chan liveSignalIntent, 1)
	wakes := make(chan struct{}, 1)
	want := liveSignalIntent{Point: .42, At: time.Now()}
	intents <- want
	got, woke := liveAutoUrgentProbe(intents, wakes)
	if got == nil || woke || got.Point != want.Point {
		t.Fatalf("post-timer signal re-probe missed urgent work: got=%+v woke=%v", got, woke)
	}
	wakes <- struct{}{}
	got, woke = liveAutoUrgentProbe(intents, wakes)
	if got != nil || !woke {
		t.Fatalf("post-timer dispatch wake re-probe missed urgent work: got=%+v woke=%v", got, woke)
	}
}

func TestR154HandlerQuoteRefreshIsConditionalAndRejectsChangedEconomics(t *testing.T) {
	now := time.Now()
	fresh := liveMirrorQuote{Price: .42, Depth: 5, Maker: false, Route: "taker",
		ObservedAt: now.Add(-liveMirrorHandlerQuoteMaxAge)}
	if !fresh.fresh(now, liveMirrorHandlerQuoteMaxAge) {
		t.Fatal("quote at the exact age bound was forced through a duplicate refresh")
	}
	if fresh.fresh(now.Add(time.Nanosecond), liveMirrorHandlerQuoteMaxAge) {
		t.Fatal("aged handler quote bypassed the final conditional refresh")
	}
	current := fresh
	current.ObservedAt = time.Now()
	if why := liveMirrorHandlerQuoteChangeReason(fresh, current, 5); why != "" {
		t.Fatalf("unchanged refreshed quote was refused: %s", why)
	}
	current.Price = .43
	if why := liveMirrorHandlerQuoteChangeReason(fresh, current, 5); why != "refreshed-executable-price-changed" {
		t.Fatalf("repriced quote did not fail closed: %s", why)
	}
	current = fresh
	current.ObservedAt = time.Now()
	current.Depth = 4
	if why := liveMirrorHandlerQuoteChangeReason(fresh, current, 5); why != "refreshed-executable-depth-fell-below-request" {
		t.Fatalf("lost depth did not fail closed: %s", why)
	}
}

func TestR155HandlerQuoteRefreshIgnoresRouteTelemetryOnly(t *testing.T) {
	prior := liveMirrorQuote{
		Price: .42, Depth: 5, Maker: false,
		Route:      "prospective-allocation-taker(q=18,r=3.0/m,eta=360s,hz=1800s)",
		ObservedAt: time.Now(),
	}
	current := prior
	current.Route = "prospective-allocation-taker(q=41,r=7.5/m,eta=328s,hz=1800s)"
	current.ObservedAt = time.Now()
	if why := liveMirrorHandlerQuoteChangeReason(prior, current, 5); why != "" {
		t.Fatalf("route telemetry drift changed an identical taker route: %s", why)
	}
}

func TestR155HandlerQuoteRefreshRejectsStableRouteIdentityOrMakerChange(t *testing.T) {
	prior := liveMirrorQuote{
		Price: .42, Depth: 5, Maker: false,
		Route:      "prospective-allocation-taker(q=18,r=3.0/m,eta=360s,hz=1800s)",
		ObservedAt: time.Now(),
	}

	current := prior
	current.Route = "current-new-ml-identical-taker(q=18,r=3.0/m,eta=360s,hz=1800s)"
	current.ObservedAt = time.Now()
	if why := liveMirrorHandlerQuoteChangeReason(prior, current, 5); why != "refreshed-executable-route-changed" {
		t.Fatalf("stable route identity change did not fail closed: %s", why)
	}

	current = prior
	current.Maker = true
	current.ObservedAt = time.Now()
	if why := liveMirrorHandlerQuoteChangeReason(prior, current, 5); why != "refreshed-executable-route-changed" {
		t.Fatalf("maker/taker change did not fail closed: %s", why)
	}
}

func TestR155RouteIdentityRequiresCompleteTelemetrySuffix(t *testing.T) {
	const malformed = "future-reason(q=literal)"
	if got := liveMirrorStableRouteIdentity(malformed); got != malformed {
		t.Fatalf("malformed route text was truncated: %q", got)
	}
	const tagged = "prospective-allocation-taker(q=41,r=7.5/m,eta=328s,hz=1800s)"
	if got := liveMirrorStableRouteIdentity(tagged); got != "prospective-allocation-taker" {
		t.Fatalf("generated route telemetry was not stripped: %q", got)
	}
}

func TestR155QuoteFailureEvidencePreservesExactBeforeAndAfterValues(t *testing.T) {
	now := time.Now().UTC()
	q := liveMirrorQuote{
		Price: .41, Depth: 6, Tick: .01, SpreadCents: 1, Maker: false,
		Route:      "prospective-allocation-taker(q=6,r=2.0/m,eta=180s,hz=900s)",
		BookSource: "kalshi_rest_full_orderbook", ObservedAt: now.Add(-300 * time.Millisecond),
	}
	got := liveMirrorQuoteEvidence(q, now)
	if got["price"] != .41 || got["depth"] != float64(6) ||
		got["route_identity"] != "prospective-allocation-taker" ||
		got["observed_at"] != q.ObservedAt.Format(time.RFC3339Nano) ||
		math.Abs(got["age_ms"].(float64)-300) > 1e-6 {
		t.Fatalf("quote evidence lost executable truth: %+v", got)
	}
}

func TestR154PendingRiskOutcomeWakeIsCoalescedAndNonblocking(t *testing.T) {
	s := &Server{}
	for i := 0; i < 10; i++ {
		s.scheduleLivePendingRiskReconcile()
	}
	if got := len(s.livePendingRiskWakeChannel()); got != 1 {
		t.Fatalf("post-submit outcome wakes were not coalesced: %d", got)
	}
}

func TestR154OnlyAuthoritativeTerminalZeroFillIsRetryable(t *testing.T) {
	if !liveKalshiTerminalZeroFill("unfilled", true, 0, 0) {
		t.Fatal("exact terminal IOC zero-fill was not recognized")
	}
	for _, tc := range []struct {
		state         string
		authoritative bool
		filled        float64
		remaining     float64
	}{
		{"unfilled", false, 0, 0},
		{"ambiguous", true, 0, 0},
		{"partial", true, 1, 0},
		{"pending", true, 0, 1},
	} {
		if liveKalshiTerminalZeroFill(tc.state, tc.authoritative, tc.filled, tc.remaining) {
			t.Fatalf("unsafe retry receipt accepted: %+v", tc)
		}
	}
}

func TestR164TerminalZeroFillDoesNotReuseDetectorAttempt(t *testing.T) {
	source, err := os.ReadFile("live_arbitration.go")
	if err != nil {
		t.Fatal(err)
	}
	consume := string(source)
	start := strings.Index(consume, "func (s *Server) consumeRankedLiveMirrorCandidatesWindow")
	if start < 0 {
		t.Fatal("ranked LIVE consumer missing")
	}
	consume = consume[start:]
	if strings.Contains(consume, "authoritative-zero-fill") ||
		strings.Contains(consume, "RetryCount") {
		t.Fatal("terminal IOC zero-fill must wait for a new detector attempt, not requeue the old id")
	}
}

func TestR154PendingRiskRaceRequeuesOnlyInsideSignalTTL(t *testing.T) {
	now := time.Now()
	candidate := liveMirrorCandidate{At: now.Add(-time.Second)}
	reason := "live-handler:automatic safety pause before venue submission: pending-risk: exact fill is reconciling"
	if !liveMirrorShouldRequeueTransient(candidate, reason, now) {
		t.Fatal("proven pre-submit pending-risk race should remain queued")
	}
	if liveMirrorShouldRequeueTransient(candidate, "live-handler:durable same-market order guard", now) {
		t.Fatal("a real same-market conflict must not be retried")
	}
	candidate.At = now.Add(-liveMirrorTTL - time.Millisecond)
	if liveMirrorShouldRequeueTransient(candidate, reason, now) {
		t.Fatal("transient retry must not extend the original signal TTL")
	}
}

func TestR154StableAckLetsUnrelatedOrdersContinueWhileFullRiskStaysReserved(t *testing.T) {
	leg := 0
	row := storage.LivePendingRiskReservation{
		Intent: storage.LivePendingRiskIntent{
			ReservationID: "risk-1", Product: "single", CostUSD: 3.25,
		},
		Legs: []storage.LivePendingRiskLeg{{
			ReservationID: "risk-1", Index: 0, Venue: "kalshi", Ticker: "TICKER-A",
			Side: "YES", Action: "BUY", Quantity: 5, LimitPrice: .60,
		}},
		Events: []storage.LivePendingRiskEvent{
			{ReservationID: "risk-1", EventType: storage.LivePendingRiskSubmitStarted,
				LegIndex: &leg, AttemptKey: "entry", Action: "BUY", ClientOrderID: "client-1"},
			{ReservationID: "risk-1", EventType: storage.LivePendingRiskAck,
				LegIndex: &leg, AttemptKey: "entry", Action: "BUY", ClientOrderID: "client-1",
				OrderID: "order-1"},
		},
	}
	if r151HasUnidentifiedPendingRisk([]storage.LivePendingRiskReservation{row}) {
		t.Fatal("stable ACK with an immutable maximum-cost reservation should not globally pause unrelated orders")
	}
	pending, _, err := r148PendingRiskOverlayRows(
		[]storage.LivePendingRiskReservation{row}, "kalshi", nil)
	if err != nil {
		t.Fatalf("pending overlay: %v", err)
	}
	if math.Abs(pending-row.Intent.CostUSD) > 1e-9 {
		t.Fatalf("stable ACK reserved %.6f, want full immutable risk %.6f", pending, row.Intent.CostUSD)
	}

	row.Events = row.Events[:1]
	if !r151HasUnidentifiedPendingRisk([]storage.LivePendingRiskReservation{row}) {
		t.Fatal("submit_started without a stable ACK must remain a global pause")
	}
	row.Events = append(row.Events,
		storage.LivePendingRiskEvent{ReservationID: "risk-1", EventType: storage.LivePendingRiskAck,
			LegIndex: &leg, AttemptKey: "entry", Action: "BUY", ClientOrderID: "client-1",
			OrderID: "order-1"},
		storage.LivePendingRiskEvent{ReservationID: "risk-1", EventType: storage.LivePendingRiskAmbiguous,
			LegIndex: &leg, AttemptKey: "entry", Action: "BUY", ClientOrderID: "client-1",
			OrderID: "order-1"})
	if !r151HasUnidentifiedPendingRisk([]storage.LivePendingRiskReservation{row}) {
		t.Fatal("an explicit ambiguity must keep the global pause even after an ACK")
	}
}

func TestR154LiveFirstPersistedReceiptSkipsOnlyTheDuplicateObservation(t *testing.T) {
	now := time.Now()
	candidate := liveMirrorCandidate{
		Platform: "kalshi", Ticker: "TEST", Side: "YES", Family: "kalshi-flow",
		InputTopology: "kalshi-book", SignalContractID: "contract-1",
		ArbitrationQuote: liveMirrorQuote{Price: .42, Depth: 7},
		ArbitrationBasis: "prospective-allocation:test", ArbitrationLower: .03,
		ArbitrationAt: now.Add(-50 * time.Millisecond), LiveFirstUnitPersisted: true,
	}
	s := &Server{liveAllocationSignal: map[string]time.Time{
		liveAllocationSignalKey(candidate): now,
	}}
	if !s.liveFirstMirrorReceiptFresh(candidate, now) {
		t.Fatal("fresh persisted LIVE-first book/proof receipt should skip the duplicate observe pass")
	}
	candidate.LiveFirstUnitPersisted = false
	if s.liveFirstMirrorReceiptFresh(candidate, now) {
		t.Fatal("an inferred/non-persisted receipt must not skip durable observation")
	}
	candidate.LiveFirstUnitPersisted = true
	candidate.ArbitrationAt = now.Add(-3 * time.Second)
	if s.liveFirstMirrorReceiptFresh(candidate, now) {
		t.Fatal("an aged receipt must not skip a current observation")
	}
}
