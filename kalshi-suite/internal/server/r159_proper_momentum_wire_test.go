package server

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR159ProperMomentumCandidateBindsCurrentAutoGeneration(t *testing.T) {
	s := testServer(t)
	s.liveSignalIntentGeneration.Store(7)
	in := momentumIntentFixture("YES", 1)
	in.SourceID = "r139ms:" + strings.Repeat("a", 64)
	now := time.Now().UTC()
	in.Candidate.Observed = now.Add(-250 * time.Millisecond)
	candidate := s.properMomentumLiveCandidate(in,
		storage.ProperMomentumSellPaperState{State: "filled", SellPrice: .61}, now)
	if !candidate.LiveIntentGenerationBound || candidate.LiveIntentGeneration != 7 ||
		candidate.Action != "SELL" || candidate.Family != "proper-score-momentum" ||
		!candidate.At.Equal(in.Candidate.Observed) {
		t.Fatalf("unbound proper-score candidate: %+v", candidate)
	}
	s.liveSignalIntentGeneration.Add(1)
	if why := s.liveIntentGenerationGateReason(true, candidate.LiveIntentGeneration,
		candidate.LiveIntentGenerationBound, true); why != liveIntentGenerationChangedReason {
		t.Fatalf("old proper-score candidate escaped generation fence: %q", why)
	}
}

func TestR159ProperMomentumFinalResidentBookMustMatchWire(t *testing.T) {
	now := time.Now()
	prior := liveMirrorQuote{
		Price: .61, Depth: 4, Tick: .01, Route: "reduce_only_fok",
		BookSource: "prior", ObservedAt: now, CheckedAt: now,
	}
	book := r159KalshiExecutableBook{
		MakerPrice: .61, MakerDepth: 1, MakerTick: .01,
		SourceAt: now, ReceivedAt: now, Source: "kalshi_ws_full_orderbook:g2:s4:q8",
	}
	current, why := properMomentumResidentSaleQuote(book, "")
	if why != "" {
		t.Fatal(why)
	}
	if reason := liveMirrorHandlerQuoteChangeReasonForTIF(prior, current, 1, true); reason != "" {
		t.Fatalf("equivalent current resident book refused: %q", reason)
	}

	moved := book
	moved.MakerPrice = .60
	current, why = properMomentumResidentSaleQuote(moved, "")
	if why != "" {
		t.Fatal(why)
	}
	if reason := liveMirrorHandlerQuoteChangeReasonForTIF(prior, current, 1, true); reason != "refreshed-executable-price-changed" {
		t.Fatalf("changed final sale price reason=%q", reason)
	}

	thin := book
	thin.MakerDepth = .5
	current, why = properMomentumResidentSaleQuote(thin, "")
	if why != "" {
		t.Fatal(why)
	}
	if reason := liveMirrorHandlerQuoteChangeReasonForTIF(prior, current, 1, true); reason != "refreshed-executable-depth-fell-below-request" {
		t.Fatalf("thin final sale book reason=%q", reason)
	}
}

func TestR159ProperMomentumFOKUsesSharedImmediateReceiptStates(t *testing.T) {
	state, authoritative, filled, remaining := properMomentumFOKReceiptState(
		&kalshi.CreateOrderResult{FillCount: "0", RemainingCount: "1"})
	if state != "unfilled" || !authoritative ||
		!liveKalshiTerminalZeroFill(state, authoritative, filled, remaining) {
		t.Fatalf("FOK zero-fill remaining=requested = %q/%v/%v/%v",
			state, authoritative, filled, remaining)
	}

	state, authoritative, filled, remaining = properMomentumFOKReceiptState(
		&kalshi.CreateOrderResult{FillCount: ".5", RemainingCount: ".5"})
	if state != "partial" || !authoritative ||
		liveProspectiveFOKPartialReason(liveProspectiveFOK, state, filled, 1) == "" {
		t.Fatalf("FOK partial lost safety alarm = %q/%v/%v/%v",
			state, authoritative, filled, remaining)
	}

	if !properMomentumFullFOKReceipt(
		&kalshi.CreateOrderResult{FillCount: "1", RemainingCount: "0"}) {
		t.Fatal("full q1 FOK no longer classified full")
	}
}

func TestR159ProperMomentumKnownNoSendClosesRiskAndClaim(t *testing.T) {
	s, st := r148RiskTestServer(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	riskID := "risk-proper-no-send"
	intent := storage.LivePendingRiskIntent{
		ReservationID: riskID, Created: now, BaselineObserved: now.Add(-time.Second),
		Product: "reduce", DispatchSource: "dispatchProperMomentumLiveSell",
		SystemID: "proper-score-momentum", Route: "reduce_only_fok",
		FeeUSD: .01, CostUSD: .01, RequestHash: strings.Repeat("c", 64),
		ProofJSON: `{}`, BaselineReceiptJSON: `{}`,
	}
	inserted, err := st.InsertLivePendingRisk(context.Background(), intent,
		[]storage.LivePendingRiskLeg{{
			Index: 0, Venue: "kalshi", Ticker: "KXTEST", Side: "YES", Action: "SELL",
			ClientOrderID: "proper-client", Quantity: 1, LimitPrice: .61,
		}},
		[]storage.LivePendingRiskCluster{{
			Index: 0, Venue: "kalshi", ClusterKey: "event:proper",
			MappingVersion: r148LiveRiskMappingVersion, ReservedUSD: .01,
		}})
	if err != nil || !inserted {
		t.Fatalf("insert risk=%v err=%v", inserted, err)
	}
	leg := 0
	if err := s.r148AppendRiskEvent(context.Background(), riskID,
		storage.LivePendingRiskSubmitStarted, "reduce", "SELL", "proper-client", "",
		"test", "submit boundary", &leg, 0, .61, .01, nil); err != nil {
		t.Fatal(err)
	}
	claimKey := "proper_momentum_live_claim:test"
	if err := st.KVSet(context.Background(), claimKey, now.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	_, reason := s.rejectProperMomentumNoSend(context.Background(),
		storage.ProperMomentumSellIntent{IntentID: "missing-fixture-row"},
		riskID, claimKey, "proper-momentum-wire-book",
		"final book changed; venue was not called", "momentum-sell-wire-book-changed", nil)
	if reason != "momentum-sell-wire-book-changed" {
		t.Fatalf("known no-send reason=%q", reason)
	}
	active, err := st.ActiveLivePendingRisk(context.Background())
	if err != nil || len(active) != 0 {
		t.Fatalf("known no-send retained risk: active=%d err=%v", len(active), err)
	}
	if _, exists := st.KVGet(context.Background(), claimKey); exists {
		t.Fatal("known no-send retained durable submit claim")
	}
}

func TestR159ProperMomentumCannotBorrowFourSystemEntryAuthority(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = strings.Join([]string{
		"kalshi|spotlag|YES|taker", "kalshi|spotlag|NO|taker",
		"kalshi|kalshi-flow|YES|taker", "kalshi|kalshi-flow|NO|taker",
	}, ",")
	s.cfgP.Store(&cfg)
	s.liveArmed, s.liveAuto = true, true
	candidate := liveMirrorCandidate{
		Platform: "kalshi", Ticker: "KXR159-NO-PROPER-ENTRY", Side: "YES",
		Action: "BUY", Family: "spotlag", Source: "r139ms:" + strings.Repeat("d", 64),
		Price: .40, At: time.Now(), LiveIntentGeneration: s.liveSignalIntentGeneration.Load(),
		LiveIntentGenerationBound: true,
	}
	if placed, why := s.dispatchLiveMirror(context.Background(), candidate); placed || why != "proper-score-momentum-source-has-no-entry-authority" {
		t.Fatalf("proper-score source borrowed entry authority: placed=%v why=%q", placed, why)
	}
}

func TestR159ProperMomentumLiveSellDisabledBeforeIntentRiskOrVenue(t *testing.T) {
	s := testServer(t)
	s.liveArmed, s.liveAuto = true, true
	candidate := liveMirrorCandidate{
		Platform: "kalshi", Ticker: "KXR159-PARKED-SELL", Side: "YES",
		Action: "SELL", Family: "proper-score-momentum",
		Source: "r139ms:" + strings.Repeat("e", 64), Price: .61, At: time.Now(),
		LiveIntentGeneration: s.liveSignalIntentGeneration.Load(), LiveIntentGenerationBound: true,
	}
	// Removing the store makes an accepted-intent read impossible. The parked LIVE branch must
	// refuse before that read and therefore before risk, book, claim, or venue work.
	s.store = nil
	placed, why := s.dispatchLiveMirror(context.Background(), candidate)
	if placed || why != properMomentumLiveDisabledReason {
		t.Fatalf("parked proper-momentum SELL escaped: placed=%v why=%q", placed, why)
	}
}

func TestR159ProperMomentumGenerationAndWireChecksPrecedeCreateOrder(t *testing.T) {
	dispatchRaw, err := os.ReadFile("liveautomirror.go")
	if err != nil {
		t.Fatal(err)
	}
	dispatch := string(dispatchRaw)
	start := strings.Index(dispatch, "func (s *Server) dispatchLiveMirror")
	end := strings.Index(dispatch[start+1:], "\nfunc ")
	if start < 0 || end < 0 {
		t.Fatal("dispatchLiveMirror source block unavailable")
	}
	dispatch = dispatch[start : start+1+end]
	generationAt := strings.Index(dispatch, "s.liveIntentGenerationReason")
	specialAt := strings.Index(dispatch, "AcceptedProperMomentumSellIntent")
	if generationAt < 0 || specialAt <= generationAt {
		t.Fatalf("proper-score special branch precedes generation gate: generation=%d special=%d",
			generationAt, specialAt)
	}

	wireRaw, err := os.ReadFile("properscore_momentum_dispatch.go")
	if err != nil {
		t.Fatal(err)
	}
	wire := string(wireRaw)
	start = strings.Index(wire, "func (s *Server) dispatchProperMomentumLiveSell")
	if start < 0 {
		t.Fatal("dispatchProperMomentumLiveSell source block unavailable")
	}
	wire = wire[start:]
	submitAt := strings.Index(wire, "storage.LivePendingRiskSubmitStarted")
	createAt := strings.Index(wire, "s.kal.CreateOrder")
	if createAt < 0 {
		t.Fatal("proper momentum CreateOrder call unavailable")
	}
	finalGenerationAt := strings.LastIndex(wire[:createAt],
		"s.liveIntentGenerationGateReason")
	deadlineAt := strings.LastIndex(wire[:createAt],
		"r159LiveSubmitBoundaryReason")
	bookAt := strings.Index(wire, "readBook(c)")
	if submitAt < 0 || finalGenerationAt <= submitAt || deadlineAt <= finalGenerationAt ||
		bookAt <= deadlineAt || createAt <= bookAt {
		t.Fatalf("proper-score final wire ordering changed: submit=%d generation=%d deadline=%d book=%d create=%d",
			submitAt, finalGenerationAt, deadlineAt, bookAt, createAt)
	}
}
