package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r151AddSettledStrategyHistory(t *testing.T, s *Server, family, side string, pnls []float64) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	catalog := make([]storage.CatalogRow, 0, len(pnls))
	for i := range pnls {
		ticker := fmt.Sprintf("KXR151-STRAT-%03d", i)
		catalog = append(catalog, storage.CatalogRow{Venue: "kalshi", Ticker: ticker,
			EventKey: fmt.Sprintf("KXR151-STRAT-EVENT-%03d", i), Kind: "winner", Title: "R151 route"})
	}
	if err := s.store.UpsertMarketCatalog(ctx, catalog); err != nil {
		t.Fatal(err)
	}
	for i, pnl := range pnls {
		ticker := fmt.Sprintf("KXR151-STRAT-%03d", i)
		opened := now.Add(-time.Duration(i+2) * time.Minute)
		inserted, err := s.insertCanonicalUnitTrial(ctx, storage.UnitTrial{OpenedTS: opened,
			Family: family, Platform: "kalshi", OriginLayer: "strategy", Ticker: ticker, Side: side,
			Episode: livePriorityProofGenerationEpisode, Ask: .40, FeePC: .01, FeeKnown: true,
			FeeSource: "kalshi:test-schedule", Depth: 10,
			QuoteSource: livePriorityProofQuoteSource("r151", "r151-test-contract")})
		if err != nil || !inserted {
			t.Fatalf("strategy history insert %d=%v err=%v", i, inserted, err)
		}
		if _, err := s.store.DBForTest().ExecContext(ctx, `UPDATE unit_trials
SET settled=1,closed_ts=?,settle_val=?,pnl_pc=?,return_per_dollar=?
WHERE family=? AND platform='kalshi' AND origin_layer='strategy' AND ticker=? AND side=?`,
			opened.Add(time.Minute).Format(time.RFC3339Nano), boolInt(pnl > 0), pnl, pnl/.41,
			family, ticker, side); err != nil {
			t.Fatal(err)
		}
	}
}

func TestR158PositiveModelCannotAuthorizeUnderpoweredExactStrategy(t *testing.T) {
	s := testServer(t)
	r147EnableProspectiveAllocation(s)
	s.kalFees = map[string]kalFeeInfo{
		"KXCANARY": {taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1},
	}
	s.kalFeesAt = time.Now()
	c := r147InsertAllocationHistoryOrigin(
		t, s, "kflow", "YES", "model", liveAllocationMinMarketsFloor, 1)
	pnls := make([]float64, liveAllocationMinMarketsFloor)
	for i := range pnls {
		if i%2 == 0 {
			pnls[i] = .55
		} else {
			pnls[i] = -.35
		}
	}
	r151AddSettledStrategyHistory(t, s, "kflow", "YES", pnls)

	ok, basis, mean, lower, _ := s.liveMirrorProof(context.Background(), c, .40, false)
	if ok || basis != "prospective-allocation-current-cost-erases-confidence-lower-bound" ||
		mean <= 0 || lower >= s.liveAllocationEdgeFloor() {
		t.Fatalf("positive model leaked through underpowered exact strategy: ok=%v basis=%q mean=%v lower=%v",
			ok, basis, mean, lower)
	}
	if ok, why, mirrorMean, mirrorLower, _ := s.livePolicyMirrorIsolatedProof(
		context.Background(), c, .40); ok ||
		why != "prospective-allocation-current-cost-erases-confidence-lower-bound" ||
		mirrorMean <= 0 || mirrorLower >= s.liveAllocationEdgeFloor() {
		t.Fatalf("isolated mirror borrowed positive model over underpowered exact strategy: "+
			"ok=%v why=%q mean=%v lower=%v", ok, why, mirrorMean, mirrorLower)
	}
}

func TestR158PositiveModelCannotAuthorizeNegativeExactStrategy(t *testing.T) {
	s := testServer(t)
	r147EnableProspectiveAllocation(s)
	s.kalFees = map[string]kalFeeInfo{
		"KXCANARY": {taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1},
	}
	s.kalFeesAt = time.Now()
	c := r147InsertAllocationHistoryOrigin(
		t, s, "kflow", "YES", "model", liveAllocationMinMarketsFloor, 1)
	pnls := make([]float64, liveAllocationMinMarketsFloor)
	for i := range pnls {
		pnls[i] = -.05
	}
	r151AddSettledStrategyHistory(t, s, "kflow", "YES", pnls)

	if ok, why, mean, lower, _ := s.liveMirrorProof(context.Background(), c, .40, false); ok ||
		why != "prospective-allocation-current-cost-erases-confidence-lower-bound" || mean >= 0 || lower >= 0 {
		t.Fatalf("negative strategy leaked through model fallback: ok=%v why=%q mean=%v lower=%v", ok, why, mean, lower)
	}
	if ok, why, mean, lower, _ := s.livePolicyMirrorIsolatedProof(
		context.Background(), c, .40); ok ||
		why != "prospective-allocation-current-cost-erases-confidence-lower-bound" ||
		mean >= 0 || lower >= 0 {
		t.Fatalf("isolated mirror borrowed positive model over negative exact strategy: "+
			"ok=%v why=%q mean=%v lower=%v", ok, why, mean, lower)
	}
}

func TestR158PositiveModelCannotReplaceMissingMatureExactStrategy(t *testing.T) {
	s := testServer(t)
	r147EnableProspectiveAllocation(s)
	s.kalFees = map[string]kalFeeInfo{
		"KXCANARY": {taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1},
	}
	s.kalFeesAt = time.Now()
	c := r147InsertAllocationHistoryOrigin(
		t, s, "kflow", "YES", "model", liveAllocationMinMarketsFloor, 1)

	want := fmt.Sprintf(
		"prospective-allocation-needs-%d-distinct-settled-exact-strategy-contracts",
		liveAllocationMinMarketsFloor)
	if ok, why, _, _, _ := s.liveMirrorProof(context.Background(), c, .40, false); ok || why != want {
		t.Fatalf("positive model replaced missing exact strategy proof: ok=%v why=%q want=%q",
			ok, why, want)
	}
	if ok, why, _, _, _ := s.livePolicyMirrorIsolatedProof(
		context.Background(), c, .40); ok || why != want {
		t.Fatalf("isolated mirror borrowed model proof: ok=%v why=%q want=%q", ok, why, want)
	}
}

func TestR151ProspectiveCurrentAskOwnsEconomicsButParityLanesKeepChase(t *testing.T) {
	if liveMirrorPaperChaseRequired(false, false, true) {
		t.Fatal("selected prospective route incorrectly retained the Paper chase gate")
	}
	for _, tc := range []struct {
		name                    string
		sealed, newML, selected bool
	}{
		{name: "sealed", sealed: true, selected: true},
		{name: "new ML", newML: true, selected: true},
		{name: "unselected", selected: false},
	} {
		if !liveMirrorPaperChaseRequired(tc.sealed, tc.newML, tc.selected) {
			t.Fatalf("%s parity lane lost its Paper chase gate", tc.name)
		}
	}

	const ticker = "KXR151-MOVE"
	s := testServer(t)
	s.kalshiExecutableBookFn = func(candidate liveMirrorCandidate) (r159KalshiExecutableBook, string) {
		if candidate.Ticker != ticker || candidate.Side != "YES" {
			return r159KalshiExecutableBook{}, "unexpected-test-candidate"
		}
		now := time.Now()
		return r159KalshiExecutableBook{
			MakerPrice: .40, MakerDepth: 10, TakerPrice: .42, TakerDepth: 8,
			MakerTick: .01, TakerTick: .01, SourceAt: now, ReceivedAt: now,
			Source: "kalshi_ws_full_orderbook:test",
		}, ""
	}
	cfg := *s.cfg()
	cfg.Auto.RouteQueueEnabled = false
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = "kalshi|spotlag|YES|taker"
	s.cfgP.Store(&cfg)

	q, why := s.liveMirrorExecutable(t.Context(), liveMirrorCandidate{Platform: "kalshi", Ticker: ticker,
		Side: "YES", Source: "auto-cons-spotlag", Price: .20, At: time.Now()})
	if why != "" || q.Price != .42 || q.Maker {
		t.Fatalf("current-priced prospective route was still rejected as a Paper chase: quote=%+v why=%q", q, why)
	}
	unselected := liveMirrorCandidate{Platform: "kalshi", Ticker: ticker, Side: "YES",
		Source: "unselected-research", Price: .20, At: time.Now()}
	if _, why := s.liveMirrorExecutable(t.Context(), unselected); why != "moved-off-paper-decision" {
		t.Fatalf("non-prospective parity route lost chase belt: %q", why)
	}
}

func TestR165AllowlistedIntentCollectsReceiptButCashRetirementWins(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	const ticker = "KXR151-SHADOW"
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = "kalshi|kalshi-flow|YES|taker"
	cfg.Risk.LiveSystemCanaryAllowlist = "kalshi|kalshi-flow|YES|taker"
	s.cfgP.Store(&cfg)
	s.paperHorizonFn = func(context.Context, string, string, string) float64 { return 1 }
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{"KXR151": {taker: .07, typ: "quadratic", multiplier: 1}}
	s.kalEventFees = map[string]kalFeeInfo{}
	s.kalFeesAt = time.Now()
	s.kalFeeMu.Unlock()
	s.metaMu.Lock()
	if s.kmktsAt == nil {
		s.kmktsAt = map[string]time.Time{}
	}
	s.kmktsAt[ticker] = time.Now()
	s.metaMu.Unlock()
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		out := in
		bid, ask, bidDepth, askDepth, age, tick := .38, .40, 20.0, 20.0, .01, .01
		fee, source, known := s.fillFeeReceipt("kalshi", ticker, false, 1, ask)
		makerFee, _, makerKnown := s.fillFeeReceipt("kalshi", ticker, true, 1, bid)
		if !known || !makerKnown {
			return storage.Signal{}, "", false
		}
		out.EntryPrice, out.BookDepth, out.SpreadCents, out.FeePC = ask, askDepth, 2, &fee
		out.BookFeatureVer, out.PricingVersion, out.BookSource = mlBookFeatureVersion, mlBookFeatureSchema, "r151-atomic-book"
		out.BookBid, out.BookAsk, out.BookBidDepth, out.BookAskDepth = &bid, &ask, &bidDepth, &askDepth
		out.BookQuoteAgeS, out.BookMakerTick, out.BookTakerTick = &age, &tick, &tick
		out.BookMakerFeePC, out.BookTakerFeePC = &makerFee, &fee
		return out, source, true
	}
	fee, _, feeOK := s.fillFeeReceipt("kalshi", ticker, false, 1, .40)
	if !feeOK {
		t.Fatal("exact fee fixture unavailable")
	}
	injectVerdicts(s, []verdictEnt{{Family: "taker:kalshi-flow@kalshi", SourceFamily: "kalshi-flow",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker",
		N: 150, Mean: .08, MeanAsk: .40, FeePC: fee}})
	r147InsertAllocationHistory(t, s, "kalshi-flow", "YES", s.liveAllocationMinMarkets(), s.liveAllocationMinMarkets())

	// An unrelated simulated system already owns this ticker. It must not affect detection or the
	// durable one-unit research receipt, while R165's global cash retirement must still prevent a
	// candidate from entering the real-money queue.
	s.gfBookMu.Lock()
	st := s.gfLoadLocked()
	st.Subs["gf:unrelated@kalshi:yes:taker"] = &kfBook{Open: []kfPos{{Ticker: ticker, Side: "YES", Price: .40, Contracts: 1}}}
	s.gfBookMu.Unlock()
	sig := storage.Signal{Platform: "kalshi", Ticker: ticker, Title: "R151 shadow intent",
		Side: "YES", SignalType: "kalshi-flow", EntryPrice: .39, ResolveHours: 1,
		ExecExpr: r147InputReceiptExpr("K", time.Now())}
	if s.enqueueLiveAllowlistedSignalIntent(ctx, sig, .08) {
		t.Fatal("retired one-contract canary entered the LIVE queue")
	}
	s.liveMu.Lock()
	logs := append([]map[string]any(nil), s.liveLog...)
	s.liveMu.Unlock()
	foundRetired := false
	for _, row := range logs {
		if row["reason"] == liveOneContractCanaryCashRetiredReason {
			foundRetired = true
		}
	}
	if !foundRetired {
		t.Fatalf("cash refusal was not the explicit R165 retirement: %+v", logs)
	}

	s.liveMirrorMu.Lock()
	queued := append([]liveMirrorCandidate(nil), s.liveMirrorQ...)
	s.liveMirrorMu.Unlock()
	if len(queued) != 0 {
		t.Fatalf("retired cash intent reached the LIVE candidate queue: %+v", queued)
	}
	s.gfBookMu.Lock()
	openLots := 0
	for _, b := range s.gfLoadLocked().Subs {
		openLots += len(b.Open)
	}
	s.gfBookMu.Unlock()
	if openLots != 1 {
		t.Fatalf("shadow intent mutated funded Paper P&L/positions: open lots=%d", openLots)
	}
	var durable int
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM unit_trials
WHERE family='kalshi-flow' AND platform='kalshi' AND ticker=? AND side='YES'
  AND quote_source LIKE 'strategy-decision/live-priority/%'`, ticker).Scan(&durable); err != nil || durable != 1 {
		t.Fatalf("shadow one-unit receipt durable=%d err=%v", durable, err)
	}
}

func TestR151LiveSignalFanoutIsBoundedAndExactAllowlistOnly(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = "kalshi|kalshi-flow|YES|taker"
	s.cfgP.Store(&cfg)
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	sig := storage.Signal{Platform: "kalshi", Side: "YES", SignalType: "kalshi-flow"}
	for i := 0; i < liveSignalIntentCap; i++ {
		sig.Ticker = fmt.Sprintf("KXR151-FANOUT-%03d", i)
		if !s.queueLiveSignalIntent(sig, .05) {
			t.Fatalf("bounded LIVE queue rejected item %d before capacity", i)
		}
	}
	if got := len(s.liveSignalIntentChannel()); got != liveSignalIntentCap {
		t.Fatalf("LIVE queue length=%d want=%d", got, liveSignalIntentCap)
	}
	sig.Ticker = "KXR151-FANOUT-OVERFLOW"
	if s.queueLiveSignalIntent(sig, .05) {
		t.Fatal("full LIVE queue blocked/expanded instead of refusing visibly")
	}
	sig.Side = "NO"
	if s.queueLiveSignalIntent(sig, .05) {
		t.Fatal("unallowlisted side entered the LIVE-priority queue")
	}
}

func TestR151LiveSignalFanoutCoalescesSameBurst(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = "kalshi|kalshi-flow|YES|taker"
	s.cfgP.Store(&cfg)
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR151-COALESCE", Side: "YES", SignalType: "kalshi-flow"}
	if !s.queueLiveSignalIntent(sig, .05) || s.queueLiveSignalIntent(sig, .05) {
		t.Fatal("same signal burst was not coalesced at the bounded LIVE boundary")
	}
}

func TestR151BoundedMirrorBurstRearmsUntilQueueDrains(t *testing.T) {
	s := testServer(t)
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	now := time.Now()
	s.liveMirrorMu.Lock()
	for i := 0; i < 9; i++ { // consumeLiveMirrorCandidates inspects at most eight per pass.
		s.liveMirrorQ = append(s.liveMirrorQ, liveMirrorCandidate{Platform: "kalshi",
			Ticker: fmt.Sprintf("KXR151-DRAIN-%02d", i), Side: "YES", Action: "BUY",
			Source: "auto-cons-kalshi-flow", Price: .40, At: now})
	}
	s.liveMirrorMu.Unlock()
	for i := 0; i < 8; i++ {
		if _, ok := s.popLiveMirrorCandidate(); !ok {
			t.Fatalf("bounded pass ran out at attempt %d", i)
		}
	}
	if !s.rearmLiveMirrorDispatchIfPending() {
		t.Fatal("ninth burst candidate was stranded without an immediate rearm")
	}
	select {
	case <-s.liveMirrorWakeChannel():
	default:
		t.Fatal("pending candidate did not publish the coalesced dispatch wake")
	}
	if _, ok := s.popLiveMirrorCandidate(); !ok {
		t.Fatal("rearmed pass could not consume the remaining candidate")
	}
	if s.rearmLiveMirrorDispatchIfPending() {
		t.Fatal("empty queue kept the immediate-dispatch loop armed")
	}
}

func TestR151DispatchRearmDoesNotRenewStaleSignalAuthority(t *testing.T) {
	s := testServer(t)
	c := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXR151-STALE", Side: "YES",
		Action: "BUY", Source: "auto-cons-kalshi-flow", Price: .40, At: time.Now(),
		InputTopology: "K", SignalContractID: "fixture-contract"}
	s.liveMirrorMu.Lock()
	s.liveMirrorQ = []liveMirrorCandidate{c}
	s.liveAllocationSignal = map[string]time.Time{
		liveAllocationSignalKey(c): time.Now().Add(-liveMirrorTTL - time.Second),
	}
	s.liveMirrorMu.Unlock()
	if !s.rearmLiveMirrorDispatchIfPending() {
		t.Fatal("pending stale candidate should still wake so dispatch can log the exact rejection")
	}
	if s.liveAllocationSignalFresh(c, time.Now().Add(-liveMirrorTTL)) {
		t.Fatal("queue rearm incorrectly renewed stale signal authority")
	}
}

func TestR151SelectedLiveIntentPreflightDropNamesExactReason(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = "kalshi|kalshi-flow|YES|taker"
	s.cfgP.Store(&cfg)
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	s.paperHorizonFn = func(context.Context, string, string, string) float64 { return 0 }
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR151-DROP", Side: "YES",
		SignalType: "kalshi-flow", ResolveHours: 0}
	if s.enqueueLiveAllowlistedSignalIntent(context.Background(), sig, .05) {
		t.Fatal("unknown funded horizon unexpectedly produced a LIVE candidate")
	}
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	if len(s.liveLog) != 1 {
		t.Fatalf("selected preflight drop receipts=%d want=1", len(s.liveLog))
	}
	got := s.liveLog[0]
	if got["event"] != "AUTO-LIVE-SIGNAL-DROP" || got["stage"] != "live-first-preflight" ||
		got["reason"] != "current-funded-entry-horizon-unavailable-or-exceeded" ||
		got["ticker"] != sig.Ticker || got["source"] != "auto-cons-kalshi-flow" {
		t.Fatalf("selected preflight drop lost exact reason/identity: %#v", got)
	}
}
