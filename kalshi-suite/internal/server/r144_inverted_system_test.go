package server

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r144SignalFixture(family, side string) storage.Signal {
	p := .63
	return storage.Signal{SignalType: family, Platform: "kalshi", Ticker: "KXR144", Side: side,
		EntryPrice: .41, SpreadCents: 7, ExecExpr: "emitted", BookFeatureVer: 2, ModelProb: &p}
}

func TestR144InverseRequiresOneSideDetectorAndNeverNests(t *testing.T) {
	for _, tc := range []struct {
		family string
		ok     bool
	}{
		{"kalshi-flow", true},
		{"invert:kalshi-flow", false},
		{"side-control:kalshi-flow", false},
		{"counterfactual:kalshi-flow", false},
		{"proper-score-executor", true}, // contrarian rule owns independent opposite-book history
		{"rfq-sim", false},              // multi-leg systems cannot manufacture one opposite leg
	} {
		_, ok := independentInverseSignalIdentity(r144SignalFixture(tc.family, "YES"))
		if ok != tc.ok {
			t.Fatalf("inverse identity %s ok=%v want %v", tc.family, ok, tc.ok)
		}
	}
}

func TestR144ExactInverseRouteGetsPaperExecutorWithoutDirectRouteLeak(t *testing.T) {
	verds := []verdictEnt{
		{Family: "taker:edge@kalshi", SourceFamily: "edge", Platform: "kalshi",
			OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 40, Mean: -.08},
		{Family: "taker:invert:edge@kalshi", SourceFamily: "invert:edge", Platform: "kalshi",
			OriginLayer: "model", Route: "taker", Side: "NO", Group: "taker", N: 3, Mean: .03},
	}
	subs := gfDynamicSubs(verds)
	if len(subs) != 2 || subs[0].Family != "gf:edge:yes:taker-k" ||
		subs[1].Family != "gf:invert:edge:no:taker-k" || subs[0].SeedMean != 0 ||
		subs[1].SeedMean != 0 || subs[0].SeedState != gfPaperPointState ||
		subs[1].SeedState != gfPaperPointState {
		t.Fatalf("independent inverse Paper roster=%+v", subs)
	}
	coverage := gfPaperExecutionCoverage(verds)
	if len(coverage) != 2 || coverage[0].Executor != "generic-follower" || coverage[0].Direction != "direct" ||
		coverage[1].Executor != "generic-follower" || coverage[1].Direction != "direct" {
		t.Fatalf("direct/inverse execution coverage=%+v", coverage)
	}
}

func TestR144IndependentInverseUsesSharedLiveRouteButHasNoLiveAuthority(t *testing.T) {
	s := testServer(t)
	c := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXINV", Side: "NO",
		Source: "auto-cons-invert:edge", Price: .40, Inverted: true}
	if got := liveMirrorFamily(c); got != "invert:edge" {
		t.Fatalf("live-ready route lost independent family: %q", got)
	}
	ok, basis, _, _, _ := s.liveMirrorProof(context.Background(), c, .40, false)
	if ok || basis != "sealed-accepted-paper-intent-required" {
		t.Fatalf("unsealed inverse gained LIVE authority: ok=%v basis=%q", ok, basis)
	}
}

func TestR144NativeInverseSelectorsNeverPoolFamilies(t *testing.T) {
	aFamily, aSelector, aCohort, aOK := concreteNativeInverseIdentity("kalshi-flow", "kalshi", "YES")
	bFamily, bSelector, bCohort, bOK := concreteNativeInverseIdentity("favorite-long", "kalshi", "YES")
	if !aOK || !bOK || aFamily != "invert:kalshi-flow" || bFamily != "invert:favorite-long" ||
		aSelector == bSelector || aCohort == bCohort {
		t.Fatalf("inverse family selectors pooled: a=%q/%q/%q b=%q/%q/%q", aFamily, aSelector, aCohort, bFamily, bSelector, bCohort)
	}
	if _, _, _, ok := concreteNativeInverseIdentity("invert:kalshi-flow", "kalshi", "NO"); ok {
		t.Fatal("an already inverted system acquired a recursive immutable selector")
	}
}

func TestR144SealedInverseIntentCannotAuthorizeAnotherFamily(t *testing.T) {
	family, selector, cohort, ok := concreteNativeInverseIdentity("kalshi-flow", "kalshi", "YES")
	if !ok {
		t.Fatal("fixture inverse identity unavailable")
	}
	cohort += "|candidate|policy=" + selector
	in := storage.ResearchPromotionIntent{
		Proof: storage.ResearchPromotionProof{SystemID: "paired-bridge-inversion", Cohort: cohort,
			Venue: "kalshi", Route: "taker", StrategyFamily: family, SelectorID: selector, FiredSide: "YES"},
		Candidate: storage.ResearchPromotionCandidate{SystemID: "paired-bridge-inversion", Cohort: cohort,
			Venue: "kalshi", Route: "taker", Ticker: "KXSAME", Side: "NO",
			StrategyFamily: family, SelectorID: selector, FiredSide: "YES"},
	}
	exact := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXSAME", Side: "NO",
		Family: family, SelectorID: selector, FiredSide: "YES", Source: "r139p:test"}
	if why := liveAutoPromotionCandidateReason(in, exact, "taker"); why != "" {
		t.Fatalf("identical accepted inverse contract rejected: %s", why)
	}
	borrowed := exact
	borrowed.Family = "invert:favorite-long"
	if why := liveAutoPromotionCandidateReason(in, borrowed, "taker"); !strings.Contains(why, "inverse family/selector") {
		t.Fatalf("second inverse family borrowed sealed intent: %q", why)
	}
	borrowed = exact
	borrowed.SelectorID = "native-inverse-v1:other"
	if why := liveAutoPromotionCandidateReason(in, borrowed, "taker"); !strings.Contains(why, "inverse family/selector") {
		t.Fatalf("selector drift borrowed sealed intent: %q", why)
	}
}

func TestR144DedicatedInverseIsNotDoubleFunded(t *testing.T) {
	v := verdictEnt{Family: "taker:invert:freshlist@kalshi", SourceFamily: "invert:freshlist",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "NO", Group: "taker", N: 2, Mean: .04}
	if subs := gfDynamicSubs([]verdictEnt{v}); len(subs) != 0 {
		t.Fatalf("dedicated FreshInv route was duplicated by generic follower: %+v", subs)
	}
	rows := gfPaperExecutionCoverage([]verdictEnt{v})
	if len(rows) != 1 || rows[0].Executor != "dedicated" {
		t.Fatalf("dedicated inverse coverage=%+v", rows)
	}
}

func TestR144InverseLineageIsExplicitAndSideSpecific(t *testing.T) {
	out, ok := independentInverseSignalIdentity(r144SignalFixture("kflow", "NO"))
	if !ok || out.Side != "YES" || !strings.Contains(out.ExecExpr, "base=kflow/emitted=NO") ||
		out.SpreadCents != 0 || out.BookFeatureVer != 0 || out.BookBid != nil || out.FeePC != nil {
		t.Fatalf("inverse lineage=%+v ok=%v", out, ok)
	}
	family, emitted, price, inverted := gfDiscoveryLineage(out)
	if family != "kflow" || emitted != "NO" || math.Abs(price-.41) > 1e-12 || inverted != "YES" {
		t.Fatalf("fill lineage family=%q emitted=%q price=%v inverted=%q", family, emitted, price, inverted)
	}
}

func TestR144CurrentRouteEdgePaysOnlyAdversePriceAndExactSizedFeeMovement(t *testing.T) {
	edge, ok := gfCurrentRouteEdge(.05, .40, .42, .01, .036, 3)
	if !ok || math.Abs(edge-.028) > 1e-12 { // 5c - 2c chase - (1.2c-1c) fee drag
		t.Fatalf("current edge=%v ok=%v", edge, ok)
	}
	edge, ok = gfCurrentRouteEdge(.05, .40, .38, .01, .024, 3)
	if !ok || math.Abs(edge-.05) > 1e-12 { // favorable movement never inflates proof
		t.Fatalf("favorable move inflated edge=%v ok=%v", edge, ok)
	}
	if edge, ok = gfCurrentRouteEdge(.02, .40, .43, .01, .03, 3); !ok || edge >= 0 {
		t.Fatalf("disappeared edge was not rejected by caller contract: edge=%v ok=%v", edge, ok)
	}
	if got := lotFamilyTag(kfPos{RouteReason: "fam:invert:edge origin=model book=kalshi-ws"}, "fallback"); got != "invert:edge" {
		t.Fatalf("route audit fields contaminated family identity: %q", got)
	}
}

func TestR144LegacySideControlNeverMasqueradesAsNamedInverse(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	insertSettled := func(family, side, ticker string, pnl float64) {
		t.Helper()
		inserted, err := s.store.InsertUnitTrial(ctx, storage.UnitTrial{
			OpenedTS: time.Now().UTC().Add(-time.Hour), Family: family, Platform: "kalshi",
			OriginLayer: "model", Ticker: ticker, Side: side, Ask: .40, FeePC: .01,
			FeeKnown: true, FeeSource: "kalshi:test-exact", Depth: 5,
			QuoteSource: "independent-opposite-side/kalshi-ws", ResolveHours: 1,
		})
		if err != nil || !inserted {
			t.Fatalf("insert %s: inserted=%v err=%v", family, inserted, err)
		}
		_, err = s.store.DBForTest().ExecContext(ctx, `UPDATE unit_trials
SET settled=1,closed_ts=?,settle_val=0,pnl_pc=?,return_per_dollar=?,capital_day=?
WHERE family=? AND ticker=?`, time.Now().UTC().Format(time.RFC3339Nano), pnl, pnl/.41, pnl/.41, family, ticker)
		if err != nil {
			t.Fatal(err)
		}
	}
	insertSettled("edge", "YES", "KXR144-DIRECT", .07)
	insertSettled("side-control:edge", "NO", "KXR144-CONTROL", .07)
	rows := s.computeExperimentVerdicts(ctx)
	named := 0
	for _, row := range rows {
		if row.SourceFamily == "invert:edge" {
			named++
		}
	}
	if named != 0 {
		t.Fatalf("legacy side-control was duplicated under invert:edge: %+v", rows)
	}

	// Only the prospective named system's own opposite-side observation may create its verdict.
	insertSettled("invert:edge", "NO", "KXR144-INVERSE", -.02)
	s.verdMu.Lock()
	s.verdAt = time.Time{}
	s.verdMu.Unlock()
	rows = s.computeExperimentVerdicts(ctx)
	named, wrongSide, seeded := 0, 0, 0
	for _, row := range rows {
		if row.SourceFamily == "invert:edge" {
			named++
			if row.Side != "NO" {
				wrongSide++
			}
			if row.SeedSource != "" {
				seeded++
			}
		}
	}
	if named != 1 || wrongSide != 0 || seeded != 0 {
		t.Fatalf("named inverse did not retain its own opposite-side economics: named=%d wrong_side=%d seeded=%d rows=%+v", named, wrongSide, seeded, rows)
	}
}

func TestR144DuplicateRetryPersistsInverseBeforePaperFillAndSettlesExactSide(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) { c.Auto.PaperTakerDelayMS = 1 })
	ctx := context.Background()
	ticker := "KXR144-INVERSE-ORDER"

	// Freeze a current authoritative quadratic schedule so both the one-share collector and the
	// sized Paper fill use the production fee calculator rather than a test-only arithmetic value.
	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{"KXR144": {taker: .07, typ: "quadratic", multiplier: 1}}
	s.kalEventFees = map[string]kalFeeInfo{}
	s.kalFeesAt = time.Now()
	s.kalFeeMu.Unlock()
	s.metaMu.Lock()
	if s.kmkts == nil {
		s.kmkts = map[string]kalshi.Market{}
	}
	if s.kmktsAt == nil {
		s.kmktsAt = map[string]time.Time{}
	}
	s.kmkts[ticker] = kalshi.Market{Ticker: ticker, EventTicker: "KXR144-INVERSE-EVENT"}
	s.kmktsAt[ticker] = time.Now()
	s.metaMu.Unlock()

	quoteFor := func(in storage.Signal, bid, ask float64) (storage.Signal, string, bool) {
		out := in
		bidDepth, askDepth, age, tick := 20.0, 20.0, .01, .01
		fee, source, known := s.fillFeeReceipt("kalshi", in.Ticker, false, 1, ask)
		if !known {
			t.Fatalf("exact fee unavailable: %q", source)
		}
		makerFee, _, makerKnown := s.fillFeeReceipt("kalshi", in.Ticker, true, 1, bid)
		if !makerKnown {
			t.Fatal("exact maker fee unavailable")
		}
		out.EntryPrice, out.BookDepth, out.SpreadCents, out.FeePC = ask, askDepth, (ask-bid)*100, &fee
		out.BookFeatureVer, out.PricingVersion, out.BookSource = mlBookFeatureVersion, mlBookFeatureSchema,
			"kalshi_ws_full_orderbook:g1:s1:q1"
		out.BookBid, out.BookAsk, out.BookBidDepth, out.BookAskDepth = &bid, &ask, &bidDepth, &askDepth
		out.BookQuoteAgeS, out.BookMakerTick, out.BookTakerTick = &age, &tick, &tick
		out.BookMakerFeePC, out.BookTakerFeePC = &makerFee, &fee
		return out, source, true
	}
	bookReady := false
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		if !bookReady || !strings.EqualFold(in.Side, "NO") {
			return storage.Signal{}, "", false
		}
		return quoteFor(in, .38, .40)
	}

	feeOne, _, ok := s.fillFeeReceipt("kalshi", ticker, false, 1, .40)
	if !ok {
		t.Fatal("one-share fee setup failed")
	}
	cell := verdictEnt{Family: "taker:invert:kalshi-flow@kalshi", SourceFamily: "invert:kalshi-flow",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "NO", Group: "taker",
		N: 1, Mean: .08, MeanAsk: .40, FeePC: feeOne}
	injectVerdicts(s, []verdictEnt{cell})
	key := gfRosterKey("invert:kalshi-flow", "kalshi", "NO", "taker")
	s.swMu.Lock()
	s.swTable = &swState{At: time.Now(), SubShare: map[string]float64{key: 100}}
	s.swMu.Unlock()

	// First signal is durable, but the opposite book is unavailable. It must not create a Paper lot.
	first := storage.Signal{Platform: "kalshi", Ticker: ticker, Title: "R144 ordering",
		Side: "YES", SignalType: "kalshi-flow", EntryPrice: .61, ResolveHours: 1}
	if err := s.insertSignal(ctx, first); err != nil {
		t.Fatal(err)
	}
	firstDeadline := time.Now().Add(2 * time.Second)
	for {
		var rejected int
		if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_log
WHERE category='inverse-system' AND message LIKE ? AND message LIKE 'REJECTED%'`,
			"%"+ticker+"%").Scan(&rejected); err != nil {
			t.Fatal(err)
		}
		if rejected > 0 {
			break
		}
		if time.Now().After(firstDeadline) {
			t.Fatal("first unavailable opposite-book Paper batch did not finish")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// A changed discovery price makes the same-slot duplicate hook due. The retry must first insert
	// the newly available named inverse receipt, then execute it at the actual NO book.
	bookReady = true
	first.EntryPrice = .62
	if err := s.insertSignal(ctx, first); err != nil {
		t.Fatal(err)
	}
	drainCtx, drainCancel := context.WithTimeout(ctx, 3*time.Second)
	if err := s.stopGenfollowPaperWorkers(drainCtx); err != nil {
		drainCancel()
		t.Fatalf("drain delayed Paper worker: %v", err)
	}
	drainCancel()

	var inverseRows, filledRows int
	var fill, storedFee float64
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*),
COUNT(*) FILTER (WHERE fill_price>0),COALESCE(MAX(fill_price),0),COALESCE(MAX(fee_pc),0)
FROM signal_log WHERE platform='kalshi' AND ticker=? AND side='NO' AND signal_type='invert:kalshi-flow'`,
		ticker).Scan(&inverseRows, &filledRows, &fill, &storedFee); err != nil {
		t.Fatal(err)
	}
	if inverseRows != 1 || filledRows != 1 || math.Abs(fill-.40) > 1e-9 || storedFee <= 0 {
		t.Fatalf("inverse fill lacked its own durable actual-book row: rows=%d filled=%d fill=%v fee=%v",
			inverseRows, filledRows, fill, storedFee)
	}
	s.gfBookMu.Lock()
	book := s.gfLoadLocked().Subs[key]
	if book == nil || len(book.Open) != 1 || book.Open[0].Side != "NO" ||
		lotFamilyTag(book.Open[0], "") != "invert:kalshi-flow" || math.Abs(book.Open[0].Price-.40) > 1e-9 {
		s.gfBookMu.Unlock()
		t.Fatalf("Paper ledger did not retain the exact inverse route: %+v", book)
	}
	s.gfBookMu.Unlock()

	// YES wins, so the independently executed NO loses. Both the signal/unit route and Paper ledger
	// must settle from NO exposure; no original YES statistic may be reused.
	if err := s.store.ResolveSignals(ctx, ticker, 1); err != nil {
		t.Fatal(err)
	}
	s.settleUnitTrials(ctx)
	s.settleGenfollowBook(ctx)
	s.gfBookMu.Lock()
	book = s.gfLoadLocked().Subs[key]
	closedOK := book != nil && len(book.Open) == 0 && len(book.Closed) == 1 &&
		book.Closed[0].Side == "NO" && !book.Closed[0].Won && book.Closed[0].PnL < 0
	s.gfBookMu.Unlock()
	if !closedOK {
		t.Fatalf("inverse Paper settlement did not use its executed NO exposure: %+v", book)
	}
	var unitSettled int
	var unitPnL float64
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT settled,pnl_pc FROM unit_trials
WHERE family='invert:kalshi-flow' AND platform='kalshi' AND ticker=? AND side='NO'`, ticker).
		Scan(&unitSettled, &unitPnL); err != nil {
		t.Fatal(err)
	}
	if unitSettled != 1 || unitPnL >= 0 {
		t.Fatalf("inverse unit route did not settle independently: settled=%d pnl=%v", unitSettled, unitPnL)
	}

	// A quote can change while SQLite prepares the durable journal. The executor must make one more
	// complete-book read after that boundary and roll the journal back instead of projecting a lot
	// at economics which are no longer current.
	movingTicker := "KXR144-INVERSE-REPRICE"
	s.metaMu.Lock()
	s.kmktsAt[movingTicker] = time.Now()
	s.metaMu.Unlock()
	moving := storage.Signal{Platform: "kalshi", Ticker: movingTicker, Title: "R144 final reprice",
		Side: "NO", SignalType: "invert:kalshi-flow", EntryPrice: .40, ResolveHours: 1,
		ExecExpr: "independent-inverse-system/base=kalshi-flow/emitted=YES/discovery-price=.60"}
	storedMoving, _, movingOK := quoteFor(moving, .38, .40)
	if !movingOK {
		t.Fatal("moving inverse fixture lacked initial book")
	}
	storedMoving.PricingVersion = "independent-inverse-executable-v1"
	if err := s.store.InsertSignal(ctx, storedMoving); err != nil {
		t.Fatal(err)
	}
	quoteCalls := 0
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		if in.Ticker != movingTicker || !strings.EqualFold(in.Side, "NO") {
			return quoteFor(in, .38, .40)
		}
		quoteCalls++
		if quoteCalls >= 5 {
			return quoteFor(in, .39, .41)
		}
		return quoteFor(in, .38, .40)
	}
	s.genfollowConsider(ctx, moving)
	if quoteCalls != 5 {
		t.Fatalf("inverse did not reprice immediately after durable prepare: calls=%d", quoteCalls)
	}
	s.gfBookMu.Lock()
	movingOpen := false
	if b := s.gfLoadLocked().Subs[key]; b != nil {
		for _, pos := range b.Open {
			movingOpen = movingOpen || pos.Ticker == movingTicker
		}
	}
	s.gfBookMu.Unlock()
	if movingOpen {
		t.Fatal("inverse Paper lot used a quote which changed after journal prepare")
	}
	var rolledBack, movingFilled int
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM inverse_paper_placement_journal
WHERE ticker=? AND state='rolled_back'`, movingTicker).Scan(&rolledBack); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM signal_log
WHERE ticker=? AND signal_type='invert:kalshi-flow' AND fill_price>0`, movingTicker).Scan(&movingFilled); err != nil {
		t.Fatal(err)
	}
	if rolledBack != 1 || movingFilled != 0 {
		t.Fatalf("changed submission quote was not cleanly refused: rolled_back=%d filled=%d", rolledBack, movingFilled)
	}

	// Defense in depth: even a caller that bypasses insertSignal cannot leave an orphan Paper lot.
	// The route may pass its economic gates, but the checked exact-family update must fail and roll
	// the just-flushed lot back because no named signal receipt exists for this ticker.
	missingTicker := "KXR144-MISSING-ATTRIBUTION"
	s.metaMu.Lock()
	s.kmktsAt[missingTicker] = time.Now()
	s.metaMu.Unlock()
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		return quoteFor(in, .38, .40)
	}
	s.genfollowConsider(ctx, storage.Signal{Platform: "kalshi", Ticker: missingTicker,
		Title: "missing attribution", Side: "NO", SignalType: "invert:kalshi-flow",
		EntryPrice: .40, ResolveHours: 1, ExecExpr: "independent-inverse-system/base=kalshi-flow/emitted=YES"})
	s.gfBookMu.Lock()
	book = s.gfLoadLocked().Subs[key]
	orphaned := false
	if book != nil {
		for _, pos := range book.Open {
			orphaned = orphaned || pos.Ticker == missingTicker
		}
	}
	s.gfBookMu.Unlock()
	if orphaned {
		t.Fatal("inverse Paper lot survived without an exact durable signal attribution row")
	}
}
