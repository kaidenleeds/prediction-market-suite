package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

func bundleFixture() ResearchRouteBundle {
	level := []ResearchRouteBundleLevel{{Price: .40, Quantity: 3,
		FeeQuotes: []ResearchRouteBundleFeeQuote{{Quantity: 1, Total: .01}}}}
	unwind := []ResearchRouteBundleLevel{{Price: .38, Quantity: 2,
		FeeQuotes: []ResearchRouteBundleFeeQuote{{Quantity: 1, Total: .01}}}}
	legs := []ResearchRouteBundleLeg{}
	for i, ticker := range []string{"KXA", "KXB"} {
		legs = append(legs, ResearchRouteBundleLeg{Index: i, LegID: ticker + "|YES", Venue: "kalshi",
			Ticker: ticker, Side: "YES", PayoffID: "payoff:" + ticker, Quantity: 1,
			IntegratedCost: .40, ExactFee: .01, VisibleDepth: 3, Tick: .01, Age: .1,
			BookSource: "kalshi_book_ws_full", SourceClockID: "g1:s1:q1", FeeSource: "kalshi-fee-v1",
			Levels: level, Payoff: []float64{float64(1 - i), float64(i)}, UnwindKnown: true,
			UnwindBookSource: "kalshi_book_ws_full", UnwindFeeSource: "kalshi-fee-v1", UnwindLevels: unwind})
	}
	return ResearchRouteBundle{BundleID: "bundle-fixture", SystemID: "payoff-constraint-solver", Cohort: "fixture",
		ExperimentVersion: 1, OpportunityID: "opp", CanonicalEventID: "event", EventVersion: 1,
		CertificateHash: "cert", CertificateStatus: "verified", RouteKind: "all_leg_taker", Observed: time.Now().UTC(), Size: 1,
		Cost: .80, Fee: .02, PayoutFloor: 1, NetFloor: .18, PartialFillWorst: -.82,
		UnwindWorst: -.04, UnwindKnown: true, DecisionLatencyMS: 2, LatencyKnown: true,
		StateVectorHash: "state-hash", Blocker: "non-atomic research route",
		Legs: legs, States: []ResearchRouteBundleState{{Index: 0, StateID: "A", Payout: 1},
			{Index: 1, StateID: "B", Payout: 1}}, Evidence: map[string]any{"actual_fill_claimed": false}}
}

func TestTypedResearchBundleIsAtomicImmutableAndZeroAuthority(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	b := bundleFixture()
	inserted, err := st.InsertResearchRouteBundle(ctx, b)
	if err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	if inserted, err = st.InsertResearchRouteBundle(ctx, b); err != nil || inserted {
		t.Fatalf("duplicate inserted=%v err=%v", inserted, err)
	}
	bundles, legs, states, grades, err := st.ResearchRouteBundleCounts(ctx)
	if err != nil || bundles != 1 || legs != 2 || states != 2 || grades != 0 {
		t.Fatalf("counts=%d/%d/%d/%d err=%v", bundles, legs, states, grades, err)
	}
	var funded, paper, live int
	if err := st.db.QueryRow(`SELECT funded,paper_authority,live_authority FROM research_route_bundles`).Scan(&funded, &paper, &live); err != nil || funded != 0 || paper != 0 || live != 0 {
		t.Fatalf("authority=%d/%d/%d err=%v", funded, paper, live, err)
	}
	if _, err := st.db.Exec(`UPDATE research_route_bundle_legs SET ticker='MUTATED'`); err == nil {
		t.Fatal("immutable bundle leg updated")
	}
	bad := bundleFixture()
	bad.BundleID = "bad-missing-clock"
	bad.Legs[1].SourceClockID = ""
	if inserted, err := st.InsertResearchRouteBundle(ctx, bad); err == nil || inserted {
		t.Fatalf("incomplete bundle inserted=%v err=%v", inserted, err)
	}
	bundles, legs, _, _, _ = st.ResearchRouteBundleCounts(ctx)
	if bundles != 1 || legs != 2 {
		t.Fatalf("partial bundle leaked header/legs=%d/%d", bundles, legs)
	}
}

func TestR143RecentPositiveResearchRouteBundlesPreservesPaperResearchBlockers(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	positive := bundleFixture()
	positive.Observed = now.Add(-time.Minute)
	if inserted, insertErr := st.InsertResearchRouteBundle(ctx, positive); insertErr != nil || !inserted {
		t.Fatalf("positive inserted=%v err=%v", inserted, insertErr)
	}
	negative := bundleFixture()
	negative.BundleID = "bundle-negative"
	negative.OpportunityID = "opp-negative"
	negative.StateVectorHash = "state-negative"
	negative.Observed = now
	negative.Blocker = ""
	negative.NetFloor = -.02
	if inserted, insertErr := st.InsertResearchRouteBundle(ctx, negative); insertErr != nil || !inserted {
		t.Fatalf("negative inserted=%v err=%v", inserted, insertErr)
	}

	got, err := st.RecentPositiveResearchRouteBundles(ctx, now.Add(-5*time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].BundleID != positive.BundleID || len(got[0].Legs) != 2 {
		t.Fatalf("positive exact bundles=%+v", got)
	}
	if got[0].NetFloor <= 0 || got[0].Blocker == "" || got[0].Legs[0].BookSource == "" ||
		got[0].Legs[0].ExactFee <= 0 || len(got[0].States) != 2 {
		t.Fatalf("frozen route truth was not preserved: %+v", got[0])
	}
}

func TestTypedResearchBundleHardSixLegAndPromotionFailClosed(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	b := bundleFixture()
	for len(b.Legs) < 7 {
		leg := b.Legs[0]
		leg.Index, leg.LegID, leg.Ticker = len(b.Legs), "extra", "EXTRA"
		leg.LegID += string(rune('A' + len(b.Legs)))
		b.Legs = append(b.Legs, leg)
	}
	if inserted, err := st.InsertResearchRouteBundle(context.Background(), b); err == nil || inserted {
		t.Fatalf("seven-leg bundle inserted=%v err=%v", inserted, err)
	}
	status, err := st.ResearchBundlePromotionStatus(context.Background())
	if err != nil || status.SealedEligible != 0 || status.CurrentAtomicRFQ != 0 ||
		status.State != "WAITING_FOR_SEALED_CONJUNCTION_BUNDLE_PASS" {
		t.Fatalf("promotion=%+v err=%v", status, err)
	}
}

func TestKalshiComboHandoffRequiresExactConjunctionStateVector(t *testing.T) {
	// Classic buy-one-of-each additive lock: exactly one leg pays in either state, so the portfolio
	// pays $1. A Kalshi MVE YES on both legs pays $0 in both states. Identical tickers are not an
	// identical security and must never reach Paper/LIVE.
	additive := bundleFixture()
	if researchBundleKalshiComboEquivalent(additive.Size, additive.Legs, additive.States) {
		t.Fatal("additive lock was misrepresented as an all-legs-win conjunction")
	}

	// A genuine conjunction certificate has a $1 payoff only in states where every selected side
	// is true. This is the only payoff vector the existing Kalshi MVE + Paper combo paths represent.
	conjunctionLegs := []ResearchRouteBundleLeg{
		{Venue: "kalshi", Payoff: []float64{1, 1, 0}},
		{Venue: "kalshi", Payoff: []float64{1, 0, 1}},
	}
	conjunctionStates := []ResearchRouteBundleState{
		{Index: 0, StateID: "both", Payout: 1},
		{Index: 1, StateID: "left-only", Payout: 0},
		{Index: 2, StateID: "right-only", Payout: 0},
	}
	if !researchBundleKalshiComboEquivalent(1, conjunctionLegs, conjunctionStates) {
		t.Fatal("exact conjunction state vector was rejected")
	}
	conjunctionStates[1].Payout = 1
	if researchBundleKalshiComboEquivalent(1, conjunctionLegs, conjunctionStates) {
		t.Fatal("one-state payoff mismatch crossed the exact security boundary")
	}
}

func TestAdditiveBundlePromotionStatusNamesTheRFQPayoffBlocker(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if inserted, err := st.InsertResearchRouteBundle(context.Background(), bundleFixture()); err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	status, err := st.ResearchBundlePromotionStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.State != "BLOCKED_KALSHI_MVE_CONJUNCTION_DIFFERS_FROM_SOLVER_PORTFOLIO" ||
		status.KalshiVerifiedBundles != 1 || status.ComboEquivalentBundles != 0 ||
		status.ComboPayoffMismatches != 1 || !status.RFQRequiresCreatorWrite ||
		status.SealedEligible != 0 || status.CurrentAtomicRFQ != 0 {
		t.Fatalf("promotion status=%+v", status)
	}
}

func conjunctionBundleFixture(id, event, cohort string, observed time.Time) ResearchRouteBundle {
	level := []ResearchRouteBundleLevel{{Price: .20, Quantity: 2,
		FeeQuotes: []ResearchRouteBundleFeeQuote{{Quantity: 1, Total: 0}}}}
	legs := []ResearchRouteBundleLeg{
		{Index: 0, LegID: id + "-A|YES", Venue: "kalshi", Ticker: id + "-A", Side: "YES",
			PayoffID: "p:a", Quantity: 1, IntegratedCost: .2, ExactFee: 0, VisibleDepth: 2,
			Tick: .01, Age: .1, BookSource: "kalshi_ws", SourceClockID: "g:s:q:a", FeeSource: "fee",
			Levels: level, Payoff: []float64{1, 1, 0}},
		{Index: 1, LegID: id + "-B|YES", Venue: "kalshi", Ticker: id + "-B", Side: "YES",
			PayoffID: "p:b", Quantity: 1, IntegratedCost: .2, ExactFee: 0, VisibleDepth: 2,
			Tick: .01, Age: .1, BookSource: "kalshi_ws", SourceClockID: "g:s:q:b", FeeSource: "fee",
			Levels: level, Payoff: []float64{1, 0, 1}},
	}
	return ResearchRouteBundle{BundleID: id, SystemID: "payoff-constraint-solver", Cohort: cohort,
		ExperimentVersion: 1, OpportunityID: "opp:" + id, CanonicalEventID: event, EventVersion: 1,
		CertificateHash: "cert:" + id, CertificateStatus: "verified", RouteKind: "all_leg_taker",
		Observed: observed, Size: 1, Cost: .4, Fee: 0, PayoutFloor: 0, NetFloor: -.4,
		PartialFillWorst: -.4, UnwindWorst: -.4, UnwindKnown: false, DecisionLatencyMS: 1,
		LatencyKnown: true, StateVectorHash: "state:" + id, Blocker: "separately filled research route",
		Legs: legs, States: []ResearchRouteBundleState{{Index: 0, StateID: "both", Payout: 1},
			{Index: 1, StateID: "left", Payout: 0}, {Index: 2, StateID: "right", Payout: 0}},
		Evidence: map[string]any{"complete_state_vector": true}}
}

func TestSealedConjunctionBundleCreatesIdenticalPaperThenRequiresSameRFQRecheck(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	created := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	cutoff, start := created.Add(24*time.Hour), created.Add(48*time.Hour)
	end, sealAt := start.Add(40*24*time.Hour), start.Add(42*24*time.Hour)
	hash := func(c string) string { return strings.Repeat(c, 64) }
	p, inserted, err := st.registerResearchInferencePreregistrationAt(ctx, ResearchInferencePreregistration{
		PreregistrationID: "typed-rfq-sealed", SystemID: "payoff-constraint-solver",
		Cohort: "typed-conjunction", Venue: "kalshi", Route: "rfq", TrainValidationCutoff: cutoff,
		UntouchedStart: start, UntouchedEnd: end, SealAt: sealAt, Embargo: 24 * time.Hour,
		RequiredDays: 30, RequiredEvents: 30, CodeManifestHash: hash("a"),
		DataManifestHash: hash("b"), SourceManifestHash: hash("c")}, created)
	if err != nil || !inserted {
		t.Fatalf("preregistered=%v p=%+v err=%v", inserted, p, err)
	}
	for i := 0; i < 40; i++ {
		observed := start.Add(time.Duration(i)*24*time.Hour + time.Hour)
		id := "typed-conjunction-" + time.Unix(int64(i+1), 0).UTC().Format("150405")
		b := conjunctionBundleFixture(id, "event:"+id, p.Cohort, observed)
		if ok, err := st.InsertResearchRouteBundle(ctx, b); err != nil || !ok {
			t.Fatalf("bundle %d inserted=%v err=%v", i, ok, err)
		}
		if ok, err := st.AppendResearchRouteBundleGrade(ctx, ResearchRouteBundleGrade{BundleID: id,
			Observed: observed.Add(12 * time.Hour), OutcomeStatus: "counterfactual_settled",
			Payout: 1, RealizedNet: .6, CapitalSeconds: 12 * 3600,
			Reason: "authoritative exact per-leg fixture settlements", EvidenceJSON: `{"exact":true}`}); err != nil || !ok {
			t.Fatalf("grade %d inserted=%v err=%v", i, ok, err)
		}
	}
	req := PreregisteredUntouchedSealRequest{PreregistrationID: p.PreregistrationID,
		CodeManifestHash: p.CodeManifestHash, DataManifestHash: p.DataManifestHash,
		SourceManifestHash: p.SourceManifestHash, InferenceContractHash: p.InferenceContractHash}
	report, sealed, err := st.sealPreregisteredUntouchedAt(ctx, req, sealAt.Add(time.Hour))
	if err != nil || !sealed {
		t.Fatalf("sealed=%v report=%+v err=%v", sealed, report, err)
	}
	var sealedLower float64
	for _, result := range report.Results {
		if result.SystemID == p.SystemID {
			sealedLower = result.UntouchedLower
		}
	}
	resultHash := strings.TrimPrefix(report.ResultHash, "sha256:")
	if _, err := st.db.Exec(`INSERT INTO research_sealed_route_economics(receipt_id,sealed_inference_run_id,
sealed_result_hash,system_id,route_id,venue,net_per_day_lower,capacity,capital_dollar_hours_per_day,
conversion_evidence_hash,created_ts,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, "typed-rfq-route", report.RunID, resultHash, p.SystemID,
		"rfq", "kalshi", sealedLower, 1.0, 12.0, hash("d"), sealAt.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("typed route economics: %v", err)
	}
	cause, insertedCause, causeErr := st.RegisterFrozenCauseExposure(ctx, FrozenCauseExposure{
		ExposureID: "typed-rfq-cause", InputState: "active", SystemID: p.SystemID,
		RouteID: "rfq", Venue: "kalshi", CauseIDs: []string{"kalshi-mve-state-vector"},
		Provenance: "typed bundle fixture", EvidenceHash: resultHash, SealedInferenceRunID: report.RunID,
		SealedRouteEconomicsReceiptID: "typed-rfq-route", EvidenceObserved: sealAt,
		ValidUntil: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), ReplicationID: "typed-untouched",
		LowerBoundMethod: "deterministic day block", ReplicatedUntouched: true,
		ExecutableFeeNetLowerBound: true, NetPerDayLower: sealedLower, Capacity: 1,
		CapitalDollarHoursPerDay: 12, Version: 1,
	})
	if causeErr != nil || !insertedCause {
		t.Fatalf("typed cause inserted=%v err=%v", insertedCause, causeErr)
	}
	entries := []string{"valid:" + cause.SpecHash}
	manifest, _ := ResearchPortfolioManifestHash("cause-graph-v1", entries)
	if _, insertedGraph, graphErr := st.InsertCauseGraphRun(ctx, ResearchPortfolioRun{
		ManifestHash: manifest, Observed: sealAt, State: "READY", InputHashes: entries,
		InputCount: 1, ValidInputCount: 1,
		ResultJSON: `[{"Systems":["payoff-constraint-solver"],"ConservativeSharedCauseNetPerDay":0.01}]`,
		Reason:     "typed fixture cause graph",
	}); graphErr != nil || !insertedGraph {
		t.Fatalf("typed cause graph inserted=%v err=%v", insertedGraph, graphErr)
	}
	current := conjunctionBundleFixture("typed-current", "event:typed-current", p.Cohort, time.Now().UTC())
	if ok, err := st.InsertResearchRouteBundle(ctx, current); err != nil || !ok {
		t.Fatalf("current inserted=%v err=%v", ok, err)
	}
	handoff, found, err := st.ResearchRouteBundleHandoffStatus(ctx, current.BundleID, .005)
	if err != nil || !found || !handoff.ComboEquivalent || !handoff.SealedUntouched ||
		handoff.ProofRunID != report.RunID || handoff.MaxAllInUnit <= .4 {
		t.Fatalf("handoff=%+v found=%v err=%v", handoff, found, err)
	}
	legs := []paper.Leg{{Platform: "kalshi", Ticker: current.Legs[0].Ticker, Side: "YES", Entry: .2},
		{Platform: "kalshi", Ticker: current.Legs[1].Ticker, Side: "YES", Entry: .2}}
	quote := ResearchRouteBundleQuote{CollectionTicker: "COL", MarketTicker: "MVE", RFQID: "rfq-1",
		QuoteID: "quote-1", Status: "open", Observed: time.Now().UTC(), Price: .4, Quantity: 1,
		ExactFee: .01, FeeSource: "kalshi exact fee"}
	drifted := handoff
	drifted.Governance.CauseSpecHash = hash("9")
	if _, _, err := st.InsertResearchRouteBundlePaperIntent(ctx, drifted, quote, legs); err == nil {
		t.Fatal("typed bundle promoted with self-asserted cause/portfolio governance")
	}
	intent, createdIntent, err := st.InsertResearchRouteBundlePaperIntent(ctx, handoff, quote, legs)
	if err != nil || !createdIntent || intent.PaperParlayID <= 0 {
		t.Fatalf("intent=%+v inserted=%v err=%v", intent, createdIntent, err)
	}
	var price, contracts, fee float64
	if err := st.db.QueryRow(`SELECT price,contracts,fees FROM paper_parlays WHERE id=?`,
		intent.PaperParlayID).Scan(&price, &contracts, &fee); err != nil || price != .4 || contracts != 1 || fee != .01 {
		t.Fatalf("identical Paper price/contracts/fee=%v/%v/%v err=%v", price, contracts, fee, err)
	}
	if err := st.AppendResearchRouteBundlePromotionEvent(ctx, intent, "live_dispatched", "accepted",
		.4, 1, .01, "forged early LIVE", nil); err == nil {
		t.Fatal("LIVE dispatch bypassed same-quote revalidation")
	}
	if err := st.AppendResearchRouteBundlePromotionEvent(ctx, intent, "quote_revalidated", "open",
		.999, 1, .01, "over-ceiling fresh quote", nil); err == nil {
		t.Fatal("over-ceiling fresh quote was revalidated")
	}
	freshLive := intent
	freshLive.RFQID, freshLive.QuoteID = "rfq-2", "quote-2"
	if err := st.AppendResearchRouteBundlePromotionEvent(ctx, freshLive, "quote_revalidated", "open",
		.41, 1, .01, "fresh later open quote for the same immutable security", nil); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendResearchRouteBundlePromotionEvent(ctx, freshLive, "live_dispatched", "accepted",
		.41, 1, .01, "accept dispatched; fill still unconfirmed",
		map[string]any{"filled": false, "market_ticker": "MVE", "dispatch_not_fill": true}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertResearchRouteBundleLiveFill(ctx, ResearchRouteBundleLiveFill{
		IntentID: intent.IntentID, MarketTicker: "MVE", RFQID: "rfq-2", QuoteID: "quote-2",
		QuoteStatus: "accepted", FilledQuantity: .5, AveragePrice: .41, ActualFee: .005,
		FeeSource: "kalshi-portfolio-fills:fee_cost", FillIDsJSON: `["f-partial"]`, PositionQuantity: .5,
	}); err == nil {
		t.Fatal("partial RFQ execution manufactured live_filled")
	}
	if err := st.InsertResearchRouteBundleLiveFill(ctx, ResearchRouteBundleLiveFill{
		IntentID: intent.IntentID, MarketTicker: "MVE", RFQID: "rfq-2", QuoteID: "quote-2",
		QuoteStatus: "executed", FilledQuantity: 1, AveragePrice: .41, ActualFee: .01,
		FeeSource: "kalshi-portfolio-fills:fee_cost", FillIDsJSON: `["f-full"]`, PositionQuantity: 1,
	}); err != nil {
		t.Fatalf("authoritative full RFQ receipt rejected: %v", err)
	}
	var liveEvents int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_route_bundle_promotion_events
WHERE intent_id=? AND event_type='live_dispatched'`, intent.IntentID).Scan(&liveEvents); err != nil || liveEvents != 1 {
		t.Fatalf("live dispatch events=%d err=%v", liveEvents, err)
	}
}
