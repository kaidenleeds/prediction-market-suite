package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r163AddLegacyStrategyHistory(t *testing.T, s *Server, family, side string, count int) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	for i := 0; i < count; i++ {
		ticker := fmt.Sprintf("KXR163-PROOF-LEGACY-%03d", i)
		opened := now.Add(-time.Duration(i+2) * time.Minute)
		inserted, err := s.store.InsertUnitTrial(ctx, storage.UnitTrial{
			OpenedTS: opened, Family: family, Platform: "kalshi", OriginLayer: "strategy",
			Ticker: ticker, Side: side, Ask: .40, FeePC: .01, FeeKnown: true,
			FeeSource: "kalshi:test", Depth: 10,
			QuoteSource: "strategy-decision/legacy-paper/kalshi-ws",
		})
		if err != nil || !inserted {
			t.Fatalf("legacy history insert %d=%v err=%v", i, inserted, err)
		}
		if _, err = s.store.DBForTest().ExecContext(ctx, `UPDATE unit_trials
SET settled=1,closed_ts=?,settle_val=1,pnl_pc=.20,return_per_dollar=.48
WHERE family=? AND platform='kalshi' AND origin_layer='strategy' AND ticker=? AND side=?`,
			opened.Add(time.Minute).Format(time.RFC3339Nano), family, ticker, side); err != nil {
			t.Fatal(err)
		}
	}
}

func TestR163ProofGenerationLegacyStrategyHistoryCannotAuthorizeCurrentLivePriority(t *testing.T) {
	s := testServer(t)
	r147EnableProspectiveAllocation(s)
	s.kalFees = map[string]kalFeeInfo{
		"KXCANARY": {taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1},
	}
	s.kalFeesAt = time.Now()
	c := r147InsertAllocationHistoryOrigin(
		t, s, "kflow", "YES", "model", liveAllocationMinMarketsFloor, 1)
	r163AddLegacyStrategyHistory(t, s, "kflow", "YES", liveAllocationMinMarketsFloor)

	// Prime the old broad-history cache key first. The current-generation reader must not reuse
	// that successful cached row merely because family, venue, origin, and side are identical.
	general, found, err := s.r154UnitTrialRouteLeaderboard(
		context.Background(), time.Now().UTC(), "kflow", "kalshi", "strategy", "YES")
	if err != nil || !found || general.SettledMarkets != liveAllocationMinMarketsFloor {
		t.Fatalf("legacy history not visible in general report: found=%v row=%+v err=%v",
			found, general, err)
	}
	want := fmt.Sprintf(
		"prospective-allocation-needs-%d-distinct-settled-exact-strategy-contracts",
		liveAllocationMinMarketsFloor)
	if ok, why, _, _, _ := s.liveMirrorProof(context.Background(), c, .40, false); ok || why != want {
		t.Fatalf("legacy strategy rows authorized current LIVE: ok=%v why=%q want=%q", ok, why, want)
	}
	if ok, why, _, _, _ := s.livePolicyMirrorIsolatedProof(
		context.Background(), c, .40); ok || why != want {
		t.Fatalf("legacy strategy rows authorized isolated mirror: ok=%v why=%q want=%q",
			ok, why, want)
	}
}

func TestR163ProofGenerationReceiptDoesNotCollideWithLegacySameTicker(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	const ticker = "KXR163-PROOF-SAME-TICKER"
	if err := s.store.UpsertMarketCatalog(ctx, []storage.CatalogRow{{
		Venue: "kalshi", Ticker: ticker, EventKey: "KXR163-PROOF-EVENT",
		Kind: "winner", Title: "generation collision fixture",
	}}); err != nil {
		t.Fatal(err)
	}
	if inserted, err := s.insertCanonicalUnitTrial(ctx, storage.UnitTrial{
		OpenedTS: time.Now().UTC().Add(-time.Minute), Family: "spotlag", Platform: "kalshi",
		OriginLayer: "strategy", Ticker: ticker, Side: "YES", Ask: .40, FeePC: .01,
		FeeKnown: true, FeeSource: "kalshi:test", Depth: 1,
		QuoteSource: "strategy-decision/legacy-paper/kalshi-ws",
	}); err != nil || !inserted {
		t.Fatalf("legacy same-ticker receipt=%v err=%v", inserted, err)
	}
	candidate := liveMirrorCandidate{Platform: "kalshi", Ticker: ticker, Side: "YES",
		Source: "auto-cons-spotlag", SignalContractID: "r163-proof-test-contract"}
	quote := liveMirrorQuote{Price: .41, Depth: 2, BookSource: "kalshi-ws-current"}
	if err := s.publishLivePriorityProofReceipt(
		ctx, candidate, quote, .01, "kalshi:test", time.Now(), false); err != nil {
		t.Fatal(err)
	}
	var all, current int
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM unit_trials
WHERE family='spotlag' AND platform='kalshi' AND ticker=? AND side='YES'`, ticker).Scan(&all); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM unit_trials
WHERE family='spotlag' AND platform='kalshi' AND ticker=? AND side='YES' AND episode=?
 AND LOWER(SUBSTR(TRIM(quote_source),1,LENGTH(?)))=LOWER(?)`,
		ticker, livePriorityProofGenerationEpisode, livePriorityProofQuoteSourcePrefix(),
		livePriorityProofQuoteSourcePrefix()).Scan(&current); err != nil {
		t.Fatal(err)
	}
	if all != 2 || current != 1 {
		t.Fatalf("generation receipt collided with legacy row: all=%d current=%d", all, current)
	}
}

func TestR163ProfitableHypotheticalRowsAndLiveZeroFillsCannotAuthorizeNormalSizing(t *testing.T) {
	s, c := r147AllocationServer(t)
	ctx := context.Background()

	// Prove the old opportunity-only calculation is strongly positive. Without the
	// fill-conditioned fence this exact fixture authorized normal multi-contract sizing.
	diagnosticOK, _, diagnosticMean, diagnosticLower, _ :=
		s.liveProspectiveDiagnosticProof(ctx, c, .40, false)
	if !diagnosticOK || diagnosticMean <= 0 ||
		diagnosticLower < s.liveAllocationEdgeFloor() {
		t.Fatalf("profitable hypothetical fixture unavailable: ok=%v mean=%v lower=%v",
			diagnosticOK, diagnosticMean, diagnosticLower)
	}

	// Every real attempt at the selected touch returns an authoritative IOC zero-fill. These
	// receipts are execution truth, but they are not positive settled fill evidence and therefore
	// can never upgrade the hypothetical cohort into cash authority.
	for i := 0; i < liveAllocationMinMarketsFloor; i++ {
		quote := liveMirrorQuote{
			Price:      .40,
			BookSource: fmt.Sprintf("kalshi_ws_full_orderbook:g1:s1:q%d", i+1),
			ObservedAt: time.Now().UTC(),
		}
		if !s.rememberFundedPaperLiveZeroFillBook(
			fmt.Sprintf("r163-proof-zero-fill-%03d", i), c.Ticker, c.Side,
			liveProspectiveIOC, "unfilled", true, 0, 1, .40, quote) {
			t.Fatalf("authoritative zero-fill %d was not accepted as venue truth", i)
		}
	}

	ok, why, mean, lower, _ := s.liveMirrorProof(ctx, c, .40, false)
	if ok || why != liveFillConditionedProofUnavailableReason ||
		mean <= 0 || lower < s.liveAllocationEdgeFloor() {
		t.Fatalf("hypothetical rows or zero-fills authorized cash: ok=%v why=%q mean=%v lower=%v",
			ok, why, mean, lower)
	}
	plan, planWhy := s.liveProspectivePlan(ctx, c,
		liveMirrorQuote{Price: .40, Depth: 20}, 400,
		verdictEnt{Mean: .10, MeanAsk: .40, FeePC: .01}, false)
	if plan.valid() || planWhy != liveFillConditionedProofUnavailableReason {
		t.Fatalf("normal quantity plan escaped fill-conditioned fence: plan=%+v why=%q",
			plan, planWhy)
	}
}

func TestR163ProofGenerationSpotLagCanaryStillUsesExplicitUnprovenOperatorLane(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = "kalshi|spotlag|YES|taker"
	cfg.Risk.LiveSystemCanaryAllowlist = "kalshi|spotlag|YES|taker"
	s.cfgP.Store(&cfg)
	s.kalFees = map[string]kalFeeInfo{
		"KXCANARY": {taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1},
	}
	s.kalFeesAt = time.Now()
	c := r147InsertAllocationHistoryOrigin(
		t, s, "spotlag", "YES", "model", liveAllocationMinMarketsFloor, 1)
	c.InputObservedAt = time.Now()
	cell := verdictEnt{Family: "taker:spotlag@kalshi", SourceFamily: "spotlag",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker",
		N: liveAllocationMinMarketsFloor, ContractMarkets: liveAllocationMinMarketsFloor,
		Mean: .08, Lo: -.02, MeanAsk: .40, FeePC: .01}
	cash, cashWhy := s.liveProspectivePlan(context.Background(), c,
		liveMirrorQuote{Price: .40, Depth: 10}, 400, cell, false)
	if cash.valid() || cashWhy != liveOneContractCanaryCashRetiredReason {
		t.Fatalf("Spot-lag model-point canary retained cash authority: plan=%+v why=%q", cash, cashWhy)
	}
	plan, why := s.liveProspectiveDiagnosticPlan(context.Background(), c,
		liveMirrorQuote{Price: .40, Depth: 10}, 400, cell, false)
	if why != "" || !plan.valid() || !plan.Canary || plan.Qty != 1 ||
		plan.TimeInForce != liveProspectiveIOC ||
		!strings.HasPrefix(plan.Basis, "one-contract-canary:UNPROVEN:") {
		t.Fatalf("Spot-lag canary path changed: plan=%+v why=%q", plan, why)
	}
}
