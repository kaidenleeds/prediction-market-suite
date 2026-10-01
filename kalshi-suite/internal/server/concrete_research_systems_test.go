package server

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/ev"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR139ConcretePairedControlsGradeForTrainingButNeverBecomeEconomicN(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	eventID, payoffID := "venue:kalshi:PAIR", "payoff:kalshi:PAIR:YES"
	if _, err := s.store.RegisterCanonicalBatch(ctx, []storage.CanonicalEventSpec{{EventID: eventID,
		EventType: "fixture", Domain: "test", SettlementSource: "fixture", SourceArtifact: "fixture",
		SourceClockID: "fixture", OutcomeSetStatus: "complete", EvidenceJSON: `{}`}},
		[]storage.CanonicalPayoffSpec{{PayoffID: payoffID, EventID: eventID, PredicateJSON: `{}`,
			SettlementSource: "fixture", SourceArtifact: "fixture", IdentityStatus: "verified",
			PayoutFloor: 0, PayoutCeiling: 1, EvidenceJSON: `{}`}},
		[]storage.CanonicalInstrumentSpec{{Venue: "kalshi", Ticker: "PAIR", EventID: eventID,
			PayoffID: payoffID, NativeSide: "YES", Orientation: "same", MarketKind: "binary",
			SettlementSource: "fixture", RulesArtifact: "fixture", RulesHash: "rules",
			FeeAuthority: "fee-v1", IdentityStatus: "verified", EvidenceJSON: `{}`}}); err != nil {
		t.Fatal(err)
	}
	identity, ok, err := s.store.CurrentCanonicalInstrument(ctx, "kalshi", "PAIR")
	if err != nil || !ok {
		t.Fatalf("identity=%+v ok=%v err=%v", identity, ok, err)
	}
	opened := now.Add(-2 * time.Hour)
	legs := []nativeExactLeg{{Venue: "kalshi", Ticker: "PAIR", Side: "YES", Ask: .41,
		Depth: 7, Tick: .01, Fee: .01, QuoteAge: .1, BookSource: "kalshi_book_ws", FeeSource: "fee-v1"},
		{Venue: "kalshi", Ticker: "PAIR", Side: "NO", Ask: .61,
			Depth: 5, Tick: .01, Fee: .01, QuoteAge: .1, BookSource: "kalshi_book_ws", FeeSource: "fee-v1"}}
	pairInput := concretePairInput{
		System: "paired-bridge-inversion", Opportunity: "pair-fixture", Cohort: "same-clock-direct-inverse",
		SourceArtifact: "fixture current book", CertificateHash: "fixture-certificate",
		Blocker: "paired control", Identity: identity, Observed: opened, Legs: legs,
		Inputs: map[string]any{"same_clock": true}}
	inserted, duplicates, err := s.insertConcretePair(ctx, pairInput)
	if err != nil || inserted != 2 || duplicates != 0 {
		t.Fatalf("inserted/duplicates=%d/%d err=%v", inserted, duplicates, err)
	}
	// Cursor recovery deliberately replays a page when a process dies before its durable bookmark.
	// The immutable opportunity/cohort keys must turn that replay into duplicates, never new n.
	pairInput.Observed = opened.Add(time.Minute) // a real later collector cycle has a new wall clock
	inserted, duplicates, err = s.insertConcretePair(ctx, pairInput)
	if err != nil || inserted != 0 || duplicates != 2 {
		t.Fatalf("replay inserted/duplicates=%d/%d err=%v", inserted, duplicates, err)
	}
	seedExactVenueSettlement(t, s.store, "kalshi", "PAIR", 1, opened.Add(-time.Hour), opened.Add(time.Hour))
	s.sweepResearchSystemTerminalGrades(ctx, now)
	var observations, candidates, updates, authority int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*),COALESCE(SUM(candidate),0),
COALESCE(SUM(funded+paper_authority+live_authority),0) FROM research_system_observations
WHERE system_id='paired-bridge-inversion'`).Scan(&observations, &candidates, &authority); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM research_system_payoff_updates`).Scan(&updates); err != nil {
		t.Fatal(err)
	}
	if observations != 2 || candidates != 0 || updates != 2 || authority != 0 {
		t.Fatalf("observations/candidates/updates/authority=%d/%d/%d/%d", observations, candidates, updates, authority)
	}
	report, _, err := s.store.RunR139ResearchInference(ctx, now)
	if err != nil || report.ExactTerminalRows != 0 {
		t.Fatalf("controls entered economic inference: exact=%d err=%v", report.ExactTerminalRows, err)
	}
}

func TestR139ConcreteFrozenSelectionSeparatesCandidateAndMatchedControlCohorts(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	eventID, payoffID := "venue:kalshi:SELECTED", "payoff:kalshi:SELECTED:YES"
	if _, err := s.store.RegisterCanonicalBatch(ctx, []storage.CanonicalEventSpec{{EventID: eventID,
		EventType: "fixture", Domain: "test", SourceArtifact: "fixture", SourceClockID: "fixture",
		OutcomeSetStatus: "complete", EvidenceJSON: `{}`}}, []storage.CanonicalPayoffSpec{{PayoffID: payoffID,
		EventID: eventID, PredicateJSON: `{}`, SourceArtifact: "fixture", IdentityStatus: "verified",
		PayoutFloor: 0, PayoutCeiling: 1, EvidenceJSON: `{}`}}, []storage.CanonicalInstrumentSpec{{
		Venue: "kalshi", Ticker: "SELECTED", EventID: eventID, PayoffID: payoffID, NativeSide: "YES",
		Orientation: "same", MarketKind: "binary", RulesArtifact: "fixture", RulesHash: "rules",
		FeeAuthority: "fee-v1", IdentityStatus: "verified", EvidenceJSON: `{}`}}); err != nil {
		t.Fatal(err)
	}
	identity, ok, err := s.store.CurrentCanonicalInstrument(ctx, "kalshi", "SELECTED")
	if err != nil || !ok {
		t.Fatalf("identity=%+v ok=%v err=%v", identity, ok, err)
	}
	legs := []nativeExactLeg{{Venue: "kalshi", Ticker: "SELECTED", Side: "YES", Ask: .41,
		Depth: 7, Tick: .01, Fee: .01, QuoteAge: .1, BookSource: "kalshi_book_ws", FeeSource: "fee-v1"},
		{Venue: "kalshi", Ticker: "SELECTED", Side: "NO", Ask: .61, Depth: 5, Tick: .01,
			Fee: .01, QuoteAge: .1, BookSource: "kalshi_book_ws", FeeSource: "fee-v1"}}
	strategyFamily, selectorID, baseCohort, identityOK := concreteNativeInverseIdentity("kalshi-flow", "kalshi", "NO")
	if !identityOK || strategyFamily != "invert:kalshi-flow" {
		t.Fatalf("native inverse identity=%q/%q/%q ok=%v", strategyFamily, selectorID, baseCohort, identityOK)
	}
	inserted, _, err := s.insertConcretePair(ctx, concretePairInput{System: "paired-bridge-inversion",
		Opportunity: "selected-fixture", Cohort: baseCohort, SourceArtifact: "fixture",
		CertificateHash: "fixture", Blocker: "matched control", SelectedSide: "YES",
		SelectorID: selectorID, Identity: identity, Observed: now, Legs: legs,
		Inputs: map[string]any{"strategy_family": strategyFamily, "fired_side": "NO",
			"inverted_side": "YES", "execution_route": "taker"}})
	if err != nil || inserted != 2 {
		t.Fatalf("inserted=%d err=%v", inserted, err)
	}
	rows, err := s.store.DBForTest().Query(`SELECT observation_kind,candidate,side,cohort,blocker
FROM research_system_observations ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seenCandidate, seenControl := false, false
	for rows.Next() {
		var kind, side, cohort, blocker string
		var candidate int
		if err := rows.Scan(&kind, &candidate, &side, &cohort, &blocker); err != nil {
			t.Fatal(err)
		}
		seenCandidate = seenCandidate || kind == "candidate" && candidate == 1 && side == "YES" && blocker == "" &&
			cohort == baseCohort+"|candidate|policy="+selectorID
		seenControl = seenControl || kind == "control" && candidate == 0 && side == "NO" && blocker != "" &&
			cohort == baseCohort+"|matched-control|policy="+selectorID
	}
	if !seenCandidate || !seenControl {
		t.Fatalf("selected/matched lanes missing candidate=%v control=%v", seenCandidate, seenControl)
	}
}

func TestR139RuleComplexityFeaturesAreDeterministicAndRawOnly(t *testing.T) {
	a := concreteRuleFeatures(`{"primary":"If A; unless B; void on cancel","early_close":"UTC"}`, `[{"name":"official"}]`)
	b := concreteRuleFeatures(`{"primary":"If A; unless B; void on cancel","early_close":"UTC"}`, `[{"name":"official"}]`)
	if storage.R138HashJSON(a) != storage.R138HashJSON(b) || a["has_void_or_cancel"] != true ||
		a["has_timezone_boundary"] != true || a["settlement_source_count"] != 1 {
		t.Fatalf("features a=%+v b=%+v", a, b)
	}
}

func TestR139CollateralNoReleaseControlIsDeterministicAndStructurallyMatched(t *testing.T) {
	target := storage.MarketGameRow{Venue: "kalshi", Ticker: "release-child", GameID: "release-game",
		MktType: "spread", YesTeam: "A", Line: -1.5, Src: "struct"}
	candidates := []collateralComparable{
		{Market: storage.MarketGameRow{Venue: "polyus", Ticker: "wrong-venue", GameID: "g1", MktType: "spread", Line: -1.5, Src: "struct"}, League: "nba", Phase: "pre", Liquidity: "medium_5_24"},
		{Market: storage.MarketGameRow{Venue: "kalshi", Ticker: "same-game", GameID: "release-game", MktType: "spread", Line: -1.5, Src: "struct"}, League: "nba", Phase: "pre", Liquidity: "medium_5_24"},
		{Market: storage.MarketGameRow{Venue: "kalshi", Ticker: "credited", GameID: "credited-game", MktType: "spread", Line: -1.5, Src: "struct"}, League: "nba", Phase: "pre", Liquidity: "medium_5_24"},
		{Market: storage.MarketGameRow{Venue: "kalshi", Ticker: "wrong-line", GameID: "g2", MktType: "spread", Line: -2.5, Src: "struct"}, League: "nba", Phase: "pre", Liquidity: "medium_5_24"},
		{Market: storage.MarketGameRow{Venue: "kalshi", Ticker: "wrong-phase", GameID: "g3", MktType: "spread", Line: -1.5, Src: "struct"}, League: "nba", Phase: "live_window", Liquidity: "medium_5_24"},
		{Market: storage.MarketGameRow{Venue: "kalshi", Ticker: "eligible-a", GameID: "g4", MktType: "spread", Line: -1.5, Src: "struct"}, League: "nba", Phase: "pre", Liquidity: "medium_5_24"},
		{Market: storage.MarketGameRow{Venue: "kalshi", Ticker: "eligible-b", GameID: "g5", MktType: "spread", Line: -1.5, Src: "struct"}, League: "nba", Phase: "pre", Liquidity: "medium_5_24"},
	}
	credited := map[string]bool{"credited-game": true}
	first, ok := chooseCollateralNoReleaseControl(target, "nba", "pre", "medium_5_24", "credit-hash", candidates, credited)
	if !ok || (first.Ticker != "eligible-a" && first.Ticker != "eligible-b") {
		t.Fatalf("first=%+v ok=%v", first, ok)
	}
	for left, right := 0, len(candidates)-1; left < right; left, right = left+1, right-1 {
		candidates[left], candidates[right] = candidates[right], candidates[left]
	}
	second, ok := chooseCollateralNoReleaseControl(target, "nba", "pre", "medium_5_24", "credit-hash", candidates, credited)
	if !ok || second.Ticker != first.Ticker {
		t.Fatalf("order changed deterministic match first=%+v second=%+v", first, second)
	}
}

func TestR139ConcreteCompletionRuntimeTruthRemovesClosedInternalGaps(t *testing.T) {
	rows := r138RuntimeCoverageContracts()
	byID := map[string]storage.ResearchSystemRuntimeReceipt{}
	for _, row := range rows {
		byID[row.SystemID] = row
	}
	attention := byID["attention-spillover-graph"]
	if attention.State != "COLLECTING" || !strings.Contains(strings.Join(attention.EvidenceTables, "|"), "research_attention_markouts") ||
		strings.Contains(strings.Join(attention.Prerequisites, "|"), "durable 60s") {
		t.Fatalf("stale attention truth: %+v", attention)
	}
	collateral := byID["collateral-release-rotation"]
	if !strings.Contains(collateral.Reason, "no-release market controls") ||
		strings.Contains(strings.Join(collateral.Prerequisites, "|"), "matched no-release") {
		t.Fatalf("stale collateral truth: %+v", collateral)
	}
	carry := byID["settlement-latency-carry"]
	if !strings.Contains(strings.Join(carry.EvidenceTables, "|"), "research_carry_frozen_inputs") ||
		strings.Contains(strings.Join(carry.Prerequisites, "|"), "frozen void") ||
		!strings.Contains(carry.Reason, "immutably blocked") {
		t.Fatalf("stale carry truth: %+v", carry)
	}
}

func TestR139ConcreteCarryPreservesAuthoritativeClockAndLabelsLegacyFallback(t *testing.T) {
	now := time.Now().UTC()
	authoritative := "kalshi-book:g2:sid7:seq91|received=2026-07-12T12:00:00Z"
	if got := concreteLegClockID(nativeExactLeg{BookSource: "kalshi_book_ws_full", SourceClockID: authoritative}, now); got != authoritative {
		t.Fatalf("authoritative source clock changed: %q", got)
	}
	legacy := concreteLegClockID(nativeExactLeg{BookSource: "historical-book-native-v2"}, now)
	if !strings.HasPrefix(legacy, "legacy-receipt-clock:") || strings.Contains(legacy, "kalshi-book:g") {
		t.Fatalf("legacy receipt clock mislabeled as venue source: %q", legacy)
	}
}

func TestR144ConcreteCarryControlReplayDoesNotInflateAcrossSlots(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	observed := time.Now().UTC().Add(-time.Hour)
	inserted, err := s.insertConcreteObserverControl(ctx, "settlement-latency-carry",
		"carry-freeze|fixture|no-trade", "frozen-carry-matched-no-trade", "fixture",
		"fixture-certificate", "prior inputs unavailable", observed, map[string]any{"freeze_id": "fixture"})
	if err != nil || !inserted {
		t.Fatalf("first carry control inserted=%v err=%v", inserted, err)
	}
	inserted, err = s.insertConcreteObserverControl(ctx, "settlement-latency-carry",
		"carry-freeze|fixture|no-trade", "frozen-carry-matched-no-trade", "fixture",
		"fixture-certificate", "prior inputs unavailable", observed.Add(10*time.Minute),
		map[string]any{"freeze_id": "fixture"})
	if err != nil || inserted {
		t.Fatalf("replayed carry control inserted=%v err=%v", inserted, err)
	}
	var n int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM research_system_observations
WHERE system_id='settlement-latency-carry' AND opportunity_id='carry-freeze|fixture|no-trade'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("carry replay rows=%d err=%v", n, err)
	}
}

func TestR143ConcreteReceiptContextFailureCannotMasqueradeAsHealthyEmpty(t *testing.T) {
	for _, tc := range []struct {
		name, wantClass string
		err             error
	}{
		{name: "deadline", wantClass: "timeout", err: context.DeadlineExceeded},
		{name: "canceled", wantClass: "canceled", err: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receipt := storage.CollectorReceipt{Status: "healthy", Metrics: map[string]any{}, Exclusions: map[string]int{}}
			finalizeConcreteResearchReceipt(&receipt, map[string]int{}, tc.err)
			if receipt.Status != "error" || receipt.ErrorClass != tc.wantClass || receipt.ErrorText == "" ||
				receipt.ExpectedZero || receipt.ZeroReason != "" {
				t.Fatalf("receipt=%+v", receipt)
			}
		})
	}
}

func TestR143ConcreteReceiptMetricErrorsCannotMasqueradeAsHealthyEmpty(t *testing.T) {
	receipt := storage.CollectorReceipt{Status: "healthy", Metrics: map[string]any{}, Exclusions: map[string]int{}}
	finalizeConcreteResearchReceipt(&receipt, map[string]int{
		"series_inserted":   0,
		"series_read_error": 2,
		"unused_error":      0,
	}, nil)
	if receipt.Status != "error" || receipt.ErrorClass != "collector_metrics" ||
		!strings.Contains(receipt.ErrorText, "series_read_error=2") || receipt.ExpectedZero || receipt.ZeroReason != "" {
		t.Fatalf("receipt=%+v", receipt)
	}
	if receipt.Exclusions["series_read_error"] != 2 {
		t.Fatalf("error metric not retained as an exclusion: %+v", receipt.Exclusions)
	}
}

func TestR143ConcreteReceiptCleanZeroRemainsHealthyEmpty(t *testing.T) {
	receipt := storage.CollectorReceipt{Status: "healthy", Metrics: map[string]any{}, Exclusions: map[string]int{}}
	finalizeConcreteResearchReceipt(&receipt, map[string]int{"series_inserted": 0, "unused_error": 0}, nil)
	if receipt.Status != "healthy_empty" || !receipt.ExpectedZero || receipt.ZeroReason == "" ||
		receipt.ErrorClass != "" || receipt.ErrorText != "" {
		t.Fatalf("receipt=%+v", receipt)
	}
}

func TestR139AttentionKalshiExitUsesActionCorrectSellFee(t *testing.T) {
	s := testServer(t)
	seedR135KalshiFee(s)
	const price = .003
	got, source, known := s.concreteAttentionExitFee("kalshi", "KXROUND-MARKET", price)
	wantSell := ev.FeeBreakdownCoeff(.07, 1, price, false).Net
	buy := ev.FeeBreakdownCoeff(.07, 1, price, true).Net
	if !known || source != "quadratic" || math.Abs(got-wantSell) > 1e-12 || got == buy {
		t.Fatalf("exit fee=%v known=%v source=%q want sell=%v distinct buy=%v", got, known, source, wantSell, buy)
	}
}
