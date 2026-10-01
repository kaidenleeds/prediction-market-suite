package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/properbetting"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func properScorePaperTestBook(now time.Time) properScoreBook {
	return properScoreBook{
		yesBid: .39, yesAsk: .40, yesBidDepth: 100, yesAskDepth: 100,
		noBid: .60, noAsk: .61, noBidDepth: 100, noAskDepth: 100,
		tick: .01, lot: 1, source: "fixture-full-book",
		sourceClock:   "fixture-clock:" + now.Format(time.RFC3339Nano),
		sourceClockAt: now.UTC(), age: .01,
		yesBids: []properbetting.Level{{Price: .39, Quantity: 100, Tick: .01}},
		yesAsks: []properbetting.Level{{Price: .40, Quantity: 100, Tick: .01}},
		noBids:  []properbetting.Level{{Price: .60, Quantity: 100, Tick: .01}},
		noAsks:  []properbetting.Level{{Price: .61, Quantity: 100, Tick: .01}},
	}
}

func registerProperScorePaperInstrument(t *testing.T, s *Server, ticker, eventID string) {
	t.Helper()
	payoffID := eventID + ":yes"
	if _, err := s.store.RegisterCanonicalBatch(context.Background(),
		[]storage.CanonicalEventSpec{{EventID: eventID, EventType: "fixture", Domain: "test",
			SettlementSource: "fixture", SourceArtifact: "fixture",
			SourceClockID: "fixture-clock", OutcomeSetStatus: "complete", EvidenceJSON: `{}`}},
		[]storage.CanonicalPayoffSpec{{PayoffID: payoffID, EventID: eventID,
			PredicateJSON: `{}`, SettlementSource: "fixture", SourceArtifact: "fixture",
			IdentityStatus: "verified", PayoutFloor: 0, PayoutCeiling: 1, EvidenceJSON: `{}`}},
		[]storage.CanonicalInstrumentSpec{{Venue: "kalshi", Ticker: ticker, EventID: eventID,
			PayoffID: payoffID, NativeSide: "YES", Orientation: "same", MarketKind: "binary",
			SettlementSource: "fixture", RulesArtifact: "fixture", RulesHash: "rules",
			FeeAuthority: "fee-v1", IdentityStatus: "verified", EvidenceJSON: `{}`}}); err != nil {
		t.Fatal(err)
	}
}

func TestProperScorePaperVectorHasOneFivePercentBudgetNotPerCoordinate(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{"KXPSPP": {
		taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1}}
	s.kalFeesAt = now
	s.kalFeeMu.Unlock()
	registerProperScorePaperInstrument(t, s, "KXPSPP-A", "EVENT-A")
	registerProperScorePaperInstrument(t, s, "KXPSPP-B", "EVENT-B")
	book := properScorePaperTestBook(now)
	rows := []properScorePrepared{
		{forecast: properScoreForecast{Ticker: "KXPSPP-A", Title: "A", ResolveHours: 1},
			platform: "kalshi", pYes: .75, book: book},
		{forecast: properScoreForecast{Ticker: "KXPSPP-B", Title: "B", ResolveHours: 1},
			platform: "kalshi", pYes: .75, book: book},
	}
	candidates := []properScoreCandidate{
		{side: "YES", normalized: .25, expected: .34},
		{side: "YES", normalized: .75, expected: .34},
	}
	planned, err := s.claimProperScorePaperVector(context.Background(), now,
		now.Truncate(5*time.Minute).Format(time.RFC3339), properScorePaperVector{
			transform: "brier", rows: rows, candidates: candidates, reasons: []string{"", ""},
		}, "ml-book-v2-calibrated", "fixture-v1", 750*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 2 {
		t.Fatalf("planned=%d, want two independent-event coordinates", len(planned))
	}
	for _, attempt := range planned {
		if attempt.shadowID == "" || attempt.signal.SignalType != "proper-score-executor" ||
			r147SignalInputTopology(attempt.signal) != "K-PROPER-BRIER" {
			t.Fatalf("Proper Paper attempt lost canonical transform lineage: %+v", attempt)
		}
	}
	var vectorMin, vectorMax, coordinateSum, reservationSum float64
	if err = s.store.DBForTest().QueryRow(`SELECT MIN(vector_budget_usd),MAX(vector_budget_usd),
SUM(coordinate_budget_usd),SUM(reservation_usd) FROM proper_score_paper_attempts
WHERE lane='brier'`).Scan(&vectorMin, &vectorMax, &coordinateSum, &reservationSum); err != nil {
		t.Fatal(err)
	}
	if math.Abs(vectorMin-20) > 1e-9 || math.Abs(vectorMax-20) > 1e-9 ||
		math.Abs(coordinateSum-20) > 1e-9 || reservationSum > 20+1e-9 {
		t.Fatalf("vector budget min/max=%v/%v coordinate sum=%v reserved=%v",
			vectorMin, vectorMax, coordinateSum, reservationSum)
	}
	var canonicalIDs int
	if err = s.store.DBForTest().QueryRow(`SELECT COUNT(DISTINCT execution_shadow_attempt_id)
FROM proper_score_paper_attempts WHERE lane='brier' AND execution_shadow_attempt_id<>''`).Scan(
		&canonicalIDs); err != nil || canonicalIDs != 2 {
		t.Fatalf("canonical Proper Paper ids=%d err=%v", canonicalIDs, err)
	}
}

func TestProperScoreTransformsDoNotShareCanonicalOpportunityIdentity(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	row := properScorePrepared{forecast: properScoreForecast{Ticker: "KXPSPP-TRANSFORM"},
		platform: "kalshi", pYes: .70, book: properScorePaperTestBook(now)}
	candidate := properScoreCandidate{side: "YES", normalized: 1, expected: .10}
	ids := map[string]bool{}
	contracts := map[string]bool{}
	for _, transform := range []string{"brier", "log", "spherical"} {
		sig, ok := s.properScorePaperCanonicalSignal(row, candidate, transform)
		if !ok || r147SignalInputTopology(sig) != properScorePaperInputTopology("kalshi", transform) {
			t.Fatalf("%s canonical signal=%+v ok=%v", transform, sig, ok)
		}
		id := s.executionShadowSignalAttemptID(sig, 0, now)
		if id == "" || ids[id] {
			t.Fatalf("%s reused canonical id %q across scoring transforms", transform, id)
		}
		ids[id] = true
		candidate := liveCandidateFromSignalIntent(liveSignalIntent{Signal: sig, At: now})
		bound, why, declared := bindStaticTakerSignalCandidate(candidate, sig)
		if !declared || why != "" || bound.SignalContractID == "" ||
			contracts[bound.SignalContractID] {
			t.Fatalf("%s contract declared=%v why=%q bound=%+v", transform, declared, why, bound)
		}
		contracts[bound.SignalContractID] = true
	}
}

func TestProperScorePaperSizedBuyUsesFullDepthFeeAndOriginalLimit(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{"KXPSPP": {
		taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1}}
	s.kalFeesAt = now
	s.kalFeeMu.Unlock()
	row := properScorePrepared{
		forecast: properScoreForecast{Ticker: "KXPSPP-A"}, platform: "kalshi",
		pYes: .75, book: properScorePaperTestBook(now),
	}
	c := properScoreCandidate{side: "YES"}
	decision, source, ok := s.properScorePaperSizedBuy(row, c, 20, 0, 0)
	if !ok || source == "" || decision.Filled <= 0 || decision.IntegratedCost+decision.Fee > 20+1e-9 ||
		decision.ExpectedNet <= 0 {
		t.Fatalf("decision=%+v source=%q ok=%v", decision, source, ok)
	}
	limit := properScorePaperLimit(decision)
	worse := row
	worse.book.yesAsks = []properbetting.Level{{Price: limit + .01, Quantity: 100, Tick: .01}}
	if fill, _, ok := s.properScorePaperSizedBuy(worse, c, 20, decision.Filled, limit); ok || fill.Filled != 0 {
		t.Fatalf("worse-than-limit book filled: %+v", fill)
	}
	better := row
	better.book.yesAsks = []properbetting.Level{{Price: limit - .01, Quantity: 2, Tick: .01}}
	fill, _, ok := s.properScorePaperSizedBuy(better, c, 20, decision.Filled, limit)
	if !ok || fill.Filled != 2 || fill.AveragePrice >= limit {
		t.Fatalf("improved partial IOC=%+v ok=%v", fill, ok)
	}
}

func TestProperScorePaperDelayedIOCRequiresNewerVenueReceipt(t *testing.T) {
	now := time.Now().UTC()
	decision := properScorePaperTestBook(now)
	same := decision
	if properScorePaperExecutionBookNewer(decision, same) {
		t.Fatal("the unchanged decision snapshot cannot prove delayed IOC liquidity")
	}
	newer := decision
	newer.sourceClock = "fixture-clock:" + now.Add(time.Millisecond).Format(time.RFC3339Nano)
	newer.sourceClockAt = now.Add(time.Millisecond)
	if !properScorePaperExecutionBookNewer(decision, newer) {
		t.Fatal("a distinct later venue receipt should be eligible for the delayed IOC check")
	}
	equalClock := newer
	equalClock.sourceClock = "different-id-same-time"
	equalClock.sourceClockAt = decision.sourceClockAt
	if properScorePaperExecutionBookNewer(decision, equalClock) {
		t.Fatal("a different receipt id at the same source time is not later")
	}
	regressed := newer
	regressed.sourceClockAt = now.Add(-time.Millisecond)
	if properScorePaperExecutionBookNewer(decision, regressed) {
		t.Fatal("a regressed source clock cannot authorize a delayed Paper fill")
	}
	missing := newer
	missing.sourceClock = ""
	if properScorePaperExecutionBookNewer(decision, missing) {
		t.Fatal("missing execution provenance cannot authorize a Paper fill")
	}
	missingTime := newer
	missingTime.sourceClockAt = time.Time{}
	if properScorePaperExecutionBookNewer(decision, missingTime) {
		t.Fatal("an opaque id without a comparable authoritative time cannot authorize a fill")
	}
}

func TestProperScorePaperUsesTheCanonicalAbsoluteExecutionWindow(t *testing.T) {
	due := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	if properScorePaperExecutionStartedLate(due, due.Add(livePolicyMirrorStartLateMax)) {
		t.Fatal("exact lateness boundary should remain eligible")
	}
	if !properScorePaperExecutionStartedLate(due,
		due.Add(livePolicyMirrorStartLateMax+time.Nanosecond)) {
		t.Fatal("worker admission shifted Proper execution beyond the canonical window")
	}
	if properScorePaperExecutionStartedLate(time.Time{}, due.Add(time.Hour)) {
		t.Fatal("legacy hermetic attempt without a schedule acquired a fake late clock")
	}
}

func properScorePaperThreeLaneDueFixture(t *testing.T, s *Server, decisionAt,
	due time.Time) []properScorePaperPlannedAttempt {
	t.Helper()
	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{"KXPSPP": {
		taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1}}
	s.kalFeesAt = time.Now().UTC()
	s.kalFeeMu.Unlock()
	row := properScorePrepared{
		forecast: properScoreForecast{Ticker: "KXPSPP-SHARED-DUE", Title: "shared due",
			ResolveHours: 1},
		platform: "kalshi", pYes: .75, book: properScorePaperTestBook(decisionAt),
	}
	candidate := properScoreCandidate{side: "YES", normalized: 1, expected: .35}
	planned := make([]properScorePaperPlannedAttempt, 0, 3)
	for i, transform := range []string{"brier", "log", "spherical"} {
		sig, ok := s.properScorePaperCanonicalSignal(row, candidate, transform)
		if !ok {
			t.Fatalf("%s fixture signal unavailable", transform)
		}
		planned = append(planned, properScorePaperPlannedAttempt{
			id: int64(i + 1), row: row, candidate: candidate, quantity: 10,
			limit: .40, reserved: 20, shadowID: "proper-due-" + transform,
			signal: sig, executeAt: due,
		})
	}
	return planned
}

func TestR170ProperScorePaperSharesOneDueBookAcrossThreeIndependentLanes(t *testing.T) {
	s := testServer(t)
	due := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	planned := properScorePaperThreeLaneDueFixture(t, s, due.Add(-750*time.Millisecond), due)
	executionBook := properScorePaperTestBook(due)
	executionBook.source = "fixture-shared-due-book"
	executionBook.sourceClock = "fixture-shared-due-clock"
	executionBook.sourceClockAt = due

	lookupCalls, horizonCalls := 0, 0
	lookup := func(context.Context, string, string) (properScoreBook, bool) {
		lookupCalls++
		if lookupCalls > 1 {
			changed := cloneProperScorePaperBook(executionBook)
			changed.yesAsk, changed.yesAsks[0].Price = .55, .55
			return changed, true
		}
		return executionBook, true
	}
	horizon := func(context.Context, string, string, string) bool {
		horizonCalls++
		return true
	}
	prepared := s.prepareProperScorePaperDueExecutions(context.Background(), planned,
		func() time.Time { return due }, lookup, horizon)
	if lookupCalls != 1 || horizonCalls != 1 {
		t.Fatalf("same scheduled market was read lookup=%d horizon=%d times, want one each",
			lookupCalls, horizonCalls)
	}
	if len(prepared) != 3 {
		t.Fatalf("prepared=%d, want three independent scoring histories", len(prepared))
	}
	ids, shadows, topologies := map[int64]bool{}, map[string]bool{}, map[string]bool{}
	firstCost, firstFee := 0.0, 0.0
	for _, execution := range prepared {
		outcome := execution.outcome
		if ids[execution.attempt.id] || shadows[execution.attempt.shadowID] {
			t.Fatalf("three scoring lanes collapsed independent history: %+v", execution.attempt)
		}
		ids[execution.attempt.id], shadows[execution.attempt.shadowID] = true, true
		topologies[r147SignalInputTopology(execution.book)] = true
		if outcome.State != "filled" || outcome.Reason != "" ||
			outcome.SourceClockID != executionBook.sourceClock ||
			outcome.BookSource != executionBook.source ||
			math.Abs(outcome.FillPrice-.40) > 1e-9 || !outcome.ProcessedAt.Equal(due) {
			t.Fatalf("lane %d did not consume the shared due-time economics: %+v",
				execution.attempt.id, outcome)
		}
		if firstCost == 0 {
			firstCost, firstFee = outcome.CostUSD, outcome.FeeUSD
		} else if math.Abs(outcome.CostUSD-firstCost) > 1e-9 ||
			math.Abs(outcome.FeeUSD-firstFee) > 1e-9 {
			t.Fatalf("fixed lane order changed due-time cost/fee: first=%v/%v lane=%v/%v",
				firstCost, firstFee, outcome.CostUSD, outcome.FeeUSD)
		}
	}
	for _, topology := range []string{"K-PROPER-BRIER", "K-PROPER-LOG", "K-PROPER-SPHERICAL"} {
		if !topologies[topology] {
			t.Fatalf("independent transform topology %s disappeared: %+v", topology, topologies)
		}
	}
	// Mutating the lookup's slices after preparation cannot rewrite any lane's frozen result.
	executionBook.yesAsks[0].Price = .99
	for _, execution := range prepared {
		if math.Abs(execution.outcome.FillPrice-.40) > 1e-9 {
			t.Fatalf("prepared due-time economics mutated later: %+v", execution.outcome)
		}
	}
}

func TestR170ProperScorePaperSharedDueBookMissZeroFillsEveryLane(t *testing.T) {
	s := testServer(t)
	due := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	planned := properScorePaperThreeLaneDueFixture(t, s, due.Add(-750*time.Millisecond), due)
	lookupCalls := 0
	prepared := s.prepareProperScorePaperDueExecutions(context.Background(), planned,
		func() time.Time { return due },
		func(context.Context, string, string) (properScoreBook, bool) {
			lookupCalls++
			return properScoreBook{}, false
		},
		func(context.Context, string, string, string) bool { return true })
	if lookupCalls != 1 || len(prepared) != 3 {
		t.Fatalf("unavailable shared book calls=%d prepared=%d", lookupCalls, len(prepared))
	}
	for _, execution := range prepared {
		if execution.outcome.State != "zero_fill" ||
			execution.outcome.Reason != "execution-book-incomplete-or-stale-after-delay" ||
			execution.outcome.FilledQty != 0 || execution.outcome.CostUSD != 0 ||
			execution.outcome.FeeUSD != 0 {
			t.Fatalf("unavailable due-time book became a lane fill: %+v", execution.outcome)
		}
	}
}

func TestProperScorePolyUSClockRequiresVenueSourceTime(t *testing.T) {
	if clock, at, ok := properScorePolyUSSourceClock(time.Time{}); ok || clock != "" || !at.IsZero() {
		t.Fatalf("missing PolyUS source time acquired authority clock=%q at=%v ok=%v", clock, at, ok)
	}
	sourceAt := time.Date(2026, 7, 17, 12, 0, 0, 123, time.UTC)
	clock, at, ok := properScorePolyUSSourceClock(sourceAt)
	if !ok || !at.Equal(sourceAt) || clock != "polyus-market-data:"+sourceAt.Format(time.RFC3339Nano) {
		t.Fatalf("PolyUS source clock=%q at=%v ok=%v", clock, at, ok)
	}
}

func TestProperScorePaperBackgroundRecoveryRunsWithoutNewVector(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	in := storage.ProperScorePaperAttempt{
		AttemptKey: "background-recovery", Lane: "brier", Transform: "brier",
		Slot:     now.Truncate(5 * time.Minute).Format(time.RFC3339),
		Platform: "kalshi", Ticker: "KXPSPP-RECOVER", EventKey: "EVENT-RECOVER",
		Title: "recover", Side: "YES", ObservedAt: now.Add(-time.Minute),
		ExecuteAfter: now.Add(-time.Minute).Add(750 * time.Millisecond),
		ForecastYes:  .70, NormalizedWeight: 1, VectorBudgetUSD: 20,
		CoordinateBudgetUSD: 20, LimitPrice: .40, RequestedQty: 10,
		ReservationUSD: 4.20, FeeSource: "fixture-fee", BookSource: "fixture-book",
		SourceClockID: "fixture-clock", ExecutionShadowAttemptID: "exec-background-recovery",
		DelayMS: 750,
	}
	id, claimed, _, err := s.store.ClaimProperScorePaperAttempt(context.Background(), in)
	if err != nil || !claimed {
		t.Fatalf("claim id=%d claimed=%v err=%v", id, claimed, err)
	}
	s.settleProperScoreTrials(context.Background())
	var state, reason string
	if err = s.store.DBForTest().QueryRow(`SELECT state,reason
FROM proper_score_paper_outcomes WHERE attempt_id=?`, id).Scan(&state, &reason); err != nil {
		t.Fatal(err)
	}
	if state != "zero_fill" || reason != "process-ended-before-delayed-IOC-check" {
		t.Fatalf("background recovery state=%q reason=%q", state, reason)
	}
}

func TestProperScorePaperBatchFailureTerminalizesAndContinues(t *testing.T) {
	s := testServer(t)
	ctx, now := context.Background(), time.Now().UTC()
	var planned []properScorePaperPlannedAttempt
	for i := 0; i < 3; i++ {
		in := storage.ProperScorePaperAttempt{
			AttemptKey: fmt.Sprintf("batch-%d", i), Lane: "brier", Transform: "brier",
			Slot:     now.Truncate(5 * time.Minute).Format(time.RFC3339),
			Platform: "kalshi", Ticker: fmt.Sprintf("KXPSPP-BATCH-%d", i),
			EventKey: fmt.Sprintf("EVENT-BATCH-%d", i), Title: "batch", Side: "YES",
			ObservedAt: now, ExecuteAfter: now.Add(750 * time.Millisecond),
			ForecastYes: .70, NormalizedWeight: 1.0 / 3, VectorBudgetUSD: 20,
			CoordinateBudgetUSD: 20.0 / 3, LimitPrice: .40, RequestedQty: 10,
			ReservationUSD: 5, FeeSource: "fixture-fee", BookSource: "fixture-book",
			SourceClockID:            "fixture-clock",
			ExecutionShadowAttemptID: fmt.Sprintf("exec-batch-%d", i), DelayMS: 750,
		}
		id, claimed, _, err := s.store.ClaimProperScorePaperAttempt(ctx, in)
		if err != nil || !claimed {
			t.Fatalf("claim %d id=%d claimed=%v err=%v", i, id, claimed, err)
		}
		planned = append(planned, properScorePaperPlannedAttempt{id: id})
	}
	sentinel := errors.New("forced middle execution failure")
	var executed []int64
	err := executeProperScorePaperBatch(ctx, planned,
		func(ctx context.Context, attempt properScorePaperPlannedAttempt) error {
			executed = append(executed, attempt.id)
			if attempt.id == planned[1].id {
				return sentinel
			}
			return s.completeProperScorePaperZero(ctx, attempt.id, "fixture-terminal")
		},
		s.store.TerminalizeProperScorePaperAttemptFailure)
	if !errors.Is(err, sentinel) {
		t.Fatalf("first error was not preserved: %v", err)
	}
	if len(executed) != 3 || executed[2] != planned[2].id {
		t.Fatalf("batch stopped early: executed=%v", executed)
	}
	var outcomes, pending int
	if err = s.store.DBForTest().QueryRow(`SELECT COUNT(o.attempt_id),
SUM(CASE WHEN o.attempt_id IS NULL THEN 1 ELSE 0 END)
FROM proper_score_paper_attempts a
LEFT JOIN proper_score_paper_outcomes o ON o.attempt_id=a.id
WHERE a.attempt_key LIKE 'batch-%'`).Scan(&outcomes, &pending); err != nil {
		t.Fatal(err)
	}
	if outcomes != 3 || pending != 0 {
		t.Fatalf("claimed batch outcomes=%d pending=%d", outcomes, pending)
	}
	var middleReason string
	if err = s.store.DBForTest().QueryRow(`SELECT reason FROM proper_score_paper_outcomes
WHERE attempt_id=?`, planned[1].id).Scan(&middleReason); err != nil {
		t.Fatal(err)
	}
	if middleReason != "execution-check-failed-before-durable-outcome" {
		t.Fatalf("middle failure reason=%q", middleReason)
	}
}

func TestProperScoreBriefShowsCashPaperLanesAndLabelsLegacyResearch(t *testing.T) {
	report := map[string]any{
		"paper_portfolios": map[string]any{
			"brier": map[string]any{"entry_mark_equity_usd": 401.25, "net_profit_usd": 1.25,
				"fills": 3, "open_positions": 1, "settled_positions": 2},
			"log": map[string]any{"entry_mark_equity_usd": 399.50, "net_profit_usd": -.50,
				"fills": 2, "open_positions": 0, "settled_positions": 2},
			"spherical": map[string]any{"entry_mark_equity_usd": 400.0, "net_profit_usd": 0.0,
				"fills": 0, "open_positions": 0, "settled_positions": 0},
		},
		"transforms": map[string]any{
			"brier": map[string]any{"normalized_executable_settled_l1": 2.0,
				"normalized_executable_completed_slots": 2, "normalized_executable_unique_settled_markets": 4,
				"l1_weighted_realized_net": .20},
			"log":       map[string]any{},
			"spherical": map[string]any{},
		},
	}
	got := properScoreBriefLine(report)
	for _, want := range []string{"Proper Betting Paper · $400 each",
		"Brier Paper · net +$1.25 · NAV $401.25 · fills 3 (open 1 / settled 2)",
		"Log Paper · net -$0.50", "Spherical Paper · net +$0.00",
		"Research/group result (not cash P&L)"} {
		if !strings.Contains(got, want) {
			t.Fatalf("brief missing %q:\n%s", want, got)
		}
	}
}

func TestPaperResetAdvancesAllProperScorePaperLanes(t *testing.T) {
	s := testServer(t)
	ctx, now := context.Background(), time.Now().UTC()
	oldEpochs := map[string]int64{}
	for _, lane := range []string{"brier", "log", "spherical"} {
		in := storage.ProperScorePaperAttempt{
			AttemptKey: "server-reset-" + lane, Lane: lane, Transform: lane,
			Slot: now.Truncate(5 * time.Minute).Format(time.RFC3339), Platform: "kalshi",
			Ticker: "KXPSPP-RESET-" + lane, EventKey: "EVENT-RESET-" + lane,
			Title: "reset fixture", Side: "YES", ObservedAt: now,
			ExecuteAfter: now.Add(750 * time.Millisecond), ForecastYes: .70,
			NormalizedWeight: 1, VectorBudgetUSD: 20, CoordinateBudgetUSD: 20,
			LimitPrice: .50, RequestedQty: 10, ReservationUSD: 6,
			FeeSource: "fixture-fee", BookSource: "fixture-book",
			SourceClockID: "fixture-clock", ExecutionShadowAttemptID: "exec-reset-" + lane,
			DelayMS: 750,
		}
		id, claimed, reason, err := s.store.ClaimProperScorePaperAttempt(ctx, in)
		if err != nil || !claimed || reason != "" {
			t.Fatalf("%s claim id=%d claimed=%v reason=%q err=%v", lane, id, claimed, reason, err)
		}
		if err = s.store.CompleteProperScorePaperAttempt(ctx, storage.ProperScorePaperOutcome{
			AttemptID: id, ProcessedAt: in.ExecuteAfter, State: "filled", FilledQty: 10,
			FillPrice: .50, CostUSD: 5, FeeUSD: .10, FeeSource: "exact-fee",
			BookSource: "fresh-book", SourceClockID: "fresh-clock",
		}); err != nil {
			t.Fatal(err)
		}
	}
	before, err := s.store.ProperScorePaperPortfolioReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, lane := range []string{"brier", "log", "spherical"} {
		row := before[lane].(map[string]any)
		oldEpochs[lane] = row["epoch_id"].(int64)
		if math.Abs(row["net_profit_usd"].(float64)+.10) > 1e-9 || row["open_positions"].(int) != 1 {
			t.Fatalf("%s pre-reset report=%+v", lane, row)
		}
	}
	s.briefProperMu.Lock()
	s.briefProperLine = "stale pre-reset Proper Betting profit"
	s.briefProperAt = now
	s.briefProperNext = now.Add(time.Hour)
	oldGen := s.briefProperGen
	s.briefProperMu.Unlock()

	res := s.doPaperResetDetailed(ctx)
	if res.ProperScoreResetError != "" {
		t.Fatalf("proper-score reset failed: %s", res.ProperScoreResetError)
	}
	after, err := s.store.ProperScorePaperPortfolioReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, lane := range []string{"brier", "log", "spherical"} {
		row := after[lane].(map[string]any)
		if row["epoch_id"].(int64) <= oldEpochs[lane] || row["seed_usd"].(float64) != 400 ||
			row["cash_usd"].(float64) != 400 || row["entry_mark_equity_usd"].(float64) != 400 ||
			row["net_profit_usd"].(float64) != 0 || row["attempts"].(int) != 0 ||
			row["fills"].(int) != 0 || row["open_positions"].(int) != 0 {
			t.Fatalf("%s post-reset report=%+v", lane, row)
		}
	}
	var attempts, outcomes int
	if err = s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM proper_score_paper_attempts`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err = s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM proper_score_paper_outcomes`).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if attempts != 3 || outcomes != 3 {
		t.Fatalf("reset rewrote history attempts=%d outcomes=%d", attempts, outcomes)
	}
	s.briefProperMu.Lock()
	line, at, next, generation := s.briefProperLine, s.briefProperAt, s.briefProperNext, s.briefProperGen
	s.briefProperMu.Unlock()
	if line != "" || !at.IsZero() || !next.IsZero() || generation != oldGen+1 {
		t.Fatalf("stale Proper Betting cache survived reset line=%q at=%v next=%v gen=%d/%d",
			line, at, next, generation, oldGen)
	}
}
