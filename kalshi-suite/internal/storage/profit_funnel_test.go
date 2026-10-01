package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func profitFunnelTestAttempt(id, system, side string, at time.Time) ExecutionShadowAttempt {
	return ExecutionShadowAttempt{
		AttemptID: id, SignalDecisionID: "decision-" + id,
		ObservedAt: at, TriggerUnixMS: at.UnixMilli(),
		Venue: "kalshi", Ticker: "KX-" + id, Side: side, Action: "BUY",
		SystemID: system, Route: "taker", SignalSource: "test-" + system,
		InputTopology: "coinbase->kalshi", SignalPrice: .40,
		QualificationBasis: "profit-funnel-test",
	}
}

func profitFunnelTestEvent(id string, attempt ExecutionShadowAttempt, offset time.Duration,
	stage, outcome string) ExecutionShadowEvent {
	at := attempt.ObservedAt.Add(offset)
	return ExecutionShadowEvent{EventID: id, AttemptID: attempt.AttemptID, At: at,
		ElapsedFromTriggerMS: at.UnixMilli() - attempt.TriggerUnixMS,
		Stage:                stage, Outcome: outcome}
}

func profitFunnelBookFeeEvent(id string, attempt ExecutionShadowAttempt,
	offset time.Duration, feeKnown bool) ExecutionShadowEvent {
	event := profitFunnelTestEvent(id, attempt, offset, "detector-branch", "qualified")
	event.BookSource = "kalshi_ws_full_orderbook:g1:s1:q1"
	event.SideBid, event.SideAsk = executionShadowFloat(.39), executionShadowFloat(.40)
	event.VisibleDepth, event.OriginalLimit = executionShadowFloat(10), executionShadowFloat(.40)
	if feeKnown {
		event.FeeQuote, event.FeeQuoteSource = executionShadowFloat(.01), "exact-route-fee"
	}
	return event
}

func TestExecutionShadowProfitFunnelClassifiesExclusiveBranchesAndKnownEconomics(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	base := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	attempts := []ExecutionShadowAttempt{
		profitFunnelTestAttempt("a-live-fill", "spotlag", "YES", base),
		profitFunnelTestAttempt("b-live-zero", "spotlag", "NO", base.Add(time.Second)),
		profitFunnelTestAttempt("c-not-sent", "spotlag", "YES", base.Add(2*time.Second)),
		profitFunnelTestAttempt("d-no-fee", "other", "NO", base.Add(3*time.Second)),
		profitFunnelTestAttempt("e-pending", "other", "YES", base.Add(4*time.Second)),
		profitFunnelTestAttempt("f-no-events", "other", "NO", base.Add(5*time.Second)),
	}
	for _, attempt := range attempts {
		if inserted, insertErr := st.InsertExecutionShadowAttempt(ctx, attempt); insertErr != nil || !inserted {
			t.Fatalf("insert %s inserted=%v err=%v", attempt.AttemptID, inserted, insertErr)
		}
	}
	appendEvent := func(event ExecutionShadowEvent) {
		t.Helper()
		if inserted, appendErr := st.AppendExecutionShadowEvent(ctx, event); appendErr != nil || !inserted {
			t.Fatalf("append %s inserted=%v err=%v", event.EventID, inserted, appendErr)
		}
	}

	// A: every lane filled and settled with exact fee-net economics.
	a := attempts[0]
	appendEvent(profitFunnelBookFeeEvent("a-detector", a, time.Millisecond, true))
	liveA := profitFunnelTestEvent("a-live", a, 2*time.Millisecond, "live-terminal", "full")
	liveA.VenueAttempted, liveA.LiveAuthoritative, liveA.LiveState = true, true, "filled"
	liveA.LiveFilledQty, liveA.LiveFillPrice = executionShadowFloat(1), executionShadowFloat(.40)
	liveA.LiveFee, liveA.LiveFeeSource = executionShadowFloat(.01), "exact-live-fee"
	appendEvent(liveA)
	paperA := profitFunnelTestEvent("a-paper", a, 3*time.Millisecond,
		"funded-paper-terminal", "paper-filled")
	paperA.PaperState, paperA.PaperAttemptID = "PAPER-FILLED", "paper-a"
	paperA.PaperFilledQty, paperA.PaperFillPrice = executionShadowFloat(1), executionShadowFloat(.40)
	paperA.PaperFee, paperA.PaperFeeSource = executionShadowFloat(.01), "exact-paper-fee"
	appendEvent(paperA)
	shadowA := profitFunnelTestEvent("a-shadow", a, 4*time.Millisecond,
		"counterfactual-execution-terminal", "modeled_fill")
	shadowA.ShadowState = "modeled_fill"
	shadowA.ShadowFilledQty, shadowA.ShadowFillPrice = executionShadowFloat(1), executionShadowFloat(.40)
	shadowA.ShadowFee, shadowA.ShadowFeeSource = executionShadowFloat(.01), "exact-shadow-fee"
	appendEvent(shadowA)
	settleA := profitFunnelTestEvent("a-settle", a, time.Hour, "settlement", "settled")
	settleA.SettlementKnown, settleA.SettlementValue = true, executionShadowFloat(1)
	settleA.SettledAt, settleA.SettlementSource, settleA.SettlementHash = settleA.At, "venue", "hash-a"
	settleA.LiveNet, settleA.PaperNet, settleA.ShadowNet =
		executionShadowFloat(.59), executionShadowFloat(.59), executionShadowFloat(.59)
	appendEvent(settleA)

	// B: LIVE had an authoritative zero-fill, Paper zero-filled, but the delayed execution model
	// filled and later received its own fee-net settlement. It is a skipped/economics-known row.
	b := attempts[1]
	appendEvent(profitFunnelBookFeeEvent("b-detector", b, time.Millisecond, true))
	liveB := profitFunnelTestEvent("b-live", b, 2*time.Millisecond, "live-exact-reconcile", "zero-fill")
	liveB.VenueAttempted, liveB.LiveAuthoritative, liveB.LiveState = true, true, "zero-fill"
	liveB.LiveFilledQty = executionShadowFloat(0)
	appendEvent(liveB)
	paperB := profitFunnelTestEvent("b-paper", b, 3*time.Millisecond,
		"funded-paper-terminal", "paper-zero-fill")
	paperB.PaperState = "PAPER-ZERO-FILL"
	appendEvent(paperB)
	shadowB := profitFunnelTestEvent("b-shadow", b, 4*time.Millisecond,
		"counterfactual-execution-terminal", "modeled_fill")
	shadowB.ShadowState = "modeled_fill"
	shadowB.ShadowFilledQty, shadowB.ShadowFillPrice = executionShadowFloat(1), executionShadowFloat(.40)
	shadowB.ShadowFee, shadowB.ShadowFeeSource = executionShadowFloat(.02), "exact-shadow-fee"
	appendEvent(shadowB)
	settleB := profitFunnelTestEvent("b-settle", b, time.Hour, "settlement", "settled")
	settleB.SettlementKnown, settleB.SettlementValue = true, executionShadowFloat(0)
	settleB.SettledAt, settleB.SettlementSource, settleB.SettlementHash = settleB.At, "venue", "hash-b"
	settleB.ShadowNet = executionShadowFloat(.58)
	appendEvent(settleB)

	// C: known local no-send, Paper rejection, modeled zero-fill, fractional settlement.
	c := attempts[2]
	appendEvent(profitFunnelBookFeeEvent("c-detector", c, time.Millisecond, true))
	liveC := profitFunnelTestEvent("c-live", c, 2*time.Millisecond, "final-dispatch", "rejected")
	liveC.LiveState = "not-sent"
	appendEvent(liveC)
	paperC := profitFunnelTestEvent("c-paper", c, 3*time.Millisecond,
		"funded-paper-terminal", "paper-rejected")
	// The corrected generic Paper worker's durable pre-attempt exclusion vocabulary is REJECTED.
	paperC.PaperState = "REJECTED"
	appendEvent(paperC)
	shadowC := profitFunnelTestEvent("c-shadow", c, 4*time.Millisecond,
		"counterfactual-execution-terminal", "modeled_zero_fill")
	shadowC.ShadowState = "modeled_zero_fill"
	appendEvent(shadowC)
	settleC := profitFunnelTestEvent("c-settle", c, time.Hour, "settlement", "settled")
	settleC.SettlementKnown, settleC.SettlementValue = true, executionShadowFloat(.50)
	settleC.SettledAt, settleC.SettlementSource, settleC.SettlementHash = settleC.At, "venue", "hash-c"
	appendEvent(settleC)

	// D: valid book but no exact fee, a local no-send, no Paper observation, explicit shadow miss.
	d := attempts[3]
	appendEvent(profitFunnelBookFeeEvent("d-detector", d, time.Millisecond, false))
	liveD := profitFunnelTestEvent("d-live", d, 2*time.Millisecond, "final-dispatch", "rejected")
	liveD.LiveState = "not-sent"
	appendEvent(liveD)
	shadowD := profitFunnelTestEvent("d-shadow", d, 3*time.Millisecond,
		"counterfactual-execution-terminal", "not_observed")
	shadowD.ShadowState = "not_observed"
	appendEvent(shadowD)

	// E: venue receipt stayed ambiguous; Paper was offered and Shadow became eligible but neither
	// reached a terminal. F has no event at all, so its Paper/Shadow branches are explicitly missing
	// and its LIVE disposition remains unknown rather than being synthesized as skipped.
	e := attempts[4]
	appendEvent(profitFunnelBookFeeEvent("e-detector", e, time.Millisecond, true))
	preflightE := profitFunnelBookFeeEvent("e-preflight", e, 2*time.Millisecond, true)
	preflightE.Stage, preflightE.Outcome = "live-first-preflight", "passed"
	appendEvent(preflightE)
	liveE := profitFunnelTestEvent("e-live", e, 3*time.Millisecond, "live-terminal", "ambiguous")
	liveE.VenueAttempted, liveE.LiveState = true, "ambiguous"
	appendEvent(liveE)
	appendEvent(profitFunnelTestEvent("e-paper-offered", e, 4*time.Millisecond,
		"funded-paper-branch", "offered"))

	report, err := st.ExecutionShadowProfitFunnel(ctx, ProfitFunnelQuery{})
	if err != nil {
		t.Fatal(err)
	}
	got := report.Totals
	if got.Detected != 6 || got.BookValid != 5 || got.FeeValid != 4 || got.VenueAttempted != 3 ||
		got.Settled != 3 {
		t.Fatalf("top funnel=%+v", got)
	}
	if got.Live != (ProfitFunnelLiveTerminals{Filled: 1, ZeroFill: 1, NotSent: 2, Unknown: 1}) {
		t.Fatalf("LIVE terminals=%+v", got.Live)
	}
	if got.Paper != (ProfitFunnelPaperTerminals{Filled: 1, ZeroFill: 1, Rejected: 1}) {
		t.Fatalf("Paper terminals=%+v", got.Paper)
	}
	if got.Shadow != (ProfitFunnelShadowTerminals{ModeledFill: 2, ZeroFill: 1,
		NotObserved: 1}) {
		t.Fatalf("Shadow terminals=%+v", got.Shadow)
	}
	if got.FeeNet.Live.KnownSettled != 1 || got.FeeNet.Live.USD == nil || *got.FeeNet.Live.USD != .59 ||
		got.FeeNet.Paper.KnownSettled != 1 || got.FeeNet.Paper.USD == nil || *got.FeeNet.Paper.USD != .59 ||
		got.FeeNet.Shadow.KnownSettled != 2 || got.FeeNet.Shadow.USD == nil || *got.FeeNet.Shadow.USD != 1.17 {
		t.Fatalf("fee-net=%+v", got.FeeNet)
	}
	if got.TakenVsSkipped.Taken.Total != 1 || got.TakenVsSkipped.Taken.OutcomeKnown != 1 ||
		got.TakenVsSkipped.Taken.ComparableEconomicsKnown != 1 ||
		got.TakenVsSkipped.Skipped.Total != 3 || got.TakenVsSkipped.Skipped.OutcomeKnown != 2 ||
		got.TakenVsSkipped.Skipped.OutcomeUnknown != 1 ||
		got.TakenVsSkipped.Skipped.ComparableEconomicsKnown != 1 ||
		got.TakenVsSkipped.Skipped.ComparableFeeNetUSD == nil ||
		*got.TakenVsSkipped.Skipped.ComparableFeeNetUSD != .58 ||
		got.TakenVsSkipped.Unknown.Total != 2 ||
		got.TakenVsSkipped.Unknown.OutcomeUnknown != 2 ||
		got.TakenVsSkipped.Unknown.ComparableEconomicsKnown != 0 ||
		got.TakenVsSkipped.Unknown.ComparableEconomicsUnknown != 2 {
		t.Fatalf("taken/skipped=%+v", got.TakenVsSkipped)
	}
	if got.Gaps.AttemptsWithoutEvents != 1 || got.Gaps.DetectorReceiptMissing != 1 ||
		got.Gaps.BookEvidenceMissing != 1 || got.Gaps.FeeEvidenceMissing != 2 ||
		got.Gaps.LiveTerminalMissing != 1 || got.Gaps.LiveTerminalIncomplete != 1 ||
		got.Gaps.PaperBranchMissing != 2 || got.Gaps.PaperTerminalMissing != 1 ||
		got.Gaps.ShadowBranchMissing != 1 || got.Gaps.ShadowTerminalMissing != 1 ||
		got.Gaps.Unsettled != 3 {
		t.Fatalf("gaps=%+v", got.Gaps)
	}
	if got.Exclusivity.ExpectedAttempts != 6 || got.Exclusivity.LiveClassified != 5 ||
		got.Exclusivity.PaperClassified != 3 || got.Exclusivity.ShadowClassified != 4 ||
		got.Exclusivity.LiveGap != 1 || got.Exclusivity.PaperGap != 3 || got.Exclusivity.ShadowGap != 2 {
		t.Fatalf("exclusivity=%+v", got.Exclusivity)
	}
	if len(report.Groups) != 4 { // the same system/venue/side/route/topology rolls up together.
		t.Fatalf("default groups=%d want 4: %+v", len(report.Groups), report.Groups)
	}

	from, to := base.Add(time.Second), base.Add(3*time.Second)
	filtered, err := st.ExecutionShadowProfitFunnel(ctx, ProfitFunnelQuery{
		FromInclusive: &from, ToExclusive: &to, Systems: []string{"SPOTLAG"},
		Sides: []string{"NO", "YES"}, GroupBy: []string{"side"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if filtered.Totals.Detected != 2 || len(filtered.Groups) != 2 ||
		filtered.TimeRange.FromInclusive == nil || filtered.TimeRange.ToExclusive == nil {
		t.Fatalf("filtered report=%+v", filtered)
	}
	if _, err = st.ExecutionShadowProfitFunnel(ctx, ProfitFunnelQuery{
		FromInclusive: &to, ToExclusive: &from,
	}); err == nil {
		t.Fatal("invalid reversed time range was accepted")
	}
}

func TestExecutionShadowProfitFunnelHasNoTwoThousandAttemptCap(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	base := time.Date(2026, 7, 22, 13, 0, 0, 0, time.UTC)
	const attempts = 2005
	for first := 0; first < attempts; first += ExecutionShadowBatchMax {
		last := min(first+ExecutionShadowBatchMax, attempts)
		writes := make([]ExecutionShadowWrite, 0, last-first)
		for i := first; i < last; i++ {
			attempt := profitFunnelTestAttempt(fmt.Sprintf("bulk-%04d", i), "bulk", "YES",
				base.Add(time.Duration(i)*time.Millisecond))
			writes = append(writes, ExecutionShadowWrite{Attempt: &attempt})
		}
		if orphan, persistErr := st.PersistExecutionShadowBatch(ctx, writes); persistErr != nil || len(orphan) != 0 {
			t.Fatalf("batch %d..%d orphan=%v err=%v", first, last, orphan, persistErr)
		}
	}
	report, err := st.ExecutionShadowProfitFunnel(ctx, ProfitFunnelQuery{GroupBy: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	if report.Totals.Detected != attempts || report.Totals.Live.Unknown != 0 ||
		report.Totals.Paper.NotObserved != 0 || report.Totals.Shadow.NotObserved != 0 ||
		report.Totals.Gaps.LiveTerminalMissing != attempts ||
		report.Totals.Gaps.PaperBranchMissing != attempts ||
		report.Totals.Gaps.ShadowBranchMissing != attempts ||
		report.Totals.TakenVsSkipped.Unknown.Total != attempts ||
		report.Totals.Exclusivity.LiveClassified != 0 ||
		report.Totals.Exclusivity.PaperClassified != 0 ||
		report.Totals.Exclusivity.ShadowClassified != 0 ||
		len(report.Groups) != 0 || report.DetailLimited || !report.FullHistory {
		t.Fatalf("full-history cap/regression: %+v", report)
	}
}

func TestExecutionShadowProfitFunnelNeverPoolsBuyAndSell(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	at := time.Date(2026, 7, 22, 15, 0, 0, 0, time.UTC)
	buy := profitFunnelTestAttempt("action-buy", "proper-score-momentum", "YES", at)
	sell := profitFunnelTestAttempt("action-sell", "proper-score-momentum", "YES", at.Add(time.Millisecond))
	sell.Action = "SELL"
	for _, attempt := range []ExecutionShadowAttempt{buy, sell} {
		if inserted, insertErr := st.InsertExecutionShadowAttempt(ctx, attempt); insertErr != nil || !inserted {
			t.Fatalf("insert %s inserted=%v err=%v", attempt.AttemptID, inserted, insertErr)
		}
	}
	report, err := st.ExecutionShadowProfitFunnel(ctx, ProfitFunnelQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Totals.Detected != 2 || len(report.Groups) != 2 {
		t.Fatalf("default action grouping pooled BUY/SELL: %+v", report)
	}
	filtered, err := st.ExecutionShadowProfitFunnel(ctx, ProfitFunnelQuery{
		Actions: []string{"sell"}, GroupBy: []string{"action"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if filtered.Totals.Detected != 1 || len(filtered.Groups) != 1 ||
		filtered.Groups[0].Dimensions["action"] != "SELL" ||
		len(filtered.Filters.Actions) != 1 || filtered.Filters.Actions[0] != "SELL" {
		t.Fatalf("SELL action filter/group=%+v", filtered)
	}
	if _, err = st.ExecutionShadowProfitFunnel(ctx,
		ProfitFunnelQuery{Actions: []string{"HEDGE"}}); err == nil {
		t.Fatal("unsupported action filter was accepted")
	}
}
