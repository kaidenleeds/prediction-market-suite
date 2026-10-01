package storage

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"
)

func TestQueuePriorityPersistsTrueQueueAndTerminalOutcome(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	level, ahead, diff, age := 12.0, 10.0, 3.5, .2
	err = st.InsertQueueResearchSample(ctx, QueueResearchOrder{OrderID: "real-1", Ticker: "KXTEST",
		OutcomeSide: "YES", BookSide: "bid", Action: "buy", Price: .42, Initial: 2, Remaining: 2},
		QueueResearchSample{Observed: time.Now(), TrueQueue: 6.5, Remaining: 2, VisibleLevel: &level,
			VisibleAhead: &ahead, Error: &diff, BookAge: &age})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.MarkQueueResearchOutcome(ctx, "real-1", "executed", 2); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertQueueResearchSample(ctx, QueueResearchOrder{OrderID: "real-2", Ticker: "KXTEST",
		OutcomeSide: "YES", BookSide: "bid", Action: "buy", Price: .42, Initial: 2, Remaining: 1.5, Filled: .5},
		QueueResearchSample{Observed: time.Now(), TrueQueue: 2, Remaining: 1.5}); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkQueueResearchOutcome(ctx, "real-2", "canceled", .5); err != nil {
		t.Fatal(err)
	}
	report, err := st.ResearchSystemsReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	q := report["queue-priority"].(map[string]any)
	if q["orders"].(int) != 2 || q["samples"].(int) != 2 || q["filled"].(int) != 1 || q["partial_canceled"].(int) != 1 {
		t.Fatalf("queue report = %#v", q)
	}
	if got := q["mean_abs_estimate_error"].(float64); math.Abs(got-3.5) > 1e-9 {
		t.Fatalf("MAE=%v", got)
	}
}

func TestQueuePriorityResultSeparatesInsertFromMinuteDedup(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	observed := time.Now().UTC().Truncate(time.Minute).Add(10 * time.Second)
	order := QueueResearchOrder{OrderID: "natural-1", Ticker: "KXQUEUE", OutcomeSide: "YES",
		BookSide: "bid", Action: "buy", Price: .42, Initial: 1, Remaining: 1}
	sample := QueueResearchSample{Observed: observed, TrueQueue: 0, Remaining: 1}
	inserted, err := st.InsertQueueResearchSampleResult(ctx, order, sample)
	if err != nil || !inserted {
		t.Fatalf("first inserted=%v err=%v", inserted, err)
	}
	inserted, err = st.InsertQueueResearchSampleResult(ctx, order, sample)
	if err != nil || inserted {
		t.Fatalf("dedup inserted=%v err=%v", inserted, err)
	}
}

func TestLifecycleReopenRequiresPriorInactiveTransition(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().Add(-6 * time.Second)
	b := LifecycleBook{Bid: .40, Ask: .42, BidDepth: 10, AskDepth: 11, Source: "ws"}
	if _, cohort, err := st.InsertLifecycleResearchEvent(ctx, LifecycleResearchEvent{
		Observed: now.Add(-time.Second), Ticker: "KXREOPEN", EventType: "deactivated", Book: b}); err != nil || cohort {
		t.Fatalf("deactivated cohort=%v err=%v", cohort, err)
	}
	id, cohort, err := st.InsertLifecycleResearchEvent(ctx, LifecycleResearchEvent{
		Observed: now, Ticker: "KXREOPEN", EventType: "activated", Book: b})
	if err != nil || !cohort || id == 0 {
		t.Fatalf("reopen id=%d cohort=%v err=%v", id, cohort, err)
	}
	if _, cohort, err := st.InsertLifecycleResearchEvent(ctx, LifecycleResearchEvent{
		Observed: now, Ticker: "KXINITIAL", EventType: "activated", Book: b}); err != nil || cohort {
		t.Fatalf("initial activation cohort=%v err=%v", cohort, err)
	}
	due, err := st.LifecycleResearchDue(ctx, 0, 10)
	if err != nil || len(due) != 1 || due[0].ID != id {
		t.Fatalf("due=%+v err=%v", due, err)
	}
	if err := st.InsertLifecycleHorizon(ctx, id, 5, b, .01); err != nil {
		t.Fatal(err)
	}
	have, err := st.HasLifecycleHorizon(ctx, id, 5)
	if err != nil || !have {
		t.Fatalf("horizon have=%v err=%v", have, err)
	}
	for _, horizon := range []int{30, 300, 1800} {
		if err := st.InsertLifecycleHorizonMiss(ctx, id, horizon, "test fixed window passed"); err != nil {
			t.Fatal(err)
		}
		have, err = st.HasLifecycleHorizon(ctx, id, horizon)
		if err != nil || !have {
			t.Fatalf("missed horizon=%d have=%v err=%v", horizon, have, err)
		}
	}
	if due, err = st.LifecycleResearchDue(ctx, 0, 10); err != nil || len(due) != 0 {
		t.Fatalf("complete lifecycle cohort stayed due: %+v err=%v", due, err)
	}
}

func TestLifecycleResearchDueKeysetDoesNotStarvePastHundredEvents(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Add(-time.Minute)
	for i := 0; i < 130; i++ {
		ticker := fmt.Sprintf("KXREOPEN-%03d", i)
		if _, _, err := st.InsertLifecycleResearchEvent(ctx, LifecycleResearchEvent{
			Observed: now.Add(-time.Second), Ticker: ticker, EventType: "deactivated"}); err != nil {
			t.Fatal(err)
		}
		if _, cohort, err := st.InsertLifecycleResearchEvent(ctx, LifecycleResearchEvent{
			Observed: now, Ticker: ticker, EventType: "activated"}); err != nil || !cohort {
			t.Fatalf("ticker=%s cohort=%v err=%v", ticker, cohort, err)
		}
	}
	first, err := st.LifecycleResearchDue(ctx, 0, 100)
	if err != nil || len(first) != 100 {
		t.Fatalf("first page n=%d err=%v", len(first), err)
	}
	second, err := st.LifecycleResearchDue(ctx, first[len(first)-1].ID, 100)
	if err != nil || len(second) != 30 {
		t.Fatalf("second page n=%d err=%v", len(second), err)
	}
	if second[0].ID <= first[len(first)-1].ID {
		t.Fatal("keyset page repeated an older lifecycle event")
	}
}

func TestLifecycleOldCohortRemainsDueAndHorizonOutcomeIsExclusive(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	observed := time.Now().UTC().Add(-3 * time.Hour)
	if _, _, err := st.InsertLifecycleResearchEvent(ctx, LifecycleResearchEvent{
		Observed: observed.Add(-time.Second), Ticker: "KXOLD", EventType: "deactivated"}); err != nil {
		t.Fatal(err)
	}
	id, cohort, err := st.InsertLifecycleResearchEvent(ctx, LifecycleResearchEvent{
		Observed: observed, Ticker: "KXOLD", EventType: "activated"})
	if err != nil || !cohort || id == 0 {
		t.Fatalf("id=%d cohort=%v err=%v", id, cohort, err)
	}
	due, err := st.LifecycleResearchDue(ctx, 0, 10)
	if err != nil || len(due) != 1 || due[0].ID != id {
		t.Fatalf("old incomplete cohort disappeared: due=%+v err=%v", due, err)
	}
	book := LifecycleBook{Bid: .4, Ask: .42, BidDepth: 2, AskDepth: 3, Source: "ws"}
	inserted, err := st.InsertLifecycleHorizonResult(ctx, id, 5, book, .01)
	if err != nil || !inserted {
		t.Fatalf("sample inserted=%v err=%v", inserted, err)
	}
	inserted, err = st.InsertLifecycleHorizonMissResult(ctx, id, 5, "too late")
	if err != nil || inserted {
		t.Fatalf("sample+miss double terminal inserted=%v err=%v", inserted, err)
	}
	inserted, err = st.InsertLifecycleHorizonMissResult(ctx, id, 30, "too late")
	if err != nil || !inserted {
		t.Fatalf("miss inserted=%v err=%v", inserted, err)
	}
	inserted, err = st.InsertLifecycleHorizonResult(ctx, id, 30, book, .01)
	if err != nil || inserted {
		t.Fatalf("miss+sample double terminal inserted=%v err=%v", inserted, err)
	}
	var samples, misses int
	if err := st.db.QueryRowContext(ctx, `SELECT
 (SELECT COUNT(*) FROM research_lifecycle_horizons WHERE event_id=?),
 (SELECT COUNT(*) FROM research_lifecycle_horizon_misses WHERE event_id=?)`, id, id).Scan(&samples, &misses); err != nil {
		t.Fatal(err)
	}
	if samples != 1 || misses != 1 {
		t.Fatalf("samples=%d misses=%d", samples, misses)
	}
}

func TestLifecycleMarkersRestoreLatestAcceptedState(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	for _, ev := range []LifecycleResearchEvent{
		{Observed: now.Add(-3 * time.Second), Ticker: "KXRESTORE", EventType: "close_date_updated", CloseTime: "2026-07-12T00:00:00Z"},
		{Observed: now.Add(-2 * time.Second), Ticker: "KXRESTORE", EventType: "activated"},
		{Observed: now.Add(-time.Second), Ticker: "KXRESTORE", EventType: "deactivated"},
	} {
		if _, _, err := st.InsertLifecycleResearchEvent(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	markers, err := st.LifecycleResearchMarkers(ctx, 100)
	if err != nil || len(markers) != 1 {
		t.Fatalf("markers=%+v err=%v", markers, err)
	}
	if markers[0].LastEvent != "deactivated" || markers[0].LastClose != "2026-07-12T00:00:00Z" {
		t.Fatalf("marker=%+v", markers[0])
	}
}

func TestEventBasketCandidateRequiresExhaustiveness(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	legs := []BasketLeg{{Ticker: "A", Side: "YES", Ask: .40, Depth: 5, Fee: .01},
		{Ticker: "B", Side: "YES", Ask: .40, Depth: 3, Fee: .01}}
	base := EventBasketObservation{Observed: time.Now(), Venue: "kalshi", EventID: "E1", Route: "buy-yes-all",
		IdentitySource: "official nested event", IdentityComplete: true, MutuallyExclusive: true, Legs: legs, PayoutLowerBound: 1}
	if err := st.InsertEventBasket(ctx, base); err != nil {
		t.Fatal(err)
	}
	var candidate int
	if err := st.db.QueryRow(`SELECT candidate FROM research_event_baskets WHERE event_id='E1'`).Scan(&candidate); err != nil {
		t.Fatal(err)
	}
	if candidate != 0 {
		t.Fatal("non-exhaustive positive subset was falsely labeled a lock")
	}
	base.EventID, base.Route, base.PayoutLowerBound = "E-NO", "buy-no-all", 1
	for i := range base.Legs {
		base.Legs[i].Side = "NO"
	}
	if err := st.InsertEventBasket(ctx, base); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT candidate FROM research_event_baskets WHERE event_id='E-NO'`).Scan(&candidate); err != nil {
		t.Fatal(err)
	}
	if candidate != 1 {
		t.Fatal("mutually-exclusive complete NO-all lock incorrectly required exhaustiveness")
	}
	base.Route = "buy-yes-all"
	for i := range base.Legs {
		base.Legs[i].Side = "YES"
	}
	base.EventID, base.Venue, base.Exhaustive = "E2", "polyus", true
	if err := st.InsertEventBasket(ctx, base); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT candidate FROM research_event_baskets WHERE event_id='E2'`).Scan(&candidate); err != nil {
		t.Fatal(err)
	}
	if candidate != 1 {
		t.Fatal("complete exhaustive fee-net basket not labeled candidate")
	}
	var partial float64
	if err := st.db.QueryRow(`SELECT partial_fill_worst_loss FROM research_event_baskets WHERE event_id='E2'`).Scan(&partial); err != nil {
		t.Fatal(err)
	}
	if math.Abs(partial-(-.82)) > 1e-9 {
		t.Fatalf("partial exposure=%v", partial)
	}
}

func TestResearchBasketAndNestedInsertResultsDistinguishDuplicates(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	observed := time.Date(2026, 7, 12, 12, 2, 0, 0, time.UTC)
	basket := EventBasketObservation{Observed: observed, Venue: "polyus", EventID: "E-DEDUP",
		Route: "buy-yes-all", IdentitySource: "authenticated complete winner set", IdentityComplete: true,
		MutuallyExclusive: true, Exhaustive: true, PayoutLowerBound: 1,
		Legs: []BasketLeg{{Ticker: "A", Side: "YES", Ask: .40, Depth: 5, Fee: .01},
			{Ticker: "B", Side: "YES", Ask: .40, Depth: 3, Fee: .01}}}
	for attempt, wantInserted := range []bool{true, false} {
		inserted, err := st.InsertEventBasketResult(ctx, basket)
		if err != nil || inserted != wantInserted {
			t.Fatalf("basket attempt %d inserted=%v want=%v err=%v", attempt+1, inserted, wantInserted, err)
		}
	}

	ladder := NestedLadderObservation{Observed: observed, Venue: "kalshi", EventID: "E-LADDER-DEDUP",
		Title: "thresholds", LowTicker: "LOW", HighTicker: "HIGH", IdentitySource: "same official event + greater",
		LowStrike: 10, HighStrike: 20, LowBid: .39, LowAsk: .40, HighBid: .45, HighAsk: .46,
		LowAskDepth: 9, HighNoAskDepth: 7, LowEntryFee: .01, HighEntryFee: .01, LowExitFee: .005, HighExitFee: .005}
	for attempt, wantInserted := range []bool{true, false} {
		inserted, err := st.InsertNestedLadderResult(ctx, ladder)
		if err != nil || inserted != wantInserted {
			t.Fatalf("ladder attempt %d inserted=%v want=%v err=%v", attempt+1, inserted, wantInserted, err)
		}
	}
	for table, want := range map[string]int{"research_event_baskets": 1, "research_nested_ladders": 1} {
		var count int
		if err := st.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != want {
			t.Fatalf("%s rows=%d want=%d err=%v", table, count, want, err)
		}
	}
}

func TestIncentiveMakerNeverAddsRewardToEV(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	err = st.InsertIncentiveMaker(ctx, IncentiveMakerObservation{Observed: time.Now(), ProgramID: "P1",
		MarketTicker: "KXINC", Side: "YES", PeriodReward: 999999, TargetSize: 10, Bid: .40, Ask: .42,
		BidDepth: 20, AskDepth: 30, MakerFee: .001, Capacity: 20, BookSource: "ws"})
	if err != nil {
		t.Fatal(err)
	}
	var included, competition int
	if err := st.db.QueryRow(`SELECT reward_included_in_ev,competition_known FROM research_incentive_maker`).Scan(&included, &competition); err != nil {
		t.Fatal(err)
	}
	if included != 0 || competition != 0 {
		t.Fatalf("included=%d competition=%d", included, competition)
	}
}

func TestNestedLadderSeparatesCompleteFillLockFromExecutionRisk(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	err = st.InsertNestedLadder(ctx, NestedLadderObservation{Observed: time.Now(), Venue: "kalshi", EventID: "E-LADDER",
		Title: "thresholds", LowTicker: "LOW", HighTicker: "HIGH", IdentitySource: "same official event + greater",
		LowStrike: 10, HighStrike: 20, LowBid: .39, LowAsk: .40, HighBid: .45, HighAsk: .46,
		LowAskDepth: 9, HighNoAskDepth: 7, LowEntryFee: .01, HighEntryFee: .01, LowExitFee: .005, HighExitFee: .005})
	if err != nil {
		t.Fatal(err)
	}
	var candidate, violation, atomic, funded int
	var net, partial, unwind float64
	err = st.db.QueryRow(`SELECT candidate,quote_violation,atomic_fill,funded,net_lock_if_both_fill,
partial_fill_worst_loss,unwind_buffer FROM research_nested_ladders`).Scan(&candidate, &violation, &atomic, &funded, &net, &partial, &unwind)
	if err != nil {
		t.Fatal(err)
	}
	if candidate != 1 || violation != 1 || atomic != 0 || funded != 0 || math.Abs(net-.03) > 1e-9 ||
		math.Abs(partial-(-.56)) > 1e-9 || math.Abs(unwind-.025) > 1e-9 {
		t.Fatalf("candidate=%d violation=%d atomic=%d funded=%d net=%v partial=%v unwind=%v",
			candidate, violation, atomic, funded, net, partial, unwind)
	}
}
