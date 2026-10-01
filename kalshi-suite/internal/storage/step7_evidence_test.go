package storage

import (
	"context"
	"testing"
	"time"
)

func TestStep7MicrostructureStoresOnlyBoundedSummariesAndSeparateFlowTruth(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC()
	episode := Step7ReplenishmentEpisode{EpisodeID: "episode-1", Ticker: "KXSTEP7", Side: "YES_ASK", Observed: now,
		Generation: 1, SubscriptionID: 7, Sequence: 11, PriorSequence: 10, StartPrice: .42, StartDepth: 10,
		DepletedPrice: .42, DepletedDepth: 4, DepletionUnits: 6, TradeUnits: 2, UnexplainedUnits: 4,
		QueueChurnUnits: 6, SourceClock: "venue_source_time", Evidence: map[string]any{"raw_tick": false}}
	mark := Step7ReplenishmentMark{EpisodeID: "episode-1", Observed: now.Add(5 * time.Second), EventType: "horizon",
		HorizonMS: 5000, BookPrice: .43, BookDepth: 8, RefillFraction: .8, TradeUnits: 2, UnexplainedUnits: 4,
		QueueChurnUnits: 10, Evidence: map[string]any{"fixed": true}}
	flow := Step7FlowDirectionPair{PairID: "pair-1", Ticker: "KXSTEP7", TradeID: "trade-1", Observed: now,
		Count: .001, YesPrice: .42, TakerOutcomeSide: "yes", TakerBookSide: "bid", AuthoritativeSide: "yes",
		InferredSide: "yes", ComparisonStatus: "agreement", SourceClockStatus: "arrival_clock_pair",
		BookObserved: now.Add(-time.Millisecond), BookAgeMS: 1, YesBid: .41, YesAsk: .42, BookGeneration: 1,
		BookSequence: 10, TradeSequence: 20, Evidence: map[string]any{"no_minimum_size": true}}
	episodeJob, err := NewStep7EpisodeProjectionJob(episode, 1, "raw-only fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	markJob, err := NewStep7MarkProjectionJob(mark, 1, "raw-only fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	flowJob, err := NewStep7FlowProjectionJob(flow, 1, "raw-only fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	jobs := []Step7ProjectionJob{episodeJob, markJob, flowJob}
	epN, markN, flowN, jobN, err := st.InsertStep7MicrostructureBatch(context.Background(),
		[]Step7ReplenishmentEpisode{episode}, []Step7ReplenishmentMark{mark},
		[]Step7FlowDirectionPair{flow}, jobs)
	if err != nil || epN != 1 || markN != 1 || flowN != 1 || jobN != 3 {
		t.Fatalf("counts=%d/%d/%d/%d err=%v", epN, markN, flowN, jobN, err)
	}
	if epN, markN, flowN, jobN, err = st.InsertStep7MicrostructureBatch(context.Background(),
		[]Step7ReplenishmentEpisode{episode}, []Step7ReplenishmentMark{mark},
		[]Step7FlowDirectionPair{flow}, jobs); err != nil || epN != 0 || markN != 0 ||
		flowN != 0 || jobN != 0 {
		t.Fatalf("duplicate counts=%d/%d/%d/%d err=%v", epN, markN, flowN, jobN, err)
	}
	var authority, cancel string
	if err = st.db.QueryRow(`SELECT authoritative_side FROM research_flow_direction_pairs WHERE pair_id='pair-1'`).Scan(&authority); err != nil || authority != "yes" {
		t.Fatalf("authority=%q err=%v", authority, err)
	}
	if err = st.db.QueryRow(`SELECT cancel_authority FROM research_replenishment_episodes WHERE episode_id='episode-1'`).Scan(&cancel); err != nil || cancel != "book_minus_authoritative_trade_proxy" {
		t.Fatalf("cancel=%q err=%v", cancel, err)
	}
}

func TestR146FlowPairPruneArchivesDurableDailyDirectionTruth(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	old := time.Now().UTC().AddDate(0, 0, -100).Truncate(time.Hour)
	flows := []Step7FlowDirectionPair{
		{PairID: "old-pair-1", Ticker: "KX-OLD", TradeID: "trade-1", Observed: old,
			Count: 1, YesPrice: .40, AuthoritativeSide: "yes", InferredSide: "no", ComparisonStatus: "mismatch",
			SourceClockStatus: "same_generation_sequence_known", BookAgeMS: 1, YesBid: .39, YesAsk: .40},
		{PairID: "old-pair-2", Ticker: "KX-OLD", TradeID: "trade-2", Observed: old.Add(time.Minute),
			Count: 2, YesPrice: .60, AuthoritativeSide: "yes", InferredSide: "no", ComparisonStatus: "mismatch",
			SourceClockStatus: "unknown", BookAgeMS: 1, YesBid: .59, YesAsk: .60},
		{PairID: "old-pair-other", Ticker: "KX-OTHER", TradeID: "trade-3", Observed: old.Add(2 * time.Minute),
			Count: 4, YesPrice: .50, AuthoritativeSide: "yes", InferredSide: "no", ComparisonStatus: "mismatch",
			SourceClockStatus: "same_generation_sequence_known", BookAgeMS: 1, YesBid: .49, YesAsk: .50},
	}
	jobs := make([]Step7ProjectionJob, 0, len(flows))
	for _, flow := range flows {
		job, err := NewStep7FlowProjectionJob(flow, 1, "raw-only prune fixture", nil)
		if err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, job)
	}
	if _, _, n, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil, flows, jobs); err != nil || n != 3 {
		t.Fatalf("flow insert n=%d err=%v", n, err)
	}
	if result, err := st.ApplyStep7ProjectionOutbox(ctx, 10); err != nil ||
		result.Completed != 0 || result.Pending != 0 {
		t.Fatalf("projection completion=%+v err=%v", result, err)
	}
	if n, err := st.PruneStep7Evidence(ctx, time.Now().UTC().AddDate(0, 0, -90)); err != nil || n != 3 {
		t.Fatalf("first prune n=%d err=%v", n, err)
	}
	var raw, pairs, complete int
	var units, priceUnits float64
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_flow_direction_pairs`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT pair_count,contract_units,yes_price_units,complete_clock_count
FROM research_flow_direction_daily WHERE ticker='KX-OLD' AND comparison_status='mismatch'
 AND authoritative_side='yes' AND inferred_side='no'`).
		Scan(&pairs, &units, &priceUnits, &complete); err != nil {
		t.Fatal(err)
	}
	if raw != 0 || pairs != 2 || units != 3 || priceUnits < 1.599999 || priceUnits > 1.600001 || complete != 1 {
		t.Fatalf("raw=%d aggregate pairs=%d units=%v price_units=%v complete=%d",
			raw, pairs, units, priceUnits, complete)
	}
	if n, err := st.PruneStep7Evidence(ctx, time.Now().UTC().AddDate(0, 0, -90)); err != nil || n != 0 {
		t.Fatalf("idempotent prune n=%d err=%v", n, err)
	}
	var after, cells int
	if err := st.db.QueryRow(`SELECT SUM(pair_count),COUNT(*) FROM research_flow_direction_daily`).Scan(&after, &cells); err != nil || after != 3 || cells != 2 {
		t.Fatalf("aggregate sum=%d ticker cells=%d err=%v", after, cells, err)
	}
}

func TestStep7MakerLifecycleCancelIsExactZeroAndZeroMarkoutIsObserved(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	id, err := st.InsertMakerAttempt(ctx, "kalshi", "KX-CANCEL", "YES", "system", .40, 2, 20, 0, nil, 1, "", "bookws")
	if err != nil {
		t.Fatal(err)
	}
	if err = st.MarkMakerExpiredRule(ctx, id, "moved-one-tick"); err != nil {
		t.Fatal(err)
	}
	rows, err := st.NaturalMakerSalvageSnapshots(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	if _, err = st.MirrorNaturalMakerSalvage(ctx, rows[0]); err != nil {
		t.Fatal(err)
	}
	var realized float64
	var known int
	if err = st.db.QueryRow(`SELECT realized_net,realized_known FROM research_maker_salvage_events WHERE attempt_id=? AND event_type='cancel'`, id).Scan(&realized, &known); err != nil || realized != 0 || known != 1 {
		t.Fatalf("cancel=%v/%d err=%v", realized, known, err)
	}
	id2, err := st.InsertMakerAttempt(ctx, "kalshi", "KX-FILL", "NO", "system", .30, 2, 20, 0, nil, 1, "", "bookws")
	if err != nil {
		t.Fatal(err)
	}
	if err = st.MarkMakerFilledWithFee(ctx, id2, .01, "kalshi:test"); err != nil {
		t.Fatal(err)
	}
	if err = st.SetMakerAdverse(ctx, id2, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = st.db.ExecContext(ctx, `UPDATE maker_fill_stats SET settle_val=0 WHERE id=?`, id2); err != nil {
		t.Fatal(err)
	}
	rows, err = st.NaturalMakerSalvageSnapshots(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.AttemptID == id2 {
			if row.Markout == nil || *row.Markout != 0 {
				t.Fatalf("zero markout lost: %+v", row)
			}
			if _, err = st.MirrorNaturalMakerSalvage(ctx, row); err != nil {
				t.Fatal(err)
			}
		}
	}
	var events int
	if err = st.db.QueryRow(`SELECT COUNT(*) FROM research_maker_salvage_events WHERE attempt_id=? AND event_type IN ('fill','markout','settlement')`, id2).Scan(&events); err != nil || events != 3 {
		t.Fatalf("events=%d err=%v", events, err)
	}
}

func TestStep7IncentiveCannotUseAdvertisedOrUnattributedReward(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	row := Step7IncentiveEvaluation{EvaluationID: "eval-control", Observed: now, Venue: "polyus", ProgramID: "p",
		Ticker: "slug", CompetitionKnown: true, CompetitionUnits: 100, ActualCredit: 12, RewardLower: 0,
		ConservativeLower: -.2, Candidate: false, Blocker: "exact_order_attributed_credited_reward_unavailable; reward_floor_zero",
		Evidence: map[string]any{"advertised_pool": 1000}}
	if inserted, err := st.InsertStep7IncentiveEvaluation(ctx, row); err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	row.EvaluationID = "invalid-candidate"
	row.Candidate = true
	row.RewardCreditKnown = false
	row.RewardLower = 1
	row.ConservativeLower = .8
	row.Blocker = ""
	if inserted, err := st.InsertStep7IncentiveEvaluation(ctx, row); err == nil || inserted {
		t.Fatalf("unattributed reward candidate inserted=%v err=%v", inserted, err)
	}
}
