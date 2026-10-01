package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func step7BookEvent(ticker string, at time.Time, seq int64, bid, ask, bidDepth, askDepth float64) kalshi.ResearchReplayEvent {
	prior := seq - 1
	var priorPtr *int64
	if seq > 1 {
		priorPtr = &prior
	}
	return kalshi.ResearchReplayEvent{Kind: "book", EntityID: ticker, FrameType: "orderbook_delta", Channel: "orderbook_delta",
		SubscriptionID: 7, SourceGeneration: 1, ObservedAt: at, SourceAt: at, SourceSequence: &seq, PriorSourceSequence: priorPtr,
		Book: &kalshi.ResearchReplayBook{Valid: true, YesBid: bid, YesAsk: ask, YesBidSize: bidDepth, YesAskSize: askDepth}}
}

func TestStep7MatchedNaturalMakerTerminalBecomesEconomicCandidate(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	ticker := "KX-MAKER-STEP7"
	_, err := s.store.RegisterCanonicalBatch(ctx, []storage.CanonicalEventSpec{{EventID: "event:maker-step7", EventType: "fixture", Domain: "test", Title: "fixture", SourceArtifact: "test", SourceClockID: "test-clock", OutcomeSetStatus: "unknown"}},
		[]storage.CanonicalPayoffSpec{{PayoffID: "payoff:maker-step7", EventID: "event:maker-step7", Label: "YES", PredicateJSON: `{"yes":true}`, PayoutFloor: 0, PayoutCeiling: 1, SourceArtifact: "test", IdentityStatus: "verified"}},
		[]storage.CanonicalInstrumentSpec{{Venue: "kalshi", Ticker: ticker, EventID: "event:maker-step7", PayoffID: "payoff:maker-step7", NativeSide: "YES", Orientation: "same", RulesArtifact: "test", IdentityStatus: "verified"}})
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.store.InsertMakerAttempt(ctx, "kalshi", ticker, "YES", "sealed-system", .40, 2, 20, 0, nil, 1, "", "kalshi_book_ws_full")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.store.SetMakerStrategy(ctx, id, "sealed-system", false); err != nil {
		t.Fatal(err)
	}
	if err = s.store.SetMakerQueueState(ctx, id, 3, 0, 1); err != nil {
		t.Fatal(err)
	}
	if err = s.store.SetMakerSalvageDecisionTruth(ctx, id, storage.MakerSalvageDecisionTruth{DecisionID: "sealed-1", Rejection: "taker-negative", BookSource: "kalshi_book_ws_full", SourceClockID: "g1:s1:q1", CertificateHash: step7Hash("sealed-proof"), MakerNetLower: .03, TakerCost: .45, TakerFee: .02, TakerNetLower: -.01, Tick: .01, QuoteAge: .1, DecisionLatencyMS: 2, VisibleCapacity: 10}); err != nil {
		t.Fatal(err)
	}
	if err = s.store.MarkMakerFilledWithFee(ctx, id, .01, "kalshi:test-fee"); err != nil {
		t.Fatal(err)
	}
	if err = s.store.SetMakerAdverse(ctx, id, 0); err != nil {
		t.Fatal(err)
	}
	settledAt := time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
	if _, err = s.store.DBForTest().ExecContext(ctx, `UPDATE maker_fill_stats SET settle_val=1,settle_mirrored_at=? WHERE id=?`, settledAt, id); err != nil {
		t.Fatal(err)
	}
	s.sweepStep7MakerSalvage(ctx, time.Now().UTC().Add(2*time.Minute))
	var kind, route, blocker string
	var candidate int
	var net float64
	err = s.store.DBForTest().QueryRowContext(ctx, `SELECT observation_kind,route,blocker,candidate,net_lower FROM research_system_observations WHERE system_id='maker-salvage-matched-cohort' AND opportunity_id=?`, fmt.Sprintf("natural-maker:%d:terminal", id)).Scan(&kind, &route, &blocker, &candidate, &net)
	if err != nil || kind != "candidate" || route != "maker" || blocker != "" || candidate != 1 || math.Abs(net-.59) > 1e-9 {
		t.Fatalf("candidate=%q/%q blocker=%q flag=%d net=%v err=%v", kind, route, blocker, candidate, net, err)
	}
}

func TestR146MakerSalvageUsesPerContractFee(t *testing.T) {
	got, ok := makerSalvagePerContractFee(.15, 10)
	if !ok || math.Abs(got-.015) > 1e-12 {
		t.Fatalf("fee per contract=%v ok=%v, want .015 true", got, ok)
	}
	if _, ok := makerSalvagePerContractFee(.15, 0); ok {
		t.Fatal("zero-contract maker fee was accepted")
	}
}

func TestStep7FreshBookStateHasHardDeterministicCap(t *testing.T) {
	s := &Server{}
	now := time.Now().UTC()
	for i := 0; i < step7BookStateCap+1; i++ {
		ticker := fmt.Sprintf("KX-CAP-%04d", i)
		s.observeStep7Replay(step7BookEvent(ticker, now, 1, .40, .42, 10, 10))
	}
	st := step7RuntimeFor(s)
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.books) != step7BookStateCap {
		t.Fatalf("book states=%d want=%d", len(st.books), step7BookStateCap)
	}
	if _, exists := st.books["KX-CAP-0000"]; exists {
		t.Fatal("oldest ticker tie-break was not deterministically evicted")
	}
	if st.exclusions["book_state_capacity_eviction"] != 1 {
		t.Fatalf("exclusions=%v", st.exclusions)
	}
}

func TestStep7EventDrivenDepletionRefillAndFixedMarks(t *testing.T) {
	s := &Server{}
	now := time.Now().UTC()
	st := step7RuntimeFor(s)
	s.observeStep7Replay(step7BookEvent("KX-EVENT", now, 1, .40, .42, 10, 10))
	s.observeStep7Replay(step7BookEvent("KX-EVENT", now.Add(time.Millisecond), 2, .40, .42, 10, 5))
	s.observeStep7Replay(step7BookEvent("KX-EVENT", now.Add(20*time.Millisecond), 3, .40, .42, 10, 8))
	drain := st.drain(now.Add(6 * time.Second))
	if len(drain.episodes) != 1 {
		t.Fatalf("episodes=%d exclusions=%v", len(drain.episodes), drain.exclusions)
	}
	if drain.episodes[0].Side != "YES_ASK" || drain.episodes[0].DepletionUnits != 5 || drain.episodes[0].UnexplainedUnits != 5 {
		t.Fatalf("episode=%+v", drain.episodes[0])
	}
	refill, h5 := false, false
	for _, mark := range drain.marks {
		if mark.EventType == "refill" {
			refill = true
		}
		if mark.EventType == "horizon" && mark.HorizonMS == 5000 {
			h5 = true
		}
	}
	if !refill || !h5 {
		t.Fatalf("marks=%+v", drain.marks)
	}
}

func TestStep7RAMClockRetainsFiveThirtyAndThreeHundredSecondSamplesAcrossDelayedDrain(t *testing.T) {
	s := &Server{}
	defer step7Runtimes.Delete(s)
	base := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	episodeAt := base.Add(time.Millisecond)
	s.observeStep7Replay(step7BookEvent("KX-TIMING-V2", base, 1, .40, .42, 10, 10))
	s.observeStep7Replay(step7BookEvent("KX-TIMING-V2", episodeAt, 2, .40, .42, 10, 5))

	st := step7RuntimeFor(s)
	samples := map[int64]time.Time{
		5000:   episodeAt.Add(5*time.Second + 200*time.Millisecond),
		30000:  episodeAt.Add(30*time.Second + 300*time.Millisecond),
		300000: episodeAt.Add(300*time.Second + 400*time.Millisecond),
	}
	for _, horizon := range []int64{5000, 30000, 300000} {
		st.advanceTimedEvidence(samples[horizon])
	}
	// Simulate persistence being delayed for minutes. The drain must retain the earlier RAM
	// sample clocks and must not regenerate any horizon at this later time.
	drain := st.drain(episodeAt.Add(10 * time.Minute))
	seen := map[int64]storage.Step7ReplenishmentMark{}
	for _, mark := range drain.marks {
		if mark.EventType == "horizon" {
			seen[mark.HorizonMS] = mark
		}
	}
	if len(seen) != 3 {
		t.Fatalf("horizon marks=%d want=3: %+v", len(seen), drain.marks)
	}
	for horizon, sampled := range samples {
		mark := seen[horizon]
		target := episodeAt.Add(time.Duration(horizon) * time.Millisecond)
		if mark.TimingVersion != step7TimingExperimentVersion || !mark.Target.Equal(target) ||
			!mark.Observed.Equal(sampled) {
			t.Fatalf("horizon=%d version=%d target=%s sampled=%s want target=%s sampled=%s",
				horizon, mark.TimingVersion, mark.Target, mark.Observed, target, sampled)
		}
		evidence, ok := mark.Evidence.(map[string]any)
		if !ok || evidence["timing_cohort"] != step7TimingCohort ||
			evidence["sampled_at"] != sampled.UTC().Format(time.RFC3339Nano) {
			t.Fatalf("horizon=%d provenance=%#v", horizon, mark.Evidence)
		}
		wantLate := sampled.Sub(target).Seconds() * 1000
		if got, ok := evidence["sample_lateness_ms"].(float64); !ok || math.Abs(got-wantLate) > 1e-9 {
			t.Fatalf("horizon=%d lateness=%v want=%v", horizon, evidence["sample_lateness_ms"], wantLate)
		}
	}
}

func TestStep7StorageFailureRequeuesOneCopyUntilSuccessfulPersistence(t *testing.T) {
	s := testServer(t)
	defer step7Runtimes.Delete(s)
	base := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	episodeAt := base.Add(time.Millisecond)
	s.observeStep7Replay(step7BookEvent("KX-REQUEUE", base, 1, .40, .42, 10, 10))
	s.observeStep7Replay(step7BookEvent("KX-REQUEUE", episodeAt, 2, .40, .42, 10, 5))
	step7RuntimeFor(s).advanceTimedEvidence(episodeAt.Add(5*time.Second + 100*time.Millisecond))

	failed, cancel := context.WithCancel(context.Background())
	cancel()
	var firstProjectionHashes []string
	for attempt := 0; attempt < 2; attempt++ {
		s.flushStep7Microstructure(failed, episodeAt.Add(6*time.Second))
		st := step7RuntimeFor(s)
		st.mu.Lock()
		episodes, marks := len(st.episodes), len(st.marks)
		hashes := make([]string, len(st.projections))
		for i := range st.projections {
			hashes[i] = st.projections[i].PayloadHash
		}
		st.mu.Unlock()
		if episodes != 1 || marks != 1 || len(hashes) != 2 {
			t.Fatalf("failed attempt %d requeued episodes=%d marks=%d projections=%d, want one copy each",
				attempt+1, episodes, marks, len(hashes))
		}
		if attempt == 0 {
			firstProjectionHashes = hashes
		} else if !reflect.DeepEqual(hashes, firstProjectionHashes) {
			t.Fatalf("retry rebuilt frozen projection payloads: first=%v retry=%v",
				firstProjectionHashes, hashes)
		}
	}

	s.flushStep7Microstructure(context.Background(), episodeAt.Add(6*time.Second))
	st := step7RuntimeFor(s)
	st.mu.Lock()
	episodes, marks, projections := len(st.episodes), len(st.marks), len(st.projections)
	st.mu.Unlock()
	if episodes != 0 || marks != 0 || projections != 0 {
		t.Fatalf("successful persistence left queued episodes=%d marks=%d projections=%d",
			episodes, marks, projections)
	}
	var episodeRows, markRows, jobs, completions, pending int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM research_replenishment_episodes
WHERE episode_id IN (SELECT episode_id FROM research_replenishment_marks)`).Scan(&episodeRows); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM research_replenishment_marks`).Scan(&markRows); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM research_step7_projection_jobs`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM research_step7_projection_completions`).Scan(&completions); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM research_step7_projection_jobs
WHERE completed_ts=''`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if episodeRows != 1 || markRows != 1 || jobs != 2 || completions != 2 || pending != 0 {
		t.Fatalf("durable rows episodes=%d marks=%d jobs=%d completions=%d pending=%d",
			episodeRows, markRows, jobs, completions, pending)
	}
}

func TestStep7RestoreDrainStaysBoundedWithConcurrentArrivalsAndRepeatedFailure(t *testing.T) {
	st := &step7Runtime{
		books:      map[string]step7Touch{},
		active:     map[string]string{},
		tracked:    map[string]*step7TrackedEpisode{},
		exclusions: map[string]int{},
	}
	oldEpisode := storage.Step7ReplenishmentEpisode{EpisodeID: "old-episode", Ticker: "OLD", Side: "YES_ASK"}
	oldMark := storage.Step7ReplenishmentMark{EpisodeID: "old-episode", EventType: "horizon"}
	oldFlow := storage.Step7FlowDirectionPair{PairID: "old-flow"}
	failed := step7Drain{
		episodes: []storage.Step7ReplenishmentEpisode{oldEpisode},
		marks:    []storage.Step7ReplenishmentMark{oldMark},
		flows:    []storage.Step7FlowDirectionPair{oldFlow},
		exclusions: map[string]int{
			"failed_batch_accounting": 1,
		},
	}
	fillConcurrent := func(prefix string) string {
		lastEpisode := ""
		for i := 0; i < step7EpisodeQueue; i++ {
			id := fmt.Sprintf("%s-episode-%d", prefix, i)
			ticker := fmt.Sprintf("%s-ticker-%d", prefix, i)
			st.episodes = append(st.episodes, storage.Step7ReplenishmentEpisode{
				EpisodeID: id, Ticker: ticker, Side: "YES_ASK",
			})
			st.tracked[id] = &step7TrackedEpisode{row: storage.Step7ReplenishmentEpisode{
				EpisodeID: id, Ticker: ticker, Side: "YES_ASK",
			}, marks: map[int64]bool{}}
			st.active[ticker+"|YES_ASK"] = id
			lastEpisode = id
		}
		for i := 0; i < step7MarkQueue; i++ {
			st.marks = append(st.marks, storage.Step7ReplenishmentMark{
				EpisodeID: "already-durable", EventType: "horizon", HorizonMS: int64(i + 1),
			})
		}
		for i := 0; i < step7FlowQueue; i++ {
			st.flows = append(st.flows, storage.Step7FlowDirectionPair{
				PairID: fmt.Sprintf("%s-flow-%d", prefix, i),
			})
		}
		return lastEpisode
	}

	droppedID := fillConcurrent("first")
	st.restoreDrain(failed)
	if len(st.episodes) != step7EpisodeQueue || len(st.marks) != step7MarkQueue ||
		len(st.flows) != step7FlowQueue {
		t.Fatalf("first restore exceeded caps: episodes=%d marks=%d flows=%d",
			len(st.episodes), len(st.marks), len(st.flows))
	}
	if st.episodes[0].EpisodeID != oldEpisode.EpisodeID ||
		st.marks[0].EpisodeID != oldMark.EpisodeID || st.flows[0].PairID != oldFlow.PairID {
		t.Fatal("failed batch did not retain priority and source order")
	}
	if st.tracked[droppedID] != nil {
		t.Fatal("overflowed concurrent episode remained tracked and could emit orphan marks")
	}
	if st.exclusions["restore_episode_concurrent_overflow"] != 1 ||
		st.exclusions["restore_mark_concurrent_overflow"] != 1 ||
		st.exclusions["restore_flow_concurrent_overflow"] != 1 {
		t.Fatalf("first restore overflow accounting=%v", st.exclusions)
	}

	// Drain the full failed batch again, accept another full callback burst while the synthetic
	// storage call is in flight, and restore. Queue sizes must remain fixed rather than growing by
	// one failed batch per retry.
	secondFailed := st.drain(time.Time{})
	fillConcurrent("second")
	st.restoreDrain(secondFailed)
	if len(st.episodes) != step7EpisodeQueue || len(st.marks) != step7MarkQueue ||
		len(st.flows) != step7FlowQueue {
		t.Fatalf("repeated restore exceeded caps: episodes=%d marks=%d flows=%d",
			len(st.episodes), len(st.marks), len(st.flows))
	}
	if st.episodes[0].EpisodeID != oldEpisode.EpisodeID ||
		st.marks[0].EpisodeID != oldMark.EpisodeID || st.flows[0].PairID != oldFlow.PairID {
		t.Fatal("repeated restore changed the original failed identities")
	}
	if st.exclusions["restore_episode_concurrent_overflow"] < step7EpisodeQueue+1 ||
		st.exclusions["restore_mark_concurrent_overflow"] < step7MarkQueue+1 ||
		st.exclusions["restore_flow_concurrent_overflow"] < step7FlowQueue+1 {
		t.Fatalf("repeated restore did not explicitly count overflow: %v", st.exclusions)
	}
}

func TestStep7FlowPairsCanonicalAndInferredWithoutSizeMinimum(t *testing.T) {
	s := &Server{}
	now := time.Now().UTC()
	st := step7RuntimeFor(s)
	s.observeStep7Replay(step7BookEvent("KX-FLOW", now, 1, .40, .42, 10, 10))
	tradeSeq := int64(1)
	trade := kalshi.ResearchReplayEvent{Kind: "trade", EntityID: "KX-FLOW", ObservedAt: now.Add(time.Millisecond),
		SourceAt: now.Add(time.Millisecond), SourceGeneration: 1, SourceSequence: &tradeSeq, Trade: &kalshi.ResearchReplayTrade{
			TradeID: "tiny", Count: .001, YesPrice: .42, TakerOutcomeSide: "yes", TakerBookSide: "bid", Aggressor: "yes"}}
	s.observeStep7Replay(trade)
	s.observeStep7Replay(trade)
	drain := st.drain(now.Add(2 * time.Millisecond))
	if len(drain.flows) != 1 {
		t.Fatalf("flows=%d exclusions=%v", len(drain.flows), drain.exclusions)
	}
	if got := drain.flows[0]; got.AuthoritativeSide != "yes" || got.InferredSide != "yes" || got.ComparisonStatus != "agreement" || got.Count != .001 {
		t.Fatalf("pair=%+v", got)
	}
	if drain.exclusions["flow_trade_replay"] != 1 {
		t.Fatalf("same-cycle trade replay was not explicit: %v", drain.exclusions)
	}
	// A reconnect replay after persistence/drain must not become a second opportunity or inflate
	// the depletion trade counter. The durable source/job identity is the restart boundary.
	trade.ObservedAt = trade.ObservedAt.Add(time.Second)
	s.observeStep7Replay(trade)
	replay := st.drain(trade.ObservedAt)
	if len(replay.flows) != 0 || replay.exclusions["flow_trade_replay"] != 1 {
		t.Fatalf("post-drain trade replay leaked: flows=%d exclusions=%v",
			len(replay.flows), replay.exclusions)
	}
}

func TestR174Step7FlowWithoutVenueTradeIDIsExcludedButStillCountsDepletion(t *testing.T) {
	s := &Server{}
	defer step7Runtimes.Delete(s)
	now := time.Now().UTC()
	st := step7RuntimeFor(s)
	s.observeStep7Replay(step7BookEvent("KX-NO-TRADE-ID", now, 1, .40, .42, 10, 10))
	tradeSeq := int64(2)
	s.observeStep7Replay(kalshi.ResearchReplayEvent{
		Kind: "trade", EntityID: "KX-NO-TRADE-ID", ObservedAt: now.Add(time.Millisecond),
		SourceAt: now.Add(time.Millisecond), SourceGeneration: 1, SourceSequence: &tradeSeq,
		Trade: &kalshi.ResearchReplayTrade{
			Count: 2, YesPrice: .42, TakerOutcomeSide: "yes", TakerBookSide: "bid",
		},
	})
	drain := st.drain(now.Add(2 * time.Millisecond))
	if len(drain.flows) != 0 || drain.exclusions["flow_trade_id_missing"] != 1 {
		t.Fatalf("missing-id trade entered flow cohort: flows=%d exclusions=%v",
			len(drain.flows), drain.exclusions)
	}
	if got := st.books["KX-NO-TRADE-ID"].TradeYes; got != 2 {
		t.Fatalf("missing-id trade disappeared from depletion accounting: trade_yes=%f", got)
	}
}

func TestR174Step7FrozenLifecycleRejectsLingeringClosedOrUnknownBooks(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	setMarket := func(m kalshi.Market) {
		s.metaMu.Lock()
		if s.kmkts == nil {
			s.kmkts = map[string]kalshi.Market{}
		}
		s.kmkts[m.Ticker] = m
		s.metaMu.Unlock()
	}
	setMarket(kalshi.Market{Ticker: "KX-STEP7-OPEN", Status: "active",
		CloseTime: now.Add(time.Hour).Format(time.RFC3339)})
	if _, reason := s.step7FrozenKalshiMarket("KX-STEP7-OPEN", now); reason != "" {
		t.Fatalf("open lifecycle rejected: %s", reason)
	}
	setMarket(kalshi.Market{Ticker: "KX-STEP7-CLOSED", Status: "closed",
		CloseTime: now.Add(time.Hour).Format(time.RFC3339)})
	if _, reason := s.step7FrozenKalshiMarket("KX-STEP7-CLOSED", now); reason != "frozen_market_not_open" {
		t.Fatalf("closed lifecycle reason=%q", reason)
	}
	setMarket(kalshi.Market{Ticker: "KX-STEP7-PAST", Status: "active",
		CloseTime: now.Add(-time.Second).Format(time.RFC3339)})
	if _, reason := s.step7FrozenKalshiMarket("KX-STEP7-PAST", now); reason != "frozen_market_close_time_invalid_or_past" {
		t.Fatalf("past lifecycle reason=%q", reason)
	}
	setMarket(kalshi.Market{Ticker: "KX-STEP7-NOCLOSE", Status: "active"})
	if _, reason := s.step7FrozenKalshiMarket("KX-STEP7-NOCLOSE", now); reason != "frozen_market_close_time_unavailable" {
		t.Fatalf("missing-close lifecycle reason=%q", reason)
	}
}

func TestR174Step7FreezerTreatsIdenticalRawReplayAsOneSource(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	flow := storage.Step7FlowDirectionPair{
		PairID: "replayed-flow", Ticker: "KX-STEP7-REPLAY", TradeID: "trade-replay",
		Observed: now, Count: 1, YesPrice: .42, AuthoritativeSide: "unknown",
		InferredSide: "yes", ComparisonStatus: "authoritative_unknown",
		SourceClockStatus: "sequenced", Evidence: map[string]any{"fixture": true},
	}
	drain := step7Drain{flows: []storage.Step7FlowDirectionPair{flow, flow}}
	if _, err := s.freezeStep7ProjectionJobs(context.Background(), &drain, func() time.Time { return now }); err != nil {
		t.Fatalf("identical raw replay wedged freezer: %v", err)
	}
	if len(drain.projections) != 1 ||
		drain.projections[0].TerminalReason != "authoritative_flow_side_conflict_or_unknown" {
		t.Fatalf("replay projections=%+v", drain.projections)
	}
}

func TestR174Step7FreezerUsesOneDecisionClockPerJob(t *testing.T) {
	s := testServer(t)
	base := time.Now().UTC().Truncate(time.Millisecond)
	drain := step7Drain{episodes: []storage.Step7ReplenishmentEpisode{
		{EpisodeID: "clock-job-1", Observed: base.Add(-time.Second), Ticker: "KX-CLOCK-1", Side: "YES_ASK"},
		{EpisodeID: "clock-job-2", Observed: base.Add(-time.Second), Ticker: "KX-CLOCK-2", Side: "YES_ASK"},
	}}
	decisionTimes := []time.Time{base, base.Add(25 * time.Millisecond)}
	call := 0
	clock := func() time.Time {
		if call >= len(decisionTimes) {
			t.Fatalf("decision clock called too many times: %d", call+1)
		}
		at := decisionTimes[call]
		call++
		return at
	}
	if _, err := s.freezeStep7ProjectionJobs(context.Background(), &drain, clock); err != nil {
		t.Fatal(err)
	}
	if call != 2 || len(drain.projections) != 2 {
		t.Fatalf("clock calls=%d jobs=%d", call, len(drain.projections))
	}
	for i, job := range drain.projections {
		var payload struct {
			Outputs []struct {
				Observation struct {
					DecisionAt time.Time
				}
			}
		}
		if err := json.Unmarshal([]byte(job.PayloadJSON), &payload); err != nil {
			t.Fatal(err)
		}
		if len(payload.Outputs) != 1 ||
			!payload.Outputs[0].Observation.DecisionAt.Equal(decisionTimes[i]) {
			t.Fatalf("job %d decision=%+v want=%s", i, payload.Outputs,
				decisionTimes[i].Format(time.RFC3339Nano))
		}
	}
}

func TestR174Step7FreezerTerminalizesRawClockAheadOfDecision(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	episode := storage.Step7ReplenishmentEpisode{
		EpisodeID: "future-raw-clock", Observed: now.Add(time.Second),
		Ticker: "KX-FUTURE-CLOCK", Side: "YES_ASK",
	}
	drain := step7Drain{episodes: []storage.Step7ReplenishmentEpisode{episode}}
	if _, err := s.freezeStep7ProjectionJobs(context.Background(), &drain,
		func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	if len(drain.projections) != 1 ||
		drain.projections[0].TerminalReason != "raw_episode_clock_ahead_of_projection" {
		t.Fatalf("future raw clock was not terminalized: %+v", drain.projections)
	}
}

func TestR146FlowLabelAuditKeepsCompactPairWithoutObserverMirrors(t *testing.T) {
	s := testServer(t)
	defer step7Runtimes.Delete(s)
	ctx := context.Background()
	now := time.Now().UTC()
	st := step7RuntimeFor(s)
	st.mu.Lock()
	st.attemptTrades = 1
	st.flows = append(st.flows, storage.Step7FlowDirectionPair{
		PairID: "compact-flow-pair", Ticker: "KX-COMPACT-FLOW", TradeID: "trade-1",
		AuthoritativeSide: "unknown", InferredSide: "yes", ComparisonStatus: "authoritative_unknown",
		SourceClockStatus: "same_generation_sequence_known", Observed: now, SourceAt: now,
		BookObserved: now.Add(-time.Millisecond), Count: 1, YesPrice: .42, BookAgeMS: 1,
		YesBid: .40, YesAsk: .42, BookGeneration: 1, BookSequence: 10, TradeSequence: 11,
		Evidence: map[string]any{"complete_pair": true},
	})
	st.mu.Unlock()
	s.flushStep7Microstructure(ctx, now)
	var pairs, observations, routeMirrors, jobs, completions, pending int
	var terminalReason string
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM research_flow_direction_pairs
WHERE pair_id='compact-flow-pair'`).Scan(&pairs); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM research_system_observations
WHERE system_id='flow-direction-integrity'`).Scan(&observations); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM research_route_opportunities
WHERE system_name='flow-direction-integrity'`).Scan(&routeMirrors); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*),COALESCE(MAX(terminal_reason),'')
FROM research_step7_projection_jobs`).Scan(&jobs, &terminalReason); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM research_step7_projection_completions`).Scan(&completions); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM research_step7_projection_jobs
WHERE completed_ts=''`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pairs != 1 || observations != 0 || routeMirrors != 0 || jobs != 1 || completions != 1 ||
		pending != 0 || terminalReason != "authoritative_flow_side_conflict_or_unknown" {
		t.Fatalf("pairs=%d observations=%d routes=%d jobs=%d complete=%d pending=%d terminal=%q",
			pairs, observations, routeMirrors, jobs, completions, pending, terminalReason)
	}
}

func TestR146FlowIntegrityAuthoritativeSideOwnsExactCandidateAndOppositeControl(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	const ticker = "KX-FLOW-INTEGRITY"
	_, err := s.store.RegisterCanonicalBatch(ctx,
		[]storage.CanonicalEventSpec{{EventID: "event:flow-integrity", EventType: "fixture", Domain: "test",
			Title: "flow fixture", SourceArtifact: "fixture", SourceClockID: "fixture-clock", OutcomeSetStatus: "complete"}},
		[]storage.CanonicalPayoffSpec{{PayoffID: "payoff:flow-integrity", EventID: "event:flow-integrity",
			Label: "YES", PredicateJSON: `{"yes":true}`, PayoutFloor: 0, PayoutCeiling: 1,
			SourceArtifact: "fixture", IdentityStatus: "verified"}},
		[]storage.CanonicalInstrumentSpec{{Venue: "kalshi", Ticker: ticker, EventID: "event:flow-integrity",
			PayoffID: "payoff:flow-integrity", NativeSide: "YES", Orientation: "same", MarketKind: "binary",
			RulesArtifact: "fixture", IdentityStatus: "verified"}})
	if err != nil {
		t.Fatal(err)
	}
	identity, found, err := s.store.CurrentCanonicalInstrument(ctx, "kalshi", ticker)
	if err != nil || !found {
		t.Fatalf("identity=%+v found=%v err=%v", identity, found, err)
	}
	legs := []nativeExactLeg{
		{Venue: "kalshi", Ticker: ticker, Side: "YES", Ask: .42, Depth: 8, Tick: .01,
			Fee: .01, QuoteAge: .0005, QuoteObserved: now.Add(-500 * time.Microsecond),
			BookSource: "kalshi-book:g1:sid1:seq1", FeeSource: "fixture-fee", SourceClockID: "g1:sid1:seq1"},
		{Venue: "kalshi", Ticker: ticker, Side: "NO", Ask: .60, Depth: 7, Tick: .01,
			Fee: .01, QuoteAge: .0005, QuoteObserved: now.Add(-500 * time.Microsecond),
			BookSource: "kalshi-book:g1:sid1:seq1", FeeSource: "fixture-fee", SourceClockID: "g1:sid1:seq1"},
	}
	pair := storage.Step7FlowDirectionPair{PairID: "pair-r146", Ticker: ticker, TradeID: "trade-r146",
		AuthoritativeSide: "no", InferredSide: "yes", ComparisonStatus: "mismatch",
		Observed: now.Add(-time.Millisecond), Count: 3, YesPrice: .40}
	in, ok := step7FlowIntegrityAtomicPairInput(pair, identity, legs, now)
	if !ok || in.SelectedSide != "NO" || in.SelectorID != step7FlowAtomicSelector {
		t.Fatalf("input=%+v ok=%v", in, ok)
	}
	outputs, err := step7FrozenConcretePairOutputs(in, pair.Observed, step7FlowExperimentVersion)
	if err != nil || len(outputs) != 2 {
		t.Fatalf("outputs=%+v err=%v", outputs, err)
	}
	seenCandidate, seenControl := false, false
	for _, output := range outputs {
		row := output.Observation
		if row.ExperimentVersion != step7FlowExperimentVersion ||
			output.InstrumentVersion != identity.InstrumentVersion {
			t.Fatalf("unversioned frozen output=%+v", output)
		}
		if !row.Observed.Equal(pair.Observed) {
			t.Fatalf("projection moved out of its raw trigger window: raw=%s projected=%s",
				pair.Observed.Format(time.RFC3339Nano), row.Observed.Format(time.RFC3339Nano))
		}
		if !row.DecisionAt.Equal(now) {
			t.Fatalf("frozen decision clock was lost: want=%s got=%s",
				now.Format(time.RFC3339Nano), row.DecisionAt.Format(time.RFC3339Nano))
		}
		if !row.DecisionAt.After(legs[0].QuoteObserved) {
			t.Fatalf("decision predates quote snapshot: decision=%s quote=%s",
				row.DecisionAt.Format(time.RFC3339Nano), legs[0].QuoteObserved.Format(time.RFC3339Nano))
		}
		if math.Abs(row.DecisionLatencyMS-1) > 0.25 {
			t.Fatalf("decision latency=%f want about 1ms", row.DecisionLatencyMS)
		}
		seenCandidate = seenCandidate || row.Side == "NO" && row.Kind == "candidate" && row.Candidate && row.Blocker == ""
		seenControl = seenControl || row.Side == "YES" && row.Kind == "control" && !row.Candidate && row.Blocker != ""
	}
	if !seenCandidate || !seenControl {
		t.Fatalf("candidate/control missing: candidate=%v control=%v", seenCandidate, seenControl)
	}
	futureLegs := append([]nativeExactLeg(nil), legs...)
	futureLegs[0].QuoteObserved = now.Add(time.Millisecond)
	if _, ok := step7FlowIntegrityAtomicPairInput(pair, identity, futureLegs, now); ok {
		t.Fatal("decision clock accepted a quote snapshot from its future")
	}
	pair.AuthoritativeSide = "conflict"
	if _, ok := step7FlowIntegrityAtomicPairInput(pair, identity, legs, now); ok {
		t.Fatal("authoritative conflict emitted an executable action")
	}
}

func TestR175Step7StructuralKalshiIdentityRetainsBlockedExactFlowObservation(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 25, 18, 0, 0, 0, time.UTC)
	const ticker = "KX-STEP7-STRUCTURAL"
	_, err := s.store.RegisterCanonicalBatch(ctx,
		[]storage.CanonicalEventSpec{{
			EventID: "event:step7-structural", EventType: "venue-local-binary", Domain: "kalshi",
			Title: "structural fixture", SourceArtifact: "kalshi venue fixture",
			SourceClockID: "kalshi-catalog-generation-7", OutcomeSetStatus: "unknown",
		}},
		[]storage.CanonicalPayoffSpec{{
			PayoffID: "payoff:step7-structural", EventID: "event:step7-structural",
			Label: "venue YES predicate", PredicateJSON: `{"venue_native_yes":true}`,
			PayoutFloor: 0, PayoutCeiling: 1, SourceArtifact: "kalshi venue fixture",
			IdentityStatus: "structural",
		}},
		[]storage.CanonicalInstrumentSpec{{
			Venue: "kalshi", Ticker: ticker, EventID: "event:step7-structural",
			PayoffID: "payoff:step7-structural", NativeSide: "YES", Orientation: "same",
			MarketKind: "binary", RulesArtifact: "kalshi exact ticker fixture",
			IdentityStatus: "structural",
		}})
	if err != nil {
		t.Fatal(err)
	}
	identity, found, err := s.store.CurrentCanonicalInstrument(ctx, "kalshi", ticker)
	if err != nil || !found {
		t.Fatalf("identity=%+v found=%v err=%v", identity, found, err)
	}
	const clockID = "kalshi-book:g7:sid11:seq101|received=2026-07-25T17:59:59.999Z"
	legs := []nativeExactLeg{
		{Venue: "kalshi", Ticker: ticker, Side: "YES", Ask: .41, Depth: 9, Tick: .01,
			Fee: .007, QuoteAge: .001, QuoteObserved: now.Add(-time.Millisecond),
			BookSource: "kalshi_book_ws_full", FeeSource: "kalshi-fee-generation-4",
			SourceClockID: clockID},
		{Venue: "kalshi", Ticker: ticker, Side: "NO", Ask: .61, Depth: 8, Tick: .01,
			Fee: .006, QuoteAge: .001, QuoteObserved: now.Add(-time.Millisecond),
			BookSource: "kalshi_book_ws_full", FeeSource: "kalshi-fee-generation-4",
			SourceClockID: clockID},
	}
	pair := storage.Step7FlowDirectionPair{
		PairID: "pair-r175-structural", Ticker: ticker, TradeID: "trade-r175-structural",
		AuthoritativeSide: "yes", InferredSide: "no", ComparisonStatus: "mismatch",
		Observed: now.Add(-2 * time.Millisecond), Count: 2, YesPrice: .40,
	}
	in, ok := step7FlowIntegrityAtomicPairInput(pair, identity, legs, now)
	if !ok {
		t.Fatal("exact structural venue-local identity/book/fee/clock was rejected")
	}
	if in.Opportunity != "authoritative-flow-v3|"+pair.PairID ||
		in.Cohort != step7FlowAtomicCohort || in.SelectorID != step7FlowAtomicSelector {
		t.Fatalf("old/new flow cohorts mixed: %+v", in)
	}
	outputs, err := step7FrozenConcretePairOutputs(in, pair.Observed, step7FlowExperimentVersion)
	if err != nil || len(outputs) != 2 {
		t.Fatalf("outputs=%+v err=%v", outputs, err)
	}
	selectedObservationID := int64(0)
	for _, output := range outputs {
		row := output.Observation
		if row.ExperimentVersion != 3 || row.CertificateStatus != "structural" ||
			output.InstrumentVersion != identity.InstrumentVersion ||
			row.CanonicalEventID != identity.EventID || row.EventVersion != identity.EventVersion ||
			row.CanonicalPayoffID != identity.PayoffID || row.PayoffVersion != identity.PayoffVersion ||
			row.BookSource != "kalshi_book_ws_full" ||
			row.FeeSource != "kalshi-fee-generation-4" || row.SourceClockID != clockID ||
			!row.QuoteAgeKnown || !row.LatencyKnown || !row.TickKnown || !row.DepthKnown || !row.FeeKnown ||
			math.Abs(row.DecisionLatencyMS-2) > 1e-9 {
			t.Fatalf("structural route lost frozen exact truth: %+v", output)
		}
		inputs, ok := row.Inputs.(map[string]any)
		if !ok {
			t.Fatalf("structural route inputs are not a map: %#v", row.Inputs)
		}
		if crossVenue, ok := inputs["cross_venue_equivalence_verified"].(bool); !ok || crossVenue {
			t.Fatalf("venue-local row claimed cross-venue equivalence: %#v", row.Inputs)
		}
		if row.Side == "YES" {
			if row.Kind != "negative" || row.Candidate || row.Blocker != step7FlowStructuralBlocker ||
				math.Abs(row.Cost-.41) > 1e-12 || math.Abs(row.Fee-.007) > 1e-12 ||
				math.Abs(row.VisibleCapacity-9) > 1e-12 {
				t.Fatalf("selected structural observation gained authority or lost economics: %+v", row)
			}
		} else if row.Side == "NO" {
			if row.Kind != "control" || row.Candidate || row.Blocker == "" ||
				math.Abs(row.Cost-.61) > 1e-12 || math.Abs(row.Fee-.006) > 1e-12 ||
				math.Abs(row.VisibleCapacity-8) > 1e-12 {
				t.Fatalf("matched control changed semantics: %+v", row)
			}
		} else {
			t.Fatalf("unexpected side: %+v", row)
		}
		row.InstrumentVersion = output.InstrumentVersion
		id, _, err := s.store.InsertResearchSystemObservation(ctx, row)
		if err != nil {
			t.Fatalf("store structural observation: %v", err)
		}
		if row.Side == "YES" {
			selectedObservationID = id
		}
	}

	var observations, authority, routeCandidates, blockedStructural int
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*),
COALESCE(SUM(funded+paper_authority+live_authority),0)
FROM research_system_observations
WHERE system_id='flow-direction-integrity' AND opportunity_id LIKE 'authoritative-flow-v3|pair-r175-structural|%'`).
		Scan(&observations, &authority); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT
COALESCE(SUM(decision='candidate'),0),
COALESCE(SUM(identity_status='structural' AND decision='blocked'),0)
FROM research_route_opportunities WHERE system_name='flow-direction-integrity'`).
		Scan(&routeCandidates, &blockedStructural); err != nil {
		t.Fatal(err)
	}
	paperCandidates, err := s.store.RecentResearchPaperCandidates(ctx, now.Add(-time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if observations != 2 || authority != 0 || routeCandidates != 0 || blockedStructural != 1 ||
		len(paperCandidates) != 0 {
		t.Fatalf("structural authority leak observations=%d authority=%d route_candidates=%d blocked=%d paper=%d",
			observations, authority, routeCandidates, blockedStructural, len(paperCandidates))
	}
	if _, promotable, err := s.store.LatestResearchPromotionCandidate(ctx, storage.ResearchPromotionProof{
		SystemID: "flow-direction-integrity", ExperimentVersion: step7FlowExperimentVersion,
		Cohort: step7FlowAtomicCohort + "|structural-action|policy=" + step7FlowAtomicSelector,
		Venue:  "kalshi", Route: "taker",
	}, now.Add(-time.Minute)); err != nil || promotable {
		t.Fatalf("blocked structural observation reached promotion: found=%v err=%v", promotable, err)
	}

	resolved := now.Add(time.Hour)
	seedExactVenueSettlement(t, s.store, "kalshi", ticker, 1, now.Add(-time.Hour), resolved)
	pending, err := s.store.PendingResearchSystemTakerSettlements(ctx, 10)
	if err != nil || len(pending) != 2 {
		t.Fatalf("structural selected action/control did not reach exact settlement: rows=%+v err=%v",
			pending, err)
	}
	negativePending := 0
	for _, row := range pending {
		if row.ObservationID == selectedObservationID && row.ObservationKind == "negative" {
			negativePending++
		}
	}
	if negativePending != 1 {
		t.Fatalf("selected structural action settlement rows=%d want=1: %+v", negativePending, pending)
	}
	s.sweepResearchSystemTerminalGrades(ctx, resolved.Add(time.Minute))
	var payout, realized float64
	var payoffAuthority int
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT u.payout_lower,u.realized_net,
u.funded+u.paper_authority+u.live_authority
FROM research_system_payoff_updates u WHERE u.observation_id=?`, selectedObservationID).
		Scan(&payout, &realized, &payoffAuthority); err != nil {
		t.Fatal(err)
	}
	if payout != 1 || math.Abs(realized-.583) > 1e-12 || payoffAuthority != 0 {
		t.Fatalf("structural exact payout/net/authority=%v/%v/%d want 1/.583/0",
			payout, realized, payoffAuthority)
	}
	report, _, err := s.store.RunR139ResearchInference(ctx, resolved.Add(30*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if report.ExactTerminalRows != 0 || report.Exclusions.Identity < 1 ||
		report.Funded || report.PaperAuthority || report.LiveAuthority {
		t.Fatalf("structural payoff escaped inference identity gate: %+v", report)
	}

	badFee := append([]nativeExactLeg(nil), legs...)
	badFee[0].FeeSource = ""
	if _, ok := step7FlowIntegrityAtomicPairInput(pair, identity, badFee, now); ok {
		t.Fatal("structural flow accepted a route without exact fee authority")
	}
	badClock := append([]nativeExactLeg(nil), legs...)
	badClock[1].SourceClockID = "different-book-frame"
	if _, ok := step7FlowIntegrityAtomicPairInput(pair, identity, badClock, now); ok {
		t.Fatal("structural flow accepted YES/NO from different book clocks")
	}
	mixedCase := append([]nativeExactLeg(nil), legs...)
	mixedCase[0].Side, mixedCase[1].Side = "yes", "nO"
	mixedInput, ok := step7FlowIntegrityAtomicPairInput(pair, identity, mixedCase, now)
	if !ok {
		t.Fatal("valid mixed-case native sides were not normalized")
	}
	mixedOutputs, err := step7FrozenConcretePairOutputs(mixedInput, pair.Observed,
		step7FlowExperimentVersion)
	if err != nil {
		t.Fatal(err)
	}
	selectedCount := 0
	for _, output := range mixedOutputs {
		if output.Observation.Side != "YES" && output.Observation.Side != "NO" {
			t.Fatalf("non-normalized stored side: %q", output.Observation.Side)
		}
		if output.Observation.Kind == "negative" &&
			output.Observation.Blocker == step7FlowStructuralBlocker {
			selectedCount++
		}
	}
	if selectedCount != 1 {
		t.Fatalf("mixed-case route emitted %d selected structural actions, want exactly one",
			selectedCount)
	}
}

func TestR175Step7UnverifiedOrMissingKalshiIdentityRemainsExplicitZeroOutput(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 25, 18, 5, 0, 0, time.UTC)
	const ticker = "KX-STEP7-UNVERIFIED"
	_, err := s.store.RegisterCanonicalBatch(ctx,
		[]storage.CanonicalEventSpec{{
			EventID: "event:step7-unverified", EventType: "venue-local-binary", Domain: "kalshi",
			Title: "unverified fixture", SourceArtifact: "fixture", SourceClockID: "fixture-clock",
			OutcomeSetStatus: "unknown",
		}},
		[]storage.CanonicalPayoffSpec{{
			PayoffID: "payoff:step7-unverified", EventID: "event:step7-unverified",
			Label: "unknown predicate", PredicateJSON: `{}`, PayoutFloor: 0, PayoutCeiling: 1,
			SourceArtifact: "fixture", IdentityStatus: "unverified",
		}},
		[]storage.CanonicalInstrumentSpec{{
			Venue: "kalshi", Ticker: ticker, EventID: "event:step7-unverified",
			PayoffID: "payoff:step7-unverified", NativeSide: "YES", Orientation: "unknown",
			MarketKind: "binary", RulesArtifact: "fixture", IdentityStatus: "unverified",
		}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		ticker string
	}{
		{name: "unverified", ticker: ticker},
		{name: "missing", ticker: "KX-STEP7-MISSING"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pair := storage.Step7FlowDirectionPair{
				PairID: "pair-" + tc.name, Ticker: tc.ticker, TradeID: "trade-" + tc.name,
				AuthoritativeSide: "yes", Observed: now.Add(-time.Millisecond),
			}
			outputs, terminal, err := s.freezeStep7FlowOutputs(ctx, pair, func() time.Time { return now })
			if err != nil || len(outputs) != 0 ||
				terminal != "authoritative_flow_identity_unverified" {
				t.Fatalf("outputs=%+v terminal=%q err=%v", outputs, terminal, err)
			}
		})
	}
}

func TestStep7AuthoritativeConflictNeverGetsSilentlyNormalized(t *testing.T) {
	trade := &kalshi.ResearchReplayTrade{TakerOutcomeSide: "yes", TakerBookSide: "ask"}
	if got := step7AuthoritativeDirection(trade); got != "conflict" {
		t.Fatalf("direction=%q", got)
	}
	if got := step7Comparison("conflict", "yes"); got != "authoritative_conflict" {
		t.Fatalf("comparison=%q", got)
	}
}

func TestStep7ReplenishmentActionRuleSeparatesTradePersistenceFromRefillAndCancelProxy(t *testing.T) {
	base := storage.Step7ReplenishmentMark{EventType: "horizon", HorizonMS: 5000, TradeUnits: 8, UnexplainedUnits: 2}
	if ok, reason := step7ReplenishmentActionRule(base); !ok || reason != "frozen_persistent_authoritative_depletion_rule_met" {
		t.Fatalf("eligible=%v %q", ok, reason)
	}
	refill := base
	refill.Refilled = true
	if ok, reason := step7ReplenishmentActionRule(refill); ok || reason != "refilled_before_horizon" {
		t.Fatalf("refill=%v %q", ok, reason)
	}
	proxy := base
	proxy.TradeUnits = 2
	proxy.UnexplainedUnits = 8
	if ok, reason := step7ReplenishmentActionRule(proxy); ok || reason != "cancel_or_nontrade_proxy_dominates_depletion" {
		t.Fatalf("proxy=%v %q", ok, reason)
	}
}
