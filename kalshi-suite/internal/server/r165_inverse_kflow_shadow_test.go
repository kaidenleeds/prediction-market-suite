package server

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR165InverseKflowShadowAllowlistIsExactAndMoneySeparate(t *testing.T) {
	for _, tc := range []struct {
		raw, canonical string
		valid          bool
	}{
		{"", "", true},
		{" Kalshi|INVERT:kalshi-flow|no|TAKER ", inverseKflowShadowIdentity, true},
		{"kalshi|kalshi-flow|NO|taker", "", false},
		{"kalshi|invert:kalshi-flow|NO|taker,kalshi|spotlag|NO|taker", "", false},
		{"kalshi|invert:kalshi-flow|YES|taker", "", false},
	} {
		got, why := parseLiveSystemShadowAllowlist(tc.raw)
		if (why == "") != tc.valid || got != tc.canonical {
			t.Fatalf("parse %q = (%q,%q), valid=%v", tc.raw, got, why, tc.valid)
		}
	}
	work := livePolicyMirrorWork{ExperimentKind: inverseKflowShadowExperimentKind,
		Reason: "", Preflighted: true}
	if livePolicyMirrorPriorityWork(work) {
		t.Fatal("money-free shadow work entered the LIVE-priority/cash-comparison lane")
	}
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = ""
	cfg.Risk.LiveSystemCanaryAllowlist = ""
	cfg.Risk.LiveSystemShadowAllowlist = inverseKflowShadowIdentity
	s.cfgP.Store(&cfg)
	if !s.inverseKflowShadowEnabled() {
		t.Fatal("exact shadow identity did not enable the money-free experiment")
	}
	candidate := liveMirrorCandidate{Platform: "kalshi", Ticker: "KX-R165",
		Side: "NO", Family: "invert:kalshi-flow", Source: inverseKflowShadowSignalSource}
	if why := s.liveSystemSelectionReason(candidate, "taker"); why == "" {
		t.Fatal("shadow allowlist accidentally granted direct LIVE authority")
	}
	if r163SettingsPublishesLiveMoneyPolicy(map[string]any{
		"live_system_shadow_allowlist": inverseKflowShadowIdentity,
	}) {
		t.Fatal("research-only shadow setting rotated the LIVE money-policy generation")
	}
	t0 := time.Now().UTC()
	if !s.claimInverseKflowShadowEpisode("KX-R165-EPISODE", t0) ||
		s.claimInverseKflowShadowEpisode("KX-R165-EPISODE", t0.Add(29*time.Second)) ||
		!s.claimInverseKflowShadowEpisode("KX-R165-EPISODE", t0.Add(31*time.Second)) {
		t.Fatal("shadow dedup did not use independent 30-second economic episodes")
	}
}

func TestR165InverseKflowShadowAcceptsOnlyOriginalYESAndClearsCopiedPrice(t *testing.T) {
	at := time.Now().UTC().Add(-73 * time.Millisecond)
	original := storage.Signal{Platform: "kalshi", Ticker: "KX-R165", Title: "fixture",
		Side: "YES", SignalType: "kalshi-flow", EntryPrice: .62, ResolveHours: 1,
		ExecExpr: r147InputReceiptExpr("K", at)}
	inverse, ok := inverseKflowShadowSignal(original)
	if !ok || inverse.SignalType != "invert:kalshi-flow" || inverse.Side != "NO" {
		t.Fatalf("wrong inverse identity: %#v ok=%v", inverse, ok)
	}
	if inverse.EntryPrice != 0 || inverse.BookAsk != nil || inverse.FeePC != nil {
		t.Fatalf("inverse copied emitted-side execution data: %#v", inverse)
	}
	if got := r147SignalInputObservedAt(inverse); !got.Equal(at) {
		t.Fatalf("original source clock lost: got=%s want=%s expr=%q", got, at, inverse.ExecExpr)
	}
	original.Side = "NO"
	if _, ok := inverseKflowShadowSignal(original); ok {
		t.Fatal("ordinary Kalshi-flow NO entered the original-YES inverse experiment")
	}
}

func TestR165FullBookEvidenceLogsBothSidesAndEveryReturnedLevel(t *testing.T) {
	checked := time.Now().UTC()
	received := checked.Add(-37 * time.Millisecond)
	book := &kalshi.Orderbook{Ticker: "KX-R165",
		YesBids: []kalshi.OrderbookLevel{{Price: .60, Size: 9}, {Price: .59, Size: 4}},
		YesAsks: []kalshi.OrderbookLevel{{Price: .63, Size: 7}, {Price: .64, Size: 3}}}
	e := fullKalshiBookEvidence(book, kalshi.BookProvenance{Channel: "orderbook_delta",
		Generation: 3, SubscriptionID: 8, Sequence: 55,
		SourceAt: received.Add(-time.Millisecond), ReceivedAt: received}, checked)
	yes := e["yes_top"].(map[string]any)
	no := e["no_top"].(map[string]any)
	if yes["bid"] != .60 || yes["ask"] != .63 || no["bid"] != .37 || no["ask"] != .40 {
		t.Fatalf("wrong two-sided top book: yes=%v no=%v", yes, no)
	}
	if got := (no["ask"].(float64) - no["bid"].(float64)) * 100; math.Abs(got-3) > 1e-9 {
		t.Fatalf("wrong NO-side spread: got=%v cents want=3", got)
	}
	if len(e["yes_bid_levels"].([]map[string]any)) != 2 ||
		len(e["yes_ask_levels"].([]map[string]any)) != 2 ||
		len(e["no_bid_levels"].([]map[string]any)) != 2 ||
		len(e["no_ask_levels"].([]map[string]any)) != 2 {
		t.Fatalf("full returned ladder was not retained: %#v", e)
	}
	if e["generation"] != uint64(3) || e["subscription_id"] != int64(8) ||
		e["sequence"] != int64(55) || e["age_ms"].(float64) < 36 {
		t.Fatalf("book provenance missing: %#v", e)
	}
	// The independent NO ask is 40c from the actual opposite book, not 1-.62=38c.
	filled, why := livePolicyMirrorIOC(no["ask"].(float64), 1,
		liveMirrorQuote{Price: .39, Depth: 2})
	if why != "" || filled != 1 {
		t.Fatalf("q1 IOC rule did not use the actual NO limit: fill=%v why=%q", filled, why)
	}
}

func TestR165InverseKflowShadowAttemptHasNoCashOrVenueAuthorityAndDedupsDurably(t *testing.T) {
	s := testServer(t)
	release := holdExecutionShadowWriterStart(t, s)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.stopExecutionShadowWriter(ctx)
	})
	at := time.Now().UTC().Add(-25 * time.Millisecond)
	sourceAt := at.Add(-17 * time.Millisecond)
	work := livePolicyMirrorWork{
		Signal: storage.Signal{Platform: "kalshi", Ticker: "KX-R165-ATTEMPT", Title: "fixture",
			Side: "NO", SignalType: "invert:kalshi-flow", ResolveHours: 1,
			ExecExpr: "independent-inverse-system/base=kalshi-flow/emitted=YES/" +
				r147InputReceiptExpr("K", sourceAt)},
		Point: .62, SignalAt: at,
		Candidate: liveMirrorCandidate{Platform: "kalshi", Ticker: "KX-R165-ATTEMPT",
			Title: "fixture", Side: "NO", Action: "BUY", Family: "invert:kalshi-flow",
			Source: inverseKflowShadowSignalSource, At: at, InputObservedAt: sourceAt},
		ExperimentKind: inverseKflowShadowExperimentKind,
	}
	contract, bindWhy, declared := gfSignalContractBinding(work.Signal)
	if !declared || bindWhy != "" {
		t.Fatalf("inverse fixture did not bind its static contract: declared=%t reason=%q", declared, bindWhy)
	}
	wantContractID := r147SignalContractIdentity(contract)
	c, ok := s.beginInverseKflowShadowAttempt(work)
	if !ok || c.ShadowAttemptID == "" {
		t.Fatal("shadow-only attempt was not admitted")
	}
	s.recordInverseKflowShadowTerminal(c, inverseKflowShadowBook{}, inverseKflowShadowBook{},
		livePolicyMirrorNotObserved, "fixture-no-book", false)
	release()
	deadline := time.Now().Add(3 * time.Second)
	for {
		views, err := s.store.ListExecutionShadowAttempts(context.Background(), 10)
		if err == nil && len(views) == 1 && len(views[0].Events) >= 5 {
			attempt := views[0].Attempt
			if attempt.SystemID != "invert:kalshi-flow" || attempt.Side != "NO" ||
				attempt.SignalSource != inverseKflowShadowSignalSource ||
				attempt.SignalContract != wantContractID || attempt.InputTopology != "K" ||
				attempt.TriggerUnixMS != at.UnixMilli() ||
				!attempt.InputObservedAt.Equal(sourceAt) {
				t.Fatalf("wrong immutable attempt: %#v", attempt)
			}
			foundDetectorEvidence, foundLiveNoSend := false, false
			foundPaperBranch, foundPaperTerminal, foundShadowTerminal := false, false, false
			var paperTerminalEvent storage.ExecutionShadowEvent
			for _, event := range views[0].Events {
				if event.VenueAttempted || (event.LiveState != "" && event.LiveState != "not-sent") || event.LiveReservationID != "" ||
					event.VenueOrderID != "" || event.LiveFilledQty != nil {
					t.Fatalf("shadow experiment acquired cash/venue fields: %#v", event)
				}
				if event.Stage == "inverse-kflow-shadow-detector" {
					foundDetectorEvidence = true
					if event.Evidence["decision_emitted_unix_ms"] != float64(at.UnixMilli()) ||
						event.Evidence["source_trade_received_unix_ms"] != float64(sourceAt.UnixMilli()) ||
						event.Evidence["source_to_decision_ms"] != float64(17) ||
						event.Evidence["forward_generation_id"] != r168ForwardGenerationID ||
						event.Evidence["signal_contract_id"] != wantContractID ||
						event.Evidence["execution_model_version"] != inverseKflowShadowModelVersion {
						t.Fatalf("source and decision clocks were not kept separate: %#v", event.Evidence)
					}
				}
				if event.Stage == "live-selection" && event.LiveState == "not-sent" {
					foundLiveNoSend = true
				}
				if event.Stage == "funded-paper-branch" && event.Outcome == "not-offered" {
					foundPaperBranch = true
				}
				if event.Stage == "funded-paper-terminal" && event.Outcome == "not_observed" &&
					event.PaperState == "PAPER-NOT-OBSERVED" {
					foundPaperTerminal = true
					paperTerminalEvent = event
					if event.PaperAttemptID != "" || event.PaperFilledQty != nil ||
						event.PaperFillPrice != nil || event.PaperFee != nil || event.PaperNet != nil {
						t.Fatalf("Paper non-participation invented economics: %#v", event)
					}
				}
				if event.Stage == "counterfactual-execution-terminal" {
					foundShadowTerminal = true
				}
			}
			if !foundDetectorEvidence {
				t.Fatal("detector clock evidence missing")
			}
			if !foundLiveNoSend {
				t.Fatal("money-free inverse attempt lacks explicit LIVE no-send receipt")
			}
			if !foundPaperBranch || !foundPaperTerminal || !foundShadowTerminal {
				t.Fatalf("canonical lane closure missing: paperBranch=%v paperTerminal=%v shadowTerminal=%v events=%+v",
					foundPaperBranch, foundPaperTerminal, foundShadowTerminal, views[0].Events)
			}
			if !paperTerminalEvent.At.Equal(at) || paperTerminalEvent.ElapsedFromTriggerMS != 0 {
				t.Fatalf("Paper non-participation did not retain the trigger clock: %#v", paperTerminalEvent)
			}
			replay := inverseKflowPaperNonParticipationEvent(s, c.ShadowAttemptID,
				"funded-paper-terminal", "not_observed", at)
			replay.PaperState = "PAPER-NOT-OBSERVED"
			if replay.EventID != paperTerminalEvent.EventID {
				t.Fatalf("Paper terminal replay id changed: got=%s want=%s",
					replay.EventID, paperTerminalEvent.EventID)
			}
			if inserted, replayErr := s.store.AppendExecutionShadowEvent(context.Background(), replay); replayErr != nil || inserted {
				t.Fatalf("stable Paper terminal replay inserted=%v err=%v", inserted, replayErr)
			}
			fills, fillErr := s.store.ListPaperFills(context.Background())
			if fillErr != nil || len(fills) != 0 {
				t.Fatalf("shadow-only inverse created funded Paper fills: fills=%v err=%v", fills, fillErr)
			}
			funnel, funnelErr := s.store.ExecutionShadowProfitFunnel(context.Background(),
				storage.ProfitFunnelQuery{Systems: []string{"invert:kalshi-flow"}})
			if funnelErr != nil {
				t.Fatal(funnelErr)
			}
			if funnel.Totals.Detected != 1 || funnel.Totals.Live.NotSent != 1 ||
				funnel.Totals.Paper.NotObserved != 1 || funnel.Totals.Shadow.NotObserved != 1 ||
				funnel.Totals.Gaps.LiveTerminalMissing != 0 ||
				funnel.Totals.Gaps.PaperBranchMissing != 0 ||
				funnel.Totals.Gaps.PaperTerminalMissing != 0 ||
				funnel.Totals.Gaps.ShadowBranchMissing != 0 ||
				funnel.Totals.Gaps.ShadowTerminalMissing != 0 ||
				funnel.Totals.Exclusivity.LiveGap != 0 ||
				funnel.Totals.Exclusivity.PaperGap != 0 ||
				funnel.Totals.Exclusivity.ShadowGap != 0 {
				t.Fatalf("inverse canonical funnel did not close every lane: %+v", funnel.Totals)
			}
			found, err := s.store.HasRecentExecutionShadowAttempt(context.Background(), "kalshi",
				attempt.Ticker, "NO", "invert:kalshi-flow", inverseKflowShadowSignalSource,
				inverseKflowShadowQualification, at.Add(-liveMirrorDedup))
			if err != nil || !found {
				t.Fatalf("durable one-contract dedup missing: found=%v err=%v", found, err)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("shadow rows not committed: views=%v err=%v", views, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestR165InverseKflowShadowDedupIgnoresOlderUnrelatedInverseRoutes(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	insert := func(id, source, qualification string, at time.Time) {
		t.Helper()
		if _, err := s.store.InsertExecutionShadowAttempt(ctx, storage.ExecutionShadowAttempt{
			AttemptID: id, SignalDecisionID: "decision-" + id, ObservedAt: at,
			TriggerUnixMS: at.UnixMilli(), Venue: "kalshi", Ticker: "KX-R165-SCOPE",
			Side: "NO", Action: "BUY", SystemID: "invert:kalshi-flow", Route: "taker",
			SignalSource: source, QualificationBasis: qualification,
		}); err != nil {
			t.Fatal(err)
		}
	}
	insert("exec-r165-unrelated", "auto-cons-invert:kalshi-flow", "historical-model-route", now)
	found, err := s.store.HasRecentExecutionShadowAttempt(ctx, "kalshi", "KX-R165-SCOPE", "NO",
		"invert:kalshi-flow", inverseKflowShadowSignalSource,
		inverseKflowShadowQualification, now.Add(-liveMirrorDedup))
	if err != nil || found {
		t.Fatalf("unrelated inverse route suppressed R165 cohort: found=%v err=%v", found, err)
	}
	insert("exec-r165-exact-old", inverseKflowShadowSignalSource,
		inverseKflowShadowQualification, now.Add(-time.Hour))
	found, err = s.store.HasRecentExecutionShadowAttempt(ctx, "kalshi", "KX-R165-SCOPE", "NO",
		"invert:kalshi-flow", inverseKflowShadowSignalSource,
		inverseKflowShadowQualification, now.Add(-liveMirrorDedup))
	if err != nil || found {
		t.Fatalf("older exact episode suppressed current R165 cohort: found=%v err=%v", found, err)
	}
	insert("exec-r165-exact-current", inverseKflowShadowSignalSource,
		inverseKflowShadowQualification, now)
	found, err = s.store.HasRecentExecutionShadowAttempt(ctx, "kalshi", "KX-R165-SCOPE", "NO",
		"invert:kalshi-flow", inverseKflowShadowSignalSource,
		inverseKflowShadowQualification, now.Add(-liveMirrorDedup))
	if err != nil || !found {
		t.Fatalf("exact R165 route did not dedup durably: found=%v err=%v", found, err)
	}
}

func TestR165InverseKflowShadowRunsDisarmedWithoutPublishingLive(t *testing.T) {
	const ticker = "KX-R165-DISARMED"
	s, _, quoteFor := r154PaperTakerFixture(t, ticker)
	cfg := *s.cfg()
	cfg.Risk.LiveSystemAllowlist = ""
	cfg.Risk.LiveSystemCanaryAllowlist = ""
	cfg.Risk.LiveSystemShadowAllowlist = inverseKflowShadowIdentity
	s.cfgP.Store(&cfg)
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		return quoteFor(in, .59, .60, 20)
	}
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = false, false
	s.liveMu.Unlock()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.stopLivePolicyMirrorWorker(ctx)
		_ = s.stopExecutionShadowWriter(ctx)
	})
	at := time.Now().UTC()
	original := storage.Signal{Platform: "kalshi", Ticker: ticker,
		Title: "fixture", Side: "YES", SignalType: "kalshi-flow", EntryPrice: .31,
		ResolveHours: 1, ExecExpr: r147InputReceiptExpr("K", at)}
	if !s.queueInverseKflowShadow(original, at) {
		t.Fatal("disarmed money-free shadow was not queued")
	}
	if s.liveOperatorAutoOn() || len(s.liveSignalIntentChannel()) != 0 {
		t.Fatal("shadow-only work armed or published a LIVE intent")
	}
	deadline := time.Now().Add(4 * time.Second)
	for {
		views, err := s.store.ListExecutionShadowAttempts(context.Background(), 10)
		if err == nil && len(views) == 1 {
			if views[0].Attempt.SystemID != "invert:kalshi-flow" ||
				views[0].Attempt.TriggerUnixMS != at.UnixMilli() {
				t.Fatalf("wrong disarmed shadow attempt: %#v", views[0].Attempt)
			}
			foundLiveNoSend := false
			foundGenericDetector := false
			for _, event := range views[0].Events {
				if event.VenueAttempted || (event.LiveState != "" && event.LiveState != "not-sent") || event.VenueOrderID != "" {
					t.Fatalf("disarmed shadow reached LIVE fields: %#v", event)
				}
				foundLiveNoSend = foundLiveNoSend ||
					(event.Stage == "live-selection" && event.LiveState == "not-sent")
				foundGenericDetector = foundGenericDetector || event.Stage == "detector-branch"
			}
			if !foundLiveNoSend {
				t.Fatal("disarmed shadow lacks explicit LIVE no-send receipt")
			}
			if !foundGenericDetector {
				t.Fatal("retired dedicated producer did not route through the generic detector fanout")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("disarmed shadow attempt not persisted: views=%v err=%v", views, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestR165InverseKflowShadowFillSettlesAsNOExposure(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Millisecond)
	attemptID := "exec-r165-inverse-settlement"
	if _, err := st.InsertExecutionShadowAttempt(ctx, storage.ExecutionShadowAttempt{
		AttemptID: attemptID, SignalDecisionID: "decision-r165-inverse-settlement",
		ObservedAt: at, TriggerUnixMS: at.UnixMilli(), Venue: "kalshi",
		Ticker: "KX-R165-SETTLE", Side: "NO", Action: "BUY",
		SystemID: "invert:kalshi-flow", Route: "taker",
		SignalSource:       "shadow-only:original-kalshi-flow-YES",
		QualificationBasis: "fixture",
	}); err != nil {
		t.Fatal(err)
	}
	s.executionShadowRememberTrigger(attemptID, at.UnixMilli())
	c := liveMirrorCandidate{Platform: "kalshi", Ticker: "KX-R165-SETTLE",
		Side: "NO", Action: "BUY", Family: "invert:kalshi-flow", At: at,
		ShadowAttemptID: attemptID}
	initial := inverseKflowShadowBook{quote: liveMirrorQuote{Price: .40, Depth: 3,
		Tick: .01, BookSource: "kalshi_ws_full_orderbook:g1:s2:q3", ObservedAt: at,
		CheckedAt: at}, fee: .02, feeKnown: true, feeSource: "kalshi-exact-resident:quadratic"}
	final := initial
	final.quote.BookSource = "kalshi_ws_full_orderbook:g1:s2:q4"
	s.recordInverseKflowShadowTerminal(c, initial, final, livePolicyMirrorModeledFill, "", true)
	deadline := time.Now().Add(3 * time.Second)
	for {
		rows, err := st.ListExecutionShadowAttempts(ctx, 5)
		if err == nil && len(rows) == 1 && len(rows[0].Events) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fill terminal not persisted: rows=%v err=%v", rows, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if inserted, err := st.RecordVenueSettlement(ctx, "kalshi", c.Ticker, 0,
		at.Add(time.Hour), "fixture-final"); err != nil || !inserted {
		t.Fatalf("settlement inserted=%v err=%v", inserted, err)
	}
	s.settleExecutionShadowLedger(ctx)
	drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := s.stopExecutionShadowWriter(drainCtx); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	rows, err := st.ListExecutionShadowAttempts(ctx, 5)
	if err != nil || len(rows) != 1 {
		t.Fatalf("settled rows=%d err=%v", len(rows), err)
	}
	for _, event := range rows[0].Events {
		if event.ShadowNet != nil {
			if got, want := *event.ShadowNet, .58; got < want-1e-9 || got > want+1e-9 {
				t.Fatalf("NO shadow net=%v want=%v", got, want)
			}
			return
		}
	}
	t.Fatal("NO shadow fill did not enter settlement lane")
}
