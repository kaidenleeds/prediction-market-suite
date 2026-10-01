package server

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR139PromotionBridgeFailsClosedWithoutSealedAcceptedPaperIntent(t *testing.T) {
	s := testServer(t)
	fake := "r139p:" + strings.Repeat("a", 64)
	if route, ok := s.researchPromotionRouteForSource(context.Background(), fake); ok || route != "" {
		t.Fatalf("forged source acquired route=%q ok=%v", route, ok)
	}
	c := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXFAKE", Side: "YES", Source: fake, Price: .4}
	if ok, basis, _, _, _ := s.liveMirrorProof(context.Background(), c, .4, false); ok || basis == "" {
		t.Fatalf("forged source acquired LIVE proof: ok=%v basis=%q", ok, basis)
	}
	proofs, err := s.store.CurrentResearchPromotionProofs(context.Background())
	if err != nil || len(proofs) != 0 {
		t.Fatalf("fresh store promotion proofs=%+v err=%v", proofs, err)
	}
	for _, id := range storage.SystemIDs() {
		spec, ok := storage.SystemExecutionCapability(id)
		if !ok || spec.ActionClass == "" || spec.HandoffClass == "" {
			t.Fatalf("%s lacks fail-closed handoff metadata", id)
		}
	}
}

func TestR139PromotionPaperCostGateRunsBeforeAnyRouteReceipt(t *testing.T) {
	in := storage.ResearchPromotionIntent{MaxAllInUnit: .415}
	if !researchPromotionPaperCostAllowed(in, 1, .40, .01) {
		t.Fatal("valid exact one-unit Paper route was rejected")
	}
	if researchPromotionPaperCostAllowed(in, 1, .41, .01) ||
		researchPromotionPaperCostAllowed(in, 2, .20, .01) {
		t.Fatal("over-cap or resized Paper route bypassed immutable ceiling")
	}
}

func TestR142SystemCandidateSourceCarriesRouteWithoutForgingLiveAuthority(t *testing.T) {
	c := storage.ResearchPromotionCandidate{ObservationID: 42, SystemID: "clientele-clock-basis", Route: "maker"}
	source := researchPaperExplorationSource(c)
	if route, ok := researchPaperExplorationRoute(source); !ok || route != "maker" {
		t.Fatalf("paper exploration source route=%q ok=%v source=%q", route, ok, source)
	}
	if storage.ResearchPromotionIntentFromSource(source) != "" {
		t.Fatal("Paper exploration source was misclassified as sealed promotion authority")
	}
	if systemID, ok := researchPaperExplorationSystem(source); !ok || systemID != c.SystemID {
		t.Fatalf("Paper exploration source lost System attribution: system=%q ok=%v", systemID, ok)
	}
	if got := liveMirrorFamily(liveMirrorCandidate{Source: source}); got != c.SystemID {
		t.Fatalf("Paper maker/fill attribution=%q, want %q", got, c.SystemID)
	}
	legacy := "r142x:maker:41:3f829cd2"
	if route, ok := researchPaperExplorationRoute(legacy); !ok || route != "maker" {
		t.Fatalf("legacy route parsing regressed: route=%q ok=%v", route, ok)
	}
	if systemID, ok := researchPaperExplorationSystem(legacy); ok || systemID != "" {
		t.Fatalf("legacy hash manufactured System attribution: system=%q ok=%v", systemID, ok)
	}
	for _, bad := range []string{"", "r142x:rfq:42:x", "r142x:maker:x"} {
		if _, ok := researchPaperExplorationRoute(bad); ok {
			t.Fatalf("invalid exploration source accepted: %q", bad)
		}
	}
}

func TestR145NamedInverseExplorationKeepsItsOwnFamilyAndExactPositiveRoute(t *testing.T) {
	s := testServer(t)
	candidate := storage.ResearchPromotionCandidate{ObservationID: 51,
		SystemID: "paired-bridge-inversion", StrategyFamily: "invert:kalshi-flow",
		FiredSide: "YES", Venue: "kalshi", Ticker: "KXINV", Side: "NO", Route: "taker"}
	source := researchPaperExplorationSource(candidate)
	if got := researchPaperCandidateFamily(candidate); got != "invert:kalshi-flow" {
		t.Fatalf("inverse candidate family=%q", got)
	}
	if got, ok := researchPaperExplorationSystem(source); !ok || got != "invert:kalshi-flow" {
		t.Fatalf("inverse source attribution=%q ok=%v source=%q", got, ok, source)
	}
	s.verdMu.Lock()
	s.verdCache = []verdictEnt{
		{SourceFamily: "invert:kalshi-flow", Platform: "polyus", Side: "NO", Route: "taker",
			Group: "taker", N: 100, Markets: 100, Mean: .90, MeanAsk: .30, FeePC: .01,
			ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
		{SourceFamily: "invert:kalshi-flow", Platform: "kalshi", Side: "YES", Route: "taker",
			Group: "taker", N: 90, Markets: 90, Mean: .80, MeanAsk: .30, FeePC: .01,
			ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
		{SourceFamily: "invert:kalshi-flow", Platform: "kalshi", Side: "NO", Route: "maker",
			Group: "maker", N: 80, Markets: 80, Mean: .70, MeanAsk: .30, FeePC: .01,
			ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
		{SourceFamily: "invert:kalshi-flow", Platform: "kalshi", Side: "NO", Route: "taker",
			Group: "taker", N: 12, Markets: 12, Mean: .04, MeanAsk: .40, FeePC: .01,
			ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
	}
	s.verdAt = time.Now()
	s.verdMu.Unlock()
	point, baseline, samples, ok := s.researchPaperExactPoint(candidate)
	if !ok || math.Abs(point-.04) > 1e-12 || math.Abs(baseline-.41) > 1e-12 || samples != 12 {
		t.Fatalf("exact inverse point=%v baseline=%v samples=%d ok=%v", point, baseline, samples, ok)
	}
	bad := candidate
	bad.FiredSide = "NO" // no longer an actual opposite-side action
	if got := researchPaperCandidateFamily(bad); got != "paired-bridge-inversion" {
		t.Fatalf("invalid inverse lineage retained named family: %q", got)
	}
	bad = candidate
	bad.SystemID = "proper-score-executor"
	if got := researchPaperCandidateFamily(bad); got != "proper-score-executor" {
		t.Fatalf("non-inverse System borrowed named inverse attribution: %q", got)
	}
}

func TestR145ResearchPaperExactPointUsesDeterministicSampleFirstRoute(t *testing.T) {
	s := testServer(t)
	candidate := storage.ResearchPromotionCandidate{SystemID: "proper-score-executor",
		Venue: "kalshi", Ticker: "KXPOINT", Side: "YES", Route: "taker"}
	s.verdMu.Lock()
	s.verdCache = []verdictEnt{
		{SourceFamily: "proper-score-executor", Platform: "kalshi", Side: "YES", Route: "taker",
			Group: "strategy", N: 200, Markets: 200, Mean: .20, MeanAsk: .30, FeePC: .01,
			ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
		{SourceFamily: "proper-score-executor", Platform: "kalshi", Side: "YES", Route: "taker",
			Group: "taker", N: 1, Markets: 1, Mean: .90, MeanAsk: .20, FeePC: .01,
			ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
		{SourceFamily: "proper-score-executor", Platform: "kalshi", Side: "YES", Route: "taker",
			Group: "taker", N: 50, Markets: 50, Mean: .04, MeanAsk: .40, FeePC: .01,
			ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
		{SourceFamily: "proper-score-executor", Platform: "kalshi", Side: "YES", Route: "taker",
			Group: "taker", N: 50, Markets: 50, Mean: .03, MeanAsk: .42, FeePC: .01,
			ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
	}
	s.verdAt = time.Now()
	s.verdMu.Unlock()
	point, baseline, samples, ok := s.researchPaperExactPoint(candidate)
	if !ok || math.Abs(point-.03) > 1e-12 || math.Abs(baseline-.43) > 1e-12 || samples != 50 {
		t.Fatalf("sample-first point=%v baseline=%v samples=%d ok=%v", point, baseline, samples, ok)
	}
}

func TestR145AllRegisteredSystemsDeclareExecutionHandoff(t *testing.T) {
	ids := storage.SystemIDs()
	if len(ids) != 19 {
		t.Fatalf("registered systems=%d, want 19", len(ids))
	}
	for _, id := range ids {
		spec, ok := storage.SystemExecutionCapability(id)
		if !ok {
			t.Fatalf("%s missing execution capability", id)
		}
		if spec.SystemID != id || spec.ActionClass == "" || spec.HandoffClass == "" ||
			spec.TriggerSource == "" || spec.PaperHandoff == "" || spec.LiveHandoff == "" ||
			spec.SettlementPath == "" {
			t.Fatalf("%s incomplete execution capability: %+v", id, spec)
		}
		adapter, reason := researchSystemDispatchPlan(id)
		if adapter == "" || reason == "" ||
			(adapter == researchSystemDispatchUnsupported && len(spec.UnsupportedReasons) == 0) {
			t.Fatalf("%s ambiguous dispatch adapter=%q reason=%q spec=%+v", id, adapter, reason, spec)
		}
	}
}

func TestR145PersonaSelectedActionUsesSharedExecutor(t *testing.T) {
	candidate := storage.ResearchPromotionCandidate{ObservationID: 44,
		SystemID: "forecast-persona-router", Venue: "kalshi", Ticker: "KXPERSONA",
		Title: "persona selected contract", Side: "YES", Route: "taker", ObservedPrice: .41}
	called := 0
	accepted, reason := dispatchResearchSystemCandidateWith(context.Background(), candidate,
		researchPaperExplorationSource(candidate), func(_ context.Context, venue, ticker, title, side string,
			price, signalPrice float64, _ int, _ int, source string) bool {
			called++
			if venue != "kalshi" || ticker != "KXPERSONA" || title != "persona selected contract" ||
				side != "YES" || price != .41 || signalPrice != .41 {
				t.Fatalf("wrong shared-executor action: %s %s %s %s %.3f %.3f", venue, ticker, title, side, price, signalPrice)
			}
			if route, ok := researchPaperExplorationRoute(source); !ok || route != "taker" {
				t.Fatalf("source=%q route=%q ok=%v", source, route, ok)
			}
			return true
		})
	if !accepted || reason != "shared-single:accepted" || called != 1 {
		t.Fatalf("accepted=%v reason=%q calls=%d", accepted, reason, called)
	}
}

func TestR145SystemActionClassCannotMasqueradeAsSingleOrder(t *testing.T) {
	for _, tc := range []struct {
		name, system, venue, route string
	}{
		{"native bundle", "payoff-constraint-solver", "kalshi", "taker"},
		{"maker overlay", "maker-salvage-matched-cohort", "kalshi", "maker"},
		{"unsupported venue", "proper-score-executor", "polymarket", "taker"},
		{"unsupported route", "proper-score-executor", "kalshi", "rfq"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := storage.ResearchPromotionCandidate{ObservationID: 45, SystemID: tc.system,
				Venue: tc.venue, Ticker: "X", Side: "YES", Route: tc.route, ObservedPrice: .4}
			called := false
			accepted, reason := dispatchResearchSystemCandidateWith(context.Background(), candidate, "test-source",
				func(context.Context, string, string, string, string, float64, float64, int, int, string) bool {
					called = true
					return true
				})
			if accepted || called || reason == "" {
				t.Fatalf("accepted=%v called=%v reason=%q", accepted, called, reason)
			}
		})
	}
}

func TestR145SharedExecutorRejectionRemainsRejection(t *testing.T) {
	candidate := storage.ResearchPromotionCandidate{ObservationID: 46,
		SystemID: "proper-score-executor", Venue: "polyus", Ticker: "market-1",
		Side: "NO", Route: "taker", ObservedPrice: .37}
	accepted, reason := dispatchResearchSystemCandidateWith(context.Background(), candidate, "test-source",
		func(context.Context, string, string, string, string, float64, float64, int, int, string) bool {
			return false
		})
	if accepted || reason != "shared-single:normal-executor-rejected" {
		t.Fatalf("accepted=%v reason=%q", accepted, reason)
	}
}

func TestR166CanonicalResearchSinglesBindOnlyToDelayedRealisticPaper(t *testing.T) {
	s := testServer(t)
	checked := 0
	for _, spec := range storage.SystemExecutionSpecs() {
		if spec.HandoffClass != storage.SystemHandoffSingle {
			continue
		}
		for _, variant := range spec.ExactVariants() {
			contracts := r147ExactSignalContracts(spec.SystemID, variant.Venue,
				variant.Side, variant.Route)
			if len(contracts) == 0 {
				t.Fatalf("%s %s/%s/%s has no exact delayed-Paper contract",
					spec.SystemID, variant.Venue, variant.Side, variant.Route)
			}
			for _, contract := range contracts {
				candidate := storage.ResearchPromotionCandidate{ObservationID: int64(1000 + checked),
					Observed: time.Now().UTC(), SystemID: spec.SystemID, Venue: variant.Venue,
					Ticker: "R166-RESEARCH", Title: "R166 research candidate",
					Side: variant.Side, Route: variant.Route, ObservedPrice: .40, ObservedFeePC: .01}
				if strings.EqualFold(spec.SystemID, "proper-score-executor") {
					parts := strings.Split(strings.ToLower(contract.InputTopology), "-proper-")
					if len(parts) != 2 {
						t.Fatalf("proper-score contract lacks transform topology: %+v", contract)
					}
					candidate.Cohort = "proper-score-v2|transform=" + parts[1] + "|fixture=true"
				}
				signal, why := researchCandidateDelayedPaperSignal(candidate)
				if why != "" {
					t.Fatalf("%s %s/%s/%s input=%s did not bind to delayed Paper: %s",
						spec.SystemID, variant.Venue, variant.Side, variant.Route,
						contract.InputTopology, why)
				}
				if signal.SignalType != spec.SystemID || signal.Platform != variant.Venue ||
					signal.Side != variant.Side || signal.PricingVersion != fundedPaperCorrectedExecutionGenerationV1 ||
					r147SignalInputTopology(signal) != contract.InputTopology {
					t.Fatalf("candidate identity changed in delayed handoff: contract=%+v signal=%+v",
						contract, signal)
				}
				checked++
			}
		}
	}
	if checked < 40 {
		t.Fatalf("only %d canonical exact single-order variants reached the delayed handoff", checked)
	}
	var immediate int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM paper_fills`).Scan(&immediate); err != nil {
		t.Fatal(err)
	}
	if immediate != 0 {
		t.Fatalf("candidate binding created %d immediate synthetic paper_fills", immediate)
	}
}

func TestR166DelayedPaperWorkerReturnsExplicitTerminalToResearchCallback(t *testing.T) {
	s := testServer(t)
	in := make(chan genfollowPaperIntent, 1)
	done := make(chan struct{})
	terminal := make(chan genfollowPaperTerminalResult, 1)
	old := time.Now().UTC().Add(-genfollowPaperSignalMaxAge - time.Second)
	in <- genfollowPaperIntent{EnqueuedAt: old, Signals: []genfollowPaperSignal{{
		Signal: storage.Signal{Platform: "kalshi", Ticker: "R166-EXPIRED", Side: "YES",
			SignalType: "proper-score-executor"},
		SignalAt: old,
		onTerminal: func(_ context.Context, result genfollowPaperTerminalResult) {
			terminal <- result
		},
	}}}
	close(in)
	go s.runGenfollowPaperWorkers(context.Background(), in, done)
	select {
	case result := <-terminal:
		if result.State != "PAPER-NOT-OBSERVED" || result.Reason != "paper-original-signal-expired" {
			t.Fatalf("delayed research terminal=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("delayed Paper worker did not terminalize the queued research candidate")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("delayed Paper worker did not drain")
	}
	var immediate int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM paper_fills`).Scan(&immediate); err != nil {
		t.Fatal(err)
	}
	if immediate != 0 {
		t.Fatalf("not-observed candidate created %d immediate synthetic fills", immediate)
	}
}

func TestR139SealedOneUnitNeverResizesToPolyUSMinimum(t *testing.T) {
	if count, why := liveMirrorApplyPolyUSMinimum(1, 2, .41, 10, true, true); count != 0 ||
		why != "sealed-one-unit-proof-below-venue-minimum" {
		t.Fatalf("sealed count=%v why=%q", count, why)
	}
	if count, why := liveMirrorApplyPolyUSMinimum(1, .01, .41, 10, true, true); count != 1 || why != "" {
		t.Fatalf("fractional-min sealed count=%v why=%q", count, why)
	}
	if _, why := liveMirrorApplyPolyUSMinimum(1, 0, .41, 10, false, true); why == "" {
		t.Fatal("sealed PolyUS route accepted without current minimum receipt")
	}
	if liveMirrorFullDepthAllows(false, .99, 1) || liveMirrorFullDepthAllows(false, 2, 3) ||
		!liveMirrorFullDepthAllows(false, 3, 3) || !liveMirrorFullDepthAllows(true, .25, 1) {
		t.Fatal("full-book depth guard did not separate taker consumption from a maker queue attempt")
	}
}

func TestR139LiveMirrorUsesSideCorrectFullBookDepth(t *testing.T) {
	kbook := &kalshi.Orderbook{YesBids: []kalshi.OrderbookLevel{{Price: .39, Size: 7}},
		YesAsks: []kalshi.OrderbookLevel{{Price: .42, Size: 3}}}
	my, mdy, ty, tdy, ok := kalshiFullBookSides(kbook, "YES")
	if !ok || my != .39 || mdy != 7 || ty != .42 || tdy != 3 {
		t.Fatalf("Kalshi YES full book=%v/%v %v/%v ok=%v", my, mdy, ty, tdy, ok)
	}
	mn, mdn, tn, tdn, ok := kalshiFullBookSides(kbook, "NO")
	if !ok || math.Abs(mn-.58) > 1e-12 || mdn != 3 || math.Abs(tn-.61) > 1e-12 || tdn != 7 {
		t.Fatalf("Kalshi NO full book=%v/%v %v/%v ok=%v", mn, mdn, tn, tdn, ok)
	}
	pbids := []polymarketus.BookLevel{{Price: .38, Quantity: 11}}
	pasks := []polymarketus.BookLevel{{Price: .43, Quantity: 2}}
	mn, mdn, tn, tdn, ok = polyUSFullBookSides(pbids, pasks, "NO")
	if !ok || math.Abs(mn-.57) > 1e-12 || mdn != 2 || math.Abs(tn-.62) > 1e-12 || tdn != 11 {
		t.Fatalf("PolyUS NO full book=%v/%v %v/%v ok=%v", mn, mdn, tn, tdn, ok)
	}
}
