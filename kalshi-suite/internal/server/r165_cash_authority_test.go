package server

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR165AuthenticatedProfitEvidenceRequiresWholeContract(t *testing.T) {
	valid := verdictEnt{ProfitEvidence: true, FillConditioned: true,
		EvidenceTier: "authenticated_live_fill"}
	if !verdictHasAuthenticatedProfitEvidence(valid) {
		t.Fatal("complete authenticated exchange-profit contract was rejected")
	}
	for name, edit := range map[string]func(*verdictEnt){
		"no profit flag":      func(v *verdictEnt) { v.ProfitEvidence = false },
		"not fill based":      func(v *verdictEnt) { v.FillConditioned = false },
		"research tier":       func(v *verdictEnt) { v.EvidenceTier = "historical_assumed_fill_simulation_void" },
		"blank evidence tier": func(v *verdictEnt) { v.EvidenceTier = "" },
	} {
		v := valid
		edit(&v)
		if verdictHasAuthenticatedProfitEvidence(v) {
			t.Fatalf("%s was allowed to mutate Paper", name)
		}
	}
}

func TestR165LegacyPerformanceControlsRemainTelemetryOnly(t *testing.T) {
	s := testServer(t)
	source := "auto-cons-r165-route"

	s.wrMu.Lock()
	s.wrCache = map[string][2]float64{source: {0, 100}}
	s.autoInvState = map[string]bool{source: true}
	s.wrMu.Unlock()
	s.decideMu.Lock()
	s.decideMap = map[string]srcVerdict{source: {
		Source: source, N: 100, MeanPC: -.50, FeePC: .01, PCCILo: -.60,
		PCCIHi: -.40, Verdict: "retire",
	}}
	s.decideAt = time.Now()
	s.decideMu.Unlock()
	s.polMu.Lock()
	s.polMap = map[string]string{"r165-route": "retired"}
	s.polMu.Unlock()

	if !s.policyRetired(source) {
		t.Fatal("research policy status was not retained for display")
	}
	if s.paperPolicyRetired(source) {
		t.Fatal("signal-log replay retired corrected Paper")
	}
	if s.autoInvertedByLoss(source) {
		t.Fatal("legacy Paper losses changed the emitted side")
	}
	if edge, ok := s.betEdge(context.Background(), "kalshi", "R165", "YES", source, .40, "realized-only"); ok || edge != 0 {
		t.Fatalf("legacy DECIDES edge resized a route: edge=%v ok=%v", edge, ok)
	}
	if got := s.signalKellyFrac("kalshi", source, "R165", "YES", .40, .25); got != .25 {
		t.Fatalf("Paper win-rate history changed Kelly fraction: got=%v want=.25", got)
	}
}

func TestR165RouteExactEvidenceIsTheOnlyVarianceInput(t *testing.T) {
	s := testServer(t)
	base := verdictEnt{
		Family: "taker:r165-route@kalshi", SourceFamily: "r165-route", Platform: "kalshi",
		Side: "YES", OriginLayer: "model", Route: "taker", Group: "taker",
		N: 100, Mean: .04, Lo: .02, Hi: .06,
	}
	injectVerdicts(s, []verdictEnt{base})
	if _, _, ok := s.famEdgeSE("kalshi", "auto-cons-r165-route", "YES", "taker"); ok {
		t.Fatal("assumed-fill verdict entered variance sizing")
	}

	base.ProfitEvidence, base.FillConditioned = true, true
	base.EvidenceTier = "authenticated_live_fill"
	injectVerdicts(s, []verdictEnt{base})
	m, se, ok := s.famEdgeSE("kalshi", "auto-cons-r165-route", "YES", "taker")
	if !ok || math.Abs(m-.04) > 1e-12 || math.Abs(se-.01) > 1e-12 {
		t.Fatalf("exact authenticated route evidence missing: m=%v se=%v ok=%v", m, se, ok)
	}
	for _, tc := range []struct{ platform, source, side, route string }{
		{"polyus", "auto-cons-r165-route", "YES", "taker"},
		{"kalshi", "auto-cons-other", "YES", "taker"},
		{"kalshi", "auto-cons-r165-route", "NO", "taker"},
		{"kalshi", "auto-cons-r165-route", "YES", "maker"},
	} {
		if _, _, ok := s.famEdgeSE(tc.platform, tc.source, tc.side, tc.route); ok {
			t.Fatalf("wrong exact-route identity borrowed profit evidence: %+v", tc)
		}
	}
}

func TestR165GenericFollowerResearchSignCannotSelectOrSize(t *testing.T) {
	s := testServer(t)
	base := verdictEnt{
		Family: "taker:r165-route@kalshi", SourceFamily: "r165-route", Platform: "kalshi",
		Side: "YES", OriginLayer: "model", Route: "taker", Group: "taker",
		N: 100, Mean: -.20, MeanAsk: .40, FeePC: .01,
	}
	injectVerdicts(s, []verdictEnt{base})
	cell, ok := s.gfModeCached("r165-route", "kalshi", "YES")
	if !ok || cell.Mean != 0 || cell.MeanAsk != 0 || cell.FeePC != 0 {
		t.Fatalf("negative research route did not enter neutral exploration: cell=%+v ok=%v", cell, ok)
	}
	if subs := gfDynamicSubs([]verdictEnt{base}); len(subs) != 1 || subs[0].Family != "gf:r165-route:yes:taker-k" {
		t.Fatalf("research sign selected the corrected-Paper roster: %+v", subs)
	}
	rows := gfPaperExecutionCoverage([]verdictEnt{base})
	if len(rows) != 1 || rows[0].Executor != "generic-follower" || rows[0].CurrentCents != -20 {
		t.Fatalf("coverage hid neutral research collection: %+v", rows)
	}

	base.ProfitEvidence, base.FillConditioned = true, true
	base.EvidenceTier = "authenticated_live_fill"
	injectVerdicts(s, []verdictEnt{base})
	if _, ok := s.gfModeCached("r165-route", "kalshi", "YES"); ok {
		t.Fatal("authenticated negative exact route was not defeated")
	}
	if subs := gfDynamicSubs([]verdictEnt{base}); len(subs) != 0 {
		t.Fatalf("authenticated negative route remained funded: %+v", subs)
	}

	base.Mean = .03
	injectVerdicts(s, []verdictEnt{base})
	cell, ok = s.gfModeCached("r165-route", "kalshi", "YES")
	if !ok || math.Abs(cell.Mean-.03) > 1e-12 || math.Abs(cell.MeanAsk-.40) > 1e-12 {
		t.Fatalf("authenticated positive exact route was not retained: cell=%+v ok=%v", cell, ok)
	}
	if _, ok := s.gfModeCached("r165-route", "kalshi", "NO"); ok {
		t.Fatal("YES evidence leaked into the NO route")
	}
}

func TestR165SealedPaperAloneHasNoCashAuthority(t *testing.T) {
	s := testServer(t)
	in := storage.ResearchPromotionIntent{
		IntentID: "current", Proof: storage.ResearchPromotionProof{
			SystemID: "system", ExperimentVersion: 1, Cohort: "cohort",
			Venue: "kalshi", Route: "taker",
		}, Candidate: storage.ResearchPromotionCandidate{
			StrategyFamily: "family", SelectorID: "selector", FiredSide: "YES", Side: "YES",
		},
	}
	if got := s.researchPromotionCashAuthorityReason(context.Background(), in); got !=
		sealedPromotionNoLiveCohortReason {
		t.Fatalf("sealed Paper-only promotion refusal=%q", got)
	}
}

func TestR165TerminalExchangeAttemptAloneStillHasNoCashAuthority(t *testing.T) {
	for name, cohort := range map[string]storage.ResearchLiveExecutionCohort{
		"one zero-fill": {Attempts: 1, Terminal: 1, ZeroFilled: 1},
		"one fill":      {Attempts: 1, Terminal: 1, Filled: 1},
	} {
		if got := researchPromotionCashAuthorityCohortReason(cohort); got !=
			sealedPromotionNoPositiveSettledLiveCohortReason {
			t.Fatalf("%s refusal=%q", name, got)
		}
	}
}

func TestR165NewMLPaperEvidenceHasNoCashAuthority(t *testing.T) {
	s := testServer(t)
	ok, why, _, _, _ := s.newMLV2LiveProof(liveMirrorCandidate{}, .40, false)
	if ok || why != newMLV2LiveCashRetiredReason {
		t.Fatalf("New-ML Paper proof crossed to cash: ok=%v why=%q", ok, why)
	}
}

func TestR165OperatorRankingsExcludeNonProfitEvidence(t *testing.T) {
	v := verdictEnt{Family: "taker:void@kalshi", SourceFamily: "void", Platform: "kalshi",
		OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker",
		N: 100, Markets: 100, ContractMarkets: 100, Mean: .20, Lo: .10, Hi: .30}
	if got := briefRankedSystems([]verdictEnt{v}, true, 5); len(got) != 0 {
		t.Fatalf("non-profit evidence entered operator ranking: %+v", got)
	}
	v.ProfitEvidence = true
	v.FillConditioned = true
	v.EvidenceTier = "authenticated_live_fill"
	if got := briefRankedSystems([]verdictEnt{v}, true, 5); len(got) != 1 {
		t.Fatalf("explicit profit evidence was not rankable: %+v", got)
	}
}

func TestR165OperatorVerdictsNeverCallResearchProfitProven(t *testing.T) {
	raw := verdictEnt{
		Family: "kalshi-flow", Group: "signal", N: 316, Mean: -.5075, Lo: -.60, Hi: -.40,
		State: "PROVEN-", Invert: .28,
		Venues: map[string]venueVerdict{
			"kalshi": {N: 316, Mean: -.5075, State: "PROVEN-", Invert: .28},
		},
	}
	got := operatorVerdict(raw)
	if got.State != "RESEARCH-" || got.SimulationState != "PROVEN-" || got.Invert != 0 ||
		got.ProfitEvidence || got.LiveAuthorizes || got.EvidenceTier != "research_model_or_simulation" {
		t.Fatalf("research row still masquerades as profit proof: %+v", got)
	}
	if got.Venues["kalshi"].State != "RESEARCH-" || got.Venues["kalshi"].Invert != 0 {
		t.Fatalf("nested venue result still masquerades as profit proof: %+v", got.Venues["kalshi"])
	}
	if raw.State != "PROVEN-" || raw.Invert == 0 {
		t.Fatalf("operator view mutated the retained research calculation: %+v", raw)
	}
	if line := verdictTransitionLine(got); !strings.HasPrefix(line, "Research-only result") {
		t.Fatalf("research transition was not plainly labelled: %q", line)
	}
}

func TestR165OperatorVerdictPreservesExplicitProfitEvidence(t *testing.T) {
	raw := verdictEnt{Family: "future-authenticated-cohort", State: "PROVEN+", ProfitEvidence: true,
		FillConditioned: true, LiveAuthorizes: false, EvidenceTier: "authenticated_live_fill"}
	got := operatorVerdict(raw)
	if got.State != "PROVEN+" || got.SimulationState != "" || !got.ProfitEvidence {
		t.Fatalf("explicit authenticated profit evidence was relabelled: %+v", got)
	}
}
