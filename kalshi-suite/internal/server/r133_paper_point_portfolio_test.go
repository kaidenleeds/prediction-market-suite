package server

import (
	"math"
	"strings"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR133PaperPointEligibilityIgnoresNAndVerdictState(t *testing.T) {
	direct := verdictEnt{Family: "new-edge", Group: "signal", Venues: map[string]venueVerdict{
		"kalshi": {N: 1, Mean: 0.004, State: "FUTILE"},
	}}
	inv, score, ok := gfVenueMode(direct, direct.Family, "kalshi")
	if !ok || inv || score.N != 1 || score.Mean != 0.004 || score.State != gfPaperPointState {
		t.Fatalf("n=1 point-positive PAPER lane = inv=%v ok=%v score=%+v", inv, ok, score)
	}

	inverse := verdictEnt{Family: "new-fade", Group: "signal", Venues: map[string]venueVerdict{
		"polyus": {N: 1, Mean: -0.06, FeePC: 0.01, InvHC: 0.02, State: "COLLECTING"},
	}}
	inv, score, ok = gfVenueMode(inverse, inverse.Family, "polyus")
	if ok || inv {
		t.Fatalf("n=1 losing emitted-side lane must remain diagnostic: inv=%v ok=%v score=%+v", inv, ok, score)
	}

	tooShallow := inverse
	tooShallow.Venues = map[string]venueVerdict{"polyus": {N: 1, Mean: -0.03, FeePC: 0.01, InvHC: 0.02}}
	if inv, _, ok := gfVenueMode(tooShallow, tooShallow.Family, "polyus"); ok || inv {
		t.Fatal("a 3c loss cannot fund an inverse after 4c of fee/spread drag")
	}
}

func TestR133PaperCoverageMapsEveryPositiveSignalExpressionOnce(t *testing.T) {
	verds := []verdictEnt{
		{Family: "taker:plain@kalshi", SourceFamily: "plain", Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 1, Mean: .03},
		{Family: "taker:loser@polyus", SourceFamily: "loser", Platform: "polyus", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 1, Mean: -.07},
		{Family: "taker:xvgap@polyus", SourceFamily: "xvgap", Platform: "polyus", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 7, Mean: .02},
		{Family: "taker:freshlist@kalshi", SourceFamily: "freshlist", Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "NO", Group: "taker", N: 3, Mean: -.07},
		{Family: "taker:locked@kalshi", SourceFamily: "locked", Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 20, Mean: .10, Locked: true},
		{Family: "taker:ml-edge@kalshi", SourceFamily: "ml-edge", Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 20, Mean: .10},
		{Family: "taker:no-edge@polyus", SourceFamily: "no-edge", Platform: "polyus", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 20, Mean: -.01},
		{Family: "plain", Group: "signal", Venues: map[string]venueVerdict{"kalshi": {N: 1, Mean: 0.03}}},
		{Family: "loser", Group: "signal", Venues: map[string]venueVerdict{"polyus": {N: 1, Mean: -0.07, FeePC: 0.01, InvHC: 0.02}}},
		{Family: "xvgap", Group: "signal", Venues: map[string]venueVerdict{"polyus": {N: 7, Mean: 0.02}}},
		{Family: "freshlist", Group: "signal", Venues: map[string]venueVerdict{"kalshi": {N: 3, Mean: -0.07, FeePC: 0.01, InvHC: 0.02}}},
		{Family: "locked", Group: "signal", Locked: true, Venues: map[string]venueVerdict{"kalshi": {N: 20, Mean: 0.10}}},
		{Family: "ml-edge", Group: "signal", Venues: map[string]venueVerdict{"kalshi": {N: 20, Mean: 0.10}}},
		{Family: "no-edge", Group: "signal", Venues: map[string]venueVerdict{"polyus": {N: 20, Mean: -0.01}}},
		{Family: "not-a-signal", Group: "book", Venues: map[string]venueVerdict{"kalshi": {N: 20, Mean: 0.10}}},
	}
	rows := gfPaperExecutionCoverage(verds)
	by := map[string]gfPaperCoverageRow{}
	for _, r := range rows {
		by[r.Family+"@"+r.Platform] = r
		if r.Direction != "" && r.Executor == "" {
			t.Fatalf("eligible expression has no PAPER executor: %+v", r)
		}
	}
	checks := map[string]string{
		"plain@kalshi": "generic-follower",
		"xvgap@polyus": "dedicated",
	}
	for key, want := range checks {
		if got := by[key].Executor; got != want {
			t.Fatalf("coverage[%s].executor=%q want %q; row=%+v", key, got, want, by[key])
		}
	}
	if got := by["loser@polyus"]; got.Executor != "generic-follower" || got.Direction != "direct" {
		t.Fatalf("negative research point removed neutral direct collection: %+v", got)
	}
	if got := by["freshlist@kalshi"]; got.Executor != "generic-follower" || got.Direction != "direct" {
		t.Fatalf("research sign changed direct route registration: %+v", got)
	}
	if got := by["locked@kalshi"].Excluded; got != "venue-locked" {
		t.Fatalf("locked exclusion=%q", got)
	}
	if got := by["ml-edge@kalshi"].Excluded; got != "ml-not-generic-follower" {
		t.Fatalf("ML exclusion=%q", got)
	}
	if got := by["no-edge@polyus"]; got.Executor != "generic-follower" || got.Direction != "direct" {
		t.Fatalf("no-edge research sign changed route registration: %+v", got)
	}
	if _, ok := by["not-a-signal@kalshi"]; ok {
		t.Fatal("non-signal scoreboard rows do not belong in the signal execution coverage map")
	}
}

func TestR144UnclusteredHistoricalPointCanFundPaperButNotProof(t *testing.T) {
	v := verdictEnt{Family: "taker:legacy-edge@kalshi", SourceFamily: "legacy-edge",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker",
		N: 0, Markets: 0, SettledRows: 80, ContractMarkets: 60, UnclusteredMarkets: 60,
		Mean: 0, Lo: -9999, Hi: 9999, State: "COLLECTING", PaperPointMean: .025,
		PaperPointMeanAsk: .40, PaperPointMeanFeePC: .01}
	rows := gfPaperExecutionCoverage([]verdictEnt{v})
	if len(rows) != 1 || rows[0].Executor != "generic-follower" || rows[0].N != 0 ||
		rows[0].Markets != 0 || rows[0].PointMarkets != 60 || rows[0].CurrentCents != 2.5 {
		t.Fatalf("historical point/PAPER separation failed: %+v", rows)
	}
	if ask, fee := gfPaperPointCosts(v); ask != .40 || fee != .01 {
		t.Fatalf("historical PAPER point lost its matching cost cohort: ask=%v fee=%v", ask, fee)
	}
	state, _, _ := liveMirrorUnitAdjusted(storage.UnitTrialLeaderboardStat{
		Family: "legacy-edge", Platform: "kalshi", N: 0, SettledMarkets: 60,
		MeanPC: .025, MeanAsk: .40,
	}, .40, 0)
	if state == "PROVEN+" {
		t.Fatal("unclustered historical point incorrectly authorized LIVE")
	}
}

func TestR144BriefingShowsLegacyPaperPointWithoutInventingIndependentMarkets(t *testing.T) {
	v := verdictEnt{Family: "taker:legacy-edge@kalshi", SourceFamily: "legacy-edge",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker",
		N: 0, Markets: 0, ContractMarkets: 60, PaperPointMean: .025,
		Lo: -9999, Hi: 9999, State: "COLLECTING"}
	rows := briefRankedSystems([]verdictEnt{v}, true, 5)
	if len(rows) != 0 {
		t.Fatalf("legacy Paper point entered canonical compact rank: %+v", rows)
	}
	line := briefRankedSystemLine(briefRankedSystem{v: v, cell: "K/Y/T", mean: .025,
		pointOnly: true, pointN: 60}, map[string]int{})
	for _, want := range []string{"+2.5¢", "n60", "CI unavailable", "K/Y/T", "PAPER point"} {
		if !strings.Contains(line, want) {
			t.Fatalf("legacy point line missing %q: %s", want, line)
		}
	}
	if strings.Contains(line, "m60") || strings.Contains(line, "m0") || strings.Contains(line, "m unavailable") {
		t.Fatalf("removed m denominator leaked into legacy display: %s", line)
	}
}

func TestR133DedicatedMapNeverHidesPartialFrozenOrDifferentExpressions(t *testing.T) {
	for _, c := range []struct {
		fam, venue string
		want       bool
	}{
		{"xvgap", "polyus", true},
		{"xvgap", "kalshi", false},
		{"freshlist", "polyus", true},
		{"freshlist", "kalshi", false},
		{"arb", "kalshi", false}, // single cheap-side signal != two-leg lock
		{"arb", "polyus", false},
		{"favlong", "kalshi", false}, // specialist covers only 80-92c
		{"favlong", "polyus", false},
		{"wxedge", "kalshi", false},    // old weather specialist is frozen
		{"kflow", "kalshi", false},     // pre/live twins are frozen
		{"freshfade", "polyus", false}, // only its inverse is the PUS direct lane
	} {
		if got := gfDedicatedDirectExpression(c.fam, c.venue); got != c.want {
			t.Fatalf("dedicated direct %s@%s=%v want %v", c.fam, c.venue, got, c.want)
		}
	}
}

func TestR165GenericPortfolioWeightsIgnoreUnverifiedPoints(t *testing.T) {
	ents := []swEntry{
		{Family: "gf:n1-k", Cents: 4, N: 1, State: "FUTILE"},
		{Family: "gf:negative-k", Cents: -1, N: 10000, State: "PROVEN+"},
		{Family: "legacy", Cents: 2, N: 100, State: "PROVEN+"},
	}
	swNormalize(ents)
	for _, ent := range ents {
		if math.Abs(ent.Weight-1.0/3.0) > 1e-12 || !ent.Explore {
			t.Fatalf("unverified point changed equal Paper exploration: %+v", ents)
		}
	}

	ents[0].ProfitEvidence = true
	ents[0].State = "PROVEN+"
	ents[1].ProfitEvidence = true
	swNormalize(ents)
	if math.Abs(ents[0].Weight-.9) > 1e-12 || ents[0].Explore || ents[1].Weight != 0 ||
		!ents[2].Explore || math.Abs(ents[2].Weight-.1) > 1e-12 {
		t.Fatalf("authenticated evidence did not control only its own allocation share: %+v", ents)
	}
}

// TestR133LegacyGenericProfitRateNudge remains a replay compatibility pin. R158 removed this
// helper from funded Paper sizing because the adaptive conservative Net/day share already counts
// profit rate once.
func TestR133LegacyGenericProfitRateNudge(t *testing.T) {
	exp := 0.65
	fast, slow := gfProfitRateMultiplier(0.5, exp), gfProfitRateMultiplier(6, exp)
	if fast != gfTurnoverMaxMult || slow != 1 {
		t.Fatalf("bounded C4 multipliers fast=%v slow=%v", fast, slow)
	}
	if 0.05*fast <= 0.05*slow {
		t.Fatal("legacy replay helper must slightly favor the quicker market")
	}
	if 0.05*fast >= 0.06*slow {
		t.Fatal("the bounded speed nudge must not outrank a materially stronger c/ct edge")
	}
	if gfProfitRateMultiplier(0, exp) != 1 || gfProfitRateMultiplier(1, 0) != 1 {
		t.Fatal("unknown horizon or disabled C4 must be neutral")
	}
	state, _, _ := liveMirrorUnitAdjusted(storage.UnitTrialLeaderboardStat{
		Family: "n1", Platform: "kalshi", N: 1, MeanPC: 0.50, MeanAsk: 0.30,
	}, 0.30, 0)
	if state == "PROVEN+" {
		t.Fatal("historical Paper behavior must not weaken LIVE's route-specific PROVEN+ gate")
	}
}

func TestR133BriefingCallsPaperLedgersPortfolios(t *testing.T) {
	s := testServer(t)
	injectVerdicts(s, []verdictEnt{{Family: "taker:r133@kalshi", SourceFamily: "r133", Platform: "kalshi",
		Side: "YES", OriginLayer: "model", Route: "taker", Group: "taker", N: 1, Mean: 0.025}})
	if got := s.activePortfolioSystemsLine(); got != "🚀 Portfolio systems · 0 positive authenticated exchange-profit routes · avg n/a" ||
		strings.Contains(got, "allocations") || strings.Contains(got, "sealed") {
		t.Fatalf("active portfolio-system briefing wording=%q", got)
	}
}
