package server

// r130_test.go — pins for the R130 pass: the AUTOMATIC roster sync (gfDynamicSubs / gfFollowMode),
// the generic signal-follower executor (gate ladder, settle, verdict fold, hedge guard, book
// walls) and the parlay-lab never-gradable leg screen.

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// TestR130FollowMode — the follower's PAPER eligibility ladder: signal group only, never locked
// or ML, current venue c/ct only (not n/state), dedicated expressions not duplicated, and the
// fee/spread-adjusted inverse doctrine honored.
func TestR130FollowMode(t *testing.T) {
	vmap := map[string]verdictEnt{
		"winner":    {Family: "winner", Group: "signal", Venues: map[string]venueVerdict{"kalshi": {N: 100, Mean: 0.05, State: "COLLECTING"}}},
		"thin":      {Family: "thin", Group: "signal", Venues: map[string]venueVerdict{"kalshi": {N: 19, Mean: 0.09, State: "COLLECTING"}}},
		"locked":    {Family: "locked", Group: "signal", Locked: true, Venues: map[string]venueVerdict{"kalshi": {N: 500, Mean: 0.04}}},
		"futile":    {Family: "futile", Group: "signal", Venues: map[string]venueVerdict{"kalshi": {N: 5000, Mean: 0.001, State: "FUTILE"}}},
		"bookfam":   {Family: "bookfam", Group: "book", Venues: map[string]venueVerdict{"kalshi": {N: 300, Mean: 0.05}}},
		"ml-edge":   {Family: "ml-edge", Group: "signal", Venues: map[string]venueVerdict{"kalshi": {N: 500, Mean: 0.20}}},
		"xvgap":     {Family: "xvgap", Group: "signal", Venues: map[string]venueVerdict{"polyus": {N: 9000, Mean: 0.04, State: "PROVEN+"}}},
		"freshlist": {Family: "freshlist", Group: "signal", Venues: map[string]venueVerdict{"kalshi": {N: 400, Mean: -0.06, State: "PROVEN-", Invert: 0.02, FeePC: 0.01, InvHC: 0.02, SD: 0.2}}},
		"loser":     {Family: "loser", Group: "signal", Venues: map[string]venueVerdict{"kalshi": {N: 400, Mean: -0.06, State: "PROVEN-", Invert: 0.02, FeePC: 0.01, InvHC: 0.02, SD: 0.2}}},
		"deadloss":  {Family: "deadloss", Group: "signal", Venues: map[string]venueVerdict{"kalshi": {N: 400, Mean: -0.01, State: "COLLECTING"}}},
	}
	cases := []struct {
		fam     string
		wantOK  bool
		wantInv bool
	}{
		{"winner", true, false},
		{"thin", true, false},       // n is telemetry, never a PAPER gate
		{"locked", false, false},    // venue-locked
		{"futile", true, false},     // current positive point wins over PAPER verdict state
		{"bookfam", false, false},   // book families are already expressed
		{"ml-edge", false, false},   // ML remains research-only
		{"xvgap", false, false},     // dedicated PolyUS executor (book:xvgap)
		{"freshlist", false, false}, // Kalshi's exact invert already runs in the FreshInv book
		{"loser", false, false},     // opposite side needs its own trigger and route proof
		{"deadloss", false, false},  // negative, no qualifying twin
		{"ghost", false, false},
	}
	for _, c := range cases {
		venue := "kalshi"
		if c.fam == "xvgap" {
			venue = "polyus"
		}
		inv, _, ok := gfVenueMode(vmap[c.fam], c.fam, venue)
		if ok != c.wantOK || inv != c.wantInv {
			t.Fatalf("gfFollowMode(%s) = inv=%v ok=%v, want inv=%v ok=%v", c.fam, inv, ok, c.wantInv, c.wantOK)
		}
	}
	// A dedicated DIRECT lane must not suppress the separately measured opposite-side edge.
	xvBad := verdictEnt{Family: "xvgap", Group: "signal", Venues: map[string]venueVerdict{
		"kalshi": {N: 400, Mean: -0.06, State: "PROVEN-", Invert: 0.02, FeePC: 0.01, InvHC: 0.02, SD: 0.2}}}
	if inv, _, ok := gfVenueMode(xvBad, "xvgap", "kalshi"); ok || inv {
		t.Fatalf("a PROVEN- xvgap lane must not manufacture an opposite trade, got inv=%v ok=%v", inv, ok)
	}
}

func TestR132ProvenMinusTelegramNamesEdgeAndRunningInvert(t *testing.T) {
	s := testServer(t)
	v := verdictEnt{Family: "loser", Group: "signal", Unit: "$/contract", N: 400,
		Platform: "kalshi", Side: "YES", Route: "taker",
		Mean: -0.06, FeePC: 0.01, InvHC: 0.02, State: "PROVEN-", Invert: 0.02,
		Venues: map[string]venueVerdict{"kalshi": {
			N: 400, Mean: -0.06, FeePC: 0.01, InvHC: 0.02, SD: 0.2, State: "PROVEN-", Invert: 0.02}}}
	got := s.invertVerdictStatus(v)
	for _, want := range []string{"+2.0¢/ct", "COLLECTING", "invert:loser"} {
		if !strings.Contains(got, want) {
			t.Fatalf("invert verdict status missing %q: %s", want, got)
		}
	}

	v.Mean, v.Invert, v.Venues = -0.03, 0, nil // inverse = -1¢ after 2¢ fees + 2¢ spread
	got = s.invertVerdictStatus(v)
	if !strings.Contains(got, "-1.0¢/ct") || !strings.Contains(got, "COLLECTING") {
		t.Fatalf("cost-negative inverse must be named and refused: %s", got)
	}

	injectVerdicts(s, []verdictEnt{{Family: "taker:invert:loser@kalshi", SourceFamily: "invert:loser",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "NO", Group: "taker",
		N: 7, Mean: .03, SeedSource: "legacy-exact-side-control-book-route"}})
	v.Mean, v.Invert = -.06, .02
	got = s.invertVerdictStatus(v)
	for _, want := range []string{"PAPER-PAUSED", "+3.0 cents/ct", "pre-R144", "LIVE armed=false (sealed)"} {
		if !strings.Contains(got, want) {
			t.Fatalf("truthful inverse runtime status missing %q: %s", want, got)
		}
	}
}

// TestR130DynamicSubs — every exact non-profit-evidence route gets a neutral exploration line on
// its executable venue; historical point sign cannot select the Paper roster.
func TestR130DynamicSubs(t *testing.T) {
	verds := []verdictEnt{
		{Family: "taker:dual@kalshi", SourceFamily: "dual", Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 60, Mean: .05},
		{Family: "taker:dual@polyus", SourceFamily: "dual", Platform: "polyus", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 40, Mean: .04},
		{Family: "taker:pusheavy@kalshi", SourceFamily: "pusheavy", Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 13, Mean: -.05},
		{Family: "taker:pusheavy@polyus", SourceFamily: "pusheavy", Platform: "polyus", OriginLayer: "model", Route: "taker", Side: "NO", Group: "taker", N: 53, Mean: .14},
		// both venues qualify: 0.6*100=60 kalshi rows, 0.4*100=40 polyus rows
		{Family: "dual", Group: "signal", Venues: map[string]venueVerdict{
			"kalshi": {N: 60, Mean: 0.05, State: "COLLECTING"}, "polyus": {N: 40, Mean: 0.04, State: "COLLECTING"}}},
		// Kalshi is negative enough to run its separately-labelled inverse; PolyUS runs direct.
		{Family: "pusheavy", Group: "signal", Venues: map[string]venueVerdict{
			"kalshi": {N: 13, Mean: -0.05, State: "COLLECTING"}, "polyus": {N: 53, Mean: 0.14, State: "PROVEN+"}}},
		// inverted follow: seed must be the twin
		{Family: "loser", Group: "signal", Venues: map[string]venueVerdict{
			"kalshi": {N: 400, Mean: -0.06, SD: 0.2, State: "PROVEN-", Invert: 0.02, FeePC: 0.01, InvHC: 0.02}}},
		// No exact taker row exists for neg, so there is no registered route.
		{Family: "neg", Group: "signal", Venues: map[string]venueVerdict{"kalshi": {N: 400, Mean: -0.01, State: "COLLECTING"}}},
	}
	subs := gfDynamicSubs(verds)
	byFam := map[string]bookSub{}
	for _, sb := range subs {
		byFam[sb.Family] = sb
	}
	if len(subs) != 4 {
		t.Fatalf("want 4 registered exact-route lines, got %d: %+v", len(subs), subs)
	}
	if sb := byFam["gf:dual:yes:taker-k"]; sb.Book != vbKalshi || sb.Seed != "taker:dual@kalshi" {
		t.Fatalf("gf:dual-k wrong: %+v", sb)
	}
	if sb := byFam["gf:dual:yes:taker-p"]; sb.Book != vbPolyus || sb.Seed != "taker:dual@polyus" {
		t.Fatalf("gf:dual-p wrong: %+v", sb)
	}
	if sb := byFam["gf:pusheavy:yes:taker-k"]; sb.Book != vbKalshi {
		t.Fatalf("negative research point removed its direct one-share exploration lane: %+v", sb)
	}
	if sb := byFam["gf:pusheavy:no:taker-p"]; sb.Book != vbPolyus {
		t.Fatalf("gf:pusheavy-p missing/wrong: %+v", sb)
	}
	if _, ok := byFam["gf:loser-k"]; ok {
		t.Fatal("losing emitted-side family must not seed an executable inverse twin")
	}
	if _, ok := byFam["gf:neg-k"]; ok {
		t.Fatal("measured-negative family without a twin must not join")
	}
}

// TestR130SeedWeights — a follower line with no settled lots rides the underlying family's
// scoreboard ¢ in the weight table (the operator's ask: leaders join AT their measured share).
func TestR130SeedWeights(t *testing.T) {
	s := testServer(t)
	injectVerdicts(s, []verdictEnt{
		{Family: "pfbridge", Group: "signal", Unit: "$/contract", N: 66, Mean: 0.14,
			Venues: map[string]venueVerdict{"polyus": {N: 53, Mean: 0.14, State: "PROVEN+"}}},
		{Family: "taker:pfbridge@polyus", SourceFamily: "pfbridge", Platform: "polyus", OriginLayer: "model",
			Route: "taker", Side: "YES", Group: "taker", Unit: "$/contract", N: 53, Mean: .14},
	})
	// scoreWeights recomputes verdicts through the cache (fresh injectVerdicts read).
	tab := s.scoreWeights(context.Background())
	var ent *swEntry
	for i := range tab.Books[vbPolyus] {
		if tab.Books[vbPolyus][i].Family == "gf:pfbridge:yes:taker-p" {
			ent = &tab.Books[vbPolyus][i]
		}
	}
	if ent == nil {
		t.Fatalf("gf:pfbridge-p missing from the polyus roster: %+v", tab.Books[vbPolyus])
	}
	if ent.Cents != 0 || ent.N != 0 || ent.State != gfPaperPointState || ent.ProfitEvidence {
		t.Fatalf("seeded cents wrong: %+v (want 14¢ n=53 venue-only)", ent)
	}
	if ent.Weight <= 0 {
		t.Fatalf("hypothesis exploration line must carry a small weight, got %v", ent.Weight)
	}
	if share, ok := s.subShareUSD("gf:pfbridge:yes:taker-p"); !ok || share <= 0 {
		t.Fatalf("SubShare must exist for the seeded line, got %v/%v", share, ok)
	}
}

// TestR130FollowerGates — the placement ladder refuses: cold cache, unknown/negative/locked
// families, poly-int rows, wide spreads, knob-off. Nothing may touch the ledger.
func TestR130FollowerGates(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	lots := func() int {
		s.gfBookMu.Lock()
		defer s.gfBookMu.Unlock()
		n := 0
		for _, b := range s.gfLoadLocked().Subs {
			n += len(b.Open)
		}
		return n
	}
	try := func(plat, ticker, fam string, px float64) {
		s.genfollowConsider(ctx, storage.Signal{Platform: plat, Ticker: ticker, Title: "t",
			Side: "YES", SignalType: fam, EntryPrice: px})
	}
	try("kalshi", "GF-COLD", "winner", 0.40) // cold verdict cache fails closed
	injectVerdicts(s, []verdictEnt{
		{Family: "winner", Group: "signal", Unit: "$/contract", Venues: map[string]venueVerdict{"kalshi": {N: 100, Mean: 0.05, State: "COLLECTING"}}},
		{Family: "negfam", Group: "signal", Unit: "$/contract", Venues: map[string]venueVerdict{"kalshi": {N: 400, Mean: -0.02, State: "COLLECTING"}}},
		{Family: "lockedfam", Group: "signal", Unit: "$/contract", Locked: true, Venues: map[string]venueVerdict{"kalshi": {N: 400, Mean: 0.05}}},
		{Family: "taker:winner@kalshi", SourceFamily: "winner", Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 100, Mean: .05},
		{Family: "taker:negfam@kalshi", SourceFamily: "negfam", Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 400, Mean: -.02},
	})
	try("polymarket", "GF-PINT", "winner", 0.40) // poly-int can never place
	try("kalshi", "GF-NEG", "negfam", 0.40)      // measured-negative, no twin
	try("kalshi", "GF-LOCK", "lockedfam", 0.40)  // venue-locked
	try("kalshi", "", "winner", 0.40)            // no ticker
	try("kalshi", "GF-PX", "winner", 0.999)      // price sanity
	s.genfollowConsider(ctx, storage.Signal{Platform: "kalshi", Ticker: "GF-SPREAD", Title: "t",
		Side: "YES", SignalType: "winner", EntryPrice: 0.40, SpreadCents: 9})
	if n := lots(); n != 0 {
		t.Fatalf("every gate case must refuse, ledger has %d lots", n)
	}
	off := false
	s.mutateCfg(func(c *config.Config) { c.Auto.GenfollowEnabled = &off })
	try("kalshi", "GF-KNOB", "winner", 0.40)
	if n := lots(); n != 0 {
		t.Fatalf("knob-off must refuse, ledger has %d lots", n)
	}
}

// TestR130FollowerSettleAndFold — settled follower lots grade as their own gf:* families, the
// hedge guard is venue-scoped, and exposure/net land on the right venue books.
func TestR130FollowerSettleAndFold(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)
	alphaView, alphaLot := r167PaperProofFixture("r130-alpha-attempt", "alpha", "kalshi",
		"GFS-K", "YES", 10, .40, .10, 20)
	betaView, betaLot := r167PaperProofFixture("r130-beta-attempt", "beta", "polyus",
		"gfs-p", "NO", 5, .30, .05, 20)
	alphaLot.TS, alphaLot.Title = now, "t"
	betaLot.TS, betaLot.Title = now, "t"
	for _, lot := range []*kfPos{&alphaLot, &betaLot} {
		lot.ExecutionTruthContract = fundedPaperLiveTruthContractV1
		lot.ExecutionLiveTerminalKind = fundedPaperLiveTerminalFill
		lot.ExecutionLiveTerminal = "filled"
		lot.ExecutionLiveTerminalID = "display-only-terminal"
	}
	r167InsertPaperProof(t, s, alphaView)
	r167InsertPaperProof(t, s, betaView)
	s.gfBookMu.Lock()
	st := s.gfLoadLocked()
	st.Subs["gf:alpha-k"] = &kfBook{Open: []kfPos{alphaLot}}
	st.Subs["gf:beta-p"] = &kfBook{Open: []kfPos{betaLot}}
	s.gfBookDirty = true
	s.gfBookMu.Unlock()
	// hedge guard: venue-scoped by key suffix
	if side := s.gfBookOpenSide("kalshi", "GFS-K"); side != "YES" {
		t.Fatalf("kalshi sub must expose YES, got %q", side)
	}
	if side := s.gfBookOpenSide("polyus", "GFS-K"); side != "" {
		t.Fatalf("polyus lookup must not see the kalshi lot, got %q", side)
	}
	// exposure walls: kalshi 0.40×10+0.10 = 4.10 · polyus 0.30×5+0.05 = 1.55
	ku, kl, kok := s.gfExposureByVenue(vbKalshi)
	pu, pl, pok := s.gfExposureByVenue(vbPolyus)
	if !kok || !pok {
		t.Fatal("healthy follower ledger must report exposure as readable")
	}
	if math.Abs(ku-4.10) > 1e-9 || kl != 1 || math.Abs(pu-1.55) > 1e-9 || pl != 1 {
		t.Fatalf("venue exposure wrong: k=%v/%d p=%v/%d", ku, kl, pu, pl)
	}
	mustInsertResolvedSignal(t, s, "kalshi", "GFS-K", "YES", 0.40, 1) // YES wins
	mustInsertResolvedSignal(t, s, "polyus", "gfs-p", "NO", 0.70, 0)  // NO wins
	if inserted, err := s.store.RecordVenueSettlement(ctx, "polyus", "gfs-p", 0, time.Now(),
		storage.PolyUSFinalEndpointSettlementV2); err != nil || !inserted {
		t.Fatalf("trusted PolyUS terminal receipt inserted=%v err=%v", inserted, err)
	}
	s.settleGenfollowBook(ctx)
	s.gfBookMu.Lock()
	aw := st.Subs["gf:alpha-k"].Wins
	bw := st.Subs["gf:beta-p"].Wins
	ao := len(st.Subs["gf:alpha-k"].Open)
	bo := len(st.Subs["gf:beta-p"].Open)
	s.gfBookMu.Unlock()
	if aw != 1 || bw != 1 || ao != 0 || bo != 0 {
		t.Fatalf("settle failed: alpha wins=%d open=%d · beta wins=%d open=%d", aw, ao, bw, bo)
	}
	// display nets follow the venue split
	if net := s.gfNetByVenue(vbKalshi); net <= 0 {
		t.Fatalf("kalshi follower net must be positive after the winning settle, got %v", net)
	}
	// verdict fold: both subs grade as their own families
	s.verdMu.Lock()
	s.verdCache, s.verdAt = nil, time.Time{}
	s.verdMu.Unlock()
	found := map[string]bool{}
	for _, v := range s.computeExperimentVerdicts(ctx) {
		if (v.Family == "gf:alpha-k" || v.Family == "gf:beta-p") && v.N == 1 && v.Group == "book" {
			found[v.Family] = true
		}
	}
	if !found["gf:alpha-k"] || !found["gf:beta-p"] {
		t.Fatalf("both follower subs must fold into the verdict engine, got %v", found)
	}
	// own-ledger ¢/ct exists now → the weight table must prefer it over any seed
	if _, ok := s.briefBookCentsPerContract(ctx)["gf:alpha-k"]; !ok {
		t.Fatal("follower sub must publish its own ¢/ct once lots settle")
	}
}

// TestR130PlabLegGradable — retained name, now pinning the strict current Combo Lab horizon.
func TestR130PlabLegGradable(t *testing.T) {
	s := testServer(t)
	s.plabHorizonFn = nil
	s.metaMu.Lock()
	if s.kmkts == nil {
		s.kmkts = map[string]kalshi.Market{}
	}
	s.kmkts["KX-SOON"] = kalshi.Market{Ticker: "KX-SOON",
		ExpectedExpiration: time.Now().Add(4 * time.Hour).UTC().Format(time.RFC3339)}
	s.kmkts["KX-2028"] = kalshi.Market{Ticker: "KX-2028",
		ExpectedExpiration: time.Now().Add(600 * 24 * time.Hour).UTC().Format(time.RFC3339)}
	s.kmkts["KX-CLOSEONLY"] = kalshi.Market{Ticker: "KX-CLOSEONLY",
		CloseTime: time.Now().Add(90 * 24 * time.Hour).UTC().Format(time.RFC3339)}
	s.metaMu.Unlock()
	if !s.plabLegGradable("kalshi", "KX-SOON") {
		t.Fatal("near-dated leg must pass")
	}
	if s.plabLegGradable("kalshi", "KX-2028") {
		t.Fatal("a 600-day leg can never grade inside the 45d sweep — must refuse")
	}
	if s.plabLegGradable("kalshi", "KX-CLOSEONLY") {
		t.Fatal("close_time 90d out must refuse too (fallback horizon)")
	}
	// A kalshi ticker ABSENT from the windowed universe IS far-dated (the window exists because
	// far-out close times are excluded from the pull) — fail closed. Verified live at ship:
	// KXPRESPERSON-28 / KXOSCARPIC-27 / KXBTCMINY-27 all absent from kmkts yet scored by the
	// sidecar — the permissive version waved through exactly the legs the screen targets.
	if s.plabLegGradable("kalshi", "KX-NOT-IN-WINDOW") {
		t.Fatal("kalshi leg absent from the windowed universe must refuse (fail closed)")
	}
	// In-cache but unparseable timestamp and a non-catalogued venue leg both fail closed.
	s.metaMu.Lock()
	s.kmkts["KX-BADTS"] = kalshi.Market{Ticker: "KX-BADTS", ExpectedExpiration: "not-a-time"}
	s.metaMu.Unlock()
	if s.plabLegGradable("kalshi", "KX-BADTS") {
		t.Fatal("in-cache unparseable timestamp must fail closed")
	}
	if s.plabLegGradable("polyus", "whatever") {
		t.Fatal("unknown PolyUS horizon must fail closed")
	}
}
