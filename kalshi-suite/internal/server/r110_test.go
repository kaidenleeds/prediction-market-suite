package server

// R110 pins, R111-CORRECTED (operator refutation): TRUE restatements (identical payoff — crypto
// dual-series twins) are blocked; CORRELATED-DISTINCT variants (full-game/F3/F5/F7, spread
// ladders, corners bands) are ALLOWED and tracked as correlated-exposure clusters with a stake
// cap. Parlay-lab legality now delegates to the real kalshi.ComboLegalAny rules (same-game
// multi-leg combos are venue-legal — the operator's 4-leg France–Morocco app combo).

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

// R111 CORRECTED taxonomy (operator refutation of R110): correlated-distinct variants of one
// underlying (F3/F5/F7 vs full game, spreads, corners) share a CLUSTER key but are NOT duplicates;
// only identical-payoff twins (crypto dual-series) share a TRUE-restatement key.
func TestR111Keys(t *testing.T) {
	game := corrClusterKey("kalshi", "KXMLBGAME-26JUL071415MILSTLG1-MIL")
	if game == "" {
		t.Fatal("full-game winner must carry a cluster identity")
	}
	// The venue variants of "Milwaukee …" CLUSTER together (correlated) …
	for _, tk := range []string{
		"KXMLBF3-26JUL071415MILSTLG1-MIL",
		"KXMLBF5-26JUL071415MILSTLG1-MIL",
		"KXMLBF7-26JUL071415MILSTLG1-MIL",
	} {
		if corrClusterKey("kalshi", tk) != game {
			t.Fatalf("%s must share the cluster %q", tk, game)
		}
		// … but they are NOT true restatements (different payoff windows).
		if trueRestatementKey("kalshi", tk) == trueRestatementKey("kalshi", "KXMLBGAME-26JUL071415MILSTLG1-MIL") {
			t.Fatalf("%s is a different payoff than the full game — must NOT be a true restatement", tk)
		}
	}
	// Spread lines cluster with the winner (ARG2 ~ ARG); the opponent does not.
	a := corrClusterKey("kalshi", "KXWCGAME-26JUL07ARGEGY-ARG")
	if a == "" || corrClusterKey("kalshi", "KXWCSPREAD-26JUL07ARGEGY-ARG2") != a {
		t.Fatal("winner + spread of one team/game must share one cluster")
	}
	if corrClusterKey("kalshi", "KXWCGAME-26JUL07ARGEGY-EGY") == a {
		t.Fatal("the opponent is not the same cluster side")
	}
	// TRUE restatement: the live-verified identical-payoff twin pair (byte-identical
	// rules_primary, same strike/close — probed 2026-07-07).
	tw := trueRestatementKey("kalshi", "KXETH-26JUL0817-T2509.99")
	if tw == "" || trueRestatementKey("kalshi", "KXETHD-26JUL0817-T2509.99") != tw {
		t.Fatal("KXETH/KXETHD same strike+window must share a TRUE restatement identity")
	}
	if trueRestatementKey("kalshi", "KXETHD-26JUL0817-T2469.99") == tw {
		t.Fatal("a different strike is a different payoff")
	}
	if trueRestatementKey("kalshi", "KXETHD-26JUL0917-T2509.99") == tw {
		t.Fatal("a different window is a different payoff")
	}
	// Ladders: strikes of ONE event share a cluster; different windows are different clusters —
	// the R107 xvident anchor property, pinned here per the R111 ask.
	l1 := corrClusterKey("kalshi", "KXBTCD-26JUL0717-T108000")
	if l1 == "" || corrClusterKey("kalshi", "KXBTCD-26JUL0717-T110000") != l1 {
		t.Fatal("two strikes of one BTC ladder must share a cluster")
	}
	if corrClusterKey("kalshi", "KXBTCD-26JUL0817-T108000") == l1 {
		t.Fatal("same strike in a different window is a different cluster")
	}
	// Non-kalshi: inert.
	if corrClusterKey("polyus", "aec-mlb-mil-stl-2026-07-07") != "" || trueRestatementKey("polyus", "x-y-z") != "" {
		t.Fatal("non-kalshi platforms carry no identities")
	}
}

func TestR111DupVsCorrelatedGuard(t *testing.T) {
	s := testServer(t)
	if !s.twinLoad(context.Background()) {
		t.Fatal("empty persisted twin registry must load")
	}
	// R112: after a successful registry load, an UNVERIFIED twin candidate is DISTINCT
	// (operator default: bettable).
	pos := []paper.Position{{Platform: "kalshi", Ticker: "KXETH-26JUL0817-T2509.99", Contracts: 2, CostBasis: 1}}
	if r := s.betConflictReason("kalshi", "KXETHD-26JUL0817-T2509.99", "YES", "gate", pos); r != "" {
		t.Fatalf("unverified twin candidate must be allowed, got %q", r)
	}
	// VERIFIED identical twin held → blocked, any source.
	seedTwinIdentical(s, trueRestatementKey("kalshi", "KXETH-26JUL0817-T2509.99"))
	if r := s.betConflictReason("kalshi", "KXETHD-26JUL0817-T2509.99", "YES", "gate", pos); r != "dup-underlying" {
		t.Fatalf("API-verified identical-payoff twin must refuse (dup-underlying), got %q", r)
	}
	// CORRELATED-DISTINCT (the operator's correction): F5 vs full game is ALLOWED.
	pos = []paper.Position{{Platform: "kalshi", Ticker: "KXMLBGAME-26JUL071415MILSTLG1-MIL", Contracts: 2, CostBasis: 1}}
	if r := s.betConflictReason("kalshi", "KXMLBF5-26JUL071415MILSTLG1-MIL", "YES", "auto-cons-kalshi", pos); r != "" {
		t.Fatalf("F5 vs full game = correlated-distinct, must be ALLOWED, got %q", r)
	}
	// Spread ladder second line is allowed too (the four-Dodgers correction) …
	pos = []paper.Position{{Platform: "kalshi", Ticker: "KXMLBSPREAD-26JUL07COLLAD-LAD15", Contracts: 2, CostBasis: 1}}
	if r := s.betConflictReason("kalshi", "KXMLBSPREAD-26JUL07COLLAD-LAD25", "YES", "auto-cons-kalshi", pos); r != "" {
		t.Fatalf("a second spread line = correlated-distinct, must be ALLOWED, got %q", r)
	}
	// … and R112: the cap DEFAULTS OFF — cluster exposure is information, not a brake.
	pos = []paper.Position{
		{Platform: "kalshi", Ticker: "KXMLBSPREAD-26JUL07COLLAD-LAD15", Contracts: 100, CostBasis: 30},
		{Platform: "kalshi", Ticker: "KXMLBGAME-26JUL07COLLAD-LAD", Contracts: 100, CostBasis: 30},
	}
	if r := s.betConflictReason("kalshi", "KXMLBSPREAD-26JUL07COLLAD-LAD25", "YES", "auto-cons-kalshi", pos); r != "" {
		t.Fatalf("cap defaults OFF (R112) — cluster stake must be allowed, got %q", r)
	}
	// … until the operator re-arms it with a positive dollar value: then the brake binds.
	s.mutateCfg(func(c *config.Config) { c.Auto.ClusterExposureCap = 50 })
	pos = []paper.Position{
		{Platform: "kalshi", Ticker: "KXMLBSPREAD-26JUL07COLLAD-LAD15", Contracts: 100, CostBasis: 30},
		{Platform: "kalshi", Ticker: "KXMLBGAME-26JUL07COLLAD-LAD", Contracts: 100, CostBasis: 30},
	}
	if r := s.betConflictReason("kalshi", "KXMLBSPREAD-26JUL07COLLAD-LAD25", "YES", "auto-cons-kalshi", pos); r != "corr-cluster-cap" {
		t.Fatalf("cluster stake past cluster_exposure_cap must refuse (corr-cluster-cap), got %q", r)
	}
	// The cap is an AUTO brake only — gate/manual is operator territory.
	if r := s.betConflictReason("kalshi", "KXMLBSPREAD-26JUL07COLLAD-LAD25", "YES", "gate", pos); r != "" {
		t.Fatalf("gate source must not be cluster-capped, got %q", r)
	}
}

func TestR111RawFlowCorrelatedAllowed(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) {
		c.Auto.PaperTotalStart = 1000
		c.Auto.AllocKalshi, c.Auto.AllocPolyus, c.Auto.AllocML, c.Auto.AllocRawFlow = 0.35, 0.10, 0.35, 0.20
	})
	s.rfBookMu.Lock()
	b := s.rfLoadLocked()
	b.Open = append(b.Open, kfPos{TS: time.Now().UTC().Format(time.RFC3339),
		Ticker: "KXWCGAME-26JUL07ARGEGY-ARG", Title: "Argentina wins", Side: "YES", Price: 0.55, Contracts: 5})
	s.rfBookMu.Unlock()
	// R111: ARG winner + ARG −1.5 spread are DIFFERENT payoffs — the book may hold both
	// (R110 called this the "proven ARG+ARG2 hole"; the operator refuted that).
	if r := s.betConflictReason("kalshi", "KXWCSPREAD-26JUL07ARGEGY-ARG2", "YES", "auto-cons-rawflow", nil); r != "" {
		t.Fatalf("correlated-distinct RawFlow variant must be ALLOWED, got %q", r)
	}
	// Opposing a correlated variant is not an exact hedge either — allowed (tracked as cluster).
	if r := s.betConflictReason("kalshi", "KXWCSPREAD-26JUL07ARGEGY-ARG2", "NO", "auto-cons-kalshi", nil); r != "" {
		t.Fatalf("opposite side of a correlated-distinct variant must be allowed, got %q", r)
	}
	// The EXACT ticker hedge protection is unchanged.
	if r := s.betConflictReason("kalshi", "KXWCGAME-26JUL07ARGEGY-ARG", "NO", "auto-cons-kalshi", nil); r != "rawflow-hedge" {
		t.Fatalf("exact-ticker opposite side must still refuse (rawflow-hedge), got %q", r)
	}
	// A VERIFIED TRUE twin of a held RawFlow lot still self-blocks (synthetic twin fixture).
	seedTwinIdentical(s, trueRestatementKey("kalshi", "KXETH-26JUL0817-T2509.99"))
	s.rfBookMu.Lock()
	b = s.rfLoadLocked()
	b.Open = append(b.Open, kfPos{TS: time.Now().UTC().Format(time.RFC3339),
		Ticker: "KXETH-26JUL0817-T2509.99", Title: "ETH", Side: "YES", Price: 0.4, Contracts: 5})
	s.rfBookMu.Unlock()
	if r := s.betConflictReason("kalshi", "KXETHD-26JUL0817-T2509.99", "YES", "auto-cons-rawflow", nil); r != "rawflow-dup-underlying" {
		t.Fatalf("true twin of a held RawFlow lot must refuse (rawflow-dup-underlying), got %q", r)
	}
	// Cluster cap across a BOOK: pump the held cluster stake past the cap → auto brake.
	s.mutateCfg(func(c *config.Config) { c.Auto.ClusterExposureCap = 2 })
	if r := s.betConflictReason("kalshi", "KXWCSPREAD-26JUL07ARGEGY-ARG2", "YES", "auto-cons-rawflow", nil); r != "corr-cluster-cap" {
		t.Fatalf("book cluster stake past cap must refuse (corr-cluster-cap), got %q", r)
	}
}

// seedTwinIdentical marks a twin key as API-verified identical (twins.go fixture).
func seedTwinIdentical(s *Server, tkey string) {
	s.twinMu.Lock()
	if s.twinVerdict == nil {
		s.twinVerdict = map[string]string{}
	}
	s.twinLoaded = true
	s.twinVerdict[tkey] = "identical"
	s.twinMu.Unlock()
}

// R112 REWRITE: legality is API-oracle-driven. No inferred rules — the probe cache answers;
// no answer = "unprobed" + queued (highest EV first). Structural pre-filter only for what the
// API cannot combine (cross-venue) and verified-identical twins (degenerate).
func TestR112PlabLegality(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	if !s.twinLoad(ctx) {
		t.Fatal("empty persisted twin registry must load")
	}
	kyes := func(tk string) plabLeg {
		return plabLeg{Ticker: tk, Side: "YES", Platform: "kalshi", Price: 0.5, PWin: 0.55}
	}
	// Clean all-Kalshi pair with no cached venue answer → unprobed, queued for probing.
	pair := []plabLeg{kyes("KXWNBAGAME-26JUL07DALNY-DAL"), kyes("KXMLBGAME-26JUL07COLLAD-LAD")}
	if g, corr := s.plabLegality(ctx, pair, 0.02); g != "unprobed" || corr {
		t.Fatalf("no venue answer yet = unprobed/!corr, got %q/%v", g, corr)
	}
	s.cprobeMu.Lock()
	if len(s.cprobeQueue) != 1 {
		t.Fatalf("unprobed candidate must be queued, queue=%d", len(s.cprobeQueue))
	}
	s.cprobeMu.Unlock()
	// Cross-venue → synthetic only (structural: PolyUS combos are institutional-beta; poly-int has none).
	pus := plabLeg{Ticker: "aec-mlb-mil-stl-2026-07-07", Side: "YES", Platform: "polyus", Price: 0.5, PWin: 0.55}
	if g, _ := s.plabLegality(ctx, []plabLeg{kyes("KXMLBGAME-26JUL07COLLAD-LAD"), pus}, 0.02); g != "synthetic_legs_only" {
		t.Fatalf("cross-venue = synthetic_legs_only, got %q", g)
	}
	// UNVERIFIED twin pair → NOT illegal (operator default: distinct until the venue proves identical).
	twins := []plabLeg{kyes("KXETH-26JUL0817-T2509.99"), kyes("KXETHD-26JUL0817-T2509.99")}
	if g, _ := s.plabLegality(ctx, twins, 0.02); g == "illegal" {
		t.Fatal("unverified twin pair must not be illegal")
	}
	// VERIFIED identical twin pair → illegal (degenerate, poisons the stats).
	seedTwinIdentical(s, trueRestatementKey("kalshi", "KXETH-26JUL0817-T2509.99"))
	if g, _ := s.plabLegality(ctx, twins, 0.02); g != "illegal" {
		t.Fatalf("verified twin pair = illegal, got %q", g)
	}
	// Same-game correlated legs: corr-tagged, verdict comes from the exchange (unprobed until then).
	if g, corr := s.plabLegality(ctx, []plabLeg{kyes("KXMLBGAME-26JUL071415MILSTLG1-MIL"), kyes("KXMLBF5-26JUL071415MILSTLG1-MIL")}, 0.02); g != "unprobed" || !corr {
		t.Fatalf("same-game correlated pair = unprobed/corr, got %q/%v", g, corr)
	}
	// Cached venue answers are authoritative, both ways, and survive via kv (cprobeStore path).
	s.cprobeStore(ctx, cprobeKey(pair), cprobeVerdict{Verdict: "legal_probed", Collection: "KXMVESPORTSMULTIGAMEEXTENDED-R", At: time.Now()})
	if g, _ := s.plabLegality(ctx, pair, 0.02); g != "legal_probed" {
		t.Fatalf("cached venue yes = legal_probed, got %q", g)
	}
	refused := []plabLeg{kyes("KXMLBTOTAL-26JUL071415MILSTLG1-9"), kyes("KXMLBTOTAL-26JUL071415MILSTLG1-11")}
	s.cprobeStore(ctx, cprobeKey(refused), cprobeVerdict{Verdict: "illegal_probed", Reason: `{"error":{"code":"invalid_parameters","message":"markets not combinable"}}`, At: time.Now()})
	if g, _ := s.plabLegality(ctx, refused, 0.02); g != "illegal_probed" {
		t.Fatalf("cached venue no = illegal_probed, got %q", g)
	}
	// The probe key is order-insensitive (same leg set = same venue signature).
	if cprobeKey(pair) != cprobeKey([]plabLeg{pair[1], pair[0]}) {
		t.Fatal("cprobeKey must be order-insensitive")
	}
	// Reason strings canonicalize to short buckets for reporting.
	if got := cprobeReasonShort(`{"error":{"code":"invalid_parameters","message":"markets not combinable"}}`); got != "invalid_parameters markets not combinable" {
		t.Fatalf("reason bucket, got %q", got)
	}
}

// R113: the R112 oracle misread the deprecated lookup's 404 not_found as "illegal" and cached
// crypto combos as illegal_probed (refuted by operator screenshots + live replication). The fix:
// (a) verdicts live under a bumped kv namespace so every R112 artifact is orphaned; (b)
// rolling-window misses ("no collection lists all leg events RIGHT NOW") expire after 1h, not
// 24h, because the venue rolls crypto events in near start time.
func TestR113ProbeCacheSemantics(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	if cprobeKVPrefix == "cprobe:" {
		t.Fatal("kv namespace must be bumped past the R112 artifact namespace")
	}
	kyes := func(tk string) plabLeg {
		return plabLeg{Ticker: tk, Side: "YES", Platform: "kalshi", Price: 0.5, PWin: 0.55}
	}
	legs := []plabLeg{kyes("KXBTC15M-26JUL072330-30"), kyes("KXSOLD-26JUL0800-T62.9999")}
	key := cprobeKey(legs)
	// Fresh out-of-window negative → served (fail-closed: still not executable)…
	s.cprobeStore(ctx, key, cprobeVerdict{Verdict: "illegal_probed", Reason: cprobeOutOfWindow, At: time.Now()})
	if v, ok := s.comboVerdict(legs); !ok || v.Verdict != "illegal_probed" {
		t.Fatalf("fresh out-of-window negative must serve, got ok=%v v=%+v", ok, v)
	}
	// …but it expires on the SOFT TTL (event may roll into the collection within the hour).
	s.cprobeStore(ctx, key, cprobeVerdict{Verdict: "illegal_probed", Reason: cprobeOutOfWindow, At: time.Now().Add(-2 * cprobeSoftTTL)})
	if _, ok := s.comboVerdict(legs); ok {
		t.Fatal("out-of-window negative older than the soft TTL must expire")
	}
	// A real venue refusal at the same age keeps the full 24h TTL.
	s.cprobeStore(ctx, key, cprobeVerdict{Verdict: "illegal_probed", Reason: `{"error":{"code":"invalid_parameters","message":"markets not combinable"}}`, At: time.Now().Add(-2 * cprobeSoftTTL)})
	if v, ok := s.comboVerdict(legs); !ok || v.Verdict != "illegal_probed" {
		t.Fatalf("real venue refusal keeps 24h TTL, got ok=%v v=%+v", ok, v)
	}
	// Weekly create budget: spends, caps, and resets on a new week.
	s.cprobeCreateAt, s.cprobeCreateUsed = time.Now(), cprobeCreateWeekly
	if s.cprobeCreateAllow(ctx) {
		t.Fatal("exhausted weekly create budget must refuse")
	}
	s.cprobeCreateAt = time.Now().Add(-8 * 24 * time.Hour)
	if !s.cprobeCreateAllow(ctx) || s.cprobeCreateUsed != 1 {
		t.Fatalf("new week must reset the create budget, used=%d", s.cprobeCreateUsed)
	}
}

// R112: the latency telegram latch — one warn per class per 6h GLOBALLY (survives oscillation),
// one recovery note per warn window (the pre-R112 bug: recovery deleted the latch, so an
// oscillating class re-warned every few minutes).
func TestR112LatencyLatch(t *testing.T) {
	m := newLatMon()
	now := time.Now()
	warms := 0
	recovers := 0
	// oscillate: 3 breaches (warn) → clean (recover) → 3 breaches → clean … for 2 hours
	for cycle := 0; cycle < 20; cycle++ {
		for i := 0; i < 3; i++ {
			_, _, w, _ := latEvalDecide(m, map[string]bool{"polyint_rest": true}, now)
			warms += len(w)
			now = now.Add(30 * time.Second)
		}
		_, _, _, r := latEvalDecide(m, map[string]bool{"polyint_rest": false}, now)
		recovers += len(r)
		now = now.Add(30 * time.Second)
	}
	if warms != 1 {
		t.Fatalf("oscillating class must warn exactly once per 6h, got %d", warms)
	}
	if recovers != 1 {
		t.Fatalf("one recovery note per warn window, got %d", recovers)
	}
	// after 6h+ the latch expires and a sustained breach warns again
	now = now.Add(7 * time.Hour)
	for i := 0; i < 3; i++ {
		_, _, w, _ := latEvalDecide(m, map[string]bool{"polyint_rest": true}, now)
		warms += len(w)
		now = now.Add(30 * time.Second)
	}
	if warms != 2 {
		t.Fatalf("after 6h the latch re-arms, total warns %d != 2", warms)
	}
}
