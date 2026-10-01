package server

// r126combo_test.go — R126 Part 3 pins (hermetic — no venue client, no store, no network):
//   1. the ticker classifier (series suffix → ml/spread/total + sport base),
//   2. the pair sampler (same-game corr pairs spread across class pairs + sports; cross-game
//      indep controls preferring different sports; round-robin fairness under a small budget),
//   3. product-vs-adjusted joint math against a stubbed correlation cell (exact plabRho parity,
//      and the indep control collapsing to the pure product),
//   4. param clamping (defaults + hard caps; create defaults CLOSED).

import (
	"math"
	"testing"
)

func TestR126ComboClassifier(t *testing.T) {
	cases := []struct {
		tk, class, sport string
		ok               bool
	}{
		{"KXMLBGAME-26JUL071840NYYTB-TB", "ml", "KXMLB", true},
		{"KXMLBSPREAD-26JUL011507DETNYY-2", "spread", "KXMLB", true},
		{"KXMLBTOTAL-26JUL011507DETNYY-11", "total", "KXMLB", true},
		{"KXWNBATOTAL-26JUL09SEAATL-160", "total", "KXWNBA", true},
		{"kxwnbagame-26jul09seaatl-sea", "ml", "KXWNBA", true}, // series match is case-insensitive
		{"KXBTCD-26JUL0213-T108000", "", "", false},            // crypto — not a class market
		{"KXHIGHNY-26JUL09-B90", "", "", false},                // weather
		{"KXWNBAPTS-26JUL09SEAATL-AW25", "", "", false},        // player props — no class suffix
		{"NODASH", "", "", false},
	}
	for _, c := range cases {
		class, sport, ok := r126Classify(c.tk)
		if class != c.class || sport != c.sport || ok != c.ok {
			t.Errorf("r126Classify(%q) = (%q,%q,%v), want (%q,%q,%v)", c.tk, class, sport, ok, c.class, c.sport, c.ok)
		}
	}
	if got := r126PairClass("total", "ml"); got != "ML+TOTAL" { // order-insensitive label
		t.Errorf("r126PairClass(total,ml) = %q, want ML+TOTAL", got)
	}
	if got := r126PairClass("spread", "total"); got != "SPREAD+TOTAL" {
		t.Errorf("r126PairClass(spread,total) = %q, want SPREAD+TOTAL", got)
	}
}

func r126TestMkt(t *testing.T, tk, gk string) r126Mkt {
	t.Helper()
	class, sport, ok := r126Classify(tk)
	if !ok {
		t.Fatalf("test market %q did not classify", tk)
	}
	return r126Mkt{Ticker: tk, Event: "EV-" + tk, GameKey: gk, Class: class, Sport: sport, Bid: 0.48, Ask: 0.52}
}

func TestR126ComboPairSampling(t *testing.T) {
	g1, g2, g3 := "kgame:26JUL091840NYYTB", "kgame:26JUL091910KCNYM", "kgame:26JUL09SEAATL"
	mkts := []r126Mkt{
		// game 1 (MLB): all three classes
		r126TestMkt(t, "KXMLBGAME-26JUL091840NYYTB-NYY", g1),
		r126TestMkt(t, "KXMLBSPREAD-26JUL091840NYYTB-2", g1),
		r126TestMkt(t, "KXMLBTOTAL-26JUL091840NYYTB-9", g1),
		// game 2 (MLB): ml + total only
		r126TestMkt(t, "KXMLBGAME-26JUL091910KCNYM-KC", g2),
		r126TestMkt(t, "KXMLBTOTAL-26JUL091910KCNYM-8", g2),
		// game 3 (WNBA): all three classes
		r126TestMkt(t, "KXWNBAGAME-26JUL09SEAATL-SEA", g3),
		r126TestMkt(t, "KXWNBASPREAD-26JUL09SEAATL-SEA5", g3),
		r126TestMkt(t, "KXWNBATOTAL-26JUL09SEAATL-160", g3),
	}

	pairs := r126SamplePairs(mkts, 40, 20)
	classCount := map[string]int{}
	nCorr, nIndep := 0, 0
	for _, p := range pairs {
		switch p.Kind {
		case "corr":
			nCorr++
			classCount[p.Class]++
			if p.A.GameKey != p.B.GameKey || p.A.GameKey != p.GameKey {
				t.Errorf("corr pair %s+%s must share its game key", p.A.Ticker, p.B.Ticker)
			}
			if p.A.Ticker == p.B.Ticker {
				t.Errorf("degenerate pair %s", p.A.Ticker)
			}
		case "indep":
			nIndep++
			if p.A.GameKey == p.B.GameKey {
				t.Errorf("indep pair %s+%s must span different games", p.A.Ticker, p.B.Ticker)
			}
		default:
			t.Errorf("unknown pair kind %q", p.Kind)
		}
	}
	// corr: game1 3 class pairs + game2 1 (ML+TOTAL) + game3 3 = 7
	if nCorr != 7 {
		t.Fatalf("corr pairs = %d, want 7 (%v)", nCorr, classCount)
	}
	if classCount["ML+SPREAD"] != 2 || classCount["ML+TOTAL"] != 3 || classCount["SPREAD+TOTAL"] != 2 {
		t.Fatalf("class spread wrong: %v", classCount)
	}
	// indep: 3 game representatives → 1 disjoint pair, and the sport-interleave makes it
	// CROSS-SPORT (MLB + WNBA) even though two MLB games were adjacent in game order.
	if nIndep != 1 {
		t.Fatalf("indep pairs = %d, want 1", nIndep)
	}
	ind := pairs[len(pairs)-1]
	if ind.A.Sport == ind.B.Sport {
		t.Errorf("indep pair should prefer different sports, got %s+%s", ind.A.Ticker, ind.B.Ticker)
	}

	// small budget: round-robin must spread across DIFFERENT class pairs, not exhaust one class
	pairs = r126SamplePairs(mkts, 2, 0)
	if len(pairs) != 2 {
		t.Fatalf("nCorr=2 must yield exactly 2 pairs, got %d", len(pairs))
	}
	if pairs[0].Class == pairs[1].Class {
		t.Errorf("round-robin fairness: 2 pairs landed in one class %q", pairs[0].Class)
	}

	// nIndep=0 yields no controls
	for _, p := range r126SamplePairs(mkts, 1, 0) {
		if p.Kind != "corr" {
			t.Errorf("nIndep=0 must yield no indep pairs")
		}
	}
}

func TestR126ComboJointMath(t *testing.T) {
	a := r126Mkt{Ticker: "KXMLBGAME-26JUL091840NYYTB-NYY", GameKey: "kgame:26JUL091840NYYTB",
		Class: "ml", Sport: "KXMLB", Bid: 0.58, Ask: 0.60}
	b := r126Mkt{Ticker: "KXMLBTOTAL-26JUL091840NYYTB-9", GameKey: "kgame:26JUL091840NYYTB",
		Class: "total", Sport: "KXMLB", Bid: 0.40, Ask: 0.44}

	// the pair class the joint math looks up must be the EXACT live-lab cell name
	pairClass := plabClass([]plabLeg{
		{Ticker: a.Ticker, EventKey: a.GameKey},
		{Ticker: b.Ticker, EventKey: b.GameKey},
	}, a.GameKey)
	if pairClass != "ov:kgame:KXMLBGAME+KXMLBTOTAL" {
		t.Fatalf("pair class = %q, want ov:kgame:KXMLBGAME+KXMLBTOTAL", pairClass)
	}

	cell := plabCorrC{N: 60, A: 30, B: 30, AB: 22} // positively correlated counters
	s := &Server{plabCorr: map[string]*plabCorrC{pairClass: &cell}}

	s.plabMu.Lock()
	prod, adj, rho, rhoN := s.r126JointForPair(r126Pair{Kind: "corr", Class: "ML+TOTAL", GameKey: a.GameKey, A: a, B: b})
	bFar := b
	bFar.GameKey = "kgame:26JUL09OTHERGM"
	prodI, adjI, rhoI, _ := s.r126JointForPair(r126Pair{Kind: "indep", Class: "INDEP", A: a, B: bFar})
	s.plabMu.Unlock()

	near := func(x, y float64) bool { return math.Abs(x-y) < 1e-9 }
	p1, p2 := 0.59, 0.42
	if !near(prod, p1*p2) {
		t.Fatalf("product = %v, want %v", prod, p1*p2)
	}
	wantRho, wantN := plabRho(cell)
	if wantRho <= 0 {
		t.Fatalf("stub cell must produce positive rho, got %v", wantRho)
	}
	if !near(rho, wantRho) || rhoN != wantN {
		t.Fatalf("rho = (%v,%d), want (%v,%d)", rho, rhoN, wantRho, wantN)
	}
	wantAdj := clampF(p1*p2+wantRho*math.Sqrt(p1*(1-p1)*p2*(1-p2)), 0.0005, 0.9995)
	wantAdj = clampF(wantAdj, 0.0001, 0.9999)
	if !near(adj, wantAdj) {
		t.Fatalf("adjusted joint = %v, want %v", adj, wantAdj)
	}
	if adj <= prod {
		t.Fatalf("positive rho must lift the joint above the product: adj=%v prod=%v", adj, prod)
	}
	// indep control: no overlap group → pure product, rho 0
	if !near(prodI, adjI) || rhoI != 0 {
		t.Fatalf("indep pair must collapse to the product: prod=%v adj=%v rho=%v", prodI, adjI, rhoI)
	}
}

func TestR126ComboParamClamp(t *testing.T) {
	cases := []struct {
		inC, inI, inL, inCr int
		want                r126Params
	}{
		{0, 0, 0, 0, r126Params{NCorr: 20, NIndep: 10, Lookups: 40, Creates: 0}},     // all defaults
		{-3, -1, -5, -2, r126Params{NCorr: 20, NIndep: 10, Lookups: 40, Creates: 0}}, // negatives → defaults / closed
		{100, 100, 100, 100, r126Params{NCorr: 40, NIndep: 20, Lookups: 60, Creates: 20}},
		{5, 5, 5, 5, r126Params{NCorr: 5, NIndep: 5, Lookups: 5, Creates: 5}},
		{40, 20, 60, 20, r126Params{NCorr: 40, NIndep: 20, Lookups: 60, Creates: 20}}, // exactly at caps
	}
	for _, c := range cases {
		if got := r126ClampParams(c.inC, c.inI, c.inL, c.inCr); got != c.want {
			t.Errorf("r126ClampParams(%d,%d,%d,%d) = %+v, want %+v", c.inC, c.inI, c.inL, c.inCr, got, c.want)
		}
	}
}
