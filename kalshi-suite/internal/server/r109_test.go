package server

// R109 pins — parlay lab (log-only research collector): enumeration buckets, correlation
// estimator shrinkage, joint-p composition, class labels, combo identity.

import (
	"math"
	"strings"
	"testing"
)

func TestR109PlabRhoShrinkage(t *testing.T) {
	// low n ⇒ independence (honest estimator)
	if rho, _ := plabRho(plabCorrC{N: 3, A: 3, B: 3, AB: 3}); rho != 0 {
		t.Fatalf("n<5 must return rho 0 (shrunk to independence), got %v", rho)
	}
	// perfectly correlated coin-flip marginals: raw rho = 1, shrunk by n/(n+50)
	c := plabCorrC{N: 100, A: 50, B: 50, AB: 50}
	rho, n := plabRho(c)
	want := 1.0 * 100 / 150
	if n != 100 || math.Abs(rho-want) > 1e-9 {
		t.Fatalf("shrunk rho: got %v (n=%d), want %v", rho, n, want)
	}
	// degenerate marginals (all wins) ⇒ no co-movement information ⇒ 0
	if rho, _ := plabRho(plabCorrC{N: 60, A: 60, B: 60, AB: 60}); rho != 0 {
		t.Fatalf("degenerate marginals must return 0, got %v", rho)
	}
}

func TestR109PlabJointP(t *testing.T) {
	legs := []plabLeg{{PWin: 0.6}, {PWin: 0.5}}
	// independent: plain product
	if jp := plabJointP(legs, false, 0.9); math.Abs(jp-0.3) > 1e-9 {
		t.Fatalf("indep joint p: got %v, want 0.30", jp)
	}
	// linked with rho=0 ⇒ same as independent (shrunk-to-independence at low n)
	if jp := plabJointP(legs, true, 0); math.Abs(jp-0.3) > 1e-9 {
		t.Fatalf("linked rho=0 must equal product, got %v", jp)
	}
	// positive correlation lifts the joint probability
	jp := plabJointP(legs, true, 0.4)
	want := 0.3 + 0.4*math.Sqrt(0.6*0.4*0.5*0.5)
	if math.Abs(jp-want) > 1e-9 {
		t.Fatalf("linked rho=0.4: got %v, want %v", jp, want)
	}
	// third leg multiplies independently
	legs3 := append(legs, plabLeg{PWin: 0.5})
	if jp3 := plabJointP(legs3, true, 0.4); math.Abs(jp3-want*0.5) > 1e-9 {
		t.Fatalf("3-leg: got %v, want %v", jp3, want*0.5)
	}
}

func TestR109PlabEnumerateBuckets(t *testing.T) {
	pool := []plabLeg{
		{Ticker: "KXMLB-26JUL07DETNYY-DET", EventKey: "kgame:26JUL07DETNYY", PWin: 0.6, Price: 0.55, EVNet: 0.03},
		{Ticker: "KXMLBTOTAL-26JUL07DETNYY-9", EventKey: "kgame:26JUL07DETNYY", PWin: 0.55, Price: 0.5, EVNet: 0.02},
		{Ticker: "KXBTCD-26JUL0713-T118000", EventKey: "coin:btc:KXBTCD-26JUL0713", PWin: 0.4, Price: 0.35, EVNet: 0.02},
		{Ticker: "KXHIGHLAX-26JUL07-B80", EventKey: "k:KXHIGHLAX-26JUL07", PWin: 0.7, Price: 0.65, EVNet: 0.01},
	}
	// R123: buckets/classes now derive from plabComboShape over the exhaustive enumerator.
	tw := make([]string, len(pool))
	combos, _ := plabEnumerateEx(pool, tw, 10, 400, -100, nil, timeNowPlus15s())
	var ov, ind, tri int
	for _, legs := range combos {
		bucket, _, shared, _ := plabComboShape(legs)
		if bucket == "overlap" && len(legs) == 2 {
			ov++
			if legs[0].EventKey != legs[1].EventKey || shared == "" {
				t.Fatalf("overlap pair with mismatched event keys: %+v", legs)
			}
		}
		if bucket == "indep" && len(legs) == 2 {
			ind++
			if legs[0].EventKey == legs[1].EventKey {
				t.Fatalf("indep pair sharing an event key: %+v", legs)
			}
		}
		if len(legs) == 3 {
			tri++
		}
	}
	if ov != 1 {
		t.Fatalf("want exactly 1 overlap pair (the DETNYY game), got %d", ov)
	}
	if ind == 0 {
		t.Fatalf("want indep pairs, got none")
	}
	if tri == 0 {
		t.Fatalf("want 3-leg candidates, got none")
	}
}

func TestR109PlabClassAndID(t *testing.T) {
	legs := []plabLeg{
		{Ticker: "KXMLBTOTAL-26JUL07DETNYY-9", Side: "YES", Platform: "kalshi"},
		{Ticker: "KXMLB-26JUL07DETNYY-DET", Side: "YES", Platform: "kalshi"},
	}
	cl := plabClass(legs, "kgame:26JUL07DETNYY")
	if !strings.HasPrefix(cl, "ov:kgame:") || !strings.Contains(cl, "KXMLB+KXMLBTOTAL") {
		t.Fatalf("class label: got %q", cl)
	}
	// R122: 2-leg independents carry a sorted genre pair (cross-genre linkage classes). These
	// legs have no EventKey and non-crypto/weather tickers ⇒ genre "other" both.
	if got := plabClass(legs, ""); got != "ind:2leg:other+other" {
		t.Fatalf("indep class: got %q", got)
	}
	// mixed-genre pair sorts deterministically
	gl := []plabLeg{
		{Ticker: "KXBTCD-1", EventKey: "coin:btc:KXBTCD-26JUL0713"},
		{Ticker: "KXHIGHLAX-26JUL07-B80", EventKey: "k:KXHIGHLAX-26JUL07"},
	}
	if got := plabClass(gl, ""); got != "ind:2leg:crypto+weather" {
		t.Fatalf("cross-genre indep class: got %q", got)
	}
	// identity is order-invariant
	id1 := plabID(legs)
	id2 := plabID([]plabLeg{legs[1], legs[0]})
	if id1 != id2 || len(id1) != 16 {
		t.Fatalf("plabID must be order-invariant: %q vs %q", id1, id2)
	}
}
