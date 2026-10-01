package server

// r123_test.go — R123 pins:
//   1. plabEnumerateEx COMPLETENESS: brute-force set equality on a mixed pool (the "provably
//      complete" claim, tested literally), twin-key restatement exclusion, budget/threshold
//      honesty (floor is the only cut — every emitted combo clears the reported T).
//   2. plabComboShape / plabJointPEx multi-group generalization + plabCorrSlack bound.
//   3. Scoreboard CIs (plabCellCI) + proven-positive class detection (Part 3).
//   4. mleval: calibrator np.interp semantics, the 1e-6 parity gate (accept + refuse),
//      tick-score overlay + binary complement + freshness fallback (Part 1).

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"testing"
	"time"
)

func timeNowPlus15s() time.Time { return time.Now().Add(15 * time.Second) }

// ---- Part 2: exhaustive enumeration ----------------------------------------------------------

func TestR123EnumCompletenessBruteForce(t *testing.T) {
	// 12 legs, 3 shared events, 2 twin-key pairs, one deliberately poor leg (log-edge < 0).
	mk := func(i int, ev, twin string, p, px float64) (plabLeg, string) {
		return plabLeg{Ticker: fmt.Sprintf("T%02d", i), Side: "YES", Platform: "kalshi",
			EventKey: ev, PWin: p, Price: px}, twin
	}
	var pool []plabLeg
	var twins []string
	add := func(l plabLeg, tw string) { pool = append(pool, l); twins = append(twins, tw) }
	add(mk(0, "e1", "", 0.60, 0.50))
	add(mk(1, "e1", "", 0.55, 0.50))
	add(mk(2, "e2", "tw1", 0.40, 0.35))
	add(mk(3, "e2", "tw1", 0.42, 0.36)) // twin of leg 2 — may never co-occur with it
	add(mk(4, "e3", "", 0.70, 0.65))
	add(mk(5, "e4", "", 0.30, 0.28))
	add(mk(6, "e5", "", 0.80, 0.74))
	add(mk(7, "e6", "tw2", 0.52, 0.49))
	add(mk(8, "e7", "tw2", 0.66, 0.61))
	add(mk(9, "e8", "", 0.25, 0.26)) // NEGATIVE log-edge — must still enumerate when the sum clears
	add(mk(10, "e9", "", 0.45, 0.41))
	add(mk(11, "e1", "", 0.35, 0.33))
	const maxLegs, T = 4, 0.12
	combos, st := plabEnumerateEx(pool, twins, maxLegs, 1_000_000, T, nil, timeNowPlus15s())
	if !st.Complete {
		t.Fatalf("unbounded budget must complete (st=%+v)", st)
	}
	got := map[string]bool{}
	for _, legs := range combos {
		sum := 0.0
		for _, l := range legs {
			sum += math.Log(l.PWin / l.Price)
		}
		if sum < T-1e-12 {
			t.Fatalf("emitted combo below threshold: %v sum=%v", legs, sum)
		}
		got[plabID(legs)] = true
	}
	// brute force: every subset of size 2..4, twin-dedup, Σ log-edge ≥ T
	want := map[string]bool{}
	n := len(pool)
	var rec func(start int, idxs []int)
	rec = func(start int, idxs []int) {
		if len(idxs) >= 2 {
			tw := map[string]int{}
			dup := false
			sum := 0.0
			legs := make([]plabLeg, len(idxs))
			for x, i := range idxs {
				legs[x] = pool[i]
				sum += math.Log(pool[i].PWin / pool[i].Price)
				if twins[i] != "" {
					tw[twins[i]]++
					if tw[twins[i]] > 1 {
						dup = true
					}
				}
			}
			if !dup && sum >= T {
				want[plabID(legs)] = true
			}
		}
		if len(idxs) == maxLegs {
			return
		}
		for j := start; j < n; j++ {
			rec(j+1, append(idxs, j))
		}
	}
	rec(0, nil)
	if len(got) != len(want) {
		t.Fatalf("completeness: enumerated %d vs brute-force %d", len(got), len(want))
	}
	for id := range want {
		if !got[id] {
			t.Fatalf("brute-force combo %s missing from enumeration", id)
		}
	}
}

func TestR123EnumBudgetThresholdHonesty(t *testing.T) {
	// 14 all-positive legs → space at floor 0 is Σ C(14,k) k=2..6 = huge vs budget 25:
	// the floor stays the only cut — T rises, every emitted combo clears the REPORTED T.
	var pool []plabLeg
	tw := make([]string, 14)
	for i := 0; i < 14; i++ {
		pool = append(pool, plabLeg{Ticker: fmt.Sprintf("B%02d", i), EventKey: fmt.Sprintf("ev%d", i),
			PWin: 0.50 + 0.02*float64(i%7), Price: 0.45, Side: "YES", Platform: "kalshi"})
	}
	combos, st := plabEnumerateEx(pool, tw, 10, 25, 0, nil, timeNowPlus15s())
	if len(combos) == 0 || len(combos) > 25 {
		t.Fatalf("budget must bound emissions: got %d", len(combos))
	}
	if st.Complete || st.CountAtFloor != -1 {
		t.Fatalf("over-budget space must report incomplete + CountAtFloor=-1 (st=%+v)", st)
	}
	if st.TEffective < st.FloorLogEdge {
		t.Fatalf("walked threshold %v below the floor %v", st.TEffective, st.FloorLogEdge)
	}
	for _, legs := range combos {
		sum := 0.0
		for _, l := range legs {
			sum += math.Log(l.PWin / l.Price)
		}
		if sum < st.TEffective-1e-9 {
			t.Fatalf("emitted combo (Σ %.6f) below reported T %.6f — sampling, not a floor cut", sum, st.TEffective)
		}
	}
	if st.RawSpace != 6461 {
		t.Fatalf("raw space must report the hard-capped Σ C(14,2..6): got %v", st.RawSpace)
	}
}

func TestR123EnumTwinExclusion(t *testing.T) {
	pool := []plabLeg{
		{Ticker: "A", EventKey: "e1", PWin: 0.6, Price: 0.5},
		{Ticker: "B", EventKey: "e2", PWin: 0.6, Price: 0.5},
		{Ticker: "C", EventKey: "e3", PWin: 0.6, Price: 0.5},
	}
	twins := []string{"tw", "tw", ""}
	combos, _ := plabEnumerateEx(pool, twins, 3, 1000, -10, nil, timeNowPlus15s())
	for _, legs := range combos {
		seenAB := 0
		for _, l := range legs {
			if l.Ticker == "A" || l.Ticker == "B" {
				seenAB++
			}
		}
		if seenAB > 1 {
			t.Fatalf("twin-key pair co-occurred (restatement guard): %+v", legs)
		}
	}
	// A+C, B+C, A+B(excluded), A+B+C(excluded) ⇒ exactly 2 combos
	if len(combos) != 2 {
		t.Fatalf("want exactly 2 twin-legal combos, got %d", len(combos))
	}
}

func TestR123ComboShapeAndJointP(t *testing.T) {
	legs := []plabLeg{
		{Ticker: "KXMLB-X-DET", EventKey: "kgame:g1", PWin: 0.6},
		{Ticker: "KXMLBTOTAL-X-9", EventKey: "kgame:g1", PWin: 0.5},
		{Ticker: "KXNBA-Y-LAL", EventKey: "kgame:g2", PWin: 0.7},
		{Ticker: "KXNBATOTAL-Y-200", EventKey: "kgame:g2", PWin: 0.4},
		{Ticker: "KXBTCD-1", EventKey: "coin:btc:w1", PWin: 0.55},
	}
	bucket, class, _, ovg := plabComboShape(legs)
	if bucket != "overlap" || len(ovg) != 2 {
		t.Fatalf("two-group combo: bucket=%q groups=%d", bucket, len(ovg))
	}
	if class != "ov:multi:5leg" {
		t.Fatalf("multi-group class: got %q", class)
	}
	// one-group naming must keep the R109 scheme exactly (kv cells keep aggregating)
	_, c1, _, _ := plabComboShape(legs[:2])
	if c1 != plabClass(legs[:2], "kgame:g1") {
		t.Fatalf("one-group class must match the R109 label: %q", c1)
	}
	// joint p: rho applies per group's first pair, read from the PAIR class cell
	s := &Server{plabCorr: map[string]*plabCorrC{}}
	pairClass := plabClass([]plabLeg{legs[0], legs[1]}, "kgame:g1")
	s.plabCorr[pairClass] = &plabCorrC{N: 100, A: 50, B: 50, AB: 50} // raw rho 1 → shrunk 2/3
	jp, rhoUsed, _ := s.plabJointPEx(legs, ovg)
	rho := 1.0 * 100 / 150
	p12 := 0.6*0.5 + rho*math.Sqrt(0.6*0.4*0.5*0.5)
	want := p12 * (0.7 * 0.4) * 0.55 // group2 has no corr cell → independent; leg5 independent
	if math.Abs(jp-want) > 1e-9 || math.Abs(rhoUsed-rho) > 1e-9 {
		t.Fatalf("jointPEx: got %v (rho %v), want %v (rho %v)", jp, rhoUsed, want, rho)
	}
	// slack: bounded by the positive-rho pair boost, > 0 with the cell present
	slack := s.plabCorrSlack(legs, 10)
	if slack <= 0 || slack > math.Log(clampF(p12, 0.0005, 0.9995)/(0.6*0.5))+1e-9 {
		t.Fatalf("corr slack out of range: %v", slack)
	}
	if s2 := (&Server{plabCorr: map[string]*plabCorrC{}}).plabCorrSlack(legs, 10); s2 != 0 {
		t.Fatalf("no corr cells ⇒ slack 0, got %v", s2)
	}
}

// ---- Part 3: scoreboard CIs -------------------------------------------------------------------

func TestR123ScoreboardCIAndProven(t *testing.T) {
	componentCell := func(n int, low, high float64) *plabAgg {
		a := &plabAgg{N: n, N2: n}
		for i := 0; i < n; i++ {
			v := low
			if i%2 == 1 {
				v = high
			}
			a.Outcomes = append(a.Outcomes, v)
			a.IndependenceGroups = append(a.IndependenceGroups, []string{fmt.Sprintf("independent-%d", i)})
			a.Sum2 += v
			a.Sq2 += v * v
		}
		return a
	}
	// 30 post-R123 rows at +0.50/$1 with tiny variance → CI-lo must clear 0 ⇒ proven
	strong := componentCell(30, .49, .51)
	mean, lo, _, n, ok := plabCellCI(strong)
	if !ok || n != 30 || math.Abs(mean-0.5) > 1e-9 || lo <= 0 {
		t.Fatalf("strong cell must be CI-positive: mean=%v lo=%v ok=%v", mean, lo, ok)
	}
	// pre-R123 cell (no squares) → no CI
	if _, _, _, _, ok := plabCellCI(&plabAgg{N: 500, SumReal: 100}); ok {
		t.Fatal("pre-R123 cell without squares must not fake a CI")
	}
	// One new grade (or a zero-variance cohort) has an unbounded CS. It must stay collecting and
	// must never leak ±Inf into /api/parlaylab's JSON.
	if mean, lo, hi, n, ok := plabCellCI(componentCell(1, -1.07, -1.07)); ok || n != 1 || mean != -1.07 || lo != 0 || hi != 0 {
		t.Fatalf("unbounded one-row CI must be omitted and finite: mean=%v lo=%v hi=%v n=%d ok=%v", mean, lo, hi, n, ok)
	}
	s := &Server{plabStats: map[string]*plabAgg{
		"overlap|2leg|ov:kgame:A+B|legal_probed": strong,
		"indep|2leg|ind:2leg:other+other|":       componentCell(30, -.11, -.09),
		"indep|3leg|ind:3leg|":                   componentCell(10, .89, .91), // n<20 — never proven
	}}
	proven := s.plabProvenClassesLocked()
	if !proven["ov:kgame:A+B"] || proven["ind:2leg:other+other"] || proven["ind:3leg"] {
		t.Fatalf("proven detection wrong: %+v", proven)
	}
}

// ---- Part 1: mleval ---------------------------------------------------------------------------

func TestR123MLEvalCalibApply(t *testing.T) {
	iso := &mlEvalCalib{Method: "isotonic", X: []float64{0.2, 0.4, 0.8}, Y: []float64{0.1, 0.5, 0.9}}
	cases := [][2]float64{
		{0.0, 0.1}, {0.2, 0.1}, {0.3, 0.3}, {0.4, 0.5}, {0.6, 0.7}, {0.8, 0.9}, {1.0, 0.9},
	}
	for _, c := range cases {
		if got := iso.apply(c[0]); math.Abs(got-c[1]) > 1e-12 {
			t.Fatalf("isotonic apply(%v): got %v want %v", c[0], got, c[1])
		}
	}
	sig := &mlEvalCalib{Method: "sigmoid", A: -2, B: 0.5}
	want := 1.0 / (1.0 + math.Exp(-2*0.7+0.5))
	if got := sig.apply(0.7); math.Abs(got-want) > 1e-12 {
		t.Fatalf("sigmoid apply: got %v want %v", got, want)
	}
}

func synthExport(t *testing.T, corrupt bool) []byte {
	t.Helper()
	var e mlEvalExport
	e.Version = "test-1"
	e.FeatureSchema = mlBookFeatureSchema
	e.Backend = "test"
	e.CalibMethod = "sigmoid"
	e.NFeatures = 2
	e.FeatureNames = []string{"entry_price", "momentum"}
	fold := mlEvalFold{Cmp: "le", Acc: "f64", Bias: 0, CalInput: "margin",
		Calib: mlEvalCalib{Method: "sigmoid", A: -1, B: 0},
		Trees: []mlEvalTree{{
			F: []int32{0, -1, -1}, T: []float64{0.5, -1.0, 1.0}, L: []int32{1, -1, -1}, R: []int32{2, -1, -1},
		}}}
	e.Folds = []mlEvalFold{fold}
	e.Parity.N = 2
	e.Parity.Vectors = [][]float64{{0.0, 0}, {1.0, 0}}
	pl := func(m float64) float64 { return 1.0 / (1.0 + math.Exp(-m)) }
	e.Parity.PWin = []float64{pl(-1), pl(1)}
	if corrupt {
		e.Parity.PWin[1] += 5e-6 // outside the 1e-6 gate
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestR123MLEvalParityGate(t *testing.T) {
	m, err := mlEvalParse(synthExport(t, false))
	if err != nil {
		t.Fatalf("healthy export must load: %v", err)
	}
	if m.parityMax > 1e-12 {
		t.Fatalf("exact synthetic pairs must replay exactly, got %v", m.parityMax)
	}
	if p := m.pwin([]float64{0.0, 0}); math.Abs(p-1.0/(1.0+math.Exp(1))) > 1e-12 {
		t.Fatalf("eval: got %v", p)
	}
	if _, err := mlEvalParse(synthExport(t, true)); err == nil {
		t.Fatal("corrupted pair must REFUSE the export (parity is the contract)")
	}
}

func TestR123MLEvalOverlayComplementFreshness(t *testing.T) {
	s := testServer(t)
	m, err := mlEvalParse(synthExport(t, false))
	if err != nil {
		t.Fatal(err)
	}
	vec := &mlEvalVec{Ticker: "TICK-1", Side: "YES", Platform: "polyus", PWin: 0.7, X: []float64{0.9, 0}}
	s.mlEval.model = m
	s.mlEval.vecs = map[string]*mlEvalVec{"polyus|TICK-1|YES": vec}
	s.mlEval.byTicker = map[string][]string{"polyus|TICK-1": {"polyus|TICK-1|YES"}}
	want := m.pwin(vec.X)
	s.mlEval.scores = map[string]mlEvalScoreEnt{"polyus|TICK-1|YES": {
		pwin: want, sidecar: vec.PWin, px: .9, at: time.Now(), src: "test"}}
	pw, at, ok := s.goEvalScore("polyus", "TICK-1", "YES")
	if !ok || time.Since(at) > time.Second {
		t.Fatalf("tick score must be readable: ok=%v", ok)
	}
	// x0=0.9 > 0.5 → leaf +1 → margin 1 → sigmoid-calibrated (a=-1,b=0) 1/(1+e^-1)
	want = 1.0 / (1.0 + math.Exp(-1))
	if math.Abs(pw-want) > 1e-12 {
		t.Fatalf("scored p_win: got %v want %v", pw, want)
	}
	// complement side reads 1 − p (the sidecar's all_scored math)
	if pwNo, _, okNo := s.goEvalScore("polyus", "TICK-1", "NO"); !okNo || math.Abs(pwNo-(1-want)) > 1e-12 {
		t.Fatalf("complement: got %v ok=%v want %v", pwNo, okNo, 1-want)
	}
	// overlay freshness: fresh ⇒ tick value; stale ⇒ caller's file value untouched
	if got := s.tickFreshPWin("polyus", "TICK-1", "YES", 0.111); math.Abs(got-want) > 1e-12 {
		t.Fatalf("fresh overlay: got %v", got)
	}
	sc := s.mlEval.scores["polyus|TICK-1|YES"]
	sc.at = time.Now().Add(-10 * time.Minute)
	s.mlEval.scores["polyus|TICK-1|YES"] = sc
	if got := s.tickFreshPWin("polyus", "TICK-1", "YES", 0.111); got != 0.111 {
		t.Fatalf("stale score must fall back to the file value, got %v", got)
	}
	// Book-v1 patching fails closed without an exact full-book snapshot; a ticker mid can no longer
	// rewrite the legacy signal-price input or fabricate executable features.
	m2 := &mlEvalModel{nf: 2, idx: map[string]int{"book_feature_ver": 0, "book_taker_price": 1}}
	x := []float64{1, .4}
	if s.mlEvalPatch(m2, &mlEvalVec{Ticker: "NO-BOOK", Platform: "polyus", Side: "YES"}, x, .8) {
		t.Fatal("book-v1 patch must refuse a ticker mid without exact book/depth/fee truth")
	}
	if x[1] != .4 {
		t.Fatalf("failed patch must not replace the frozen executable ask: %v", x)
	}
}

// BenchmarkR123MLEvalPWin — the honest per-eval latency on the REAL exported model (Windows
// time.Now granularity makes the per-tick ring read 0µs; this is the measured number for the
// docs). Skips when no live export exists (CI/fresh checkout).
func BenchmarkR123MLEvalPWin(b *testing.B) {
	bts, err := osReadFileForBench("../../data/ml_model_export.json")
	if err != nil {
		b.Skip("no live export:", err)
	}
	m, err := mlEvalParse(bts)
	if err != nil {
		b.Fatal(err)
	}
	x := make([]float64, m.nf)
	copy(x, mlEvalBenchVector(bts, m.nf))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.pwin(x)
	}
}

func osReadFileForBench(p string) ([]byte, error) { return os.ReadFile(p) }

func mlEvalBenchVector(raw []byte, nf int) []float64 {
	var e mlEvalExport
	if json.Unmarshal(raw, &e) == nil && len(e.Parity.Vectors) > 0 {
		return e.Parity.Vectors[0]
	}
	return make([]float64, nf)
}

// keep sort import live for future table-driven additions
var _ = sort.Float64s
