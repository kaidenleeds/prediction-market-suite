package server

// mlaccuracy_test.go — R117 Part 4: pure-function tests for the real-bet accuracy math
// (AUC / decile bucketing / ECE / discrimination / ICIR thin-sample / plain answer).

import (
	"math"
	"os"
	"strings"
	"testing"
)

func TestMLAccAUCKnownCase(t *testing.T) {
	// wins at p {0.8, 0.6}, losses at p {0.4, 0.7}: pairs (0.8>0.4)=1 (0.8>0.7)=1 (0.6>0.4)=1
	// (0.6<0.7)=0 → AUC = 3/4.
	bets := []mlAccBet{
		{P: 0.8, Y: 1}, {P: 0.6, Y: 1}, {P: 0.4, Y: 0}, {P: 0.7, Y: 0},
	}
	if got := mlAccAUC(bets); math.Abs(got-0.75) > 1e-9 {
		t.Fatalf("AUC = %v, want 0.75", got)
	}
	// tie counts 0.5: one win and one loss both at p=0.5 → AUC 0.5
	if got := mlAccAUC([]mlAccBet{{P: 0.5, Y: 1}, {P: 0.5, Y: 0}}); math.Abs(got-0.5) > 1e-9 {
		t.Fatalf("tied AUC = %v, want 0.5", got)
	}
	// no loser → undefined sentinel
	if got := mlAccAUC([]mlAccBet{{P: 0.5, Y: 1}}); got != -1 {
		t.Fatalf("degenerate AUC = %v, want -1", got)
	}
}

func TestMLAccDecilesAndECE(t *testing.T) {
	// 4 bets in [0.6,0.7): predicted mean 0.65, 3 wins → real 0.75, gap +0.10
	// 2 bets in [0.9,1.0] (1.0 lands in the top bin, inclusive): pred 0.95, 1 win → real 0.5, gap −0.45
	bets := []mlAccBet{
		{P: 0.60, Y: 1}, {P: 0.65, Y: 1}, {P: 0.65, Y: 1}, {P: 0.70, Y: 0}, // 0.70 falls in the NEXT bin
		{P: 0.90, Y: 1}, {P: 1.00, Y: 0},
	}
	d := mlAccDeciles(bets)
	if len(d) != 3 { // [0.6,0.7) n=3, [0.7,0.8) n=1, [0.9,1.0] n=2
		t.Fatalf("deciles = %d buckets (%+v), want 3", len(d), d)
	}
	b0 := d[0]
	if b0.N != 3 || math.Abs(b0.Pred-0.633) > 1e-9 || math.Abs(b0.Real-1.0) > 1e-9 {
		t.Fatalf("bucket0 = %+v", b0)
	}
	top := d[2]
	if top.N != 2 || math.Abs(top.Pred-0.95) > 1e-9 || math.Abs(top.Real-0.5) > 1e-9 || math.Abs(top.Gap-(-0.45)) > 1e-9 {
		t.Fatalf("top bucket = %+v (p=1.0 must be inclusive)", top)
	}
	// ECE = (3*|1−0.633| + 1*|0−0.7| + 2*|0.5−0.95|) / 6
	want := (3*0.367 + 1*0.7 + 2*0.45) / 6
	if got := mlAccECE(d, len(bets)); math.Abs(got-want) > 1e-6 {
		t.Fatalf("ECE = %v, want %v", got, want)
	}
	if got := mlAccECE(nil, 0); got != 0 {
		t.Fatalf("empty ECE = %v, want 0", got)
	}
}

func TestMLAccMAE(t *testing.T) {
	bets := []mlAccBet{{P: 0.8, Y: 1}, {P: 0.8, Y: 0}} // |0.2| + |0.8| → 0.5
	if got := mlAccMAE(bets); math.Abs(got-0.5) > 1e-9 {
		t.Fatalf("MAE = %v, want 0.5", got)
	}
}

func TestMLAccDisc(t *testing.T) {
	bets := []mlAccBet{
		{P: 0.70, Y: 1}, {P: 0.65, Y: 1}, {P: 0.61, Y: 0}, // hi: 2/3
		{P: 0.30, Y: 0}, {P: 0.10, Y: 1}, // lo: 1/2
		{P: 0.50, Y: 1}, {P: 0.60, Y: 1}, {P: 0.40, Y: 0}, // middle band (0.40/0.60 exclusive) — counted nowhere
	}
	hiN, hiWR, loN, loWR := mlAccDisc(bets)
	if hiN != 3 || math.Abs(hiWR-2.0/3.0) > 1e-9 {
		t.Fatalf("hi = n%d wr%v", hiN, hiWR)
	}
	if loN != 2 || math.Abs(loWR-0.5) > 1e-9 {
		t.Fatalf("lo = n%d wr%v", loN, loWR)
	}
}

func TestMLAccICIRThinSample(t *testing.T) {
	now := int64(1_750_000_000)
	// 3 bets on one day — below both the 5-per-day and 5-days floors → nil + reason
	bets := []mlAccBet{
		{P: 0.6, Y: 1, R: 0.4, TS: now - 86400},
		{P: 0.5, Y: 0, R: -0.5, TS: now - 86400},
		{P: 0.7, Y: 1, R: 0.3, TS: now - 86400},
	}
	icir, note := mlAccICIR(bets, now)
	if icir != nil {
		t.Fatalf("thin-sample ICIR = %v, want nil", *icir)
	}
	if !strings.Contains(note, "too thin") {
		t.Fatalf("thin-sample note = %q", note)
	}
	// 6 days × 6 bets with a perfectly consistent ranking → every daily IC = 1, sd = 0 → nil (variance note)
	var big []mlAccBet
	for d := int64(1); d <= 6; d++ {
		for i := 0; i < 6; i++ {
			p := 0.1 + 0.1*float64(i)
			big = append(big, mlAccBet{P: p, R: p, TS: now - d*86400})
		}
	}
	icir, note = mlAccICIR(big, now)
	if icir != nil || !strings.Contains(note, "variance") {
		t.Fatalf("zero-variance ICIR = %v note %q, want nil + variance note", icir, note)
	}
}

func TestMLAccSpearman(t *testing.T) {
	if r, ok := mlAccSpearman([]float64{1, 2, 3, 4}, []float64{10, 20, 30, 40}); !ok || math.Abs(r-1) > 1e-9 {
		t.Fatalf("monotone spearman = %v ok=%v, want 1", r, ok)
	}
	if r, ok := mlAccSpearman([]float64{1, 2, 3}, []float64{9, 5, 1}); !ok || math.Abs(r+1) > 1e-9 {
		t.Fatalf("inverse spearman = %v ok=%v, want -1", r, ok)
	}
	if _, ok := mlAccSpearman([]float64{2, 2, 2}, []float64{1, 2, 3}); ok {
		t.Fatal("zero-variance spearman must report !ok")
	}
}

func TestMLAccPlainAnswer(t *testing.T) {
	// overconfident book: says ~75%, wins 50%
	var bets []mlAccBet
	for i := 0; i < 8; i++ {
		bets = append(bets, mlAccBet{P: 0.75, Y: float64(i % 2)})
	}
	d := mlAccDeciles(bets)
	s := mlAccPlain(d, len(bets))
	if !strings.Contains(s, "75%") || !strings.Contains(s, "50%") || !strings.Contains(s, "overconfident") {
		t.Fatalf("plain answer = %q", s)
	}
	if !strings.Contains(s, "8 settled") {
		t.Fatalf("plain answer missing n: %q", s)
	}
	// calibrated book (gap < 2pt) → "about right"
	cal := []mlAccBet{{P: 0.5, Y: 1}, {P: 0.5, Y: 0}}
	if s2 := mlAccPlain(mlAccDeciles(cal), 2); !strings.Contains(s2, "about right") {
		t.Fatalf("calibrated answer = %q", s2)
	}
	if s3 := mlAccPlain(nil, 0); !strings.Contains(s3, "No settled") {
		t.Fatalf("empty answer = %q", s3)
	}
}

func TestMLAccReadBookFailsClosedWithoutEpoch(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/book.json"
	body := `{"closed":[
		{"reset_close":true,"p_win":0.9,"won":1},
		{"p_win":0.7,"won":1,"pnl":0.6,"contracts":2,"closed_ts":100,"model_cohort":"book-native-v2"},
		{"p_win":0.8,"won":1,"pnl":0.7,"contracts":2,"closed_ts":101,"model_cohort":"legacy-v1"},
		{"won":1},
		{"p_win":1.4,"won":0}
	]}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	bets := mlAccReadBook(path, "ml_paper")
	if len(bets) != 0 {
		t.Fatalf("bets = %d (%+v), missing epoch/reset must fail closed", len(bets), bets)
	}
	if got := mlAccReadBook(dir+"/missing.json", "x"); got != nil {
		t.Fatalf("missing file → %v, want nil", got)
	}
}
