package server

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
)

func TestR138MLScoresCannotAuthorizeMoneyWithoutReceipt(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) { c.Auto.GoEvalEnabled = true })
	key := "kalshi|KX-R138|YES"
	now := time.Now()
	s.mlPredAt = now
	s.mlPredMap = map[string]float64{key: .71}
	s.mlPredExecAuthority = false
	s.mlEval.scores = map[string]mlEvalScoreEnt{key: {pwin: .83, at: now, src: "tick"}}

	if p, ok := s.mlPredPWin("kalshi", "KX-R138", "YES"); ok || p != 0 {
		t.Fatalf("research-only sidecar/Go score authorized money: p=%v ok=%v", p, ok)
	}
	if p, ok := s.mlResearchPWin("kalshi", "KX-R138", "YES"); !ok || math.Abs(p-.83) > 1e-12 {
		t.Fatalf("risk/telemetry seam lost the fresh score: p=%v ok=%v", p, ok)
	}
	s.mlPredExecAuthority = true
	if p, ok := s.mlPredPWin("kalshi", "KX-R138", "YES"); !ok || math.Abs(p-.83) > 1e-12 {
		t.Fatalf("explicit authority receipt did not open the money seam: p=%v ok=%v", p, ok)
	}

	s.mlEdgeAt = now
	s.mlEdgeMap = map[string]float64{"KX-R138|YES": .64}
	s.mlEdgeExecAuthority = false
	if _, ok := s.mlPWin("KX-R138", "YES"); ok {
		t.Fatal("all_scored research estimate became a sizing authority")
	}
	if p, ok := s.mlResearchSignalPWin("KX-R138", "YES"); !ok || math.Abs(p-.64) > 1e-12 {
		t.Fatalf("research estimate unavailable to logging/risk: p=%v ok=%v", p, ok)
	}
}

func TestR138MLRevocationClearsPriorGoVectors(t *testing.T) {
	s := testServer(t)
	key := "kalshi|KX-OLD|YES"
	s.mlEval.vecs = map[string]*mlEvalVec{key: {Ticker: "KX-OLD", Platform: "kalshi", Side: "YES", X: []float64{1}}}
	s.mlEval.byTicker = map[string][]string{"kalshi|KX-OLD": {key}}
	s.mlEval.scores = map[string]mlEvalScoreEnt{key: {pwin: .9, at: time.Now()}}
	s.mlEval.scoreAt = map[string]time.Time{key: time.Now()}
	b, err := json.Marshal(map[string]any{
		"generated_at": time.Now().Unix(), "version": "", "authority": false, "rows": []any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, mlEvalVecsFile), b, 0o644); err != nil {
		t.Fatal(err)
	}
	s.mlEvalLoadVecs()
	if len(s.mlEval.vecs) != 0 || len(s.mlEval.scores) != 0 || len(s.mlEval.byTicker) != 0 {
		t.Fatalf("authority revocation left stale evaluator state: vecs=%d scores=%d tickers=%d",
			len(s.mlEval.vecs), len(s.mlEval.scores), len(s.mlEval.byTicker))
	}
}

func TestR138MLGateLeavesStrategyCombosIndependent(t *testing.T) {
	called := false
	deny := func() bool { called = true; return false }
	for _, origin := range []string{"auto", "xvgap", "kalshi-system"} {
		called = false
		if !mlFundedRouteAllowed(origin, deny) || called {
			t.Fatalf("non-ML combo origin %q consulted or inherited ML authority", origin)
		}
	}
	if mlFundedRouteAllowed("ml", deny) || !called {
		t.Fatal("ML-origin combo bypassed its authority callback")
	}
	if !mlFundedRouteAllowed("ML", func() bool { return true }) {
		t.Fatal("explicitly authorized ML origin was not recognized")
	}
	if !mlPaperMoneyAuthority(true, true) || mlPaperMoneyAuthority(true, false) ||
		mlPaperMoneyAuthority(false, true) {
		t.Fatal("paper receipt must require execution + paper authority")
	}
	if mlLiveMoneyAuthority(true, true, false) || mlLiveMoneyAuthority(true, false, true) ||
		mlLiveMoneyAuthority(false, true, true) || !mlLiveMoneyAuthority(true, true, true) {
		t.Fatal("LIVE receipt must remain unanimous")
	}
	if mlMoneyAuthority(true, true, false) || !mlMoneyAuthority(true, true, true) {
		t.Fatal("legacy money-authority alias must remain the strict LIVE gate")
	}

	s := testServer(t)
	file := map[string]any{
		"execution_enabled": false, "paper_authority": false, "live_authority": false,
		"predictions": []map[string]any{{"ticker": "KX-BLOCKED", "side": "YES", "platform": "kalshi",
			"price": .4, "p_win": .8, "ev_per_contract": .4, "ev_net": .39, "resolve_hours": 1}},
	}
	b, _ := json.Marshal(file)
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, "ml_predictions.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	if got := s.liveProposals(context.Background(), 100, 100, map[string]bool{}); len(got) != 0 {
		t.Fatalf("research-only ML file produced %d live proposals", len(got))
	}
}
