package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRefreshMLAccuracyKeepsLastGoodOnMakerLookupError(t *testing.T) {
	s := testServer(t)
	dir := s.cfg().DataDir
	reset := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	epoch := "ml-v2-r150-integrity"
	paper := map[string]any{
		"epoch_id": epoch,
		"reset_at": reset.Format(time.RFC3339Nano),
		"closed": []any{
			map[string]any{"reset_close": true, "epoch_marker": true},
			map[string]any{
				"ticker": "R150-PAPER", "side": "YES", "platform": "kalshi",
				"opened": float64(reset.Add(time.Minute).Unix()), "closed_ts": float64(reset.Add(2 * time.Minute).Unix()),
				"p_win": .7, "won": 1.0, "model_cohort": currentMLCohort, "epoch_id": epoch,
			},
		},
	}
	paperBytes, err := json.Marshal(paper)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ml_paper.json"), paperBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.RefreshMLAccuracy(context.Background(), true); err != nil {
		t.Fatalf("initial complete refresh: %v", err)
	}
	reportPath := filepath.Join(dir, "ml_accuracy.json")
	lastGood, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}

	// This fill has not yet settled. A canceled database lookup is a transient read failure, not
	// proof that the row is unresolved; the prior complete report must remain byte-for-byte intact.
	maker := map[string]any{"seq": 1, "row": map[string]any{
		"ticker": "R150-MAKER", "side": "YES", "platform": "kalshi",
		"opened": float64(reset.Add(3 * time.Minute).Unix()), "fill_ts": float64(reset.Add(3 * time.Minute).Unix()),
		"p_win": .6, "model_cohort": currentMLCohort, "epoch_id": epoch,
	}}
	makerBytes, err := json.Marshal(maker)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ml_maker_fills.jsonl"), append(makerBytes, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.RefreshMLAccuracy(ctx, true); err == nil {
		t.Fatal("canceled settlement lookup must make the refresh incomplete")
	}
	after, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(lastGood) {
		t.Fatal("incomplete refresh replaced the last-good report")
	}

	// The HTTP endpoint may serve the still-current last-good epoch, but labels the response so a
	// caller can distinguish it from a freshly recomputed report.
	req := httptest.NewRequest("GET", "/api/mlaccuracy", nil).WithContext(ctx)
	rr := httptest.NewRecorder()
	s.handleMLAccuracy(rr, req)
	if rr.Code != 200 || rr.Header().Get("X-ML-Accuracy-Refresh") != "last-good" {
		t.Fatalf("last-good response status/header = %d/%q", rr.Code, rr.Header().Get("X-ML-Accuracy-Refresh"))
	}
	var served map[string]any
	if json.Unmarshal(rr.Body.Bytes(), &served) != nil || served["ready"] != true || served["n_bets"] != float64(1) {
		t.Fatalf("endpoint did not preserve complete last-good report: %s", rr.Body.String())
	}
}

func TestMLAccUniversePropagatesIncompleteScan(t *testing.T) {
	s := testServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if universe, err := s.mlAccUniverse(ctx, nil); err == nil || universe != nil {
		t.Fatalf("canceled universe scan = (%v,%v), want (nil,error)", universe, err)
	}
}

func TestMLAccuracyRefreshDefersWhileLiveArmed(t *testing.T) {
	s := testServer(t)
	s.liveMu.Lock()
	s.liveArmed = true
	s.liveMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.RefreshMLAccuracy(ctx, false); err != nil {
		t.Fatalf("LIVE-armed analytic refresh should preserve last-good without reading DB: %v", err)
	}
	s.liveMu.Lock()
	s.liveArmed = false
	s.liveMu.Unlock()
	if err := s.RefreshMLAccuracy(ctx, false); err == nil {
		t.Fatal("unarmed refresh unexpectedly ignored its canceled DB context")
	}
}
