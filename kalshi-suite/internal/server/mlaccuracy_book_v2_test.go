package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestMLAccPlacedBrierAndLogLoss(t *testing.T) {
	bets := []mlAccBet{{P: .80, Y: 1}, {P: .25, Y: 0}}
	if got, ok := mlAccBrier(bets); !ok || math.Abs(got-.05125) > 1e-12 {
		t.Fatalf("placed Brier=(%v,%v), want (.05125,true)", got, ok)
	}
	wantLog := (-math.Log(.80) - math.Log(.75)) / 2
	if got, ok := mlAccLogLoss(bets); !ok || math.Abs(got-wantLog) > 1e-12 {
		t.Fatalf("placed log loss=(%v,%v), want (%v,true)", got, ok, wantLog)
	}
	if _, ok := mlAccBrier(nil); ok {
		t.Fatal("empty placed cohort must not report a zero Brier")
	}
	if _, ok := mlAccLogLoss(nil); ok {
		t.Fatal("empty placed cohort must not report a zero log loss")
	}
	if got, ok := mlAccLogLoss([]mlAccBet{{P: 0, Y: 1}}); !ok || math.IsInf(got, 0) || math.IsNaN(got) || got < 30 {
		t.Fatalf("clipped certain miss log loss=(%v,%v), want finite large penalty", got, ok)
	}
}

func TestRefreshMLAccuracyReportsCurrentPlacedPaperScores(t *testing.T) {
	s := testServer(t)
	dir := s.cfg().DataDir
	body := `{"epoch_id":"ml-v2-fixture","reset_at":"1970-01-01T00:00:01Z","closed":[
		{"reset_close":true,"epoch_marker":true},
		{"ticker":"KXA","side":"YES","platform":"kalshi","opened":2,"p_win":0.80,"won":1,"model_cohort":"book-native-v2","epoch_id":"ml-v2-fixture"},
		{"ticker":"KXB","side":"NO","platform":"kalshi","opened":3,"p_win":0.25,"won":0,"model_cohort":"book-native-v2","epoch_id":"ml-v2-fixture"},
		{"ticker":"OLD","side":"YES","platform":"kalshi","opened":4,"p_win":0.99,"won":0,"model_cohort":"legacy-v1","epoch_id":"ml-v2-fixture"},
		{"ticker":"BAD","side":"YES","platform":"kalshi","opened":5,"p_win":0.50,"won":2,"model_cohort":"book-native-v2","epoch_id":"ml-v2-fixture"}
	]}`
	if err := os.WriteFile(filepath.Join(dir, "ml_paper.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.RefreshMLAccuracy(context.Background(), true); err != nil {
		t.Fatalf("RefreshMLAccuracy: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "ml_accuracy.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report map[string]any
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatal(err)
	}
	if got := report["book_v2_brier"]; got != .0512 {
		t.Fatalf("book_v2_brier=%v, want 0.0512 (four-decimal report rounding)", got)
	}
	if got := report["book_v2_brier_n"]; got != float64(2) {
		t.Fatalf("book_v2_brier_n=%v, want 2 current-v2 placed Paper lots", got)
	}
	if got := report["book_v2_log_loss"]; got != .2554 {
		t.Fatalf("book_v2_log_loss=%v, want 0.2554", got)
	}
	if got := report["book_v2_log_loss_n"]; got != float64(2) {
		t.Fatalf("book_v2_log_loss_n=%v, want 2", got)
	}
	for _, field := range []string{"book_v2_brier_scope", "book_v2_log_loss_scope"} {
		scope, _ := report[field].(string)
		if !strings.Contains(scope, "settled current-reset book-native-v2 ml_paper lots only") {
			t.Fatalf("%s=%q does not keep real placed-Paper scope explicit", field, scope)
		}
	}
	if got := report["n_bets"]; got != float64(2) {
		t.Fatalf("n_bets=%v, want 2; legacy and invalid-outcome rows must stay excluded", got)
	}
}

func TestRefreshMLAccuracyFailsClosedWithoutValidEpochContract(t *testing.T) {
	cases := map[string]string{
		"missing":        `{"closed":[{"ticker":"LEAK","side":"YES","opened":10,"p_win":0.9,"won":1,"model_cohort":"book-native-v2"}]}`,
		"malformed-id":   `{"epoch_id":"legacy","reset_at":"1970-01-01T00:00:01Z","closed":[{"ticker":"LEAK","side":"YES","opened":10,"p_win":0.9,"won":1,"model_cohort":"book-native-v2","epoch_id":"legacy"}]}`,
		"malformed-time": `{"epoch_id":"ml-v2-current","reset_at":"not-a-time","closed":[{"ticker":"LEAK","side":"YES","opened":10,"p_win":0.9,"won":1,"model_cohort":"book-native-v2","epoch_id":"ml-v2-current"}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			s := testServer(t)
			path := filepath.Join(s.cfg().DataDir, "ml_paper.json")
			if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			if bets, epoch := mlAccReadBookEpoch(path, "ml_paper"); len(bets) != 0 || epoch.Valid {
				t.Fatalf("invalid epoch admitted bets=%v epoch=%+v", bets, epoch)
			}
			if err := s.RefreshMLAccuracy(context.Background(), true); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(filepath.Join(s.cfg().DataDir, "ml_accuracy.json"))
			if err != nil {
				t.Fatal(err)
			}
			var report struct {
				Ready         bool               `json:"ready"`
				NBets         int                `json:"n_bets"`
				EpochError    string             `json:"epoch_error"`
				SourceReceipt mlAccSourceReceipt `json:"source_receipt"`
			}
			if json.Unmarshal(raw, &report) != nil || report.Ready || report.NBets != 0 ||
				report.EpochError == "" || report.SourceReceipt.Valid {
				t.Fatalf("invalid epoch report did not fail closed: %+v", report)
			}
		})
	}
}

func TestRefreshMLAccuracyExcludesEveryPreResetV2Row(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	dir := s.cfg().DataDir
	reset := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	epoch := "ml-v2-current"
	doc := map[string]any{
		"epoch_id": epoch,
		"reset_at": reset.Format(time.RFC3339Nano),
		"closed": []any{
			map[string]any{"ticker": "BEFORE-MARKER", "side": "YES", "opened": float64(reset.Add(-time.Hour).Unix()),
				"p_win": .99, "won": 0.0, "model_cohort": currentMLCohort, "epoch_id": "ml-v2-old"},
			map[string]any{"reset_close": true, "epoch_marker": true, "closed_ts": float64(reset.Unix())},
			map[string]any{"ticker": "LATE-OLD-EPOCH", "side": "YES", "opened": float64(reset.Add(time.Minute).Unix()),
				"p_win": .99, "won": 0.0, "model_cohort": currentMLCohort, "epoch_id": "ml-v2-old"},
			map[string]any{"ticker": "STALE-TIME", "side": "YES", "opened": float64(reset.Add(-time.Minute).Unix()),
				"p_win": .99, "won": 0.0, "model_cohort": currentMLCohort, "epoch_id": epoch},
			map[string]any{"ticker": "CURRENT-PAPER", "side": "YES", "platform": "kalshi",
				"opened": float64(reset.Add(time.Minute).Unix()), "closed_ts": float64(reset.Add(2 * time.Minute).Unix()),
				"p_win": .8, "won": 1.0, "pnl": .2, "contracts": 1.0,
				"model_cohort": currentMLCohort, "epoch_id": epoch},
		},
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ml_paper.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	lines := []map[string]any{
		{"seq": 1, "row": map[string]any{"ticker": "MAKER-OLD-EPOCH", "side": "YES", "opened": float64(reset.Add(time.Minute).Unix()),
			"fill_ts": float64(reset.Add(time.Minute).Unix()), "p_win": .99, "model_cohort": currentMLCohort, "epoch_id": "ml-v2-old"}},
		{"seq": 2, "row": map[string]any{"ticker": "MAKER-STALE-TIME", "side": "YES", "opened": float64(reset.Add(-time.Minute).Unix()),
			"fill_ts": float64(reset.Add(-time.Minute).Unix()), "p_win": .99, "model_cohort": currentMLCohort, "epoch_id": epoch}},
		{"seq": 3, "row": map[string]any{"ticker": "MAKER-CURRENT", "side": "YES", "opened": float64(reset.Add(time.Minute).Unix()),
			"fill_ts": float64(reset.Add(time.Minute).Unix()), "p_win": .7, "model_cohort": currentMLCohort, "epoch_id": epoch}},
	}
	journal := ""
	for _, line := range lines {
		b, marshalErr := json.Marshal(line)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		journal += string(b) + "\n"
	}
	if err := os.WriteFile(filepath.Join(dir, "ml_maker_fills.jsonl"), []byte(journal), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.store.InsertSignal(ctx, storage.Signal{Platform: "kalshi", Ticker: "MAKER-CURRENT",
		Side: "YES", SignalType: "fixture", EntryPrice: .5}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.store.DBForTest().ExecContext(ctx, `UPDATE signal_log SET resolved=1,won=1,settle_val=1,resolved_at=? WHERE ticker=?`,
		reset.Add(2*time.Minute).Format(time.RFC3339Nano), "MAKER-CURRENT"); err != nil {
		t.Fatal(err)
	}
	if err := s.RefreshMLAccuracy(ctx, true); err != nil {
		t.Fatal(err)
	}
	accuracyRaw, err := os.ReadFile(filepath.Join(dir, "ml_accuracy.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report map[string]any
	if err := json.Unmarshal(accuracyRaw, &report); err != nil {
		t.Fatal(err)
	}
	if report["paper_epoch_id"] != epoch || report["paper_reset_at"] != reset.Format(time.RFC3339Nano) {
		t.Fatalf("reset receipt missing: epoch=%v reset=%v", report["paper_epoch_id"], report["paper_reset_at"])
	}
	if got := report["book_v2_brier_n"]; got != float64(1) {
		t.Fatalf("book_v2_brier_n=%v, want only the one current-reset Paper lot", got)
	}
	if got := report["book_v2_brier"]; got != .04 {
		t.Fatalf("book_v2_brier=%v, want .04 from current Paper lot only", got)
	}
	if got := report["n_bets"]; got != float64(2) {
		t.Fatalf("n_bets=%v, want one current Paper + one same-epoch maker fill", got)
	}
	byBook, _ := report["by_book"].(map[string]any)
	if fmt.Sprint(byBook["ml_paper"]) != "1" || fmt.Sprint(byBook["maker_fills"]) != "1" {
		t.Fatalf("by_book=%v, pre-reset/old-epoch rows leaked", byBook)
	}
	firstReceipt := report["source_receipt"].(map[string]any)
	firstHash, _ := firstReceipt["sha256"].(string)
	if firstReceipt["settled_count"] != float64(2) || len(firstHash) != 64 {
		t.Fatalf("initial durable source receipt=%v", firstReceipt)
	}
	// Same epoch, new settled Paper row: a report younger than any age window must refresh.
	doc["closed"] = append(doc["closed"].([]any), map[string]any{
		"ticker": "CURRENT-PAPER-2", "side": "YES", "platform": "kalshi",
		"opened":    float64(reset.Add(2 * time.Minute).Unix()),
		"closed_ts": float64(reset.Add(3 * time.Minute).Unix()),
		"p_win":     .6, "won": 0.0, "pnl": -.6, "contracts": 1.0,
		"model_cohort": currentMLCohort, "epoch_id": epoch,
	})
	raw, err = json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ml_paper.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.RefreshMLAccuracy(ctx, false); err != nil {
		t.Fatal(err)
	}
	accuracyRaw, err = os.ReadFile(filepath.Join(dir, "ml_accuracy.json"))
	if err != nil || json.Unmarshal(accuracyRaw, &report) != nil {
		t.Fatalf("read same-epoch Paper refresh: %v", err)
	}
	secondReceipt := report["source_receipt"].(map[string]any)
	secondHash, _ := secondReceipt["sha256"].(string)
	if report["n_bets"] != float64(3) || report["book_v2_brier_n"] != float64(2) ||
		secondReceipt["settled_count"] != float64(3) || secondHash == firstHash ||
		secondReceipt["paper_max_close_unix"] != float64(reset.Add(3*time.Minute).Unix()) {
		t.Fatalf("same-epoch Paper close did not invalidate: report=%v receipt=%v", report["n_bets"], secondReceipt)
	}
	// Same epoch, new maker fill whose outcome is now durable: the maker receipt/hash also moves.
	newMaker := map[string]any{"seq": 4, "row": map[string]any{
		"ticker": "MAKER-CURRENT-2", "side": "YES", "platform": "kalshi",
		"opened":  float64(reset.Add(4 * time.Minute).Unix()),
		"fill_ts": float64(reset.Add(4 * time.Minute).Unix()), "p_win": .65,
		"model_cohort": currentMLCohort, "epoch_id": epoch}}
	line, err := json.Marshal(newMaker)
	if err != nil {
		t.Fatal(err)
	}
	journal += string(line) + "\n"
	if err := os.WriteFile(filepath.Join(dir, "ml_maker_fills.jsonl"), []byte(journal), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.store.InsertSignal(ctx, storage.Signal{Platform: "kalshi", Ticker: "MAKER-CURRENT-2",
		Side: "YES", SignalType: "fixture", EntryPrice: .5}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.store.DBForTest().ExecContext(ctx, `UPDATE signal_log SET resolved=1,won=0,settle_val=0,resolved_at=? WHERE ticker=?`,
		reset.Add(5*time.Minute).Format(time.RFC3339Nano), "MAKER-CURRENT-2"); err != nil {
		t.Fatal(err)
	}
	if err := s.RefreshMLAccuracy(ctx, false); err != nil {
		t.Fatal(err)
	}
	accuracyRaw, err = os.ReadFile(filepath.Join(dir, "ml_accuracy.json"))
	if err != nil || json.Unmarshal(accuracyRaw, &report) != nil {
		t.Fatalf("read same-epoch maker refresh: %v", err)
	}
	thirdReceipt := report["source_receipt"].(map[string]any)
	thirdHash, _ := thirdReceipt["sha256"].(string)
	if report["n_bets"] != float64(4) || thirdReceipt["maker_fill_count"] != float64(2) ||
		thirdReceipt["settled_count"] != float64(4) || thirdHash == secondHash ||
		thirdReceipt["maker_max_fill_unix"] != float64(reset.Add(4*time.Minute).Unix()) {
		t.Fatalf("same-epoch maker fill did not invalidate: n=%v receipt=%v", report["n_bets"], thirdReceipt)
	}
	// A current-version report must still invalidate immediately when the Paper reset receipt
	// changes; file age is never permission to serve the prior epoch.
	reset2, epoch2 := reset.Add(time.Hour), "ml-v2-next"
	doc["epoch_id"], doc["reset_at"] = epoch2, reset2.Format(time.RFC3339Nano)
	doc["closed"] = []any{
		map[string]any{"reset_close": true, "epoch_marker": true, "closed_ts": float64(reset2.Unix())},
		map[string]any{"ticker": "NEXT-PAPER", "side": "YES", "platform": "kalshi",
			"opened": float64(reset2.Add(time.Minute).Unix()), "p_win": .6, "won": 1.0,
			"model_cohort": currentMLCohort, "epoch_id": epoch2},
	}
	raw, err = json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ml_paper.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.RefreshMLAccuracy(ctx, false); err != nil {
		t.Fatal(err)
	}
	accuracyRaw, err = os.ReadFile(filepath.Join(dir, "ml_accuracy.json"))
	if err != nil || json.Unmarshal(accuracyRaw, &report) != nil {
		t.Fatalf("read next epoch report: %v", err)
	}
	if report["paper_epoch_id"] != epoch2 || report["n_bets"] != float64(1) {
		t.Fatalf("fresh cached report crossed reset: epoch=%v n=%v", report["paper_epoch_id"], report["n_bets"])
	}
}
