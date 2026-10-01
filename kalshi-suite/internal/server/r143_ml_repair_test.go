package server

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
)

func TestR143ZeroOpenMLResetStartsCleanV2EpochAtConfiguredBank(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) { c.Auto.BookMLUSD = 600 })
	path := filepath.Join(s.cfg().DataDir, "ml_paper.json")
	seed := map[string]any{
		"bank0": 1000.0,
		"open":  []any{},
		"closed": []any{
			map[string]any{"model_cohort": "legacy-v1", "pnl": -20.0, "contracts": 10.0},
		},
		"lifetime": map[string]any{"net": -20.0, "closed": 1.0, "wins": 0.0},
		"stats":    map[string]any{"net": -20.0, "equity": 980.0, "open_n": 1.0},
	}
	b, _ := json.Marshal(seed)
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if got := s.resetMLBook(context.Background(), "ml_paper.json"); got != 0 {
		t.Fatalf("zero-open reset closed %d positions, want 0", got)
	}
	var pf map[string]any
	if !s.readJSONLoose(path, &pf) {
		t.Fatal("reset book became unreadable")
	}
	epoch, _ := pf["epoch_id"].(string)
	if epoch == "" {
		t.Fatal("zero-open reset did not create a durable epoch id")
	}
	closed, _ := pf["closed"].([]any)
	foundMarker := false
	for _, row := range closed {
		m, _ := row.(map[string]any)
		if marker, _ := m["epoch_marker"].(bool); marker && m["epoch_id"] == epoch {
			foundMarker = true
		}
	}
	if !foundMarker {
		t.Fatal("zero-open reset did not append its epoch marker")
	}
	stats, _ := pf["stats"].(map[string]any)
	if pf["bank0"] != 600.0 || stats["equity"] != 600.0 || stats["net"] != 0.0 || stats["open_n"] != 0.0 {
		t.Fatalf("reset state = bank=%v equity=%v net=%v open=%v, want 600/600/0/0",
			pf["bank0"], stats["equity"], stats["net"], stats["open_n"])
	}
}

func TestR143CurrentMLViewArchivesLegacyAndUsesActualArrays(t *testing.T) {
	epoch := "ml-v2-test"
	raw := map[string]any{
		"epoch_id": epoch,
		"bank0":    600.0,
		"open": []any{
			map[string]any{"model_cohort": currentMLCohort, "epoch_id": epoch, "contracts": 2.0, "unrealized": 1.5},
			map[string]any{"model_cohort": "legacy-v1", "contracts": 20.0},
		},
		"closed": []any{
			map[string]any{"reset_close": true, "epoch_marker": true},
			map[string]any{"model_cohort": currentMLCohort, "epoch_id": epoch, "pnl": 3.0, "won": 1.0, "contracts": 2.0},
			map[string]any{"model_cohort": "legacy-v1", "pnl": -50.0, "won": 0.0, "contracts": 20.0},
		},
		"stats":    map[string]any{"open_n": 99.0, "net": -50.0, "equity": 550.0},
		"lifetime": map[string]any{"net": -50.0, "closed": 20.0},
	}
	view, ok := currentMLPaperView(raw).(map[string]any)
	if !ok {
		t.Fatal("current ML view returned wrong type")
	}
	open, _ := view["open"].([]any)
	closed, _ := view["closed"].([]any)
	stats, _ := view["stats"].(map[string]any)
	if len(open) != 1 || len(closed) != 1 {
		t.Fatalf("current arrays = open %d closed %d, want 1/1", len(open), len(closed))
	}
	if stats["open_n"] != 1.0 || stats["net"] != 3.0 || stats["equity"] != 604.5 {
		t.Fatalf("recomputed stats = %+v, want actual v2 open/net/equity", stats)
	}
	if view["live_unreal"] != 1.5 || view["marked_open_n"] != 1 {
		t.Fatalf("current marked economics = unreal %v marked %v", view["live_unreal"], view["marked_open_n"])
	}
	archive, _ := view["legacy_archive"].(map[string]any)
	if archive["open"] != 1 || archive["closed"] != 1 || archive["excluded_from_current"] != true {
		t.Fatalf("legacy archive receipt = %+v", archive)
	}
}

func TestR143MLBriefingUsesProvisionalV2MetricsAndCurrentEconomicsOnly(t *testing.T) {
	s := testServer(t)
	epoch := "ml-v2-brief"
	now := time.Now()
	write := func(name string, v any) {
		b, _ := json.Marshal(v)
		if err := os.WriteFile(filepath.Join(s.cfg().DataDir, name), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("ml_paper.json", map[string]any{
		"epoch_id": epoch, "bank0": 600.0, "reset_at": now.Add(-12 * time.Hour).UTC().Format(time.RFC3339Nano),
		"open": []any{
			map[string]any{"model_cohort": currentMLCohort, "epoch_id": epoch,
				"platform": "kalshi", "price": .40, "contracts": 10.0, "fee": .20,
				"opened": float64(now.Add(-time.Hour).Unix()),
				"marks":  []any{[]any{2000.0, .30, "mark"}}},
		},
		"closed": []any{
			map[string]any{"reset_close": true, "epoch_marker": true},
			map[string]any{"model_cohort": currentMLCohort, "epoch_id": epoch,
				"platform": "kalshi", "fill_kind": "taker", "pnl": 2.0, "contracts": 10.0,
				"price": .40, "fee": .20, "ev": .05, "ev_net": .03, "p_win": .45,
				"opened": float64(now.Add(-8 * time.Hour).Unix()), "closed_ts": float64(now.Add(-2 * time.Hour).Unix()),
				"won": 1.0},
			map[string]any{"model_cohort": "legacy-v1", "pnl": -999.0, "contracts": 10.0},
		},
		"stats": map[string]any{"net": -999.0, "equity": -399.0, "open_n": 17.0},
	})
	write("ml_predictions.json", map[string]any{
		"feature_schema": currentMLCohort, "model_cohort": currentMLCohort,
		"model_status": "PAPER_PROVISIONAL", "metric_scope": "provisional_paper",
		"provisional_auc": .612, "provisional_brier": .2211, "provisional_brier_raw": .2311,
		"provisional_log_loss": .6543, "provisional_ece": .0312,
		"book_v2_resolved": 1801, "book_v2_open": 488,
		"split_receipt":   map[string]any{"days": 3, "test_rows": 120},
		"live_validation": map[string]any{"days_observed": 3, "days_required": 5},
	})
	got := s.MLBookBriefing()
	for _, want := range []string{"provisional model holdout (not real bets)", "AUC 0.612", "Brier 0.2211",
		"log loss 0.6543", "ECE 0.0312", "calibration improved Brier 0.0100",
		"test n=120", "LIVE 3/5 days", "marked eq $600.80", "realized net $2.00",
		"latest side-book marks -$1.20 on 1/1 open", "settled real Paper predictions · m0 🔴 · rows1",
		"Brier 0.3025", "realized +20.0¢/share", "fee-net predicted +3.0¢/share",
		"🧮 Proper Betting Paper · $400 each",
		"Brier Paper · net +$0.00 · NAV $400.00 · fills 0",
		"Research/group result (not cash P&L) · Brier · 0 | Log · 0 | Spherical · 0"} {
		if !strings.Contains(got, want) {
			t.Fatalf("briefing missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "999") || strings.Contains(got, "legacy-v1") {
		t.Fatalf("legacy economics leaked into New ML briefing:\n%s", got)
	}
	if main := s.BriefingText(context.Background()); !strings.Contains(main, "🧮 Proper Betting") ||
		!strings.Contains(main, "Paper · 1 settled") {
		t.Fatalf("main briefing omitted ML/Proper effectiveness:\n%s", main)
	}
}

func TestR143MLEconomicsUsesFeeNetEpochTimeAndOpenCapital(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	evNet := .03
	pWin, won := .45, 1.0
	pf := mlPaperBrief{Bank0: 600, ResetAt: now.Add(-12 * time.Hour).Format(time.RFC3339Nano)}
	pf.Closed = []mlBriefLot{{Model: currentMLCohort, Contracts: 10, Price: .40, Fee: .20,
		PnL: 2, EV: .20, EVNet: &evNet, PWin: &pWin, Won: &won,
		Opened: float64(now.Add(-8 * time.Hour).Unix()), ClosedTS: float64(now.Add(-2 * time.Hour).Unix())}}
	pf.Open = []mlBriefLot{{Model: currentMLCohort, Contracts: 5, Price: .50, Fee: .10,
		Opened: float64(now.Add(-2 * time.Hour).Unix())}}
	e := mlCurrentEconomics(pf, now)
	if e.PredictedCents != 3 || e.RealizedCents != 20 || e.SpanDays != .5 || e.NetPerDay != 4 {
		t.Fatalf("fee-net/epoch economics = %+v", e)
	}
	// closed: $4.20 for 6h = 1.05 $-days; open: $2.60 for 2h = .2166667 $-days.
	if diff := e.CapitalDollarDays - (1.05 + 2.6/12); diff < -1e-6 || diff > 1e-6 {
		t.Fatalf("capital time=%v want %v", e.CapitalDollarDays, 1.05+2.6/12)
	}
	if e.AccuracyN != 1 || math.Abs(e.RealBrier-.3025) > 1e-12 || e.RateReady {
		t.Fatalf("real-bet/rate receipt = %+v", e)
	}
}

func TestR143ProperScoreBriefLineNamesControlsAndCounts(t *testing.T) {
	report := map[string]any{
		"transforms": map[string]any{
			"brier": map[string]any{"normalized_executable_settled": 3, "normalized_executable_settled_l1": 1.5,
				"normalized_executable_completed_slots": 2, "normalized_executable_unique_settled_markets": 7,
				"l1_weighted_realized_net": .12},
			"log": map[string]any{"normalized_executable_settled": 2, "normalized_executable_settled_l1": 1,
				"normalized_executable_completed_slots": 1, "normalized_executable_unique_settled_markets": 3,
				"l1_weighted_realized_net": -.04},
			"spherical": map[string]any{"normalized_executable_settled": 1, "normalized_executable_settled_l1": .5,
				"normalized_executable_completed_slots": 1, "normalized_executable_unique_settled_markets": 2,
				"l1_weighted_realized_net": .01},
		},
		"comparators": map[string]any{"metrics": map[string]any{
			"max-margin": map[string]any{"completed_slots": 2, "settled_l1": 2.0, "l1_weighted_realized_net": .03},
			"no-trade":   map[string]any{"completed_slots": 2, "settled_l1": 0.0, "l1_weighted_realized_net": 0.0},
		}},
	}
	got := properScoreBriefLine(report)
	for _, want := range []string{"🧮 Proper Betting", "Brier · +8.0¢ · 2 batches · 7 markets",
		"Log · -4.0¢ · 1 batch · 3 markets", "Spherical · +2.0¢ · 1 batch · 2 markets"} {
		if !strings.Contains(got, want) {
			t.Fatalf("proper-score line missing %q: %s", want, got)
		}
	}
}

func TestR143ProperScoreBriefLineDoesNotCallZeroWeightHistoryEffective(t *testing.T) {
	report := map[string]any{
		"transforms": map[string]any{
			"brier": map[string]any{"settled": 3007, "normalized_executable_settled": 0,
				"legacy_unnormalized_settled": 3007, "l1_weighted_realized_net": 0.0},
			"log": map[string]any{"settled": 3007, "normalized_executable_settled": 0,
				"legacy_unnormalized_settled": 3007, "l1_weighted_realized_net": 0.0},
			"spherical": map[string]any{"normalized_executable_settled": 0},
		},
		"comparators": map[string]any{"metrics": map[string]any{}},
	}
	got := properScoreBriefLine(report)
	for _, want := range []string{"Brier · 0", "Log · 0", "Spherical · 0"} {
		if !strings.Contains(got, want) {
			t.Fatalf("proper-score warming line missing %q: %s", want, got)
		}
	}
	if strings.Contains(got, "+$0.000 net (n=3007)") {
		t.Fatalf("zero-weight history presented as effectiveness: %s", got)
	}
	for _, clutter := range []string{"legacy excluded", "max-margin", "no-trade", "/L1", "slots", "legs"} {
		if strings.Contains(got, clutter) {
			t.Fatalf("compact Proper Betting line leaked dashboard-only detail %q: %s", clutter, got)
		}
	}
}
