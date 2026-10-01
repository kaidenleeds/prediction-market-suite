package server

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r146AggregateComboLegs(tickers ...string) []paper.Leg {
	legs := make([]paper.Leg, 0, len(tickers))
	for _, ticker := range tickers {
		legs = append(legs, paper.Leg{Platform: "kalshi", Ticker: ticker, Side: "YES", Entry: .5})
	}
	return legs
}

func r146SettledAggregateCombo(t *testing.T, s *Server, route, cohort string, realized float64, tickers ...string) {
	t.Helper()
	id, err := s.store.InsertParlay(context.Background(), paper.Parlay{
		Stake: 5, Price: .25, Contracts: 20, Legs: r146AggregateComboLegs(tickers...),
		RouteSource: route, Cohort: cohort,
	})
	if err != nil {
		t.Fatalf("insert combo: %v", err)
	}
	if err := s.store.SettleParlay(context.Background(), id, 1, realized); err != nil {
		t.Fatalf("settle combo: %v", err)
	}
}

func TestR146NetworthSeparatesMLComboAndIncludesItInTotal(t *testing.T) {
	s := testServer(t)
	r146SettledAggregateCombo(t, s, positiveSystemComboRouteSource,
		storage.ComboLabCohortRollingPositive, 3, "SYS-A", "SYS-B")
	r146SettledAggregateCombo(t, s, mlComboPaperRoute, mlComboPaperCohort,
		7, "ML-A", "ML-B")

	var got map[string]any
	if err := json.Unmarshal(s.buildNetworth(context.Background()), &got); err != nil {
		t.Fatalf("decode networth: %v", err)
	}
	for key, want := range map[string]float64{"parlay_net": 3, "ml_combo_net": 7, "total_net": 10} {
		if value, ok := got[key].(float64); !ok || math.Abs(value-want) > 1e-9 {
			t.Fatalf("%s=%v, want %.2f (payload=%v)", key, got[key], want, got)
		}
	}
	if got["parlay_net"] == got["total_net"] {
		t.Fatal("ML Combo result was hidden inside the System Combo field")
	}
}

func TestR146AggregatePaperCurveIncludesBothSeparateComboLedgers(t *testing.T) {
	s := testServer(t)
	r146SettledAggregateCombo(t, s, positiveSystemComboRouteSource,
		storage.ComboLabCohortRollingPositive, 2.5, "SYS-C", "SYS-D")
	r146SettledAggregateCombo(t, s, mlComboPaperRoute, mlComboPaperCohort,
		4.5, "ML-C", "ML-D")

	got, ok := s.currentNetPnL(context.Background())
	if !ok || math.Abs(got-7) > 1e-9 {
		t.Fatalf("aggregate Paper curve net=(%.2f,%v), want 7.00 from separate 2.50 + 4.50 ledgers", got, ok)
	}
	if sys, err := s.store.ParlaysRealized(context.Background()); err != nil || math.Abs(sys-2.5) > 1e-9 {
		t.Fatalf("System Combo aggregate was contaminated: %.2f, %v", sys, err)
	}
}

func TestR146MLComboDashboardChipAndTrueZero(t *testing.T) {
	for _, want := range []string{
		"🤖🎲 ML Combo '+echip('book_ml_combos_usd'",
		"s.equity===null||s.equity===undefined",
		"ML combos $\"+(d.ml_combo_net||0).toFixed(2)",
	} {
		if !strings.Contains(dashboardHTML, want) {
			t.Fatalf("ML Combo dashboard presentation missing %q", want)
		}
	}
	if strings.Contains(dashboardHTML, "money(s.equity||600)") {
		t.Fatal("dashboard still turns a real zero ML Combo equity into the $600 fallback")
	}
}
