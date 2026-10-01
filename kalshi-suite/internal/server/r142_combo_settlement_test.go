package server

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR142ComboLabScalarSettlementUsesSideAdjustedEconomicPayout(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.plabMu.Lock()
	s.plabState(ctx)
	s.plabMu.Unlock()
	legs := []plabLeg{
		{Ticker: "SCALAR-A", Side: "YES", Platform: "kalshi", Price: .5, PWin: .6},
		{Ticker: "SCALAR-B", Side: "NO", Platform: "kalshi", Price: .5, PWin: .6},
	}
	ent := plabOpenEnt{ID: "scalar-grade", At: time.Now().Add(-time.Hour), Legs: legs,
		Bucket: "indep", Class: "ind:2leg", Prod: .25, JointP: .36, FeeMVE: .03,
		EVSyn: .2, LegFees: []float64{.02, .04}, Legality: "synthetic_legs_only"}
	if n, err := s.store.PlabInsertBatch(ctx, []storage.PlabCand{plabToRow(ent)}); err != nil || n != 1 {
		t.Fatalf("insert n=%d err=%v", n, err)
	}
	if _, err := s.store.PlabResolveTicker(ctx, "kalshi", "SCALAR-A", .1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.store.PlabResolveTicker(ctx, "kalshi", "SCALAR-B", .5); err != nil {
		t.Fatal(err)
	}
	s.parlayLabGradePassWithFetcher(ctx, func(context.Context, storage.PlabLegKey) plabSettlementResult {
		t.Fatal("fully settled scalar legs must use the store fast path")
		return plabSettlementResult{}
	})
	key := "indep|2leg|ind:2leg|synthetic_legs_only|all-eligible"
	s.plabMu.Lock()
	ag := s.plabStats[key]
	s.plabMu.Unlock()
	if ag == nil || ag.N != 1 {
		t.Fatalf("missing scalar grade: %+v", ag)
	}
	// Side payouts are .1 and (1-.5)=.5, so proceeds are .05/.25=.20. Total entry capital is
	// 1+.02+.04=1.06; return per all-in dollar is (.20-1.06)/1.06.
	wantSynthetic := (.20 - 1.06) / 1.06
	if math.Abs(ag.SumReal-wantSynthetic) > 1e-9 {
		t.Fatalf("scalar synthetic result=%v want %v", ag.SumReal, wantSynthetic)
	}
	wantMVE := (.20 - 1.03) / 1.03
	if ag.NMVE != 1 || math.Abs(ag.SumMVE-wantMVE) > 1e-9 {
		t.Fatalf("scalar MVE result n=%d sum=%v want 1,%v", ag.NMVE, ag.SumMVE, wantMVE)
	}
}

func TestR142ComboLabBudgetFairnessAndAttemptOnlyStamps(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	batch := make([]storage.PlabCand, 0, 35)
	for i := 0; i < 35; i++ {
		at := now.Add(-time.Minute)
		prefix := fmt.Sprintf("K%02d", i)
		if i == 34 {
			// Lexically last but horizon-passed: it must move ahead of newer rows.
			at = now.Add(-7 * time.Hour)
			prefix = "Z34"
		}
		legs := []plabLeg{
			{Platform: "kalshi", Ticker: prefix + "A", Side: "YES"},
			{Platform: "kalshi", Ticker: prefix + "B", Side: "YES"},
		}
		batch = append(batch, plabToRow(plabOpenEnt{ID: fmt.Sprintf("fair-%02d", i), At: at,
			Legs: legs, Bucket: "indep", Class: "ind:2leg", Prod: .25, JointP: .3,
			FeeMVE: -1, LegFees: []float64{0, 0}, Legality: "synthetic_legs_only"}))
	}
	if n, err := s.store.PlabInsertBatch(ctx, batch); err != nil || n != int64(len(batch)) {
		t.Fatalf("insert n=%d err=%v", n, err)
	}
	attempted := map[string]int{}
	fetch := func(_ context.Context, tk storage.PlabLegKey) plabSettlementResult {
		attempted[tk.Ticker]++
		return plabSettlementResult{State: plabSettlementNotTerminal}
	}
	s.parlayLabGradePassWithFetcher(ctx, fetch)
	if len(attempted) != plabFetchBudg {
		t.Fatalf("first pass attempts=%d want budget=%d", len(attempted), plabFetchBudg)
	}
	if attempted["Z34A"] == 0 || attempted["Z34B"] == 0 {
		t.Fatalf("passed-horizon rows were not prioritized: attempts=%v", attempted)
	}
	unstamped := 0
	for i := 0; i < 34; i++ {
		for _, suffix := range []string{"A", "B"} {
			key := "kalshi|" + fmt.Sprintf("K%02d%s", i, suffix)
			if attempted[fmt.Sprintf("K%02d%s", i, suffix)] == 0 {
				unstamped++
				if _, stamped := s.plabChkAt[key]; stamped {
					t.Fatalf("budget-skipped key was stamped before an attempt: %s", key)
				}
			}
		}
	}
	if unstamped == 0 {
		t.Fatal("fixture did not exhaust the settlement-fetch budget")
	}
	// Cooldowns skip the first 30 actual attempts, allowing the formerly budget-starved tail
	// through on later passes. Every one of the 70 distinct legs must eventually be attempted.
	for pass := 0; pass < 3 && len(attempted) < 70; pass++ {
		s.parlayLabGradePassWithFetcher(ctx, fetch)
	}
	if len(attempted) != 70 {
		t.Fatalf("budget tail starved: attempted=%d want 70", len(attempted))
	}
}

func TestR142ComboLabSeparatesTransportFailureFromNormalOpen(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	legs := []plabLeg{
		{Platform: "kalshi", Ticker: "FAIL", Side: "YES"},
		{Platform: "kalshi", Ticker: "OPEN", Side: "YES"},
	}
	ent := plabOpenEnt{ID: "transport-state", At: time.Now().Add(-7 * time.Hour), Legs: legs,
		Bucket: "indep", Class: "ind:2leg", Prod: .25, JointP: .3, FeeMVE: -1,
		LegFees: []float64{0, 0}, Legality: "synthetic_legs_only"}
	if _, err := s.store.PlabInsertBatch(ctx, []storage.PlabCand{plabToRow(ent)}); err != nil {
		t.Fatal(err)
	}
	s.parlayLabGradePassWithFetcher(ctx, func(_ context.Context, tk storage.PlabLegKey) plabSettlementResult {
		if tk.Ticker == "FAIL" {
			return plabSettlementResult{State: plabSettlementTransportFailure}
		}
		return plabSettlementResult{State: plabSettlementNotTerminal}
	})
	if _, ok := s.plabFetchBad["kalshi|FAIL"]; !ok {
		t.Fatal("transport failure did not enter short retry lane")
	}
	if _, ok := s.plabChkAt["kalshi|FAIL"]; ok {
		t.Fatal("transport failure was falsely stamped as a healthy lifecycle check")
	}
	if _, ok := s.plabChkAt["kalshi|OPEN"]; !ok {
		t.Fatal("normal non-terminal market did not receive normal check cooldown")
	}
	if _, ok := s.plabFetchBad["kalshi|OPEN"]; ok {
		t.Fatal("normal non-terminal market was falsely reported as transport failure")
	}
}
