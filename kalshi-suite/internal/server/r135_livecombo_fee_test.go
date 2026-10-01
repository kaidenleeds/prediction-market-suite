package server

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR135LiveComboUsesAcceptedExactFeeAndQuarantinesAmbiguousPnL(t *testing.T) {
	s := testServer(t)
	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{
		"KXMVE": {taker: .07, typ: "quadratic", multiplier: 1},
	}
	s.kalFeesAt = time.Now()
	s.kalFeeMu.Unlock()

	fee, known, source := s.kalFeeExact("KXMVE-TEST", false, 2, .20)
	if !known || source != "quadratic" || fee <= 0 {
		t.Fatalf("exact combo fee=(%.6f,%v,%q)", fee, known, source)
	}
	ctx := context.Background()
	insert := func(market, state string) storage.LiveCombo {
		t.Helper()
		if err := s.store.InsertLiveCombo(ctx, storage.LiveComboInsert{
			Market: market, Collection: "KXMVE", LegsJSON: `[{"ticker":"A","side":"yes"}]`,
			ModelCohort: currentMLCohort,
			Fair:        .8, ProdPrice: .2, Quote: .2, Contracts: 2, AcceptedFee: fee,
			FeeSource: source, FeeKnown: known, AcceptState: state,
			RFQID: "rfq-" + market, QuoteID: "quote-" + market,
		}); err != nil {
			t.Fatalf("InsertLiveCombo(%s): %v", state, err)
		}
		rows, err := s.store.ListOpenLiveCombos(ctx, 10)
		if err != nil {
			t.Fatalf("ListOpenLiveCombos: %v", err)
		}
		for _, row := range rows {
			if row.Market == market {
				return row
			}
		}
		t.Fatalf("inserted combo %s missing", market)
		return storage.LiveCombo{}
	}

	accepted := insert("KXMVE-ACCEPTED", storage.LiveComboDispatched)
	if err := s.store.ConfirmLiveComboExecution(ctx, accepted.ID, accepted.Market, storage.LiveComboFilled, 2, fee,
		"kalshi-portfolio-fills:fee_cost", `{"fill_ids":["f1"]}`); err != nil {
		t.Fatalf("confirm accepted fill: %v", err)
	}
	settled, err := s.store.SettleLiveCombo(ctx, accepted.ID, 1)
	if err != nil {
		t.Fatalf("settle accepted: %v", err)
	}
	want := 2*(1-.2) - fee
	if !settled.Reportable || math.Abs(settled.Realized-want) > 1e-12 {
		t.Fatalf("accepted settlement=%+v, want fee-net %.6f", settled, want)
	}

	ambiguous := insert("KXMVE-AMBIGUOUS", storage.LiveComboAcceptAmbiguous)
	settled, err = s.store.SettleLiveCombo(ctx, ambiguous.ID, 1)
	if err != nil {
		t.Fatalf("settle ambiguous: %v", err)
	}
	if settled.Reportable || math.Abs(settled.Realized-want) > 1e-12 {
		t.Fatalf("ambiguous receipt must retain calculable P&L but stay out of reporting: %+v", settled)
	}
	_ = insert("KXMVE-OPEN", storage.LiveComboDispatched)
	if total, err := s.store.LiveCombosRealized(ctx); err != nil || math.Abs(total-want) > 1e-12 {
		t.Fatalf("fee-net total=(%.6f,%v), want accepted-only %.6f", total, err, want)
	}
	systems, coverage := s.collectLiveComboSystems(ctx)
	if len(systems) != 1 || len(coverage) != 1 {
		t.Fatalf("live combo systems=(%d,%+v), coverage=%+v", len(systems), systems, coverage)
	}
	settledN, openN := 0, 0
	for _, system := range systems {
		if system.System != "combo-rfq/live" || system.FeeSource != "kalshi-portfolio-fills:fee_cost" || !system.FeeKnown || !system.CompleteHistory {
			t.Fatalf("native live combo route/provenance wrong: %+v", system)
		}
		if system.PnLKnown {
			settledN++
			if math.Abs(system.PnLPC-want/2) > 1e-12 {
				t.Fatalf("native live combo fee-net PnL=%v, want %v", system.PnLPC, want/2)
			}
		} else {
			openN++
		}
	}
	if settledN != 1 || openN != 0 || !strings.Contains(coverage[0].Note, "1 accept-ambiguous") ||
		!strings.Contains(coverage[0].Note, "1 pending/partial") {
		t.Fatalf("native live combo settled/open/coverage wrong: systems=%+v coverage=%+v", systems, coverage[0])
	}
	calibration, err := s.mlAccGatherLiveCombos(ctx)
	if err != nil {
		t.Fatalf("gather live-combo calibration: %v", err)
	}
	if len(calibration) != 1 || calibration[0].Book != "live_combos" ||
		calibration[0].Venue != "kalshi" || calibration[0].BundleKey != "A|YES" ||
		calibration[0].Ticker != "combo:A|YES" || calibration[0].Opened == 0 ||
		len(calibration[0].ResolutionKeys) != 1 {
		t.Fatalf("placed-combo calibration included ambiguous/non-reportable row: %+v", calibration)
	}
}
