package server

import (
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

// The Predictions /series response is the registry boundary. The fee-change endpoint retains
// history from the separate Margin exchange; detached PERP rows must never become prediction
// systems or generate false prediction-fee alerts.
func TestR144FeeRegistryIgnoresDetachedMarginHistory(t *testing.T) {
	current := map[string]kalFeeInfo{
		"KXBTC": kalFeeInfoFromVenue("quadratic", 1),
	}
	changes := map[string]kalshi.SeriesFeeChange{
		"KXBTC":     {SeriesTicker: "KXBTC", FeeType: "quadratic_with_maker_fees", FeeMultiplier: .5},
		"KXBTCPERP": {SeriesTicker: "KXBTCPERP", FeeType: "margin_market_maker_program_fees", FeeMultiplier: 0},
		"KXOLD":     {SeriesTicker: "KXOLD", FeeType: "flat", FeeMultiplier: 1},
	}

	if got := applyCurrentKalFeeChanges(current, changes); got != 2 {
		t.Fatalf("detached changes = %d, want 2", got)
	}
	if _, ok := current["KXBTCPERP"]; ok {
		t.Fatal("detached Margin-exchange PERP history entered Predictions fee registry")
	}
	if _, ok := current["KXOLD"]; ok {
		t.Fatal("detached flat-fee history entered current Predictions fee registry")
	}
	info := current["KXBTC"]
	if info.typ != "quadratic_with_maker_fees" || info.taker != .035 || info.maker != .00875 {
		t.Fatalf("current prediction override = %+v", info)
	}
}

func TestR144EventFeeOverrideRequiresCurrentPredictionParent(t *testing.T) {
	current := map[string]kalFeeInfo{
		"KXBTC": kalFeeInfoFromVenue("quadratic", 1),
	}
	makerType := "quadratic_with_maker_fees"
	marginType := "margin_market_maker_program_fees"
	half := .5
	zero := 0.0
	changes := map[string]kalshi.EventFeeChange{
		"KXBTC-26": {
			EventTicker: "KXBTC-26", SeriesTicker: "KXBTC",
			FeeTypeOverride: &makerType, FeeMultiplierOverride: &half,
		},
		"KXBTCPERP-EVENT": {
			EventTicker: "KXBTCPERP-EVENT", SeriesTicker: "KXBTCPERP",
			FeeTypeOverride: &marginType, FeeMultiplierOverride: &zero,
		},
	}

	events, parents, detached := currentKalEventFeeOverrides(current, changes)
	if detached != 1 {
		t.Fatalf("detached event changes = %d, want 1", detached)
	}
	if _, ok := events["KXBTCPERP-EVENT"]; ok {
		t.Fatal("detached Margin-exchange event entered Predictions fee registry")
	}
	if parents["KXBTC-26"] != "KXBTC" {
		t.Fatalf("event parent = %q, want KXBTC", parents["KXBTC-26"])
	}
	info, ok := events["KXBTC-26"]
	if !ok || info.typ != makerType || info.taker != .035 || info.maker != .00875 {
		t.Fatalf("current prediction event override = %+v ok=%v", info, ok)
	}
}

func TestR144CurrentFlatPredictionFeeStillFailsClosed(t *testing.T) {
	info := kalFeeInfoFromVenue("flat", 1)
	if kalFeeInfoSupported(info) {
		t.Fatal("current flat Predictions fee must stay blocked until venue publishes exact arithmetic")
	}
	if info.typ != "flat" || info.multiplier != 1 {
		t.Fatalf("flat fee evidence was not preserved: %+v", info)
	}
}
