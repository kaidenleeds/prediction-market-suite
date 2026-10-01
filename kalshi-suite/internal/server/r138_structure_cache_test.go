package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func TestR138StructureLifecyclePatchesWarmServerCacheWithoutRefreshingPrice(t *testing.T) {
	var market kalshi.Market
	if err := json.Unmarshal([]byte(`{"ticker":"KXTICK","price_level_structure":"old","price_ranges":[{"start":"0.0","end":"1.0","step":"0.01"}]}`), &market); err != nil {
		t.Fatal(err)
	}
	priceAt := time.Now().Add(-time.Minute)
	s := &Server{kmkts: map[string]kalshi.Market{"KXTICK": market}, kmktsAt: map[string]time.Time{"KXTICK": priceAt}}

	// A changed structure with omitted ranges must clear the stale grid instead of inheriting it.
	s.applyKalshiPriceStructureUpdate(kalshi.PriceStructureUpdate{Ticker: "kxtick", PriceLevelStructure: "new"})
	got, ok := s.kmkt("KXTICK")
	if !ok || got.PriceLevelStructure != "new" || len(got.PriceRanges) != 0 {
		t.Fatalf("warm cache was not fail-closed patched: %#v", got)
	}
	if _, known := got.TickForKnown(.5); known {
		t.Fatal("omitted new grid inherited stale tick metadata")
	}
	if !s.kmktsAt["KXTICK"].Equal(priceAt) {
		t.Fatal("structure receipt incorrectly refreshed market-price age")
	}

	// An explicit scalar tick can be applied exactly even though its fixed-point decoder is private
	// to the venue package.
	s.applyKalshiPriceStructureUpdate(kalshi.PriceStructureUpdate{Ticker: "KXTICK", TickSize: .002, TickSizePresent: true})
	got, _ = s.kmkt("KXTICK")
	if tick, known := got.TickForKnown(.5); !known || tick != .002 {
		t.Fatalf("explicit tick = %v known=%v, want .002 true", tick, known)
	}
}
