package kalshi

import (
	"math"
	"testing"
	"time"
)

func TestLifecyclePriceStructureHotPatchesMarketCache(t *testing.T) {
	c := &Client{mktCache: []Market{{
		Ticker: "KXTICK", PriceLevelStructure: "linear_cent", TickSize: flexFloat(0.01),
		PriceRanges: []PriceRange{{Start: flexFloat(0), End: flexFloat(1), Step: flexFloat(0.01)}},
	}}}
	c.ingestLifecycle([]byte(`{"type":"market_lifecycle_v2","msg":{"event_type":"price_level_structure_updated","market_ticker":"KXTICK","price_level_structure":"center_whole_edge_quint_cent","price_ranges":[{"start":"0.0000","end":"0.1000","step":"0.0020"},{"start":"0.1000","end":"0.9000","step":"0.0100"}],"tick_size_dollars":"0.0020","ts_ms":1700000000123}}`))

	u, ok := c.LatestPriceStructure("KXTICK")
	if !ok || u.PriceLevelStructure != "center_whole_edge_quint_cent" || !u.RangesPresent || len(u.PriceRanges) != 2 || !u.TickSizePresent || math.Abs(u.TickSize-0.002) > 1e-12 {
		t.Fatalf("parsed structure receipt missing: ok=%v update=%+v", ok, u)
	}
	if !u.Observed.Equal(time.UnixMilli(1700000000123).UTC()) {
		t.Fatalf("observed=%v, want WS timestamp", u.Observed)
	}
	c.mktMu.Lock()
	m := c.mktCache[0]
	c.mktMu.Unlock()
	if m.PriceLevelStructure != u.PriceLevelStructure || len(m.PriceRanges) != 2 || math.Abs(m.TickSize.Float()-0.002) > 1e-12 {
		t.Fatalf("market cache was not hot-patched: %+v", m)
	}
	if tick, ok := m.TickForKnown(0.05); !ok || math.Abs(tick-0.002) > 1e-12 {
		t.Fatalf("updated edge tick=%v known=%v, want 0.002", tick, ok)
	}
}

func TestLifecycleStructureChangeWithoutRangesClearsStaleGrid(t *testing.T) {
	c := &Client{mktCache: []Market{{
		Ticker: "KXTICK", PriceLevelStructure: "old", TickSize: flexFloat(0.001),
		PriceRanges: []PriceRange{{Start: flexFloat(0), End: flexFloat(1), Step: flexFloat(0.001)}},
	}}}
	c.ingestLifecycle([]byte(`{"type":"market_lifecycle_v2","msg":{"event_type":"price_level_structure_updated","market_ticker":"KXTICK","price_level_structure":"new"}}`))
	c.mktMu.Lock()
	m := c.mktCache[0]
	c.mktMu.Unlock()
	if m.PriceLevelStructure != "new" || len(m.PriceRanges) != 0 || m.TickSize.Float() != 0 {
		t.Fatalf("stale price grid survived a structure change: %+v", m)
	}
}
