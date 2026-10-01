package kalshi

import "testing"

func TestR133TickForKnownDoesNotPromoteFallbackToSchemaTruth(t *testing.T) {
	if _, ok := (Market{}).TickForKnown(.50); ok {
		t.Fatal("absent tick metadata must stay unknown for book-v1")
	}
	if tick, ok := (Market{PriceLevelStructure: "linear_cent"}).TickForKnown(.50); !ok || tick != .01 {
		t.Fatalf("authoritative linear_cent tick=%v ok=%v", tick, ok)
	}
	m := Market{PriceRanges: []PriceRange{{Start: flexFloat(0), End: flexFloat(.10), Step: flexFloat(.001)}}}
	if tick, ok := m.TickForKnown(.05); !ok || tick != .001 {
		t.Fatalf("taper tick=%v ok=%v", tick, ok)
	}
}
