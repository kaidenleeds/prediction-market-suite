package kalshi

// R124: the venue's LIVE sub-cent schema arrived EARLY (2026-07-09, ~1,040 golf tickers with
// price_level_structure "tapered_deci_cent") and spells price_ranges as start/end/step — the R106
// defensive decode (min/max/tick + *_price/_size variants) missed that spelling, so TickFor
// silently fell back to the 1¢ default on those markets (orders stayed valid — every 1¢ price is
// on the 0.1¢ grid — but the write path was blind to the fine tail ticks). The payload below is
// the venue's verbatim shape for KXPGAR1LEAD-GESO26-JKRU, captured live and pinned.

import (
	"encoding/json"
	"math"
	"testing"
)

func TestR124TaperedDeciCentDecode(t *testing.T) {
	raw := `{"ticker":"KXPGAR1LEAD-GESO26-JKRU","status":"active","price_level_structure":"tapered_deci_cent",
	  "price_ranges":[{"start":"0.0000","end":"0.1000","step":"0.0010"},
	                  {"start":"0.1000","end":"0.9000","step":"0.0100"},
	                  {"start":"0.9000","end":"1.0000","step":"0.0010"}]}`
	var m Market
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m.PriceLevelStructure != "tapered_deci_cent" {
		t.Fatalf("price_level_structure = %q, want tapered_deci_cent", m.PriceLevelStructure)
	}
	for _, c := range []struct{ px, want float64 }{
		{0.05, 0.001}, // low tail: deci-cent
		{0.50, 0.01},  // body: classic cent
		{0.95, 0.001}, // high tail: deci-cent
		{0.10, 0.001}, // band boundary: first covering band wins (documented)
	} {
		if got := m.TickFor(c.px); got != c.want {
			t.Fatalf("TickFor(%.2f) = %v, want %v", c.px, got, c.want)
		}
	}
	// Markets without tick metadata keep the venue-historical 1¢.
	var plain Market
	if got := plain.TickFor(0.5); got != 0.01 {
		t.Fatalf("bare market TickFor = %v, want 0.01", got)
	}
}

func TestR132DirectionalTicksAtTaperedBoundaries(t *testing.T) {
	var m Market
	if err := json.Unmarshal([]byte(`{"price_ranges":[{"start":"0.0000","end":"0.1000","step":"0.0010"},{"start":"0.1000","end":"0.9000","step":"0.0100"},{"start":"0.9000","end":"1.0000","step":"0.0010"}]}`), &m); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ got, want float64 }{
		{m.NextPrice(0.10), 0.11},
		{m.PrevPrice(0.10), 0.099},
		{m.NextPrice(0.90), 0.901},
		{m.PrevPrice(0.90), 0.89},
		{m.NextPrice(0.055), 0.056},
		{m.PrevPrice(0.055), 0.054},
	} {
		if math.Abs(tc.got-tc.want) > 1e-9 {
			t.Fatalf("directional price=%v, want %v", tc.got, tc.want)
		}
	}
}
