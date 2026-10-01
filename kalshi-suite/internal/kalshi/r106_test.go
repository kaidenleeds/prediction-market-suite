// r106_test.go — R106 pins (auditor bug 254): per-market tick metadata + the tick-aware snap and
// wire formatting that replace the cent-locked write-path assumptions ahead of the venue's
// ~Jul-23 sub-cent pilot. Today's behavior must be byte-identical (1¢ default, 2-decimal wire).
package kalshi

import (
	"encoding/json"
	"testing"
)

func TestR106TickForDefaultsAndRanges(t *testing.T) {
	var m Market
	if got := m.TickFor(0.50); got != 0.01 {
		t.Fatalf("bare market tick = %v, want the historical 0.01", got)
	}
	// market-level tick_size (either spelling era), decoded via the wire path
	if err := json.Unmarshal([]byte(`{"ticker":"X","tick_size":0.005}`), &m); err != nil {
		t.Fatal(err)
	}
	if got := m.TickFor(0.50); got != 0.005 {
		t.Fatalf("tick_size market = %v, want 0.005", got)
	}
	// banded price_ranges win over the market-level tick; uncovered prices fall back
	var b Market
	if err := json.Unmarshal([]byte(`{"ticker":"X","tick_size":0.01,
		"price_ranges":[{"min_price":0.01,"max_price":0.05,"tick_size":0.001},{"min":0.05,"max":0.95,"tick":0.005}]}`), &b); err != nil {
		t.Fatal(err)
	}
	if got := b.TickFor(0.03); got != 0.001 {
		t.Fatalf("low band tick = %v, want 0.001", got)
	}
	if got := b.TickFor(0.50); got != 0.005 {
		t.Fatalf("mid band tick = %v, want 0.005", got)
	}
	if got := b.TickFor(0.97); got != 0.01 {
		t.Fatalf("uncovered price tick = %v, want the market-level 0.01", got)
	}
}

func TestR106SnapAndFmtPx(t *testing.T) {
	// today's grid: identical behavior to the old ±0.01 + %.2f path
	if got := SnapPx(0.47, 0.01); got != 0.47 {
		t.Fatalf("cent snap = %v", got)
	}
	if got := FmtPx(0.40); got != "0.40" {
		t.Fatalf("FmtPx(0.40) = %q, want the historical 2-decimal wire shape", got)
	}
	if got := FmtPx(0.47); got != "0.47" {
		t.Fatalf("FmtPx(0.47) = %q", got)
	}
	// sub-cent expressible without float dust
	if got := FmtPx(SnapPx(0.485, 0.005)); got != "0.485" {
		t.Fatalf("half-cent = %q, want 0.485", got)
	}
	if got := FmtPx(SnapPx(0.4849, 0.005)); got != "0.485" {
		t.Fatalf("snap-to-half-cent = %q, want 0.485", got)
	}
	// The executable path can create ordinary cent prices through complements such as 1-price.
	// Those values carry binary dust in float64, but Kalshi accepts at most four wire decimals.
	for in, want := range map[float64]string{
		0.050000000000000044: "0.05",
		0.06999999999999995:  "0.07",
		0.07999999999999996:  "0.08",
		0.18000000000000005:  "0.18",
		0.1234:               "0.1234",
	} {
		if got := FmtPx(in); got != want {
			t.Fatalf("FmtPx(%0.18f) = %q, want %q", in, got, want)
		}
	}
	// clamps: never 0, never 1
	if got := SnapPx(0.001, 0.01); got != 0.01 {
		t.Fatalf("low clamp = %v, want 0.01", got)
	}
	if got := SnapPx(0.999, 0.01); got != 0.99 {
		t.Fatalf("high clamp = %v, want 0.99", got)
	}
	// zero/absent tick degrades to the cent grid
	if got := SnapPx(0.4849, 0); got != 0.48 {
		t.Fatalf("zero-tick snap = %v, want 0.48", got)
	}
}
