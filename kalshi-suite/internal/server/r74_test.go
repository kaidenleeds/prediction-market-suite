package server

// R74 tests — the book-WS working-set selection (pure) and the config cap clamp. No network, no
// sleeps: bookWorkingSet is a pure function and bookWSCap only reads cfg.

import (
	"reflect"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
)

func TestBookWorkingSetPriorityDedupeAndVolumeFill(t *testing.T) {
	groups := [][]string{
		{"POS-1", "POS-2"},       // held positions (highest priority)
		{"REST-1", "POS-1"},      // resting orders — POS-1 dupe must not repeat
		{"PROP-1", "", "PROP-2"}, // proposal candidates — empty ticker skipped
		{"COMBO-1", "REST-1"},    // combo legs — dupe again
	}
	byVol := []string{"POS-2", "VOL-1", "VOL-2", "VOL-3"} // POS-2 already in — fill starts at VOL-1
	got := bookWorkingSet(8, groups, byVol)
	want := []string{"POS-1", "POS-2", "REST-1", "PROP-1", "PROP-2", "COMBO-1", "VOL-1", "VOL-2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("working set = %v, want %v", got, want)
	}
}

func TestBookWorkingSetCapTruncatesInPriorityOrder(t *testing.T) {
	groups := [][]string{{"P1", "P2"}, {"R1", "R2"}, {"Q1"}, {"C1"}}
	got := bookWorkingSet(3, groups, []string{"V1", "V2"})
	want := []string{"P1", "P2", "R1"} // positions before resting before proposals; volume never displaces always-in
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("capped set = %v, want %v", got, want)
	}
	if out := bookWorkingSet(0, groups, nil); out != nil {
		t.Fatalf("cap 0 must yield nil, got %v", out)
	}
	// Exactly-at-cap: nothing from the fill list sneaks past the boundary.
	if out := bookWorkingSet(6, groups, []string{"V1"}); !reflect.DeepEqual(out, []string{"P1", "P2", "R1", "R2", "Q1", "C1"}) {
		t.Fatalf("at-cap set = %v", out)
	}
}

func TestBookWSCapClamp(t *testing.T) {
	for _, tc := range []struct{ in, want int }{
		{0, 600},     // R133 default
		{5, 50},      // clamp floor
		{50, 50},     //
		{700, 700},   // in-range passes through
		{5000, 1000}, // clamp ceiling
	} {
		s := &Server{}
		s.cfgP.Store(&config.Config{KalshiBookWSCap: tc.in}) // R76 (bug 18): copy-on-write snapshot
		if got := s.bookWSCap(); got != tc.want {
			t.Fatalf("bookWSCap(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
