package server

import (
	"context"
	"strings"
	"testing"
)

func TestR144BriefingDecisionSectionsAreCompactAndOrdered(t *testing.T) {
	s := testServer(t)
	got := s.BriefingText(context.Background())
	wants := []string{
		"🧬 build ",
		"⚪ Kalshi ",
		"🚀 Portfolio systems ·",
		"📊 Systems",
		"📈 Top 7 positive",
		"📉 Bottom 2 negative",
		"🤖 New ML",
		"Test ·",
		"Paper ·",
		"🧮 Proper Betting",
	}
	last := -1
	for _, want := range wants {
		i := strings.Index(got, want)
		if i < 0 {
			t.Fatalf("briefing missing %q:\n%s", want, got)
		}
		if i <= last {
			t.Fatalf("briefing section %q is out of order:\n%s", want, got)
		}
		last = i
	}
	for _, clutter := range []string{"Combo Lab", "Proper Betting effectiveness", "positions:", "controls:",
		"🧪 progress:", "⚠ research:", "🧭 routes:", "📐 signal CLV", "🎲 Combo Paper"} {
		if strings.Contains(got, clutter) {
			t.Fatalf("briefing leaked dashboard-only detail %q:\n%s", clutter, got)
		}
	}
	if lines := len(strings.Split(strings.TrimSpace(got), "\n")); lines > 34 {
		t.Fatalf("briefing is no longer a sub-minute scan (%d lines):\n%s", lines, got)
	}
}

func TestR145BriefExecutionIdentityRequiresExactChildSideAndRoute(t *testing.T) {
	for _, tc := range []struct {
		v    verdictEnt
		want string
		ok   bool
	}{
		{verdictEnt{Platform: "kalshi", Side: "YES", Route: "taker"}, "K/Y/T", true},
		{verdictEnt{Platform: "polyus", Side: "NO", Route: "maker"}, "PUS/N/M", true},
		{verdictEnt{Platform: "kalshi", Side: "BOTH", Route: "maker+taker"}, "", false},
		{verdictEnt{Platform: "kalshi", Side: "YES", Route: "maker+taker"}, "", false},
		{verdictEnt{Platform: "kalshi", Side: "", Route: "taker"}, "", false},
		{verdictEnt{Platform: "polymarket", Side: "YES", Route: "taker"}, "", false},
	} {
		got, ok := briefExactExecutionCell(tc.v)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("briefExactExecutionCell(%+v) = %q,%v; want %q,%v", tc.v, got, ok, tc.want, tc.ok)
		}
	}
}
