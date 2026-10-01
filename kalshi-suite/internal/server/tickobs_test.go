package server

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestTickObsSeriesKey(t *testing.T) {
	tests := []struct {
		event, ticker, want string
	}{
		{"KXBTC15M-26JUL11-T10", "ignored", "KXBTC15M"},
		{"", "kxgolf-26jul11-player", "KXGOLF"},
		{" KXWCGOAL ", "", "KXWCGOAL"},
		{"", "", ""},
	}
	for _, tc := range tests {
		if got := tickObsSeriesKey(tc.event, tc.ticker); got != tc.want {
			t.Fatalf("tickObsSeriesKey(%q,%q) = %q, want %q", tc.event, tc.ticker, got, tc.want)
		}
	}
}

func TestTickObsPilotStateSurvivesBootAndBoundsRollingSeries(t *testing.T) {
	now := time.Date(2026, 7, 11, 15, 0, 0, 0, time.UTC)
	var st tickObsDiskState
	var first []tickObsPilotCandidate
	for i := 0; i < 20; i++ {
		first = append(first, tickObsPilotCandidate{
			Ticker: fmt.Sprintf("KXBTC15M-%02d", i), Series: "KXBTC15M", Label: "tick=0.0010", Close: int64(i),
		})
	}
	first = append(first, tickObsPilotCandidate{Ticker: "KXGOLF-P1", Series: "KXGOLF", Label: "structure=tapered_deci_cent"})
	if got := len(tickObsApplyPilotSweep(&st, first, now)); got != tickObsPilotPerSeriesCap+1 {
		t.Fatalf("new pilot samples = %d, want %d", got, tickObsPilotPerSeriesCap+1)
	}
	if len(st.Pilots) != tickObsPilotPerSeriesCap+1 {
		t.Fatalf("retained pilots = %d, want %d", len(st.Pilots), tickObsPilotPerSeriesCap+1)
	}
	if st.SeriesLatchesLifetime != 2 {
		t.Fatalf("series latches = %d, want 2", st.SeriesLatchesLifetime)
	}

	path := filepath.Join(t.TempDir(), tickObsStateFile)
	if err := tickObsSaveState(path, st); err != nil {
		t.Fatal(err)
	}
	reloaded, err := tickObsLoadState(path)
	if err != nil {
		t.Fatal(err)
	}
	// A sweep rewrites the same state path every ten minutes; pin replacement semantics on Windows.
	if err := tickObsSaveState(path, reloaded); err != nil {
		t.Fatalf("replace existing state: %v", err)
	}
	before := reloaded.TickerLatchesLifetime
	if got := len(tickObsApplyPilotSweep(&reloaded, first, now.Add(time.Hour))); got != 0 {
		t.Fatalf("same current samples relatched after boot: %d", got)
	}
	if reloaded.TickerLatchesLifetime != before {
		t.Fatalf("lifetime latches changed after boot: %d -> %d", before, reloaded.TickerLatchesLifetime)
	}

	var next []tickObsPilotCandidate
	for i := 20; i < 40; i++ {
		next = append(next, tickObsPilotCandidate{
			Ticker: fmt.Sprintf("KXBTC15M-%02d", i), Series: "KXBTC15M", Label: "tick=0.0010", Close: int64(i),
		})
	}
	next = append(next, tickObsPilotCandidate{Ticker: "KXNEW-P1", Series: "KXNEW", Label: "tick=0.0010"})
	tickObsApplyPilotSweep(&reloaded, next, now.Add(2*time.Hour))
	perCrypto := 0
	for _, p := range reloaded.Pilots {
		if p.Series == "KXBTC15M" {
			perCrypto++
		}
	}
	if perCrypto != tickObsPilotPerSeriesCap {
		t.Fatalf("rolling crypto retained %d samples, want %d", perCrypto, tickObsPilotPerSeriesCap)
	}
	if _, ok := reloaded.Series["KXNEW"]; !ok {
		t.Fatal("new stable series was blocked by rolling crypto")
	}
}

func TestTickObsGlobalCapEvictsOldestInsteadOfBlockingDiscovery(t *testing.T) {
	now := time.Date(2026, 7, 11, 15, 0, 0, 0, time.UTC)
	st := tickObsDiskState{}
	tickObsInitState(&st)
	for i := 0; i < tickObsPilotCap; i++ {
		ticker := fmt.Sprintf("OLD-%04d", i)
		st.Pilots[ticker] = tickObsPilotState{Series: fmt.Sprintf("S%04d", i/tickObsPilotPerSeriesCap), LastSeen: now.Add(-time.Hour).Unix()}
	}
	newTicker := "BRANDNEW-1"
	tickObsApplyPilotSweep(&st, []tickObsPilotCandidate{{Ticker: newTicker, Series: "BRANDNEW", Label: "tick=0.0010"}}, now)
	if len(st.Pilots) != tickObsPilotCap {
		t.Fatalf("retained pilots = %d, want cap %d", len(st.Pilots), tickObsPilotCap)
	}
	if _, ok := st.Pilots[newTicker]; !ok {
		t.Fatal("new series ticker was blocked at the global cap")
	}
}
