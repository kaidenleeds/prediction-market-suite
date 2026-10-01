package server

import (
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

func TestR133ClosedSinceBootRefusesTruncatedHistory(t *testing.T) {
	boot := time.Now().Add(-time.Hour)
	recent := make([]time.Time, 40)
	for i := range recent {
		recent[i] = boot.Add(time.Duration(i+1) * time.Minute)
	}
	if n, ok := closedSinceBoot(recent, 40, boot); ok || n != 40 {
		t.Fatalf("truncated newest-only history claimed completeness: n=%d ok=%v", n, ok)
	}
	recent[39] = boot.Add(-time.Second)
	if n, ok := closedSinceBoot(recent, 40, boot); !ok || n != 39 {
		t.Fatalf("history reaching boot was not complete: n=%d ok=%v", n, ok)
	}
}

func TestR152OpenShrinkForcesPrioritySettlementOnlyOnce(t *testing.T) {
	s := &Server{}
	s.liveSettleAt = time.Now()
	if s.observeKalshiLiveOpenCount(9) {
		t.Fatal("first authenticated count is a baseline, not a shrink")
	}
	if s.observeKalshiLiveOpenCount(9) {
		t.Fatal("unchanged count forced a settlement read")
	}
	if !s.observeKalshiLiveOpenCount(7) || !s.liveSettleAt.IsZero() {
		t.Fatal("9 -> 7 did not force priority settlement")
	}
	s.liveSettleAt = time.Now()
	if s.observeKalshiLiveOpenCount(7) || s.liveSettleAt.IsZero() {
		t.Fatal("same post-shrink snapshot forced duplicate settlement work")
	}
}

func TestR133PolyUSLiveBriefCountsAndUnknowns(t *testing.T) {
	boot := time.Now().Add(-time.Hour).UTC()
	pos := []polymarketus.PUSPosition{{Net: 2}, {Net: -1}, {Net: 3, Expired: true}, {Net: 0}}
	hist := []polymarketus.PUSActivity{
		{Type: "POSITION_RESOLUTION", Time: boot.Add(10 * time.Minute).Format(time.RFC3339Nano)},
		{Type: "TRADE", Time: boot.Add(5 * time.Minute).Format(time.RFC3339Nano)},
	}
	n := polyUSLiveBriefCounts(pos, hist, boot, true, true)
	if !n.OpenOK || n.Open != 2 || !n.ClosedOK || n.Closed != 1 {
		t.Fatalf("live PolyUS n truth drifted: %+v", n)
	}
	if got := liveBriefNSuffix(liveBriefN{}); got != "🅾️n/a©️n/a" {
		t.Fatalf("unknown live counts fabricated zeros: %q", got)
	}
}
