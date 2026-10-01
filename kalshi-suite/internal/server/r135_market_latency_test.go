package server

import (
	"strings"
	"testing"
	"time"
)

// Complete-board classification must never enter the signal-only PolyUS start-borrow matcher.
// That matcher scans the PolyUS universe; calling it once per Kalshi row made /api/markets
// O(Kalshi x PolyUS) and turned a warm-cache request into a 27-second rebuild.
func TestKalshiBoardLiveLabelNeverScansStartBorrow(t *testing.T) {
	resetMatchTelemetry()
	s := &Server{}

	if got := s.kalshiBoardLiveLabel("KXNOEMBEDDEDDATE-TEST"); got != -1 {
		t.Fatalf("dateless board row = %d, want unknown (-1)", got)
	}
	if attempts, hits := matchFamilyCounts("startborrow"); attempts != 0 || hits != 0 {
		t.Fatalf("board liveness touched start-borrow matcher: attempts=%d hits=%d", attempts, hits)
	}
}

// The fast board path keeps R135's start-first semantics: known future is PRE despite a fresh
// tape, while a recently started market with fresh activity is LIVE.
func TestKalshiBoardLiveLabelPreservesStartFirstSemantics(t *testing.T) {
	et := etLocation()
	if et == nil {
		t.Fatal("America/New_York timezone unavailable")
	}
	tickerAt := func(at time.Time) string {
		return "KXTEST-" + strings.ToUpper(at.In(et).Format("06Jan021504")) + "-SIDE"
	}
	now := time.Now()
	future := tickerAt(now.Add(2 * time.Hour))
	past := tickerAt(now.Add(-time.Hour))
	s := &Server{kmktsAt: map[string]time.Time{future: now, past: now}}

	if got := s.kalshiBoardLiveLabel(future); got != 0 {
		t.Fatalf("future market with fresh tape = %d, want pre (0)", got)
	}
	if got := s.kalshiBoardLiveLabel(past); got != 1 {
		t.Fatalf("started market with fresh tape = %d, want live (1)", got)
	}
}

func BenchmarkKalshiBoardLiveLabel(b *testing.B) {
	s := &Server{}
	tickers := make([]string, 20_000)
	for i := range tickers {
		tickers[i] = "KXCOMPLETEBOARD-NODATE-" + string(rune('A'+i%26))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.kalshiBoardLiveLabel(tickers[i%len(tickers)])
	}
}
