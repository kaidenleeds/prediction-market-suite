package server

// R97 tests — the operator's four phone-screenshot bugs (2026-07-06 15:14/15:20 sends):
//   bug 1: the telegram body died with "Paper briefing unavailable right now." on ONE failed
//          fills read, silently — now: bounded retry + last-good cache + "as of HH:MM" stamp +
//          WARN with the real cause; the ML/Shadow/Parlays sections always render.
//   bug 2: "Kalshi auto" and "PolyUS auto" mirrored each other to the cent (both −$5.60) — the
//          lines rode frac × SHARED NAV; now each line is its own venue's realized+unrealized,
//          and a book with no data this tick renders a tiny "(n/a)", never another book's number.
//   bug 3: Portfolio composition (Shadow/Kflow excluded) is pinned in the revised
//          TestR95PortfolioSumsBookDeltas.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

func TestR132PaperFillsFailureSingleflight(t *testing.T) {
	s := testServer(t)
	s.fillsCacheTTL = -1
	var calls atomic.Int32
	s.fillsReadFn = func(context.Context) ([]paper.Fill, error) {
		calls.Add(1)
		return nil, errors.New("db unavailable")
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, _, err := s.paperFillsCached(context.Background())
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err == nil {
			t.Fatal("cold-cache failure must reach every waiter")
		}
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("20 concurrent callers ran %d underlying reads, want one three-attempt flight", got)
	}
}

// headerLine plucks the first header line starting with "<dot> <name> " for exact-shape asserts.
func headerLine(txt, name string) string {
	for _, ln := range strings.Split(strings.TrimSpace(txt), "\n") {
		for _, dot := range []string{"🟢 ", "🔴 ", "⚪ "} {
			if strings.HasPrefix(ln, dot+name+" ") {
				return ln
			}
		}
	}
	return ""
}

// TestR97BriefingSurvivesFillsOutageWithCache — bug 1's exact scenario (R127 shapes): the DB read
// starts failing mid-session. The briefing must serve the last-good paper snapshot with an as-of
// stamp — the venue-book lines keep real numbers, never a hole.
func TestR97BriefingSurvivesFillsOutageWithCache(t *testing.T) {
	s := testServer(t)
	s.fillsCacheTTL = -1 // always refetch — the cache acts purely as the last-good fallback here
	ctx := context.Background()
	if _, _, fresh, err := s.paperFillsCached(ctx); err != nil || !fresh {
		t.Fatalf("prime read failed: fresh=%v err=%v", fresh, err)
	}
	s.fillsReadFn = func(context.Context) ([]paper.Fill, error) {
		return nil, errors.New("database is locked (5) (SQLITE_BUSY)")
	}
	txt := s.BriefingText(ctx)
	if strings.Contains(txt, "unavailable right now") {
		t.Fatalf("bug 1 fallback line still present:\n%s", txt)
	}
	if strings.Contains(txt, "🎰 Parlays") {
		t.Fatalf("R101: the parlay section is archived — it must stay gone (outage or not):\n%s", txt)
	}
	if !strings.Contains(txt, "paper numbers as of ") {
		t.Fatalf("last-good serve must stamp its as-of time:\n%s", txt)
	}
	if !strings.Contains(txt, "Kalshi +$0.00 · n/a/bet · n/a¢/u") {
		t.Fatalf("the Kalshi book line must serve from the last-good cache, not degrade to n/a:\n%s", txt)
	}
}

// TestR97BriefingDegradesHonestlyWithNoCache — cold boot straight into an outage (no last-good
// snapshot yet): the body still renders and says what's missing; the venue-book lines read
// "net n/a" — honest, and structurally incapable of fabricating a zero.
func TestR97BriefingDegradesHonestlyWithNoCache(t *testing.T) {
	s := testServer(t)
	s.fillsCacheTTL = -1
	s.fillsReadFn = func(context.Context) ([]paper.Fill, error) {
		return nil, errors.New("database is locked (5) (SQLITE_BUSY)")
	}
	ctx := context.Background()
	txt := s.BriefingText(ctx)
	if strings.Contains(txt, "unavailable right now") {
		t.Fatalf("bug 1 fallback line still present:\n%s", txt)
	}
	if strings.Contains(txt, "🎰 Parlays") {
		t.Fatalf("R101: the archived parlay section must stay gone even with no fills data:\n%s", txt)
	}
	if !strings.Contains(txt, "paper stats n/a this tick") {
		t.Fatalf("cold-cache outage must say so:\n%s", txt)
	}
	if !strings.Contains(txt, "Kalshi n/a · n/a/bet · n/a¢/u") || !strings.Contains(txt, "PolyUS n/a · n/a/bet · n/a¢/u") {
		t.Fatalf("venue-book lines with NO data must render net n/a, not fake zeros:\n%s", txt)
	}
}

// TestR97VenueLinesComputeIndependently — bug 2 (R127 shapes): a Kalshi-only round trip must move
// ONLY the Kalshi book's all-time net; the PolyUS book stays flat (the venue books are composed
// from per-venue truth, structurally incapable of mirroring one shared number).
func TestR97VenueLinesComputeIndependently(t *testing.T) {
	s := testServer(t)
	s.fillsCacheTTL = -1
	ctx := context.Background()
	// Kalshi-only round trip: BUY 10 @ 50c → SELL 10 @ 60c = +$1.00 realized, PolyUS untouched.
	for _, f := range []paper.Fill{
		{Platform: "kalshi", Ticker: "KXR97", Title: "R97", Side: "YES", Action: "BUY", Price: 0.50, Contracts: 10, Source: "auto-cons-kalshi"},
		{Platform: "kalshi", Ticker: "KXR97", Title: "R97", Side: "YES", Action: "SELL", Price: 0.60, Contracts: 10, Source: "auto-cons-kalshi"},
	} {
		if _, err := s.store.InsertPaperFill(ctx, f); err != nil {
			t.Fatalf("insert fill: %v", err)
		}
	}
	hdr := s.equityHeaderBlock(ctx)
	k, us := headerLine(hdr, "Kalshi"), headerLine(hdr, "PolyUS")
	if !strings.HasPrefix(k, "🟢 Kalshi +$1.00 · +$1.00/bet · +10.0¢/u") {
		t.Fatalf("the Kalshi book must carry this run's +$1.00: %q\n%s", k, hdr)
	}
	if !strings.HasPrefix(us, "⚪ PolyUS +$0.00 · n/a/bet · n/a¢/u") {
		t.Fatalf("the PolyUS book must stay flat (bug 2 was the venues mirroring one number): %q\n%s", us, hdr)
	}
	if !strings.Contains(hdr, "Live Kalshi") || !strings.Contains(hdr, "Live PolyUS") {
		t.Fatalf("real-money briefing must always split both venue accounts:\n%s", hdr)
	}
}
