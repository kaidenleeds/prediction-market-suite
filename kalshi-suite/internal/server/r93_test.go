package server

// R93 tests — operator: "make sure totals in cash is reset too" (the reset-straggler hunt) + the
// exact-format header. The R93 live diff (reset the running suite, diff every money field) found
// two user-visible numbers that never returned to baseline:
//  1. The briefing quick-read "(real $X · unreal now $Y)" suffixes printed LIFETIME realized-net
//     (vs[].realizedNet over the whole retained fill history) — post-reset the phone still said
//     "real -$2425.89". Fixed: venueRealizedNet (the RESETSIZE since-last-reset number).
//  2. The "🤖 ML eq … · K $X · US $Y" line summed ml_paper.json closed[] over the WHOLE retained
//     history — resets never moved it (force-closes even grew it). Fixed: only rows after the
//     last reset_close marker (markers excluded), the same epoch rule live_ml.py uses (R92).
// Venue-truth (live balance/positions) and explicitly-lifetime fields (ml net_lifetime and
// /api/stats by_source EV history) stay unreset by design.

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

// TestR93MLVenueSumsSinceReset — both compact ML briefing surfaces cover the current epoch only.
func TestR93MLVenueSumsSinceReset(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	closed := time.Now().Add(time.Minute).Unix()
	writeBookJSON(t, s.cfg().DataDir, "ml_paper.json", map[string]any{
		"bank0": 500.0, "epoch_id": "r93-v2",
		"open": []any{},
		"closed": []any{
			map[string]any{"platform": "kalshi", "ticker": "OLD-K", "model_cohort": currentMLCohort, "epoch_id": "r93-v2", "pnl": -50.0, "contracts": 10.0, "price": .4, "closed_ts": closed},
			map[string]any{"platform": "polyus", "ticker": "OLD-P", "model_cohort": currentMLCohort, "epoch_id": "r93-v2", "pnl": -400.0, "contracts": 10.0, "price": .4, "closed_ts": closed},
			map[string]any{"platform": "kalshi", "pnl": -10.0, "reset_close": 1.0}, // marker row (numeric encoding) — excluded
			map[string]any{"platform": "kalshi", "ticker": "R93-K", "model_cohort": currentMLCohort, "epoch_id": "r93-v2", "pnl": 7.0, "contracts": 10.0, "price": .4, "closed_ts": closed},
			map[string]any{"platform": "polyus", "ticker": "R93-P", "model_cohort": currentMLCohort, "epoch_id": "r93-v2", "pnl": -3.0, "contracts": 10.0, "price": .4, "closed_ts": closed},
		},
		"lifetime": map[string]any{"net": -456.0, "closed": 5.0, "wins": 1.0, "net_base": -460.0, "bank_ver": 3.0},
		"stats":    map[string]any{"net": 4.0, "equity": 504.0},
	})
	txt := s.BriefingText(ctx)
	if !strings.Contains(txt, "ML +$4.00") || !strings.Contains(txt, "Paper · 2 settled · +$4.00") {
		t.Fatalf("both compact ML surfaces must use only the current book-native-v2 epoch (want +$4.00):\n%s", txt)
	}
	if strings.Contains(txt, "-$53.00") || strings.Contains(txt, "-$403.00") {
		t.Fatalf("ML venue sums leaked pre-reset history:\n%s", txt)
	}
}

// (TestR93QuickReadRealSinceEpoch removed in R95: the "since start" quick-read block it pinned is
// gone — the operator's exact-format header replaced it; see r95_test.go for the new pins.)

// seedKalMeta puts a FRESH kalshi market into the in-memory meta cache (the xvgap price fallback
// path when no WS client exists — hermetic servers have s.kal == nil). Callers build the Market
// literal themselves: LastPrice's type is unexported, so only untyped constants can set it here.
func seedKalMeta(s *Server, km kalshi.Market) {
	s.metaMu.Lock()
	if s.kmkts == nil {
		s.kmkts = map[string]kalshi.Market{}
	}
	if s.kmktsAt == nil {
		s.kmktsAt = map[string]time.Time{}
	}
	s.kmkts[km.Ticker] = km
	s.kmktsAt[km.Ticker] = time.Now()
	s.metaMu.Unlock()
}

func seedKalBook(s *Server, km kalshi.Market) {
	seedKalMeta(s, km)
	if s.kal == nil {
		s.kal = kalshi.NewClient(kalshi.BaseDemo, nil, 100, time.Second)
	}
	s.kal.SeedTickers([]kalshi.Market{km})
}

func seedXVWinnerTwin(s *Server, slug, ticker, team string) {
	s.giMu.Lock()
	defer s.giMu.Unlock()
	st := s.gi()
	gameID := "test|" + slug
	st.games[gameID] = &giGame{away: team, home: "OPP"}
	st.gameMkts[gameID] = map[string][]string{"kalshi": {ticker}, "polyus": {slug}}
	st.byMkt["kalshi|"+ticker] = &giMktRef{gameID: gameID, venue: "kalshi", id: ticker, mktType: "winner", yesTeam: team}
	st.byMkt["polyus|"+slug] = &giMktRef{gameID: gameID, venue: "polyus", id: slug, mktType: "winner", yesTeam: team}
}

// TestR93XVGapCandidates — the R93 cross-venue gap logger: deterministic slug↔ticker matching,
// cheap-venue row shape, 3¢ floor, draw skip, start-time-prefixed abbreviation shape.
// R126: every deduped gap now emits BOTH expressions — the PUS row (family "xvgap" for the proven
// same-side-YES cohort, "xvgap2" for NO-side/flip) FIRST, then the Kalshi mirror ("xvgapk").
func TestR93XVGapCandidates(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.polyUSMu.Lock()
	s.polyUSAt = time.Now()
	s.polyUSMu.Unlock()
	seedKalBook(s, kalshi.Market{Ticker: "KXMLBGAME-26JUL06PHIKC-PHI", Status: "active", YesBid: 0.59, YesAsk: 0.61, LastPrice: 0.60})
	seedKalBook(s, kalshi.Market{Ticker: "KXFIBAGAME-26JUL061100ESTCZE-EST", Status: "active", YesBid: 0.39, YesAsk: 0.41, LastPrice: 0.40})
	seedXVWinnerTwin(s, "atc-mlb-phi-kc-2026-07-06-phi", "KXMLBGAME-26JUL06PHIKC-PHI", "PHI")
	seedXVWinnerTwin(s, "atc-fiba-est-cze-2026-07-06-est", "KXFIBAGAME-26JUL061100ESTCZE-EST", "EST")
	seedR139ResolutionBasis(t, s, "KXMLBGAME-26JUL06PHIKC-PHI", "atc-mlb-phi-kc-2026-07-06-phi")
	seedR139ResolutionBasis(t, s, "KXFIBAGAME-26JUL061100ESTCZE-EST", "atc-fiba-est-cze-2026-07-06-est")
	mk := func(slug string, yes float64) polyUSMarket {
		return polyUSMarket{Slug: slug, Game: "G", Yes: yes, Bid: yes - 0.01, Ask: yes + 0.01, BidSz: 20, AskSz: 30}
	}
	// PUS cheap by 10¢ → buy PUS YES (proven cohort) + the log-only Kalshi NO mirror.
	c := s.xvGapCandidates(ctx, []polyUSMarket{mk("atc-mlb-phi-kc-2026-07-06-phi", 0.50)})
	if len(c) != 2 || c[0].Platform != "polyus" || c[0].Side != "YES" || c[0].SignalType != "xvgap" {
		t.Fatalf("want polyus xvgap row first, got %+v", c)
	}
	if c[0].EntryPrice != 0.51 || c[0].Underlying != 0.60 || math.Abs(c[0].Strength-0.10) > 1e-9 ||
		!strings.Contains(c[0].ExecExpr, ":book=polyus-snapshot-book") {
		t.Fatalf("row prices wrong: %+v", c[0])
	}
	if c[1].Platform != "kalshi" || c[1].SignalType != "xvgapk" || c[1].Side != "NO" ||
		math.Abs(c[1].EntryPrice-0.41) > 1e-9 || !strings.Contains(c[1].ExecExpr, ":book=kalshi-ticker-ws") {
		t.Fatalf("kalshi mirror row wrong: %+v", c[1])
	}
	if c[0].Episode < 1 || c[0].Episode != c[1].Episode {
		t.Fatalf("episode must stamp both expressions of one signal: %+v vs %+v", c[0], c[1])
	}
	// Kalshi cheap by 10¢ → the SAME direction expressed as PUS NO (family xvgap2, NEW cohort,
	// log-only) + the Kalshi YES mirror (xvgapk) — pre-R126 this direction had no PUS expression.
	c = s.xvGapCandidates(ctx, []polyUSMarket{mk("atc-mlb-phi-kc-2026-07-06-phi", 0.70)})
	if len(c) != 2 || c[0].Platform != "polyus" || c[0].SignalType != "xvgap2" || c[0].Side != "NO" ||
		math.Abs(c[0].EntryPrice-0.31) > 1e-9 { // PUS NO ask = 1−YES bid
		t.Fatalf("kalshi-cheap direction must emit the PUS NO expression first: %+v", c)
	}
	if c[1].Platform != "kalshi" || c[1].Ticker != "KXMLBGAME-26JUL06PHIKC-PHI" || c[1].SignalType != "xvgapk" ||
		c[1].Side != "YES" || c[1].EntryPrice != 0.61 || math.Abs(c[1].Underlying-0.70) > 1e-9 {
		t.Fatalf("kalshi-cheap mirror row wrong: %+v", c[1])
	}
	// gap below the 3¢ floor → nothing.
	if c = s.xvGapCandidates(ctx, []polyUSMarket{mk("atc-mlb-phi-kc-2026-07-06-phi", 0.585)}); len(c) != 0 {
		t.Fatalf("sub-floor gap must not log: %+v", c)
	}
	// draws have no clean twin → skip.
	if c = s.xvGapCandidates(ctx, []polyUSMarket{mk("atc-fwc-phi-kc-2026-07-06-draw", 0.20)}); len(c) != 0 {
		t.Fatalf("draw must be skipped: %+v", c)
	}
	// start-time-prefixed kalshi shape (KXFIBAGAME-26JUL06→1100←ESTCZE) still matches; PUS 0.30
	// vs kalshi 0.40 → PUS is the cheap venue.
	if c = s.xvGapCandidates(ctx, []polyUSMarket{mk("atc-fiba-est-cze-2026-07-06-est", 0.30)}); len(c) != 2 || c[0].Platform != "polyus" || c[0].EntryPrice != 0.31 || c[0].Underlying != 0.40 {
		t.Fatalf("time-prefixed abbr shape must match (PUS cheap 0.30 vs kalshi 0.40): %+v", c)
	}
}

// TestR93XVGapSweepThrottle — one row per pair+side per 30 minutes; the sweep must not
// re-log the same gap every 60s tick. (R126: keys are side-aware; the Kalshi mirror row
// stamps its own key in the same pass.)
func TestR93XVGapSweepThrottle(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.polyUSMu.Lock()
	s.polyUSAt = time.Now()
	s.polyUSMu.Unlock()
	seedKalBook(s, kalshi.Market{Ticker: "KXMLBGAME-26JUL06PHIKC-PHI", Status: "active", YesBid: 0.59, YesAsk: 0.61, LastPrice: 0.60})
	seedXVWinnerTwin(s, "atc-mlb-phi-kc-2026-07-06-phi", "KXMLBGAME-26JUL06PHIKC-PHI", "PHI")
	seedR139ResolutionBasis(t, s, "KXMLBGAME-26JUL06PHIKC-PHI", "atc-mlb-phi-kc-2026-07-06-phi")
	mkts := []polyUSMarket{{Slug: "atc-mlb-phi-kc-2026-07-06-phi", Game: "G", Yes: 0.50,
		Bid: 0.49, Ask: 0.51, BidSz: 20, AskSz: 30}}
	s.xvGapSweep(ctx, mkts)
	s.xvGapMu.Lock()
	first, ok := s.xvGapSeen["atc-mlb-phi-kc-2026-07-06-phi|polyus|YES"]
	_, okMirror := s.xvGapSeen["KXMLBGAME-26JUL06PHIKC-PHI|kalshi|NO"]
	n := len(s.xvGapSeen)
	s.xvGapMu.Unlock()
	if !ok || !okMirror || n != 2 {
		t.Fatalf("first sweep must stamp both expression keys (n=%d ok=%v mirror=%v)", n, ok, okMirror)
	}
	s.xvGapSweep(ctx, mkts)
	s.xvGapMu.Lock()
	second := s.xvGapSeen["atc-mlb-phi-kc-2026-07-06-phi|polyus|YES"]
	s.xvGapMu.Unlock()
	if !second.Equal(first) {
		t.Fatal("second sweep inside 30min must SKIP (throttle re-stamped)")
	}
}

// (TestR93HeaderPercentOfStartingBankroll removed in R127: the session-delta header it pinned is
// superseded by the four-book all-time block — see r127_test.go for the new header pins. The
// Shadow book no longer has a header line; its body line stays pinned in r101_test.go.)
