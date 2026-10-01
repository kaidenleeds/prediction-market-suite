package server

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"
)

func TestR169PaperSurfaceShowsOnlyCurrentFundedGenfollowLots(t *testing.T) {
	s := testServer(t)
	view, lot := r167PaperProofFixture(
		"r169-paper-surface", "spotlag", "kalshi", "KXR169-SURFACE", "YES", 2, .40, .03, 25,
	)
	r167InsertPaperProof(t, s, view)

	// A modeled PAPER-FILLED receipt is execution evidence, not a funded position by itself.
	if rows, available := s.currentGenfollowFundedPositions(); !available || len(rows) != 0 {
		t.Fatalf("shadow-only modeled fill became a funded position: available=%v rows=%+v", available, rows)
	}

	legacyPolyUS := kfPos{TS: time.Now().UTC().Format(time.RFC3339Nano),
		Ticker: "R169-POLYUS-LEGACY", Side: "NO", Price: .31, Contracts: 1}
	s.gfBook = &gfBookState{Subs: map[string]*kfBook{
		"gf:spotlag-k": {Open: []kfPos{lot}},
		"gf:xvgap-p":   {Open: []kfPos{legacyPolyUS}},
	}}
	rows, available := s.currentGenfollowFundedPositions()
	if !available || len(rows) != 2 {
		t.Fatalf("current funded Genfollow lot missing: available=%v rows=%+v", available, rows)
	}
	var got paperFundedPosition
	var polyUS paperFundedPosition
	for _, row := range rows {
		if row.Ticker == lot.Ticker {
			got = row
		}
		if row.Ticker == legacyPolyUS.Ticker {
			polyUS = row
		}
	}
	if got.Ticker != lot.Ticker || got.SubBook != "gf:spotlag-k" || got.Portfolio != vbKalshi ||
		got.ExecutionShadowAttempt != view.Attempt.AttemptID || got.State != "funded-current" ||
		!got.ReadOnly || got.CloseSupportedByPaperAPI ||
		math.Abs(got.CostBasisUSD-(lot.Contracts*lot.Price+lot.Fee)) > 1e-12 {
		t.Fatalf("funded position projection is not truthful: %+v", got)
	}
	if polyUS.Platform != vbPolyus || polyUS.Portfolio != vbPolyus {
		t.Fatalf("legacy -p lot lost its authoritative PolyUS sub-book venue: %+v", polyUS)
	}

	// A clean reset archives old opens and the current surface must immediately become empty.
	if result, err := s.resetGenfollowBooksAt(time.Now().UTC().Add(time.Second)); err != nil || !result.Committed {
		t.Fatalf("reset Genfollow result=%+v err=%v", result, err)
	}
	if rows, available = s.currentGenfollowFundedPositions(); !available || len(rows) != 0 {
		t.Fatalf("archived pre-reset lot remained current: available=%v rows=%+v", available, rows)
	}
}

func TestR169PaperAPISeparatesCloseableAndFundedLedgers(t *testing.T) {
	s := testServer(t)
	_, lot := r167PaperProofFixture(
		"r169-api-funded", "spotlag", "kalshi", "KXR169-API", "NO", 3, .25, .02, 20,
	)
	s.gfBook = &gfBookState{Subs: map[string]*kfBook{
		"gf:spotlag-k": {Open: []kfPos{lot}},
	}}

	closeable, _, _, err := s.paperPositionsCached(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(closeable) != 0 {
		t.Fatalf("Genfollow lot leaked into closeable paper_fills positions: %+v", closeable)
	}
	funded, fundedAvailable := s.currentGenfollowFundedPositions()
	if !fundedAvailable || len(funded) != 1 || funded[0].Ticker != lot.Ticker {
		t.Fatalf("funded read-only position missing from Paper surface: %+v", funded)
	}
	portfolios := s.currentPaperPortfolioSurfaces(context.Background())
	if len(portfolios) != 5 {
		t.Fatalf("portfolio summaries=%d, want five independent books", len(portfolios))
	}
	var kalshi *paperPortfolioSurface
	for i := range portfolios {
		if portfolios[i].ID == vbKalshi {
			kalshi = &portfolios[i]
			break
		}
	}
	if kalshi == nil || !kalshi.Available || kalshi.FundedOpenPositions == nil ||
		*kalshi.FundedOpenPositions != 1 || kalshi.ReservedOpenExposureUSD == nil ||
		math.Abs(*kalshi.ReservedOpenExposureUSD-(lot.Contracts*lot.Price+lot.Fee)) > 1e-12 {
		t.Fatalf("Kalshi portfolio summary did not include its funded Genfollow lot: %+v", kalshi)
	}
}

func TestR169PoisonedGenfollowSurfaceIsUnavailableNotZero(t *testing.T) {
	s := testServer(t)
	s.gfBook = &gfBookState{Subs: map[string]*kfBook{
		"gf:spotlag-k": {Open: []kfPos{{Ticker: "KX-POISON", Side: "YES", Price: .4, Contracts: 1}}},
	}}
	s.gfBookPoisoned.Store(true)
	if rows, available := s.currentGenfollowFundedPositions(); available || rows != nil {
		t.Fatalf("poisoned funded ledger rendered as healthy: available=%v rows=%+v", available, rows)
	}
	for _, row := range s.currentPaperPortfolioSurfaces(context.Background()) {
		if row.ID == vbKalshi {
			if row.Available || row.EquityUSD != nil || row.ReservedOpenExposureUSD != nil ||
				row.UnavailableReason == "" {
				t.Fatalf("poisoned Kalshi portfolio rendered fabricated money: %+v", row)
			}
			return
		}
	}
	t.Fatal("Kalshi portfolio row missing")
}

func TestR169DashboardRendersFundedPositionsAsReadOnly(t *testing.T) {
	for _, want := range []string{
		"d&&d.portfolios", "d&&d.funded_positions", "funded_positions_available",
		"Funded system positions", "committed portfolio lots, not shadow/model fills",
		"read-only here",
	} {
		if !strings.Contains(dashboardHTML, want) {
			t.Fatalf("dashboard missing truthful funded-Paper surface %q", want)
		}
	}
}
