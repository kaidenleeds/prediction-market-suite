package server

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func TestR154KalshiProductionNAVAddsCashAndPortfolioValue(t *testing.T) {
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/portfolio/balance" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"balance":40331,"portfolio_value":584}`))
	}))
	defer venue.Close()

	s := testServer(t)
	s.kal = kalshi.NewClient(venue.URL, queueTestSigner(t), 10_000, time.Second)
	nav, ok := s.liveVenueNAV(context.Background(), "kalshi", 123.45)
	if !ok || math.Abs(nav-409.15) > 1e-9 {
		t.Fatalf("Kalshi raw NAV = %.2f ok=%v, want $403.31 cash + $5.84 portfolio value = $409.15", nav, ok)
	}
	s.balMu.Lock()
	cash := s.balUSD
	s.balMu.Unlock()
	if math.Abs(cash-403.31) > 1e-9 {
		t.Fatalf("cash display cache = %.2f, want $403.31 from the same account receipt", cash)
	}
}

func TestR154KalshiProductionNAVFailsClosedWithoutPortfolioValue(t *testing.T) {
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/portfolio/balance" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"balance":39631}`))
	}))
	defer venue.Close()

	s := testServer(t)
	s.kal = kalshi.NewClient(venue.URL, queueTestSigner(t), 10_000, time.Second)
	if nav, ok := s.liveVenueNAV(context.Background(), "kalshi", 8); ok || nav != 0 {
		t.Fatalf("missing portfolio_value produced NAV %.2f ok=%v; production must fail closed", nav, ok)
	}
}

func TestR154LiveSizingCapsRawNAVAndNAVSeamFailsClosed(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) {
		c.Risk.KalshiLiveBankroll = 100
	})
	s.liveBankCashRead = func(context.Context, string) (float64, bool) {
		return 999, true
	}
	s.liveBankNAVRead = func(context.Context, string) (float64, bool) {
		return 125, true
	}

	raw, ok := s.liveVenueNAV(context.Background(), "kalshi", 50)
	if !ok || raw != 125 {
		t.Fatalf("raw injected NAV = %.2f ok=%v, want $125 independent of cash/exposure reconstruction", raw, ok)
	}
	if bank, src := s.liveVenueBankroll(context.Background(), "kalshi"); bank != 100 || src != "config-ceiling" {
		t.Fatalf("sizing bankroll = %.2f (%s), want $100 config ceiling over raw $125 NAV", bank, src)
	}

	s.liveBankNAVRead = func(context.Context, string) (float64, bool) {
		return 0, false
	}
	if bank, src := s.liveVenueBankroll(context.Background(), "kalshi"); bank != 0 || src != "unknown" {
		t.Fatalf("failed authoritative NAV seam fell back to cash: %.2f (%s)", bank, src)
	}
}

func TestR154LegacyCashSeamKeepsHermeticCashPlusExposureFallback(t *testing.T) {
	s := testServer(t)
	s.liveBankCashRead = func(context.Context, string) (float64, bool) {
		return 70, true
	}
	nav, ok := s.liveVenueNAV(context.Background(), "kalshi", 5)
	if !ok || nav != 75 {
		t.Fatalf("hermetic cash seam NAV = %.2f ok=%v, want $70 cash + $5 exposure", nav, ok)
	}
}
