package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func TestR159KalshiCachedExecutableBookUsesSequenceProvenWSWithoutREST(t *testing.T) {
	now := time.Now().UTC()
	candidate := liveMirrorCandidate{Ticker: "KXR159", Side: "YES"}
	book := &kalshi.Orderbook{Ticker: candidate.Ticker,
		YesBids: []kalshi.OrderbookLevel{{Price: .40, Size: 8}},
		YesAsks: []kalshi.OrderbookLevel{{Price: .42, Size: 7}}}
	provenance := kalshi.BookProvenance{Generation: 4, SubscriptionID: 12, Sequence: 99,
		SourceAt: now.Add(-20 * time.Millisecond), ReceivedAt: now.Add(-10 * time.Millisecond)}
	markets := []kalshi.Market{{Ticker: candidate.Ticker, Status: "active",
		ExpectedExpiration:  now.Add(time.Hour).Format(time.RFC3339),
		PriceLevelStructure: "linear_cent"}}

	got, why := r159KalshiCachedExecutableBook(
		candidate, book, provenance, markets, now.Add(-time.Minute), now)
	if why != "" || got.TakerPrice != .42 || got.TakerDepth != 7 ||
		got.MakerPrice != .40 || got.MakerDepth != 8 ||
		got.MakerTick <= 0 || got.TakerTick <= 0 ||
		got.ReceivedAt != provenance.ReceivedAt ||
		!strings.Contains(got.Source, "g4:s12:q99") {
		t.Fatalf("cached executable receipt=%+v reason=%q", got, why)
	}
}

func TestR159KalshiCachedExecutableBookClassifiesMissingTruth(t *testing.T) {
	now := time.Now().UTC()
	candidate := liveMirrorCandidate{Ticker: "KXR159", Side: "YES"}
	book := &kalshi.Orderbook{YesBids: []kalshi.OrderbookLevel{{Price: .40, Size: 8}},
		YesAsks: []kalshi.OrderbookLevel{{Price: .42, Size: 7}}}
	provenance := kalshi.BookProvenance{Generation: 1, SubscriptionID: 2, Sequence: 3,
		ReceivedAt: now}
	market := kalshi.Market{Ticker: candidate.Ticker, Status: "active",
		ExpectedExpiration:  now.Add(time.Hour).Format(time.RFC3339),
		PriceLevelStructure: "linear_cent"}

	if _, why := r159KalshiCachedExecutableBook(candidate, book, provenance,
		[]kalshi.Market{market}, now.Add(-r159KalshiMarketCacheMaxAge-time.Millisecond), now); why != "kalshi-market-metadata-cache-stale" {
		t.Fatalf("stale metadata reason=%q", why)
	}
	book.YesAsks = nil
	if _, why := r159KalshiCachedExecutableBook(candidate, book, provenance,
		[]kalshi.Market{market}, now, now); why != "kalshi-ws-book-one-sided-or-empty" {
		t.Fatalf("one-sided book reason=%q", why)
	}
}

func TestR159KalshiCachedExecutableBookFailsClosedForTerminalLifecycle(t *testing.T) {
	now := time.Now().UTC()
	candidate := liveMirrorCandidate{Ticker: "KXTERMINAL", Side: "YES"}
	book := &kalshi.Orderbook{
		YesBids: []kalshi.OrderbookLevel{{Price: .40, Size: 8}},
		YesAsks: []kalshi.OrderbookLevel{{Price: .42, Size: 7}},
	}
	provenance := kalshi.BookProvenance{
		Generation: 1, SubscriptionID: 2, Sequence: 3, ReceivedAt: now,
	}
	for _, status := range []string{"determined", "settled"} {
		market := kalshi.Market{
			Ticker: candidate.Ticker, Status: status,
			ExpectedExpiration:  now.Add(time.Hour).Format(time.RFC3339),
			PriceLevelStructure: "linear_cent",
		}
		if _, why := r159KalshiCachedExecutableBook(candidate, book, provenance,
			[]kalshi.Market{market}, now, now); why != "kalshi-cached-market-not-active" {
			t.Fatalf("terminal status %q remained executable: reason=%q", status, why)
		}
	}
}

func TestR159ProspectiveTakerRouteDoesNotCallPublicHorizonREST(t *testing.T) {
	var calls atomic.Int64
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected public REST call", http.StatusInternalServerError)
	}))
	defer venue.Close()

	s := testServer(t)
	s.kal = kalshi.NewClient(venue.URL, nil, 100, time.Second)
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = "kalshi|spotlag|YES|taker"
	s.cfgP.Store(&cfg)
	now := time.Now().UTC()
	s.kalshiExecutableBookFn = func(liveMirrorCandidate) (r159KalshiExecutableBook, string) {
		return r159KalshiExecutableBook{
			MakerPrice: .40, MakerDepth: 10, TakerPrice: .41, TakerDepth: 9,
			MakerTick: .01, TakerTick: .01, SourceAt: now, ReceivedAt: now,
			Source: "r159-test-current-ws",
		}, ""
	}
	candidate := liveMirrorCandidate{
		Platform: "kalshi", Ticker: "KXR159-NOREST", Side: "YES",
		Family: "spotlag", Source: "auto-cons-spotlag", Price: .40, At: now,
	}
	quote, why := s.liveMirrorExecutable(context.Background(), candidate)
	if why != "" || quote.Maker || !strings.Contains(quote.Route, "prospective-allocation-taker") {
		t.Fatalf("prospective quote=%+v reason=%q", quote, why)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("prospective fixed-taker route made %d public horizon REST calls", got)
	}
}
