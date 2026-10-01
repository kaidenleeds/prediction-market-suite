package server

import (
	"fmt"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

const (
	// Book freshness is transport freshness, not time since the last price/depth change. A quiet
	// book remains current while the owning socket generation is healthy.
	r159KalshiMoneyTransportMaxAge = 3 * time.Second
	r159KalshiMarketCacheMaxAge    = 5 * time.Minute
)

type r159KalshiExecutableBook struct {
	MakerPrice, MakerDepth float64
	TakerPrice, TakerDepth float64
	MakerTick, TakerTick   float64
	SourceAt, ReceivedAt   time.Time
	Source                 string
}

func r159KalshiCachedExecutableBook(candidate liveMirrorCandidate, book *kalshi.Orderbook,
	provenance kalshi.BookProvenance, markets []kalshi.Market, marketCacheAt, now time.Time) (
	r159KalshiExecutableBook, string) {
	if book == nil {
		return r159KalshiExecutableBook{}, "kalshi-ws-full-book-unavailable"
	}
	makerPrice, makerDepth, takerPrice, takerDepth, ok :=
		kalshiFullBookSides(book, candidate.Side)
	if !ok {
		return r159KalshiExecutableBook{}, "kalshi-ws-book-one-sided-or-empty"
	}
	if provenance.Generation == 0 || provenance.SubscriptionID <= 0 ||
		provenance.Sequence <= 0 || provenance.ReceivedAt.IsZero() {
		return r159KalshiExecutableBook{}, "kalshi-ws-book-provenance-incomplete"
	}
	cacheAge := now.Sub(marketCacheAt)
	if marketCacheAt.IsZero() || cacheAge < 0 || cacheAge > r159KalshiMarketCacheMaxAge {
		return r159KalshiExecutableBook{}, "kalshi-market-metadata-cache-stale"
	}
	var market kalshi.Market
	found := false
	for i := range markets {
		if strings.EqualFold(strings.TrimSpace(markets[i].Ticker),
			strings.TrimSpace(candidate.Ticker)) {
			market, found = markets[i], true
			break
		}
	}
	if !found {
		return r159KalshiExecutableBook{}, "kalshi-market-metadata-cache-missing-ticker"
	}
	if market.Result != "" || (!strings.EqualFold(market.Status, "active") &&
		!strings.EqualFold(market.Status, "open")) {
		return r159KalshiExecutableBook{}, "kalshi-cached-market-not-active"
	}
	closeRaw := strings.TrimSpace(market.ExpectedExpiration)
	if closeRaw == "" {
		closeRaw = strings.TrimSpace(market.CloseTime)
	}
	if closeRaw != "" {
		closeAt, err := time.Parse(time.RFC3339, closeRaw)
		if err != nil || !closeAt.After(now) {
			return r159KalshiExecutableBook{}, "kalshi-cached-market-close-time-invalid-or-past"
		}
	}
	makerTick, makerKnown := market.TickForKnown(makerPrice)
	takerTick, takerKnown := market.TickForKnown(takerPrice)
	if !makerKnown || !takerKnown || makerTick <= 0 || takerTick <= 0 {
		return r159KalshiExecutableBook{}, "kalshi-cached-market-tick-unavailable"
	}
	return r159KalshiExecutableBook{
		MakerPrice: makerPrice, MakerDepth: makerDepth,
		TakerPrice: takerPrice, TakerDepth: takerDepth,
		MakerTick: makerTick, TakerTick: takerTick,
		SourceAt: provenance.SourceAt, ReceivedAt: provenance.ReceivedAt,
		Source: fmt.Sprintf("kalshi_ws_full_orderbook:g%d:s%d:q%d",
			provenance.Generation, provenance.SubscriptionID, provenance.Sequence),
	}, ""
}

// r159KalshiWSExecutableBook is the zero-REST money-book boundary. It atomically reads the current
// sequence-stamped ladder, then pairs it with the complete-board cache that lifecycle/tick
// WebSocket events patch in place. The caller may briefly wait for an already-requested WebSocket
// snapshot when a promoted market is cold; a healthy resident receipt pays no wait or REST.
func (s *Server) r159KalshiWSExecutableBook(candidate liveMirrorCandidate) (
	r159KalshiExecutableBook, string) {
	if s == nil || s.kal == nil {
		return r159KalshiExecutableBook{}, "kalshi-client-unavailable"
	}
	book, _, provenance, ok := s.kal.LiveBookWithProvenance(
		candidate.Ticker, r159KalshiMoneyTransportMaxAge)
	if !ok {
		return r159KalshiExecutableBook{}, "kalshi-ws-current-generation-full-book-unavailable"
	}
	markets, cacheAt := s.kal.CompleteBoardSnapshot()
	return r159KalshiCachedExecutableBook(candidate, book, provenance, markets, cacheAt, time.Now())
}
