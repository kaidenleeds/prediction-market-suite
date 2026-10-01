package server

import (
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

// polyUSFreshSweepMarket returns immutable reference data only from the last complete successful
// open-universe crawl. It performs no network I/O. Production crawls build pusSweepBySlug once, so
// the order path is O(1); the linear fallback supports focused tests and old in-memory fixtures.
func (s *Server) polyUSFreshSweepMarket(slug string) (polymarketus.Market, time.Time, bool) {
	if s == nil {
		return polymarketus.Market{}, time.Time{}, false
	}
	slug = strings.ToLower(strings.TrimSpace(slug))
	if slug == "" {
		return polymarketus.Market{}, time.Time{}, false
	}
	now := time.Now()
	s.polyUSMu.Lock()
	defer s.polyUSMu.Unlock()
	age := now.Sub(s.pusSweepAt)
	if s.pusSweepAt.IsZero() || age < 0 || age > polymarketus.MarketsRESTLifecycleMaxAge {
		return polymarketus.Market{}, time.Time{}, false
	}
	m, ok := s.pusSweepBySlug[slug]
	if !ok {
		for i := range s.pusSweep {
			if strings.EqualFold(strings.TrimSpace(s.pusSweep[i].Slug), slug) {
				m, ok = s.pusSweep[i], true
				break
			}
		}
	}
	if !ok {
		return polymarketus.Market{}, time.Time{}, false
	}
	return m, s.pusSweepAt, true
}

// polyUSLiveMarketRules is the zero-network metadata authority for a real-money request. Lifecycle
// is independently re-proved from current-generation MARKET_DATA; the complete crawl supplies only
// immutable tick, minimum-quantity, fee, title, and identity fields.
func (s *Server) polyUSLiveMarketRules(slug string) (polymarketus.Market, time.Time, bool) {
	m, at, ok := s.polyUSFreshSweepMarket(slug)
	if !ok || !m.LifecycleKnown() || !m.Open() ||
		m.TickSize <= 0 || m.TickSize >= 1 || math.IsNaN(m.TickSize) ||
		math.IsInf(m.TickSize, 0) || m.MinimumQty <= 0 || math.IsNaN(m.MinimumQty) ||
		math.IsInf(m.MinimumQty, 0) || m.FeeCoeff == nil ||
		!validPolyUSFeeTheta(*m.FeeCoeff) {
		return polymarketus.Market{}, time.Time{}, false
	}
	return m, at, true
}

// polyUSExecutableDepthAtLimit integrates only prices the submitted BUY limit can actually cross.
// PolyUS full books are YES-denominated; a NO buy consumes YES bids at 1-price.
func polyUSExecutableDepthAtLimit(bids, asks []polymarketus.BookLevel, side string,
	limit float64) (touch, depth float64, ok bool) {
	if limit <= 0 || limit >= 1 || math.IsNaN(limit) || math.IsInf(limit, 0) {
		return 0, 0, false
	}
	switch strings.ToUpper(strings.TrimSpace(side)) {
	case "YES":
		if len(asks) == 0 {
			return 0, 0, false
		}
		touch = asks[0].Price
		for _, level := range asks {
			if level.Price > limit+1e-12 {
				break
			}
			if level.Quantity > 0 {
				depth += level.Quantity
			}
		}
	case "NO":
		if len(bids) == 0 {
			return 0, 0, false
		}
		touch = 1 - bids[0].Price
		for _, level := range bids {
			noPrice := 1 - level.Price
			if noPrice > limit+1e-12 {
				break
			}
			if level.Quantity > 0 {
				depth += level.Quantity
			}
		}
	default:
		return 0, 0, false
	}
	return touch, depth, touch > 0 && touch < 1 && depth > 0
}
