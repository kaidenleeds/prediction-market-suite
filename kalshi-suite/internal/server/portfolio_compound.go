package server

import (
	"context"
	"math"
	"time"
)

// portfolioEquityTTL is deliberately short: a settlement changes the next stake quickly, while
// dozens of simultaneous signal candidates share one bounded ledger read instead of each scanning
// every funded sub-ledger. Tests shorten it when exercising refresh/failure behavior.
var portfolioEquityTTL = 3 * time.Second

type portfolioEquitySnapshot struct {
	Equity float64
	NetUSD float64
	Grant  float64
	Epoch  time.Time
	At     time.Time
}

func compoundedPortfolioEquity(grant, feeNet float64) float64 {
	if grant <= 0 || math.IsNaN(grant) || math.IsInf(grant, 0) ||
		math.IsNaN(feeNet) || math.IsInf(feeNet, 0) {
		return 0
	}
	return math.Max(0, grant+feeNet)
}

func (s *Server) portfolioSizingEpoch() time.Time {
	s.pnlMu.Lock()
	epoch := s.pnlEpoch
	s.pnlMu.Unlock()
	if epoch.IsZero() {
		epoch = s.bootAt
	}
	return epoch
}

func (s *Server) readPortfolioSessionMetrics(ctx context.Context, book string) (bookSessionMetrics, bool) {
	if s.portfolioEquityReadForTest != nil {
		return s.portfolioEquityReadForTest(ctx, book)
	}
	return s.bookSessionClosedMetrics(ctx, book)
}

// bookCurrentEquityUSD is the only Paper sizing equity authority for the four funded portfolios.
// Equity = configured/reset grant + this portfolio's own current-epoch realized fee-net P&L,
// floored only at zero. It intentionally has no drawdown floor and never borrows another book's
// result. ML's Python/Go venue-sleeve router further partitions this total into its two independently
// compounding destination sleeves.
func (s *Server) bookCurrentEquityUSD(ctx context.Context, book string) (float64, bool) {
	grant := s.bookBankUSD(book)
	if grant <= 0 {
		return 0, false
	}
	epoch := s.portfolioSizingEpoch()
	now := time.Now()
	s.portfolioEquityMu.Lock()
	if snap, ok := s.portfolioEquityCache[book]; ok && snap.Grant == grant && snap.Epoch.Equal(epoch) &&
		now.Sub(snap.At) >= 0 && now.Sub(snap.At) < portfolioEquityTTL {
		s.portfolioEquityMu.Unlock()
		return snap.Equity, true
	}
	s.portfolioEquityMu.Unlock()

	// Only one goroutine may do the cross-ledger refresh. Recheck after acquiring the load lock so
	// a burst of candidates consumes one refresh, not one refresh per candidate.
	s.portfolioEquityLoadMu.Lock()
	defer s.portfolioEquityLoadMu.Unlock()
	now = time.Now()
	s.portfolioEquityMu.Lock()
	if snap, ok := s.portfolioEquityCache[book]; ok && snap.Grant == grant && snap.Epoch.Equal(epoch) &&
		now.Sub(snap.At) >= 0 && now.Sub(snap.At) < portfolioEquityTTL {
		s.portfolioEquityMu.Unlock()
		return snap.Equity, true
	}
	s.portfolioEquityMu.Unlock()

	// Respect a tighter caller deadline, otherwise bound a cold refresh. An expired cache plus a
	// failed read is a hard zero-authority result: stale profit is never reused to place money.
	if ctx == nil {
		ctx = context.Background()
	}
	rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	m, ok := s.readPortfolioSessionMetrics(rctx, book)
	if !ok || math.IsNaN(m.NetUSD) || math.IsInf(m.NetUSD, 0) {
		return 0, false
	}
	equity := compoundedPortfolioEquity(grant, m.NetUSD)
	snap := portfolioEquitySnapshot{Equity: equity, NetUSD: m.NetUSD, Grant: grant, Epoch: epoch, At: time.Now()}
	s.portfolioEquityMu.Lock()
	if s.portfolioEquityCache == nil {
		s.portfolioEquityCache = make(map[string]portfolioEquitySnapshot, 4)
	}
	s.portfolioEquityCache[book] = snap
	s.portfolioEquityMu.Unlock()
	return equity, true
}

func (s *Server) bookSizingEquityUSD(ctx context.Context, book string) float64 {
	equity, ok := s.bookCurrentEquityUSD(ctx, book)
	if !ok {
		return 0
	}
	return equity
}

func (s *Server) invalidatePortfolioEquityCache() {
	s.portfolioEquityMu.Lock()
	s.portfolioEquityCache = nil
	s.portfolioEquityMu.Unlock()
}
