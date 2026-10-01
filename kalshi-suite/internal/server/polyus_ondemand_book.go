package server

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	polyUSOnDemandBookTTL       = 5 * time.Second
	polyUSOnDemandRetryCooldown = 5 * time.Second
	polyUSOnDemandFetchTimeout  = 2500 * time.Millisecond
	polyUSOnDemandWaitTimeout   = 3 * time.Second
	polyUSOnDemandPerMinute     = 24
	polyUSOnDemandConcurrency   = 2
)

type polyUSOnDemandBookReceipt struct {
	book polymarketus.BookData
	at   time.Time
}

type polyUSOnDemandBookState struct {
	sync.Mutex
	rows        map[string]polyUSOnDemandBookReceipt
	inflight    map[string]chan struct{}
	lastAttempt map[string]time.Time
	window      time.Time
	used        int
	slots       chan struct{}
}

func (s *Server) polyUSOnDemandState() *polyUSOnDemandBookState {
	s.polyUSOnDemandMu.Lock()
	defer s.polyUSOnDemandMu.Unlock()
	if s.polyUSOnDemand == nil {
		s.polyUSOnDemand = &polyUSOnDemandBookState{rows: map[string]polyUSOnDemandBookReceipt{},
			inflight: map[string]chan struct{}{}, lastAttempt: map[string]time.Time{},
			slots: make(chan struct{}, polyUSOnDemandConcurrency)}
	}
	return s.polyUSOnDemand
}

func (st *polyUSOnDemandBookState) cached(slug string, now time.Time) (polyUSOnDemandBookReceipt, bool) {
	st.Lock()
	defer st.Unlock()
	receipt, ok := st.rows[slug]
	return receipt, ok && now.Sub(receipt.at) >= 0 && now.Sub(receipt.at) <= polyUSOnDemandBookTTL
}

func (st *polyUSOnDemandBookState) pruneLocked(now time.Time) {
	if len(st.rows) > 256 {
		for key, row := range st.rows {
			if now.Sub(row.at) > time.Minute {
				delete(st.rows, key)
			}
		}
	}
	// Failed/one-off slugs live only in lastAttempt, so pruning rows alone would leak up to 24
	// new strings per minute forever. Keep the retry map bounded independently of successes.
	if len(st.lastAttempt) > 256 {
		for key, attempted := range st.lastAttempt {
			if now.Sub(attempted) > time.Minute {
				delete(st.lastAttempt, key)
			}
		}
	}
}

// polyUSOnDemandFullBook is the bounded overflow path for a genuine current system signal whose
// market fell outside the fixed 1,000 full-depth WS prefix. LITE+TRADE remain prioritized. This path
// fetches one authoritative OPEN full book, shares concurrent requests for the same slug, caps
// both concurrency and calls/minute, and never expands long-lived subscriptions into a flood.
func (s *Server) polyUSOnDemandFullBook(slug string) (polyUSOnDemandBookReceipt, bool) {
	slug = strings.ToLower(strings.TrimSpace(slug))
	if s == nil || s.polyUS == nil || slug == "" {
		return polyUSOnDemandBookReceipt{}, false
	}
	now := time.Now()
	state := s.polyUSOnDemandState()
	if receipt, ok := state.cached(slug, now); ok {
		return receipt, true
	}

	state.Lock()
	if wait, ok := state.inflight[slug]; ok {
		state.Unlock()
		timer := time.NewTimer(polyUSOnDemandWaitTimeout)
		defer timer.Stop()
		select {
		case <-wait:
			return state.cached(slug, time.Now())
		case <-timer.C:
			return polyUSOnDemandBookReceipt{}, false
		}
	}
	if last := state.lastAttempt[slug]; !last.IsZero() && now.Sub(last) < polyUSOnDemandRetryCooldown {
		state.Unlock()
		return polyUSOnDemandBookReceipt{}, false
	}
	if state.window.IsZero() || now.Sub(state.window) >= time.Minute {
		state.window, state.used = now, 0
	}
	if state.used >= polyUSOnDemandPerMinute {
		state.Unlock()
		return polyUSOnDemandBookReceipt{}, false
	}
	wait := make(chan struct{})
	state.inflight[slug], state.lastAttempt[slug] = wait, now
	state.used++
	state.Unlock()

	finish := func(receipt polyUSOnDemandBookReceipt, ok bool) {
		state.Lock()
		if ok {
			state.rows[slug] = receipt
		}
		delete(state.inflight, slug)
		close(wait)
		// Long-lived market churn must not grow either the success cache or failure cooldown map.
		state.pruneLocked(now)
		state.Unlock()
	}

	acquire := time.NewTimer(time.Second)
	defer acquire.Stop()
	select {
	case state.slots <- struct{}{}:
	case <-acquire.C:
		finish(polyUSOnDemandBookReceipt{}, false)
		return polyUSOnDemandBookReceipt{}, false
	}
	defer func() { <-state.slots }()

	ctx, cancel := context.WithTimeout(context.Background(), polyUSOnDemandFetchTimeout)
	defer cancel()
	book, _, code, err := s.polyUS.BookFull(ctx, slug)
	if err != nil || code != http.StatusOK || book == nil || !polyUSLifecycleOpen(book.State) ||
		book.BestBid <= 0 || book.BestAsk <= book.BestBid || book.BestAsk >= 1 ||
		book.BestBidQty < 1 || book.BestAskQty < 1 {
		finish(polyUSOnDemandBookReceipt{}, false)
		return polyUSOnDemandBookReceipt{}, false
	}
	receipt := polyUSOnDemandBookReceipt{book: *book, at: time.Now()}
	finish(receipt, true)
	return receipt, true
}

func (s *Server) stampPolyUSOnDemandBookSnapshot(sig *storage.Signal) bool {
	if sig == nil || !strings.EqualFold(sig.Platform, "polyus") || sig.Ticker == "" {
		return false
	}
	receipt, ok := s.polyUSOnDemandFullBook(sig.Ticker)
	if !ok {
		return false
	}
	touch, ok := mlNormalizeBookSide(sig.Side, receipt.book.BestBid, receipt.book.BestAsk,
		receipt.book.BestBidQty, receipt.book.BestAskQty)
	if !ok {
		return false
	}
	market, ok := s.mlPUSBookMeta(sig.Ticker)
	if !ok || market.FeeCoeff == nil || !validPolyUSFeeTheta(*market.FeeCoeff) || market.TickSize <= 0 {
		return false
	}
	makerFee := polyUSFeeWithTheta(true, 1, touch.MakerPrice, *market.FeeCoeff)
	takerFee := polyUSFeeWithTheta(false, 1, touch.TakerPrice, *market.FeeCoeff)
	quoteAge := time.Since(receipt.at).Seconds()
	if quoteAge < 0 || quoteAge > polyUSOnDemandBookTTL.Seconds() || !finiteMLBook(makerFee) || !finiteMLBook(takerFee) {
		return false
	}
	makerPrice, takerPrice := touch.MakerPrice, touch.TakerPrice
	makerDepth, takerDepth := touch.MakerDepth, touch.TakerDepth
	tick := market.TickSize
	sig.BookBid, sig.BookAsk = &makerPrice, &takerPrice
	sig.BookBidDepth, sig.BookAskDepth = &makerDepth, &takerDepth
	sig.BookQuoteAgeS, sig.BookMakerTick, sig.BookTakerTick = &quoteAge, &tick, &tick
	sig.BookMakerFeePC, sig.BookTakerFeePC = &makerFee, &takerFee
	sig.BookSource = "polyus-rest-full-on-demand"
	sig.PricingVersion = mlBookFeatureSchema
	sig.BookFeatureVer = mlBookFeatureVersion
	return true
}
