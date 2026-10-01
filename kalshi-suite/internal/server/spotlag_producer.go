package server

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	// Spot-lag is a LIVE-selected short-horizon detector. It gets its own fast clock instead of
	// waiting behind the broad research discovery pass or the one-minute flow/whale producer.
	spotlagDetectorInterval = time.Second
	spotlagDetectorDeadline = 4 * time.Second
	spotlagBoardCacheMaxAge = 2 * time.Minute
	spotlagBoardFutureSkew  = 2 * time.Second
)

var spotlagCoins = []string{"btc", "eth", "sol", "xrp"}

const (
	spotlagReasonBoardMissing       = "complete-board-missing"
	spotlagReasonBoardClockInvalid  = "complete-board-clock-invalid"
	spotlagReasonBoardStale         = "complete-board-stale"
	spotlagReasonFeedsUnavailable   = "shared-live-feeds-unavailable"
	spotlagReasonUnderlyingMissing  = "coinbase-all-listed-sources-unavailable"
	spotlagReasonSignalInsertFailed = "signal-insert-failed"
	spotlagReasonProducerPanic      = "producer-panic"
)

type spotlagCachedMarket struct {
	Coin   string
	Ticker string
	Title  string
	Up     float64
	Expiry time.Time
}

type spotlagSpotRead func(context.Context, string) (price, move float64, observedAt time.Time, ok bool)
type spotlagSigmaRead func(string) (float64, bool)
type spotlagMomentumRead func(string) (float64, bool)
type spotlagInsert func(context.Context, storage.Signal) error
type spotlagLivePriceRead func(string) (float64, bool)

func spotlagCoinForTicker(ticker string) string {
	upper := strings.ToUpper(strings.TrimSpace(ticker))
	if !strings.Contains(upper, "15M") {
		return ""
	}
	for _, coin := range spotlagCoins {
		if strings.Contains(upper, strings.ToUpper(coin)) {
			return coin
		}
	}
	return ""
}

// spotlagMarketsFromCompleteBoard selects the soonest active future window per coin from one
// successfully published complete-board snapshot. Board/catalog freshness is proved only by the
// snapshot receipt. A per-market REST refresh timestamp is deliberately not consulted: it used to
// expire independently of the healthy board and Kalshi WS, intermittently pausing Spot-lag.
//
// A fresh board with no listed 15-minute window is a healthy zero-opportunity result. A missing,
// future-dated, or stale complete-board receipt fails closed and returns an exact durable reason.
func spotlagMarketsFromCompleteBoard(now, boardAt time.Time, markets []kalshi.Market,
	readLivePrice spotlagLivePriceRead) ([]spotlagCachedMarket, bool, string) {
	if boardAt.IsZero() || len(markets) == 0 {
		return nil, false, spotlagReasonBoardMissing
	}
	boardAge := now.Sub(boardAt)
	if boardAge < -spotlagBoardFutureSkew {
		return nil, false, spotlagReasonBoardClockInvalid
	}
	if boardAge > spotlagBoardCacheMaxAge {
		return nil, false, spotlagReasonBoardStale
	}

	best := make(map[string]spotlagCachedMarket, len(spotlagCoins))
	for _, market := range markets {
		ticker := strings.TrimSpace(market.Ticker)
		if ticker == "" {
			continue
		}
		if market.Result != "" ||
			(market.Status != "" && !strings.EqualFold(market.Status, "active")) {
			continue
		}
		coin := spotlagCoinForTicker(ticker)
		if coin == "" {
			continue
		}
		expiry, parsed := parseTime(strings.TrimSpace(market.ExpectedExpiration))
		if !parsed {
			expiry, parsed = parseTime(strings.TrimSpace(market.CloseTime))
		}
		if !parsed || !expiry.After(now) {
			continue
		}
		up := market.ImpliedProbability()
		if readLivePrice != nil {
			if live, ok := readLivePrice(ticker); ok {
				up = live
			}
		}
		if up <= 0.02 || up >= 0.98 {
			continue
		}
		row := spotlagCachedMarket{
			Coin: coin, Ticker: ticker, Title: market.Title, Up: up, Expiry: expiry,
		}
		if prior, exists := best[coin]; !exists || expiry.Before(prior.Expiry) {
			best[coin] = row
		}
	}

	rows := make([]spotlagCachedMarket, 0, len(best))
	for _, coin := range spotlagCoins {
		row, exists := best[coin]
		if !exists {
			continue
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Coin < rows[j].Coin })
	return rows, true, ""
}

// spotlagCachedKalshiMarkets is a nonblocking cache read. The persistent Kalshi board refresher
// owns venue I/O; this one-second money path only consumes its last successful complete snapshot
// and the independent live ticker-WS price cache. Final LIVE admission still requires the current
// full executable order book, lifecycle, fee, horizon, and account checks.
func (s *Server) spotlagCachedKalshiMarkets(now time.Time) ([]spotlagCachedMarket, bool, string) {
	if s == nil || s.kal == nil {
		return nil, false, spotlagReasonBoardMissing
	}
	board, boardAt := s.kal.CompleteBoardSnapshot()
	return spotlagMarketsFromCompleteBoard(now, boardAt, board, s.kal.LivePrice)
}

// runSpotlagDetectorPass fetches the independent underlying observations in parallel, then
// evaluates every current Kalshi window. A threshold miss, missing sigma history, or a quiet
// ticker is a healthy zero-opportunity result. A failed underlying read or durable insert is a
// real producer error and therefore makes the heartbeat fail closed.
func runSpotlagDetectorPass(ctx context.Context, markets []spotlagCachedMarket,
	readSpot spotlagSpotRead, readSigma spotlagSigmaRead, readMomentum spotlagMomentumRead,
	insert spotlagInsert) (attempts, errorsN int) {
	attempts, errorsN, _ = runSpotlagDetectorPassDetailed(ctx, markets,
		readSpot, readSigma, readMomentum, insert)
	return attempts, errorsN
}

func runSpotlagDetectorPassDetailed(ctx context.Context, markets []spotlagCachedMarket,
	readSpot spotlagSpotRead, readSigma spotlagSigmaRead, readMomentum spotlagMomentumRead,
	insert spotlagInsert) (attempts, errorsN int, reason string) {
	if len(markets) == 0 {
		return 0, 0, ""
	}
	type observation struct {
		market     spotlagCachedMarket
		price      float64
		move       float64
		observedAt time.Time
		ok         bool
	}
	observations := make(chan observation, len(markets))
	var wg sync.WaitGroup
	for _, market := range markets {
		market := market
		wg.Add(1)
		go func() {
			defer wg.Done()
			if readSpot == nil {
				observations <- observation{market: market}
				return
			}
			price, move, observedAt, ok := readSpot(ctx, market.Coin)
			observations <- observation{
				market: market, price: price, move: move, observedAt: observedAt, ok: ok,
			}
		}()
	}
	wg.Wait()
	close(observations)

	validSources := 0
	for observed := range observations {
		if !observed.ok || observed.price <= 0 || observed.observedAt.IsZero() {
			continue
		}
		validSources++
		sigma, sigmaOK := 0.0, false
		if readSigma != nil {
			sigma, sigmaOK = readSigma(observed.market.Coin)
		}
		momentum, momentumOK := 0.0, false
		if readMomentum != nil {
			momentum, momentumOK = readMomentum(observed.market.Ticker)
		}
		signal, emit := spotlagSignalFromInputs(
			observed.market.Ticker, observed.market.Title, observed.market.Up,
			observed.price, observed.move, momentum, sigma, sigmaOK && momentumOK,
			observed.observedAt,
		)
		if !emit {
			continue
		}
		attempts++
		if insert == nil || insert(ctx, signal) != nil {
			errorsN++
		}
	}
	// A quiet ticker can legitimately go five seconds without a trade. That coin is individually
	// ineligible because it cannot emit a signal, but it must not pause fresh BTC/ETH/etc. inputs.
	// Fail the exact producer only when every currently listed Spot-lag market lacks a current
	// underlying source; then there is no money-authorizing input left at all.
	if validSources == 0 {
		errorsN++
		return attempts, errorsN, spotlagReasonUnderlyingMissing
	}
	if errorsN > 0 {
		return attempts, errorsN, spotlagReasonSignalInsertFailed
	}
	return attempts, errorsN, ""
}

func (s *Server) spotlagDetectorTick(ctx context.Context) {
	attempts, errorsN, reason := 0, 0, ""
	defer func() {
		if recovered := recover(); recovered != nil {
			errorsN++
			reason = spotlagReasonProducerPanic
			s.finishSignalProducerWithReason("spotlag", time.Now(), attempts, errorsN, reason)
			panic(recovered)
		}
		s.finishSignalProducerWithReason("spotlag", time.Now(), attempts, errorsN, reason)
	}()
	if s == nil || s.kal == nil || !s.feedsReady() {
		errorsN = 1
		reason = spotlagReasonFeedsUnavailable
		return
	}
	markets, sourceOK, catalogReason := s.spotlagCachedKalshiMarkets(time.Now())
	if !sourceOK {
		errorsN = 1
		reason = catalogReason
		return
	}
	attempts, errorsN, reason = runSpotlagDetectorPassDetailed(ctx, markets,
		func(_ context.Context, coin string) (float64, float64, time.Time, bool) {
			return coinbaseSpotForSpotlag(coin, time.Now())
		},
		coinbaseSpotSigmaForSpotlag,
		s.kal.Momentum,
		func(ctx context.Context, signal storage.Signal) error {
			signal.Confidence = s.signalConfidence("spotlag", signal.EntryPrice)
			return s.insertSignal(ctx, signal)
		},
	)
}
