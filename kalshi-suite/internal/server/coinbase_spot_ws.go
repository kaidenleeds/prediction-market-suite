package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	coinbaseSpotWSURL            = "wss://ws-feed.exchange.coinbase.com"
	coinbaseSpotWSMaxAge         = 5 * time.Second
	coinbaseSpotWSMaxSourceLag   = 5 * time.Second
	coinbaseSpotWSMaxEndToEndAge = coinbaseSpotWSMaxSourceLag + coinbaseSpotWSMaxAge
	coinbaseSpotWSFutureSkew     = 2 * time.Second
	coinbaseSpotSampleCadence    = 20 * time.Second
)

var coinbaseSpotProducts = []string{"BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD"}

// coinbaseSpotLiveCache is deliberately separate from spotCache. The latter is shared with the
// slower REST research reader, which is allowed to replace its own latest observation. LIVE
// Spot-lag provenance must never disappear merely because that unrelated reader refreshed.
// Both maps use spotMu so a detector pass sees one coherent source/price/sample snapshot.
var coinbaseSpotLiveCache = map[string]spotEntry{}

type coinbaseSpotWSMessage struct {
	Type      string `json:"type"`
	ProductID string `json:"product_id"`
	Price     string `json:"price"`
	Time      string `json:"time"`
	TradeID   int64  `json:"trade_id"`
	Message   string `json:"message"`
}

func coinbaseSpotCoin(productID string) string {
	productID = strings.ToUpper(strings.TrimSpace(productID))
	for _, product := range coinbaseSpotProducts {
		if productID == product {
			return strings.ToLower(strings.TrimSuffix(product, "-USD"))
		}
	}
	return ""
}

// ingestCoinbaseSpotTick keeps the newest exchange-time price but only appends one history sample
// every 20 seconds. That preserves the established five-minute move/sigma definition instead of
// quietly turning every trade print into a new volatility sample.
func ingestCoinbaseSpotTick(productID string, price float64, sourceAt, receivedAt time.Time) bool {
	return ingestCoinbaseSpotTrade(productID, price, 0, sourceAt, receivedAt)
}

func ingestCoinbaseSpotTrade(productID string, price float64, tradeID int64,
	sourceAt, receivedAt time.Time) bool {
	coin := coinbaseSpotCoin(productID)
	if coin == "" || price <= 0 || sourceAt.IsZero() || receivedAt.IsZero() {
		return false
	}
	lag := receivedAt.Sub(sourceAt)
	if lag < -coinbaseSpotWSFutureSkew || lag > coinbaseSpotWSMaxSourceLag {
		return false
	}
	sym := strings.ToUpper(coin)
	spotMu.Lock()
	defer spotMu.Unlock()
	liveEntry := coinbaseSpotLiveCache[sym]
	if !liveEntry.at.IsZero() {
		if sourceAt.Before(liveEntry.at) {
			return false
		}
		// Coinbase can publish distinct trades with the same timestamp. Trade ID preserves their
		// actual order without letting an exact duplicate/replay refresh LIVE price age.
		if sourceAt.Equal(liveEntry.at) &&
			(tradeID <= 0 || tradeID <= liveEntry.tradeID) {
			return false
		}
	}
	if len(liveEntry.samples) == 0 ||
		sourceAt.Sub(liveEntry.samples[len(liveEntry.samples)-1].at) >= coinbaseSpotSampleCadence {
		liveEntry.samples = append(liveEntry.samples, spotSample{p: price, at: sourceAt})
	}
	cut := sourceAt.Add(-20 * time.Minute)
	for len(liveEntry.samples) > 0 && liveEntry.samples[0].at.Before(cut) {
		liveEntry.samples = liveEntry.samples[1:]
	}
	liveEntry.price, liveEntry.at, liveEntry.receivedAt, liveEntry.tradeID, liveEntry.source =
		price, sourceAt, receivedAt, tradeID, "coinbase-ws"
	coinbaseSpotLiveCache[sym] = liveEntry

	// Keep the established general-purpose cache warm too, but never let its REST provenance or
	// sampling ring become the LIVE-authorizing record above.
	entry := spotCache[sym]
	if entry.at.IsZero() || sourceAt.After(entry.at) ||
		(sourceAt.Equal(entry.at) && tradeID > 0 && tradeID > entry.tradeID) {
		if len(entry.samples) == 0 ||
			sourceAt.Sub(entry.samples[len(entry.samples)-1].at) >= coinbaseSpotSampleCadence {
			entry.samples = append(entry.samples, spotSample{p: price, at: sourceAt})
		}
		for len(entry.samples) > 0 && entry.samples[0].at.Before(cut) {
			entry.samples = entry.samples[1:]
		}
		entry.price, entry.at, entry.receivedAt, entry.tradeID, entry.source =
			price, sourceAt, receivedAt, tradeID, "coinbase-ws"
		spotCache[sym] = entry
	}
	return true
}

// coinbaseSpotForSpotlag is the money-critical Spot-lag input. It never relabels the slower REST
// cache as real time: only a recent exchange-timestamped ticker frame is accepted.
func coinbaseSpotForSpotlag(coin string, now time.Time) (price, move float64, observedAt time.Time, ok bool) {
	sym := strings.ToUpper(strings.TrimSpace(coin))
	spotMu.Lock()
	defer spotMu.Unlock()
	entry, exists := coinbaseSpotLiveCache[sym]
	// Freshness uses the local receive clock. Coinbase exchange time remains the signal's
	// provenance, but comparing it directly with a host clock that is a couple seconds ahead
	// would silently shrink this five-second safety window.
	age := now.Sub(entry.receivedAt)
	sourceAge := now.Sub(entry.at)
	if !exists || entry.source != "coinbase-ws" || entry.price <= 0 ||
		entry.at.IsZero() || entry.receivedAt.IsZero() ||
		age < -coinbaseSpotWSFutureSkew || age > coinbaseSpotWSMaxAge ||
		sourceAge < -coinbaseSpotWSFutureSkew ||
		sourceAge > coinbaseSpotWSMaxEndToEndAge {
		return 0, 0, time.Time{}, false
	}
	return entry.price, spotMoveWindow(entry.samples, entry.price, 5*time.Minute), entry.at, true
}

func coinbaseSpotSigmaForSpotlag(coin string) (float64, bool) {
	sym := strings.ToUpper(strings.TrimSpace(coin))
	spotMu.Lock()
	defer spotMu.Unlock()
	entry, exists := coinbaseSpotLiveCache[sym]
	if !exists || entry.source != "coinbase-ws" {
		return 0, false
	}
	return spotSigma(entry.samples, 5*time.Minute)
}

func (s *Server) streamCoinbaseSpot(ctx context.Context) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		Proxy:            http.ProxyFromEnvironment,
	}
	conn, _, err := dialer.DialContext(ctx, coinbaseSpotWSURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	subscribe := map[string]any{
		"type":        "subscribe",
		"product_ids": coinbaseSpotProducts,
		"channels":    []string{"ticker", "heartbeat"},
	}
	if err := conn.WriteJSON(subscribe); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		var message coinbaseSpotWSMessage
		if err := json.Unmarshal(payload, &message); err != nil {
			continue
		}
		switch message.Type {
		case "ticker":
			price, err := strconv.ParseFloat(message.Price, 64)
			if err != nil {
				continue
			}
			sourceAt, err := time.Parse(time.RFC3339Nano, message.Time)
			if err != nil {
				continue
			}
			ingestCoinbaseSpotTrade(message.ProductID, price, message.TradeID, sourceAt, time.Now())
		case "error":
			return fmt.Errorf("coinbase websocket: %s", strings.TrimSpace(message.Message))
		}
	}
}

// monitorCoinbaseSpot owns one public ticker subscription for all five underlyings. Disconnects
// fail the exact Spot-lag heartbeat through source age, while this loop reconnects with bounded
// exponential backoff. The existing REST reader remains available to research/cold-start paths.
func (s *Server) monitorCoinbaseSpot(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		connectedAt := time.Now()
		err := s.streamCoinbaseSpot(ctx)
		if ctx.Err() != nil {
			return
		}
		if time.Since(connectedAt) >= 30*time.Second {
			backoff = time.Second
		}
		if s != nil && s.log != nil {
			s.log.Warn("Coinbase spot websocket reconnecting", "err", err, "backoff", backoff)
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}
