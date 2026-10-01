// Package signal derives lightweight, keyless trading signals from a single
// market snapshot: a locked YES/NO arbitrage (rigorous, after fees) and a recent
// price move (a soft momentum / order-flow proxy). These feed the dashboard's
// "Signal" column and, later, the strategy engine.
package signal

import "github.com/kalshi-suite/kalshi-suite/internal/ev"

// ArbProfit returns the guaranteed profit per YES+NO pair (in dollars) when both
// sides can be bought for under $1, net of two taker fees. Buying one YES and one
// NO always pays out exactly $1 (one side wins), so if yesAsk+noAsk+fees < 1 the
// rest is locked profit. A value <= 0 means there is no arbitrage.
func ArbProfit(cls ev.MarketClass, yesAsk, noAsk float64) float64 {
	if yesAsk <= 0 || noAsk <= 0 || yesAsk >= 1 || noAsk >= 1 {
		return 0
	}
	cost := yesAsk + noAsk
	if cost >= 1 {
		return 0
	}
	fees := ev.Fee(cls, false, 1, yesAsk) + ev.Fee(cls, false, 1, noAsk)
	return 1 - cost - fees
}

// MoveCents is the recent price change in cents (last vs the prior reference
// price). Returns 0 when either price is missing or the market is effectively
// settled (≤2¢ or ≥98¢), to avoid reporting noise on decided markets.
func MoveCents(last, prev float64) float64 {
	if last <= 0.02 || last >= 0.98 || prev <= 0.02 || prev >= 0.98 {
		return 0
	}
	return (last - prev) * 100
}

// SpreadCents is the YES bid/ask spread in cents (0 if unknown or crossed).
func SpreadCents(yesBid, yesAsk float64) float64 {
	if yesBid <= 0 || yesAsk <= 0 || yesAsk < yesBid {
		return 0
	}
	return (yesAsk - yesBid) * 100
}
