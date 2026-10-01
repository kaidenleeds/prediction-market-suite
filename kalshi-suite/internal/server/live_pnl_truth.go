package server

import (
	"strings"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

// kalshiSettledSideMark returns the venue's authoritative payout for a held side. Settlement
// truth always outranks an empty/stale order book: a winning finalized position is worth $1 even
// when its now-closed book has no bid.
func kalshiSettledSideMark(m kalshi.Market, side string) (float64, bool) {
	yes := m.SettledYes()
	if yes < 0 || yes > 1 {
		return 0, false
	}
	if strings.EqualFold(strings.TrimSpace(side), "NO") {
		yes = 1 - yes
	}
	return yes, true
}

func kalshiHeldPositionMark(market *kalshi.Market, side string, activeMark float64, activeKnown bool) (float64, bool) {
	if market != nil {
		if settled, ok := kalshiSettledSideMark(*market, side); ok {
			return settled, true
		}
	}
	return activeMark, activeKnown
}

// polyUSOpenPositionPnL reads only the authenticated portfolio cashValue receipt. Known zero is a
// valid mark; an omitted cashValue is not. Closed/expired rows are not open-mark requirements.
func polyUSOpenPositionPnL(p polymarketus.PUSPosition) (float64, bool) {
	if p.Net == 0 || p.Expired {
		return 0, false
	}
	if !p.CashValKnown {
		return 0, false
	}
	return p.CashVal - p.Cost, true
}

func liveKalshiPnLComplete(positionReadOK, everyOpenMarked, ledgerComplete bool) bool {
	return positionReadOK && everyOpenMarked && ledgerComplete
}

// kalshiPositionFinalizedInLossLedger prevents a non-atomic venue read from counting one settled
// market twice. The settlement ledger and its fee-net delta are updated under liveMu together, but
// Kalshi's open-position endpoint can briefly lag that terminal receipt. Once the durable ledger
// owns a ticker, that stale position row is no longer open exposure or unrealized P&L.
func kalshiPositionFinalizedInLossLedger(finals map[string]int64, ticker string) bool {
	_, finalized := finals[strings.TrimSpace(ticker)]
	return finalized
}

func livePolyUSPnLComplete(accountRequired, accountFresh, everyOpenMarked, ledgerComplete bool) bool {
	return accountRequired && accountFresh && everyOpenMarked && ledgerComplete
}
