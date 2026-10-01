package server

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

func TestR153KalshiExitMarkTreatsEmptyBidAsKnownZero(t *testing.T) {
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/markets/KXZERO/orderbook":
			_, _ = w.Write([]byte(`{"ticker":"KXZERO","orderbook_fp":{"yes_dollars":[],"no_dollars":[]}}`))
		case "/markets/KXBOOK/orderbook":
			_, _ = w.Write([]byte(`{"ticker":"KXBOOK","orderbook_fp":{"yes_dollars":[["0.2000","5"]],"no_dollars":[["0.7000","4"]]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer venue.Close()
	s := testServer(t)
	s.kal = kalshi.NewClient(venue.URL, queueTestSigner(t), 10_000, time.Second)
	if mark, ok := s.curKalshiExitMark(context.Background(), "KXZERO", "YES"); !ok || mark != 0 {
		t.Fatalf("known empty YES bid = %.4f ok=%v, want 0,true", mark, ok)
	}
	if mark, ok := s.curKalshiExitMark(context.Background(), "KXBOOK", "YES"); !ok || mark != .2 {
		t.Fatalf("YES exit mark = %.4f ok=%v", mark, ok)
	}
	if mark, ok := s.curKalshiExitMark(context.Background(), "KXBOOK", "NO"); !ok || mark != .7 {
		t.Fatalf("NO exit mark = %.4f ok=%v", mark, ok)
	}
}

func TestR153KalshiPnLRequiresSuccessfulCurrentPositionRead(t *testing.T) {
	if liveKalshiPnLComplete(false, true, true) {
		t.Fatal("a failed authenticated positions read must make Kalshi P&L incomplete")
	}
	if !liveKalshiPnLComplete(true, true, true) {
		t.Fatal("successful positions + complete marks + fresh ledger should be complete")
	}
	if !strings.Contains(dashboardHTML, "live_pnl_kalshi_complete===true") ||
		!strings.Contains(dashboardHTML, ">K n/a</span>") {
		t.Fatal("header must render Kalshi P&L as n/a when its completeness receipt is false")
	}
}

func TestR153PolyUSPnLRequiresFreshAccountAndEveryOpenMark(t *testing.T) {
	knownZero := polymarketus.PUSPosition{
		Slug: "known-zero", Net: 3, Cost: 1.2, CashVal: 0, CashValKnown: true,
	}
	if pnl, marked := polyUSOpenPositionPnL(knownZero); !marked || pnl != -1.2 {
		t.Fatalf("explicit venue $0 mark pnl=%v marked=%v, want -1.2,true", pnl, marked)
	}
	unknown := knownZero
	unknown.CashValKnown = false
	if _, marked := polyUSOpenPositionPnL(unknown); marked {
		t.Fatal("omitted cashValue must not masquerade as an authoritative $0 mark")
	}
	for name, got := range map[string]bool{
		"venue-disabled": livePolyUSPnLComplete(false, true, true, true),
		"account-stale":  livePolyUSPnLComplete(true, false, true, true),
		"mark-missing":   livePolyUSPnLComplete(true, true, false, true),
		"ledger-stale":   livePolyUSPnLComplete(true, true, true, false),
	} {
		if got {
			t.Fatalf("%s unexpectedly reported complete", name)
		}
	}
	if !livePolyUSPnLComplete(true, true, true, true) {
		t.Fatal("fresh account + every mark + fresh ledger should be complete")
	}
}

func TestR153FinalSettlementOverridesEmptyActiveBookMark(t *testing.T) {
	winningYES := kalshi.Market{Ticker: "KXFINAL", Status: "finalized", Result: "yes"}
	if mark, ok := kalshiHeldPositionMark(&winningYES, "YES", 0, true); !ok || mark != 1 {
		t.Fatalf("finalized YES winner mark=%v ok=%v, want authoritative 1,true", mark, ok)
	}
	if mark, ok := kalshiHeldPositionMark(&winningYES, "NO", 0.73, true); !ok || mark != 0 {
		t.Fatalf("finalized NO loser mark=%v ok=%v, want authoritative 0,true", mark, ok)
	}
	active := kalshi.Market{Ticker: "KXACTIVE", Status: "active"}
	if mark, ok := kalshiHeldPositionMark(&active, "YES", 0, true); !ok || mark != 0 {
		t.Fatalf("active empty bid mark=%v ok=%v, want conservative 0,true", mark, ok)
	}
}

func TestR153FinalSettlementCannotAlsoCountAsLaggingOpenPosition(t *testing.T) {
	const ticker = "KX-R153-SETTLED"
	var lagging kalshi.MarketPosition
	if err := json.Unmarshal([]byte(`{
		"ticker":"KX-R153-SETTLED",
		"position_fp":"10.00",
		"market_exposure_dollars":"5.00",
		"realized_pnl_dollars":"0.00",
		"fees_paid_dollars":"0.10"
	}`), &lagging); err != nil {
		t.Fatal(err)
	}
	if lagging.PositionQty() == 0 {
		t.Fatal("fixture must model a non-flat position endpoint lag")
	}
	finals := map[string]int64{ticker: liveLossUSDToUnits(4.90)}
	if !kalshiPositionFinalizedInLossLedger(finals, lagging.Ticker) {
		t.Fatal("authoritative terminal ticker was allowed back into open position totals")
	}
	// The durable settlement remains the sole P&L contribution; the lagging row contributes zero.
	if got := liveLossUnitsToUSD(finals[ticker]); math.Abs(got-4.90) > 1e-9 {
		t.Fatalf("authoritative realized P&L=%v, want 4.90", got)
	}
}
