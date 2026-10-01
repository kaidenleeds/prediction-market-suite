package server

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/ev"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

func seedR135KalshiFee(s *Server) {
	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{
		"KXROUND": {taker: .07, maker: 0, typ: "quadratic", multiplier: 1},
	}
	s.kalFeesAt = time.Now()
	s.kalFeesNext = time.Time{}
	s.kalFeeMu.Unlock()
}

func TestR135BlendedFeeRejectsUnknownPlatformInsteadOfDefaultingPolyInt(t *testing.T) {
	s := testServer(t)
	for _, platform := range []string{"", "kalshii", "poly-us", "unknown"} {
		if got := s.blendedFee(platform, "market", "sports", false, 1, .5); got != unsupportedKalFee {
			t.Fatalf("blendedFee(%q) = %v, want fail-closed %v", platform, got, unsupportedKalFee)
		}
	}
	if got := s.blendedFee(" POLYMARKET ", "market", "sports", false, 1, .5); got == unsupportedKalFee {
		t.Fatal("explicit canonical Poly-int platform should remain supported")
	}
}

func TestR135KalshiExactFeeActionPreservesBuyWrapperAndUsesSellRounding(t *testing.T) {
	s := testServer(t)
	seedR135KalshiFee(s)
	const contracts, price = .37, .003

	buy, buyKnown, _ := s.kalFeeExact("KXROUND-MARKET", false, contracts, price)
	sell, sellKnown, _ := s.kalFeeExactAction("KXROUND-MARKET", false, contracts, price, false)
	wantBuy := ev.FeeBreakdownCoeff(.07, contracts, price, true).Net
	wantSell := ev.FeeBreakdownCoeff(.07, contracts, price, false).Net
	if !buyKnown || !sellKnown || math.Abs(buy-wantBuy) > 1e-12 || math.Abs(sell-wantSell) > 1e-12 {
		t.Fatalf("action fees buy=%v/%v sell=%v/%v, want buy=%v sell=%v", buy, buyKnown, sell, sellKnown, wantBuy, wantSell)
	}
	if buy == sell {
		t.Fatalf("test fixture must exercise different buy/sell balance rounding; both were %v", buy)
	}
	if got := s.blendedFee("kalshi", "KXROUND-MARKET", "", false, contracts, price); math.Abs(got-wantBuy) > 1e-12 {
		t.Fatalf("legacy blendedFee entry wrapper = %v, want BUY fee %v", got, wantBuy)
	}
	if got := s.blendedFeeAction("kalshi", "KXROUND-MARKET", "", false, contracts, price, false); math.Abs(got-wantSell) > 1e-12 {
		t.Fatalf("action-aware blended SELL fee = %v, want %v", got, wantSell)
	}
}

func TestR135PaperSellPersistsKalshiSellSideFee(t *testing.T) {
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/trade-api/v2/markets/KXROUND-MARKET/orderbook" {
			_ = json.NewEncoder(w).Encode(map[string]any{"orderbook": map[string]any{"yes": []any{}, "no": []any{}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"markets": []any{}, "cursor": ""})
	}))
	defer venue.Close()

	s := testServer(t)
	s.kal = kalshi.NewClient(venue.URL, nil, 1000, time.Second)
	seedR135KalshiFee(s)
	cfg := *s.cfg()
	cfg.Auto.FeeMakerShare = 0 // deterministic taker exit
	cfg.Auto.MakerFirst = false
	s.cfgP.Store(&cfg)

	const contracts, price = .37, .003
	ctx := context.Background()
	if _, err := s.store.InsertPaperFill(ctx, paper.Fill{
		Platform: "kalshi", Ticker: "KXROUND-MARKET", Title: "rounding", Side: "YES",
		Action: "BUY", Price: price, Contracts: contracts, Source: "historical-test",
	}); err != nil {
		t.Fatalf("insert open paper position: %v", err)
	}
	if sold, _, err := s.sellPaperQty(ctx, "kalshi", "KXROUND-MARKET", "YES", "test-close", contracts); err != nil || math.Abs(sold-contracts) > 1e-12 {
		t.Fatalf("sellPaperQty sold=%v err=%v", sold, err)
	}
	fills, err := s.store.ListPaperFills(ctx)
	if err != nil || len(fills) != 2 {
		t.Fatalf("paper fills len=%d err=%v", len(fills), err)
	}
	want := ev.FeeBreakdownCoeff(.07, contracts, price, false).Net
	if got := fills[1].Fee; math.Abs(got-want) > 1e-12 {
		t.Fatalf("persisted paper SELL fee = %v, want sell-side %v", got, want)
	}
}
