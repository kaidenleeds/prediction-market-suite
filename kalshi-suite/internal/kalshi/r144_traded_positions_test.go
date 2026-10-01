package kalshi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestR144GetTradedPositionsUsesDurableTotalTradedFilter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("count_filter"); got != "total_traded" {
			t.Errorf("count_filter=%q want total_traded", got)
		}
		writeTestJSON(t, w, map[string]any{
			"market_positions": []map[string]any{{
				"ticker": "KX-FLAT-CLOSED", "position_fp": "0", "total_traded_dollars": "12.34",
				"realized_pnl_dollars": "-0.7000", "fees_paid_dollars": "0.0100",
			}},
			"cursor": "",
		})
	}))
	defer srv.Close()

	rows, err := newPortfolioTestClient(t, srv.URL).GetTradedPositions(context.Background())
	if err != nil || len(rows) != 1 {
		t.Fatalf("GetTradedPositions rows=%+v err=%v", rows, err)
	}
	if rows[0].Ticker != "KX-FLAT-CLOSED" || rows[0].PositionQty() != 0 ||
		rows[0].RealizedUSD() != -0.70 || rows[0].FeesUSD() != 0.01 || !rows[0].FeesKnown() {
		t.Fatalf("closed cumulative row parsed incorrectly: %+v", rows[0])
	}
}
