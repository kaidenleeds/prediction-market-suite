package kalshi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func newPortfolioTestClient(t *testing.T, baseURL string) *Client {
	t.Helper()
	signer, err := NewSigner("throwaway-test-key", []byte(testPEM(t)))
	if err != nil {
		t.Fatalf("build throwaway signer: %v", err)
	}
	return NewClient(baseURL, signer, 10000, 2*time.Second)
}

func writeTestJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Errorf("encode fixture: %v", err)
	}
}

func TestPortfolioReadersPaginateAndFilter(t *testing.T) {
	var ordersHits, positionsHits, fillsHits, settlementsHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch r.URL.Path {
		case "/portfolio/orders":
			ordersHits.Add(1)
			if q.Get("limit") != "1000" || q.Get("status") != "resting" {
				t.Errorf("orders filters = %s, want limit=1000 status=resting", r.URL.RawQuery)
			}
			if q.Get("cursor") == "" {
				writeTestJSON(t, w, map[string]any{"orders": []map[string]any{{"order_id": "o1"}}, "cursor": "orders-next"})
			} else {
				if q.Get("cursor") != "orders-next" {
					t.Errorf("orders cursor = %q", q.Get("cursor"))
				}
				writeTestJSON(t, w, map[string]any{"orders": []map[string]any{{"order_id": "o2"}}, "cursor": ""})
			}
		case "/portfolio/positions":
			positionsHits.Add(1)
			if q.Get("limit") != "1000" || q.Get("count_filter") != "position" {
				t.Errorf("positions filters = %s, want limit=1000 count_filter=position", r.URL.RawQuery)
			}
			if q.Get("cursor") == "" {
				writeTestJSON(t, w, map[string]any{"market_positions": []map[string]any{{"ticker": "p1", "position_fp": "1.00"}}, "cursor": "positions-next"})
			} else {
				writeTestJSON(t, w, map[string]any{"market_positions": []map[string]any{{"ticker": "p2", "position_fp": "2.00"}}, "cursor": ""})
			}
		case "/portfolio/fills":
			fillsHits.Add(1)
			if q.Get("limit") != "1000" {
				t.Errorf("fills limit = %q", q.Get("limit"))
			}
			minTS, err := strconv.ParseInt(q.Get("min_ts"), 10, 64)
			if err != nil || time.Since(time.Unix(minTS, 0)) < 23*time.Hour || time.Since(time.Unix(minTS, 0)) > 25*time.Hour {
				t.Errorf("fills min_ts is not a 24h rolling bound: %q err=%v", q.Get("min_ts"), err)
			}
			if q.Get("cursor") == "" {
				writeTestJSON(t, w, map[string]any{"fills": []map[string]any{{"fill_id": "f1"}}, "cursor": "fills-next"})
			} else {
				writeTestJSON(t, w, map[string]any{"fills": []map[string]any{{"fill_id": "f2"}}, "cursor": ""})
			}
		case "/portfolio/settlements":
			hit := settlementsHits.Add(1)
			wantLimit := "2"
			if hit == 2 {
				wantLimit = "1"
			}
			if q.Get("limit") != wantLimit {
				t.Errorf("settlements page %d limit=%q want %q", hit, q.Get("limit"), wantLimit)
			}
			if q.Get("cursor") == "" {
				writeTestJSON(t, w, map[string]any{"settlements": []map[string]any{{"ticker": "s1"}}, "cursor": "settlements-next"})
			} else {
				writeTestJSON(t, w, map[string]any{"settlements": []map[string]any{{"ticker": "s2"}}, "cursor": ""})
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newPortfolioTestClient(t, srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if rows, err := c.GetOrders(ctx); err != nil || len(rows) != 2 || rows[0].OrderID != "o1" || rows[1].OrderID != "o2" {
		t.Fatalf("GetOrders rows=%+v err=%v", rows, err)
	}
	if rows, err := c.GetPositions(ctx); err != nil || len(rows) != 2 || rows[0].Ticker != "p1" || rows[1].Ticker != "p2" {
		t.Fatalf("GetPositions rows=%+v err=%v", rows, err)
	}
	if rows, err := c.GetFills(ctx); err != nil || len(rows) != 2 || rows[0].FillID != "f1" || rows[1].FillID != "f2" {
		t.Fatalf("GetFills rows=%+v err=%v", rows, err)
	}
	if rows, err := c.GetSettlements(ctx, 2); err != nil || len(rows) != 2 || rows[0].Ticker != "s1" || rows[1].Ticker != "s2" {
		t.Fatalf("GetSettlements rows=%+v err=%v", rows, err)
	}
	if ordersHits.Load() != 2 || positionsHits.Load() != 2 || fillsHits.Load() != 2 || settlementsHits.Load() != 2 {
		t.Fatalf("page hits orders=%d positions=%d fills=%d settlements=%d", ordersHits.Load(), positionsHits.Load(), fillsHits.Load(), settlementsHits.Load())
	}
}

func TestPortfolioPaginationRejectsRepeatedCursor(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		writeTestJSON(t, w, map[string]any{
			"orders": []map[string]any{{"order_id": "duplicate-page"}},
			"cursor": "stuck",
		})
	}))
	defer srv.Close()

	c := newPortfolioTestClient(t, srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if rows, err := c.GetOrders(ctx); err == nil || rows != nil {
		t.Fatalf("repeated cursor must fail closed: rows=%+v err=%v", rows, err)
	}
	if hits.Load() != 2 {
		t.Fatalf("repeated cursor must stop after page 2, hits=%d", hits.Load())
	}
}

func TestGetSettlementsWindowReportsCappedVersusComplete(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/portfolio/settlements" {
			http.NotFound(w, r)
			return
		}
		writeTestJSON(t, w, map[string]any{
			"settlements": []map[string]any{{"ticker": "s1"}},
			"cursor":      "older-receipts-exist",
		})
	}))
	defer srv.Close()
	c := newPortfolioTestClient(t, srv.URL)
	rows, complete, err := c.GetSettlementsWindow(context.Background(), 1)
	if err != nil || len(rows) != 1 || complete {
		t.Fatalf("capped settlement window rows=%v complete=%v err=%v", rows, complete, err)
	}
}

func TestGetOrdersRejectsMissingRequiredEnvelopeInsteadOfHealthyEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeTestJSON(t, w, map[string]any{"cursor": ""})
	}))
	defer srv.Close()
	c := newPortfolioTestClient(t, srv.URL)
	rows, err := c.GetOrders(context.Background())
	if err == nil || rows != nil {
		t.Fatalf("missing orders array must be a schema error: rows=%+v err=%v", rows, err)
	}
}
