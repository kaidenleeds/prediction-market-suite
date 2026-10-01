package kalshi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestR148OrderCurrentExecutionSchemaRejectsNullOrMalformedRequiredCounts(t *testing.T) {
	base := `{"order_id":"o","client_order_id":"c","ticker":"KX","status":"canceled","type":"limit","outcome_side":"yes","book_side":"bid","initial_count_fp":"1","fill_count_fp":%s}`
	for _, rawFill := range []string{"null", `""`, `"garbage"`, `"NaN"`, `"-1"`} {
		var order Order
		if err := json.Unmarshal([]byte(fmt.Sprintf(base, rawFill)), &order); err != nil {
			t.Fatal(err)
		}
		if order.CurrentExecutionSchemaKnown() {
			t.Fatalf("malformed fill_count_fp=%s accepted", rawFill)
		}
	}
}

func TestR148GetOrdersByClientOrderIDUsesCompleteAllStatusHistory(t *testing.T) {
	minTS := time.Date(2026, 7, 15, 12, 34, 56, 0, time.UTC)
	hits := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		q := r.URL.Query()
		if r.URL.Path != "/portfolio/orders" || q.Get("ticker") != "KX-R148" ||
			q.Get("min_ts") != strconv.FormatInt(minTS.Unix(), 10) || q.Get("limit") != "1000" ||
			q.Get("status") != "" {
			t.Fatalf("recovery query=%s", r.URL.String())
		}
		if q.Get("cursor") == "" {
			writeTestJSON(t, w, map[string]any{"orders": []map[string]any{{
				"order_id": "other", "client_order_id": "other-client", "ticker": "KX-R148",
			}}, "cursor": "next"})
			return
		}
		if q.Get("cursor") != "next" {
			t.Fatalf("cursor=%q", q.Get("cursor"))
		}
		writeTestJSON(t, w, map[string]any{"orders": []map[string]any{{
			"order_id": "wanted", "client_order_id": "stg-wanted", "ticker": "KX-R148",
			"status": "executed", "type": "limit", "outcome_side": "yes", "book_side": "bid", "initial_count_fp": "1", "fill_count_fp": "1",
		}}, "cursor": ""})
	}))
	defer ts.Close()
	c := newPortfolioTestClient(t, ts.URL)
	rows, err := c.GetOrdersByClientOrderID(context.Background(), "KX-R148", "stg-wanted", minTS)
	if err != nil || len(rows) != 1 || rows[0].OrderID != "wanted" || hits != 2 {
		t.Fatalf("rows=%+v hits=%d err=%v", rows, hits, err)
	}
}

func TestR148GetOrdersByClientOrderIDEmptyIsAuthoritativeEmpty(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeTestJSON(t, w, map[string]any{"orders": []any{}, "cursor": ""})
	}))
	defer ts.Close()
	c := newPortfolioTestClient(t, ts.URL)
	rows, err := c.GetOrdersByClientOrderID(context.Background(), "KX-R148", "missing", time.Now().Add(-time.Hour))
	if err != nil || rows == nil || len(rows) != 0 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
}

func TestR148GetOrdersByClientOrderIDFailsClosedOnCursorLoopOrTickerDrift(t *testing.T) {
	t.Run("cursor-loop", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeTestJSON(t, w, map[string]any{"orders": []any{}, "cursor": "stuck"})
		}))
		defer ts.Close()
		c := newPortfolioTestClient(t, ts.URL)
		rows, err := c.GetOrdersByClientOrderID(context.Background(), "KX-R148", "wanted", time.Now().Add(-time.Hour))
		if err == nil || rows != nil || !strings.Contains(err.Error(), "cursor") {
			t.Fatalf("rows=%+v err=%v", rows, err)
		}
	})
	t.Run("ticker-drift", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeTestJSON(t, w, map[string]any{"orders": []map[string]any{{
				"order_id": "wrong", "client_order_id": "wanted", "ticker": "KX-WRONG",
			}}, "cursor": ""})
		}))
		defer ts.Close()
		c := newPortfolioTestClient(t, ts.URL)
		rows, err := c.GetOrdersByClientOrderID(context.Background(), "KX-R148", "wanted", time.Now().Add(-time.Hour))
		if err == nil || rows != nil || !strings.Contains(err.Error(), "ticker filter") {
			t.Fatalf("rows=%+v err=%v", rows, err)
		}
	})
}
