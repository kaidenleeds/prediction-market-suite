package polymarketus

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestR148PlaceLiveOrderMapsEveryOutcomeActionExactly(t *testing.T) {
	tests := []struct {
		outcome, side, wantOutcome, wantAction, wantPrice string
	}{
		{"YES", "BUY", "OUTCOME_SIDE_YES", "ORDER_ACTION_BUY", "0.37"},
		{"YES", "SELL", "OUTCOME_SIDE_YES", "ORDER_ACTION_SELL", "0.37"},
		{"NO", "BUY", "OUTCOME_SIDE_NO", "ORDER_ACTION_BUY", "0.63"},
		{"NO", "SELL", "OUTCOME_SIDE_NO", "ORDER_ACTION_SELL", "0.63"},
	}
	for _, tc := range tests {
		t.Run(tc.outcome+"_"+tc.side, func(t *testing.T) {
			var got struct {
				Outcome string `json:"outcomeSide"`
				Action  string `json:"action"`
				Price   struct {
					Value string `json:"value"`
				} `json:"price"`
			}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
					t.Errorf("decode request: %v", err)
				}
				_, _ = w.Write([]byte(`{"id":"order-id"}`))
			}))
			defer ts.Close()
			c := testSignedClient(ts)
			if _, err := c.PlaceLiveOrder(context.Background(), LiveOrder{MarketSlug: "market",
				Outcome: tc.outcome, Side: tc.side, Price: 0.37, Size: 1}); err != nil {
				t.Fatalf("PlaceLiveOrder: %v", err)
			}
			if got.Outcome != tc.wantOutcome || got.Action != tc.wantAction || got.Price.Value != tc.wantPrice {
				t.Fatalf("wire outcome/action/price=%q/%q/%q want %q/%q/%q", got.Outcome, got.Action,
					got.Price.Value, tc.wantOutcome, tc.wantAction, tc.wantPrice)
			}
		})
	}
}

func TestR148PlaceLiveOrderRejectsUnknownOutcomeOrActionBeforeNetwork(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"id":"unexpected"}`))
	}))
	defer ts.Close()
	c := testSignedClient(ts)
	for _, order := range []LiveOrder{
		{MarketSlug: "market", Outcome: "", Side: "BUY", Price: 0.42, Size: 1},
		{MarketSlug: "market", Outcome: "MAYBE", Side: "BUY", Price: 0.42, Size: 1},
		{MarketSlug: "market", Outcome: "YES", Side: "", Price: 0.42, Size: 1},
		{MarketSlug: "market", Outcome: "YES", Side: "HOLD", Price: 0.42, Size: 1},
	} {
		if _, err := c.PlaceLiveOrder(context.Background(), order); err == nil {
			t.Fatalf("invalid order was accepted: %+v", order)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid orders reached venue: calls=%d", calls.Load())
	}
}

func TestR148SynchronousFOKPreservesExactExecutionReceipt(t *testing.T) {
	var got struct {
		TIF                  string `json:"tif"`
		SynchronousExecution bool   `json:"synchronousExecution"`
		MaxBlockTime         string `json:"maxBlockTime"`
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode request: %v", err)
		}
		_, _ = w.Write([]byte(`{
			"id":"order-7",
			"executions":[{
				"id":"execution-9","tradeId":"trade-11","type":"EXECUTION_TYPE_FILL",
				"transactTime":"2026-07-15T12:00:00Z","lastShares":"2.5","lastPx":{"value":"0.31","currency":"USD"},
				"commissionNotionalCollected":"0.0275",
				"order":{"id":"order-7","marketSlug":"match","intent":"ORDER_INTENT_BUY_SHORT",
					"state":"ORDER_STATE_FILLED","tif":"TIME_IN_FORCE_FILL_OR_KILL",
					"price":{"value":"0.32","currency":"USD"},
					"avgPx":{"value":"0.31","currency":"USD"},"quantity":2.5,
					"cumQuantity":2.5,"leavesQuantity":0}
			}]
		}`))
	}))
	defer ts.Close()
	c := testSignedClient(ts)
	result, err := c.PlaceLiveOrderDetailed(context.Background(), LiveOrder{MarketSlug: "match",
		Outcome: "NO", Side: "BUY", Price: 0.68, Size: 2.5, FillOrKill: true,
		Synchronous: true, MaxBlockTimeSeconds: 10})
	if err != nil {
		t.Fatalf("PlaceLiveOrderDetailed: %v", err)
	}
	if got.TIF != "TIME_IN_FORCE_FILL_OR_KILL" || !got.SynchronousExecution || got.MaxBlockTime != "10" {
		t.Fatalf("synchronous FOK wire=%+v", got)
	}
	if result.OrderID != "order-7" || len(result.Executions) != 1 {
		t.Fatalf("result=%+v", result)
	}
	e := result.Executions[0]
	if e.OrderID != "order-7" || e.TradeID != "trade-11" || e.Outcome != "NO" || e.Action != "BUY" ||
		math.Abs(e.OrderQty-2.5) > 1e-9 || math.Abs(e.CumQty-2.5) > 1e-9 ||
		math.Abs(e.YesAveragePrice-0.31) > 1e-9 || math.Abs(e.AveragePrice-0.69) > 1e-9 ||
		!e.CommissionKnown || math.Abs(e.CommissionUSD-0.0275) > 1e-9 {
		t.Fatalf("execution receipt=%+v", e)
	}
}

func TestR148OrderResponseSidePairIsAtomicWithIntentOnlyFallback(t *testing.T) {
	tests := []struct {
		name, outcome, action, intent, wantOutcome, wantAction string
	}{
		{"current pair wins", "OUTCOME_SIDE_NO", "ORDER_ACTION_SELL", "ORDER_INTENT_BUY_LONG", "NO", "SELL"},
		{"legacy intent fallback", "", "", "ORDER_INTENT_BUY_SHORT", "NO", "BUY"},
		{"partial outcome pair", "OUTCOME_SIDE_YES", "", "ORDER_INTENT_BUY_LONG", "", ""},
		{"partial action pair", "", "ORDER_ACTION_BUY", "ORDER_INTENT_BUY_LONG", "", ""},
		{"malformed pair", "OUTCOME_SIDE_MAYBE", "ORDER_ACTION_BUY", "ORDER_INTENT_BUY_LONG", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			outcome, action := orderSides(tc.outcome, tc.action, tc.intent)
			if outcome != tc.wantOutcome || action != tc.wantAction {
				t.Fatalf("sides=%q/%q want %q/%q", outcome, action, tc.wantOutcome, tc.wantAction)
			}
		})
	}
}

func TestR148PlaceLiveOrderMapsTimeInForce(t *testing.T) {
	tests := []struct {
		name  string
		order LiveOrder
		want  string
	}{
		{name: "default IOC", order: LiveOrder{}, want: "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL"},
		{name: "post only GTC", order: LiveOrder{PostOnly: true}, want: "TIME_IN_FORCE_GOOD_TILL_CANCEL"},
		{name: "complete FOK", order: LiveOrder{FillOrKill: true}, want: "TIME_IN_FORCE_FILL_OR_KILL"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got struct {
				TIF                     string `json:"tif"`
				ParticipateDontInitiate bool   `json:"participateDontInitiate"`
			}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
					t.Errorf("decode request: %v", err)
				}
				_, _ = w.Write([]byte(`{"id":"order-id"}`))
			}))
			defer ts.Close()
			c := testSignedClient(ts)
			o := tc.order
			o.MarketSlug, o.Outcome, o.Side, o.Price, o.Size = "market", "YES", "BUY", 0.42, 1
			if _, err := c.PlaceLiveOrder(context.Background(), o); err != nil {
				t.Fatalf("PlaceLiveOrder: %v", err)
			}
			if got.TIF != tc.want {
				t.Fatalf("tif=%q want %q", got.TIF, tc.want)
			}
			if got.ParticipateDontInitiate != o.PostOnly {
				t.Fatalf("participateDontInitiate=%v want %v", got.ParticipateDontInitiate, o.PostOnly)
			}
		})
	}
}

func TestR148PlaceLiveOrderRejectsPostOnlyFillOrKill(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"id":"unexpected"}`))
	}))
	defer ts.Close()
	c := testSignedClient(ts)
	_, err := c.PlaceLiveOrder(context.Background(), LiveOrder{
		MarketSlug: "market", Outcome: "YES", Side: "BUY", Price: 0.42, Size: 1,
		PostOnly: true, FillOrKill: true,
	})
	if err == nil {
		t.Fatal("post-only + fill-or-kill was accepted")
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid order reached venue: calls=%d", calls.Load())
	}
}
