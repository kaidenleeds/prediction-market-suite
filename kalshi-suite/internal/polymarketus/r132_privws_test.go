package polymarketus

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestR132PrivateOrderPricesUseOutcomeSpace(t *testing.T) {
	tests := []struct {
		name       string
		intent     string
		outcome    string
		action     string
		wantOut    string
		wantAction string
		wantPrice  float64
	}{
		{name: "buy long", intent: "ORDER_INTENT_BUY_LONG", wantOut: "YES", wantAction: "BUY", wantPrice: 0.73},
		{name: "sell long", intent: "ORDER_INTENT_SELL_LONG", wantOut: "YES", wantAction: "SELL", wantPrice: 0.73},
		{name: "buy short", intent: "ORDER_INTENT_BUY_SHORT", wantOut: "NO", wantAction: "BUY", wantPrice: 0.27},
		{name: "sell short", intent: "ORDER_INTENT_SELL_SHORT", wantOut: "NO", wantAction: "SELL", wantPrice: 0.27},
		{name: "explicit no", outcome: "OUTCOME_SIDE_NO", action: "ORDER_ACTION_SELL", wantOut: "NO", wantAction: "SELL", wantPrice: 0.27},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := pusOrderWire{
				ID: "o1", MarketSlug: "m1", Price: pusAmt{Value: "0.73"},
				OutcomeSide: tt.outcome, Action: tt.action, Intent: tt.intent,
			}
			got := o.flat()
			if got.Outcome != tt.wantOut || got.Action != tt.wantAction {
				t.Fatalf("sides = %s/%s, want %s/%s", got.Outcome, got.Action, tt.wantOut, tt.wantAction)
			}
			if math.Abs(got.YesPrice-0.73) > 1e-9 || math.Abs(got.Price-tt.wantPrice) > 1e-9 {
				t.Fatalf("prices outcome=%v yes=%v, want outcome=%v yes=0.73", got.Price, got.YesPrice, tt.wantPrice)
			}
		})
	}
}

func TestR132PrivateOrderSnapshotPublishesOnlyAtEOF(t *testing.T) {
	w := (&Client{}).NewPrivateWS()
	w.connected, w.lastMsg = true, time.Now()
	w.ingest([]byte(`{"orderSubscriptionSnapshot":{"orders":[{"id":"a","marketSlug":"m-a","price":{"value":"0.4"},"intent":"ORDER_INTENT_BUY_LONG"}],"eof":false}}`))
	if got, ok := w.Orders(); ok || got != nil {
		t.Fatalf("incomplete snapshot became visible: ok=%v rows=%v", ok, got)
	}
	w.ingest([]byte(`{"orderSubscriptionSnapshot":{"orders":[{"id":"b","marketSlug":"m-b","price":{"value":"0.6"},"intent":"ORDER_INTENT_BUY_SHORT"}],"eof":true}}`))
	got, ok := w.Orders()
	if !ok || len(got) != 2 {
		t.Fatalf("completed snapshot = ok=%v rows=%d, want true/2", ok, len(got))
	}
}

func TestR132PrivateFillPricesUseOutcomeSpace(t *testing.T) {
	w := (&Client{}).NewPrivateWS()
	frame := map[string]any{
		"orderSubscriptionUpdate": map[string]any{
			"execution": map[string]any{
				"order": map[string]any{
					"id": "short-fill", "marketSlug": "m1",
					"price":  map[string]any{"value": "0.71", "currency": "USD"},
					"intent": "ORDER_INTENT_SELL_SHORT",
				},
				"lastShares": "2.5",
				"lastPx":     map[string]any{"value": "0.72", "currency": "USD"},
				"type":       "EXECUTION_TYPE_PARTIAL_FILL",
				"tradeId":    "t1",
			},
		},
	}
	raw, err := json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	w.ingest(raw)
	fills := w.DrainFills()
	if len(fills) != 1 {
		t.Fatalf("fills=%d, want 1", len(fills))
	}
	f := fills[0]
	if f.Outcome != "NO" || f.Action != "SELL" {
		t.Fatalf("sides = %s/%s, want NO/SELL", f.Outcome, f.Action)
	}
	if math.Abs(f.YesPrice-0.72) > 1e-9 || math.Abs(f.Px-0.28) > 1e-9 {
		t.Fatalf("prices outcome=%v yes=%v, want outcome=0.28 yes=0.72", f.Px, f.YesPrice)
	}
}

func TestR132PrivateFeeReceiptsAndAggressorAliases(t *testing.T) {
	for _, tc := range []struct {
		name       string
		commission string
		aggressor  string
		wantFee    float64
		wantTaker  bool
		wantKnown  bool
	}{
		{name: "camel execution fields", commission: `"commissionNotionalCollected":{"value":"0.07","currency":"USD"}`, aggressor: `"aggressor":true`, wantFee: 0.07, wantTaker: true, wantKnown: true},
		{name: "isAggressor variant", commission: `"commissionNotionalCollected":"-0.01"`, aggressor: `"isAggressor":false`, wantFee: -0.01, wantTaker: false, wantKnown: true},
		{name: "snake fields", commission: `"commission_notional_collected":0.03`, aggressor: `"is_aggressor":true`, wantFee: 0.03, wantTaker: true, wantKnown: true},
		{name: "execution object with explicit flag", commission: `"commissionNotionalCollected":0.02`, aggressor: `"aggressor":{"id":"e1","aggressor":true}`, wantFee: 0.02, wantTaker: true, wantKnown: true},
		{name: "execution object without user flag", commission: `"commissionNotionalCollected":0.01`, aggressor: `"aggressor":{"id":"e2","order":{"id":"o2"}}`, wantFee: 0.01, wantTaker: false, wantKnown: false},
		{name: "explicit retail flag overrides execution object", commission: `"commissionNotionalCollected":0.04`, aggressor: `"isAggressor":false,"aggressor":{"id":"e3","aggressor":true}`, wantFee: 0.04, wantTaker: false, wantKnown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := (&Client{}).NewPrivateWS()
			frame := []byte(fmt.Sprintf(`{"orderSubscriptionUpdate":{"execution":{"order":{"id":"o1","marketSlug":"m1","price":{"value":"0.40"},"intent":"ORDER_INTENT_BUY_LONG"},"lastShares":"2","lastPx":{"value":"0.41"},"type":"EXECUTION_TYPE_FILL","tradeId":"t1",%s,%s}}}`, tc.commission, tc.aggressor))
			w.ingest(frame)
			fills := w.DrainFills()
			if len(fills) != 1 {
				t.Fatalf("fills=%d, want 1", len(fills))
			}
			f := fills[0]
			if !f.CommissionKnown || math.Abs(f.CommissionUSD-tc.wantFee) > 1e-9 {
				t.Fatalf("commission=%v/%v, want %v/true", f.CommissionUSD, f.CommissionKnown, tc.wantFee)
			}
			if f.AggressorKnown != tc.wantKnown || f.Aggressor != tc.wantTaker {
				t.Fatalf("aggressor=%v/%v, want %v/%v", f.Aggressor, f.AggressorKnown, tc.wantTaker, tc.wantKnown)
			}
		})
	}
}

func TestR132OrderCumulativeCommissionNeverBecomesPerFillCommission(t *testing.T) {
	w := (&Client{}).NewPrivateWS()
	w.connected, w.snapReady, w.lastMsg = true, true, time.Now()
	w.ingest([]byte(`{"orderSubscriptionUpdate":{"execution":{"order":{"id":"o1","marketSlug":"m1","price":{"value":"0.40"},"intent":"ORDER_INTENT_BUY_LONG","state":"ORDER_STATE_PARTIALLY_FILLED","commissionNotionalTotalCollected":{"value":"0.12"}},"lastShares":"1","lastPx":{"value":"0.41"},"type":"EXECUTION_TYPE_PARTIAL_FILL","tradeId":"t1"}}}`))

	orders, ok := w.Orders()
	if !ok || len(orders) != 1 || !orders[0].CommissionTotalKnown || math.Abs(orders[0].CommissionTotalUSD-0.12) > 1e-9 {
		t.Fatalf("order cumulative receipt not preserved: ok=%v orders=%+v", ok, orders)
	}
	fills := w.DrainFills()
	if len(fills) != 1 {
		t.Fatalf("fills=%d, want 1", len(fills))
	}
	if fills[0].CommissionKnown || fills[0].CommissionUSD != 0 {
		t.Fatalf("cumulative order commission was double-counted as per-fill: %+v", fills[0])
	}
}

func TestR132RESTReceiptsDecodeCommissionAndAggressorVariants(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/order/o1":
			_, _ = w.Write([]byte(`{"order":{"id":"o1","state":"ORDER_STATE_FILLED","cumQuantity":2,"leavesQuantity":0,"avgPx":{"value":"0.44"},"price":{"value":"0.45"},"intent":"ORDER_INTENT_BUY_LONG","commission_notional_total_collected":"0.08"}}`))
		case "/v1/order/no-missing-average":
			_, _ = w.Write([]byte(`{"order":{"id":"no-missing-average","state":"ORDER_STATE_NEW","cumQuantity":0,"leavesQuantity":1,"price":{"value":"0.75"},"intent":"ORDER_INTENT_BUY_SHORT","tif":"TIME_IN_FORCE_GOOD_TILL_CANCEL","commission_notional_total_collected":"0"}}`))
		case "/v1/portfolio/activities":
			_, _ = w.Write([]byte(`{"activities":[
				{"type":"ACTIVITY_TYPE_TRADE","trade":{"marketSlug":"a","price":{"value":"0.40"},"qty":"1","aggressor":true,"commissionNotionalCollected":{"value":"0.02"}}},
				{"type":"ACTIVITY_TYPE_TRADE","trade":{"marketSlug":"b","price":{"value":"0.50"},"qty":"2","isAggressor":false,"commissionNotionalCollected":"-0.01"}},
				{"type":"ACTIVITY_TYPE_TRADE","trade":{"marketSlug":"c","price":{"value":"0.60"},"qty":"3","is_aggressor":true,"commission_notional_collected":0.03}},
				{"type":"ACTIVITY_TYPE_TRADE","trade":{"marketSlug":"d","price":{"value":"0.61"},"qty":"4","aggressor":{"id":"exec","order":{"id":"o4"}},"commissionNotionalCollected":{"value":"0.04"}}},
				{"type":"ACTIVITY_TYPE_TRADE","trade":{"marketSlug":"e","price":{"value":"0.62"},"qty":"5","aggressor":{"aggressor":true},"commissionNotionalCollected":{"value":"0.05"}}},
				{"type":"ACTIVITY_TYPE_TRADE","trade":{"marketSlug":"f","price":{"value":"0.63"},"qty":"6","isAggressor":false,"aggressor":{"aggressor":true},"commissionNotionalCollected":{"value":"0.06"}}}
			],"eof":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()
	c := &Client{baseURL: ts.URL, keyID: "test", priv: make(ed25519.PrivateKey, ed25519.PrivateKeySize), http: ts.Client()}

	st, err := c.GetOrderState(context.Background(), "o1")
	if err != nil || !st.CommissionTotalKnown || math.Abs(st.CommissionTotalUSD-0.08) > 1e-9 {
		t.Fatalf("order receipt = %+v err=%v", st, err)
	}
	noMissing, err := c.GetOrderState(context.Background(), "no-missing-average")
	if err != nil || noMissing.AvgPx != 0 || noMissing.YesAvgPx != 0 {
		t.Fatalf("missing NO average must remain unknown zero, receipt=%+v err=%v", noMissing, err)
	}
	acts, err := c.Activities(context.Background())
	if err != nil || len(acts) != 6 {
		t.Fatalf("activities=%+v err=%v", acts, err)
	}
	for i, want := range []struct {
		taker bool
		known bool
		fee   float64
	}{{true, true, 0.02}, {false, true, -0.01}, {true, true, 0.03}, {false, false, 0.04}, {true, true, 0.05}, {false, true, 0.06}} {
		if acts[i].TakerKnown != want.known || acts[i].Taker != want.taker || !acts[i].CommissionKnown || math.Abs(acts[i].CommissionUSD-want.fee) > 1e-9 {
			t.Fatalf("activity %d receipt=%+v want taker=%v/%v fee=%v", i, acts[i], want.taker, want.known, want.fee)
		}
	}
}
