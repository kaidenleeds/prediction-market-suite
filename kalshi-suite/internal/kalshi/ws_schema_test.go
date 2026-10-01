package kalshi

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestIngestTradeCanonicalDirectionAndMillis(t *testing.T) {
	c := &Client{}
	c.ingestTrade([]byte(`{"type":"trade","msg":{"trade_id":"trade-1","market_ticker":"KXTEST","count_fp":"2.00","yes_price_dollars":"0.4100","no_price_dollars":"0.5900","taker_outcome_side":"no","taker_book_side":"ask","ts_ms":1700000000123}}`))
	tape, ok := c.LiveTape()
	if !ok || len(tape) != 1 {
		t.Fatalf("live tape missing canonical trade: ok=%v len=%d", ok, len(tape))
	}
	tr := tape[0]
	if tr.TradeID != "trade-1" || tr.Aggressor() != "no" || tr.TakerSide != "no" {
		t.Fatalf("wrong decoded trade: %+v", tr)
	}
	want := time.UnixMilli(1700000000123).UTC()
	got, err := time.Parse(time.RFC3339Nano, tr.CreatedTime)
	if err != nil || !got.Equal(want) {
		t.Fatalf("ts_ms lost: got=%q want=%s err=%v", tr.CreatedTime, want, err)
	}
}

func TestIngestLifecycleCanonicalEnvelope(t *testing.T) {
	c := &Client{}
	var settledTicker string
	var settledValue float64
	listings := 0
	structures := 0
	c.SetLifecycleHandler(func(ticker string, yes float64) {
		settledTicker, settledValue = ticker, yes
	})
	c.SetListingHandler(func(string, bool) { listings++ })
	c.SetStructureHandler(func(kind string, _ []byte) {
		if kind != "price_level_structure_updated" {
			t.Fatalf("wrong structure event kind %q", kind)
		}
		structures++
	})

	c.ingestLifecycle([]byte(`{"type":"market_lifecycle_v2","msg":{"event_type":"determined","market_ticker":"KXSCALAR","result":"yes","settlement_value":"0.1000"}}`))
	if settledTicker != "KXSCALAR" || math.Abs(settledValue-0.1) > 1e-9 {
		t.Fatalf("canonical settlement_value not honored: %q %.4f", settledTicker, settledValue)
	}

	c.ingestLifecycle([]byte(`{"type":"market_lifecycle_v2","msg":{"event_type":"price_level_structure_updated","market_ticker":"KXTICK","price_level_structure":"deci_cent"}}`))
	c.ingestLifecycle([]byte(`{"type":"market_lifecycle_v2","msg":{"event_type":"deactivated","market_ticker":"KXPAUSED","is_deactivated":true}}`))
	c.ingestLifecycle([]byte(`{"type":"market_lifecycle_v2","msg":{"event_type":"settled","market_ticker":"KXSETTLED","settled_ts":1700000000}}`))
	if structures != 1 || listings != 0 {
		t.Fatalf("lifecycle routing wrong: structures=%d listings=%d", structures, listings)
	}

	openTS := time.Now().Add(-time.Minute).Unix()
	c.ingestLifecycle([]byte(fmt.Sprintf(`{"type":"market_lifecycle_v2","msg":{"event_type":"created","market_ticker":"KXNEW","open_ts":%d}}`, openTS)))
	if listings != 1 {
		t.Fatalf("created event did not reach listing handler: %d", listings)
	}
}

func TestIngestPrivateAccountFixedPointFill(t *testing.T) {
	c := &Client{}
	var got Fill
	events := 0
	c.SetPrivateAccountHandlers(func(f Fill) { got = f }, func() { events++ })
	c.ingestPrivateAccount([]byte(`{"type":"fill","msg":{"trade_id":"tr-1","order_id":"ord-1","market_ticker":"KXTEST","outcome_side":"no","book_side":"ask","action":"buy","yes_price_dollars":"0.2700","count_fp":"1.25","fee_cost":"0.0123","is_taker":true,"ts_ms":1700000000123}}`))
	if got.FillID != "tr-1" || got.Ticker != "KXTEST" || got.SideYesNo() != "no" || !got.IsEntry() {
		t.Fatalf("private fill direction/id lost: %+v", got)
	}
	if math.Abs(got.Qty()-1.25) > 1e-9 || math.Abs(got.YesPriceUSD()-0.27) > 1e-9 || math.Abs(got.NoPriceUSD()-0.73) > 1e-9 || math.Abs(got.FeeUSD()-0.0123) > 1e-9 {
		t.Fatalf("private fill fixed-point values lost: %+v", got)
	}
	when, err := time.Parse(time.RFC3339Nano, got.CreatedTime)
	if err != nil || !when.Equal(time.UnixMilli(1700000000123).UTC()) {
		t.Fatalf("private fill ts_ms lost: %q err=%v", got.CreatedTime, err)
	}
	c.ingestPrivateAccount([]byte(`{"type":"user_order","msg":{"order_id":"ord-1","status":"canceled"}}`))
	if events != 2 {
		t.Fatalf("private event invalidations=%d, want 2", events)
	}
}
