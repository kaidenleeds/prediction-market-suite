package kalshi

import (
	"testing"
	"time"
)

func TestPublicTradeHandlerReceivesNormalizedTradeOutsideClientLock(t *testing.T) {
	c := &Client{}
	got := make(chan Trade, 1)
	c.SetPublicTradeHandler(func(trade Trade) {
		// This takes txMu internally. If ingest invokes the callback while holding txMu, the test
		// deadlocks instead of receiving the normalized observation.
		_, _ = c.LiveTape()
		got <- trade
	})
	c.ingestTrade([]byte(`{
		"type":"trade","sid":7,"seq":9,
		"msg":{"trade_id":"trade-1","market_ticker":"KXTEST-YES","count_fp":"3",
		"yes_price_dollars":"0.42","no_price_dollars":"0.58",
		"taker_outcome_side":"yes","taker_book_side":"bid","ts_ms":1784908800123}
	}`))
	select {
	case trade := <-got:
		if trade.TradeID != "trade-1" || trade.Ticker != "KXTEST-YES" ||
			trade.Count.Float() != 3 || trade.YesPrice.Float() != .42 ||
			trade.NoPrice.Float() != .58 || trade.Aggressor() != "yes" {
			t.Fatalf("normalized trade=%+v", trade)
		}
	case <-time.After(time.Second):
		t.Fatal("public trade callback blocked or was not invoked")
	}
}
