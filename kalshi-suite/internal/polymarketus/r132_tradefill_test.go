package polymarketus

import (
	"testing"
	"time"
)

func TestR132TradeThroughRequiresOppositeAggressor(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	since := time.Now().Add(-time.Second)
	ws.ingest([]byte(`{"trade":{"market_slug":"m","price":{"value":"0.40"},"quantity":{"value":"10"},"taker":{"intent":"ORDER_INTENT_BUY_SHORT"}}}`))
	if ws.TradeThrough("m", "YES", 0.40, since) {
		t.Fatal("at-level print cannot prove a queue-unknown resting YES bid filled")
	}
	if ws.TradeThrough("m", "NO", 0.60, since) {
		t.Fatal("same-side NO aggressor cannot fill a resting NO bid")
	}
	ws.ingest([]byte(`{"trade":{"market_slug":"m","price":{"value":"0.39"},"quantity":{"value":"10"},"taker":{"intent":"ORDER_INTENT_BUY_SHORT"}}}`))
	if !ws.TradeThrough("m", "YES", 0.40, since) {
		t.Fatal("opposite aggressor through the level must prove the resting YES bid filled")
	}
	ws.ingest([]byte(`{"trade":{"market_slug":"n","price":{"value":"0.41"},"quantity":{"value":"5"},"taker":{"intent":"ORDER_INTENT_BUY_LONG"}}}`))
	if !ws.TradeThrough("n", "NO", 0.60, since) {
		t.Fatal("aggressive YES at 41c YES (=59c NO) must prove a resting 60c NO bid traded through")
	}
	if ws.TradeThrough("n", "YES", 0.40, time.Now().Add(time.Second)) {
		t.Fatal("prints before the post timestamp must not fill it")
	}
}
