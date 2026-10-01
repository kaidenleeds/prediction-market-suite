package polymarketus

import (
	"testing"
	"time"
)

func TestRawFlowRetainsTinyValidPolyUSPrintWithoutDollarFloor(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	ws.ingest([]byte(`{"trade":{"market_slug":"tiny-pus","price":0.01,"quantity":0.1,"taker":{"intent":"ORDER_INTENT_BUY_LONG"}}}`))
	rows := ws.RawFlow(time.Minute)
	if len(rows) != 1 {
		t.Fatalf("tiny valid print was filtered from raw flow: n=%d", len(rows))
	}
	if rows[0].Notional <= 0 || rows[0].Notional >= 0.01 || rows[0].Side != "YES" {
		t.Fatalf("unexpected tiny raw row: %+v", rows[0])
	}
	yes, no, n := ws.TakerStats("tiny-pus", time.Minute)
	if n != 1 || yes <= 0 || no != 0 {
		t.Fatalf("tiny print missing from aggregate: yes=%g no=%g n=%d", yes, no, n)
	}
}

func TestRawFlowRejectsInvalidPolyUSPrintsOnly(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	ws.ingest([]byte(`{"trade":{"market_slug":"zero-pus","price":0.50,"quantity":0,"taker":{"intent":"ORDER_INTENT_BUY_LONG"}}}`))
	if rows := ws.RawFlow(time.Minute); len(rows) != 0 {
		t.Fatalf("invalid zero print entered raw flow: %+v", rows)
	}
}
