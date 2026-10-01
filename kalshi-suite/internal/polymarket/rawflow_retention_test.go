package polymarket

import (
	"fmt"
	"testing"
	"time"
)

func TestRawFlowRetainsTinyValidPolyIntPrintWithoutDollarFloor(t *testing.T) {
	c := NewClient(time.Second)
	msg := fmt.Sprintf(`{"topic":"activity","type":"trades","payload":{"proxyWallet":"0xabc","conditionId":"tiny-int","side":"BUY","size":0.1,"price":0.02,"timestamp":%d,"title":"Tiny","outcome":"Yes","outcomeIndex":0}}`, time.Now().Unix())
	c.ingestLiveMessage([]byte(msg))
	rows := c.LiveTrades()
	if len(rows) != 1 {
		t.Fatalf("tiny valid print was filtered from raw RTDS tape: n=%d", len(rows))
	}
	if got := rows[0].Size * rows[0].Price; got <= 0 || got >= 0.01 {
		t.Fatalf("expected retained sub-cent notional, got $%.6f", got)
	}
}

func TestRawFlowRejectsInvalidPolyIntPrintsOnly(t *testing.T) {
	c := NewClient(time.Second)
	msg := fmt.Sprintf(`{"topic":"activity","type":"trades","payload":{"proxyWallet":"0xabc","conditionId":"zero-int","side":"BUY","size":0,"price":0.02,"timestamp":%d,"title":"Zero","outcome":"Yes"}}`, time.Now().Unix())
	c.ingestLiveMessage([]byte(msg))
	if rows := c.LiveTrades(); len(rows) != 0 {
		t.Fatalf("invalid zero print entered raw RTDS tape: %+v", rows)
	}
}
