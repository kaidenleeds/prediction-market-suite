package kalshi

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestR133TickerKeepsExecutableBBOSeparateFromMid(t *testing.T) {
	c := &Client{}
	c.ingestTicker([]byte(fmt.Sprintf(`{"type":"ticker","msg":{"market_ticker":"KXR133-BBO","yes_bid_dollars":"0.41","yes_ask_dollars":"0.47","price_dollars":"0.45","ts_ms":%d}}`, time.Now().UnixMilli())))

	mark, ok := c.LivePrice("KXR133-BBO")
	if !ok || math.Abs(mark-0.44) > 1e-9 {
		t.Fatalf("mid mark = %v, %v; want 0.44, true", mark, ok)
	}
	bid, ask, ok := c.LiveBidAsk("KXR133-BBO")
	if !ok || math.Abs(bid-0.41) > 1e-9 || math.Abs(ask-0.47) > 1e-9 {
		t.Fatalf("BBO = %v/%v, %v; want 0.41/0.47, true", bid, ask, ok)
	}
}

func TestR133TickerBBORefusesOneSidedOrLastTradeFallback(t *testing.T) {
	c := &Client{}
	c.ingestTicker([]byte(fmt.Sprintf(`{"type":"ticker","msg":{"market_ticker":"KXR133-LAST","price_dollars":"0.45","ts_ms":%d}}`, time.Now().UnixMilli())))
	if mark, ok := c.LivePrice("KXR133-LAST"); !ok || math.Abs(mark-0.45) > 1e-9 {
		t.Fatalf("last-trade mark = %v, %v; want 0.45, true", mark, ok)
	}
	if bid, ask, ok := c.LiveBidAsk("KXR133-LAST"); ok || bid != 0 || ask != 0 {
		t.Fatalf("non-executable BBO = %v/%v, %v; want refused", bid, ask, ok)
	}
}
