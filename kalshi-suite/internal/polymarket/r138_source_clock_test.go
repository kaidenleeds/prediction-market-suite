package polymarket

import (
	"fmt"
	"testing"
	"time"
)

func TestR138CLOBSourceClockRequiresExplicitVenueTimestamp(t *testing.T) {
	c := NewClient(time.Second)
	want := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	c.ingestCLOBMarket([]byte(fmt.Sprintf(`{"event_type":"book","market":"condition","asset_id":"token",
		"timestamp":%q,"bids":[{"price":"0.40","size":"2"}],"asks":[{"price":"0.42","size":"2"}]}`,
		fmt.Sprint(want.UnixMilli()))), nil)
	got, rows, connected := c.CLOBSourceClock()
	if !connected || rows != 1 || !got.Equal(want) {
		t.Fatalf("source clock connected=%v rows=%d at=%s, want true/1/%s", connected, rows, got, want)
	}
	// A newer frame without the documented timestamp may remain a compatibility quote, but cannot
	// claim a current source watermark.
	c.ingestCLOBMarket([]byte(`{"event_type":"best_bid_ask","market":"condition","asset_id":"token",
		"best_bid":"0.41","best_ask":"0.43"}`), nil)
	if got, rows, _ := c.CLOBSourceClock(); rows != 0 || !got.IsZero() {
		t.Fatalf("missing timestamp fabricated CLOB source clock rows=%d at=%s", rows, got)
	}
}
