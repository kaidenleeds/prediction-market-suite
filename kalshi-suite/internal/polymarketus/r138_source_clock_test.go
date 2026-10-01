package polymarketus

import (
	"fmt"
	"testing"
	"time"
)

func TestR138FullAndLiteSourceClocksRemainSeparate(t *testing.T) {
	ws := &MarketsWS{live: map[string]*pusLive{}}
	want := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	ws.ingest([]byte(fmt.Sprintf(`{"marketData":{"marketSlug":"full","transactTime":%q,
		"state":"MARKET_STATE_OPEN","bids":[{"px":"0.40","qty":"2"}],
		"offers":[{"px":"0.42","qty":"2"}]}}`, want.Format(time.RFC3339Nano))))
	got, fullFrames := ws.FullSourceClock()
	if fullFrames != 1 || !got.Equal(want) {
		t.Fatalf("full clock frames=%d at=%s, want 1/%s", fullFrames, got, want)
	}
	ws.ingest([]byte(`{"marketDataLite":{"marketSlug":"lite","bestBid":"0.30","bestAsk":"0.32"}}`))
	arrival, liteFrames := ws.LiteArrivalClock()
	if liteFrames != 1 || arrival.IsZero() {
		t.Fatalf("lite arrival clock frames=%d at=%s", liteFrames, arrival)
	}
	got, fullFrames = ws.FullSourceClock()
	if fullFrames != 1 || !got.Equal(want) {
		t.Fatalf("LITE altered full source clock: frames=%d at=%s", fullFrames, got)
	}
}
