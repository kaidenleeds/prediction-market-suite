package polymarketus

import (
	"testing"
	"time"
)

func TestR133FullBookTouchNeverPairsLiteBBOWithOldDepth(t *testing.T) {
	ws := &MarketsWS{live: map[string]*pusLive{}, restOpen: map[string]bool{"slug": true}, restOpenAt: time.Now()}
	ws.ingest([]byte(`{"marketData":{"marketSlug":"slug","state":"OPEN","bids":[{"px":"0.40","qty":"12"}],"offers":[{"px":"0.42","qty":"7"}]}}`))
	bid, ask, bd, ad, _, ok := ws.FullBookTouchAt("slug")
	if !ok || bid != .40 || ask != .42 || bd != 12 || ad != 7 {
		t.Fatalf("full touch=(%v,%v,%v,%v) ok=%v", bid, ask, bd, ad, ok)
	}
	ws.ingest([]byte(`{"market_data_lite":{"market_slug":"slug","best_bid":"0.50","best_ask":"0.52"}}`))
	bid, ask, bd, ad, _, ok = ws.FullBookTouchAt("slug")
	if !ok || bid != .40 || ask != .42 || bd != 12 || ad != 7 {
		t.Fatalf("LITE must not relabel old full depth: (%v,%v,%v,%v) ok=%v", bid, ask, bd, ad, ok)
	}
}

func TestR139FullBookLevelsPreserveEveryExecutableLevelAndSourceClock(t *testing.T) {
	ws := &MarketsWS{live: map[string]*pusLive{}, restOpen: map[string]bool{"slug": true}, restOpenAt: time.Now()}
	ws.ingest([]byte(`{"marketData":{"marketSlug":"slug","state":"OPEN","transactTime":"2026-07-12T12:34:56Z","bids":[{"px":"0.40","qty":"12"},{"px":"0.39","qty":"8"}],"offers":[{"px":"0.42","qty":"7"},{"px":"0.44","qty":"9"}]}}`))
	bids, asks, sourceAt, receivedAt, ok := ws.FullBookLevelsAt("slug")
	if !ok || len(bids) != 2 || len(asks) != 2 || bids[0].Price != .40 || bids[1].Quantity != 8 ||
		asks[0].Price != .42 || asks[1].Quantity != 9 || sourceAt.IsZero() || receivedAt.IsZero() {
		t.Fatalf("full levels bids=%+v asks=%+v source=%v received=%v ok=%v", bids, asks, sourceAt, receivedAt, ok)
	}
}
