package kalshi

import "testing"

func TestR150MarketCachedReadsCompleteBoardWithoutWarmTapeCache(t *testing.T) {
	want := Market{Ticker: "KX-R150-FULL-BOARD", Status: "active", ExpectedExpiration: "2026-07-16T23:00:00Z"}
	c := &Client{mktCache: []Market{{Ticker: "OTHER"}, want}}

	got, ok := c.MarketCached("  kx-r150-full-board ")
	if !ok || got.Ticker != want.Ticker || got.ExpectedExpiration != want.ExpectedExpiration {
		t.Fatalf("complete-board lookup got=%+v ok=%v", got, ok)
	}
	if c.mktRefreshing {
		t.Fatal("cache-only execution lookup must not start a board REST refresh")
	}
	if _, ok := c.MarketCached("MISSING"); ok {
		t.Fatal("unknown ticker must remain unavailable instead of fabricating market metadata")
	}
	if _, ok := c.MarketCached(" "); ok {
		t.Fatal("blank ticker must remain unavailable")
	}
}
