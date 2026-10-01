package kalshi

import (
	"testing"
	"time"
)

func TestR158CompleteBoardSnapshotReturnsSuccessfulCacheReceiptWithoutIO(t *testing.T) {
	observedAt := time.Date(2026, 7, 18, 13, 0, 0, 123, time.UTC)
	client := &Client{
		mktCache:   []Market{{Ticker: "KXBTC15M-R158", Status: "active"}},
		mktCacheAt: observedAt,
	}

	markets, gotAt := client.CompleteBoardSnapshot()
	if len(markets) != 1 || markets[0].Ticker != "KXBTC15M-R158" ||
		!gotAt.Equal(observedAt) {
		t.Fatalf("snapshot markets=%+v observed_at=%v, want exact published cache receipt",
			markets, gotAt)
	}
}

func TestR158CompleteBoardSnapshotNilClientFailsClosed(t *testing.T) {
	var client *Client
	markets, observedAt := client.CompleteBoardSnapshot()
	if markets != nil || !observedAt.IsZero() {
		t.Fatalf("nil client snapshot=%+v observed_at=%v, want missing receipt",
			markets, observedAt)
	}
}
