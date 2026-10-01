package server

import (
	"context"
	"testing"
)

func TestR132NoSyntheticEntryQuoteFallback(t *testing.T) {
	s := testServer(t)
	if got := s.entryAsk(context.Background(), "kalshi", "NO-BOOK", "YES", 0.40); got != 0 {
		t.Fatalf("missing ask returned %.4f; synthetic signal+spread quotes are forbidden", got)
	}
	if got := s.entryBid(context.Background(), "kalshi", "NO-BOOK", "YES", 0.40); got != 0 {
		t.Fatalf("missing bid returned %.4f; synthetic signal-spread posts are forbidden", got)
	}
}

func TestR132TakerSizeCannotExceedTouchDepth(t *testing.T) {
	for _, tc := range []struct {
		desired, depth, want float64
	}{
		{10, 3.9, 3},
		{2, 8, 2},
		{1, 0.99, 0},
		{0, 20, 0},
	} {
		if got := executableContracts(tc.desired, tc.depth); got != tc.want {
			t.Fatalf("executableContracts(%v,%v)=%v, want %v", tc.desired, tc.depth, got, tc.want)
		}
	}
}

func TestR132QueueClassificationUsesVenueTick(t *testing.T) {
	if got := queueTradeClass(0.499, 0.500, 0.001); got != queueTradeThrough {
		t.Fatalf("one 0.1-cent tick through classified %d, want through", got)
	}
	if got := queueTradeClass(0.500, 0.500, 0.001); got != queueTradeAt {
		t.Fatalf("at-level sub-cent print classified %d, want at", got)
	}
	if got := queueTradeClass(0.0546, 0.055, 0.001); got != queueTradeThrough {
		t.Fatalf("off-grid robustness print below post classified %d, want through", got)
	}
	if got := queueTradeClass(0.49, 0.50, 0.01); got != queueTradeThrough {
		t.Fatalf("one cent tick through classified %d, want through", got)
	}
}

func TestR132PolyUSLifecycleUsesExactTokens(t *testing.T) {
	for _, raw := range []string{"OPEN", "ACTIVE", "MARKET_STATE_OPEN", "market_status_active"} {
		if !polyUSLifecycleOpen(raw) {
			t.Fatalf("%q should be executable", raw)
		}
	}
	for _, raw := range []string{"", "PREOPEN", "MARKET_STATE_PREOPEN", "HALTED", "REOPEN_PENDING", "CLOSED"} {
		if polyUSLifecycleOpen(raw) {
			t.Fatalf("%q must not pass the exact lifecycle gate", raw)
		}
	}
}
