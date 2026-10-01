package server

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR142WalletSkillCanceledRefreshPreservesLastGoodAndBacksOff(t *testing.T) {
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	want := &storage.WalletSkill{Wallet: "0xlastgood", Markets: 12, Shrunk: .04}
	s := &Server{store: st, skillMap: map[string]*storage.WalletSkill{want.Wallet: want},
		skillAt: time.Now().Add(-11 * time.Minute)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := s.walletSkills(ctx)
	if got[want.Wallet] != want {
		t.Fatalf("canceled refresh discarded last-good skill map: %+v", got)
	}
	s.skillMu.Lock()
	loading, retryAt := s.skillLoading, s.skillRetryAt
	s.skillMu.Unlock()
	if loading || !retryAt.After(time.Now().Add(9*time.Minute)) {
		t.Fatalf("canceled refresh loading=%v retry_at=%v; want stopped with bounded backoff", loading, retryAt)
	}
	// A caller inside the retry window must get the same cache without reopening the aggregate.
	got = s.walletSkills(ctx)
	if got[want.Wallet] != want {
		t.Fatalf("retry backoff discarded last-good skill map: %+v", got)
	}
}
