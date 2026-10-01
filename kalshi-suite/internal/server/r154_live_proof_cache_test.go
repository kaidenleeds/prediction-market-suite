package server

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR154RouteProofCacheReusesOnlySuccessfulShortLivedHistory(t *testing.T) {
	var cache r154LiveRouteProofCache
	now := time.Now()
	key := r154LiveRouteProofKey("kalshi-flow", "kalshi", "strategy", "YES")
	calls := 0
	load := func() (storage.UnitTrialLeaderboardStat, bool, error) {
		calls++
		return storage.UnitTrialLeaderboardStat{SettledMarkets: 77, MeanPC: .04}, true, nil
	}
	first, found, err := cache.get(context.Background(), now, key, load)
	if err != nil || !found || first.SettledMarkets != 77 {
		t.Fatalf("first cache load=(%+v,%v,%v)", first, found, err)
	}
	second, found, err := cache.get(context.Background(), now.Add(time.Second), key, load)
	if err != nil || !found || second.SettledMarkets != 77 || calls != 1 {
		t.Fatalf("cached load=(%+v,%v,%v) calls=%d", second, found, err, calls)
	}
	_, _, err = cache.get(context.Background(),
		now.Add(r154LiveRouteProofCacheMaxAge+time.Millisecond), key, load)
	if err != nil || calls != 2 {
		t.Fatalf("expired cache err=%v calls=%d", err, calls)
	}
}

func TestR154RouteProofCacheDoesNotCacheReadFailure(t *testing.T) {
	var cache r154LiveRouteProofCache
	now := time.Now()
	key := r154LiveRouteProofKey("spotlag", "kalshi", "strategy", "NO")
	calls := 0
	load := func() (storage.UnitTrialLeaderboardStat, bool, error) {
		calls++
		if calls == 1 {
			return storage.UnitTrialLeaderboardStat{}, false, errors.New("temporary read failure")
		}
		return storage.UnitTrialLeaderboardStat{SettledMarkets: 80}, true, nil
	}
	if _, _, err := cache.get(context.Background(), now, key, load); err == nil {
		t.Fatal("first read failure unexpectedly succeeded")
	}
	stat, found, err := cache.get(context.Background(), now, key, load)
	if err != nil || !found || stat.SettledMarkets != 80 || calls != 2 {
		t.Fatalf("retry=(%+v,%v,%v) calls=%d", stat, found, err, calls)
	}
}

func TestR154RouteProofCacheInvalidationFencesInflightSettlementGeneration(t *testing.T) {
	var cache r154LiveRouteProofCache
	now := time.Now()
	key := r154LiveRouteProofKey("spotlag", "kalshi", "strategy", "YES")
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	result := make(chan storage.UnitTrialLeaderboardStat, 1)
	go func() {
		stat, _, _ := cache.get(context.Background(), now, key, func() (
			storage.UnitTrialLeaderboardStat, bool, error) {
			if calls.Add(1) == 1 {
				close(started)
				<-release
				return storage.UnitTrialLeaderboardStat{MeanPC: .05}, true, nil
			}
			return storage.UnitTrialLeaderboardStat{MeanPC: -.03}, true, nil
		})
		result <- stat
	}()
	<-started
	cache.invalidate()
	close(release)
	if got := <-result; got.MeanPC != -.03 || calls.Load() != 2 {
		t.Fatalf("generation-fenced proof=%+v loads=%d", got, calls.Load())
	}
	if got, _, err := cache.get(context.Background(), now.Add(time.Millisecond), key, func() (
		storage.UnitTrialLeaderboardStat, bool, error) {
		t.Fatal("invalidated generation was not cached after reload")
		return storage.UnitTrialLeaderboardStat{}, false, nil
	}); err != nil || got.MeanPC != -.03 {
		t.Fatalf("post-invalidation cache=%+v err=%v", got, err)
	}
}
