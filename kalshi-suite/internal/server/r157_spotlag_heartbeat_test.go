package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR157SpotlagOnlyAuthorityRequiresExactProducer(t *testing.T) {
	routes := []string{
		"kalshi|spotlag|NO|taker",
		"kalshi|spotlag|YES|taker",
	}
	required := signalProducerRequirementsForRoutes(routes, []string{"kalshi"})
	if len(required) != 1 || required[0] != "spotlag" {
		t.Fatalf("Spot-lag-only requirements=%v, want exact spotlag heartbeat", required)
	}

	now := time.Date(2026, 7, 18, 4, 0, 0, 0, time.UTC)
	receipts := map[string]signalProducerReceipt{
		"kalshi":  {Completed: now.Add(-10 * time.Minute), Errors: 1},
		"spotlag": {Completed: now.Add(-time.Second)},
	}
	if ok, detail := signalProducersClassify(now, 3*time.Minute, required, receipts); !ok {
		t.Fatalf("fresh exact Spot-lag receipt was masked by unrelated Kalshi producer: %s", detail)
	}
	delete(receipts, "spotlag")
	if ok, detail := signalProducersClassify(now, 3*time.Minute, required, receipts); ok ||
		!strings.Contains(detail, "spotlag=warming") {
		t.Fatalf("missing exact Spot-lag heartbeat passed: ok=%v detail=%s", ok, detail)
	}
}

func TestR157ProducerSnapshotClockCannotPrecedeCopiedReceipt(t *testing.T) {
	s := &Server{}
	completed := time.Now()
	s.finishSignalProducer("spotlag", completed, 0, 0)
	receipts, snapshotAt := s.signalProducerSnapshotAt()
	receipt := receipts["spotlag"]
	if snapshotAt.Before(receipt.Completed) {
		t.Fatalf("snapshot clock %v precedes copied receipt %v", snapshotAt, receipt.Completed)
	}
	if ok, detail := signalProducersClassify(snapshotAt, 3*time.Minute,
		[]string{"spotlag"}, receipts); !ok {
		t.Fatalf("same-snapshot fresh receipt classified unhealthy: %s", detail)
	}
}

func TestR157MixedOrMultiVenueAuthorityKeepsVenueProducerFallback(t *testing.T) {
	tests := []struct {
		name     string
		routes   []string
		fallback []string
		want     []string
	}{
		{
			name: "mixed Kalshi families",
			routes: []string{
				"kalshi|spotlag|YES|taker",
				"kalshi|kalshi-flow|YES|taker",
			},
			fallback: []string{"kalshi"},
			want:     []string{"kalshi"},
		},
		{
			name: "second execution venue",
			routes: []string{
				"kalshi|spotlag|YES|taker",
				"polyus|polyus-flow|YES|taker",
			},
			fallback: []string{"kalshi", "polyus"},
			want:     []string{"kalshi", "polyus"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := signalProducerRequirementsForRoutes(tc.routes, tc.fallback)
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("requirements=%v want %v", got, tc.want)
			}
		})
	}
}

func TestR157SpotlagZeroOpportunityHeartbeatIsHealthy(t *testing.T) {
	called := false
	attempts, errorsN := runSpotlagDetectorPass(context.Background(), nil,
		func(context.Context, string) (float64, float64, time.Time, bool) {
			called = true
			return 0, 0, time.Time{}, false
		}, nil, nil, nil)
	if called || attempts != 0 || errorsN != 0 {
		t.Fatalf("ordinary no-market gap was not healthy: called=%v attempts=%d errors=%d",
			called, attempts, errorsN)
	}

	market := []spotlagCachedMarket{{Coin: "btc", Ticker: "KXBTC15M-R157", Title: "BTC up", Up: .42}}
	at := time.Date(2026, 7, 18, 4, 0, 0, 0, time.UTC)
	inserted := 0
	attempts, errorsN = runSpotlagDetectorPass(context.Background(), market,
		func(context.Context, string) (float64, float64, time.Time, bool) {
			return 64000, .01, at, true
		},
		func(string) (float64, bool) { return .02, true },
		func(string) (float64, bool) { return .005, true },
		func(context.Context, storage.Signal) error {
			inserted++
			return nil
		})
	if attempts != 0 || errorsN != 0 || inserted != 0 {
		t.Fatalf("threshold miss was not a healthy zero opportunity: attempts=%d errors=%d inserted=%d",
			attempts, errorsN, inserted)
	}
}

func TestR157SpotlagPassReceiptsRealSourceAndInsertFailures(t *testing.T) {
	market := []spotlagCachedMarket{{Coin: "btc", Ticker: "KXBTC15M-R157", Title: "BTC up", Up: .42}}
	at := time.Date(2026, 7, 18, 4, 0, 0, 0, time.UTC)
	sigma := func(string) (float64, bool) { return .02, true }
	momentum := func(string) (float64, bool) { return .005, true }

	var got storage.Signal
	attempts, errorsN := runSpotlagDetectorPass(context.Background(), market,
		func(context.Context, string) (float64, float64, time.Time, bool) {
			return 64000, .04, at, true
		}, sigma, momentum, func(_ context.Context, signal storage.Signal) error {
			got = signal
			return nil
		})
	if attempts != 1 || errorsN != 0 || got.SignalType != "spotlag" ||
		got.Side != "YES" || got.ExecExpr == "" {
		t.Fatalf("valid detector pass receipt=(%d,%d) signal=%+v", attempts, errorsN, got)
	}

	attempts, errorsN = runSpotlagDetectorPass(context.Background(), market,
		func(context.Context, string) (float64, float64, time.Time, bool) {
			return 0, 0, time.Time{}, false
		}, sigma, momentum, nil)
	if attempts != 0 || errorsN != 1 {
		t.Fatalf("source failure receipt=(%d,%d), want (0,1)", attempts, errorsN)
	}

	partial := []spotlagCachedMarket{
		{Coin: "btc", Ticker: "KXBTC15M-R157", Title: "BTC up", Up: .42},
		{Coin: "doge", Ticker: "KXDOGE15M-R157", Title: "DOGE up", Up: .42},
	}
	attempts, errorsN = runSpotlagDetectorPass(context.Background(), partial,
		func(_ context.Context, coin string) (float64, float64, time.Time, bool) {
			if coin == "doge" {
				return 0, 0, time.Time{}, false
			}
			return 64000, .01, at, true
		}, sigma, momentum, func(context.Context, storage.Signal) error {
			return errors.New("quiet input should not emit")
		})
	if attempts != 0 || errorsN != 0 {
		t.Fatalf("one quiet coin paused other fresh inputs: attempts=%d errors=%d", attempts, errorsN)
	}

	attempts, errorsN = runSpotlagDetectorPass(context.Background(), market,
		func(context.Context, string) (float64, float64, time.Time, bool) {
			return 64000, .04, at, true
		}, sigma, momentum, func(context.Context, storage.Signal) error {
			return errors.New("sqlite unavailable")
		})
	if attempts != 1 || errorsN != 1 {
		t.Fatalf("insert failure receipt=(%d,%d), want (1,1)", attempts, errorsN)
	}
}

func resetSpotCacheForR157(t *testing.T) {
	t.Helper()
	spotMu.Lock()
	prior := spotCache
	priorLive := coinbaseSpotLiveCache
	spotCache = map[string]spotEntry{}
	coinbaseSpotLiveCache = map[string]spotEntry{}
	spotMu.Unlock()
	t.Cleanup(func() {
		spotMu.Lock()
		spotCache = prior
		coinbaseSpotLiveCache = priorLive
		spotMu.Unlock()
	})
}

func TestR157SpotlagAcceptsOnlyFreshCoinbaseWSSourceTime(t *testing.T) {
	resetSpotCacheForR157(t)
	now := time.Now().UTC()
	if ingestCoinbaseSpotTick("BTC-USD", 64000,
		now.Add(-coinbaseSpotWSMaxSourceLag-time.Nanosecond), now) {
		t.Fatal("stale exchange source time entered the Spot-lag cache")
	}
	if ingestCoinbaseSpotTick("BTC-USD", 64000,
		now.Add(coinbaseSpotWSFutureSkew+time.Nanosecond), now) {
		t.Fatal("future-skewed exchange source time entered the Spot-lag cache")
	}
	sourceAt := now.Add(-time.Second)
	if !ingestCoinbaseSpotTick("BTC-USD", 64000, sourceAt, now) {
		t.Fatal("fresh exchange ticker was refused")
	}
	if price, _, gotAt, ok := coinbaseSpotForSpotlag("btc", now); !ok ||
		price != 64000 || !gotAt.Equal(sourceAt) {
		t.Fatalf("fresh WS read=(%v,%v,%v), want 64000,%v,true", price, gotAt, ok, sourceAt)
	}
	if _, _, _, ok := coinbaseSpotForSpotlag("btc",
		now.Add(coinbaseSpotWSMaxAge-time.Millisecond)); !ok {
		t.Fatal("host/exchange clock offset silently shortened the local five-second freshness window")
	}
	if _, _, _, ok := coinbaseSpotForSpotlag("btc",
		now.Add(coinbaseSpotWSMaxAge+time.Nanosecond)); ok {
		t.Fatal("aged WS price remained LIVE-authorizing")
	}
	delayedSourceAt := now.Add(-coinbaseSpotWSMaxSourceLag)
	if !ingestCoinbaseSpotTick("ETH-USD", 3200, delayedSourceAt, now) {
		t.Fatal("source at the accepted ingest-lag boundary was refused")
	}
	if _, _, _, ok := coinbaseSpotForSpotlag("eth",
		now.Add(coinbaseSpotWSMaxAge-time.Millisecond)); !ok {
		t.Fatal("accepted source lag silently shortened the local five-second freshness window")
	}
	if _, _, _, ok := coinbaseSpotForSpotlag("eth",
		now.Add(coinbaseSpotWSMaxAge+time.Nanosecond)); ok {
		t.Fatal("local receive age bypassed the bounded end-to-end source age")
	}
	// A slower research REST refresh shares spotCache but must not erase the independent LIVE
	// WebSocket observation or its source label.
	spotMu.Lock()
	entry := spotCache["BTC"]
	entry.price, entry.at, entry.receivedAt, entry.source =
		65000, now.Add(time.Second), now.Add(time.Second), "coinbase-rest"
	spotCache["BTC"] = entry
	spotMu.Unlock()
	if price, _, gotAt, ok := coinbaseSpotForSpotlag("btc", now); !ok ||
		price != 64000 || !gotAt.Equal(sourceAt) {
		t.Fatalf("research cache poisoned LIVE WS read=(%v,%v,%v)", price, gotAt, ok)
	}
	spotMu.Lock()
	entry = coinbaseSpotLiveCache["BTC"]
	entry.source = "coinbase-rest"
	coinbaseSpotLiveCache["BTC"] = entry
	spotMu.Unlock()
	if _, _, _, ok := coinbaseSpotForSpotlag("btc", now); ok {
		t.Fatal("REST fallback was mislabeled as the one-second Spot-lag feed")
	}
}

func TestR157CoinbaseWSKeepsTwentySecondSigmaSamplingCadence(t *testing.T) {
	resetSpotCacheForR157(t)
	start := time.Now().UTC()
	if !ingestCoinbaseSpotTick("ETH-USD", 3000, start, start) {
		t.Fatal("first ticker was refused")
	}
	for second := 1; second < int(coinbaseSpotSampleCadence/time.Second); second++ {
		at := start.Add(time.Duration(second) * time.Second)
		if !ingestCoinbaseSpotTick("ETH-USD", 3000+float64(second), at, at) {
			t.Fatalf("fresh ticker at second %d was refused", second)
		}
	}
	spotMu.Lock()
	entry := coinbaseSpotLiveCache["ETH"]
	spotMu.Unlock()
	if len(entry.samples) != 1 || entry.price != 3019 {
		t.Fatalf("every trade changed the proven sigma ring: samples=%d latest=%v",
			len(entry.samples), entry.price)
	}
	at := start.Add(coinbaseSpotSampleCadence)
	if !ingestCoinbaseSpotTick("ETH-USD", 3020, at, at) {
		t.Fatal("cadence-boundary ticker was refused")
	}
	spotMu.Lock()
	entry = coinbaseSpotLiveCache["ETH"]
	spotMu.Unlock()
	if len(entry.samples) != 2 || !entry.samples[1].at.Equal(at) {
		t.Fatalf("downsampled ring=%+v, want second sample at %v", entry.samples, at)
	}
}

func TestR157CoinbaseWSSameTimestampUsesTradeIDOrder(t *testing.T) {
	resetSpotCacheForR157(t)
	sourceAt := time.Now().UTC().Add(-time.Second)
	firstReceived := time.Now().UTC()
	if !ingestCoinbaseSpotTrade("BTC-USD", 64000, 100, sourceAt, firstReceived) {
		t.Fatal("first timestamped trade was refused")
	}
	if !ingestCoinbaseSpotTrade("BTC-USD", 64001, 101, sourceAt,
		firstReceived.Add(time.Millisecond)) {
		t.Fatal("later trade with the same Coinbase timestamp was refused")
	}
	if ingestCoinbaseSpotTrade("BTC-USD", 63999, 100, sourceAt,
		firstReceived.Add(2*time.Millisecond)) {
		t.Fatal("older same-timestamp trade replay refreshed the LIVE record")
	}
	price, _, gotSourceAt, ok := coinbaseSpotForSpotlag("btc",
		firstReceived.Add(3*time.Millisecond))
	if !ok || price != 64001 || !gotSourceAt.Equal(sourceAt) {
		t.Fatalf("ordered same-timestamp price=(%v,%v,%v), want 64001,%v,true",
			price, gotSourceAt, ok, sourceAt)
	}
}
