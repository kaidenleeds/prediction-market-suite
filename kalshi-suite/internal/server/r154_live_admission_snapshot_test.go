package server

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR158KalshiAdmissionSnapshotLeaseIsOneSecond(t *testing.T) {
	if r154KalshiAdmissionSnapshotRefreshMaxAge != time.Second {
		t.Fatalf("account snapshot lease = %s; want 1s unchanged-account lease",
			r154KalshiAdmissionSnapshotRefreshMaxAge)
	}
}

type r154AdmissionSnapshotFetcherFake struct {
	mu sync.Mutex

	balanceCalls   int
	positionsCalls int
	ordersCalls    int

	balanceErr   error
	positionsErr error
	ordersErr    error

	balance   int64
	portfolio int64
	positions []kalshi.MarketPosition
	orders    []kalshi.Order

	gate    <-chan struct{}
	started chan<- string
}

func (f *r154AdmissionSnapshotFetcherFake) wait(ctx context.Context, resource string) error {
	f.mu.Lock()
	gate := f.gate
	started := f.started
	f.mu.Unlock()
	if started != nil {
		started <- resource
	}
	if gate == nil {
		return nil
	}
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *r154AdmissionSnapshotFetcherFake) GetBalance(ctx context.Context) (*kalshi.Balance, error) {
	f.mu.Lock()
	f.balanceCalls++
	f.mu.Unlock()
	if err := f.wait(ctx, "balance"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.balanceErr != nil {
		return nil, f.balanceErr
	}
	portfolio := f.portfolio
	return &kalshi.Balance{Balance: f.balance, PortfolioValue: &portfolio}, nil
}

func (f *r154AdmissionSnapshotFetcherFake) GetPositions(ctx context.Context) ([]kalshi.MarketPosition, error) {
	f.mu.Lock()
	f.positionsCalls++
	f.mu.Unlock()
	if err := f.wait(ctx, "positions"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.positionsErr != nil {
		return nil, f.positionsErr
	}
	return append([]kalshi.MarketPosition(nil), f.positions...), nil
}

func (f *r154AdmissionSnapshotFetcherFake) GetOrders(ctx context.Context) ([]kalshi.Order, error) {
	f.mu.Lock()
	f.ordersCalls++
	f.mu.Unlock()
	if err := f.wait(ctx, "orders"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ordersErr != nil {
		return nil, f.ordersErr
	}
	return append([]kalshi.Order(nil), f.orders...), nil
}

func (f *r154AdmissionSnapshotFetcherFake) calls() (int, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.balanceCalls, f.positionsCalls, f.ordersCalls
}

func TestR154KalshiAdmissionSnapshotConcurrentSingleflightAndFreshCache(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan string, 32)
	fetcher := &r154AdmissionSnapshotFetcherFake{
		balance:   40_000,
		portfolio: 1_234,
		positions: []kalshi.MarketPosition{{Ticker: "POS-1"}},
		orders:    []kalshi.Order{{Ticker: "ORDER-1"}},
		gate:      gate,
		started:   started,
	}
	now := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	var clockMu sync.Mutex
	clock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		clockMu.Lock()
		now = now.Add(d)
		clockMu.Unlock()
	}
	cache := newR154KalshiAdmissionSnapshotCacheWithClock(
		fetcher, r154KalshiAdmissionSnapshotRefreshMaxAge, clock)

	const callers = 8
	results := make(chan r154KalshiAdmissionSnapshot, callers)
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			snapshot, err := cache.Snapshot(context.Background())
			results <- snapshot
			errs <- err
		}()
	}

	seen := map[string]bool{}
	for len(seen) < 3 {
		select {
		case resource := <-started:
			seen[resource] = true
		case <-time.After(time.Second):
			t.Fatalf("three account resources did not start concurrently; started=%v", seen)
		}
	}
	if balance, positions, orders := fetcher.calls(); balance != 1 || positions != 1 || orders != 1 {
		t.Fatalf("concurrent callers escaped singleflight before release: calls=%d/%d/%d",
			balance, positions, orders)
	}
	close(gate)

	for i := 0; i < callers; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("snapshot caller %d failed: %v", i, err)
		}
		snapshot := <-results
		if snapshot.Balance().Balance != 40_000 ||
			len(snapshot.Positions()) != 1 || len(snapshot.Orders()) != 1 {
			t.Fatalf("snapshot caller %d received incomplete copy: %+v", i, snapshot)
		}
	}
	if balance, positions, orders := fetcher.calls(); balance != 1 || positions != 1 || orders != 1 {
		t.Fatalf("singleflight made duplicate resource calls: calls=%d/%d/%d", balance, positions, orders)
	}

	if _, err := cache.Snapshot(context.Background()); err != nil {
		t.Fatalf("fresh cache read failed: %v", err)
	}
	if balance, positions, orders := fetcher.calls(); balance != 1 || positions != 1 || orders != 1 {
		t.Fatalf("fresh cache unexpectedly refreshed: calls=%d/%d/%d", balance, positions, orders)
	}

	advance(r154KalshiAdmissionSnapshotRefreshMaxAge + time.Millisecond)
	if _, err := cache.Snapshot(context.Background()); err != nil {
		t.Fatalf("stale cache refresh failed: %v", err)
	}
	if balance, positions, orders := fetcher.calls(); balance != 2 || positions != 2 || orders != 2 {
		t.Fatalf("stale cache did not refresh all resources once: calls=%d/%d/%d", balance, positions, orders)
	}
}

func TestR154KalshiAdmissionSnapshotIsCopySafeAndContextReusable(t *testing.T) {
	fetcher := &r154AdmissionSnapshotFetcherFake{
		balance:   50_000,
		portfolio: 250,
		positions: []kalshi.MarketPosition{{Ticker: "POS-ORIGINAL"}},
		orders:    []kalshi.Order{{Ticker: "ORDER-ORIGINAL"}},
	}
	cache := newR154KalshiAdmissionSnapshotCache(fetcher)
	ctx, snapshot, err := cache.SnapshotContext(context.Background())
	if err != nil {
		t.Fatalf("SnapshotContext: %v", err)
	}

	balance := snapshot.Balance()
	*balance.PortfolioValue = 99_999
	positions := snapshot.Positions()
	positions[0].Ticker = "POS-MUTATED"
	orders := snapshot.Orders()
	orders[0].Ticker = "ORDER-MUTATED"
	snapshot.positions[0].Ticker = "DIRECT-MUTATION"

	carried, ok := r154KalshiAdmissionSnapshotFromContext(ctx)
	if !ok {
		t.Fatal("snapshot missing from context")
	}
	if _, ok := r154FreshKalshiAdmissionSnapshotFromContext(ctx,
		snapshot.ObservedAt().Add(r154KalshiAdmissionSnapshotRefreshMaxAge+time.Nanosecond),
		r154KalshiAdmissionSnapshotRefreshMaxAge); ok {
		t.Fatal("stale carried snapshot passed the final-boundary freshness check")
	}
	if *carried.Balance().PortfolioValue != 250 ||
		carried.Positions()[0].Ticker != "POS-ORIGINAL" ||
		carried.Orders()[0].Ticker != "ORDER-ORIGINAL" {
		t.Fatalf("context snapshot was aliased: balance=%+v positions=%+v orders=%+v",
			carried.Balance(), carried.Positions(), carried.Orders())
	}

	ctxAgain, again, err := cache.SnapshotContext(ctx)
	if err != nil {
		t.Fatalf("context reuse: %v", err)
	}
	if ctxAgain != ctx {
		t.Fatal("fresh carried snapshot should reuse the existing context")
	}
	if again.Positions()[0].Ticker != "POS-ORIGINAL" {
		t.Fatalf("context reuse returned mutated data: %+v", again.Positions())
	}
	if balanceCalls, positionsCalls, ordersCalls := fetcher.calls(); balanceCalls != 1 || positionsCalls != 1 || ordersCalls != 1 {
		t.Fatalf("context reuse refetched account: calls=%d/%d/%d",
			balanceCalls, positionsCalls, ordersCalls)
	}
}

func TestR155CarriedAdmissionSnapshotSeedsRiskBaselineWithoutDuplicateREST(t *testing.T) {
	venueAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	var resting kalshi.Order
	if err := json.Unmarshal([]byte(`{"order_id":"order-1","ticker":"KXTEST","status":"resting",`+
		`"type":"limit","side":"yes","action":"buy","outcome_side":"yes","book_side":"bid",`+
		`"yes_price_dollars":"0.40","no_price_dollars":"0.60","remaining_count_fp":"2",`+
		`"initial_count_fp":"2"}`), &resting); err != nil {
		t.Fatal(err)
	}
	fetcher := &r154AdmissionSnapshotFetcherFake{
		balance: 50_000, portfolio: 250,
		positions: []kalshi.MarketPosition{{
			Ticker: "KXTEST", Position: 3, LastUpdatedTS: venueAt.Format(time.RFC3339Nano),
		}},
		orders: []kalshi.Order{resting},
	}
	cache := newR154KalshiAdmissionSnapshotCache(fetcher)
	ctx, snapshot, err := cache.SnapshotContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{kal: &kalshi.Client{}}
	s.kalshiAdmissionSnapshotOnce.Do(func() { s.kalshiAdmissionSnapshot = cache })

	baseline, err := s.r148KalshiRiskBaseline(ctx, "KXTEST")
	if err != nil {
		t.Fatal(err)
	}
	if balance, positions, orders := fetcher.calls(); balance != 1 || positions != 1 || orders != 1 {
		t.Fatalf("risk baseline repeated authenticated REST: calls=%d/%d/%d",
			balance, positions, orders)
	}
	if !baseline.Observed.Equal(venueAt) || math.Abs(baseline.PositionQty-3) > 1e-9 ||
		math.Abs(baseline.RestingRiskUSD-.8) > 1e-9 {
		t.Fatalf("carried baseline mismatch: %+v", baseline)
	}
	var receipt struct {
		Source         string    `json:"source"`
		RequestStarted time.Time `json:"request_started_at"`
	}
	if err := json.Unmarshal([]byte(baseline.ReceiptJSON), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Source != "kalshi-carried-admission-snapshot" ||
		!receipt.RequestStarted.Equal(snapshot.ObservedAt()) {
		t.Fatalf("carried baseline provenance mismatch: %+v snapshot=%s",
			receipt, snapshot.ObservedAt())
	}
}

func TestR155EmptyCarriedAdmissionSnapshotKeepsBaselineFallbackAndProvenance(t *testing.T) {
	fetcher := &r154AdmissionSnapshotFetcherFake{balance: 50_000, portfolio: 250}
	cache := newR154KalshiAdmissionSnapshotCache(fetcher)
	ctx, snapshot, err := cache.SnapshotContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{kal: &kalshi.Client{}}
	s.kalshiAdmissionSnapshotOnce.Do(func() { s.kalshiAdmissionSnapshot = cache })

	baseline, err := s.r148KalshiRiskBaseline(ctx, "KXEMPTY")
	if err != nil {
		t.Fatal(err)
	}
	if !baseline.Observed.Equal(snapshot.ObservedAt()) || baseline.PositionQty != 0 ||
		baseline.RestingRiskUSD != 0 {
		t.Fatalf("empty carried baseline mismatch: %+v snapshot=%s",
			baseline, snapshot.ObservedAt())
	}
	var receipt struct {
		Source              string `json:"source"`
		TargetPositionFound *bool  `json:"baseline_target_position_found"`
	}
	if err := json.Unmarshal([]byte(baseline.ReceiptJSON), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Source != "kalshi-carried-admission-snapshot" ||
		receipt.TargetPositionFound == nil || *receipt.TargetPositionFound {
		t.Fatalf("empty carried baseline receipt mismatch: %+v", receipt)
	}
	if balance, positions, orders := fetcher.calls(); balance != 1 || positions != 1 || orders != 1 {
		t.Fatalf("empty carried baseline repeated REST: calls=%d/%d/%d",
			balance, positions, orders)
	}
}

func TestR158UnchangedSnapshotCanBeReusedThroughTheOneSecondExecutionLease(t *testing.T) {
	now := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	fetcher := &r154AdmissionSnapshotFetcherFake{balance: 50_000, portfolio: 0}
	cache := newR154KalshiAdmissionSnapshotCacheWithClock(fetcher,
		r154KalshiAdmissionSnapshotRefreshMaxAge, func() time.Time { return now })
	snapshot, err := cache.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	now = now.Add(500 * time.Millisecond)
	if !cache.Current(snapshot, now) {
		t.Fatal("unchanged 500ms snapshot did not remain reusable inside the one-second lease")
	}
	if !cache.CurrentForExecution(snapshot, now) {
		t.Fatal("same-request carried snapshot did not retain its one-second execution lease")
	}
	receipt := cache.ConsumeDetailed(snapshot, now)
	if !receipt.Consumed || receipt.Failure != r154KalshiAdmissionConsumeOK ||
		receipt.SnapshotAgeNS != int64(500*time.Millisecond) ||
		receipt.SnapshotObservedAt != snapshot.ObservedAt().Format(time.RFC3339Nano) ||
		receipt.CurrentEpoch != snapshot.epoch || receipt.CurrentGeneration != snapshot.generation ||
		!receipt.CacheHave {
		t.Fatalf("execution consume receipt mismatch: %+v", receipt)
	}
}

func TestR155SupersededCarriedSnapshotCannotConsumeNewRefresh(t *testing.T) {
	now := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	fetcher := &r154AdmissionSnapshotFetcherFake{balance: 50_000, portfolio: 0}
	cache := newR154KalshiAdmissionSnapshotCacheWithClock(fetcher,
		r154KalshiAdmissionSnapshotRefreshMaxAge, func() time.Time { return now })
	first, err := cache.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(r154KalshiAdmissionSnapshotRefreshMaxAge + time.Millisecond)
	second, err := cache.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.epoch != second.epoch || first.generation == second.generation {
		t.Fatalf("refresh identity mismatch: first=%+v second=%+v", first, second)
	}
	stale := cache.ConsumeDetailed(first, now)
	if stale.Consumed || stale.Failure != r154KalshiAdmissionConsumeSuperseded ||
		stale.SnapshotGeneration != first.generation ||
		stale.CurrentGeneration != second.generation || !stale.CacheHave {
		t.Fatalf("superseded snapshot consumed replacement: %+v", stale)
	}
	if current := cache.ConsumeDetailed(second, now); !current.Consumed {
		t.Fatalf("current refresh could not consume after stale rejection: %+v", current)
	}
	if balance, positions, orders := fetcher.calls(); balance != 2 || positions != 2 || orders != 2 {
		t.Fatalf("replacement refresh calls=%d/%d/%d", balance, positions, orders)
	}
}

func TestR155ConsumeReceiptDistinguishesExpiryAndInvalidation(t *testing.T) {
	start := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	now := start
	fetcher := &r154AdmissionSnapshotFetcherFake{balance: 50_000, portfolio: 0}
	boundaryCache := newR154KalshiAdmissionSnapshotCacheWithClock(fetcher,
		r154KalshiAdmissionSnapshotRefreshMaxAge, func() time.Time { return now })
	boundary, err := boundaryCache.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	now = start.Add(r154KalshiAdmissionSnapshotExecutionLease)
	if receipt := boundaryCache.ConsumeDetailed(boundary, now); !receipt.Consumed {
		t.Fatalf("exact one-second execution boundary failed: %+v", receipt)
	}

	now = start
	cache := newR154KalshiAdmissionSnapshotCacheWithClock(fetcher,
		r154KalshiAdmissionSnapshotRefreshMaxAge, func() time.Time { return now })
	expiring, err := cache.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	now = start.Add(r154KalshiAdmissionSnapshotExecutionLease + time.Nanosecond)
	expired := cache.ConsumeDetailed(expiring, now)
	if expired.Consumed || expired.Failure != r154KalshiAdmissionConsumeExpired ||
		expired.SnapshotAgeNS != int64(r154KalshiAdmissionSnapshotExecutionLease+time.Nanosecond) ||
		expired.CurrentEpoch != expiring.epoch || !expired.CacheHave {
		t.Fatalf("expiry receipt mismatch: %+v", expired)
	}

	cache.Invalidate()
	invalidated := cache.ConsumeDetailed(expiring, start.Add(100*time.Millisecond))
	if invalidated.Consumed || invalidated.Failure != r154KalshiAdmissionConsumeEpochInvalidated ||
		invalidated.CurrentEpoch == invalidated.SnapshotEpoch || invalidated.CacheHave ||
		invalidated.CurrentGeneration != 0 {
		t.Fatalf("invalidation receipt mismatch: %+v", invalidated)
	}
}

func TestR155ConcurrentDoubleConsumeHasExactlyOneWinner(t *testing.T) {
	fetcher := &r154AdmissionSnapshotFetcherFake{balance: 50_000, portfolio: 0}
	cache := newR154KalshiAdmissionSnapshotCache(fetcher)
	snapshot, err := cache.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	now := snapshot.ObservedAt().Add(100 * time.Millisecond)
	results := make(chan r154KalshiAdmissionConsumeReceipt, 2)
	for i := 0; i < 2; i++ {
		go func() { results <- cache.ConsumeDetailed(snapshot, now) }()
	}
	consumed := 0
	for i := 0; i < 2; i++ {
		if receipt := <-results; receipt.Consumed {
			consumed++
		}
	}
	if consumed != 1 {
		t.Fatalf("double consume winners=%d, want exactly one", consumed)
	}
}

func TestR154KalshiAdmissionSnapshotFailsClosedInsteadOfServingStale(t *testing.T) {
	now := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	fetcher := &r154AdmissionSnapshotFetcherFake{balance: 50_000, portfolio: 0}
	cache := newR154KalshiAdmissionSnapshotCacheWithClock(fetcher, 250*time.Millisecond, func() time.Time {
		return now
	})
	if _, err := cache.Snapshot(context.Background()); err != nil {
		t.Fatalf("initial snapshot: %v", err)
	}

	now = now.Add(time.Second)
	fetcher.mu.Lock()
	fetcher.ordersErr = errors.New("orders unavailable")
	fetcher.mu.Unlock()
	if snapshot, err := cache.Snapshot(context.Background()); err == nil {
		t.Fatalf("stale last-good snapshot authorized after refresh failure: %+v", snapshot)
	}

	fetcher.mu.Lock()
	fetcher.ordersErr = nil
	fetcher.mu.Unlock()
	if _, err := cache.Snapshot(context.Background()); err != nil {
		t.Fatalf("cache did not recover after fail-closed refresh: %v", err)
	}

	incomplete := &r154AdmissionSnapshotFetcherFake{balance: 50_000, portfolio: -1}
	if _, err := newR154KalshiAdmissionSnapshotCache(incomplete).Snapshot(context.Background()); !errors.Is(err, errR154KalshiAdmissionSnapshotIncomplete) {
		t.Fatalf("invalid NAV receipt did not fail closed: %v", err)
	}
}

func TestR154KalshiAdmissionSnapshotInvalidationFencesInflightRead(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan string, 16)
	fetcher := &r154AdmissionSnapshotFetcherFake{
		balance: 50_000, portfolio: 0, gate: gate, started: started,
	}
	cache := newR154KalshiAdmissionSnapshotCache(fetcher)
	result := make(chan error, 1)
	go func() {
		_, err := cache.Snapshot(context.Background())
		result <- err
	}()
	for i := 0; i < 3; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("initial refresh did not start all account resources")
		}
	}

	cache.Invalidate()
	close(gate)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("post-invalidation clean retry failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("in-flight invalidation did not complete through a clean retry")
	}
	if balance, positions, orders := fetcher.calls(); balance != 2 || positions != 2 || orders != 2 {
		t.Fatalf("invalidated in-flight receipt was reused: calls=%d/%d/%d", balance, positions, orders)
	}
}

func TestR154KalshiAdmissionSnapshotWaiterHonorsOwnContext(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan string, 8)
	fetcher := &r154AdmissionSnapshotFetcherFake{
		balance: 50_000, portfolio: 0, gate: gate, started: started,
	}
	cache := newR154KalshiAdmissionSnapshotCache(fetcher)
	ownerDone := make(chan error, 1)
	go func() {
		_, err := cache.Snapshot(context.Background())
		ownerDone <- err
	}()
	for i := 0; i < 3; i++ {
		<-started
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := cache.Snapshot(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting caller ignored its own deadline: %v", err)
	}
	close(gate)
	if err := <-ownerDone; err != nil {
		t.Fatalf("owner refresh failed after release: %v", err)
	}
}

func TestR154KalshiAdmissionSnapshotAgeStartsBeforeOldestResourceRead(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan string, 8)
	fetcher := &r154AdmissionSnapshotFetcherFake{
		balance: 50_000, portfolio: 0, gate: gate, started: started,
	}
	now := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	var clockMu sync.Mutex
	clock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	cache := newR154KalshiAdmissionSnapshotCacheWithClock(
		fetcher, r154KalshiAdmissionSnapshotRefreshMaxAge, clock)
	result := make(chan error, 1)
	go func() {
		_, err := cache.Snapshot(context.Background())
		result <- err
	}()
	for i := 0; i < 3; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("account refresh did not start all resources")
		}
	}
	clockMu.Lock()
	now = now.Add(r154KalshiAdmissionSnapshotRefreshMaxAge + time.Millisecond)
	clockMu.Unlock()
	close(gate)
	select {
	case err := <-result:
		if !errors.Is(err, errR154KalshiAdmissionSnapshotExpired) {
			t.Fatalf("slow refresh was relabeled fresh: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow account refresh did not fail closed")
	}
}

func TestR154KalshiAdmissionSnapshotEpochFencesCarriedContextAndAtomicConsume(t *testing.T) {
	fetcher := &r154AdmissionSnapshotFetcherFake{balance: 50_000, portfolio: 0}
	cache := newR154KalshiAdmissionSnapshotCache(fetcher)
	ctx, first, err := cache.SnapshotContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !cache.Current(first, first.ObservedAt()) {
		t.Fatal("new snapshot is not current")
	}
	cache.Invalidate()
	if cache.Current(first, first.ObservedAt()) {
		t.Fatal("invalidated carried epoch remained current")
	}

	ctx, second, err := cache.SnapshotContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second.epoch == first.epoch {
		t.Fatalf("stale context epoch was reused: first=%d second=%d", first.epoch, second.epoch)
	}
	if balance, positions, orders := fetcher.calls(); balance != 2 || positions != 2 || orders != 2 {
		t.Fatalf("invalidated context did not refresh all account resources: %d/%d/%d",
			balance, positions, orders)
	}
	if !cache.Consume(second, second.ObservedAt()) {
		t.Fatal("current snapshot could not be atomically consumed")
	}
	if cache.Current(second, second.ObservedAt()) || cache.Consume(second, second.ObservedAt()) {
		t.Fatal("consumed pre-submit epoch remained reusable")
	}
}

func TestR172KalshiReservationOnlyNoSendNeverFabricatesSubmitOrVenueReject(t *testing.T) {
	s, st := r148RiskTestServer(t)
	intent := r148RiskTestReservation(t, st, "risk-r154-no-send")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the durable reservation release must outlive an expired handler request
	evidence := r154KalshiAdmissionConsumeReceipt{
		Consumed: false, Failure: r154KalshiAdmissionConsumeExpired,
		SnapshotObservedAt: "2026-07-17T12:00:00.000000001Z",
		CheckedAt:          "2026-07-17T12:00:01.000000002Z",
		SnapshotAgeNS:      int64(time.Second + time.Nanosecond),
		SnapshotAgeMS:      1000.000001,
		SnapshotEpoch:      7, CurrentEpoch: 7,
		SnapshotGeneration: 11, CurrentGeneration: 11, CacheHave: true,
		RefreshMaxAgeMS:       r154KalshiAdmissionSnapshotRefreshMaxAge.Milliseconds(),
		FinalExecutionLeaseMS: r154KalshiAdmissionSnapshotExecutionLease.Milliseconds(),
	}
	if err := s.r154RejectKalshiNoSendRisk(ctx, intent.ReservationID,
		"pre-submit-account-snapshot",
		"account snapshot changed; venue was not called", evidence); err != nil {
		t.Fatal(err)
	}

	active, err := st.ActiveLivePendingRisk(context.Background())
	if err != nil || len(active) != 0 {
		t.Fatalf("known no-send stayed risk-reserved: active=%d err=%v", len(active), err)
	}
	if reason := s.liveAutoSafetyPauseReason(); reason != "" {
		t.Fatalf("known no-send left LIVE AUTO paused: %q", reason)
	}
	row, found, err := st.LivePendingRiskByID(context.Background(), intent.ReservationID)
	if err != nil || !found || len(row.Events) < 2 {
		t.Fatalf("no-send terminal proof unavailable: found=%v events=%d err=%v",
			found, len(row.Events), err)
	}
	released := row.Events[len(row.Events)-1]
	for _, event := range row.Events {
		if event.EventType == storage.LivePendingRiskSubmitStarted ||
			event.EventType == storage.LivePendingRiskCleanRejected {
			t.Fatalf("known no-send fabricated a venue attempt: %+v", event)
		}
	}
	if released.EventType != storage.LivePendingRiskReleased {
		t.Fatalf("no-send terminal sequence ended with %+v", released)
	}
	var persisted r154KalshiAdmissionConsumeReceipt
	if err := json.Unmarshal([]byte(released.EvidenceJSON), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Failure != r154KalshiAdmissionConsumeExpired ||
		persisted.SnapshotObservedAt != evidence.SnapshotObservedAt ||
		persisted.SnapshotAgeNS != evidence.SnapshotAgeNS ||
		persisted.SnapshotEpoch != 7 || persisted.CurrentEpoch != 7 ||
		persisted.SnapshotGeneration != 11 || persisted.CurrentGeneration != 11 ||
		!persisted.CacheHave {
		t.Fatalf("clean-reject lost exact snapshot diagnostics: %+v", persisted)
	}
}

func TestR172KalshiNoSendRefusesToRewriteAnExistingSubmitAsNoVenueCall(t *testing.T) {
	s, st := r148RiskTestServer(t)
	intent := r148RiskTestReservation(t, st, "risk-r172-submit-contradiction")
	leg := 0
	if err := s.r148AppendRiskEvent(context.Background(), intent.ReservationID,
		storage.LivePendingRiskSubmitStarted, "entry", "BUY", "client-1", "",
		"kalshi-create-order", "network mutation begins", &leg, 0, .5, .03, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.r154RejectKalshiNoSendRisk(context.Background(), intent.ReservationID,
		"test", "venue was not called", nil); err == nil ||
		!strings.Contains(err.Error(), "cannot follow submit_started") {
		t.Fatalf("contradictory no-send error = %v", err)
	}
	active, err := st.ActiveLivePendingRisk(context.Background())
	if err != nil || len(active) != 1 {
		t.Fatalf("contradictory submit was unsafely released: active=%d err=%v", len(active), err)
	}
}

func TestR154KalshiPrivateAccountEventInvalidatesAdmissionEpoch(t *testing.T) {
	fetcher := &r154AdmissionSnapshotFetcherFake{balance: 50_000, portfolio: 0}
	cache := newR154KalshiAdmissionSnapshotCache(fetcher)
	snapshot, err := cache.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{kal: &kalshi.Client{}}
	s.kalshiAdmissionSnapshotOnce.Do(func() { s.kalshiAdmissionSnapshot = cache })
	s.kalshiPrivateAccountInvalidate()
	if cache.Current(snapshot, snapshot.ObservedAt()) {
		t.Fatal("private account event did not fence the cached admission epoch")
	}
	receipt := cache.ConsumeDetailed(snapshot, snapshot.ObservedAt().Add(100*time.Millisecond))
	if receipt.Consumed || receipt.Failure != r154KalshiAdmissionConsumeEpochInvalidated ||
		receipt.CurrentEpoch == receipt.SnapshotEpoch || receipt.CacheHave ||
		receipt.CurrentGeneration != 0 {
		t.Fatalf("private invalidation diagnostics mismatch: %+v", receipt)
	}
}

func TestR154KalshiErrorOnlyMutationFencesCacheBeforeAndAfterCall(t *testing.T) {
	fetcher := &r154AdmissionSnapshotFetcherFake{balance: 50_000, portfolio: 0}
	cache := newR154KalshiAdmissionSnapshotCache(fetcher)
	before, err := cache.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{kal: &kalshi.Client{}}
	s.kalshiAdmissionSnapshotOnce.Do(func() { s.kalshiAdmissionSnapshot = cache })
	sentinel := errors.New("ambiguous mutation result")
	var during r154KalshiAdmissionSnapshot
	err = s.r154RunKalshiAccountMutation(func() error {
		if cache.Current(before, before.ObservedAt()) {
			t.Fatal("pre-mutation admission snapshot was not fenced")
		}
		var refreshErr error
		during, refreshErr = cache.Snapshot(context.Background())
		if refreshErr != nil {
			t.Fatal(refreshErr)
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("mutation error changed: %v", err)
	}
	if cache.Current(during, during.ObservedAt()) {
		t.Fatal("post-mutation defer did not fence a cache refill during an ambiguous result")
	}
}

func TestR154PendingRiskVisibilityAndReleaseInvalidateAdmissionEpoch(t *testing.T) {
	s, st := r148RiskTestServer(t)
	s.kal = &kalshi.Client{}
	fetcher := &r154AdmissionSnapshotFetcherFake{balance: 50_000, portfolio: 0}
	cache := newR154KalshiAdmissionSnapshotCache(fetcher)
	s.kalshiAdmissionSnapshotOnce.Do(func() { s.kalshiAdmissionSnapshot = cache })
	intent := r148RiskTestReservation(t, st, "risk-r154-cache-fence")
	ctx := context.Background()
	leg := 0
	if err := s.r148AppendRiskEvent(ctx, intent.ReservationID, storage.LivePendingRiskSubmitStarted,
		"entry", "BUY", "client-1", "", "test", "submit", &leg, 0, .5, .03, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.r148AppendRiskEvent(ctx, intent.ReservationID, storage.LivePendingRiskAck,
		"entry", "BUY", "client-1", "order-1", "test", "ack", &leg, 0, .5, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.r148AppendRiskEvent(ctx, intent.ReservationID, storage.LivePendingRiskFillSeen,
		"entry", "BUY", "client-1", "order-1", "test", "fill", &leg, 2, .5, .03, nil); err != nil {
		t.Fatal(err)
	}
	beforeVisible, err := cache.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.r148AppendRiskEvent(ctx, intent.ReservationID, storage.LivePendingRiskAccountVisible,
		"entry", "BUY", "client-1", "order-1", "test", "visible", &leg, 2, .5, .03, nil); err != nil {
		t.Fatal(err)
	}
	if cache.Current(beforeVisible, beforeVisible.ObservedAt()) {
		t.Fatal("AccountVisible removed pending exposure without invalidating the account cache")
	}

	beforeRelease, err := cache.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.r148ReleaseRisk(ctx, intent.ReservationID, "test", "released", nil); err != nil {
		t.Fatal(err)
	}
	if cache.Current(beforeRelease, beforeRelease.ObservedAt()) {
		t.Fatal("Released removed pending exposure without invalidating the account cache")
	}
}
