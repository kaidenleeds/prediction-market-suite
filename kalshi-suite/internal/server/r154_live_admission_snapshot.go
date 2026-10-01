package server

// R154 Kalshi LIVE admission snapshot.
//
// The final order path needs one coherent, very recent view of cash/NAV, positions, and resting
// orders. Fetching those three authenticated resources repeatedly inside sibling guards adds
// seconds of avoidable latency and can make later checks disagree with earlier checks. This cache
// fetches all three resources concurrently, singleflights cold refreshes, and lets a request carry
// the immutable result through context. Durable pending-risk rows are deliberately NOT part of
// this snapshot: every guard must continue reading that append-only ledger at its write boundary.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	// RefreshMaxAge controls cache reuse and refresh coherence. A caller entering the account
	// guard should not inherit an old cached view merely because the final execution lease is
	// longer.
	// The cache is bound to the authenticated account epoch and invalidated immediately after any
	// order attempt or pending-risk visibility change. One second therefore reuses only an unchanged
	// account view. The old 250ms lease expired during ordinary proof + durable-reservation work and
	// caused known-no-send failures even when cash, positions, orders, and pending risk had not moved.
	r154KalshiAdmissionSnapshotRefreshMaxAge = time.Second
	// ExecutionLease is the maximum age of the immutable snapshot carried by one serialized
	// order handler. Epoch invalidation remains authoritative at every age.
	r154KalshiAdmissionSnapshotExecutionLease = time.Second
	// After the durable mutation boundary, detach from the caller but retain an overall deadline.
	// This bounds rate-limit waits/retries as well as the HTTP request while preventing a phone or
	// browser disconnect from manufacturing an unattempted durable submit.
	liveKalshiPostBoundaryTimeout = 12 * time.Second
)

var (
	errR154KalshiAdmissionFetcherUnavailable  = errors.New("kalshi admission account fetcher unavailable")
	errR154KalshiAdmissionSnapshotIncomplete  = errors.New("kalshi admission account snapshot incomplete")
	errR154KalshiAdmissionSnapshotInvalidated = errors.New("kalshi admission account snapshot invalidated during refresh")
	errR154KalshiAdmissionSnapshotExpired     = errors.New("kalshi admission account snapshot aged during refresh")
)

// r154KalshiAdmissionFetcher is intentionally the exact read-only subset implemented by
// *kalshi.Client. Tests can inject a hermetic fake without constructing an HTTP client.
type r154KalshiAdmissionFetcher interface {
	GetBalance(context.Context) (*kalshi.Balance, error)
	GetPositions(context.Context) ([]kalshi.MarketPosition, error)
	GetOrders(context.Context) ([]kalshi.Order, error)
}

// r154KalshiAdmissionSnapshot is immutable by contract. Its reference-bearing fields are private,
// every cache/context boundary clones them, and accessors return fresh copies.
type r154KalshiAdmissionSnapshot struct {
	observedAt time.Time
	epoch      uint64
	generation uint64
	balance    kalshi.Balance
	positions  []kalshi.MarketPosition
	orders     []kalshi.Order
}

func (s r154KalshiAdmissionSnapshot) ObservedAt() time.Time {
	return s.observedAt
}

func (s r154KalshiAdmissionSnapshot) Balance() *kalshi.Balance {
	balance := cloneR154KalshiBalance(s.balance)
	return &balance
}

func (s r154KalshiAdmissionSnapshot) Positions() []kalshi.MarketPosition {
	return append([]kalshi.MarketPosition(nil), s.positions...)
}

func (s r154KalshiAdmissionSnapshot) Orders() []kalshi.Order {
	return append([]kalshi.Order(nil), s.orders...)
}

func (s r154KalshiAdmissionSnapshot) Fresh(now time.Time, maxAge time.Duration) bool {
	if s.observedAt.IsZero() || now.IsZero() || maxAge <= 0 {
		return false
	}
	age := now.Sub(s.observedAt)
	return age >= 0 && age <= maxAge
}

func (s r154KalshiAdmissionSnapshot) clone() r154KalshiAdmissionSnapshot {
	out := s
	out.balance = cloneR154KalshiBalance(s.balance)
	out.positions = append([]kalshi.MarketPosition(nil), s.positions...)
	out.orders = append([]kalshi.Order(nil), s.orders...)
	return out
}

func cloneR154KalshiBalance(in kalshi.Balance) kalshi.Balance {
	out := in
	if in.PortfolioValue != nil {
		value := *in.PortfolioValue
		out.PortfolioValue = &value
	}
	return out
}

type r154KalshiAdmissionSnapshotFlight struct {
	done chan struct{}
}

// r154KalshiAdmissionSnapshotCache is safe for concurrent use. A stale cache never authorizes an
// order: refresh failures return an error, rather than falling back to last-good account data.
type r154KalshiAdmissionSnapshotCache struct {
	mu         sync.Mutex
	fetcher    r154KalshiAdmissionFetcher
	maxAge     time.Duration
	now        func() time.Time
	snapshot   r154KalshiAdmissionSnapshot
	have       bool
	flight     *r154KalshiAdmissionSnapshotFlight
	epoch      uint64
	generation uint64
}

func newR154KalshiAdmissionSnapshotCache(fetcher r154KalshiAdmissionFetcher) *r154KalshiAdmissionSnapshotCache {
	return newR154KalshiAdmissionSnapshotCacheWithClock(
		fetcher,
		r154KalshiAdmissionSnapshotRefreshMaxAge,
		time.Now,
	)
}

func (s *Server) r154KalshiAdmissionCache() *r154KalshiAdmissionSnapshotCache {
	if s == nil || s.kal == nil {
		return nil
	}
	s.kalshiAdmissionSnapshotOnce.Do(func() {
		s.kalshiAdmissionSnapshot = newR154KalshiAdmissionSnapshotCache(s.kal)
	})
	return s.kalshiAdmissionSnapshot
}

func (s *Server) r154KalshiAdmissionSnapshotContext(ctx context.Context) (
	context.Context, r154KalshiAdmissionSnapshot, error) {
	cache := s.r154KalshiAdmissionCache()
	if cache == nil {
		return ctx, r154KalshiAdmissionSnapshot{}, errR154KalshiAdmissionFetcherUnavailable
	}
	next, snapshot, err := cache.SnapshotContext(ctx)
	if err == nil {
		s.scheduleKalshiSemanticSnapshotWarm(snapshot)
	}
	return next, snapshot, err
}

func (s *Server) r154KalshiAdmissionSnapshotCurrent(ctx context.Context, now time.Time) (
	r154KalshiAdmissionSnapshot, bool) {
	cache := s.r154KalshiAdmissionCache()
	if cache == nil {
		return r154KalshiAdmissionSnapshot{}, false
	}
	snapshot, ok := r154KalshiAdmissionSnapshotFromContext(ctx)
	if !ok || !cache.CurrentForExecution(snapshot, now) {
		return r154KalshiAdmissionSnapshot{}, false
	}
	return snapshot, true
}

type r154KalshiAdmissionConsumeFailure string

const (
	r154KalshiAdmissionConsumeOK               r154KalshiAdmissionConsumeFailure = ""
	r154KalshiAdmissionConsumeCacheUnavailable r154KalshiAdmissionConsumeFailure = "cache-unavailable"
	r154KalshiAdmissionConsumeContextMissing   r154KalshiAdmissionConsumeFailure = "context-missing"
	r154KalshiAdmissionConsumeEpochInvalidated r154KalshiAdmissionConsumeFailure = "epoch-invalidated"
	r154KalshiAdmissionConsumeSuperseded       r154KalshiAdmissionConsumeFailure = "snapshot-superseded"
	r154KalshiAdmissionConsumeCacheEmpty       r154KalshiAdmissionConsumeFailure = "cache-empty"
	r154KalshiAdmissionConsumeClockRegression  r154KalshiAdmissionConsumeFailure = "clock-regression"
	r154KalshiAdmissionConsumeExpired          r154KalshiAdmissionConsumeFailure = "expired"
)

// r154KalshiAdmissionConsumeReceipt is durable evidence for the final account boundary. Nanosecond
// age and RFC3339Nano timestamps preserve the exact carried value; the human-scale millisecond
// field keeps audit receipts easy to inspect.
type r154KalshiAdmissionConsumeReceipt struct {
	Consumed              bool                              `json:"consumed"`
	Failure               r154KalshiAdmissionConsumeFailure `json:"failure"`
	SnapshotObservedAt    string                            `json:"snapshot_observed_at"`
	CheckedAt             string                            `json:"checked_at"`
	SnapshotAgeNS         int64                             `json:"snapshot_age_ns"`
	SnapshotAgeMS         float64                           `json:"snapshot_age_ms"`
	SnapshotEpoch         uint64                            `json:"snapshot_epoch"`
	CurrentEpoch          uint64                            `json:"current_epoch"`
	SnapshotGeneration    uint64                            `json:"snapshot_generation"`
	CurrentGeneration     uint64                            `json:"current_generation"`
	CacheHave             bool                              `json:"cache_have"`
	RefreshMaxAgeMS       int64                             `json:"refresh_max_age_ms"`
	FinalExecutionLeaseMS int64                             `json:"final_execution_lease_ms"`
}

func newR154KalshiAdmissionConsumeReceipt(snapshot r154KalshiAdmissionSnapshot,
	now time.Time) r154KalshiAdmissionConsumeReceipt {
	out := r154KalshiAdmissionConsumeReceipt{
		SnapshotAgeNS:         -1,
		SnapshotAgeMS:         -1,
		SnapshotEpoch:         snapshot.epoch,
		SnapshotGeneration:    snapshot.generation,
		RefreshMaxAgeMS:       r154KalshiAdmissionSnapshotRefreshMaxAge.Milliseconds(),
		FinalExecutionLeaseMS: r154KalshiAdmissionSnapshotExecutionLease.Milliseconds(),
	}
	if !snapshot.observedAt.IsZero() {
		out.SnapshotObservedAt = snapshot.observedAt.UTC().Format(time.RFC3339Nano)
	}
	if !now.IsZero() {
		out.CheckedAt = now.UTC().Format(time.RFC3339Nano)
		if !snapshot.observedAt.IsZero() {
			age := now.Sub(snapshot.observedAt)
			out.SnapshotAgeNS = age.Nanoseconds()
			out.SnapshotAgeMS = float64(age) / float64(time.Millisecond)
		}
	}
	return out
}

// consumeR154KalshiAdmissionSnapshot is the final single-order write boundary. Validation and
// invalidation happen under one cache lock, so a private-account event or another venue mutation
// that invalidated the carried epoch before this point cannot slip through a check-then-clear gap.
func (s *Server) consumeR154KalshiAdmissionSnapshot(ctx context.Context,
	now time.Time) r154KalshiAdmissionConsumeReceipt {
	cache := s.r154KalshiAdmissionCache()
	if cache == nil {
		out := newR154KalshiAdmissionConsumeReceipt(r154KalshiAdmissionSnapshot{}, now)
		out.Failure = r154KalshiAdmissionConsumeCacheUnavailable
		return out
	}
	snapshot, ok := r154KalshiAdmissionSnapshotFromContext(ctx)
	if !ok {
		out := newR154KalshiAdmissionConsumeReceipt(r154KalshiAdmissionSnapshot{}, now)
		cache.mu.Lock()
		out.CurrentEpoch, out.CacheHave = cache.epoch, cache.have
		if cache.have {
			out.CurrentGeneration = cache.snapshot.generation
		}
		cache.mu.Unlock()
		out.Failure = r154KalshiAdmissionConsumeContextMissing
		return out
	}
	return cache.ConsumeDetailed(snapshot, now)
}

// r154RejectKalshiNoSendRisk releases a durable reservation while the suite can still prove
// CreateOrder was never called. SubmitStarted now sits at the last boundary immediately before
// CreateOrder; finding one here contradicts the claimed no-send and must fail closed rather than
// fabricate a venue rejection. A reservation-only refusal may be released directly.
func (s *Server) r154RejectKalshiNoSendRisk(ctx context.Context, riskID, source, reason string,
	evidence any) error {
	row, err := s.r148RiskReservation(context.WithoutCancel(ctx), riskID)
	if err != nil {
		return err
	}
	for _, event := range row.Events {
		if event.EventType == storage.LivePendingRiskSubmitStarted {
			return errors.New("known Kalshi no-send cannot follow submit_started")
		}
	}
	return s.r148ReleaseRisk(context.WithoutCancel(ctx), riskID, source,
		"venue was provably not called before submit_started: "+reason, evidence)
}

func (s *Server) invalidateR154KalshiAdmissionSnapshot() {
	if s == nil {
		return
	}
	if cache := s.r154KalshiAdmissionCache(); cache != nil {
		cache.Invalidate()
	}
}

// r154RunKalshiAccountMutation fences account cache state on both sides of a mutation whose API
// returns only an error (currently RFQ acceptance). The defer preserves the post-call fence for
// clean rejections, transport ambiguity, and future early returns inside the mutation closure.
func (s *Server) r154RunKalshiAccountMutation(mutate func() error) error {
	if mutate == nil {
		return errors.New("kalshi account mutation unavailable")
	}
	s.invalidateR154KalshiAdmissionSnapshot()
	defer s.invalidateR154KalshiAdmissionSnapshot()
	return mutate()
}

func (s *Server) refreshR154KalshiAdmissionSnapshotAsync() {
	cache := s.r154KalshiAdmissionCache()
	if cache == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if snapshot, err := cache.Snapshot(ctx); err == nil {
			s.scheduleKalshiSemanticSnapshotWarm(snapshot)
		}
	}()
}

func newR154KalshiAdmissionSnapshotCacheWithClock(fetcher r154KalshiAdmissionFetcher,
	maxAge time.Duration, now func() time.Time) *r154KalshiAdmissionSnapshotCache {
	if maxAge <= 0 {
		maxAge = r154KalshiAdmissionSnapshotRefreshMaxAge
	}
	if now == nil {
		now = time.Now
	}
	return &r154KalshiAdmissionSnapshotCache{
		fetcher: fetcher,
		maxAge:  maxAge,
		now:     now,
	}
}

// Snapshot returns a copy-safe account snapshot no older than the configured maximum. Concurrent
// stale callers wait for one refresh. A waiter whose own context expires fails immediately; it
// never inherits another caller's canceled context as its result.
func (c *r154KalshiAdmissionSnapshotCache) Snapshot(ctx context.Context) (r154KalshiAdmissionSnapshot, error) {
	if c == nil || c.fetcher == nil {
		return r154KalshiAdmissionSnapshot{}, errR154KalshiAdmissionFetcherUnavailable
	}
	if ctx == nil {
		return r154KalshiAdmissionSnapshot{}, errors.New("kalshi admission account snapshot context unavailable")
	}

	for {
		if err := ctx.Err(); err != nil {
			return r154KalshiAdmissionSnapshot{}, err
		}

		c.mu.Lock()
		now := c.now()
		if c.have && c.snapshot.Fresh(now, c.maxAge) {
			snapshot := c.snapshot.clone()
			c.mu.Unlock()
			return snapshot, nil
		}
		if c.flight != nil {
			done := c.flight.done
			c.mu.Unlock()
			select {
			case <-done:
				// Re-check the cache. If the owner failed, one live waiter becomes the next owner;
				// no caller is allowed to use stale last-good data.
				continue
			case <-ctx.Done():
				return r154KalshiAdmissionSnapshot{}, ctx.Err()
			}
		}

		flight := &r154KalshiAdmissionSnapshotFlight{done: make(chan struct{})}
		epoch := c.epoch
		c.flight = flight
		c.mu.Unlock()

		snapshot, err := fetchR154KalshiAdmissionSnapshot(ctx, c.fetcher, c.now)

		c.mu.Lock()
		if err == nil && epoch != c.epoch {
			// An order mutation invalidated the account view while the REST reads were in flight.
			// Their relative timing is unknowable, so discard them and force a clean post-mutation
			// refresh instead of authorizing from a maybe-before/maybe-after mixture.
			err = errR154KalshiAdmissionSnapshotInvalidated
		}
		if err == nil {
			snapshot.epoch = epoch
			if !snapshot.Fresh(c.now(), c.maxAge) {
				// observedAt is the start of the oldest possible component read. A slow three-way
				// refresh must fail closed instead of relabeling its earliest response as new.
				err = errR154KalshiAdmissionSnapshotExpired
			}
		}
		if err == nil {
			c.generation++
			snapshot.generation = c.generation
			c.snapshot = snapshot.clone()
			c.have = true
		}
		c.flight = nil
		close(flight.done)
		c.mu.Unlock()

		if errors.Is(err, errR154KalshiAdmissionSnapshotInvalidated) {
			continue
		}
		if err != nil {
			return r154KalshiAdmissionSnapshot{}, err
		}
		return snapshot.clone(), nil
	}
}

// Invalidate must be called after every Kalshi order mutation (including ambiguous results). It
// also fences an in-flight refresh, whose mixed-timing receipt is discarded when it completes.
func (c *r154KalshiAdmissionSnapshotCache) Invalidate() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.have = false
	c.snapshot = r154KalshiAdmissionSnapshot{}
	c.epoch++
	c.mu.Unlock()
}

func (c *r154KalshiAdmissionSnapshotCache) Current(snapshot r154KalshiAdmissionSnapshot,
	now time.Time) bool {
	if c == nil || !snapshot.Fresh(now, c.maxAge) {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.have && snapshot.epoch == c.epoch &&
		snapshot.generation != 0 && snapshot.generation == c.snapshot.generation
}

func (c *r154KalshiAdmissionSnapshotCache) CurrentForExecution(snapshot r154KalshiAdmissionSnapshot,
	now time.Time) bool {
	if c == nil || !snapshot.Fresh(now, r154KalshiAdmissionSnapshotExecutionLease) {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.have && snapshot.epoch == c.epoch &&
		snapshot.generation != 0 && snapshot.generation == c.snapshot.generation
}

func (c *r154KalshiAdmissionSnapshotCache) Consume(snapshot r154KalshiAdmissionSnapshot,
	now time.Time) bool {
	return c.ConsumeDetailed(snapshot, now).Consumed
}

func (c *r154KalshiAdmissionSnapshotCache) ConsumeDetailed(snapshot r154KalshiAdmissionSnapshot,
	now time.Time) r154KalshiAdmissionConsumeReceipt {
	out := newR154KalshiAdmissionConsumeReceipt(snapshot, now)
	if c == nil {
		out.Failure = r154KalshiAdmissionConsumeCacheUnavailable
		return out
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out.CurrentEpoch, out.CacheHave = c.epoch, c.have
	if c.have {
		out.CurrentGeneration = c.snapshot.generation
	}
	switch {
	case snapshot.epoch != c.epoch:
		out.Failure = r154KalshiAdmissionConsumeEpochInvalidated
		return out
	case !c.have:
		out.Failure = r154KalshiAdmissionConsumeCacheEmpty
		return out
	case snapshot.generation == 0 || snapshot.generation != c.snapshot.generation:
		out.Failure = r154KalshiAdmissionConsumeSuperseded
		return out
	case out.SnapshotAgeNS < 0:
		out.Failure = r154KalshiAdmissionConsumeClockRegression
		return out
	case !snapshot.Fresh(now, r154KalshiAdmissionSnapshotExecutionLease):
		out.Failure = r154KalshiAdmissionConsumeExpired
		return out
	}
	c.have = false
	c.snapshot = r154KalshiAdmissionSnapshot{}
	c.epoch++
	out.Consumed = true
	out.Failure = r154KalshiAdmissionConsumeOK
	return out
}

// SnapshotContext returns a context carrying one fresh immutable snapshot. Downstream guards can
// call r154KalshiAdmissionSnapshotFromContext and avoid all repeated account REST calls.
func (c *r154KalshiAdmissionSnapshotCache) SnapshotContext(ctx context.Context) (
	context.Context, r154KalshiAdmissionSnapshot, error) {
	if c == nil || c.fetcher == nil {
		return ctx, r154KalshiAdmissionSnapshot{}, errR154KalshiAdmissionFetcherUnavailable
	}
	if ctx == nil {
		return nil, r154KalshiAdmissionSnapshot{},
			errors.New("kalshi admission account snapshot context unavailable")
	}
	if carried, ok := r154KalshiAdmissionSnapshotFromContext(ctx); ok &&
		c.Current(carried, c.now()) {
		return ctx, carried, nil
	}
	snapshot, err := c.Snapshot(ctx)
	if err != nil {
		return ctx, r154KalshiAdmissionSnapshot{}, err
	}
	return r154ContextWithKalshiAdmissionSnapshot(ctx, snapshot), snapshot, nil
}

func fetchR154KalshiAdmissionSnapshot(ctx context.Context, fetcher r154KalshiAdmissionFetcher,
	now func() time.Time) (r154KalshiAdmissionSnapshot, error) {
	if fetcher == nil {
		return r154KalshiAdmissionSnapshot{}, errR154KalshiAdmissionFetcherUnavailable
	}
	if ctx == nil {
		return r154KalshiAdmissionSnapshot{}, errors.New("kalshi admission account snapshot context unavailable")
	}
	if now == nil {
		now = time.Now
	}
	// No authenticated endpoint supplies one atomic cross-resource watermark. The request start is
	// therefore the conservative oldest possible observation time for all three parallel reads.
	// Stamping after wg.Wait would make an early balance/position response look newer than it is.
	observedAt := now()
	if observedAt.IsZero() {
		return r154KalshiAdmissionSnapshot{}, errR154KalshiAdmissionSnapshotIncomplete
	}

	var (
		balance      *kalshi.Balance
		positions    []kalshi.MarketPosition
		orders       []kalshi.Order
		balanceErr   error
		positionsErr error
		ordersErr    error
		wg           sync.WaitGroup
	)
	wg.Add(3)
	go func() {
		defer wg.Done()
		balance, balanceErr = fetcher.GetBalance(ctx)
	}()
	go func() {
		defer wg.Done()
		positions, positionsErr = fetcher.GetPositions(ctx)
	}()
	go func() {
		defer wg.Done()
		orders, ordersErr = fetcher.GetOrders(ctx)
	}()
	wg.Wait()

	if balanceErr != nil || positionsErr != nil || ordersErr != nil {
		return r154KalshiAdmissionSnapshot{}, fmt.Errorf(
			"kalshi admission account refresh failed: balance=%v positions=%v orders=%v",
			balanceErr, positionsErr, ordersErr,
		)
	}
	if balance == nil || balance.Balance < 0 || balance.PortfolioValue == nil ||
		*balance.PortfolioValue < 0 {
		return r154KalshiAdmissionSnapshot{}, errR154KalshiAdmissionSnapshotIncomplete
	}
	return r154KalshiAdmissionSnapshot{
		observedAt: observedAt,
		balance:    cloneR154KalshiBalance(*balance),
		positions:  append([]kalshi.MarketPosition(nil), positions...),
		orders:     append([]kalshi.Order(nil), orders...),
	}, nil
}

type r154KalshiAdmissionSnapshotContextKey struct{}

func r154ContextWithKalshiAdmissionSnapshot(ctx context.Context,
	snapshot r154KalshiAdmissionSnapshot) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, r154KalshiAdmissionSnapshotContextKey{}, snapshot.clone())
}

func r154KalshiAdmissionSnapshotFromContext(ctx context.Context) (
	r154KalshiAdmissionSnapshot, bool) {
	if ctx == nil {
		return r154KalshiAdmissionSnapshot{}, false
	}
	snapshot, ok := ctx.Value(r154KalshiAdmissionSnapshotContextKey{}).(r154KalshiAdmissionSnapshot)
	if !ok || snapshot.observedAt.IsZero() || snapshot.balance.Balance < 0 ||
		snapshot.balance.PortfolioValue == nil || *snapshot.balance.PortfolioValue < 0 {
		return r154KalshiAdmissionSnapshot{}, false
	}
	return snapshot.clone(), true
}

func r154FreshKalshiAdmissionSnapshotFromContext(ctx context.Context, now time.Time,
	maxAge time.Duration) (r154KalshiAdmissionSnapshot, bool) {
	snapshot, ok := r154KalshiAdmissionSnapshotFromContext(ctx)
	if !ok || !snapshot.Fresh(now, maxAge) {
		return r154KalshiAdmissionSnapshot{}, false
	}
	return snapshot, true
}
