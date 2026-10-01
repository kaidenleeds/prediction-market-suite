package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"
)

// ── R85 wedge regression: ctx-deadline-exceeded during limited calls must return permits ────────
// The 2026-07-05 12h REST wedge hypothesis was "permits acquired but never returned on ctx
// expiry". The bucket's invariant is stronger: tokens are debited ONLY at grant (waitFloor's
// success branch), so a failed wait has nothing to return. This test PROVES the invariant: a
// storm of deadline-exceeded background waits on a drained bucket leaves the level exactly where
// refill says it should be, and the limiter stays fully usable afterwards.
func TestCtxExpiryReturnsPermits(t *testing.T) {
	b := New(10) // burst 10, 10 tokens/s
	for i := 0; i < 10; i++ { // drain the full burst at priority
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err := b.Wait(ctx)
		cancel()
		if err != nil {
			t.Fatalf("drain %d: %v", i, err)
		}
	}
	lvl0 := b.Level()

	// 40 BACKGROUND waits that all die on ctx-deadline/cancel while the tier is starved
	// (reserve 3 ⇒ they need the level to reach ≥ ~4 — unreachable inside this storm).
	for i := 0; i < 40; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		if i%5 == 0 {
			cancel() // mix in already-canceled ctxs — the other early-exit path
		}
		err := b.WaitBackground(ctx, 3)
		cancel()
		if err == nil {
			t.Fatalf("call %d got a token from a starved background tier", i)
		}
	}

	// Permits fully returned: the level may only have RISEN (refill), never dropped.
	if lvl := b.Level(); lvl < lvl0-0.01 {
		t.Fatalf("failed waits leaked tokens: level %.3f -> %.3f", lvl0, lvl)
	}
	// Limiter usable after N failures: a fresh priority wait succeeds promptly.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Wait(ctx); err != nil {
		t.Fatalf("limiter unusable after the ctx-expiry storm: %v", err)
	}
}

// ── R85 root cause: the background floor must SCALE with the penalized refill rate ──────────────
// Old behavior: background needed the ABSOLUTE level 1+reserve (cfg 20 ⇒ 7) while Penalize429
// could pin the refill at 4/s — at/below the suite's steady priority draw, so the level never
// climbed and the background tier passed ZERO calls for the whole episode (board pull dead → mkts
// chip 0/0, briefing marks dead). With the fix the floor shrinks proportionally (6 × 4/20 = 1.2),
// keeping the priority lane's ~30% headroom RATIO without zeroing background throughput.
func TestPenalizedFloorScalesWithEffectiveRate(t *testing.T) {
	b := New(20)
	for i := 0; i < 6; i++ { // pin the refill at the AIMD floor (max(2, 20%×20) = 4/s)
		b.mu.Lock()
		b.pen429At = time.Time{} // bypass the 500ms penalty throttle (test-only)
		b.mu.Unlock()
		b.Penalize429()
	}
	if r := b.EffectiveRate(); r > 4.01 {
		t.Fatalf("penalty did not pin the refill: %.2f/s", r)
	}
	// Sustained priority pressure keeps the level low — park it just above the SCALED need.
	b.mu.Lock()
	b.tokens = 2.5
	b.last = time.Now()
	b.mu.Unlock()
	// Old absolute floor: needs level 7 ⇒ (7−2.5)/4 ≈ 1.1s away ⇒ a 300ms wait was hopeless.
	// Scaled floor: 6×(4/20)=1.2 ⇒ needs 2.2 ≤ 2.5 ⇒ grants immediately.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := b.WaitBackground(ctx, 6); err != nil {
		t.Fatalf("background starved under a penalized refill (absolute-floor regression): %v", err)
	}
}

// A configured rate below 2/s made background waits MATHEMATICALLY impossible before the clamp
// (1 + reserve > bucket capacity ⇒ the level could never qualify): permanent wedge by config typo.
func TestBackgroundPossibleAtTinyRate(t *testing.T) {
	b := New(1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := b.WaitBackground(ctx, 10); err != nil {
		t.Fatalf("background impossible at rate 1/s (capacity clamp regression): %v", err)
	}
}

// ── R85: deadline-free PRIORITY waits are bounded too ────────────────────────────────────────────
// A priority Wait on a ctx with NO deadline (e.g. a browser request ctx) used to park FOREVER
// under starvation — while holding order-path mutexes. It now errors at the anti-wedge ceiling.
func TestPriorityNoDeadlineCeilingBounds(t *testing.T) {
	old := priNoDeadlineCeiling
	priNoDeadlineCeiling = 120 * time.Millisecond
	defer func() { priNoDeadlineCeiling = old }()

	b := New(1)
	if err := b.Wait(context.Background()); err != nil { // take the only token
		t.Fatalf("first token: %v", err)
	}
	b.mu.Lock() // starve hard: next token ~20s out, no recovery creep
	b.tokens = 0
	b.refillRate = 0.05
	b.cfgRate = 0.05
	b.last = time.Now()
	b.mu.Unlock()

	start := time.Now()
	err := b.Wait(context.Background())
	if err == nil {
		t.Fatal("deadline-free priority Wait should hit the anti-wedge ceiling, not get a token")
	}
	if !errors.Is(err, ErrStarved) {
		t.Fatalf("want ErrStarved, got %v", err)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("ceiling not enforced in time: waited %v", el)
	}
}

// Reset must rebuild the configured state (full burst, configured rate, penalties cleared) so the
// REST-health watchdog's self-heal actually un-wedges waiters on their next poll.
func TestResetRestoresConfiguredState(t *testing.T) {
	b := New(20)
	for i := 0; i < 5; i++ {
		b.mu.Lock()
		b.pen429At = time.Time{}
		b.mu.Unlock()
		b.Penalize429()
	}
	b.mu.Lock()
	b.tokens = 0
	b.mu.Unlock()

	b.Reset()
	if r := b.EffectiveRate(); r != 20 {
		t.Fatalf("rate not restored by Reset: %.2f", r)
	}
	if l := b.Level(); l < 19.5 {
		t.Fatalf("burst not restored by Reset: %.2f", l)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := b.Wait(ctx); err != nil {
		t.Fatalf("wait after Reset: %v", err)
	}
}
