// Package ratelimit provides a minimal token-bucket limiter using only the
// standard library. One token is consumed per request; the bucket refills at a
// fixed rate and allows a small burst equal to the per-second rate.
package ratelimit

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Bucket struct {
	mu         sync.Mutex
	tokens     float64
	max        float64
	refillRate float64 // CURRENT tokens per second (adaptive — see Penalize429)
	cfgRate    float64 // the configured ceiling refillRate recovers toward
	last       time.Time
	pen429At   time.Time // last 429 penalty (rate-limits the penalty itself)
}

// New returns a bucket refilling at ratePerSec tokens/second (min 1).
func New(ratePerSec float64) *Bucket {
	if ratePerSec <= 0 {
		ratePerSec = 1
	}
	return &Bucket{
		tokens:     ratePerSec,
		max:        ratePerSec,
		refillRate: ratePerSec,
		cfgRate:    ratePerSec,
		last:       time.Now(),
	}
}

// Penalize429 is the venue's own pushback signal (R85, 2026-07-05 universal stall): when
// rate_limit_per_sec is configured ABOVE the account's real cap, every pass 429s and each request
// retries up to 5 attempts — each attempt burning a FRESH token — so attempted demand amplified
// 2-5× past both the client budget and the venue cap, and the background tier starved for hours.
// On a 429 the effective refill halves (AIMD; floored at max(2/s, 20% of configured)) and then
// recovers ~2%/s toward the configured ceiling inside refillLocked — the client self-tunes to the
// venue's REAL throughput instead of hammering it. Penalty applies at most once per 500ms so one
// burst of concurrent 429s counts as one signal, not a collapse to the floor.
func (b *Bucket) Penalize429() {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if now.Sub(b.pen429At) < 500*time.Millisecond {
		return
	}
	b.pen429At = now
	floor := b.cfgRate * 0.2
	if floor < 2 {
		floor = 2
	}
	if r := b.refillRate / 2; r > floor {
		b.refillRate = r
	} else {
		b.refillRate = floor
	}
}

// EffectiveRate reports the current (possibly 429-penalized) refill rate — observability only.
func (b *Bucket) EffectiveRate() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.refillRate
}

// SetRate — R106 (auditor bug 272): hot-apply a NEW configured ceiling. The settings knob
// kalshi_rate_limit_per_sec was accepted + persisted + echoed but never reached the running
// bucket (built once at boot) — a silent no-op. Semantics mirror New(): the ceiling, burst cap
// and current refill move to the new rate (a live 429 penalty is cleared — the next 429 re-tunes
// against the NEW ceiling); tokens clamp to the new max so lowering the rate can't leave an
// oversized burst in flight.
func (b *Bucket) SetRate(ratePerSec float64) {
	if ratePerSec <= 0 {
		ratePerSec = 1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(time.Now())
	b.cfgRate = ratePerSec
	b.max = ratePerSec
	b.refillRate = ratePerSec
	if b.tokens > b.max {
		b.tokens = b.max
	}
}

// Rate reports the configured ceiling (observability/tests).
func (b *Bucket) Rate() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cfgRate
}

// Level reports the current token level (refilled to now) — observability/tests only.
// TOKEN-ACCOUNTING INVARIANT (R85 wedge audit): tokens are debited ONLY at the instant a wait is
// GRANTED (waitFloor's success branch); a waiter that exits on ctx-expiry/ErrStarved has taken
// nothing, so there is no reservation to return and no leak path — N failed waits leave the level
// exactly where refill says it should be. This accessor exists so tests can PROVE that.
func (b *Bucket) Level() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(time.Now())
	return b.tokens
}

// Reset rebuilds the token bucket to its configured state: full burst, configured refill rate,
// penalty clock cleared. R85 SELF-HEAL hook (2026-07-05, 12h universal REST wedge): the REST-health
// watchdog calls this when no Kalshi HTTP response has landed for >120s while the WS is alive —
// whatever pathological level/rate state the bucket is in, waiters see a full bucket on their next
// poll (they re-lock and re-check every cycle, so no wakeup signal is needed). Safe under load:
// worst case is one over-budget burst that the venue answers with 429s, which Penalize429 absorbs.
func (b *Bucket) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tokens = b.max
	b.refillRate = b.cfgRate
	b.last = time.Now()
	b.pen429At = time.Time{}
}

// refillLocked advances the token count to now and creeps a penalized rate back toward the
// configured ceiling (+2%/s of the ceiling). Callers must hold b.mu.
func (b *Bucket) refillLocked(now time.Time) {
	dt := now.Sub(b.last).Seconds()
	if dt < 0 {
		dt = 0
	}
	if b.refillRate < b.cfgRate {
		b.refillRate += b.cfgRate * 0.02 * dt
		if b.refillRate > b.cfgRate {
			b.refillRate = b.cfgRate
		}
	}
	b.tokens += dt * b.refillRate
	if b.tokens > b.max {
		b.tokens = b.max
	}
	b.last = now
}

// Wait blocks until a token is available or ctx is cancelled.
func (b *Bucket) Wait(ctx context.Context) error { return b.waitFloor(ctx, 0) }

// WaitBackground blocks like Wait but only takes a token while at least `reserve` tokens remain
// afterwards — the PRIORITY TIER (audit #12/§9): background research polling may only spend the
// budget's surplus, so orders/cancels/marks (which use Wait) always find headroom instead of
// queueing behind research pulls. reserve is clamped to half the bucket.
func (b *Bucket) WaitBackground(ctx context.Context, reserve float64) error {
	if reserve < 0 {
		reserve = 0
	}
	if reserve > b.max/2 {
		reserve = b.max / 2
	}
	return b.waitFloor(ctx, reserve)
}

// ErrStarved is returned when a wait with NO ctx deadline outlives its tier's hard ceiling.
// R85 (2026-07-05 universal stall, auditor bug 24): a background waiter on an app-lifetime ctx
// blocked FOREVER while the tier was starved — and every auto tick that hit one wedged in-flight
// for hours. A deadline-less background wait now errors at 90s: callers degrade to cached data
// and retry next tick, exactly like a ctx-deadline expiry. R85 wedge audit: the PRIORITY tier is
// no longer fully exempt either — a priority Wait on a deadline-free ctx (e.g. a browser request
// ctx, which carries NO deadline) parked FOREVER under starvation while HOLDING livePlaceMu-class
// mutexes, wedging every sibling path. Risk-reducing calls still queue patiently, but bounded: at
// 5 minutes (vastly past any sane order latency) they error out instead of deadlocking the suite.
var ErrStarved = errors.New("ratelimit: wait starved past its no-deadline ceiling (limiter oversubscribed)")

// Ceilings for waiters whose ctx has NO deadline. Package vars (not consts) so tests can shrink
// them; production never mutates these.
var (
	bgNoDeadlineCeiling  = 90 * time.Second
	priNoDeadlineCeiling = 5 * time.Minute
)

func (b *Bucket) waitFloor(ctx context.Context, floor float64) error {
	var ceiling time.Time
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		if floor > 0 {
			ceiling = time.Now().Add(bgNoDeadlineCeiling)
		} else {
			ceiling = time.Now().Add(priNoDeadlineCeiling)
		}
	}
	for {
		b.mu.Lock()
		now := time.Now()
		b.refillLocked(now)
		// R85 ROOT-CAUSE FIX (2026-07-05, 12h universal REST wedge): the background floor was an
		// ABSOLUTE token level derived from the CONFIGURED rate (client reserve = 0.3×cfg, so with
		// cfg=20 background needed the level to reach 7). Two failure modes wedged the whole tier:
		//   (1) Penalize429 pins the refill at max(2, 0.2×cfg) — at/below the suite's steady
		//       priority draw (~5/s of settles/balance/orders), so priority took every token the
		//       moment it appeared at level 1 and the level could NEVER climb to 1+reserve: the
		//       background tier passed ZERO calls for the entire episode (board pull dead → chip
		//       0/0, briefing marks dead, discovery dead) even though capacity existed.
		//   (2) with cfg < 2 the bucket CAPACITY itself is below 1+reserve — background was
		//       mathematically impossible forever.
		// The floor now SCALES with the current refill (penalized bucket ⇒ proportionally smaller
		// reserve — the priority lane keeps the same ~30% headroom RATIO it was designed to have)
		// and is clamped so 1+floor always fits within capacity.
		eff := floor
		if eff > 0 {
			if b.cfgRate > 0 {
				eff *= b.refillRate / b.cfgRate
			}
			if hi := b.max - 1; eff > hi {
				eff = hi
			}
			if eff < 0 {
				eff = 0
			}
		}
		if b.tokens >= 1+eff {
			b.tokens--
			b.mu.Unlock()
			return nil
		}
		wait := time.Duration((1 + eff - b.tokens) / b.refillRate * float64(time.Second))
		b.mu.Unlock()
		if wait < time.Millisecond {
			wait = time.Millisecond // never busy-spin on rounding dust
		}

		if !ceiling.IsZero() {
			if now.After(ceiling) {
				return ErrStarved
			}
			if rem := ceiling.Sub(now); wait > rem {
				wait = rem
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
