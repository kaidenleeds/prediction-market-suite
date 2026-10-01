package server

// P3 tests (audit §7): kill-switch halt, live risk gating, idempotency keys, and the cache-bound
// helpers — the safety plumbing that must not regress silently.

import (
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/killswitch"
)

// A tripped kill switch must block (ksBlocked gates every trading loop and live order path).
func TestKillSwitchBlocks(t *testing.T) {
	s := &Server{ks: killswitch.New(nil)}
	if s.ksBlocked() {
		t.Fatal("fresh switch must not block")
	}
	s.ks.Trip("test")
	if !s.ksBlocked() {
		t.Fatal("tripped switch MUST block")
	}
	s.ks.Reset()
	if s.ksBlocked() {
		t.Fatal("reset switch must unblock")
	}
	// nil switch (not yet wired) must fail-safe to NOT panic and not block paper flows
	if (&Server{}).ksBlocked() {
		t.Fatal("nil switch must not block")
	}
}

// Live risk caps: per-order ceiling plus stop-loss truth. UTC-day turnover is reporting only.
func TestLiveRiskCheck(t *testing.T) {
	s := &Server{}
	tcfg := config.Config{}
	tcfg.Risk.LiveMaxOrderUSD = 2
	tcfg.Risk.LiveMaxDailyLossUSD = 10
	tcfg.Risk.LiveProspectiveAllocation = true
	tcfg.Risk.LiveSystemKalshi = true
	s.cfgP.Store(&tcfg) // R76 (bug 18): seed the copy-on-write snapshot
	s.liveBankArm = map[string]float64{"kalshi": 10}
	if msg := s.liveRiskCheck(1.50, "kalshi"); msg != "" {
		t.Fatalf("$1.50 under a $2 cap must pass, got %q", msg)
	}
	if msg := s.liveRiskCheck(2.50, "kalshi"); !strings.Contains(msg, "active live order rail") {
		t.Fatalf("$2.50 over a $2 cap must fail with the per-order reason, got %q", msg)
	}
	// Accepted-cost turnover remains visible, but the operator removed its old daily allowance.
	s.liveRiskBook(9, "kalshi")
	if msg := s.liveRiskCheck(2, "kalshi"); msg != "" {
		t.Fatalf("reported turnover must not block an otherwise valid order, got %q", msg)
	}
	if msg := s.liveRiskCheck(0.5, "polyus"); !strings.Contains(msg, "no enabled LIVE capital authority") {
		t.Fatalf("disabled PolyUS must not borrow Kalshi capital, got %q", msg)
	}
	// Turnover is still split by venue for visibility.
	if got := s.liveVenueDayTurnover("kalshi"); got != 9 {
		t.Fatalf("kalshi day bucket = %v, want 9", got)
	}
	if got := s.liveVenueDayTurnover("polyus"); got != 0 {
		t.Fatalf("polyus day bucket = %v, want 0", got)
	}
}

// Idempotency keys: deterministic for the same intent, distinct across intents, prefix kept.
func TestIdemKeyDeterministic(t *testing.T) {
	a := idemKey("live-", "KXBTC", "bid", "55", "2", "2026-07-02T10:15")
	b := idemKey("live-", "KXBTC", "bid", "55", "2", "2026-07-02T10:15")
	if a != b {
		t.Fatalf("same intent must produce the same key: %q vs %q", a, b)
	}
	if !strings.HasPrefix(a, "live-") || len(a) != len("live-")+20 {
		t.Fatalf("key shape wrong: %q", a)
	}
	if c := idemKey("live-", "KXBTC", "bid", "56", "2", "2026-07-02T10:15"); c == a {
		t.Fatal("different price must produce a different key")
	}
	if d := idemKey("auto-", "KXBTC", "bid", "55", "2", "2026-07-02T10:15"); strings.TrimPrefix(d, "auto-") == strings.TrimPrefix(a, "live-") {
		// same parts, different prefix — hash equality is fine, full key must differ
		if d == a {
			t.Fatal("prefix must differentiate keys")
		}
	}
}

// pruneTimeMap: under the soft cap nothing is touched; over it, stale entries go, fresh stay.
func TestPruneTimeMap(t *testing.T) {
	m := map[string]time.Time{}
	old := time.Now().Add(-2 * time.Hour)
	for i := 0; i < 6; i++ {
		m[strings.Repeat("o", i+1)] = old
	}
	m["fresh"] = time.Now()
	pruneTimeMap(m, func(v time.Time) time.Time { return v }, 100, time.Hour)
	if len(m) != 7 {
		t.Fatalf("under cap must not prune, len=%d", len(m))
	}
	pruneTimeMap(m, func(v time.Time) time.Time { return v }, 3, time.Hour)
	if len(m) != 1 {
		t.Fatalf("over cap must drop stale entries, len=%d", len(m))
	}
	if _, ok := m["fresh"]; !ok {
		t.Fatal("fresh entry must survive the prune")
	}
}

func TestCapMap(t *testing.T) {
	m := map[string]float64{}
	for i := 0; i < 10; i++ {
		m[strings.Repeat("k", i+1)] = float64(i)
	}
	if got := capMap(m, 100); len(got) != 10 {
		t.Fatal("under hard cap must be returned unchanged")
	}
	if got := capMap(m, 5); len(got) != 0 {
		t.Fatal("over hard cap must reset (rolling baselines rebuild next refresh)")
	}
}

// Config sanity: the one-portfolio + honest-fill defaults the suite now assumes.
func TestConfigDefaults(t *testing.T) {
	cfg := config.Default()
	if !cfg.Auto.MLDrivenDirectional {
		t.Fatal("ml_driven_directional must default ON (one-portfolio mode)")
	}
	if cfg.Risk.LiveMaxOrderPct <= 0 || cfg.Risk.LiveExposureCapPct <= 0 || cfg.Risk.LiveCryptoCapPct <= 0 || cfg.Risk.LiveDailyLossPct <= 0 ||
		cfg.Risk.LiveKellyMaxFrac <= 0 || cfg.Risk.LiveKellyMaxFrac > 0.50 {
		t.Fatal("bankroll-proportional live rails must default to non-zero (fail-safe)")
	}
}
