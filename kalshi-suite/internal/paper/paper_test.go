package paper

// P3 tests (audit §7 "no tests for P&L units / fees / settlement zero-value"): pin the accounting
// invariants the audits found violated elsewhere — per-contract × contracts units, fee-net wins,
// and the zero-value settlement path — so a regression breaks the build, not the P&L.

import (
	"math"
	"testing"
)

func approx(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

// Units: realized P&L must be (exit − avg entry) × contracts — round dollars, not per-contract.
func TestAggregateRealizedUnits(t *testing.T) {
	fills := []Fill{
		{TS: "2026-01-01T00:00:00Z", Platform: "kalshi", Ticker: "T", Side: "YES", Action: "BUY", Price: 0.40, Contracts: 10},
		{TS: "2026-01-01T01:00:00Z", Platform: "kalshi", Ticker: "T", Side: "YES", Action: "SELL", Price: 0.55, Contracts: 10},
	}
	pos, sum := Aggregate(fills)
	if len(pos) != 0 {
		t.Fatalf("expected flat book, got %d open positions", len(pos))
	}
	approx(t, "sum.Realized", sum.Realized, 10*(0.55-0.40)) // $1.50, not $0.15
}

// Average-cost basis: a partial sell realizes against the running average, remainder stays open.
func TestAggregatePartialSellAvgCost(t *testing.T) {
	fills := []Fill{
		{TS: "2026-01-01T00:00:00Z", Platform: "kalshi", Ticker: "T", Side: "YES", Action: "BUY", Price: 0.40, Contracts: 10},
		{TS: "2026-01-01T00:30:00Z", Platform: "kalshi", Ticker: "T", Side: "YES", Action: "BUY", Price: 0.60, Contracts: 10}, // avg 0.50
		{TS: "2026-01-01T01:00:00Z", Platform: "kalshi", Ticker: "T", Side: "YES", Action: "SELL", Price: 0.70, Contracts: 5},
	}
	pos, sum := Aggregate(fills)
	if len(pos) != 1 {
		t.Fatalf("expected 1 open position, got %d", len(pos))
	}
	approx(t, "realized", sum.Realized, 5*(0.70-0.50))
	approx(t, "open contracts", pos[0].Contracts, 15)
	approx(t, "avg price", pos[0].AvgPrice, 0.50)
	approx(t, "cost basis", pos[0].CostBasis, 15*0.50)
}

// Settlement zero-value (audit #7): a lost position settling at $0.00 books the FULL loss —
// exit price 0, realized = −cost, Win=false. This is the path the Poly-US zero-settle corruption
// abused; the accounting itself must stay exact.
func TestZeroValueSettlementBooksFullLoss(t *testing.T) {
	fills := []Fill{
		{TS: "2026-01-01T00:00:00Z", Platform: "polyus", Ticker: "L", Side: "YES", Action: "BUY", Price: 0.60, Contracts: 5},
		{TS: "2026-01-02T00:00:00Z", Platform: "polyus", Ticker: "L", Side: "YES", Action: "SELL", Price: 0.0, Contracts: 5, Source: "settled-loss"},
	}
	_, sum := Aggregate(fills)
	approx(t, "realized", sum.Realized, -3.0)
	ct := ClosedTrades(fills)
	if len(ct) != 1 {
		t.Fatalf("expected 1 closed round, got %d", len(ct))
	}
	approx(t, "exit price", ct[0].ExitPrice, 0)
	if ct[0].Win {
		t.Fatal("zero-value settlement must not count as a win")
	}
	if ct[0].ExitSource != "settled-loss" {
		t.Fatalf("exit source = %q, want settled-loss", ct[0].ExitSource)
	}
}

// Fee-net win (audit Q1 #4): a round that's up gross but down after fees is a LOSS.
func TestClosedTradeWinIsFeeNet(t *testing.T) {
	mk := func(exit float64, feeEach float64) []Fill {
		return []Fill{
			{TS: "2026-01-01T00:00:00Z", Platform: "kalshi", Ticker: "W", Side: "YES", Action: "BUY", Price: 0.50, Contracts: 10, Fee: feeEach},
			{TS: "2026-01-01T01:00:00Z", Platform: "kalshi", Ticker: "W", Side: "YES", Action: "SELL", Price: exit, Contracts: 10, Fee: feeEach},
		}
	}
	// gross +$0.30, fees $0.70 → net −$0.40 → NOT a win
	if ct := ClosedTrades(mk(0.53, 0.35)); len(ct) != 1 || ct[0].Win {
		t.Fatal("gross-up/net-down round must be a loss (fee-net win)")
	}
	// gross +$2.00, fees $0.70 → net +$1.30 → win
	if ct := ClosedTrades(mk(0.70, 0.35)); len(ct) != 1 || !ct[0].Win {
		t.Fatal("net-positive round must be a win")
	}
}

// R89 (auditor bug 42): a duplicate settlement SELL against an already-flat lot must NOT record a
// phantom zero-quantity loss round — it deflated the win-rates the placement policy consumes.
func TestStatsSkipsPhantomZeroQtyRound(t *testing.T) {
	fills := []Fill{
		{TS: "2026-01-01T00:00:00Z", Platform: "kalshi", Ticker: "D", Side: "YES", Action: "BUY", Price: 0.40, Contracts: 10, Source: "auto-signal"},
		{TS: "2026-01-01T01:00:00Z", Platform: "kalshi", Ticker: "D", Side: "YES", Action: "SELL", Price: 1.0, Contracts: 10, Source: "settled-win"},
		// duplicate settle (push + sweep race replayed from history)
		{TS: "2026-01-01T01:00:05Z", Platform: "kalshi", Ticker: "D", Side: "YES", Action: "SELL", Price: 1.0, Contracts: 10, Source: "settled-win"},
	}
	st := Stats(fills)
	if st.ClosedTrades != 1 {
		t.Fatalf("ClosedTrades = %d, want 1 (phantom zero-qty round recorded)", st.ClosedTrades)
	}
	if st.Wins != 1 {
		t.Fatalf("Wins = %d, want 1", st.Wins)
	}
	ct := ClosedTrades(fills)
	if len(ct) != 1 {
		t.Fatalf("ClosedTrades() rows = %d, want 1 (phantom trade emitted)", len(ct))
	}
	approx(t, "round contracts", ct[0].Contracts, 10)
}

// R89 (auditor bug 53): a flat→reopen lot must not inherit the previous lot's TP/SL/source/fees —
// the stop sweep could instantly auto-exit a fresh entry on the OLD lot's stops.
func TestAggregateResetsLotOnReopen(t *testing.T) {
	fills := []Fill{
		{TS: "2026-01-01T00:00:00Z", Platform: "kalshi", Ticker: "R", Side: "YES", Action: "BUY", Price: 0.40, Contracts: 10, Fee: 0.35, TP: 0.90, SL: 0.10, Source: "whale"},
		{TS: "2026-01-01T01:00:00Z", Platform: "kalshi", Ticker: "R", Side: "YES", Action: "SELL", Price: 0.55, Contracts: 10, Fee: 0.10},
		{TS: "2026-01-01T02:00:00Z", Platform: "kalshi", Ticker: "R", Side: "YES", Action: "BUY", Price: 0.50, Contracts: 4, Fee: 0.20, Source: "gate"},
	}
	pos, sum := Aggregate(fills)
	if len(pos) != 1 {
		t.Fatalf("expected 1 open position, got %d", len(pos))
	}
	p := pos[0]
	if p.TP != 0 || p.SL != 0 {
		t.Fatalf("re-opened lot inherited stale stops: tp=%v sl=%v", p.TP, p.SL)
	}
	if p.Source != "gate" {
		t.Fatalf("re-opened lot source = %q, want gate (stale carryover)", p.Source)
	}
	approx(t, "reopened lot fees", p.Fees, 0.20)
	approx(t, "reopened lot realized", p.Realized, 0)
	approx(t, "sum.Realized keeps the closed round", sum.Realized, 10*(0.55-0.40))
	approx(t, "sum.Fees still counts every fill", sum.Fees, 0.35+0.10+0.20)
}

// R89 (auditor bug 54): RealizedSince must be FEE-NET (like RealizedSeries and the fee-net win
// convention) — the daily-loss breaker consumed a gross value and undercounted real losses.
func TestRealizedSinceIsFeeNet(t *testing.T) {
	fills := []Fill{
		{TS: "2026-01-02T01:00:00Z", Platform: "kalshi", Ticker: "F", Side: "YES", Action: "BUY", Price: 0.50, Contracts: 10, Fee: 0.35},
		{TS: "2026-01-02T02:00:00Z", Platform: "kalshi", Ticker: "F", Side: "YES", Action: "SELL", Price: 0.60, Contracts: 10, Fee: 0.35},
	}
	got := RealizedSince(fills, "2026-01-02T00:00:00Z")
	approx(t, "fee-net realized since", got, 10*(0.60-0.50)-0.70)
	// fills before the window: their fees must NOT count against today
	got = RealizedSince(fills, "2026-01-02T01:30:00Z")
	approx(t, "only in-window fees", got, 10*(0.60-0.50)-0.35)
}

// Oversell guard: selling more than held realizes only what was held (no negative inventory).
func TestAggregateOversellClamped(t *testing.T) {
	fills := []Fill{
		{TS: "2026-01-01T00:00:00Z", Platform: "kalshi", Ticker: "O", Side: "NO", Action: "BUY", Price: 0.30, Contracts: 3},
		{TS: "2026-01-01T01:00:00Z", Platform: "kalshi", Ticker: "O", Side: "NO", Action: "SELL", Price: 1.0, Contracts: 999},
	}
	pos, sum := Aggregate(fills)
	if len(pos) != 0 {
		t.Fatalf("expected flat book, got %d positions", len(pos))
	}
	approx(t, "realized", sum.Realized, 3*(1.0-0.30))
}
