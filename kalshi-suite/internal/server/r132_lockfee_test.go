package server

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

// TestR132LockStackAggregateFeeRounding pins the actual two-venue receipts at the three sizes
// LockStack uses most often. In particular, n=2 and n=10 must not multiply the one-contract fee.
func TestR132LockStackAggregateFeeRounding(t *testing.T) {
	s := testServer(t)
	// A successfully loaded change log with no series row means Kalshi's 0.07 taker default.
	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{}
	s.kalFeesAt = time.Now()
	s.kalFeeMu.Unlock()

	c := xvlCandidate{pair: "K-PUS", aVenue: "kalshi", aID: "KLOCK-TEST",
		bVenue: "polyus", bID: "pus-lock"}
	one := s.xvlAggregateStake(c, xvlOrient{aAsk: 0.46, bAsk: 0.47}, 1)
	o := xvlOrient{aAsk: 0.46, bAsk: 0.47, feeA: one.FeeA, feeB: one.FeeB,
		marginC: one.MarginC}

	tests := []struct {
		n                              float64
		feeA, feeB, cost, marginC, pnl float64
	}{
		{n: 1, feeA: 0.02, feeB: 0.01, cost: 0.96, marginC: 4.0, pnl: 0.04},
		{n: 2, feeA: 0.04, feeB: 0.03, cost: 1.93, marginC: 3.5, pnl: 0.07},
		{n: 10, feeA: 0.18, feeB: 0.15, cost: 9.63, marginC: 3.7, pnl: 0.37},
	}
	for _, tc := range tests {
		t.Run(string(rune('0'+int(tc.n))), func(t *testing.T) {
			got := s.xvlAggregateStake(c, o, tc.n)
			assertNear := func(name string, actual, want float64) {
				t.Helper()
				if math.Abs(actual-want) > 1e-9 {
					t.Fatalf("%s = %.12f, want %.12f", name, actual, want)
				}
			}
			assertNear("fee A", got.FeeA, tc.feeA)
			assertNear("fee B", got.FeeB, tc.feeB)
			assertNear("cost", got.Cost, tc.cost)
			assertNear("margin cents", got.MarginC, tc.marginC)

			op := xvLockOpp{AAsk: o.aAsk, BAsk: o.bAsk, FeeA: o.feeA, FeeB: o.feeB,
				Staked: tc.n, StakeFeeA: got.FeeA, StakeFeeB: got.FeeB, StakeFeesExact: true,
				StakedPnL: tc.pnl, SettledTS: time.Now().UTC().Format(time.RFC3339)}
			assertNear("stored exposure", op.stakedCostUSD(), tc.cost)
			assertNear("stored realized", op.stakedNetUSD(), tc.pnl)
		})
	}

	if got, multiplied := s.xvlAggregateStake(c, o, 2).FeeB, one.FeeB*2; got == multiplied {
		t.Fatalf("n=2 PolyUS fee incorrectly multiplied one-contract rounding: %.2f", got)
	}
	if got, multiplied := s.xvlAggregateStake(c, o, 10).FeeA, one.FeeA*10; got == multiplied {
		t.Fatalf("n=10 Kalshi fee incorrectly multiplied one-contract rounding: %.2f", got)
	}
	// Research fields remain exactly the one-contract quote even when the money slice is n=10.
	if math.Abs(o.feeA-0.02) > 1e-9 || math.Abs(o.feeB-0.01) > 1e-9 || math.Abs(o.marginC-4.0) > 1e-9 {
		t.Fatalf("one-contract research quote mutated: %+v", o)
	}
}

func TestR132LockStackLegacyRowsRemainReadable(t *testing.T) {
	op := xvLockOpp{AAsk: 0.45, BAsk: 0.50, FeeA: 0.01, FeeB: 0.01,
		RealizedC: 3, Staked: 5} // pre-R132 JSON has no exact aggregate fields
	if got := op.stakedCostUSD(); math.Abs(got-4.85) > 1e-9 {
		t.Fatalf("legacy exposure = %.4f, want 4.85", got)
	}
	if got := op.stakedNetUSD(); math.Abs(got-0.15) > 1e-9 {
		t.Fatalf("legacy realized = %.4f, want 0.15", got)
	}
}

func TestR132LockStackStoresAndReadsExactStakedPnL(t *testing.T) {
	s := testServer(t)
	s.bootAt = time.Now().Add(-time.Minute)
	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{}
	s.kalFeesAt = time.Now()
	s.kalFeeMu.Unlock()

	c := xvlCandidate{pair: "K-PUS", aVenue: "kalshi", aID: "KLOCK-TEST",
		bVenue: "polyus", bID: "pus-lock"}
	o := xvlOrient{aAsk: 0.46, bAsk: 0.47}
	terms := s.xvlAggregateStake(c, o, 2)
	s.xvlMu.Lock()
	b := s.xvlLoadLocked()
	b.Open = append(b.Open, xvLockOpp{Key: "K-PUS|A|B|YESa+NOb", Pair: "K-PUS",
		AVenue: "kalshi", AID: "KLOCK-TEST", ASide: "YES",
		BVenue: "polyus", BID: "pus-lock", BSide: "NO",
		AAsk: 0.46, BAsk: 0.47, FeeA: 0.02, FeeB: 0.01, RealizedC: 4,
		Staked: 2, StakeFeeA: terms.FeeA, StakeFeeB: terms.FeeB, StakeFeesExact: true,
		AWon: 1, BWon: 0})
	s.xvlDirty = true
	s.xvlMu.Unlock()

	if usd, lots, ok := s.bookOpenExposure(context.Background(), vbCombos); !ok || lots != 1 || math.Abs(usd-1.93) > 1e-9 {
		t.Fatalf("exact open exposure = %.4f/%d/%v, want 1.93/1/true", usd, lots, ok)
	}
	s.settleXvLocks(context.Background())
	s.xvlMu.Lock()
	if len(b.Closed) != 1 {
		s.xvlMu.Unlock()
		t.Fatalf("closed rows = %d, want 1", len(b.Closed))
	}
	closed := b.Closed[0]
	s.xvlMu.Unlock()
	if math.Abs(closed.StakedPnL-0.07) > 1e-9 || math.Abs(closed.stakedNetUSD()-0.07) > 1e-9 {
		t.Fatalf("stored exact PnL = %.4f (reader %.4f), want 0.07", closed.StakedPnL, closed.stakedNetUSD())
	}
	if net, ok := s.bookAllTimeNet(context.Background(), vbCombos); !ok || math.Abs(net-0.07) > 1e-9 {
		t.Fatalf("Combos net = %.4f/%v, want 0.07/true", net, ok)
	}
	if net, rounds, ok := s.bookSessionClosedStats(context.Background(), vbCombos); !ok || rounds != 1 || math.Abs(net-0.07) > 1e-9 {
		t.Fatalf("session net = %.4f/%d/%v, want 0.07/1/true", net, rounds, ok)
	}
}

func TestR132PolyUSUsesPerMarketFeeCoefficient(t *testing.T) {
	theta := 0.10
	standard := 0.06
	zero := 0.0
	s := &Server{polyUSMkts: []polyUSMarket{
		{Slug: "fee-ten", FeeCoeff: &theta},
		{Slug: "standard", FeeCoeff: &standard},
		{Slug: "fee-free", FeeCoeff: &zero},
	}}
	if got := s.polyUSFeeFor("fee-ten", false, 100, 0.50); got != 2.50 {
		t.Fatalf("10%% market taker fee = %.2f, want 2.50", got)
	}
	if got := s.polyUSFeeFor("fee-free", false, 100, 0.50); got != 0 {
		t.Fatalf("explicit fee-free market = %.2f, want 0", got)
	}
	if got := s.polyUSFeeFor("schema-absent", false, 100, 0.50); got != 1.50 {
		t.Fatalf("absent-schema fallback = %.2f, want exchange default 1.50", got)
	}
	if got := s.polyUSFeeFor("standard", true, 100, 0.50); got != -0.31 {
		t.Fatalf("standard-schedule maker rebate = %.2f, want -0.31 with banker's cent rounding", got)
	}
	if got := s.polyUSFeeFor("fee-ten", true, 100, 0.50); got != 0 {
		t.Fatalf("custom-theta market inherited standard maker rebate: %.2f", got)
	}
	if got := s.polyUSFeeFor("fee-free", true, 100, 0.50); got != 0 {
		t.Fatalf("fee-free market inherited standard maker rebate: %.2f", got)
	}
}

func TestR132PolyUSLiveFeeRequiresFreshValidCoefficient(t *testing.T) {
	standard, custom, zero, invalid := 0.06, 0.10, 0.0, -0.01
	for _, tc := range []struct {
		name  string
		m     polymarketus.Market
		maker bool
		want  float64
		ok    bool
	}{
		{name: "missing", m: polymarketus.Market{}, ok: false},
		{name: "invalid", m: polymarketus.Market{FeeCoeff: &invalid}, ok: false},
		{name: "standard taker", m: polymarketus.Market{FeeCoeff: &standard}, want: 1.50, ok: true},
		{name: "standard maker", m: polymarketus.Market{FeeCoeff: &standard}, maker: true, want: -0.31, ok: true},
		{name: "custom taker", m: polymarketus.Market{FeeCoeff: &custom}, want: 2.50, ok: true},
		{name: "custom maker no assumed rebate", m: polymarketus.Market{FeeCoeff: &custom}, maker: true, want: 0, ok: true},
		{name: "fee-free taker", m: polymarketus.Market{FeeCoeff: &zero}, want: 0, ok: true},
		{name: "fee-free maker", m: polymarketus.Market{FeeCoeff: &zero}, maker: true, want: 0, ok: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := polyUSLiveFeeForMarket(tc.m, tc.maker, 100, 0.50)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("fee = %.2f/%v, want %.2f/%v", got, ok, tc.want, tc.ok)
			}
		})
	}
}
