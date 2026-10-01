package server

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func TestR154GolfFundedClockUsesLaterResultBoundary(t *testing.T) {
	s := testServer(t)
	s.paperHorizonFn = nil
	now := time.Now().UTC()
	s.metaMu.Lock()
	s.kmkts = map[string]kalshi.Market{}
	s.kmkts["KXPGA3BALL-R154"] = kalshi.Market{
		Ticker:             "KXPGA3BALL-R154",
		EventTicker:        "KXPGA3BALL",
		ExpectedExpiration: now.Add(2 * time.Hour).Format(time.RFC3339),
		CloseTime:          now.Add(36 * time.Hour).Format(time.RFC3339),
	}
	s.kmkts["KXTENNIS-R154"] = kalshi.Market{
		Ticker:             "KXTENNIS-R154",
		ExpectedExpiration: now.Add(2 * time.Hour).Format(time.RFC3339),
		CloseTime:          now.Add(36 * time.Hour).Format(time.RFC3339),
	}
	s.metaMu.Unlock()
	if got := s.paperEntryResolveHours(context.Background(), "kalshi", "KXPGA3BALL-R154", "Round 2 matchup"); got < 35 {
		t.Fatalf("golf funded result clock trusted the tee-style estimate: %.3fh", got)
	}
	if s.paperEntryHorizonNow(context.Background(), "kalshi", "KXPGA3BALL-R154", "Round 2 matchup") {
		t.Fatal("golf market entered the four-hour funded window before its later result boundary")
	}
	if !s.paperEntryHorizonNow(context.Background(), "kalshi", "KXTENNIS-R154", "Tennis match") {
		t.Fatal("ordinary market stopped preferring its expected result timestamp")
	}
	s.metaMu.Lock()
	delete(s.kmkts, "KXPGA3BALL-R154")
	s.metaMu.Unlock()
	if s.paperEntryHorizonNow(context.Background(), "kalshi", "KXPGA3BALL-R154", "Round 2 matchup") {
		t.Fatal("golf without full current result clocks must fail closed")
	}
}

func TestR154SessionGuardDoesNotResetAtArmOrUTCDay(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveActivationAt = time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339)
	cfg.Risk.LiveActivationBankrollUSD = 100
	cfg.Risk.LiveActivationPeakUSD = 105
	cfg.Risk.LiveRiskEpochAt = time.Now().Add(-24 * time.Hour).UTC().Format(time.RFC3339)
	cfg.Risk.LiveRiskEpochBankrollUSD = 95
	cfg.Risk.LiveRiskEpochPeakUSD = 97
	cfg.Risk.LiveSessionLossPct = .05
	cfg.Risk.LiveRollingLossPct = .10
	s.cfgP.Store(&cfg)

	got := s.liveSessionGuard(context.Background(), 89)
	if !got.Breached || !strings.Contains(got.Reason, "operator risk reset") ||
		got.SessionPnLUSD != -11 || got.PeakDrawdownUSD != -16 ||
		got.RiskPnLUSD != -6 || got.RiskPeakLossUSD != -8 {
		t.Fatalf("activation-session drawdown did not fail closed: %+v", got)
	}
	// Replacing the per-ARM receipt must not move the activation baseline or clear the brake.
	s.liveBankArm = map[string]float64{"kalshi": 89}
	again := s.liveSessionGuard(context.Background(), 89)
	if !again.Breached || again.BaselineUSD != 100 || again.ActivationAt != got.ActivationAt ||
		again.RiskBaselineUSD != 95 || again.RiskEpochAt != got.RiskEpochAt {
		t.Fatalf("ARM receipt reset the long-run guard: before=%+v after=%+v", got, again)
	}
}

func TestR154OperatorRiskResetOffsetsOnlyItsUTCDate(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveRiskEpochAt = time.Now().UTC().Format(time.RFC3339)
	cfg.Risk.LiveRiskEpochDayPnLUSD = -32.33
	cfg.Risk.LiveRiskEpochTurnoverUSD = 730.51
	s.cfgP.Store(&cfg)
	if got, period := s.liveLossAfterOperatorReset(-35); math.Abs(got-(-2.67)) > 1e-9 ||
		!strings.Contains(period, "operator risk reset") {
		t.Fatalf("same-day reset result=%v period=%q want -2.67 since reset", got, period)
	}
	if got := s.liveTurnoverAfterOperatorReset(735.51); math.Abs(got-5) > 1e-9 {
		t.Fatalf("same-day turnover reset result=%v want 5", got)
	}
	cfg.Risk.LiveRiskEpochAt = time.Now().Add(-24 * time.Hour).UTC().Format(time.RFC3339)
	s.cfgP.Store(&cfg)
	if got, period := s.liveLossAfterOperatorReset(-35); got != -35 || period != "today" {
		t.Fatalf("prior-date reset leaked into today: result=%v period=%q", got, period)
	}
	if got := s.liveTurnoverAfterOperatorReset(735.51); math.Abs(got-735.51) > 1e-9 {
		t.Fatalf("prior-date turnover reset leaked into today: %v", got)
	}
}
