package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"
)

const kvLiveActivationPeak = "live_activation_peak_v1"
const kvLiveRiskEpochPeak = "live_risk_epoch_peak_v1"

type liveActivationPeakPersisted struct {
	ActivationAt string  `json:"activation_at"`
	PeakUSD      float64 `json:"peak_usd"`
}

type liveSessionGuardSnapshot struct {
	Configured        bool    `json:"configured"`
	ActivationAt      string  `json:"activation_at,omitempty"`
	BaselineUSD       float64 `json:"baseline_usd"`
	PeakUSD           float64 `json:"peak_usd"`
	CurrentNAVUSD     float64 `json:"current_nav_usd"`
	SessionPnLUSD     float64 `json:"session_pnl_usd"`
	SessionDrawdown   float64 `json:"session_drawdown_pct"`
	PeakDrawdownUSD   float64 `json:"peak_drawdown_usd"`
	PeakDrawdown      float64 `json:"peak_drawdown_pct"`
	RiskEpochAt       string  `json:"risk_epoch_at,omitempty"`
	RiskBaselineUSD   float64 `json:"risk_baseline_usd"`
	RiskPeakUSD       float64 `json:"risk_peak_usd"`
	RiskDayPnLAtReset float64 `json:"risk_day_pnl_at_reset_usd"`
	RiskPnLUSD        float64 `json:"risk_pnl_usd"`
	RiskDrawdown      float64 `json:"risk_drawdown_pct"`
	RiskPeakLossUSD   float64 `json:"risk_peak_loss_usd"`
	RiskPeakDrawdown  float64 `json:"risk_peak_drawdown_pct"`
	AcceptedOrders    float64 `json:"accepted_orders"`
	AcceptedContracts float64 `json:"accepted_contracts"`
	TurnoverUSD       float64 `json:"turnover_usd"`
	FeeUSD            float64 `json:"fee_usd"`
	SessionLossLimit  float64 `json:"session_loss_limit_usd"`
	RollingLossLimit  float64 `json:"rolling_loss_limit_usd"`
	Breached          bool    `json:"breached"`
	Reason            string  `json:"reason,omitempty"`
}

func (s *Server) liveSessionGuardConfigured() bool {
	r := s.cfg().Risk
	return r.LiveActivationBankrollUSD > 0 || r.LiveActivationAt != ""
}

func (s *Server) livePersistedPeak(ctx context.Context, key, epochAt string, seed, current float64) float64 {
	peak := math.Max(seed, current)
	if s.store == nil {
		return peak
	}
	if raw, ok := s.store.KVGet(ctx, key); ok && raw != "" {
		var saved liveActivationPeakPersisted
		if json.Unmarshal([]byte(raw), &saved) == nil && saved.ActivationAt == epochAt &&
			saved.PeakUSD > peak {
			peak = saved.PeakUSD
		}
	}
	if current > peak {
		peak = current
	}
	if current >= peak && current > seed {
		if b, err := json.Marshal(liveActivationPeakPersisted{
			ActivationAt: epochAt, PeakUSD: current,
		}); err == nil {
			_ = s.store.KVSet(ctx, key, string(b))
		}
	}
	return peak
}

// liveSessionGuard is the non-resetting real-account brake. The daily ledger remains a separate
// faster stop; this one survives UTC rollover and repeated ARM handshakes.
func (s *Server) liveSessionGuard(ctx context.Context, currentNAV float64) liveSessionGuardSnapshot {
	out := liveSessionGuardSnapshot{CurrentNAVUSD: currentNAV}
	if !s.liveSessionGuardConfigured() {
		return out
	}
	out.Configured = true
	r := s.cfg().Risk
	start, err := time.Parse(time.RFC3339, r.LiveActivationAt)
	if err != nil || r.LiveActivationBankrollUSD <= 0 {
		out.Breached = true
		out.Reason = "LIVE activation baseline or timestamp is invalid"
		return out
	}
	out.ActivationAt = start.UTC().Format(time.RFC3339)
	out.BaselineUSD = r.LiveActivationBankrollUSD
	out.PeakUSD = s.livePersistedPeak(ctx, kvLiveActivationPeak, out.ActivationAt,
		math.Max(r.LiveActivationBankrollUSD, r.LiveActivationPeakUSD), currentNAV)
	if currentNAV <= 0 || math.IsNaN(currentNAV) || math.IsInf(currentNAV, 0) {
		out.Breached = true
		out.Reason = "current authenticated LIVE account value is unavailable"
		return out
	}
	totals, err := s.store.LiveAcceptedTotalsSince(ctx, start)
	if err != nil {
		out.Breached = true
		out.Reason = "durable LIVE turnover and fee history is unavailable"
		return out
	}
	out.AcceptedOrders, out.AcceptedContracts = totals.Orders, totals.Contracts
	out.TurnoverUSD, out.FeeUSD = totals.TotalCostUSD, totals.FeeUSD
	out.SessionPnLUSD = currentNAV - out.BaselineUSD
	out.SessionDrawdown = out.SessionPnLUSD / out.BaselineUSD
	out.PeakDrawdownUSD = currentNAV - out.PeakUSD
	if out.PeakUSD > 0 {
		out.PeakDrawdown = out.PeakDrawdownUSD / out.PeakUSD
	}
	riskAt, riskBaseline, riskSeed := r.LiveRiskEpochAt, r.LiveRiskEpochBankrollUSD, r.LiveRiskEpochPeakUSD
	if riskAt == "" && riskBaseline <= 0 {
		riskAt, riskBaseline, riskSeed = out.ActivationAt, out.BaselineUSD, out.PeakUSD
	}
	riskStart, riskErr := time.Parse(time.RFC3339, riskAt)
	if riskErr != nil || riskBaseline <= 0 {
		out.Breached = true
		out.Reason = "LIVE risk-reset baseline or timestamp is invalid"
		return out
	}
	out.RiskEpochAt = riskStart.UTC().Format(time.RFC3339)
	out.RiskBaselineUSD = riskBaseline
	out.RiskDayPnLAtReset = r.LiveRiskEpochDayPnLUSD
	out.RiskPeakUSD = s.livePersistedPeak(ctx, kvLiveRiskEpochPeak, out.RiskEpochAt,
		math.Max(riskBaseline, riskSeed), currentNAV)
	out.RiskPnLUSD = currentNAV - out.RiskBaselineUSD
	out.RiskDrawdown = out.RiskPnLUSD / out.RiskBaselineUSD
	out.RiskPeakLossUSD = currentNAV - out.RiskPeakUSD
	if out.RiskPeakUSD > 0 {
		out.RiskPeakDrawdown = out.RiskPeakLossUSD / out.RiskPeakUSD
	}
	out.SessionLossLimit = out.RiskBaselineUSD * r.LiveSessionLossPct
	out.RollingLossLimit = out.RiskPeakUSD * r.LiveRollingLossPct

	switch {
	case out.SessionLossLimit > 0 && -out.RiskPnLUSD >= out.SessionLossLimit-0.0001:
		out.Reason = fmt.Sprintf("full-account loss since the operator risk reset $%.2f breaches the limit $%.2f",
			-out.RiskPnLUSD, out.SessionLossLimit)
	case out.RollingLossLimit > 0 && -out.RiskPeakLossUSD >= out.RollingLossLimit-0.0001:
		out.Reason = fmt.Sprintf("drawdown from the post-reset LIVE peak $%.2f breaches the limit $%.2f",
			-out.RiskPeakLossUSD, out.RollingLossLimit)
	}
	out.Breached = out.Reason != ""
	return out
}

func (s *Server) liveSessionGuardReason(ctx context.Context, currentNAV float64) string {
	return s.liveSessionGuard(ctx, currentNAV).Reason
}
