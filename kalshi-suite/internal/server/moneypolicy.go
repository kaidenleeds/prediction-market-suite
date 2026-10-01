package server

// This file calculates two optional money-policy previews. Both settings are off by default.
//
// ev_capital_day_gate compares expected value with the capital and time tied up by a position.
// cluster_kelly_arm compares half-Kelly sizing with a per-cluster bankroll cap.
//
// autoPlace calls moneyPolicyPreview after it calculates the stake. The function writes a
// throttled audit entry and never changes the order. r127b_test.go pins that behavior.

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

// mpClusterCapPct returns the effective per-cluster cap % (≤0 ⇒ the documented default 22).
func mpClusterCapPct(pct float64) float64 {
	if pct <= 0 {
		return 22
	}
	return pct
}

func evPerCapitalDay(pWin, price, fee, days float64) (edge, rate float64, ok bool) {
	if days <= 0 || price <= 0 || price >= 1 || fee < 0 {
		return 0, 0, false
	}
	edge = pWin - price - fee
	entryCapital := price + fee
	if entryCapital <= 0 {
		return 0, 0, false
	}
	return edge, edge / (entryCapital * days), true
}

// moneyPolicyPreview — see the file header. NEVER alters the stake; with both knobs off it is a
// two-field read and a return.
func (s *Server) moneyPolicyPreview(ctx context.Context, platform, ticker, side, source string, price, stakeUSD float64, positions []paper.Position) {
	a := s.cfg().Auto
	if !a.EVCapitalDayGate && !a.ClusterKellyArm {
		return // shipped default: both knobs OFF — total no-op
	}
	// Audit throttle: at most one preview line per 10 min (the operator wants a taste of what
	// WOULD happen, not a firehose).
	s.mpMu.Lock()
	if time.Since(s.mpAuditAt) < 10*time.Minute {
		s.mpMu.Unlock()
		return
	}
	s.mpAuditAt = time.Now()
	s.mpMu.Unlock()

	var notes []string
	// ── C4 preview: EV per dollar per day tied up ──
	if a.EVCapitalDayGate {
		hrs := s.sigResolveHours(ctx, platform, ticker)
		note := "ev/day gate: horizon unknown — gate would PASS THROUGH"
		if hrs > 0 {
			days := math.Max(hrs/24, 1.0/24) // floor at 1h so intraday edges don't divide toward infinity
			if pw, ok := s.mlPWin(ticker, side); ok && pw > 0 {
				fee := s.blendedFee(strings.ToLower(platform), ticker, "", false, 1, price)
				edge, evPerDay, rateOK := evPerCapitalDay(pw, price, fee, days)
				if !rateOK {
					note = "ev/day gate: invalid entry capital — gate would PASS THROUGH"
				} else {
					decision := "would PASS"
					if evPerDay < a.EVCapitalDayMin {
						decision = "would BLOCK"
					}
					note = fmt.Sprintf("ev/day gate %s: edge %+.3f/ct on $%.3f entry over %.2fd = %+.4f per $·day (min %.4f)",
						decision, edge, price+fee, days, evPerDay, a.EVCapitalDayMin)
				}
			} else {
				note = fmt.Sprintf("ev/day gate: no model p_win for this pick (horizon %.2fd) — gate would PASS THROUGH", days)
			}
		}
		notes = append(notes, note)
	}
	// ── C5 preview: per-cluster cap (half-Kelly-on-clusters' hard bound) ──
	if a.ClusterKellyArm {
		ck := clusterKey(platform, ticker)
		if ck == "" {
			notes = append(notes, "cluster cap: market is not in any correlated cluster — cap would not apply")
		} else {
			open := 0.0
			for _, p := range positions {
				if p.Platform == platform && p.Contracts > 0 && clusterKey(p.Platform, p.Ticker) == ck {
					open += p.CostBasis
				}
			}
			capUSD := mpClusterCapPct(a.ClusterCapPct) / 100 * s.platformBankroll(platform)
			decision := "would FIT"
			if open+stakeUSD > capUSD {
				decision = "would CAP"
			}
			notes = append(notes, fmt.Sprintf("cluster cap %s: %s open $%.2f + stake $%.2f vs cap $%.2f (%.0f%% of bank)",
				decision, ck, open, stakeUSD, capUSD, mpClusterCapPct(a.ClusterCapPct)))
		}
	}
	// This function records the preview and leaves the stake unchanged.
	_ = s.store.Audit(ctx, "info", "moneypolicy",
		fmt.Sprintf("R127 money-policy PREVIEW (log-only, no stake was changed) %s %s [%s]: %s",
			platform, ticker, source, strings.Join(notes, " · ")), "")
}
