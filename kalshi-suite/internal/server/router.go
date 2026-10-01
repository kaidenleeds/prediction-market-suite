package server

// router.go — R122 QUEUE-AWARE EXECUTION ROUTING (operator decision: "taker usually, maker when
// the queue allows").
//
// At placement, for every paper book (and the still-DISARMED live path), estimate
//
//	expected-time-to-fill = visible queue ahead at our post price ÷ recent trade-through rate
//	                        at/through that level (from the live tape)
//
// and compare it against the strategy's EDGE HORIZON. Queue short/empty AND horizon comfortable
// ⇒ MAKER (the R105 option-c depth/momentum gate still applies on top); otherwise ⇒ TAKER through
// the existing divert paths. Every fill is tagged maker/taker + the routing reason + the estimate
// inputs (mfs route_* columns, paper_fills fill_kind/route_reason, kfPos.RouteReason, the ML lot's
// fill_rule) so the verdict engine and the auditor can grade the router.
//
// Edge horizons: R119 measured per-family edge decay (median time-to-peak; tools/r119_results.json)
// — where measured, horizon = 2× that median; unmeasured families fall back to
// time-to-resolve × route_horizon_frac (default 0.25), floor 30 min.
//
// ML-book special case (auditor r56 decision item 6 + bug 407): the maker cohort's post-boot
// fill-EV is CI-NEGATIVE twice running (−15.22¢ n=50), so ML-book maker posting is PAUSED — the
// router diverts ML candidates to taker — until the sidecar's 407 sensor (ece_gated, the ECE
// measured on rows passing the live gates) is published AND ≤ ml_maker_calib_gate_ece (default
// 2.5%, the ML-CALIB-TRANSFER success bar). No sensor ⇒ paused (fail-safe per the auditor's
// staged proposal). This is a MONEY-POLICY change, flagged in the R122 report.

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// routeDecision is one routing verdict with its estimate inputs.
type routeDecision struct {
	Maker      bool
	Reason     string  // maker:queue-clear | taker:queue-deep | taker:no-flow | taker:no-queue-data | taker:ml-calib-gate:* | maker:router-off
	Queue      float64 // visible contracts ahead at the post price (upper bound); -1 = unobservable
	RatePM     float64 // trade-through rate at/through the level, contracts/min
	ETASec     float64 // Queue ÷ RatePM (seconds; 1e9 = no flow)
	HorizonSec float64 // the strategy's edge horizon (seconds)
}

// Tag renders the decision + inputs as one compact fill tag, e.g.
// "taker:queue-deep(q=250,r=36.0/m,eta=417s,hz=300s)".
func (d routeDecision) Tag() string {
	if d.Reason == "" {
		return ""
	}
	if d.Queue < 0 {
		return d.Reason
	}
	return fmt.Sprintf("%s(q=%.0f,r=%.1f/m,eta=%.0fs,hz=%.0fs)", d.Reason, d.Queue, d.RatePM, d.ETASec, d.HorizonSec)
}

// famDecayHorizonSec — R119-measured per-family edge-decay horizons (2× median time-to-peak:
// rawflow 0.58h · kflow-live 0.43h · kflow-pre 1.40h · ml 0.24h). 0 ⇒ unmeasured (caller falls
// back to time-to-resolve × frac). Matching is substring-loose so book sources ("rawflow"),
// engine sources ("auto-cons-kflow") and family names all hit the same row.
func famDecayHorizonSec(source string) float64 {
	sl := strings.ToLower(source)
	switch {
	case strings.Contains(sl, "rawflow"):
		return 2 * 0.58 * 3600
	case strings.Contains(sl, "kflow-pre"):
		return 2 * 1.40 * 3600
	case strings.Contains(sl, "kflow"):
		return 2 * 0.43 * 3600
	case strings.Contains(sl, "ml-book") || sl == "auto-ml":
		return 2 * 0.24 * 3600
	}
	return 0
}

// tapeRatePM estimates the recent trade-through rate (contracts/min) at/through a side-adjusted
// price level from the venue-wide live tape. Kalshi WS tape only (the ring holds ~4000 prints
// venue-wide, so the honest window = min(lookback, ring coverage), floor 1 min). Matching
// semantics mirror queueChewTape exactly: only prints whose AGGRESSOR hit our side of the book,
// at or through our level, count. ok=false ⇒ no tape (PUS has a 600-print ring but no ladder, so
// the router never gets this far there).
func (s *Server) tapeRatePM(platform, ticker, side string, px float64, lookback time.Duration) (float64, bool) {
	if platform != "kalshi" || s.kal == nil {
		return 0, false
	}
	tape, ok := s.kal.LiveTape()
	if !ok || len(tape) == 0 {
		return 0, false
	}
	isNo := strings.EqualFold(side, "NO")
	now := time.Now()
	cut := now.Add(-lookback)
	oldest := now
	total := 0.0
	for _, t := range tape { // newest-first (LiveTape contract)
		ts, err := time.Parse(time.RFC3339, t.CreatedTime)
		if err != nil {
			continue
		}
		if ts.Before(cut) {
			break
		}
		if ts.Before(oldest) {
			oldest = ts // ring coverage, ALL tickers — the honest denominator
		}
		if t.Ticker != ticker {
			continue
		}
		agg := strings.ToLower(t.Aggressor())
		if (isNo && agg != "yes") || (!isNo && agg != "no") {
			continue // aggressor wasn't hitting our side
		}
		tp := float64(t.YesPrice)
		if isNo {
			tp = 1 - tp
		}
		if tp < px+0.005 { // at or through our level — this flow would chew our queue
			total += float64(t.Count)
		}
	}
	span := now.Sub(oldest)
	if span < time.Minute {
		span = time.Minute
	}
	return total / span.Minutes(), true
}

// mlCalibGate — (red, why) for the ML-book maker pause. Red while the 407 sensor (ece_gated in
// ml_predictions.json) is absent or above threshold. ml_maker_calib_gate_ece: 0 ⇒ default 0.025;
// <0 ⇒ gate disabled.
func (s *Server) mlDeployedCalibNow() (float64, int, []storage.MakerCalibBin, bool) {
	s.mlPredMu.Lock()
	if time.Since(s.mlDepCalAt) < 5*time.Minute {
		e, n, b := s.mlDepCalECE, s.mlDepCalN, append([]storage.MakerCalibBin(nil), s.mlDepCalBins...)
		s.mlPredMu.Unlock()
		return e, n, b, true
	}
	s.mlPredMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	e, n, b, err := s.store.MakerDeployedCalib(ctx, time.Now().Add(-14*24*time.Hour))
	if err != nil {
		return 0, 0, nil, false
	}
	s.mlPredMu.Lock()
	s.mlDepCalAt, s.mlDepCalECE, s.mlDepCalN, s.mlDepCalBins = time.Now(), e, n, append([]storage.MakerCalibBin(nil), b...)
	s.mlPredMu.Unlock()
	return e, n, b, true
}

func (s *Server) mlCalibGate() (bool, string) {
	th := s.cfg().Auto.MLMakerCalibGateECE
	if th < 0 {
		return false, ""
	}
	if th == 0 {
		th = 0.025
	}
	if de, n, _, ok := s.mlDeployedCalibNow(); ok && n >= 100 {
		if de > th {
			return true, fmt.Sprintf("deployed_ece %.1f%%>%.1f%% (n=%d)", de*100, th*100, n)
		}
		return false, ""
	}
	eg, gn, ok := s.mlEceGatedNow()
	if !ok || gn < 300 {
		return true, fmt.Sprintf("sensor-thin (deployed<100, gated n=%d<300)", gn)
	}
	if eg > th {
		return true, fmt.Sprintf("ece_gated %.1f%%>%.1f%% (n=%d)", eg*100, th*100, gn)
	}
	return false, ""
}

// routeMakerTaker is THE routing call — made at every placement site with the actual post price.
// It answers maker-vs-taker strategically; the option-c gate (depth+momentum) still applies
// tactically on the maker branch downstream.
func (s *Server) routeMakerTaker(ctx context.Context, platform, ticker, side string, postPx float64, source string) routeDecision {
	a := s.cfg().Auto
	if !a.RouteQueueEnabled {
		return routeDecision{Maker: true, Reason: "maker:router-off", Queue: -1}
	}
	// ML maker pause (money policy — see file header).
	if sl := strings.ToLower(source); sl == "ml-book" || sl == "auto-ml" {
		if red, why := s.mlCalibGate(); red {
			return routeDecision{Maker: false, Reason: "taker:ml-calib-gate:" + why, Queue: -1}
		}
	}
	// Edge horizon: measured family decay first, else resolve-time × frac, floor 30 min.
	horizon := famDecayHorizonSec(source)
	if horizon <= 0 {
		frac := a.RouteHorizonFrac
		if frac <= 0 {
			frac = 0.25
		}
		if rh := s.sigResolveHours(ctx, platform, ticker); rh > 0 {
			horizon = rh * 3600 * frac
		}
	}
	if horizon <= 0 {
		horizon = 1800
	}
	qa, qk := s.queueAheadAt(platform, ticker, side, postPx)
	rate, rok := 0.0, false
	if qk {
		rate, rok = s.tapeRatePM(platform, ticker, side, postPx, 15*time.Minute)
	}
	margin := a.RouteETAMargin
	if margin <= 0 {
		margin = 1.0
	}
	return routeVerdict(qa, qk, rate, rok, horizon, margin)
}

// routeVerdict is the PURE decision core (matrix-pinned in r122_test.go): expected-time-to-fill
// = queue ÷ rate vs horizon × margin.
func routeVerdict(qa float64, qk bool, rate float64, rok bool, horizon, margin float64) routeDecision {
	if !qk {
		// No observable queue (PUS always; Kalshi outside the ~300-ticker book-WS set) — the
		// operator's default wins: taker. The reason tag keeps the router gradable.
		return routeDecision{Maker: false, Reason: "taker:no-queue-data", Queue: -1, HorizonSec: horizon}
	}
	d := routeDecision{Queue: qa, RatePM: rate, HorizonSec: horizon}
	if qa <= 1 {
		// Empty/near-empty level: we ARE the front of the queue; the existing cancel rules
		// (moved-1c / close-3min / expire-10m) bound the wait, and the late-fill watch measures it.
		d.Maker, d.Reason, d.ETASec = true, "maker:queue-clear", 0
		return d
	}
	if !rok || rate <= 0 {
		d.Maker, d.Reason, d.ETASec = false, "taker:no-flow", 1e9
		return d
	}
	d.ETASec = qa / rate * 60
	if d.ETASec <= horizon*margin {
		d.Maker, d.Reason = true, "maker:queue-clear"
	} else {
		d.Maker, d.Reason = false, "taker:queue-deep"
	}
	return d
}
