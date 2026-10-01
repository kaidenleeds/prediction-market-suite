// realization.go — R90: the realization-gap program (auditor DO-THIS 5 / edge 29) + the
// operator's ML BORDERS.
//
// WHY (auditor r22 verdicts a-c): the 30v2 gate A/B closed WIN-YES / $-NULL — the ML picks
// better (57.1% vs 46.3%, z=2.41) and still loses money. Decomposition over 156 settled paper
// lots: predicted (ev_net×contracts) +$960 vs actual −$41, with entries averaging BELOW signal
// price (slip term −$748) = winner's-cursed passive fills, and the damage concentrated in
// small-n fantasy-EV families (pbridge predicted +$137 → realized −$114; kalshi-flow realizes
// only ~10% of predicted). The model's EV is systematically un-realized in a FAMILY-SPECIFIC,
// stable way — so scale gate-input EV by each family's measured realization ratio
// (actual/predicted, shrunk toward 1 at low n, floor 0, cap 1), and band every ML-scored pick
// with plausibility + EV borders. Both are config-tunable (Settings / ⚙ gate popover) and are
// mirrored to the Python sidecar via data/ml_realization.json + config.json (hot, no restart).
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// flexBool tolerates the bool encodings that actually exist in the book files: true/false,
// 0/1 (server.go's settle path wrote reset_close as a NUMBER until R91 — every synced
// ml_paper.json on disk carries `"reset_close": 1`), null, and quoted variants. R91: without
// this, ONE numeric reset_close made json.Unmarshal fail the WHOLE file and readJSONLoose
// served an empty feed — the realization haircut (edge 29) was silently dead since R90 boot.
type flexBool bool

func (b *flexBool) UnmarshalJSON(data []byte) error {
	s := strings.TrimSpace(string(data))
	s = strings.Trim(s, `"`) // `"1"` / `"true"` — be liberal in what we accept
	switch strings.ToLower(s) {
	case "true":
		*b = true
		return nil
	case "false", "null", "":
		*b = false
		return nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		*b = f != 0
		return nil
	}
	return fmt.Errorf("flexBool: cannot parse %s", string(data))
}

// realizationShrinkN is the prior weight toward ratio=1 ("trust the model until evidence"):
// shrunk = (n·raw + k·1)/(n+k). Low-n families barely haircut; kalshi-flow at r22 (raw .10,
// n=93) lands ≈.29 — exactly the medicine the decomposition prescribes.
const realizationShrinkN = 25.0

type realizationState struct {
	mu     sync.Mutex
	at     time.Time
	ratios map[string]float64   // family → shrunk actual/predicted, floor 0 cap 1
	ns     map[string]int       // family → settled-lot n behind the ratio
	qAt    map[string]time.Time // quarantine audit throttle (ticker|side → last log)
}

// refreshRealizationRatios recomputes per-family actual/predicted from the ML paper book's
// SETTLED lots (ml_paper.json closed[]; rows carry signal_type/ev_net/contracts/pnl —
// reset_close rows are mark-to-market force-closes, not realizations, and are excluded).
// Rides the 30-min alloc sweep (self-throttled ≥25min). Writes data/ml_realization.json so the
// sidecar applies the same haircut to its own book entries (re-read every cycle, no restart).
func (s *Server) refreshRealizationRatios(ctx context.Context) {
	s.realiz.mu.Lock()
	if time.Since(s.realiz.at) < 25*time.Minute {
		s.realiz.mu.Unlock()
		return
	}
	s.realiz.at = time.Now()
	s.realiz.mu.Unlock()
	var book struct {
		Closed []struct {
			SignalType string  `json:"signal_type"`
			Side       string  `json:"side"` // R106 (auditor DO-THIS 6 / watch 41): YES/NO — side-tagged calibration, log-only
			EVNet      float64 `json:"ev_net"`
			Contracts  float64 `json:"contracts"`
			PnL        float64 `json:"pnl"`
			ResetClose flexBool `json:"reset_close"` // R91: files on disk carry 0/1 — see flexBool
		} `json:"closed"`
	}
	if !s.readJSONLoose(filepath.Join(s.cfg().DataDir, "ml_paper.json"), &book) {
		return
	}
	type acc struct {
		pred, act float64
		n         int
	}
	byFam := map[string]*acc{}
	bySide := map[string]*acc{} // R106 (DO-THIS 6): YES vs NO realization, LOG-ONLY — the side-skew
	// watch (41: YES−NO +$5.53/lot CI-separable at n=136, confounded by pre-243 NO rows) needs a
	// clean side-tagged calib_ratio series; haircutEV NEVER reads this (validation data only).
	for _, c := range book.Closed {
		if c.SignalType == "" || bool(c.ResetClose) {
			continue
		}
		a := byFam[c.SignalType]
		if a == nil {
			a = &acc{}
			byFam[c.SignalType] = a
		}
		a.pred += c.EVNet * c.Contracts
		a.act += c.PnL
		a.n++
		if sd := strings.ToUpper(strings.TrimSpace(c.Side)); sd == "YES" || sd == "NO" {
			sa := bySide[sd]
			if sa == nil {
				sa = &acc{}
				bySide[sd] = sa
			}
			sa.pred += c.EVNet * c.Contracts
			sa.act += c.PnL
			sa.n++
		}
	}
	ratios, ns := map[string]float64{}, map[string]int{}
	parts := []string{}
	for fam, a := range byFam {
		if a.n < 3 || a.pred <= 0.01 {
			continue // needs a positive predicted base + a minimal sample to mean anything
		}
		raw := a.act / a.pred
		if raw < 0 {
			raw = 0 // floor 0: negative realization = full haircut, never a sign flip
		}
		shrunk := (float64(a.n)*raw + realizationShrinkN) / (float64(a.n) + realizationShrinkN)
		if shrunk > 1 {
			shrunk = 1 // a haircut never INFLATES model EV
		}
		ratios[fam], ns[fam] = shrunk, a.n
		parts = append(parts, fmt.Sprintf("%s=%.2f(n=%d raw=%.2f)", fam, shrunk, a.n, raw))
	}
	s.realiz.mu.Lock()
	s.realiz.ratios, s.realiz.ns = ratios, ns
	s.realiz.mu.Unlock()
	// R106 (auditor DO-THIS 6): side-tagged raw ratios ride the same file, LOG-ONLY — nothing
	// consumes them for sizing/haircuts; they exist so the auditor's clean post-243 side-skew
	// recut (watch 41) has a continuously-accruing calib series per side.
	sideRatios, sideNs := map[string]float64{}, map[string]int{}
	for sd, a := range bySide {
		if a.n < 3 || a.pred <= 0.01 {
			continue
		}
		raw := a.act / a.pred
		sideRatios[sd], sideNs[sd] = math.Round(raw*1000)/1000, a.n
	}
	blob, err := json.Marshal(map[string]any{
		"generated_at": time.Now().UTC().Format(time.RFC3339),
		"shrink_n":     realizationShrinkN,
		"ratios":       ratios,
		"n":            ns,
		"ratios_by_side_log_only": sideRatios, // R106 DO-THIS 6 (raw, unshrunk, unfloored — honest calib telemetry)
		"n_by_side":               sideNs,
	})
	if err == nil {
		tmp := filepath.Join(s.cfg().DataDir, "ml_realization.json.tmp")
		if os.WriteFile(tmp, blob, 0o644) == nil {
			_ = os.Rename(tmp, filepath.Join(s.cfg().DataDir, "ml_realization.json"))
		}
	}
	if len(parts) > 0 {
		sort.Strings(parts)
		_ = s.store.Audit(ctx, "info", "ml", "R90 realization ratios refreshed (edge 29 haircut inputs)", strings.Join(parts, " · "))
	}
}

// realizationRatio returns the family's shrunk ratio (1 = no haircut known) and its n.
func (s *Server) realizationRatio(family string) (float64, int) {
	s.realiz.mu.Lock()
	defer s.realiz.mu.Unlock()
	if r, ok := s.realiz.ratios[family]; ok {
		return r, s.realiz.ns[family]
	}
	return 1, 0
}

// haircutEV applies DO-THIS 5i at a gate: positive EV scales by the family's realization
// ratio when realization_haircut is on. Returns the (possibly scaled) EV + the ratio used
// (ratio is returned even in log-only mode so callers can stamp it for the validation diff).
func (s *Server) haircutEV(family string, evNet float64) (float64, float64) {
	ratio, _ := s.realizationRatio(family)
	if !s.cfg().Auto.RealizationHaircut || family == "" {
		return evNet, ratio
	}
	if evNet > 0 && ratio < 1 {
		return evNet * ratio, ratio
	}
	return evNet, ratio
}

// mlBorders (operator ask, R90): classify one ML-scored pick AFTER the haircut.
// verdict "" = pass · "reject" = outside the plausibility band / below the EV border floor ·
// "quarantine" = EV above the too-good ceiling (log for review, never bet). An explicit 0 on
// any single border disables just that border. AT the border passes (strict inequality trips).
func (s *Server) mlBorders(pwin, evNet float64) (verdict, reason string) {
	a := s.cfg().Auto
	if mn := a.MLPWinMin; mn > 0 && pwin < mn {
		return "reject", fmt.Sprintf("p_win %.3f below plausibility floor %.2f", pwin, mn)
	}
	if mx := a.MLPWinMax; mx > 0 && pwin > mx {
		return "reject", fmt.Sprintf("p_win %.3f above plausibility ceiling %.2f", pwin, mx)
	}
	if mn := a.MLEVMinCents; mn > 0 && evNet*100 < mn {
		return "reject", fmt.Sprintf("net EV %.1f¢ below border floor %.1f¢", evNet*100, mn)
	}
	if mx := a.MLEVMaxCents; mx > 0 && evNet*100 > mx {
		return "quarantine", fmt.Sprintf("net EV %.1f¢ above too-good ceiling %.1f¢", evNet*100, mx)
	}
	return "", ""
}

// mlQuarantine writes the reviewable quarantine record (audit category "mlquarantine",
// throttled 15min per ticker|side so a hot pick can't spam the trail).
func (s *Server) mlQuarantine(ctx context.Context, platform, ticker, side, family, src string, pwin, evNet float64) {
	key := ticker + "|" + side
	s.realiz.mu.Lock()
	if s.realiz.qAt == nil {
		s.realiz.qAt = map[string]time.Time{}
	}
	if t, ok := s.realiz.qAt[key]; ok && time.Since(t) < 15*time.Minute {
		s.realiz.mu.Unlock()
		return
	}
	s.realiz.qAt[key] = time.Now()
	if len(s.realiz.qAt) > 600 {
		s.realiz.qAt = map[string]time.Time{key: time.Now()}
	}
	s.realiz.mu.Unlock()
	_ = s.store.Audit(ctx, "warn", "mlquarantine",
		fmt.Sprintf("ML pick QUARANTINED (too-good EV, R90 border): %s %s %s via %s", platform, ticker, side, src),
		fmt.Sprintf(`{"family":%q,"p_win":%.4f,"ev_net_cents":%.2f}`, family, pwin, evNet*100))
}
