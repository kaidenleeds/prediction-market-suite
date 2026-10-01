// bandinvert.go — R76 OPERATOR IDEA: BANDED AUTO-INVERT (log-only first).
//
// The existing auto-invert machinery flips a WHOLE source when its aggregate session/lifetime EV
// turns significantly negative. But R72-A's band hills keep showing families whose losses live in
// ONE price band while other bands carry the family — a whole-signal flip throws the good bands
// away with the bad. This extends the evaluation to (source, adaptive price band):
//
//	flip a band iff  n ≥ max(auto_invert_min_trades, 30)
//	             AND the original direction's per-contract net mean is CI-NEGATIVE
//	                 (one-sided 95% upper bound < 0)
//	             AND the INVERTED direction is CI-POSITIVE after paying fees twice over
//	                 (inverted pc ≈ −gross − fee = −pc − 2·fee; one-sided 95% lower bound > 0)
//
// BAND GRANULARITY: reuses adaptiveBands (5¢ default / 3¢ dense / ≤10¢ sparse-merge) and goes NO
// FINER than its 3¢ floor deliberately — n starvation: at closed-trade volumes a 1–2¢ sliver
// can't reach the n≥30 bar for months, and CI tests on slivers flip on noise, which is exactly
// the failure mode the whole-signal inverter's hysteresis exists to prevent.
//
// LOG-ONLY FIRST: the evaluation always runs and is surfaced (audit rows on set changes, the
// /api/live "band_invert" view, the inverted_band row flag the dashboard badges) — but the
// PLACEMENT flip only arms behind auto_invert_banded (config, default false).
package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

// bandFlip is one flipped (source, band): [Lo,Hi) cents, with the measured evidence.
type bandFlip struct {
	Lo, Hi   int     // price band in cents, [Lo, Hi)
	N        int     // closed trades in band
	MeanPC   float64 // original direction per-contract net mean (fee-inclusive) — significantly < 0
	InvLowPC float64 // one-sided 95% t LOWER bound of the INVERTED per-contract net — > 0
}

// refreshBandInvert rebuilds the flip set from the closed-trade history the caller already
// fetched (piggybacks on refreshWinRates' ListPaperFills — no second DB read). TTL ~5min under
// bandInvMu; audits whenever the flip set CHANGES so the log-only phase leaves an evidence trail.
func (s *Server) refreshBandInvert(trades []paper.ClosedTrade) {
	s.bandInvMu.Lock()
	if time.Since(s.bandInvAt) < 5*time.Minute {
		s.bandInvMu.Unlock()
		return
	}
	s.bandInvAt = time.Now()
	s.bandInvMu.Unlock() // compute outside the lock; swap at the end

	minN := s.cfg().Auto.AutoInvertMinTrades
	if minN < 30 {
		minN = 30 // operator spec: the banded variant never decides under n=30, whatever the whole-signal knob says
	}
	type obs struct {
		c      int     // entry price cent index
		pc     float64 // per-contract net (fee-inclusive), original direction
		feePC  float64 // per-contract fee actually paid
	}
	bySrc := map[string][]obs{}
	for _, t := range trades {
		p := t.EntryPrice
		if p <= 0 || p >= 1 || t.Contracts < 1 || t.Source == "" {
			continue
		}
		switch t.Source { // same exclusions as autoInvertedByLoss: proven directional crypto edges are never inverted
		case "auto-cons-pcrypto", "auto-cons-kcrypto", "auto-cons-xmatch":
			continue
		}
		c := int(p * 100)
		if c >= 100 {
			c = 99
		}
		net := t.Realized - t.Fees
		bySrc[t.Source] = append(bySrc[t.Source], obs{c: c, pc: net / t.Contracts, feePC: t.Fees / t.Contracts})
	}
	flips := map[string][]bandFlip{}
	for src, arr := range bySrc {
		if len(arr) < minN {
			continue
		}
		var cnt [100]int
		for _, o := range arr {
			cnt[o.c]++
		}
		for _, bd := range adaptiveBands(&cnt) {
			n, s1, s2, is1, is2 := 0, 0.0, 0.0, 0.0, 0.0
			for _, o := range arr {
				if o.c < bd[0] || o.c >= bd[1] {
					continue
				}
				n++
				s1 += o.pc
				s2 += o.pc * o.pc
				ipc := -o.pc - 2*o.feePC // inverted: −gross − fee ≈ −(pc+fee) − fee (fee is p(1−p)-symmetric)
				is1 += ipc
				is2 += ipc * ipc
			}
			if n < minN {
				continue
			}
			upPC := -evLowerBound(-s1, s2, n) // one-sided 95% UPPER bound of the original mean
			invLow := evLowerBound(is1, is2, n)
			if upPC < 0 && invLow > 0 {
				flips[src] = append(flips[src], bandFlip{Lo: bd[0], Hi: bd[1], N: n, MeanPC: s1 / float64(n), InvLowPC: invLow})
			}
		}
		sort.Slice(flips[src], func(i, j int) bool { return flips[src][i].Lo < flips[src][j].Lo })
	}
	// change detection → audit (the log-only evidence trail; also what the forward review reads)
	sig := bandInvSignature(flips)
	s.bandInvMu.Lock()
	changed := sig != s.bandInvSig
	s.bandInvFlips, s.bandInvSig = flips, sig
	s.bandInvMu.Unlock()
	if changed {
		armed := "log-only"
		if s.cfg().Auto.AutoInvertBanded {
			armed = "ARMED (placement flips)"
		}
		detail := sig
		if detail == "" {
			detail = "(empty set)"
		}
		s.log.Info("banded auto-invert set changed", "mode", armed, "set", detail)
		_ = s.store.Audit(context.Background(), "info", "tune", "banded auto-invert set changed ("+armed+")", detail)
	}
}

// bandInvSignature renders the flip set as a stable string ("src:lo-hi(n=..,mean=..,invLow=..);…").
func bandInvSignature(flips map[string][]bandFlip) string {
	srcs := make([]string, 0, len(flips))
	for src := range flips {
		srcs = append(srcs, src)
	}
	sort.Strings(srcs)
	var b strings.Builder
	for _, src := range srcs {
		for _, f := range flips[src] {
			fmt.Fprintf(&b, "%s:%d-%d¢(n=%d,meanPC=%+.3f,invLow=%+.3f); ", src, f.Lo, f.Hi, f.N, f.MeanPC, f.InvLowPC)
		}
	}
	return strings.TrimSuffix(b.String(), "; ")
}

// bandInverted reports whether (source, price) falls in a flipped band. Used by the log-only row
// marker always, and by autoPlace's invert cascade ONLY when auto_invert_banded is armed.
func (s *Server) bandInverted(source string, price float64) bool {
	if price <= 0 || price >= 1 {
		return false
	}
	c := int(price * 100)
	s.bandInvMu.Lock()
	defer s.bandInvMu.Unlock()
	for _, f := range s.bandInvFlips[source] {
		if c >= f.Lo && c < f.Hi {
			return true
		}
	}
	return false
}

// bandInvertView is the /api/live surfacing: source → flipped bands with their evidence numbers.
func (s *Server) bandInvertView() map[string]any {
	out := map[string]any{}
	s.bandInvMu.Lock()
	for src, fl := range s.bandInvFlips {
		rows := make([]map[string]any, 0, len(fl))
		for _, f := range fl {
			rows = append(rows, map[string]any{"lo_c": f.Lo, "hi_c": f.Hi, "n": f.N, "mean_pc": f.MeanPC, "inv_low_pc": f.InvLowPC})
		}
		out[src] = rows
	}
	s.bandInvMu.Unlock()
	return out
}
