package server

// R117 AUTO-PROMOTION PIPELINE — a state machine over the verdict engine's families that spawns,
// grows and retires PAPER experiment books from a dedicated paper reserve.
//
// ⚠ LIVE ARMING STAYS OPERATOR-ONLY, FOREVER. This pipeline only ever spawns PAPER books; it
// must never touch live_* config, the live order paths, or real money in any form. That is a
// standing operator rule, not a default.
//
// STATES per family (persisted in data/promotion_state.json):
//
//	ELIGIBLE-WATCH        — family sighted by the verdict engine; watching for the promote rails
//	ELIGIBLE-NO-EXECUTOR  — PROVEN or not, no registered executor exists (logged once; watch-only)
//	QUEUED-RESERVE        — LEGACY (R119, retired R127): the reserve queue is gone — a legacy
//	                        queued record promotes directly when still PROVEN+, else returns to
//	                        watch. The constant stays so old state files load unchanged.
//	PROMOTED              — R127: the strategy is ACTIVE as a SUB-STRATEGY of its venue book, with
//	                        an allocation share (AllocUSD, default promotion_bankroll_usd $100)
//	                        as its per-sub open-exposure cap inside that book's bank
//	GROWN                 — the share cap doubled at a checkpoint (repeatable up to the grow cap)
//	WATCH                 — R127 D4 DEMOTION-TO-WATCH: the UNDERLYING family verdict lapsed out of
//	                        PROVEN+ — placement pauses, logging continues, the share is RETAINED;
//	                        auto-returns to PROMOTED when the family re-clears PROVEN+
//	RETIRED               — the book's OWN real-fill verdict flipped PROVEN−: wind down like the
//	                        shadow book (no new opens; lots settle naturally; history kept; the
//	                        share is released at full wind-down)
//
// RULES (promoDecide — a PURE function over a snapshot, unit-tested in promotion_test.go +
// r127_test.go):
//  1. AUTO-PROMOTE: PROVEN+ ∧ not venue-locked ∧ has an executor ∧ portfolio drawdown ≤
//     promotion_drawdown_pause_pct. R127: promotion no longer moves reserve money — activation IS
//     a sub-strategy share of the venue book (Kalshi book hosts freshinv; PolyUS hosts xvgap).
//     R119: no max-books cap (parses, ignored). R118: no min-rows/min-days rails (parse, ignored)
//     — the PROVEN+ always-valid confidence sequence IS the significance.
//  2. GROW: the promoted book's OWN real-fill verdict (family "book:<name>") still PROVEN+ at a
//     doubling checkpoint (settled rows ≥ 2 × last checkpoint; first checkpoint at 100 rows) →
//     double the SHARE CAP, bounded by promotion_grow_cap_usd (R127: no reserve limit — the
//     venue book's bank is the hard wall at placement time).
//  3. AUTO-RETIRE: the book's own real-fill verdict flips PROVEN− → RETIRED (wind-down). Applies
//     from PROMOTED, GROWN and WATCH alike.
//  4. D4 DEMOTION-TO-WATCH (R127): a PROMOTED/GROWN sub whose UNDERLYING family verdict lapses
//     out of PROVEN+ (or vanishes — invert twins exist only while their base stays deep PROVEN−)
//     goes to WATCH: placement paused, logging continues, share retained; re-clears → PROMOTED.
//  5. INVERT TWINS are first-class families ("invert:<family>", spawned by verdicts.go) — when a
//     twin goes PROVEN+ and bettable it promotes via rule 1. Full circle.
//
// RESERVE ACCOUNTING — RETIRED (R127): promotion no longer moves reserve money. The state file
// keeps reserve_remaining/nav fields for HISTORY (and the drawdown-pause rail still reads the
// pipeline NAV); every transition's AllocDelta is 0 now. The money truth lives in the four venue
// books (books.go): a sub's placement is gated by min(share cap, venue-book available).
//
// EXECUTOR REGISTRY: family → executor (book file + place-hook enable). Exactly one executor is
// registered now: "invert:freshlist" → the FreshInv book (freshinv.go). Families without an
// executor are eligibility-watch only.
//
// Every transition is stored in the audit log and promotion state file. Telegram can also report
// the change when local notification settings are present.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	promoStateWatch    = "ELIGIBLE-WATCH"
	promoStateNoExec   = "ELIGIBLE-NO-EXECUTOR"
	promoStateQueued   = "QUEUED-RESERVE" // legacy (R119) — retired R127; old records promote or fall back to watch
	promoStatePromoted = "PROMOTED"
	promoStateGrown    = "GROWN"
	// promoStateWatchD4 — R127 D4 DEMOTION-TO-WATCH: a promoted sub whose underlying family
	// verdict lapsed out of PROVEN+. Placement paused (freshInvActive/xvgActive return false),
	// logging + settlement continue, the share is retained; re-clears PROVEN+ → PROMOTED.
	promoStateWatchD4 = "WATCH"
	promoStateRetired = "RETIRED"

	// promoGrowInitialN: the FIRST grow checkpoint (settled rows in the promoted book). After it,
	// checkpoints double (n ≥ 2 × last_checkpoint_n) per the doubling rule.
	promoGrowInitialN = 100
)

// promoExecutor maps a verdict family to the paper book that can actually bet it.
type promoExecutor struct {
	Name       string // book short name ("freshinv")
	BookFile   string // data-dir book file
	BookFamily string // the book's OWN real-fill verdict family in computeExperimentVerdicts
}

// promoExecutorRegistry — the full set of bettable families. Exactly one today.
func promoExecutorRegistry() map[string]promoExecutor {
	return map[string]promoExecutor{
		"invert:freshlist": {Name: "freshinv", BookFile: "freshinv_book.json", BookFamily: "book:freshinv"},
		// R122 (auditor r56 DO-THIS 10 / EV #1): the xvgap POCKET executor — polyus-only 20-50¢
		// taker entries (xvgapexec.go). Registering it lets the pipeline fund the ONLY remaining
		// bettable PROVEN+ family (+3.94¢/ct n=5,554 at build) on its own rails.
		"xvgap": {Name: "xvgap", BookFile: "xvgap_book.json", BookFamily: "book:xvgap"},
	}
}

// promoRecord is one family's persisted pipeline state.
type promoRecord struct {
	Family          string  `json:"family"`
	State           string  `json:"state"`
	BookFile        string  `json:"book_file,omitempty"`
	AllocUSD        float64 `json:"alloc_usd,omitempty"`
	PromotedAt      string  `json:"promoted_at,omitempty"`
	Checkpoints     int     `json:"checkpoints,omitempty"`
	LastCheckpointN int     `json:"last_checkpoint_n,omitempty"`
	RetiredAt       string  `json:"retired_at,omitempty"`
	Reason          string  `json:"reason,omitempty"`
	FirstSeen       string  `json:"first_seen,omitempty"` // family first sighted by the pipeline
	Refunded        bool    `json:"refunded,omitempty"`   // RETIRED wind-down complete, bank returned
	QueuedAt        string  `json:"queued_at,omitempty"`  // R119: QUEUED-RESERVE entry time (FIFO funding order)
	// DataAgeDays is COMPUTED for the /api/promotions payload only (informational since R118 —
	// age of the family's oldest signal_log row, 90d window; no rail reads it). Never set on the
	// persisted state-file records.
	DataAgeDays float64 `json:"data_age_days,omitempty"`
}

// promoState is the whole persisted pipeline (data/promotion_state.json, atomic write).
type promoState struct {
	ReserveInit      bool                    `json:"reserve_initialized"`
	ReserveRemaining float64                 `json:"reserve_remaining"`
	NAVPeak          float64                 `json:"nav_peak"`
	LastNAV          float64                 `json:"last_nav"`
	LastDrawdownPct  float64                 `json:"last_drawdown_pct"`
	Records          map[string]*promoRecord `json:"records"`
	UpdatedAt        string                  `json:"updated_at"`
}

// promoCfgT — the config knobs with code defaults applied.
// R118: MinRows/MinDays removed — promotion_min_rows / promotion_min_days still PARSE from
// config.json (deprecated, ignored) but no longer gate anything; PROVEN+ IS the significance.
type promoCfgT struct {
	Enabled          bool
	BankrollUSD      float64
	GrowCapUSD       float64
	ReserveUSD       float64
	DrawdownPausePct float64
}

func (s *Server) promoCfg() promoCfgT {
	a := s.cfg().Auto
	c := promoCfgT{Enabled: a.PromotionEnabled == nil || *a.PromotionEnabled,
		BankrollUSD: a.PromotionBankrollUSD,
		GrowCapUSD:  a.PromotionGrowCapUSD, ReserveUSD: a.PromotionReserveUSD,
		DrawdownPausePct: a.PromotionDrawdownPausePct}
	// R118: a.PromotionMinRows / a.PromotionMinDays are deprecated no-ops (kept parseable).
	// R119: a.PromotionMaxBooks likewise deprecated (operator order: unlimited promotions;
	// the reserve + FIFO queue are the overdraw rail).
	if c.BankrollUSD <= 0 {
		c.BankrollUSD = 100
	}
	if c.GrowCapUSD <= 0 {
		c.GrowCapUSD = 800
	}
	if c.ReserveUSD <= 0 {
		c.ReserveUSD = 500
	}
	if c.DrawdownPausePct <= 0 {
		c.DrawdownPausePct = 20
	}
	return c
}

// promoBookStats — the executor book's numbers a decision needs (read by the sweep, injected
// into the pure function).
type promoBookStats struct {
	SettledN int     // lifetime settled rows (checkpoint counter)
	OpenLots int     // wind-down completion detector
	BankNet  float64 // bank + net since epoch — what a completed wind-down returns to the reserve
}

// promoSnap is the pure decision input: everything promoDecide may consult, no I/O.
type promoSnap struct {
	Now         time.Time
	Cfg         promoCfgT
	Verdicts    map[string]verdictEnt    // family → current verdict (incl. "book:*" families)
	Executors   map[string]promoExecutor // family → executor (bettable set)
	Records     map[string]promoRecord   // persisted state, BY VALUE (pure — never mutated)
	Reserve     float64
	DrawdownPct float64
	Books       map[string]promoBookStats // family → its executor book's stats
}

// promoFamilyDataStart resolves a family's earliest signal_log row — R118: INFORMATIONAL ONLY
// (the /api/promotions data_age_days payload field); no rail reads it anymore.
// invert:<X> twins share X's rows (the twin is graded off the same base-family rows), so
// "invert:freshlist" reads freshlist's data age.
func promoFamilyDataStart(ages map[string]time.Time, family string) (time.Time, bool) {
	t, ok := ages[strings.TrimPrefix(family, "invert:")]
	return t, ok && !t.IsZero()
}

// promoTransition is one decided state change (the sweep applies + narrates it).
type promoTransition struct {
	Family      string
	From, To    string
	AllocDelta  float64 // >0 reserve→book, <0 book→reserve (wind-down refund)
	NewAllocUSD float64 // book allocation after the transition (promote/grow/refund set it)
	CheckpointN int     // grow: the settled-row count that fired the checkpoint
	Reason      string
}

// promoDecide — THE promotion rules, as a pure function over one snapshot (unit-tested).
// Deterministic: families evaluated in sorted order. R127: no reserve accounting — every
// transition's AllocDelta is 0; AllocUSD is the sub-strategy's share cap within its venue book.
func promoDecide(sn promoSnap) []promoTransition {
	var out []promoTransition
	// R128 (operator standing doctrine): NO pausing, NO limiting, NO balance limiting. The
	// drawdown pause rail, the D4 demote-to-WATCH and the auto-RETIRE wind-down are GONE from
	// this state machine — the scoreboard-weight allocation engine (scoreweight.go) does all the
	// work: a bad/lapsed strategy's share goes to ~0 naturally while it keeps logging forever,
	// and it re-funds itself the moment its realized ¢/unit turns positive.

	fams := make([]string, 0, len(sn.Verdicts))
	for f := range sn.Verdicts {
		fams = append(fams, f)
	}
	sort.Strings(fams)

	// 1) FIRST SIGHTINGS: any verdict family (book:* internals excluded) without a record enters
	// watch (or no-executor watch, logged once) — this stamps first_seen in the state file.
	// Newly-sighted families join the SAME pass's promote evaluation (eff below): a PROVEN+
	// bettable family promotes on the pipeline's very first sweep, no 5-min lag.
	eff := make(map[string]promoRecord, len(sn.Records)+4)
	for f, r := range sn.Records {
		eff[f] = r
	}
	for _, f := range fams {
		if strings.HasPrefix(f, "book:") {
			continue // executor-internal measures, not promotable families
		}
		if _, ok := eff[f]; ok {
			continue
		}
		to := promoStateWatch
		if _, hasExec := sn.Executors[f]; !hasExec {
			to = promoStateNoExec
		}
		out = append(out, promoTransition{Family: f, From: "", To: to, Reason: "first sighting"})
		eff[f] = promoRecord{Family: f, State: to, FirstSeen: sn.Now.UTC().Format(time.RFC3339)}
	}

	// 2) PROMOTE / GROW / RETIRE / REFUND over the effective record set. R119 ordering: queued
	// strategies fund FIRST, FIFO by queue entry time (then name for determinism); everything
	// else evaluates in sorted-name order after them.
	recs := make([]string, 0, len(eff))
	for f := range eff {
		recs = append(recs, f)
	}
	sort.Slice(recs, func(i, j int) bool {
		ri, rj := eff[recs[i]], eff[recs[j]]
		qi, qj := ri.State == promoStateQueued, rj.State == promoStateQueued
		if qi != qj {
			return qi
		}
		if qi && ri.QueuedAt != rj.QueuedAt {
			return ri.QueuedAt < rj.QueuedAt
		}
		return recs[i] < recs[j]
	})
	for _, f := range recs {
		r := eff[f]
		ex, hasExec := sn.Executors[f]
		v, hasV := sn.Verdicts[f]
		switch r.State {
		case promoStateWatch, promoStateNoExec, promoStateQueued:
			if !hasExec || !hasV {
				continue
			}
			// R118: eligibility IS the verdict — PROVEN+ means the always-valid CS excluded zero
			// at honest sequential significance. No row-count or calendar-age rail on top of it.
			if v.State != "PROVEN+" || v.Locked {
				if r.State == promoStateQueued { // legacy queue record, verdict lapsed — back to watch
					out = append(out, promoTransition{Family: f, From: r.State, To: promoStateWatch,
						Reason: fmt.Sprintf("verdict no longer PROVEN+ (now %s) — back to watch (the R119 reserve queue is retired, R127)", v.State)})
				}
				continue
			}
			// R127: promotion moves NO money — the strategy activates as a SUB-STRATEGY of its
			// venue book with a share cap (default promotion_bankroll_usd); the venue book's bank
			// is the hard wall at placement time.
			out = append(out, promoTransition{Family: f, From: r.State, To: promoStatePromoted,
				NewAllocUSD: sn.Cfg.BankrollUSD,
				Reason: fmt.Sprintf("PROVEN+ (n=%d, edge %+.3f, 95%% [%+.3f, %+.3f]) — %s activates as a sub-strategy of its venue book with a $%.0f share cap",
					v.N, v.Mean, v.Lo, v.Hi, ex.Name, sn.Cfg.BankrollUSD)})
		case promoStatePromoted, promoStateGrown:
			if !hasExec {
				continue
			}
			bv, hasBV := sn.Verdicts[ex.BookFamily]
			st := sn.Books[f]
			// R128: the R127 RETIRE (book PROVEN−) and D4 DEMOTE-TO-WATCH (family lapsed) rules
			// are REMOVED — the scoreboard weight already drives a negative/lapsed strategy's
			// share to ~0 (scoreweight.go), placement never pauses, logging never stops, and the
			// share re-grows the moment the realized numbers turn. GROW survives as a journal
			// checkpoint only (AllocUSD is display/cold-cache fallback — the weight IS the share).
			threshold := promoGrowInitialN
			if r.LastCheckpointN > 0 {
				threshold = 2 * r.LastCheckpointN
			}
			if hasBV && bv.State == "PROVEN+" && st.SettledN >= threshold {
				delta := math.Min(r.AllocUSD, sn.Cfg.GrowCapUSD-r.AllocUSD) // double, capped
				if delta > 0.005 {
					out = append(out, promoTransition{Family: f, From: r.State, To: promoStateGrown,
						NewAllocUSD: r.AllocUSD + delta, CheckpointN: st.SettledN,
						Reason: fmt.Sprintf("checkpoint at %d settled rows, own edge still PROVEN+ (%+.3f, 95%% [%+.3f, %+.3f]) — journal share grows $%.0f → $%.0f (R128: the scoreboard weight is the live share)",
							st.SettledN, bv.Mean, bv.Lo, bv.Hi, r.AllocUSD, r.AllocUSD+delta)})
				}
			}
			_, _ = v, hasV // verdict lapse no longer transitions state (R128)
		case promoStateWatchD4:
			// R128 MIGRATION: WATCH no longer exists as a pause — every parked record returns to
			// PROMOTED unconditionally; the scoreboard weight governs its sizing from here on.
			out = append(out, promoTransition{Family: f, From: r.State, To: promoStatePromoted,
				NewAllocUSD: r.AllocUSD,
				Reason:      "R128 no-pause doctrine: WATCH retired as a state — placement resumes; the scoreboard weight (lifetime realized ¢/unit share) governs sizing"})
		case promoStateRetired:
			if r.Refunded {
				continue
			}
			st := sn.Books[f]
			if st.OpenLots == 0 { // full wind-down: the share releases (R127: no reserve money moves)
				out = append(out, promoTransition{Family: f, From: promoStateRetired, To: promoStateRetired,
					NewAllocUSD: 0,
					Reason:      fmt.Sprintf("wind-down complete — $%.0f share released; history kept (R127: no reserve money moves)", r.AllocUSD)})
			}
		}
	}
	return out
}

// ---- server plumbing --------------------------------------------------------------------------

// promoLoadLocked lazy-loads promotion_state.json. Caller holds promoMu. Bug-52 poison guard.
func (s *Server) promoLoadLocked() *promoState {
	if s.promoSt != nil {
		return s.promoSt
	}
	st := &promoState{Records: map[string]*promoRecord{}}
	path := filepath.Join(s.cfg().DataDir, "promotion_state.json")
	if !s.readJSONLoose(path, st) {
		if fi, err := os.Stat(path); err == nil && fi.Size() > 0 {
			s.promoPoisoned.Store(true)
			s.log.Error("promotion_state.json exists but failed to parse — promotion sweeps DISABLED this session (bug-52 guard)", "path", path, "size", fi.Size())
			_ = s.store.Audit(context.Background(), "error", "promotion", "promotion state file unreadable — pipeline paused this session; inspect data\\promotion_state.json", "")
		}
	}
	if st.Records == nil {
		st.Records = map[string]*promoRecord{}
	}
	s.promoSt = st
	return st
}

// promoFlushLocked persists the state atomically (tmp+rename). Caller holds promoMu.
func (s *Server) promoFlushLocked() {
	if s.promoPoisoned.Load() || s.promoSt == nil {
		return
	}
	s.promoSt.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	out, err := json.MarshalIndent(s.promoSt, "", " ")
	if err != nil {
		return
	}
	path := filepath.Join(s.cfg().DataDir, "promotion_state.json")
	tmp := path + ".tmp"
	if os.WriteFile(tmp, out, 0o644) == nil {
		_ = os.Rename(tmp, path)
	}
}

// promoRecordState returns a family's current pipeline state ("" = unknown). Cheap: one mutex +
// map read (freshInvActive calls this on every placement candidate).
func (s *Server) promoRecordState(family string) string {
	s.promoMu.Lock()
	defer s.promoMu.Unlock()
	if r, ok := s.promoLoadLocked().Records[family]; ok {
		// The legacy rolling-verdict pipeline is research-only. Historical PROMOTED/GROWN state is
		// preserved on disk for provenance, but it cannot activate a Paper or LIVE executor.
		switch r.State {
		case promoStatePromoted, promoStateGrown, promoStateWatchD4, promoStateQueued:
			return promoStateWatch
		}
		return r.State
	}
	return ""
}

// promoDataAges returns min(ts) per signal family from signal_log (90d window), cached for ~one
// sweep interval so the grouped query runs once per caller burst. R118: informational only —
// it feeds the /api/promotions data_age_days field; no promotion rail reads it.
func (s *Server) promoDataAges(ctx context.Context) map[string]time.Time {
	s.promoMu.Lock()
	if s.promoAges != nil && time.Since(s.promoAgesAt) < 4*time.Minute {
		m := s.promoAges
		s.promoMu.Unlock()
		return m
	}
	s.promoMu.Unlock()
	ages, err := s.store.FamilyFirstRows(ctx, time.Now().Add(-90*24*time.Hour))
	if err != nil {
		s.log.Warn("promotion: family data-age query failed — min-days rail falls back to pipeline first_seen", "err", err)
		return nil
	}
	s.promoMu.Lock()
	s.promoAges, s.promoAgesAt = ages, time.Now()
	s.promoMu.Unlock()
	return ages
}

func legacyRollingPromotionExecutionAllowed() bool { return false }

// sweepPromotions — the pipeline pass (5-min cadence next to sweepVerdicts): snapshot → pure
// promoDecide → apply transitions (reserve accounting, book bank, audit/telegram/docs) → persist.
func (s *Server) sweepPromotions(ctx context.Context) {
	cfgP := s.promoCfg()
	if !cfgP.Enabled || s.promoPoisoned.Load() {
		return
	}
	// Retired R139: rolling PROVEN+ leaderboard verdicts, row counts, and data age are monitoring
	// evidence only. The sole Paper bridge is sweepResearchPromotionDispatch, which requires an
	// immutable preregistered untouched pass plus frozen route/cause/Adaptive Allocation Model governance receipts.
	if !legacyRollingPromotionExecutionAllowed() {
		return
	}
	vs := s.computeExperimentVerdicts(ctx)
	if len(vs) == 0 {
		return
	}
	vmap := make(map[string]verdictEnt, len(vs))
	for _, v := range vs {
		vmap[v.Family] = v
	}
	execs := promoExecutorRegistry()

	// Executor book stats (freshinv + xvgap — extend per executor as books register).
	books := map[string]promoBookStats{}
	for fam, ex := range execs {
		switch ex.Name {
		case "freshinv":
			bank, net, settled, open := s.fiBookBankNet()
			books[fam] = promoBookStats{SettledN: settled, OpenLots: open, BankNet: bank + net}
		case "xvgap": // R122
			bank, net, settled, open := s.xvgBookBankNet()
			books[fam] = promoBookStats{SettledN: settled, OpenLots: open, BankNet: bank + net}
		}
	}

	s.promoMu.Lock()
	st := s.promoLoadLocked()
	if !st.ReserveInit {
		st.ReserveRemaining, st.ReserveInit = cfgP.ReserveUSD, true
	}
	// Pipeline NAV = reserve + every promoted/winding book's (bank + net). Peak tracked here —
	// the drawdown rail reads THIS pool, not the R78 portfolio NAV (its own explicit money).
	nav := st.ReserveRemaining
	for fam, r := range st.Records {
		if r.State == promoStatePromoted || r.State == promoStateGrown || (r.State == promoStateRetired && !r.Refunded) {
			nav += books[fam].BankNet
		}
	}
	if nav > st.NAVPeak {
		st.NAVPeak = nav
	}
	dd := 0.0
	if st.NAVPeak > 0.005 {
		dd = math.Max(0, (st.NAVPeak-nav)/st.NAVPeak*100)
	}
	st.LastNAV, st.LastDrawdownPct = math.Round(nav*100)/100, math.Round(dd*100)/100

	recCopy := make(map[string]promoRecord, len(st.Records))
	for f, r := range st.Records {
		recCopy[f] = *r
	}
	reserve := st.ReserveRemaining
	s.promoMu.Unlock()

	trans := promoDecide(promoSnap{Now: time.Now(), Cfg: cfgP, Verdicts: vmap, Executors: execs,
		Records: recCopy, Reserve: reserve, DrawdownPct: dd, Books: books})

	if len(trans) == 0 {
		s.promoMu.Lock()
		s.promoFlushLocked() // still persist nav/peak/drawdown for the panel
		s.promoMu.Unlock()
		return
	}

	now := time.Now().UTC().Format(time.RFC3339)
	for _, t := range trans {
		s.promoMu.Lock()
		r := st.Records[t.Family]
		if r == nil {
			r = &promoRecord{Family: t.Family, FirstSeen: now}
			st.Records[t.Family] = r
		}
		r.State = t.To
		r.Reason = t.Reason
		switch t.To {
		case promoStateQueued:
			r.QueuedAt = now // legacy path (R119) — unreachable since R127, kept for state-file compat
		case promoStateWatch:
			r.QueuedAt = "" // left the legacy queue (verdict lapsed)
		case promoStateWatchD4: // R127 D4: placement paused, share retained — nothing else moves
		case promoStatePromoted:
			if t.From == promoStateWatchD4 {
				// D4 return from WATCH: share + checkpoints retained — only the state flips back.
				r.AllocUSD = t.NewAllocUSD
			} else {
				if ex, ok := execs[t.Family]; ok {
					r.BookFile = ex.BookFile
				}
				r.AllocUSD, r.PromotedAt, r.LastCheckpointN = t.NewAllocUSD, now, 0
				r.QueuedAt = ""
			}
		case promoStateGrown:
			r.AllocUSD, r.Checkpoints, r.LastCheckpointN = t.NewAllocUSD, r.Checkpoints+1, t.CheckpointN
		case promoStateRetired:
			if t.From != promoStateRetired {
				r.RetiredAt = now
			} else { // RETIRED→RETIRED = the wind-down completion latch (share released)
				r.AllocUSD, r.Refunded = 0, true
			}
		}
		// R127: reserve accounting is RETIRED — every AllocDelta is 0 (field kept for history).
		st.ReserveRemaining = math.Round((st.ReserveRemaining-t.AllocDelta)*100) / 100
		s.promoFlushLocked()
		s.promoMu.Unlock()

		// R127: the book Bank field is JOURNAL-ONLY now (placement gates on the venue book's
		// available + the sub's share cap — books.go); the hook keeps the file's Bank mirroring
		// the share so old widgets/history stay coherent. WATCH transitions never touch it.
		if ex, ok := execs[t.Family]; ok &&
			(t.To == promoStatePromoted || t.To == promoStateGrown ||
				(t.To == promoStateRetired && t.From == promoStateRetired)) { // wind-down completion zeroes the journal bank
			switch ex.Name {
			case "freshinv":
				s.fiSetBank(t.NewAllocUSD)
			case "xvgap": // R122
				s.xvgSetBank(t.NewAllocUSD)
			}
		}

		if t.From == "" { // First sightings are recorded once in the audit log.
			lvl := "info"
			msg := fmt.Sprintf("promotion pipeline: new family %q sighted → %s", t.Family, t.To)
			if t.To == promoStateNoExec {
				msg += " (no registered executor — watch-only)"
			}
			_ = s.store.Audit(ctx, lvl, "promotion", msg, "")
			continue
		}
		line := fmt.Sprintf("Promotion pipeline: %s %s → %s — %s", t.Family, t.From, t.To, t.Reason)
		_ = s.store.Audit(ctx, "info", "promotion", line, "")
		s.TgSend(line + " (paper only — live stays operator-armed)")
	}
}

// handlePromotions (GET /api/promotions) — the state file + reserve + rails status for the panel.
func (s *Server) handlePromotions(w http.ResponseWriter, r *http.Request) {
	cfgP := s.promoCfg()
	ages := s.promoDataAges(r.Context()) // cached — at most one grouped query per sweep interval
	now := time.Now()
	s.promoMu.Lock()
	st := s.promoLoadLocked()
	recs := make([]promoRecord, 0, len(st.Records))
	for _, rec := range st.Records {
		cp := *rec // COPY — DataAgeDays is payload-only, never written to the state file
		switch cp.State {
		case promoStatePromoted, promoStateGrown, promoStateWatchD4, promoStateQueued:
			cp.State = promoStateWatch
			cp.Reason = "legacy rolling-verdict promotion retired; requires sealed untouched route governance"
		}
		if first, ok := promoFamilyDataStart(ages, cp.Family); ok {
			cp.DataAgeDays = math.Round(now.Sub(first).Hours()/24*100) / 100
		}
		recs = append(recs, cp)
	}
	reserve, navPeak, lastNAV, dd := st.ReserveRemaining, st.NAVPeak, st.LastNAV, st.LastDrawdownPct
	updated := st.UpdatedAt
	s.promoMu.Unlock()
	sort.Slice(recs, func(i, j int) bool {
		rank := func(s string) int {
			switch s {
			case promoStatePromoted, promoStateGrown:
				return 0
			case promoStateWatchD4: // R127 D4: paused-but-funded subs sort right under the active ones
				return 1
			case promoStateQueued:
				return 2
			case promoStateRetired:
				return 3
			case promoStateWatch:
				return 4
			}
			return 5
		}
		if a, b := rank(recs[i].State), rank(recs[j].State); a != b {
			return a < b
		}
		return recs[i].Family < recs[j].Family
	})
	execFams := []string{}
	for f := range promoExecutorRegistry() {
		execFams = append(execFams, f)
	}
	sort.Strings(execFams)
	writeJSON(w, http.StatusOK, map[string]any{
		"enabled": false, "legacy_config_enabled": cfgP.Enabled, "records": recs,
		"reserve_remaining": sanF(reserve), "nav": sanF(lastNAV), "nav_peak": sanF(navPeak),
		"drawdown_pct": sanF(dd), "drawdown_paused": dd > cfgP.DrawdownPausePct,
		"executors": execFams, "updated": updated,
		"rails": map[string]any{"bankroll_usd": cfgP.BankrollUSD, "max_books": "unlimited (R119)",
			"eligibility":  "legacy rolling verdicts are research-only; Paper requires sealed untouched all-system multiplicity control, positive executable per-share and Net/day lower bounds, capacity, capital-time, a fresh cause-graph eligibility receipt, and frozen Adaptive Allocation Model sizing inputs",
			"grow_cap_usd": cfgP.GrowCapUSD, "reserve_usd": cfgP.ReserveUSD,
			"drawdown_pause_pct": cfgP.DrawdownPausePct,
			"r127":               "promotion = a sub-strategy SHARE of the venue book (bankroll_usd is the default share cap; reserve machinery retired — fields kept for history); D4: a lapsed family verdict demotes PROMOTED→WATCH (placement paused, share retained) and re-clears back to PROMOTED"},
		"note": "legacy pipeline retired; the sealed research promotion bridge is the only Paper path and LIVE still requires an accepted identical Paper route plus explicit ARM and LIVE AUTO",
	})
}
