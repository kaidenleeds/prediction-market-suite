package server

// comboprobe.go — R112: API-driven combo legality (operator doctrine correction, 2026-07-07):
// STOP inferring venue rules. The exchange's own answer IS the legality oracle. Trial and
// error against the real exchange, cached, is the system.
//
// R140 CURRENT-OFFICIAL-DOC CORRECTION: Kalshi now marks every multivariate lookup-history/
// ticker endpoint fully deprecated. The only current venue oracle for a never-created exact leg
// set is POST create-market-in-collection (a public listing, NOT an order or RFQ; no money moves;
// venue cap 5000/wk, ours 300/wk persisted). The active collector no longer calls PUT .../lookup.
// Old lookup-derived verdicts are orphaned by a kv namespace bump.
//
//   - When the parlay lab wants to know if a leg set is legal, it consults the probe cache
//     (keyed by the leg set's venue signature). Hit → legal_probed / illegal_probed(reason).
//     Miss → the candidate is tagged "unprobed" and queued; each cycle the HIGHEST-EV queued
//     candidates are probed first, so the interesting ones get answers.
//   - Budgeted like the RFQ fetcher: ≤100 lookup calls per 10-minute window (on top of the
//     shared AIMD limiter). Negative results are cached too, WITH the venue's reason string.
//   - Verdict TTL 24h, persisted in kv ("cprobe:" prefix) so answers survive restarts.
//   - The ONLY static pre-filter left is what the API structurally cannot combine: legs on
//     different venues (PUS combos are institutional-beta only, poly-int has no combos —
//     R110 research) and verified-identical twin legs (degenerate). Everything else asks
//     the exchange.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

const (
	cprobeTTL     = 24 * time.Hour
	cprobeBudget  = 100 // venue calls per window (the RFQ-fetcher budget shape)
	cprobeWindow  = 10 * time.Minute
	cprobePerTick = 15 // max candidates consumed per parlay-lab tick

	// cprobeKVPrefix — R140 BUMP "cprobe2:"→"cprobe3:". cprobe2 mixed answers from the now
	// fully-deprecated PUT lookup with POST-create answers. cprobe3 means the venue accepted or
	// refused this exact leg set through the current documented create-market endpoint.
	cprobeKVPrefix = "cprobe3:"

	// Rolling-window miss: the venue rolls events into collections near their start (crypto:
	// only the CURRENT 15-min/hourly events are listed). "Not in any collection right now" is
	// TEMPORARY, so it gets a short TTL instead of the full 24h.
	cprobeOutOfWindow = "out_of_collection_window: no open collection currently lists all leg events (rolling window — retried hourly)"
	cprobeSoftTTL     = time.Hour

	// CreateComboProbe weekly budget: the escalation path for lookup-404 combos instantiates
	// the combined market (a listing, not an order). Venue cap is 5000/week per user; stay
	// far below it and leave room for real MVE trading.
	cprobeCreateWeekly = 300
	cprobeCreateWinKV  = cprobeKVPrefix + "createwin"
)

type cprobeVerdict struct {
	Verdict    string    `json:"verdict"` // legal_probed | illegal_probed | unmatched_window (R114, auditor 367)
	Reason     string    `json:"reason,omitempty"`
	Collection string    `json:"collection,omitempty"`
	At         time.Time `json:"at"`
	TTLSec     int       `json:"ttl_sec,omitempty"` // R114 (auditor 367): per-verdict TTL override (short-lived legs)
}

type cprobeCand struct {
	Legs []plabLeg `json:"legs"`
	EV   float64   `json:"ev"`
}

// cprobeKey — the leg set's venue signature: sorted market|side pairs, order-insensitive.
func cprobeKey(legs []plabLeg) string {
	parts := make([]string, 0, len(legs))
	for _, l := range legs {
		parts = append(parts, strings.ToUpper(l.Ticker)+"|"+strings.ToLower(l.Side))
	}
	sort.Strings(parts)
	h := sha256.Sum256([]byte(strings.Join(parts, "+")))
	return hex.EncodeToString(h[:12])
}

// cprobeLoad restores persisted verdicts once per boot (fresh answers survive restarts).
func (s *Server) cprobeLoad(ctx context.Context) {
	s.cprobeMu.Lock()
	defer s.cprobeMu.Unlock()
	if s.cprobeLoaded {
		return
	}
	s.cprobeLoaded = true
	// init-if-nil ONLY: enqueues can legitimately precede the first load in one tick — a reset
	// here would wipe them (caught live in R112 verification: 60 candidates queued, 0 probed).
	if s.cprobeVerdict == nil {
		s.cprobeVerdict = map[string]cprobeVerdict{}
	}
	if s.cprobeQueue == nil {
		s.cprobeQueue = map[string]cprobeCand{}
	}
	if s.cprobeReasons == nil {
		s.cprobeReasons = map[string]int{}
	}
	rows, err := s.store.KVPrefix(ctx, cprobeKVPrefix)
	if err != nil {
		return
	}
	for k, v := range rows {
		if k == cprobeCreateWinKV { // persisted create-budget window, not a verdict
			var w struct {
				At   time.Time `json:"at"`
				Used int       `json:"used"`
			}
			if json.Unmarshal([]byte(v), &w) == nil {
				s.cprobeCreateAt, s.cprobeCreateUsed = w.At, w.Used
			}
			continue
		}
		var pv cprobeVerdict
		if json.Unmarshal([]byte(v), &pv) != nil || time.Since(pv.At) > cprobeVerdictTTL(pv) {
			continue
		}
		if _, live := s.cprobeVerdict[strings.TrimPrefix(k, cprobeKVPrefix)]; live {
			continue // fresher in-memory answer wins over the kv copy
		}
		s.cprobeVerdict[strings.TrimPrefix(k, cprobeKVPrefix)] = pv
		if pv.Reason != "" {
			s.cprobeReasons[cprobeReasonShort(pv.Reason)]++
		}
	}
}

// cprobeVerdictTTL — rolling-window misses expire fast (the event may roll in within the hour);
// real venue answers keep the 24h TTL.
func cprobeVerdictTTL(v cprobeVerdict) time.Duration {
	if v.TTLSec > 0 { // R114 (auditor 367): short-lived legs get proportionally short negatives
		return time.Duration(v.TTLSec) * time.Second
	}
	if v.Reason == cprobeOutOfWindow || v.Verdict == "unmatched_window" {
		return cprobeSoftTTL
	}
	return cprobeTTL
}

// comboVerdict returns the cached venue answer for a leg set, if fresh.
func (s *Server) comboVerdict(legs []plabLeg) (cprobeVerdict, bool) {
	key := cprobeKey(legs)
	s.cprobeMu.Lock()
	defer s.cprobeMu.Unlock()
	v, ok := s.cprobeVerdict[key]
	if !ok || time.Since(v.At) > cprobeVerdictTTL(v) {
		return cprobeVerdict{}, false
	}
	return v, true
}

// comboEnqueue queues a leg set for probing (keeps the highest EV seen for the set).
func (s *Server) comboEnqueue(legs []plabLeg, ev float64) {
	if !validParlayLegCount(len(legs)) {
		return
	}
	key := cprobeKey(legs)
	s.cprobeMu.Lock()
	defer s.cprobeMu.Unlock()
	if s.cprobeQueue == nil {
		s.cprobeQueue = map[string]cprobeCand{}
	}
	if old, ok := s.cprobeQueue[key]; !ok || ev > old.EV {
		cp := make([]plabLeg, len(legs))
		copy(cp, legs)
		s.cprobeQueue[key] = cprobeCand{Legs: cp, EV: ev}
	}
	// hard cap so a runaway enumerator can't grow the queue unbounded: drop the lowest-EV
	if len(s.cprobeQueue) > 2000 {
		worstK, worstEV := "", 1e18
		for k, c := range s.cprobeQueue {
			if c.EV < worstEV {
				worstK, worstEV = k, c.EV
			}
		}
		delete(s.cprobeQueue, worstK)
	}
}

// cprobeReasonShort canonicalizes a venue error body to a short reason bucket for reporting.
func cprobeReasonShort(reason string) string {
	var e struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if json.Unmarshal([]byte(reason), &e) == nil {
		if e.Error.Code != "" || e.Error.Message != "" {
			return strings.TrimSpace(e.Error.Code + " " + e.Error.Message)
		}
		if e.Code != "" || e.Message != "" {
			return strings.TrimSpace(e.Code + " " + e.Message)
		}
	}
	if len(reason) > 120 {
		reason = reason[:120]
	}
	return reason
}

// comboProbeTick — probe the highest-EV queued candidates against the live exchange, under
// the 100/10min budget. Called from parlayLabTick (venue calls also ride the AIMD limiter).
func (s *Server) comboProbeTick(ctx context.Context) {
	if s.kal == nil || ctx.Err() != nil {
		return // hermetic tests / no venue client
	}
	s.cprobeLoad(ctx)
	s.cprobeMu.Lock()
	if time.Since(s.cprobeWinAt) > cprobeWindow {
		s.cprobeWinAt, s.cprobeWinUsed = time.Now(), 0
	}
	avail := cprobeBudget - s.cprobeWinUsed
	// drain: top-EV first
	type qe struct {
		key string
		c   cprobeCand
	}
	q := make([]qe, 0, len(s.cprobeQueue))
	for k, c := range s.cprobeQueue {
		q = append(q, qe{k, c})
	}
	s.cprobeMu.Unlock()
	if avail <= 0 || len(q) == 0 {
		return
	}
	sort.Slice(q, func(i, j int) bool { return q[i].c.EV > q[j].c.EV })
	if len(q) > cprobePerTick {
		q = q[:cprobePerTick]
	}
	cols := s.comboCollections(ctx)
	if len(cols) == 0 || ctx.Err() != nil {
		return // collections list unavailable — retry next tick, don't guess
	}
	for _, e := range q {
		if avail <= 0 || ctx.Err() != nil {
			break
		}
		spent, done := s.cprobeOne(ctx, e.key, e.c, cols, avail)
		avail -= spent
		s.cprobeMu.Lock()
		s.cprobeWinUsed += spent
		if done {
			delete(s.cprobeQueue, e.key)
		}
		s.cprobeMu.Unlock()
	}
}

// cprobeOne asks the exchange about ONE leg set. Returns (budget spent, resolved).
func (s *Server) cprobeOne(ctx context.Context, key string, c cprobeCand, cols []kalshi.MVCollection, avail int) (int, bool) {
	if ctx.Err() != nil {
		return 0, false
	}
	if !validParlayLegCount(len(c.Legs)) {
		s.cprobeStore(ctx, key, cprobeVerdict{Verdict: "invalid", Reason: fmt.Sprintf("combo needs 2-%d legs", parlayLegLimit), At: time.Now()})
		return 0, true
	}
	legs := make([]kalshi.MVELeg, 0, len(c.Legs))
	events := make([]string, 0, len(c.Legs))
	for _, l := range c.Legs {
		ev := s.plabEventTicker(ctx, l, cols)
		legs = append(legs, kalshi.MVELeg{MarketTicker: strings.ToUpper(l.Ticker), EventTicker: ev, Side: strings.ToLower(l.Side)})
		events = append(events, ev)
	}
	// candidate collections: ONLY collections containing ALL leg events. R113: NO best-overlap
	// fallback — probing a collection that doesn't list a leg's event can only produce a
	// membership refusal, which R112 wrongly cached as the combo being illegal. When nothing
	// matches, that's a rolling-window miss (the venue adds events near start time — crypto:
	// only the CURRENT 15-min/hourly events are listed) → short-TTL negative, retried hourly.
	cand := make([]kalshi.MVCollection, 0, 4)
	for _, col := range cols {
		all := true
		for _, ev := range events {
			if _, ok := col.Events[ev]; !ok {
				all = false
				break
			}
		}
		if all {
			cand = append(cand, col)
		}
	}
	if len(cand) == 0 {
		// R114 (auditor 367, two bugs): (1) this path stored "illegal_probed" though NO probe was
		// ever sent — 3,044/3,044 candidates read as venue-illegal while probes_sent=0, poisoning
		// the legality telemetry. Distinct verdict now, counted separately. (2) the 1h soft TTL
		// out-lived short-cycle markets entirely (15-min crypto rolls into a collection near start;
		// sealed-for-an-hour = sealed-for-life) — scale the negative TTL to half the nearest leg's
		// remaining life so rolling-window candidates get re-probed while still alive.
		ttl := cprobeSoftTTL
		for _, l := range c.Legs {
			if m, ok := s.kmkt(l.Ticker); ok && m.CloseTime != "" {
				if ct, err := time.Parse(time.RFC3339, m.CloseTime); err == nil {
					if rem := time.Until(ct) / 2; rem > 0 && rem < ttl {
						ttl = rem
					}
				}
			}
		}
		if ttl < time.Minute {
			ttl = time.Minute
		}
		s.cprobeStore(ctx, key, cprobeVerdict{Verdict: "unmatched_window", Reason: cprobeOutOfWindow, At: time.Now(), TTLSec: int(ttl / time.Second)})
		return 0, true
	}
	// Legality is collection-scoped. A refusal from the first few eligible collections cannot be
	// cached as a global refusal because a later collection may accept the exact same legs. Walk
	// every currently eligible collection in deterministic order; if the rolling API budget ends
	// mid-candidate, keep it queued and resume rather than manufacturing a negative.
	sort.Slice(cand, func(i, j int) bool { return cand[i].CollectionTicker < cand[j].CollectionTicker })
	spent, lastReason := 0, ""
	for _, col := range cand {
		if spent >= avail || ctx.Err() != nil {
			return spent, false // out of budget mid-candidate — keep queued
		}
		// Current venue oracle: instantiate/read the exact combined market through the documented
		// POST. Success proves Kalshi accepted the security definition. It still does NOT prove a
		// maker will quote the later RFQ or confirm after acceptance.
		if spent >= avail {
			return spent, false
		}
		if ctx.Err() != nil {
			return spent, false
		}
		if !s.cprobeCreateAllow(ctx) {
			return spent, false // weekly create budget exhausted — keep queued for later
		}
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		ok, reason, err := s.kal.CreateComboProbe(cctx, col.CollectionTicker, legs)
		cancel()
		spent++
		if err != nil {
			s.log.Warn("combo create-market probe transport error — will retry", "err", err)
			return spent, false
		}
		s.cprobeMu.Lock()
		s.cprobeSent++
		s.cprobeMu.Unlock()
		if ok {
			s.cprobeStore(ctx, key, cprobeVerdict{Verdict: "legal_probed", Collection: col.CollectionTicker, At: time.Now()})
			return spent, true
		}
		lastReason = reason
	}
	s.cprobeStore(ctx, key, cprobeVerdict{Verdict: "illegal_probed", Reason: lastReason, At: time.Now()})
	return spent, true
}

// cprobeCreateAllow spends one unit of the weekly CreateComboProbe budget (persisted so
// restarts can't reset it). Returns false when the week's budget is gone.
func (s *Server) cprobeCreateAllow(ctx context.Context) bool {
	s.cprobeMu.Lock()
	if time.Since(s.cprobeCreateAt) > 7*24*time.Hour {
		s.cprobeCreateAt, s.cprobeCreateUsed = time.Now(), 0
	}
	if s.cprobeCreateUsed >= cprobeCreateWeekly {
		s.cprobeMu.Unlock()
		return false
	}
	s.cprobeCreateUsed++
	at, used := s.cprobeCreateAt, s.cprobeCreateUsed
	s.cprobeMu.Unlock()
	if b, err := json.Marshal(map[string]any{"at": at, "used": used}); err == nil {
		_ = s.store.KVSet(ctx, cprobeCreateWinKV, string(b))
	}
	return true
}

func (s *Server) cprobeStore(ctx context.Context, key string, v cprobeVerdict) {
	s.cprobeMu.Lock()
	if s.cprobeVerdict == nil {
		s.cprobeVerdict = map[string]cprobeVerdict{}
	}
	if s.cprobeReasons == nil {
		s.cprobeReasons = map[string]int{}
	}
	s.cprobeVerdict[key] = v
	switch v.Verdict {
	case "legal_probed":
		s.cprobeLegal++
	case "unmatched_window": // R114 (auditor 367): a window miss is NOT a venue answer
		s.cprobeWindowMiss++
		s.cprobeReasons[cprobeReasonShort(v.Reason)]++
	default:
		s.cprobeIllegal++
		s.cprobeReasons[cprobeReasonShort(v.Reason)]++
	}
	s.cprobeMu.Unlock()
	if b, err := json.Marshal(v); err == nil {
		_ = s.store.KVSet(ctx, cprobeKVPrefix+key, string(b))
	}
}

// plabEventTicker resolves a leg's event ticker (market snapshot first, then the R111
// strip-until-a-collection-recognizes-it fallback for player props).
func (s *Server) plabEventTicker(ctx context.Context, l plabLeg, cols []kalshi.MVCollection) string {
	if m, ok := s.kmkt(l.Ticker); ok && m.EventTicker != "" {
		return m.EventTicker
	}
	inAnyCol := func(ev string) bool {
		for _, c := range cols {
			if _, ok := c.Events[ev]; ok {
				return true
			}
		}
		return false
	}
	ev := ""
	for cand := l.Ticker; ; {
		i := strings.LastIndex(cand, "-")
		if i <= 0 {
			break
		}
		cand = cand[:i]
		ev = cand
		if len(cols) == 0 || inAnyCol(cand) || strings.Count(cand, "-") < 2 {
			break
		}
	}
	return ev
}

// cprobeStats — probe telemetry for /api/parlaylab and the pass report.
func (s *Server) cprobeStats() map[string]any {
	s.cprobeMu.Lock()
	defer s.cprobeMu.Unlock()
	type rc struct {
		Reason string `json:"reason"`
		N      int    `json:"n"`
	}
	reasons := make([]rc, 0, len(s.cprobeReasons))
	for r, n := range s.cprobeReasons {
		reasons = append(reasons, rc{r, n})
	}
	sort.Slice(reasons, func(i, j int) bool { return reasons[i].N > reasons[j].N })
	if len(reasons) > 10 {
		reasons = reasons[:10]
	}
	return map[string]any{
		"probes_sent": s.cprobeSent, "legal": s.cprobeLegal, "illegal": s.cprobeIllegal,
		"window_miss": s.cprobeWindowMiss, // R114 (auditor 367): rolling-window misses, no longer counted illegal
		"cached":      len(s.cprobeVerdict), "queued": len(s.cprobeQueue),
		"window_used": s.cprobeWinUsed, "window_budget": cprobeBudget,
		"create_week_used": s.cprobeCreateUsed, "create_week_budget": cprobeCreateWeekly,
		"top_reasons": reasons,
	}
}
