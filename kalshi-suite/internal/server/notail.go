package server

// notail.go — R127-D (B3): the NO-SIDE TAIL HARVESTER detector, signal family "notail". LOG-ONLY:
// there is NO placement path anywhere in this file or behind this family — rows only.
//
// Literature basis (the Becker 72M-trade study the operator flagged): NO beats YES at 69 of the
// 99 price levels, and the effect is steepest in the tail — at a 1¢ YES price the measured EV is
// about −41% for YES vs +23% for the matching NO. The structural read is the favorite-longshot
// bias: longshot YES buyers systematically overpay, so BUYING NO at 90–99¢ (selling the tail)
// harvests that premium where it is widest. This sweep only logs candidates into signal_log so
// the verdict engine (verdicts.go / FamilyEdgeStats) grades the family automatically under the
// standard TAKER-fee convention (0.07·p·(1−p)), exactly like every other family. The MAKER
// re-grade (post at the bid instead of lifting the ask) happens OFFLINE via the
// tools/r127_favlong.py pattern — each row carries spread_cents so that study can price the cross.
//
// Candidate filter (pure predicate notailCandidate, pinned in r127b_test.go):
//   - buying NO costs 90–99¢ (YES priced 1–10¢);
//   - BROAD-BASED boards only: the market's event has ≥3 sibling markets on the cached board.
//     Kalshi grouping key = event_ticker; some WS-sourced cache rows lack it, so the DOCUMENTED
//     PROXY is the ticker minus its last '-'-segment (Kalshi market tickers are
//     <event_ticker>-<outcome>, e.g. KXMLBGAME-26JUL07COLLAD-COL). PolyUS grouping key =
//     event_id, else the game string.
//   - minimal liquidity floor: nonzero traded volume OR a two-sided book.
//
// ZERO NEW API LOAD: reads only the kmkts cache (≤10-min-fresh rows — the same board the
// bookskew/freshlist detectors read) and the PolyUS gateway snapshot (the favlong-PUS source).
// Kalshi resolve-hours come from the CACHED row's expected_expiration/close_time (no REST);
// PolyUS uses sigResolveHours, which is snapshot + market_catalog only for that venue. Poly-int
// is SKIPPED — its rows would be venue-locked (unbettable) by construction.
//
// Dedup: ONE signal per market per UTC day — in-memory day-set + a single kv key
// (notail_day_latch) so a mid-day restart doesn't re-log the same tails. InsertSignal's ~10-min
// slot unique index is a second net underneath. ≤40 inserts per sweep keeps volume sane.

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/signal"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	notailYesMin      = 0.01 // YES 1–10¢ ⇒ NO 90–99¢: the tail band the Becker study measures
	notailYesMax      = 0.10
	notailNoMin       = 0.90
	notailNoMax       = 0.99
	notailMinSiblings = 3    // broad-based board: the event must carry ≥3 sibling markets
	notailSweepCap    = 40   // max inserts per sweep pass (both venues combined)
	notailDayCap      = 5000 // hard bound on the per-day dedup set (memory sanity)
	notailKVLatch     = "notail_day_latch"
)

// notailCandidate is the PURE candidate filter (r127b_test.go pins the boundaries): yes/noCost
// are the YES price and the cost of BUYING NO; bid/ask are the YES book touch; volume is any
// nonzero traded-volume figure the venue snapshot carries; siblings is the event's market count
// on the cached board.
func notailCandidate(yes, noCost, bid, ask, volume float64, siblings int) bool {
	if yes < notailYesMin || yes > notailYesMax {
		return false
	}
	if noCost < notailNoMin || noCost > notailNoMax {
		return false
	}
	if siblings < notailMinSiblings {
		return false // narrow board — a 2-outcome game at 95¢ is a favorite bet, not a tail harvest
	}
	twoSided := bid > 0 && ask > 0 && ask < 1
	if volume <= 0 && !twoSided {
		return false // liquidity floor: something must trade or quote both sides
	}
	return true
}

// notailKalEventKey groups a cached Kalshi market under its event: event_ticker when the row
// carries it, else the documented ticker-prefix proxy (strip the last '-'-segment).
func notailKalEventKey(m kalshi.Market) string {
	if m.EventTicker != "" {
		return m.EventTicker
	}
	if i := strings.LastIndex(m.Ticker, "-"); i > 0 {
		return m.Ticker[:i]
	}
	return ""
}

// notailPusEventKey groups a PolyUS snapshot row under its event (event_id, else the game label).
func notailPusEventKey(m polyUSMarket) string {
	if m.EventID != "" {
		return m.EventID
	}
	return m.Game
}

// notailMarkOnce returns true exactly once per (venue|ticker) per UTC day. On a day rollover the
// in-memory set reloads from the kv latch, so a restart can't re-log the same market today.
func (s *Server) notailMarkOnce(ctx context.Context, key string) bool {
	day := time.Now().UTC().Format("2006-01-02")
	s.ntMu.Lock()
	defer s.ntMu.Unlock()
	if s.ntDay != day {
		s.ntDay, s.ntSeen = day, map[string]bool{}
		if raw, ok := s.store.KVGet(ctx, notailKVLatch); ok && raw != "" {
			var st struct {
				Day  string   `json:"day"`
				Keys []string `json:"keys"`
			}
			if json.Unmarshal([]byte(raw), &st) == nil && st.Day == day {
				for _, k := range st.Keys {
					s.ntSeen[k] = true
				}
			}
		}
	}
	if s.ntSeen[key] || len(s.ntSeen) >= notailDayCap {
		return false
	}
	s.ntSeen[key] = true
	return true
}

// notailPersistLatch writes the day's dedup set to the single kv key (restart safety).
func (s *Server) notailPersistLatch(ctx context.Context) {
	s.ntMu.Lock()
	day := s.ntDay
	keys := make([]string, 0, len(s.ntSeen))
	for k := range s.ntSeen {
		keys = append(keys, k)
	}
	s.ntMu.Unlock()
	sort.Strings(keys)
	if b, err := json.Marshal(map[string]any{"day": day, "keys": keys}); err == nil {
		_ = s.store.KVSet(ctx, notailKVLatch, string(b))
	}
}

// notailKalHours — resolve-horizon from the CACHED market row only (expected_expiration, else
// close_time; both RFC3339). Deliberately NOT sigResolveHours on this venue: that path can REST
// on a cache miss and this sweep is pledged zero-new-API-load. close_time overstates the horizon
// on live games (it can sit days out) — acceptable for a structural hold-to-settle family and
// documented here; 0 = unknown, same convention as every other logger.
func notailKalHours(m kalshi.Market) float64 {
	for _, ts := range []string{m.ExpectedExpiration, m.CloseTime} {
		if ts == "" {
			continue
		}
		if t, err := time.Parse(time.RFC3339, ts); err == nil {
			if h := time.Until(t).Hours(); h > 0.1 {
				return h
			}
			return 0.1
		}
	}
	return 0
}

// sweepNoTail — the MonitorPaper sweep (10-min cadence). Cached boards only; log-only.
func (s *Server) sweepNoTail(ctx context.Context) {
	logged := 0

	// ── Kalshi: the kmkts cache, ≤10-min-fresh, active, unsettled rows only ──
	var kals []kalshi.Market
	evN := map[string]int{}
	s.metaMu.Lock()
	for tk, m := range s.kmkts {
		at, okAt := s.kmktsAt[tk]
		if !okAt || time.Since(at) > 10*time.Minute {
			continue // stale cache row — not evidence of a live price
		}
		if m.Result != "" || (m.Status != "" && !strings.EqualFold(m.Status, "active")) {
			continue
		}
		if ek := notailKalEventKey(m); ek != "" {
			evN[ek]++
		}
		kals = append(kals, m)
	}
	s.metaMu.Unlock()
	sort.Slice(kals, func(i, j int) bool { return kals[i].Ticker < kals[j].Ticker }) // deterministic map walk
	for _, m := range kals {
		if logged >= notailSweepCap {
			break
		}
		ek := notailKalEventKey(m)
		if ek == "" {
			continue
		}
		yes := m.ImpliedProbability()
		noCost := m.NoAsk.Float() // the venue's actual NO ask when quoted; else the YES complement
		if noCost <= 0 || noCost >= 1 {
			noCost = 1 - yes
		}
		if !notailCandidate(yes, noCost, m.YesBid.Float(), m.YesAsk.Float(), m.Volume24h.Float(), evN[ek]) {
			continue
		}
		if !s.notailMarkOnce(ctx, "kalshi|"+m.Ticker) {
			continue
		}
		_ = s.insertSignal(ctx, storage.Signal{Platform: "kalshi", Ticker: m.Ticker, Title: m.Title,
			Side: "NO", SignalType: "notail", EntryPrice: noCost, Strength: yes, // Strength = the YES tail price (bin key for the offline study)
			SpreadCents: signal.SpreadCents(m.YesBid.Float(), m.YesAsk.Float()),
			ResolveHours: notailKalHours(m), BookDepth: m.OpenInterest.Float(),
			Confidence: s.signalConfidence("notail", noCost)})
		logged++
	}

	// ── PolyUS: the gateway snapshot (favlong-PUS source). Pregame only — an in-play 1–10¢ price
	// is information (the game state), not the structural longshot bias this family measures. ──
	pus := s.polyUSSnapshot()
	pevN := map[string]int{}
	for _, m := range pus {
		if k := notailPusEventKey(m); k != "" {
			pevN[k]++
		}
	}
	for _, m := range pus {
		if logged >= notailSweepCap {
			break
		}
		if m.Slug == "" || m.Live {
			continue
		}
		ek := notailPusEventKey(m)
		if ek == "" {
			continue
		}
		yes, noCost := m.Yes, 1-m.Yes
		if !notailCandidate(yes, noCost, m.Bid, m.Ask, m.Volume, pevN[ek]) {
			continue
		}
		if !s.notailMarkOnce(ctx, "polyus|"+m.Slug) {
			continue
		}
		title := m.Game
		if m.TeamName != "" {
			title = m.TeamName + " — " + m.Game
		}
		_ = s.insertSignal(ctx, storage.Signal{Platform: "polyus", Ticker: m.Slug, Title: title,
			Side: "NO", SignalType: "notail", EntryPrice: noCost, Strength: yes,
			SpreadCents:  signal.SpreadCents(m.Bid, m.Ask),
			ResolveHours: s.sigResolveHours(ctx, "polyus", m.Slug), // snapshot + catalog only on this venue (no REST)
			Confidence:   s.signalConfidence("notail", noCost)})
		logged++
	}
	// (poly-int deliberately absent: rows there would grade venue-locked — unbettable by design.)

	if logged > 0 {
		s.notailPersistLatch(ctx)
	}
}
