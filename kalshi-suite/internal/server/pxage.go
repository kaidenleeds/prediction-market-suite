package server

// pxage.go — R102 Part 3: decision-time zero-staleness pricing.
//
// Operator ask: "refresh as close to 0 latency as possible" at DECISION points — paper/live
// placement gates, the quarantine border check, the top-scored ranking emit. The plumbing was
// already WS-first (curKalshiYes/curPolyUSYes since R98/R100); what was missing is (a) proof —
// nothing measured the AGE of the price each decision actually consumed, and (b) PolyUS coverage —
// the WS silently subscribed 100 of 1200 requested slugs (bug 242, fixed this pass), so ~92% of
// PUS decisions actually fell to the 15s REST snapshot.
//
// This module adds source+age-aware price reads that keep the EXACT price-selection semantics of
// curKalshiYes/curPolyUSYes (no money-path behavior change) and record, per decision point:
//   - px_src (ws | kob_cache | rest_book | snapshot) and its age at the moment of the decision
//   - the REST-cache-era age (what the same decision WOULD have consumed pre-WS: the kmkts entry
//     age on Kalshi, the polyUSMkts snapshot age on PUS) — the honest before/after baseline.
// Medians surface at GET /api/pxage and in a 30-min INFO log line. Sub-ms polling loops are NOT
// the way (rate limits + CPU); event-driven freshness at decision time IS.

import (
	"context"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"time"
)

// pxAgeBucket is one decision-point|source reservoir (bounded, random-replace at cap).
type pxAgeBucket struct {
	n    int64
	ages []float64 // ms
}

const pxAgeCap = 512

func (b *pxAgeBucket) add(ms float64) {
	b.n++
	if len(b.ages) < pxAgeCap {
		b.ages = append(b.ages, ms)
		return
	}
	b.ages[rand.Intn(pxAgeCap)] = ms
}

func (b *pxAgeBucket) median() float64 {
	if len(b.ages) == 0 {
		return -1
	}
	cp := append([]float64(nil), b.ages...)
	sort.Float64s(cp)
	return cp[len(cp)/2]
}

// notePxAge records one decision-price observation. src "" is skipped. restAge < 0 = no baseline.
func (s *Server) notePxAge(point, src string, age, restAge time.Duration) {
	if point == "" || src == "" {
		return
	}
	s.pxAgeMu.Lock()
	if s.pxAgeAgg == nil {
		s.pxAgeAgg = map[string]*pxAgeBucket{}
	}
	get := func(k string) *pxAgeBucket {
		b := s.pxAgeAgg[k]
		if b == nil {
			b = &pxAgeBucket{}
			s.pxAgeAgg[k] = b
		}
		return b
	}
	get(point + "|" + src).add(float64(age) / float64(time.Millisecond))
	if restAge >= 0 {
		get(point + "|rest_era").add(float64(restAge) / float64(time.Millisecond))
	}
	s.pxAgeMu.Unlock()
}

// pxAgeSnapshot renders the aggregate: per point|src median/count.
func (s *Server) pxAgeSnapshot() map[string]any {
	out := map[string]any{}
	s.pxAgeMu.Lock()
	for k, b := range s.pxAgeAgg {
		out[k] = map[string]any{"n": b.n, "median_ms": b.median()}
	}
	s.pxAgeMu.Unlock()
	return out
}

// handlePxAge — GET /api/pxage: the decision-price freshness report (R102 verification surface).
func (s *Server) handlePxAge(w http.ResponseWriter, r *http.Request) {
	resp := map[string]any{"points": s.pxAgeSnapshot()}
	if s.polyUSWS != nil {
		slugs, frames, at := s.polyUSWS.SubPlan()
		full, lite, trade := s.polyUSWS.SubCoverage()
		openProofs, openAt := s.polyUSWS.OpenSlugStats()
		cacheLive, cachePruned := s.polyUSWS.CacheStats()
		conn := s.polyUSWS.ConnectionReceipt()
		lastCloseAt := ""
		if !conn.LastCloseAt.IsZero() {
			lastCloseAt = conn.LastCloseAt.UTC().Format(time.RFC3339)
		}
		mode, ov, yv := s.polyUSWS.DenomStats()
		frameAgeMS := -1.0
		if age, ok := s.polyUSWS.PrimaryFrameAge(); ok {
			frameAgeMS = float64(age) / float64(time.Millisecond)
		}
		dataAge, primaryAge, haveData, havePrimary := s.polyUSWS.PrimaryDataAge()
		dataAgeMS, primaryAgeMS := -1.0, -1.0
		if haveData {
			dataAgeMS = float64(dataAge) / float64(time.Millisecond)
		}
		if havePrimary {
			primaryAgeMS = float64(primaryAge) / float64(time.Millisecond)
		}
		resp["polyus_ws"] = map[string]any{
			"sub_slugs_requested": slugs, "sub_frames": frames, "sub_at": at.UTC().Format(time.RFC3339),
			"sub_full_slugs": full, "sub_full_cap": s.polyUSWS.FullBookCap(), "sub_lite_slugs": lite, "sub_trade_slugs": trade,
			"priority_plan":    s.depthBookPlanView("polyus"),
			"rest_open_proofs": openProofs, "rest_open_at": openAt.UTC().Format(time.RFC3339),
			"primary_frame_age_ms": frameAgeMS, "primary_data_age_ms": dataAgeMS, "primary_age_ms": primaryAgeMS,
			"fresh_slugs": s.polyUSWS.Count(), "executable_slugs": s.polyUSWS.ExecutableCount(),
			"cache_live": cacheLive, "cache_pruned": cachePruned,
			"active_generation": conn.ActiveGeneration, "sessions": conn.Sessions,
			"primary_switches": conn.PrimarySwitches, "disconnects": conn.Disconnects,
			"quiet_executable_slugs": conn.QuietExecutable, "current_generation_data_seen": conn.DataSeen,
			"last_close_at": lastCloseAt, "last_close_error": conn.LastCloseError,
			"denom_mode": mode, "denom_votes_outcome": ov, "denom_votes_yes": yv, // bug 243 evidence
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// curKalshiYesAged mirrors curKalshiYes' price selection EXACTLY (WS fresh ≤30s → kob ≤4s → REST
// book fetch) while reporting which source served and how old its price was. point != "" also
// records telemetry, with the REST-cache-era baseline (kmkts entry age) alongside.
func (s *Server) curKalshiYesAged(ctx context.Context, point, ticker string) (float64, bool) {
	src, age := "", time.Duration(0)
	yes, ok := 0.0, false
	if lp, at, wok := s.kal.LivePriceAt(ticker); wok && time.Since(at) <= 30*time.Second && lp > 0 && lp < 1 {
		yes, ok, src, age = lp, true, "ws", time.Since(at)
	} else {
		s.kobMu.Lock()
		if e, cok := s.kobCache[ticker]; cok && time.Since(e.at) < 4*time.Second {
			yes, ok, src, age = e.yes, e.yes > 0, "kob_cache", time.Since(e.at)
			s.kobMu.Unlock()
		} else {
			s.kobMu.Unlock()
			yes, ok = s.curKalshiYes(ctx, ticker) // cold path: the shared REST-book fetch + cache fill
			src, age = "rest_book", 0
		}
	}
	if point != "" && ok {
		restAge := time.Duration(-1)
		s.metaMu.Lock()
		if at, has := s.kmktsAt[ticker]; has {
			restAge = time.Since(at)
		}
		s.metaMu.Unlock()
		s.notePxAge(point, src, age, restAge)
	}
	return yes, ok
}

// curPolyUSYesAged mirrors curPolyUSYes with source+age telemetry. MarketsWS already
// fail-closes on generation, primary-transport, and lifecycle authority; an unchanged complete
// PolyUS snapshot observed quiet at runtime remains executable while that authority stays current.
func (s *Server) curPolyUSYesAged(point, slug string) (float64, bool) {
	if slug == "" {
		return 0, false
	}
	if s.polyUSWS != nil {
		if ly, at, ok := s.polyUSWS.LiveYesAt(slug); ok && ly > 0 {
			if point != "" {
				s.polyUSMu.Lock()
				restAge := time.Since(s.polyUSAt)
				s.polyUSMu.Unlock()
				s.notePxAge(point, "ws", time.Since(at), restAge)
			}
			return ly, true
		}
	}
	s.polyUSMu.Lock()
	snapAge := time.Since(s.polyUSAt)
	var px float64
	for i := range s.polyUSMkts {
		if s.polyUSMkts[i].Slug == slug && s.polyUSMkts[i].Yes > 0 {
			px = s.polyUSMkts[i].Yes
			break
		}
	}
	s.polyUSMu.Unlock()
	if px > 0 {
		if point != "" {
			s.notePxAge(point, "snapshot", snapAge, snapAge)
		}
		return px, true
	}
	return 0, false
}

// sideLivePriceAt is sideLivePrice with a decision-point tag (the placement-gate / adverse-check
// callers pass their point so the freshness report attributes ages to real decisions).
func (s *Server) sideLivePriceAt(ctx context.Context, point, platform, ticker, side string) (float64, bool) {
	switch strings.ToLower(platform) {
	case "kalshi", "":
		yes, ok := s.curKalshiYesAged(ctx, point, ticker)
		if !ok || yes <= 0 || yes >= 1 {
			yes, ok = s.cachedYesMark(ticker)
			if ok && point != "" {
				s.metaMu.Lock()
				restAge := time.Duration(-1)
				if at, has := s.kmktsAt[ticker]; has {
					restAge = time.Since(at)
				}
				s.metaMu.Unlock()
				s.notePxAge(point, "rest_cache", restAge, restAge)
			}
		}
		if ok && yes > 0 && yes < 1 {
			if strings.EqualFold(side, "NO") {
				return 1 - yes, true
			}
			return yes, true
		}
	case "polyus":
		if yes, ok := s.curPolyUSYesAged(point, ticker); ok && yes > 0 && yes < 1 {
			if strings.EqualFold(side, "NO") {
				return 1 - yes, true
			}
			return yes, true
		}
	}
	return 0, false
}
