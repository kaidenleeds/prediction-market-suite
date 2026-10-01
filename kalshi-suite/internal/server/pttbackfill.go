package server

// R125 PTT BACKFILL WORKER — the operator's "10%-resolved problem" (4.2M ungraded poly-int trade
// rows = 114k distinct conditions at build time).
//
// ROOT CAUSE (live-verified 2026-07-09): gamma's /markets?condition_ids= lookup now EXCLUDES
// closed markets unless &closed=true is passed — every settled market looked deleted to the
// resolution sweep, so only the tiny CLOB fallback (10 conditions/minute) ever graded anything.
// The client fix (MarketByCondition / MarketsByConditions two-pass) restores the per-minute sweep
// for the steady state; THIS worker drains the historical backlog the sweep's retry-rotation
// would take months to revisit.
//
// DESIGN: keyset-cursored walk over DISTINCT unresolved conditions, oldest-newest-trade first
// (OldestUnresolvedConditionsPage). Per condition, venue truth decides:
//   resolved outcome        → grade rows (ResolvePolyTraderTradesIdx) + any open signal_log rows
//   voided (closed, uma resolved, no decisive price) → resolved=-2 (unresolvable-at-venue)
//   deleted (gamma absent incl. closed=true AND CLOB 404, condition ≥3d old) → resolved=-2
//   still open / resolution pending → BumpConditionRetry (the sweep's own backoff)
// Budgets: ≤pttBfPageConds conditions/pass, gamma chunks paced ~worst 4 req per 40-cond chunk with
// pttBfChunkPause between chunks (≲6 req/s vs the venue's published 30/s /markets cap), CLOB
// fallback ≤pttBfCLOBBudget per pass at ≥200ms spacing (cap 900/s). Cursor persists to kv so
// restarts resume mid-walk. Log-only data hygiene: no placement paths anywhere near this file.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarket"
)

const (
	pttBfPageConds  = 1200                   // max conditions per pass
	pttBfRowLimit   = 8000                   // rows per keyset page (dedupes to conditions Go-side)
	pttBfChunkSize  = 40                     // conditions per gamma batch (2 chunked calls + ≤2 closed-pass calls)
	pttBfChunkPause = 700 * time.Millisecond // pacing between gamma batches
	pttBfCLOBBudget = 300                    // per-pass CLOB fallback lookups
	pttBfPassPause  = 45 * time.Second       // between passes
	pttBfCyclePause = 15 * time.Minute       // after a full walk completes (backlog drained → idle poll)
	pttBfMinAge     = time.Hour              // conditions younger than this belong to the sweep
	pttBfCursorKV   = "ptt_backfill_cursor"
)

// pttBackfillState — cursor + lifetime counters (served at /api/pttbackfill, journaled hourly).
// Backlog* are the worker's own hourly snapshot — the API handler NEVER queries the big table
// on the request path (the first cut did, and timed out under the worker's write load).
type pttBackfillState struct {
	CursorTS     int64  `json:"cursor_ts"`
	CursorID     int64  `json:"cursor_id"`
	Cycles       int    `json:"cycles_completed"`
	Graded       int64  `json:"conds_graded"`
	Voided       int64  `json:"conds_voided"`
	Deleted      int64  `json:"conds_deleted"`
	Open         int64  `json:"conds_still_open"`
	Pending      int64  `json:"conds_pending_resolution"`
	Errors       int64  `json:"conds_transient_errors"`
	LastPass     string `json:"last_pass"`
	LastNote     string `json:"last_note"`
	BacklogRows  int64  `json:"backlog_rows"`
	BacklogConds int64  `json:"backlog_conds"`
	GradedRows   int64  `json:"graded_rows"`
	UnresRows    int64  `json:"unresolvable_rows"`
	StatsAt      string `json:"stats_at"`
}

// MonitorPTTBackfill — guard-launched worker loop.
func (s *Server) MonitorPTTBackfill(ctx context.Context) {
	select { // boot delay: let feeds/DB settle first
	case <-ctx.Done():
		return
	case <-time.After(2 * time.Minute):
	}
	// resume the persisted cursor
	if raw, ok := s.store.KVGet(ctx, pttBfCursorKV); ok && raw != "" {
		var st pttBackfillState
		if json.Unmarshal([]byte(raw), &st) == nil {
			s.pttBfMu.Lock()
			s.pttBfState = st
			s.pttBfMu.Unlock()
		}
	}
	lastJournal := time.Time{}
	checked := map[string]int64{} // condition → unix of last venue check (skip re-checks <4h; open conds recur in later row pages)
	for {
		if ctx.Err() != nil {
			return
		}
		wrapped := false
		// Historical Poly-int wallet grading is research maintenance, not funded settlement.
		// A production soak found this worker starting after ARM and scanning the multi-million-row
		// tape while LIVE account/order reads were waiting. The cooperative gate now defers it for
		// the entire armed session and cancels an admitted pass if the operator arms mid-page.
		ran := s.tryRunHeavyResearch(ctx, "ptt-backfill", 0, func(workCtx context.Context) {
			wrapped = s.pttBackfillPass(workCtx, checked)
		})
		if !ran {
			select {
			case <-ctx.Done():
				return
			case <-time.After(15 * time.Second):
				continue
			}
		}
		if len(checked) > 150_000 || wrapped {
			checked = map[string]int64{} // bound memory; a wrap restarts the walk anyway
		}
		// hourly: refresh the backlog snapshot (worker-side ONLY — the API serves this cache)
		if time.Since(lastJournal) > time.Hour {
			lastJournal = time.Now()
			if oRows, oConds, graded, unres, err := s.store.PTTBacklogStats(ctx); err == nil {
				s.pttBfMu.Lock()
				s.pttBfState.BacklogRows, s.pttBfState.BacklogConds = oRows, oConds
				s.pttBfState.GradedRows, s.pttBfState.UnresRows = graded, unres
				s.pttBfState.StatsAt = time.Now().UTC().Format(time.RFC3339)
				st := s.pttBfState
				s.pttBfMu.Unlock()
				_ = s.store.Audit(ctx, "info", "pttbackfill", fmt.Sprintf(
					"ptt backfill: lifetime graded %d conds · voided %d · deleted %d · still-open %d · pending %d · cycle %d — backlog now %d rows / %d conds (graded rows %d, unresolvable %d)",
					st.Graded, st.Voided, st.Deleted, st.Open, st.Pending, st.Cycles, oRows, oConds, graded, unres), "")
			}
		}
		// persist cursor + counters every pass
		s.pttBfMu.Lock()
		st := s.pttBfState
		s.pttBfMu.Unlock()
		if bts, err := json.Marshal(st); err == nil {
			_ = s.store.KVSet(ctx, pttBfCursorKV, string(bts))
		}
		pause := pttBfPassPause
		if wrapped {
			pause = pttBfCyclePause
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(pause):
		}
	}
}

// pttBackfillPass processes one row-page. Returns true when the walk wrapped (page empty).
func (s *Server) pttBackfillPass(ctx context.Context, checked map[string]int64) (wrapped bool) {
	now := time.Now()
	s.pttBfMu.Lock()
	curTS, curID := s.pttBfState.CursorTS, s.pttBfState.CursorID
	s.pttBfMu.Unlock()
	rawPage, lastTS, lastID, err := s.store.UnresolvedRowsPage(ctx, curTS, curID, now.Add(-pttBfMinAge).Unix(), pttBfRowLimit)
	if err != nil {
		s.log.Warn("ptt backfill: page query failed", "err", err)
		return false
	}
	if len(rawPage) == 0 { // walk complete — restart from the top next pass
		s.pttBfMu.Lock()
		s.pttBfState.CursorTS, s.pttBfState.CursorID = 0, 0
		s.pttBfState.Cycles++
		s.pttBfState.LastPass = now.UTC().Format(time.RFC3339)
		s.pttBfState.LastNote = "cycle complete"
		s.pttBfMu.Unlock()
		return true
	}
	// Skip conditions checked recently (open/pending conds recur across row pages) + cap the pass.
	// The row walk discovers each condition through an old representative row. Fetch the actual
	// per-condition MAX(ts) in bounded indexed batches before making any age decision.
	page := rawPage[:0:0]
	for _, p := range rawPage {
		if at, ok := checked[p.ConditionID]; ok && now.Unix()-at < 4*3600 {
			continue
		}
		page = append(page, p)
		if len(page) >= pttBfPageConds {
			break
		}
	}
	pageIDs := make([]string, 0, len(page))
	for _, p := range page {
		pageIDs = append(pageIDs, p.ConditionID)
	}
	newestTSByCond, err := s.store.NewestUnresolvedTradeTimes(ctx, pageIDs)
	if err != nil {
		s.log.Warn("ptt backfill: newest-trade query failed", "err", err)
		s.pttBfMu.Lock()
		s.pttBfState.Errors += int64(len(page))
		s.pttBfState.LastPass = now.UTC().Format(time.RFC3339)
		s.pttBfState.LastNote = "newest-trade query failed; page retained for retry"
		s.pttBfMu.Unlock()
		return false // retain the cursor: age is unknown, so no mutation is safe
	}
	eligible := page[:0:0]
	for _, p := range page {
		newest, stillOpen := newestTSByCond[p.ConditionID]
		if !stillOpen || newest >= now.Add(-pttBfMinAge).Unix() {
			continue // resolved concurrently, or fresh enough for the regular sweep
		}
		eligible = append(eligible, p)
		checked[p.ConditionID] = now.Unix()
	}
	page = eligible
	var graded, voided, deleted, open, pending, terrs int64
	clobBudget := pttBfCLOBBudget
	for i := 0; i < len(page); i += pttBfChunkSize {
		if ctx.Err() != nil {
			return false
		}
		end := i + pttBfChunkSize
		if end > len(page) {
			end = len(page)
		}
		cids := make([]string, 0, end-i)
		for _, p := range page[i:end] {
			cids = append(cids, p.ConditionID)
		}
		got, gammaErr := s.poly.MarketsByConditionsStrict(ctx, cids)
		if gammaErr != nil {
			// A failed Gamma chunk is unknown venue state, never market absence. In particular,
			// do not combine it with a CLOB 404 and convert live/recent rows to resolved=-2.
			terrs += int64(len(cids))
			for _, cid := range cids {
				delete(checked, cid) // allow the next walk to retry instead of suppressing for 4h
				_ = s.store.BumpConditionRetry(ctx, cid, now.Unix())
			}
			s.log.Warn("ptt backfill: gamma batch failed; chunk retained for retry", "conditions", len(cids), "err", gammaErr)
			continue
		}
		for _, cid := range cids {
			m, ok := got[cid]
			if !ok && clobBudget > 0 {
				clobBudget--
				var cerr error
				m, ok, cerr = s.poly.MarketByCLOBStrict(ctx, cid)
				time.Sleep(200 * time.Millisecond)
				if cerr != nil {
					if errors.Is(cerr, polymarket.ErrNotFound) {
						// definitive venue 404 + absent from gamma (incl. closed) — deleted. Require
						// the condition's NEWEST unresolved trade be ≥3d old. Storage atomically
						// rechecks that age so an ingest racing these venue calls cannot be retired.
						if now.Unix()-newestTSByCond[cid] > 3*86400 {
							n, markErr := s.store.MarkConditionUnresolvableVenueIfNewestBefore(ctx, cid, now.Unix()-3*86400)
							if markErr != nil {
								terrs++
								_ = s.store.BumpConditionRetry(ctx, cid, now.Unix())
							} else if n > 0 {
								deleted++
							} else {
								open++ // a fresh row arrived while venue checks were in flight
								_ = s.store.BumpConditionRetry(ctx, cid, now.Unix())
							}
							continue
						}
					}
					terrs++
					_ = s.store.BumpConditionRetry(ctx, cid, now.Unix())
					continue
				}
			}
			if !ok { // gamma-absent, CLOB budget exhausted — next cycle
				continue
			}
			if win, done := m.Resolved(); done {
				outs, prices := m.Outcomes(), m.Prices()
				winIdx := -1
				for oi, o := range outs {
					if strings.EqualFold(strings.TrimSpace(o), strings.TrimSpace(win)) {
						winIdx = oi
						break
					}
				}
				if err := s.store.ResolvePolyTraderTradesIdx(ctx, cid, win, winIdx, prices); err == nil {
					graded++
					// same venue truth grades any open poly-int SIGNAL rows keyed by this condition
					_ = s.store.ResolveSignalsByOutcome(ctx, cid, win)
				}
				continue
			}
			switch {
			case m.Closed && strings.EqualFold(strings.TrimSpace(m.UmaResolution), "resolved"):
				// venue says resolution finished but no decisive ≥0.99 outcome — VOIDED (50/50
				// refund class). Never graded as a loss; marked unresolvable-at-venue.
				if _, e := s.store.MarkConditionUnresolvableVenue(ctx, cid); e == nil {
					voided++
				}
			case m.Closed:
				pending++ // closed, resolution still in the UMA pipeline — retry later
				_ = s.store.BumpConditionRetry(ctx, cid, now.Unix())
			default:
				open++ // genuinely still trading — the sweep's territory
				_ = s.store.BumpConditionRetry(ctx, cid, now.Unix())
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(pttBfChunkPause):
		}
	}
	s.pttBfMu.Lock()
	s.pttBfState.CursorTS, s.pttBfState.CursorID = lastTS, lastID
	s.pttBfState.Graded += graded
	s.pttBfState.Voided += voided
	s.pttBfState.Deleted += deleted
	s.pttBfState.Open += open
	s.pttBfState.Pending += pending
	s.pttBfState.Errors += terrs
	s.pttBfState.LastPass = time.Now().UTC().Format(time.RFC3339)
	s.pttBfState.LastNote = fmt.Sprintf("page %d conds: graded %d · voided %d · deleted %d · open %d · pending %d · err %d",
		len(page), graded, voided, deleted, open, pending, terrs)
	s.pttBfMu.Unlock()
	if graded+voided+deleted > 0 {
		s.log.Info("ptt backfill pass", "page", len(page), "graded", graded, "voided", voided,
			"deleted", deleted, "open", open, "pending", pending, "errs", terrs)
	}
	return false
}

// handlePTTBackfill (GET /api/pttbackfill) — worker state incl. its hourly backlog snapshot.
// Serves ONLY the in-memory cache: the request path never touches the 4M-row table (the first
// cut did, and timed out under the worker's own write load — R125 same-day root-cause).
func (s *Server) handlePTTBackfill(w http.ResponseWriter, r *http.Request) {
	s.pttBfMu.Lock()
	st := s.pttBfState
	s.pttBfMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"worker": st,
		"note": "backlog_* fields are the worker's hourly snapshot (stats_at), not a live count"})
}
