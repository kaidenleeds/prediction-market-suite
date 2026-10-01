// sse.go — one Server-Sent-Events stream + the "all systems loaded" readiness endpoint (audit §4).
//
// Before: no server push existed — ~20 endpoints were polled on a 700ms loop through the browser's
// 6-socket HTTP/1.1 cap, and there was no single place that said whether the suite was actually
// warmed up. Now /api/events pushes {panel} the moment a snapshot cache refreshes or a mutating
// POST lands (instant, cross-window via the page's BroadcastChannel relay), and /api/ready
// aggregates the signals that already existed in memory into per-component freshness dots.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

type sseHub struct {
	mu   sync.Mutex
	subs map[chan string]bool
}

// sseNotify pushes a panel-refresh event to every connected dashboard (non-blocking; a slow
// consumer just misses the hint and catches up on its slow backstop poll).
func (s *Server) sseNotify(panel string) {
	s.sseMu.Lock()
	for ch := range s.sseSubs {
		select {
		case ch <- panel:
		default:
		}
	}
	s.sseMu.Unlock()
}

// handleEvents (GET /api/events) is the SSE stream: `data: <panel>` per refresh + a 25s heartbeat.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "streaming unsupported"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "keep-alive")
	ch := make(chan string, 32)
	s.sseMu.Lock()
	if s.sseSubs == nil {
		s.sseSubs = map[chan string]bool{}
	}
	s.sseSubs[ch] = true
	s.sseMu.Unlock()
	defer func() {
		s.sseMu.Lock()
		delete(s.sseSubs, ch)
		s.sseMu.Unlock()
	}()
	_, _ = fmt.Fprintf(w, "data: connected\n\n")
	fl.Flush()
	hb := time.NewTicker(25 * time.Second)
	defer hb.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case p := <-ch:
			_, _ = fmt.Fprintf(w, "data: %s\n\n", p)
			fl.Flush()
		case <-hb.C:
			_, _ = fmt.Fprintf(w, ": ping\n\n")
			fl.Flush()
		}
	}
}

// handleNetworth (GET /api/networth) is the ONE-PORTFOLIO header line (operator directive:
// "combine auto with ML, one single portfolio"): realized net across the paper book (all sources —
// auto-ml, arb, gate, manual), the parlay/combo book (artifacts excluded), and settled live RFQ
// combos. Cheap: the per-venue realized cache + two small SQL sums, snapshot-cached 20s.
func (s *Server) handleNetworth(w http.ResponseWriter, r *http.Request) {
	s.serveSnap(w, r, &s.snapNetworth, 20*time.Second, "networth", s.buildNetworth)
}

func (s *Server) buildNetworth(ctx context.Context) []byte {
	// R106 (auditor bug 263): one CONSISTENT per-venue snapshot — the old three independent
	// venueRealizedNet calls could straddle the cache expiry and compose a paper_net out of
	// mixed-vintage components (−25.12 vs −17.01 3s apart, r31). Path + age ride the payload.
	vr, vrSrc, vrAge := s.venueRealizedNetAll()
	paperNet := vr["kalshi"] + vr["polymarket"] + vr["polyus"]
	// R107 (auditor bug 289): the networth headline and NAV had silently DIFFERENT scopes —
	// paper_net here includes the research venue (polymarket) but excludes the ML/RawFlow/
	// Weather book nets, while refreshTotalEquity's NAV includes those books. The scopes are
	// now EXPLICIT in the payload: books_net carries the allocated-book epoch nets so consumers
	// can compose either view; the headline paper_net semantics are unchanged (display-stable).
	booksNet := s.rawFlowNetSinceEpoch() + s.weatherNetSinceEpoch()
	parlayNet, _ := s.store.ParlaysRealized(ctx)
	// R146: New-ML Combo Paper owns a separate reset epoch and ledger route.  Keep its
	// realized result explicit instead of folding it into parlay_net (System Combo truth).
	mlComboNet := s.mlComboPaperStatus(ctx).RealizedNet
	liveNet, _ := s.store.LiveCombosRealized(ctx)
	// R44/R46b (operator: "still broken — dbl check"): venueRealizedNet ALREADY subtracts the
	// Reset-P&L baseline internally (RESETSIZE), so paperNet is since-reset AS RETURNED. The first
	// version subtracted eqBaseline AGAIN here — a double-subtraction that turned a ~$0 post-reset
	// book into −$1,226 (the baselines sum). Only the parlay/live-combo bases belong here, because
	// ParlaysRealized/LiveCombosRealized are raw all-time SQL sums.
	s.pnlMu.Lock()
	parlayNet -= s.parlayNetBase
	liveNet -= s.liveNetBase
	s.pnlMu.Unlock()
	// R44 "live pnl indicator": last computed live unrealized P&L (both venues) + its age — kept
	// fresh whenever the Live tab/loop builds; stale age is shown honestly instead of hidden.
	s.liveMu.Lock()
	livePnL, livePnLK, livePnLP, livePnLAt := s.lastLivePnL, s.lastLivePnLKalshi, s.lastLivePnLPolyUS, s.lastLivePnLAt
	livePnLOKK, livePnLOKP := s.lastLivePnLOKK, s.lastLivePnLOKP
	hist := make([]livePnlPt, len(s.livePnlHist)) // R60: copy the ring under the lock for the live-pnl widget
	copy(hist, s.livePnlHist)
	s.liveMu.Unlock()
	out := map[string]any{
		"live_pnl_hist": hist, // R60: ~15s live unrealized P&L samples (cap 1200) — the live-pnl widget sparkline
		"paper_net":     paperNet,
		"parlay_net":    parlayNet,
		"ml_combo_net":  mlComboNet,
		"live_net":      liveNet,
		"total_net":     paperNet + parlayNet + mlComboNet + liveNet,
		// R106 (bug 263): which internal path composed paper_net + how old that snapshot is —
		// the auditor's "log which aggregate each path composes" ask. cache|fresh are the two
		// healthy paths; *-error = the R84 fail-soft branches.
		"paper_net_src":   vrSrc,
		"paper_net_age_s": math.Round(vrAge.Seconds()),
		// R107 (bug 289): the allocated books' epoch nets (RawFlow+Weather; ML rides its own
		// file) — NAV includes these, paper_net does NOT; scope now explicit, headline unchanged.
		"books_net": math.Round(booksNet*100) / 100,
		"scope":     "paper_net = paper book incl research venue, EXCLUDING allocated book files; NAV (portfolio header) additionally includes books_net + the ML book — bug 289 scopes made explicit",
		"note":      "mixed simulation/account display since Reset P&L: Paper systems and Paper combos are modeled, while authenticated LIVE results must be read from the separate LIVE ledger; never treat this total as exchange profit",
	}
	if !livePnLAt.IsZero() {
		out["live_pnl"] = math.Round(livePnL*100) / 100
		out["live_pnl_kalshi"] = math.Round(livePnLK*100) / 100
		out["live_pnl_polyus"] = math.Round(livePnLP*100) / 100
		out["live_pnl_kalshi_complete"] = livePnLOKK
		out["live_pnl_polyus_complete"] = livePnLOKP
		out["live_pnl_age_s"] = math.Round(time.Since(livePnLAt).Seconds())
	}
	b, _ := json.Marshal(out)
	return b
}

// handleReady (GET /api/ready) aggregates readiness/freshness from signals that ALREADY live in
// memory (audit §4: "nearly all inputs already exist; low-effort/high-value"): feed warm-up, the
// Kalshi ticker WS, the Poly RTDS trade tape, the ML sidecar's predictions file age, DB liveness,
// and per-tab snapshot ages. ready=true ⇔ every core component is green.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	comp := map[string]any{}
	ok := true
	polyUSRequired, polyUSStrictOK, polyUSTransportOK := s.livePolyUSDestinationEnabled(), true, true
	polyUSReadyDetail := "not configured"
	s.liveMu.Lock()
	liveStrictBooks := s.liveArmed || s.liveAuto
	s.liveMu.Unlock()
	add := func(name string, good bool, detail string) {
		comp[name] = map[string]any{"ok": good, "detail": detail}
		if !good {
			ok = false
		}
	}
	add("feeds", s.feedsReady(), "signal feeds warmed up")
	// Diagnostic attribution only: it can explain venue-feed trouble but never changes ready,
	// Paper/LIVE authority, sizing, or routing.
	if network, sampled := s.latmon.networkPathSnapshot(); sampled {
		comp["network_path"] = map[string]any{"ok": network.State == "healthy", "state": network.State,
			"detail": networkPathDetail(network, now), "blocking": false, "trade_authorizing": false,
			"sample_age_s": math.Max(0, math.Round(now.Sub(network.At).Seconds()))}
	} else {
		comp["network_path"] = map[string]any{"ok": true, "state": "warming",
			"detail": "first bounded network-path sample pending", "blocking": false, "trade_authorizing": false}
	}
	add("kalshi_ws", s.kal.TickerCount() > 0, fmt.Sprintf("%d live tickers", s.kal.TickerCount()))
	kalshiFeesOK, kalshiFeesDetail := s.kalFeeRegistryReadiness(now)
	add("kalshi_fees", kalshiFeesOK, kalshiFeesDetail)
	// R143: feeds can be green while a native Paper ledger is poison-guarded and every placement
	// path is effectively frozen.  Make that state a first-class readiness failure instead of an
	// obscure boot-log line. Missing files are valid fresh ledgers; malformed existing files are not.
	{
		ledgers := s.paperLedgerIntegrityView()
		detail := fmt.Sprintf("%d healthy · %d fresh/missing", ledgers.Healthy, ledgers.Missing)
		if len(ledgers.Broken) > 0 {
			detail += " · " + strings.Join(ledgers.Broken, "; ")
		}
		add("paper_ledgers", ledgers.OK, detail)
	}
	// R86 "kalshi_auth" (operator's PC crash killed the shell exporting KALSHI_SUITE_PASSPHRASE →
	// the suite rebooted public-only with only a quiet log warn): green = signer loaded; RED
	// (ok=false, holds ready=false) = credentials stored but LOCKED — the detail carries the exact
	// fix; grey/neutral (state "none", ok stays true) = nothing stored, a valid unauthenticated
	// state that must not read as "warming". The extra "state" field drives the dashboard's grey
	// dot + the persistent 🔑 AUTH LOCKED chip in bar1.
	{
		st, det := s.authState, s.authDetail
		if st == "" {
			st, det = "none", "no credentials stored"
		}
		comp["kalshi_auth"] = map[string]any{"ok": st != "locked", "detail": det, "state": st}
		if st == "locked" {
			ok = false
		}
	}
	// R87 "polyus_auth" mirrors kalshi_auth for the other live venue: green = the Ed25519 client
	// built (detail names the source: file/env); RED (ok=false, holds ready=false) = credentials
	// are CONFIGURED but unusable (bad secret, or a secret file present without POLY_US_KEY_ID) —
	// the detail carries the exact fix; grey "none" = nothing configured, a valid public-data-only
	// state. Same generic dashboard dot renderer as kalshi_auth.
	{
		st, det := s.pusAuthState, s.pusAuthDetail
		if st == "" {
			st, det = "none", "no PolyUS credentials configured"
		}
		comp["polyus_auth"] = map[string]any{"ok": st != "error", "detail": det, "state": st}
		if st == "error" {
			ok = false
		}
	}
	// R119 (R118 watch item): readiness now measures the SAME scope as the R108 watchdog — the WS
	// client's all-prints LiveTradesAge — not the cash≥100 whale-filtered newest trade. Big-print
	// lulls made the old check flap ready=false for 16 min while the socket was demonstrably fine
	// (ordinary prints flowing, watchdog correctly silent). Whale age stays in the detail string.
	// R133: authenticated PolyUS is not ready merely because its socket exists. A cold failed
	// full crawl once left 8 dependency slugs subscribed, zero strict-open proofs, and still let
	// /api/ready return true. Require the actual tiered book plan + recent lifecycle/data receipts.
	{
		configured := s.polyUSWS != nil || s.polyUSAuth != nil || strings.EqualFold(s.pusAuthState, "ok")
		if !configured {
			comp["polyus_books"] = map[string]any{"ok": true,
				"detail": "not configured; no authenticated PolyUS venue requested", "state": "none"}
		} else if s.polyUSWS == nil {
			polyUSStrictOK, polyUSTransportOK = false, false
			polyUSReadyDetail = "authenticated PolyUS configured but markets websocket is unavailable"
			comp["polyus_books"] = map[string]any{"ok": false,
				"detail": "authenticated PolyUS configured but markets websocket is unavailable",
				"state":  "blocked", "blocking": polyUSRequired}
			if polyUSRequired {
				ok = false
			}
		} else {
			requested, frames, _ := s.polyUSWS.SubPlan()
			full, lite, trade := s.polyUSWS.SubCoverage()
			proofs, proofAt := s.polyUSWS.OpenSlugStats()
			proofAge := time.Duration(-1)
			if !proofAt.IsZero() {
				proofAge = now.Sub(proofAt)
			}
			dataAge, primaryAge, haveData, havePrimary := s.polyUSWS.PrimaryDataAge()
			transportAge, haveTransport := s.polyUSWS.PrimaryFrameAge()
			plan := s.depthBookPlanView("polyus")
			classification := polyUSBooksClassify(polyUSBooksReadyInput{
				Proofs: proofs, Requested: requested, Frames: frames,
				Full: full, Lite: lite, Trade: trade, FullCap: s.polyUSWS.FullBookCap(),
				Fresh: s.polyUSWS.Count(), Executable: s.polyUSWS.ExecutableCount(),
				PriorityShortfall: plan.RequiredShortfall,
				ProofAge:          proofAge, DataAge: dataAge, TransportAge: transportAge, PrimaryAge: primaryAge,
				HavePrimary: havePrimary, HaveData: haveData, HaveTransport: haveTransport,
				MaxProofAge:     polymarketus.MarketsRESTLifecycleMaxAge,
				MaxTransportAge: polymarketus.MarketsWSPrimaryTransportMaxAge,
			}, !liveStrictBooks)
			polyUSStrictOK = classification.TransportCoverageOK && classification.ExecutableOK
			polyUSTransportOK, polyUSReadyDetail = classification.TransportCoverageOK, classification.Detail
			comp["polyus_books"] = map[string]any{"ok": classification.OK,
				"detail": classification.Detail, "state": classification.State,
				"transport_coverage_ok": classification.TransportCoverageOK,
				"executable_quote_ok":   classification.ExecutableOK,
				"prearm_ok":             polyUSStrictOK}
			if !classification.OK && polyUSRequired {
				ok = false
			}
		}
	}
	s.polyMu.Lock()
	whaleAge := time.Duration(0)
	if s.pWhaleNewestTS > 0 {
		whaleAge = now.Sub(time.Unix(s.pWhaleNewestTS, 0))
	}
	s.polyMu.Unlock()
	if s.poly != nil {
		liveAgeS := s.poly.LiveTradesAge()
		// Poly-int is research-only: surface its tape health, but never let it block readiness for
		// the two authenticated execution venues (same nonblocking rule as poly_clob_ws below).
		comp["poly_tape"] = map[string]any{"ok": liveAgeS >= 0 && liveAgeS < 300,
			"detail": fmt.Sprintf("live print %ds ago · newest big print %s ago (research venue)", int(liveAgeS), whaleAge.Truncate(time.Second))}
		connected, books, fresh, frameAge := s.poly.CLOBStats()
		lastErr, lastErrAt, reconnects := s.poly.CLOBConnectionReceipt()
		lastErrAge := time.Duration(-1)
		if !lastErrAt.IsZero() {
			lastErrAge = now.Sub(lastErrAt)
		}
		clobHealth := polyIntCLOBClassify(connected, fresh, frameAge, lastErr, lastErrAge)
		transport := fmt.Sprintf("reconnects=%d", reconnects)
		if lastErr != "" {
			if len(lastErr) > 180 {
				lastErr = lastErr[:180]
			}
			transport += fmt.Sprintf(" last_error=%q error_age=%.0fs", lastErr, now.Sub(lastErrAt).Seconds())
		}
		plan := s.pintCLOBPlanView()
		catalog := s.poly.CompleteCatalogSnapshot() // live crawl progress; plan refreshes only on socket rotation
		// Poly-int is research-only, so this receipt is visible but does not block live-venue
		// readiness. Consumers themselves fail closed when this cache is not fresh.
		comp["poly_clob_ws"] = map[string]any{"ok": clobHealth.OK, "state": clobHealth.State,
			"executable_quote_ok": clobHealth.ExecutableOK,
			"detail":              fmt.Sprintf("%s | connected=%v token_books=%d fresh=%d frame_age=%.0fs %s selected=%d/%d conditions assets=%d matched=%d/%d shortfall=%d catalog_complete=%v rows=%d age=%.0fs refreshing=%v checkpoint=%d/%d limit=%d source=%s (research-only; never live-authorizing)", clobHealth.Label, connected, books, fresh, frameAge, transport, plan.Selected, plan.Cap, plan.RequestedAssets, plan.MatchedSelected, plan.Matched, plan.RequiredShortfall, catalog.Complete, len(catalog.Markets), pintSnapshotAgeSec(now, catalog.At), catalog.Refreshing, catalog.CheckpointRows, catalog.CheckpointPage, catalog.CheckpointLimit, plan.PlanSource)}
	} else {
		comp["poly_tape"] = map[string]any{"ok": false, "detail": "poly WS client not configured (research venue)"}
		comp["poly_clob_ws"] = map[string]any{"ok": false, "detail": "poly CLOB client not configured (research venue)"}
	}
	if fi, err := os.Stat(filepath.Join(s.cfg().DataDir, "ml_predictions.json")); err == nil {
		age := now.Sub(fi.ModTime())
		var meta struct {
			Status        string `json:"model_status"`
			Cohort        string `json:"model_cohort"`
			Version       int    `json:"book_feature_version"`
			Resolved      int    `json:"book_v1_resolved"`
			Open          int    `json:"book_v1_open"`
			MinTrain      int    `json:"min_train_required"`
			Actionable    int    `json:"actionable_predictions"`
			LiveAuthority bool   `json:"live_authority"`
		}
		_ = s.readJSONLoose(filepath.Join(s.cfg().DataDir, "ml_predictions.json"), &meta)
		detail := fmt.Sprintf("%s cohort=%s v=%d resolved=%d/%d open=%d actionable=%d live_authority=%v receipt=%s old",
			meta.Status, meta.Cohort, meta.Version, meta.Resolved, meta.MinTrain, meta.Open,
			meta.Actionable, meta.LiveAuthority, age.Truncate(time.Second))
		add("ml_sidecar", age < 30*time.Minute && meta.Cohort == mlBookFeatureSchema && !meta.LiveAuthority, detail)
	} else {
		add("ml_sidecar", false, "no predictions file yet")
	}
	{ // R123: Go-side tick re-scorer — RED only on parity refusal or model-drift alarm
		geOK, geDetail := s.mlEvalReady()
		add("go_eval", geOK, geDetail)
	}
	{
		ctx := r.Context()
		_, dbErr := s.store.LastEpochTS(ctx) // any cheap query proves the DB answers
		add("db", dbErr == nil, "sqlite answering")
	}
	// R76 "prearm" (auditor DO-THIS 6e): the single dot that must be GREEN before any live re-arm —
	// order-dedup latches loaded (bug 6 fail-closed state) AND the orderbook WS actually proving
	// books (fresh>0; two starvation incidents were UI-invisible). The cfg race (bug 18) is fixed
	// structurally (copy-on-write), so it no longer needs a runtime check.
	{
		subs, fresh, gaps, bwErr := s.kal.BookStats()
		kalshiBookOK := fresh > 0
		strictOK := s.latchesOK.Load() && kalshiBookOK && kalshiFeesOK && (!polyUSRequired || polyUSStrictOK)
		detail := fmt.Sprintf("latches_loaded=%v kalshi_book_ws subs=%d fresh=%d gaps=%d err=%q | kalshi_fee_registry=%v (%s) | polyus_required=%v polyus_exact_quote_ok=%v (%s)",
			s.latchesOK.Load(), subs, fresh, gaps, bwErr, kalshiFeesOK, kalshiFeesDetail, polyUSRequired, polyUSStrictOK, polyUSReadyDetail)
		if !strictOK && !liveStrictBooks && s.latchesOK.Load() && kalshiBookOK &&
			polyUSRequired && polyUSTransportOK && !polyUSStrictOK {
			comp["prearm"] = map[string]any{"ok": false, "state": "warming", "strict_ok": false,
				"arm_ready": false, "detail": detail + " | transport is healthy, but ARM still requires an executable PolyUS quote"}
			ok = false
		} else {
			add("prearm", strictOK, detail)
		}
	}
	// Conditional strategies are allowed to find zero candidates. Each required execution venue
	// publishes a completion receipt only after every attempted insert returns. A throttle-start
	// timestamp, an old signal row, or a different venue's healthy scan cannot mask a failed logger.
	requiredSignalProducers := []string{"kalshi"}
	if polyUSRequired {
		requiredSignalProducers = append(requiredSignalProducers, "polyus")
	}
	requiredSignalProducers = s.requiredLiveSignalProducers(requiredSignalProducers)
	producerReceipts, producerNow := s.signalProducerSnapshotAt()
	producerOK, producerDetail := signalProducersClassify(producerNow, 3*time.Minute,
		requiredSignalProducers, producerReceipts)
	s.polyMu.Lock()
	polyScanAt := s.polyRowsAt
	s.polyMu.Unlock()
	polyScanAge := now.Sub(polyScanAt)
	producerAge := func(at time.Time, age time.Duration) string {
		if at.IsZero() {
			return "warming (first completed scan pending)"
		}
		return age.Truncate(time.Second).String() + " ago"
	}
	add("signal_producers", producerOK, producerDetail)
	// Polymarket Global consensus is research-only, like its tape and CLOB components above. Keep
	// the heartbeat visible without reintroducing a non-execution venue as a live-readiness gate.
	comp["poly_consensus_research"] = map[string]any{"ok": !polyScanAt.IsZero() && polyScanAge <= 3*time.Minute,
		"detail": "research-only scan=" + producerAge(polyScanAt, polyScanAge) + "; never live-authorizing"}
	// R76 famine sentinel (auditor nag, 8th run): per-family MAX(ts) vs expected cadence. The 7h
	// wedge wrote ZERO rows while every dot stayed green — this is the dot that would have turned
	// red when its producer heartbeat stopped. Clock-graded families use their explicit measured
	// cadence; conditional families use the completed-scan heartbeat above. Cached ~2min.
	{
		// R90 DO-THIS 12 (famine CADENCE TABLE): per-family expected cadence replaces the
		// one-size 2h bar. Conditional pmatch opportunities have measured 32h natural gaps, so
		// the event row itself is not a liveness signal; the authenticated cross-match producer
		// heartbeat above proves its scan even when no valid pair exists. sharpline is graded ONLY while a key
		// is present (keyless is a supported state — R90 DO-THIS 3); wxedge stays ungraded
		// until its bug-120 instrumentation proves the pipeline's real cadence. Every graded
		// family's age rides the detail so cadence truth is readable from /api/ready.
		// R98: the table is built FIRST and its family names go INTO the store query —
		// SignalFamine now does one covering index seek per requested family instead of
		// walking the whole 24h window (the "query failed: context deadline exceeded" fix).
		famineCadence := []signalFamilyCadence{
			// Conditional detection rows are never required to appear on a clock. Their producer
			// heartbeat is the liveness receipt; otherwise a quiet market becomes a false outage.
		}
		if strings.TrimSpace(s.cfg().Auto.OddsAPIKey) != "" && s.cfg().Auto.SharplineEnabled {
			famineCadence = append(famineCadence,
				signalFamilyCadence{"sharpline", 6 * time.Hour}) // 30-min sweeps while keyed; ungraded while keyless
		}
		s.famineMu.Lock()
		if len(famineCadence) == 0 {
			// With no clock-graded family configured, the producer heartbeat above is the whole
			// liveness contract. Do not let an old global signal row or stale cached query error
			// turn a correct zero-opportunity scan red.
			s.famineNewest, s.famineByFam, s.famineErr = "", nil, ""
			s.famineAt = now
		} else if time.Since(s.famineAt) > 2*time.Minute {
			fams := make([]string, 0, len(famineCadence))
			for _, fc := range famineCadence {
				fams = append(fams, fc.fam)
			}
			nctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			if newest, byFam, err := s.store.SignalFamine(nctx, fams); err == nil {
				s.famineNewest, s.famineByFam, s.famineErr = newest, byFam, ""
			} else {
				s.famineErr = err.Error()
			}
			cancel()
			s.famineAt = time.Now()
		}
		newest, byFam, ferr := s.famineNewest, s.famineByFam, s.famineErr
		s.famineMu.Unlock()
		famOK, detail := signalFamineClassify(now, producerOK, producerDetail,
			newest, byFam, ferr, famineCadence)
		add("signal_famine", famOK, detail)
	}
	nv := namingVitalityNow()
	// This component is a warning dot, not a readiness blocker. Give the unique-ticker
	// denominator ten minutes to warm before applying the 10% fallback vitality threshold.
	namingOK := time.Since(s.bootAt) < 10*time.Minute || nv.Total < 100 || nv.FallbackPct <= 10
	comp["name_fallbacks"] = map[string]any{"ok": namingOK, "detail": fmt.Sprintf(
		"%d/%d mapped · %d raw (%.1f%%) · top raw prefix %s (%d) · boot age %s",
		nv.Mapped, nv.Total, nv.Fallback, nv.FallbackPct, nv.TopPrefix, nv.TopN, time.Since(s.bootAt).Truncate(time.Second))}
	s.persistNamingVitality(nv)
	// per-tab freshness (dots, not blockers): age of each cached snapshot
	tabs := map[string]any{}
	stamp := func(name string, at time.Time) {
		if at.IsZero() {
			tabs[name] = nil
			return
		}
		tabs[name] = now.Sub(at).Truncate(time.Second).String()
	}
	s.mktMu.Lock()
	stamp("markets", s.mktAt)
	s.mktMu.Unlock()
	s.whaleMu.Lock()
	stamp("whales", s.whaleAt)
	s.whaleMu.Unlock()
	s.arbMu.Lock()
	stamp("arb", s.arbAt)
	s.arbMu.Unlock()
	stamp("ml", s.snapML.ageAt())
	stamp("portfolio", s.snapPaper.ageAt())
	s.liveSnapMu.Lock()
	stamp("live", s.liveSnapAt)
	s.liveSnapMu.Unlock()
	b, _ := json.Marshal(map[string]any{"ready": ok, "components": comp, "tab_ages": tabs, "ts": now.UTC().Format(time.RFC3339)})
	writeCachedJSON(w, r, b)
}
