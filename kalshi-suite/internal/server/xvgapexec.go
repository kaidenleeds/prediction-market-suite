package server

// R122 XVGAP BOOK (xvgapexec.go) — the xvgap POCKET executor, auditor r56 DO-THIS 10 / EV #1
// ("spec-final, blocked on code only" since r48). Wired for the auto-promotion pipeline
// (promotion.go): family "xvgap" is PROVEN+ (+3.94¢/ct n=5,554, CS floor +1.49 at build time),
// and with this executor registered, promoDecide funds a $100 paper book from the reserve on its
// own — the pipeline working as designed. PAPER ONLY; live arming stays operator-only forever.
//
// THE POCKET (auditor r49/r50, measured): xvgap is a venue+band pocket, not a whole-family bet —
//   - kalshi-side rows are CI-NEGATIVE (−4.10 [−7.11,−1.08] n=758) → NEVER traded here;
//   - the pocket = POLYUS-only entries at 20–50¢: +12.40¢/ct [+10.34,+14.45] n=2,244 (r50
//     fill-preferred re-pin +13.88 [+11.80,+15.96]) — each 10¢ sub-band independently CI-positive
//     (+7.2/+17.9/+9.6), so it is structure, not band-dredging. All-YES family (369-clean).
//
// EXECUTION (R126 Part 4.1 — this IS the execution policy, explicit and deliberate): PUS gap
// candidates execute IMMEDIATELY as TAKER at the live PolyUS ask — the R93/R102 gap study shows
// PolyUS closes most of a gap inside 1–2 minutes, so waiting or resting a post systematically
// misses the edge (the queue-aware router agrees: PUS exposes no ladder ⇒ taker:no-queue-data).
// The WAIT-VS-NOW question exists ONLY for Kalshi-side candidates and runs LOG-ONLY in xvboth.go
// (xvWaitNote) until its parameters are measured from reality. This taker-immediate rule is a
// documented, deliberate deviation from the R107 "every book posts maker-first" rule; every lot
// carries the router tag so the maker/taker grader sees it. Sizing: shared Kelly engine on the
// book's own equity, family key "auto-cons-xvgap", kedge realized-only (no ML, ever — the rawflow
// rule). Exit: ride to settlement (5 exit studies), frozen ratio stop only if the global mode says
// so. Settlement authority: signal_log resolution (the xvgap family's own rows resolve on polyus
// via the standard resolver), marks via fiSideMark (platform-aware, zero-API caches).
//
// R126 Part 2 — EPISODE RE-ENTRY replaces the old one-lot-per-market rule: a gap that closed
// (≤1¢) and REOPENED ≥ threshold is a NEW episode (xvboth.go tracker) and may be entered again —
// SAME direction only (an opposite-side open lot refuses: self-hedge guard), one lot per episode,
// ≤ xvgap_max_episodes lots per market (default 3). Gate = xvgEpisodeGate, pinned in r126_test.go.

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"encoding/json"
)

const (
	xvgEntryLo = 0.20 // the measured pocket band (fill-preferred pinned, r50)
	xvgEntryHi = 0.50
)

// xvgEntryOK — pure pocket screen (pinned in r122_test.go): polyus platform, YES side (the
// family logs the cheap venue's YES), price inside the 20–50¢ pocket, sane spread (≤10¢ — the
// freshinv screen, same rationale).
func xvgEntryOK(platform string, yesPx, spreadC float64) bool {
	if platform != "polyus" {
		return false // the kalshi cells are CI-negative — never traded (see header)
	}
	if yesPx < xvgEntryLo || yesPx > xvgEntryHi {
		return false
	}
	if spreadC > 10 {
		return false
	}
	return true
}

// xvgLiveBaseballBlocked is the R132 measured category gate. The full venue schema supplies both
// league and live state. Proven in-play baseball is blocked; live rows whose league is missing are
// also blocked because the suite cannot prove they are outside that negative cell. Signals
// continue logging so the cell can earn its way back, but the executor fails closed when the
// category derivation needed by a money rule is unknown (487-decision replay: in-play baseball
// -8.90% ROI, 223 decisions / 49 games).
func xvgLiveBaseballBlocked(m polyUSMarket) bool {
	if !m.Live {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(m.League)) {
	case "", "mlb", "cws", "baseball":
		return true
	default:
		return false
	}
}

func (s *Server) xvgPlacementCategoryOK(slug string) bool {
	for _, m := range s.polyUSSnapshot() {
		if m.Slug == slug {
			return !xvgLiveBaseballBlocked(m)
		}
	}
	return false // missing venue metadata cannot prove lifecycle/category eligibility
}

// xvgTwinFromTitle extracts the Kalshi twin ticker the detector embeds in the signal Title
// after "⇄" (xvGapCandidates: Title = game + " ⇄ " + twin on polyus-cheap rows).
func xvgTwinFromTitle(title string) string {
	i := strings.LastIndex(title, "⇄")
	if i < 0 {
		return ""
	}
	return strings.TrimSpace(title[i+len("⇄"):])
}

// xvgRulesEquiv — R124 C1b: the venue settlement-rules EQUIVALENCE gate on the EXECUTOR path.
// Before a cross-venue gap is treated as tradeable, the twin must re-derive through the
// STRUCTURAL game-identity join (gameident.go) — the deterministic venue-metadata match that
// enforces same market type, same team/line, same-side economics, and the R106 advance-vs-
// regulation settlement semantics. Regex-fuzz-matched candidates (giNoteFuzz path) keep LOGGING
// for research but are REFUSED here: a fuzz match carries no rules-equivalence evidence. The
// Kalshi twin must also be a live, unsettled market on fresh meta (the bug-462 closed-market
// class can otherwise serve a stale twin price). Refuse-only by construction — this gate can
// never ADD a trade. Pinned in r124_test.go.
func (s *Server) xvgRulesEquiv(slug, twin string) (bool, string) {
	if twin == "" {
		return false, "no-twin-in-signal"
	}
	st, sameSide, ok := s.giKalshiTwin(slug)
	if !ok {
		return false, "no-struct-twin" // fuzz-only match — no settlement-rules evidence
	}
	if !sameSide {
		return false, "inverted-twin" // opposite-side economics must never gap-trade as same-side
	}
	if st != twin {
		return false, "twin-drift" // the signal's twin no longer re-derives — stale identity
	}
	s.metaMu.Lock()
	km, okM := s.kmkts[twin]
	at, okA := s.kmktsAt[twin]
	s.metaMu.Unlock()
	if !okM || !okA || time.Since(at) > 2*time.Minute {
		return false, "twin-meta-stale" // same freshness rule as the detector's pricing path
	}
	if km.Result != "" {
		return false, "twin-settled"
	}
	if status := strings.ToLower(km.Status); status != "" && status != "active" {
		return false, "twin-not-active:" + status
	}
	return true, ""
}

// xvgActive — R165 hard retirement for NEW Paper lots. Persisted WATCH/PROMOTED/GROWN labels came
// from assumed-fill history and cannot activate an executor. Detection keeps logging and
// xvgWindingDown continues to settle existing lots.
func (s *Server) xvgActive() bool {
	return false
}

// xvgWindingDown — retired/paused with lots still open or an un-rebased epoch net (268 doctrine).
func (s *Server) xvgWindingDown() bool {
	s.xvgBookMu.Lock()
	defer s.xvgBookMu.Unlock()
	b := s.xvgLoadLocked()
	return len(b.Open) > 0 || math.Abs(b.Net-b.NetBase) > 0.005
}

// xvgLoadLocked lazy-loads xvgap_book.json. Caller holds xvgBookMu. Bank seeds at 0 — the
// promotion pipeline sets it (xvgSetBank). Bug-52 poison guard, same as every book.
func (s *Server) xvgLoadLocked() *kfBook {
	if s.xvgBook != nil {
		return s.xvgBook
	}
	b := &kfBook{}
	path := filepath.Join(s.cfg().DataDir, "xvgap_book.json")
	if !s.readJSONLoose(path, b) {
		if st, err := os.Stat(path); err == nil && st.Size() > 0 {
			s.xvgBookPoisoned.Store(true)
			s.log.Error("xvgap_book.json exists but failed to parse — XvGap flushes DISABLED this session (bug-52 guard)", "path", path, "size", st.Size())
			_ = s.store.Audit(context.Background(), "error", "xvgap", "XvGap book file unreadable — flushes disabled this session; inspect data\\xvgap_book.json", "")
		}
	}
	s.xvgBook = b
	return b
}

// xvgSetBank — the promotion pipeline's ONLY money hook for this book.
func (s *Server) xvgSetBank(v float64) {
	s.xvgBookMu.Lock()
	b := s.xvgLoadLocked()
	b.Bank = math.Round(v*100) / 100
	s.xvgBookDirty = true
	s.xvgBookMu.Unlock()
	s.xvgFlush()
}

// xvgBookBankNet — (bank, net since epoch, settled lifetime rows, open lots) for the promotion sweep.
func (s *Server) xvgBookBankNet() (bank, netSince float64, settledN, openN int) {
	s.xvgBookMu.Lock()
	defer s.xvgBookMu.Unlock()
	b := s.xvgLoadLocked()
	return b.Bank, b.Net - b.NetBase, b.Wins + b.Losses, len(b.Open)
}

// xvgPlace routes one just-logged POLYUS xvgap signal into the book (called from xvGapSweep right
// after the signal row lands — detection and placement stay decoupled: this self-gates on the
// promotion state + kill switch; logging upstream is unconditional).
func (s *Server) xvgPlace(ctx context.Context, slug, title string, yesPx, spreadC, resolveHours float64, epN int) {
	if s.xvgBookPoisoned.Load() || !s.xvgActive() || s.ksBlocked() {
		return
	}
	if !s.paperEntryHorizonOK(resolveHours, slug, title) {
		return
	}
	// R128 timeout-inventory fix: fail closed on provably-dead PolyUS feeds (the venue books'
	// R126 gate; this path only had the 2-min twin-meta freshness + 3¢ chase guard).
	if s.pxOfflineReason("polyus", slug) != "" {
		return
	}
	if !xvgEntryOK("polyus", yesPx, spreadC) {
		return
	}
	if !s.xvgPlacementCategoryOK(slug) {
		_ = s.store.Audit(ctx, "info", "xvgap",
			fmt.Sprintf("XvGap REFUSED %s: in-play baseball category gate (R132 measured negative); signal still logged", slug), "")
		return
	}
	// R124 C1b: settlement-rules equivalence gate (refuse-only — see xvgRulesEquiv). Every
	// refusal is journaled: the sweep throttle (30min/pair, ≤30 rows) bounds the audit volume.
	if ok, why := s.xvgRulesEquiv(slug, xvgTwinFromTitle(title)); !ok {
		_ = s.store.Audit(ctx, "info", "xvgap",
			fmt.Sprintf("XvGap REFUSED %s: settlement-rules equivalence gate (%s) — logged, not traded", slug, why), "")
		return
	}
	side := "YES" // the family's side by construction (cheap venue's YES)
	if r := s.betConflictReason("polyus", slug, side, "auto-cons-xvgap", nil); r != "" {
		return
	}
	maxEp := s.cfg().Auto.XvgapMaxEpisodes
	// R127 FOUR-BOOK GATE: xvgap is a promotion SUB-STRATEGY of the POLYUS venue book. Both rails
	// computed OUTSIDE xvgBookMu (bookAvailableUSD takes each sub's own mutex): venueAvail = the
	// PolyUS book's bank − Σ ALL subs' open exposure; subCap = the promotion share (AllocUSD).
	// The old b.Bank field is journal-only now.
	venueAvail := s.bookAvailableUSD(ctx, vbPolyus)
	subCap := s.promoShareCap("xvgap")
	s.xvgBookMu.Lock()
	subHeld := 0.0
	{
		b := s.xvgLoadLocked()
		// R126 Part 2: episode gate replaces one-lot-per-market (same direction only, one lot
		// per episode, per-market cap; self-hedge guarded). See xvgEpisodeGate (xvboth.go).
		if why := xvgEpisodeGate(b.Open, slug, side, epN, maxEp); why != "" {
			s.xvgBookMu.Unlock()
			return
		}
		if len(b.Open) >= 100 {
			s.xvgBookMu.Unlock()
			return // runaway guard
		}
		subHeld = kfLotsExposure(b.Open)
	}
	s.xvgBookMu.Unlock()
	sizingEquity := math.Min(subCap, s.bookSizingEquityUSD(ctx, vbPolyus))
	if sizingEquity <= 0 || venueAvail <= 0 || subHeld >= subCap {
		return
	}
	stake := s.engineStake(ctx, "polyus", slug, side, "auto-cons-xvgap", yesPx, 0, sizingEquity, false, "realized-only")
	contracts := math.Floor(stake / yesPx)
	if contracts < 1 {
		contracts = 1
	}
	s.autoMu.Lock()
	slRatio, slMode := s.autoSLTPRatio, s.autoSLTPMode
	s.autoMu.Unlock()
	sl := 0.0
	if slMode != "ride" && slRatio > 0 {
		sl = math.Max(0.01, yesPx-slRatio*yesPx)
	}
	// TAKER entry at the live ask, by spec (see header). Router runs for the TAG (PUS ⇒
	// taker:no-queue-data), never to flip this book to maker.
	rd := s.routeMakerTaker(ctx, "polyus", slug, side, yesPx, "xvgap")
	px, fillCt, _, quoteOK := s.executableTaker(ctx, "polyus", slug, side, contracts)
	if !quoteOK || px > yesPx+0.03 || px < xvgEntryLo || px > xvgEntryHi {
		return // moved off the signal or out of the pocket — skip, never chase
	}
	contracts = fillCt
	fee := s.blendedFee("polyus", slug, "", false, contracts, px)
	cost := contracts * px
	s.xvgBookMu.Lock()
	b := s.xvgLoadLocked()
	held := kfLotsExposure(b.Open)
	// R127: sub share cap + venue-book available replace the old per-book Bank check.
	if held+cost > subCap || cost > venueAvail {
		s.xvgBookMu.Unlock()
		return // fully invested (sub share or venue book) — skip rather than margin a paper experiment
	}
	if why := xvgEpisodeGate(b.Open, slug, side, epN, maxEp); why != "" { // re-check under the lock (sizing window)
		s.xvgBookMu.Unlock()
		return
	}
	nowTS := time.Now().UTC().Format(time.RFC3339)
	lot := kfPos{TS: nowTS, Ticker: slug,
		Title: title, Side: side, Price: px, Contracts: contracts, Fee: fee, SL: sl, EpisodeN: epN,
		FillKind: "taker", FillRule: "taker:xvgap-pocket-spec", Platform: "polyus",
		RouteReason: rd.Tag(),
		SignalTS:    nowTS, DecisionTS: nowTS, FillTS: nowTS}
	s.stampKFExactFee(&lot)
	if !s.stampFundedKFRelation(ctx, &lot, "xvgap", "taker") {
		s.xvgBookMu.Unlock()
		return
	}
	b.Open = append(b.Open, lot)
	s.xvgBookDirty = true
	s.xvgBookMu.Unlock()
	s.xvgFlush()
	_ = s.store.Audit(ctx, "info", "xvgap",
		fmt.Sprintf("XvGap OPEN %s YES ×%.0f @ %.0f¢ ($%.2f, fee $%.2f) ep%d fill_kind=taker (pocket spec) route=%s", slug, contracts, px*100, cost, fee, epN, rd.Tag()), "")
	s.enqueueLiveMirrorCandidate(liveMirrorCandidate{
		Platform: "polyus", Ticker: slug, Title: title, Side: side, Source: "xvgap",
		Price: px, At: time.Now(),
	})
}

// settleXvgBook grades the book on the settlement tick: resolution first (signal_log authority),
// then the frozen stop against the cached venue mark. Runs beside settleFreshInvBook.
func (s *Server) settleXvgBook(ctx context.Context) {
	if s.xvgBookPoisoned.Load() {
		return
	}
	if !s.xvgActive() && !s.xvgWindingDown() {
		return
	}
	s.xvgBookMu.Lock()
	b := s.xvgLoadLocked()
	if len(b.Open) == 0 {
		s.xvgBookMu.Unlock()
		return
	}
	now := time.Now()
	still := b.Open[:0:0]
	changed := false
	var closedNotes []string
	var relationOutcomes []fundedKFOutcome
	for _, p := range b.Open {
		if yv, res := s.store.ResolvedYesForVenue(ctx, fiLotPlatform(p), p.Ticker); res {
			payout, pnl, won := fiSettlePnL(p.Side, yv, p.Price, p.Contracts, p.Fee)
			kfMarksClose(&p, payout, "settle")
			b.Net += pnl
			if won {
				b.Wins++
			} else {
				b.Losses++
			}
			b.Closed = append(b.Closed, kfClosed{kfPos: p, Payout: payout,
				PnL: math.Round(pnl*100) / 100, Won: won, SettledTS: now.UTC().Format(time.RFC3339), Reason: "settled"})
			relationOutcomes = append(relationOutcomes, fundedKFOutcome{Position: p, PnL: pnl, Source: "xvgap-settlement"})
			closedNotes = append(closedNotes, fmt.Sprintf("%s settled %+0.2f", p.Ticker, pnl))
			s.xvgBookDirty, changed = true, true
			continue
		}
		if sidePx, ok := s.fiSideMark(p); ok { // platform-aware zero-API mark (reused helper)
			nMk := len(p.Marks)
			kfStampMark(&p, sidePx, "mark")
			if len(p.Marks) != nMk {
				s.xvgBookDirty = true
			}
		}
		if p.SL > 0 {
			if sidePx, ok := s.fiSideMark(p); ok && sidePx <= p.SL {
				kfMarksClose(&p, sidePx, "stop")
				exitFee := s.blendedFeeAction("polyus", p.Ticker, "", false, p.Contracts, sidePx, false)
				pnl := p.Contracts*(sidePx-p.Price) - p.Fee - exitFee
				b.Net += pnl
				b.Losses++
				b.Closed = append(b.Closed, kfClosed{kfPos: p, Payout: sidePx,
					PnL: math.Round(pnl*100) / 100, Won: false, SettledTS: now.UTC().Format(time.RFC3339), Reason: "stop-loss"})
				relationOutcomes = append(relationOutcomes, fundedKFOutcome{Position: p, PnL: pnl, Source: "xvgap-stop-loss"})
				closedNotes = append(closedNotes, fmt.Sprintf("%s stop-loss %+0.2f", p.Ticker, pnl))
				s.xvgBookDirty, changed = true, true
				continue
			}
		}
		still = append(still, p)
	}
	b.Open = still
	if changed {
		if len(b.Closed) > 300 {
			b.Closed = b.Closed[len(b.Closed)-300:]
		}
		b.Equity = append(b.Equity, [2]float64{float64(now.Unix()), math.Round((b.Net-b.NetBase)*100) / 100})
		if len(b.Equity) > 400 {
			b.Equity = b.Equity[len(b.Equity)-400:]
		}
	}
	s.xvgBookMu.Unlock()
	for _, n := range closedNotes {
		_ = s.store.Audit(ctx, "info", "xvgap", "XvGap CLOSE "+n, "")
	}
	if s.xvgFlush() {
		s.settleFundedKFOutcomes(context.WithoutCancel(ctx), relationOutcomes)
	} else if len(relationOutcomes) > 0 {
		_ = s.store.Audit(context.WithoutCancel(ctx), "error", "relation",
			"XvGap funded outcomes retained until its position ledger persists", "")
	}
}

// xvgFlush persists the book if dirty (atomic tmp+rename; poison-guarded). R122 (auditor 438
// class): the dirty flag clears ONLY on rename success.
func (s *Server) xvgFlush() bool {
	defer s.invalidatePortfolioEquityCache()
	s.xvgBookMu.Lock()
	defer s.xvgBookMu.Unlock()
	if s.xvgBookPoisoned.Load() {
		return false
	}
	if !s.xvgBookDirty || s.xvgBook == nil {
		return true
	}
	out, err := json.Marshal(s.xvgBook)
	if err != nil {
		return false
	}
	path := filepath.Join(s.cfg().DataDir, "xvgap_book.json")
	tmp := path + ".tmp"
	if os.WriteFile(tmp, out, 0o644) == nil {
		if os.Rename(tmp, path) == nil {
			s.xvgBookDirty = false
			return true
		}
	}
	return false
}

// resetXvgBook starts a flat current epoch while retaining lifetime closed evidence.
func (s *Server) resetXvgBook(ctx context.Context) {
	res, err := s.resetXvgBookAt(time.Now())
	err = s.finishKFResetResult(ctx, res, err)
	if s.store != nil {
		level := "info"
		msg := fmt.Sprintf("reset P&L: XvGap started flat; %d prior opens archived", len(res.ArchivedOpen))
		if err != nil {
			level, msg = "error", "reset P&L: XvGap clean epoch failed: "+err.Error()
		}
		_ = s.store.Audit(context.WithoutCancel(ctx), level, "xvgap", msg, "")
	}
}

// xvgBookOpenSide — the symmetric cross-book hedge leg (fiBookOpenSide pattern): other books must
// not bet the opposite side of a market this book is riding.
func (s *Server) xvgBookOpenSide(platform, ticker string) string {
	if platform != "polyus" {
		return ""
	}
	if !s.xvgActive() && !s.xvgWindingDown() {
		return ""
	}
	s.xvgBookMu.Lock()
	defer s.xvgBookMu.Unlock()
	b := s.xvgLoadLocked()
	for _, p := range b.Open {
		if p.Ticker == ticker {
			return p.Side
		}
	}
	return ""
}

// handleXvgBook (GET /api/xvgap-book) — the running book, minimal surface (promotion panel +
// briefing read the pipeline stats; this is the direct view).
func (s *Server) handleXvgBook(w http.ResponseWriter, r *http.Request) {
	s.xvgBookMu.Lock()
	b := s.xvgLoadLocked()
	netSince, wins, losses := kfCurrentEpochStats(b)
	out := map[string]any{
		"active": s.promoRecordState("xvgap"), "bank": b.Bank,
		"accounting": "current-reset-epoch",
		"net":        math.Round(netSince*100) / 100,
		"wins":       wins, "losses": losses,
		"net_since_epoch": math.Round(netSince*100) / 100,
		"open":            len(b.Open), "open_lots": b.Open, "equity": b.Equity,
		"lifetime": map[string]any{"net": math.Round(b.Net*100) / 100, "wins": b.Wins, "losses": b.Losses},
		"note":     "xvgap pocket executor (R122): POLYUS-only 20-50c YES entries, taker by spec; R127: sub-strategy of the PolyUS venue book (promotion share cap); ride to settlement",
	}
	s.xvgBookMu.Unlock()
	writeJSON(w, http.StatusOK, out)
}
