package server

// makersim.go — R107 (operator order): EVERY paper book places via the MAKER SIM.
//
// Before R107 only the two autoPlace books (Kalshi auto / PolyUS auto) filled honestly (post at
// entryBid → rest as pendingMaker → fill ONLY on opposite-aggressor tape/queue consumption,
// with R106 live-parity cancels).
// The ML / RawFlow / Weather books booked INSTANTLY at their signal/live price with a maker fee —
// 100% fill probability at a price the market never proved, the exact class the audit-Q5 harness
// falsified for pcrypto. R107 routes all three through the SAME pendingMakers engine:
//
//   RawFlow/Weather — the place fns post at entryBid instead of appending; the sweep appends the
//   lot to the book (under its own mutex) only when the sim fills. Option-c (depth+momentum,
//   R105) gates the post: unsafe ⇒ DIVERT to an instant taker fill at entryAsk with taker fees,
//   tagged. Every lot carries fill_kind (maker|taker) plus executable fill/cancel provenance.
//
//   ML book — the Python sidecar can't share Go mutexes, so the contract is: sidecar POSTs its
//   buy decision to /api/mlpost (token-authed, loopback). Gate-unsafe or venue-blocked ⇒ the
//   response is an instant TAKER quote (px+fee) the sidecar books itself (single-writer: only the
//   sidecar appends to its own open[] mid-cycle). Gate-safe ⇒ Go posts a pendingMaker carrying
//   the sidecar's full lot row; after a proven tape/queue fill it lands in data/ml_maker_fills.jsonl
//   (GO-owned, append-only) which the sidecar ingests by watermark at cycle start — no shared
//   read-modify-write on ml_paper.json, no clobber window. GET /api/mlpending lets the sidecar
//   count resting posts toward deployed capital + one-lot-per-market.
//
//   PolyUS venue honesty (Part-1 verdict, R107): the live retest PROVED retail post-only GTC
//   orders REST at the venue (6 orders, 4 market classes, one held 50s in state NEW until we
//   canceled) — R105's "participate but not initiate" was the documented post-only-would-cross
//   rejection plus our own 1¢ stale-cancel sweep echoing back through the private WS. So PolyUS
//   paper books keep REAL maker sim. The exception path the operator specced stays wired behind
//   `polyus_maker_blocked` (default FALSE): flip it if the venue ever changes the rule and PUS
//   placements divert to taker tagged px_src=taker_venue_rule — paper must predict live.
//
//   Cost of patience — a canceled post joins a 30-min late-fill watch: if the market later
//   trades through the canceled level, mfs.late_fill=1 + late_fill_s stamp what the R106 cancel
//   rules cost in foregone fills; watch expiry stamps late_fill=0.
//
// The transition does NOT reset any epoch: books change fill behavior in place; the boot that
// activates this stamps ONE audit row so the auditor can split pre/post evidence.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// makerSimBooksOn — master knob (config maker_sim_books, default true; operator order R107).
func (s *Server) makerSimBooksOn() bool {
	return s.cfg().Auto.MakerSimBooks
}

// pusMakerBlockedNow — the Part-1 exception knob: TRUE forces PolyUS paper placements onto the
// taker path tagged px_src=taker_venue_rule. Default FALSE (retest verdict: venue allows maker).
func (s *Server) pusMakerBlockedNow(platform string) bool {
	return strings.EqualFold(platform, "polyus") && s.cfg().Auto.PolyusMakerBlocked
}

// makerSimTransitionStamp — one audit row on the first sweep of a maker-sim boot (NO epoch reset;
// the auditor splits book evidence at this timestamp).
func (s *Server) makerSimTransitionStamp(ctx context.Context) {
	if !s.makerSimBooksOn() {
		return
	}
	s.makerSimOnce.Do(func() {
		_ = s.store.Audit(ctx, "info", "paper",
			"MAKER-SIM TRANSITION R132: fills require an opposite-aggressor trade print; queue-known Kalshi posts also consume visible queue ahead. Midpoint crossings never fill. Cancel parity and taker diversion stay active.",
			fmt.Sprintf(`{"build":%q,"ts":%q}`, s.buildVersion(), time.Now().UTC().Format(time.RFC3339)))
	})
}

// ---- generalized pending-post plumbing ------------------------------------------------------

// bookMakerPost posts one book-routed maker attempt (rawflow/weather): mfs row + pendingMaker.
// Caller has already sized, gated, and (for its own book) reserved capital. Returns false when a
// pending attempt already rests on the market (dedup — shared namespace with the paper book by
// design: one resting quote per market process-wide). R122: rd = the queue-aware router's verdict
// (zero value ok — legacy callers/tests), stamped on the mfs row for router grading.
func (s *Server) bookMakerPost(ctx context.Context, book, platform, ticker, title, side, source string, postPx, contracts, sl float64, rd routeDecision) bool {
	if s.hasPendingMaker(platform, ticker) {
		return false
	}
	candidate := liveMirrorCandidate{
		Platform: platform, Ticker: ticker, Title: title, Side: side, Source: source,
		Price: postPx, At: time.Now(),
		Inverted: source == "freshinv" && !strings.EqualFold(platform, "polyus"),
	}
	sp, dep, depSrc := 0.0, 0.0, "none"
	if platform == "kalshi" {
		if s.kal != nil { // hermetic tests run without a venue client
			sp, _, _ = s.kalSigMeta(ctx, ticker)
		}
		dep, depSrc = s.kalDepthSrc(ticker)
	} else if s.polyUSWS != nil {
		if _, _, bs, as, ok := s.polyUSWS.Book3(ticker); ok {
			dep, depSrc = bs+as, "book3"
		}
	}
	mom, mok := s.pxRingMom5m(platform, ticker)
	var momPtr *float64
	if mok {
		m := mom
		momPtr = &m
	}
	relation := s.fundedSingleRelation(ctx, platform, ticker, side, source, "maker")
	relationID, relationErr := relation.allowBeforePlacement("passed-current-specialist-maker-checks", contracts, postPx,
		s.pureMakerFee(platform, ticker, contracts, postPx))
	if relationErr != nil {
		return false
	}
	id, err := s.store.InsertMakerAttempt(ctx, platform, ticker, side, source, postPx, sp, dep, 0, momPtr, s.cfg().Auto.MakerMomGateC, "", depSrc)
	if err != nil {
		_, _ = s.store.RecordFundedRelationPlacementFailure(context.WithoutCancel(ctx), relationID, "maker-attempt-insert-failed")
		return false
	}
	// R133: persist the actual canonical route before it can become maker LIVE evidence. An
	// unrecognized source stays blank and is deliberately ineligible rather than guessed.
	_ = s.store.SetMakerStrategy(ctx, id, liveMirrorFamily(candidate), candidate.Inverted)
	if rd.Reason != "" { // R122: router verdict + estimate inputs on the attempt row
		_ = s.store.SetMakerRoute(ctx, id, rd.Reason, rd.Queue, rd.RatePM, rd.ETASec, rd.HorizonSec)
	}
	qa, qk := s.queueAheadAt(platform, ticker, side, postPx) // R115 queue model
	s.pendMu.Lock()
	s.pendingMakers = append(s.pendingMakers, &pendingMaker{
		id: id, platform: platform, ticker: ticker, title: title, side: side, source: source,
		note: source, px: postPx, contracts: contracts, sl: sl, postedAt: time.Now(), book: book,
		queueAhead: qa, queueLeft: qa, queueKnown: qk, tapeCutoff: time.Now(),
		routeTag:          rd.Tag(), // R122: rides onto the book lot at fill
		relationReceiptID: relationID,
	})
	s.pendMu.Unlock()
	_ = s.store.Audit(ctx, "info", book, fmt.Sprintf("%s maker RESTING %s %s ×%.0f @ %.0f¢ — waiting for a real opposite trade print", book, strings.ToUpper(side), ticker, contracts, postPx*100), "")
	s.enqueueLiveMirrorCandidate(candidate)
	return true
}

// pendingBookStake sums the reserved cost of this book's resting posts (place-time capital
// accounting: a book must not over-post while fills pend). Takes pendMu ONLY — callers must NOT
// hold a book mutex (lock-order: pendMu is always leaf-or-alone).
func (s *Server) pendingBookStake(book string) float64 {
	s.pendMu.Lock()
	defer s.pendMu.Unlock()
	sum := 0.0
	for _, p := range s.pendingMakers {
		if p.book == book {
			sum += p.px * p.contracts
		}
	}
	return sum
}

// pendingPortfolioExposure reserves every funded maker post against its destination portfolio,
// including generic shared-auto posts (book=""). Previously only a specialist's own local check
// saw its posts, so another system in the same portfolio could spend the same dollars while the
// first quote rested. RawFlow/subcent research are not funded portfolio members; New ML owns its
// venue-sleeve reservation accounting in ml_paper.json and is therefore not duplicated here.
func (s *Server) pendingPortfolioExposure(book string) (float64, int) {
	type reservation struct {
		platform, ticker string
		px, contracts    float64
	}
	rows := make([]reservation, 0, 16)
	s.pendMu.Lock()
	for _, p := range s.pendingMakers {
		if p == nil || p.contracts <= 0 || p.px <= 0 || p.book == "rawflow" ||
			p.book == "subcent-golf" || p.book == "ml" {
			continue
		}
		platform := strings.ToLower(strings.TrimSpace(p.platform))
		if (book == vbKalshi && platform != vbKalshi) || (book == vbPolyus && platform != vbPolyus) ||
			(book != vbKalshi && book != vbPolyus) {
			continue
		}
		rows = append(rows, reservation{platform: platform, ticker: p.ticker, px: p.px, contracts: p.contracts})
	}
	s.pendMu.Unlock()
	total := 0.0
	for _, p := range rows {
		// Reserve the actual maker fee schedule as well as contract cost. This is outside pendMu:
		// fee lookup may consult venue metadata and must never invert the maker-sweep lock order.
		// A PolyUS maker rebate remains signed in the eventual fill, but cannot reduce collateral
		// reserved while the order is merely resting.
		total += p.px*p.contracts + math.Max(0, s.pureMakerFee(p.platform, p.ticker, p.contracts, p.px))
	}
	return total, len(rows)
}

// pendingBookHasTicker — one-post-per-market within a book (the cross-book dedup already lives in
// hasPendingMaker; this is the book-scoped variant used by capital math callers).
func (s *Server) pendingBookHasTicker(book, ticker string) bool {
	s.pendMu.Lock()
	defer s.pendMu.Unlock()
	for _, p := range s.pendingMakers {
		if p.book == book && p.ticker == ticker {
			return true
		}
	}
	return false
}

// ---- book fill routers (called from sweepPendingMakers after proven tape/queue execution) ----

// rfMakerFill books a filled RawFlow post into the RawFlow book. Returns "" on success or the
// cancel-rule tag when the fill can no longer be honored (conflict/capital moved while pending).
func (s *Server) rfMakerFill(ctx context.Context, p *pendingMaker, cur float64) string {
	if !s.paperEntryHorizonNow(ctx, p.platform, p.ticker, p.title) {
		return "entry-horizon"
	}
	if r := s.betConflictReason("kalshi", p.ticker, p.side, "auto-cons-rawflow", nil); r != "" {
		return "conflict"
	}
	fee := s.pureMakerFee(p.platform, p.ticker, p.contracts, p.px)
	cost := p.contracts * p.px
	s.rfBookMu.Lock()
	b := s.rfLoadLocked()
	for _, o := range b.Open {
		if o.Ticker == p.ticker {
			s.rfBookMu.Unlock()
			return "conflict"
		}
	}
	held := 0.0
	for _, o := range b.Open {
		held += o.Contracts*o.Price + o.Fee
	}
	if b.Bank+(b.Net-b.NetBase)-held < cost {
		s.rfBookMu.Unlock()
		return "capital"
	}
	// R120 provenance: signal/decision/post all happened in the post's creation pass (postedAt);
	// FillTS is now — post→fill is the honest maker latency.
	rfPostTS := p.postedAt.UTC().Format(time.RFC3339)
	lot := kfPos{TS: time.Now().UTC().Format(time.RFC3339), Ticker: p.ticker,
		Title: p.title, Side: p.side, Price: p.px, Contracts: p.contracts, Fee: fee, SL: p.sl,
		FillKind: "maker", FillRule: pendingFillRule(p), RouteReason: p.routeTag,
		SignalTS: rfPostTS, DecisionTS: rfPostTS, PostTS: rfPostTS,
		FillTS: time.Now().UTC().Format(time.RFC3339), RelationReceiptID: p.relationReceiptID}
	s.stampKFExactFee(&lot)
	b.Open = append(b.Open, lot)
	s.rfDirty = true
	s.rfBookMu.Unlock()
	s.rawFlowFlush()
	_ = s.store.Audit(ctx, "info", "rawflow",
		fmt.Sprintf("RawFlow MAKER FILL %s %s ×%.0f @ %.0f¢ (mark %.0f¢, fee $%.2f) fill_kind=maker fill_rule=%s", p.ticker, strings.ToUpper(p.side), p.contracts, p.px*100, cur*100, fee, pendingFillRule(p)), "")
	return ""
}

// wxMakerFill — the Weather-book twin of rfMakerFill.
func (s *Server) wxMakerFill(ctx context.Context, p *pendingMaker, cur float64) string {
	if !s.paperEntryHorizonNow(ctx, p.platform, p.ticker, p.title) {
		return "entry-horizon"
	}
	if r := s.betConflictReason("kalshi", p.ticker, p.side, "auto-cons-weather", nil); r != "" {
		return "conflict"
	}
	fee := s.pureMakerFee(p.platform, p.ticker, p.contracts, p.px)
	cost := p.contracts * p.px
	// R127: the Weather book is a sub of the KALSHI venue book — the venue-available gate
	// replaces the old per-book Bank check (computed BEFORE wxBookMu; lock order).
	// bookAvailableUSD already reserves this resting order. Add only this order's reservation
	// back while converting it from pending to filled; every other pending/open dollar stays held.
	venueAvail := s.bookAvailableUSD(ctx, vbKalshi) + cost + fee
	s.wxBookMu.Lock()
	b := s.wxBookLoadLocked()
	for _, o := range b.Open {
		if o.Ticker == p.ticker {
			s.wxBookMu.Unlock()
			return "conflict"
		}
	}
	if cost > venueAvail {
		s.wxBookMu.Unlock()
		return "capital"
	}
	// R120 provenance (same convention as rfMakerFill above).
	wxPostTS := p.postedAt.UTC().Format(time.RFC3339)
	lot := kfPos{TS: time.Now().UTC().Format(time.RFC3339), Ticker: p.ticker,
		Title: p.title, Side: p.side, Price: p.px, Contracts: p.contracts, Fee: fee, SL: p.sl,
		FillKind: "maker", FillRule: pendingFillRule(p), RouteReason: p.routeTag,
		SignalTS: wxPostTS, DecisionTS: wxPostTS, PostTS: wxPostTS,
		FillTS: time.Now().UTC().Format(time.RFC3339), RelationReceiptID: p.relationReceiptID}
	s.stampKFExactFee(&lot)
	b.Open = append(b.Open, lot)
	s.wxBookDirty = true
	s.wxBookMu.Unlock()
	s.weatherBookFlush()
	_ = s.store.Audit(ctx, "info", "weather",
		fmt.Sprintf("Weather MAKER FILL %s %s ×%.0f @ %.0f¢ (mark %.0f¢, fee $%.2f) fill_kind=maker fill_rule=%s", p.ticker, strings.ToUpper(p.side), p.contracts, p.px*100, cur*100, fee, pendingFillRule(p)), "")
	return ""
}

// mlMakerFill appends the filled ML lot to data/ml_maker_fills.jsonl (Go-owned, append-only; the
// sidecar ingests by watermark at cycle start — see live_ml.py R107). The row is the sidecar's own
// lot dict with the fill facts stamped over it.
func (s *Server) mlMakerFill(ctx context.Context, p *pendingMaker, cur float64) string {
	if held := s.mlBookOpenSides()[p.platform+"|"+p.ticker]; held != "" {
		return "conflict" // the market gained an ML lot while this post rested — one lot per market
	}
	row := map[string]any{}
	if len(p.mlRow) > 0 {
		if json.Unmarshal(p.mlRow, &row) != nil {
			row = map[string]any{}
		}
	}
	row["ticker"] = p.ticker
	row["side"] = p.side
	row["platform"] = p.platform
	row["contracts"] = p.contracts
	row["price"] = math.Round(p.px*1000) / 1000
	// R144 ML provenance: the maker fill price is the actual entry. Preserve the separate
	// decision-time market price/model edge carried in mlRow, but never leave entry_price at the
	// pre-post ask after a better resting fill.
	row["entry_price"] = row["price"]
	fee := s.pureMakerFee(p.platform, p.ticker, p.contracts, p.px)
	row["fee_known"], row["fee_source"] = false, ""
	if exact, source, known := s.fillFeeReceipt(p.platform, p.ticker, true, p.contracts, p.px); known {
		fee, row["fee_known"], row["fee_source"] = exact, true, source
	}
	row["fee"] = math.Round(fee*10000) / 10000
	row["opened"] = time.Now().Unix()
	// R120 provenance: the sidecar's row already carries signal_ts/decision_ts (stamped at the
	// chute); Go owns post_ts (when the post went up) and fill_ts (now). `opened` stays fill-time
	// (pre-R120 convention, unchanged for downstream readers).
	row["post_ts"] = p.postedAt.Unix()
	row["fill_ts"] = time.Now().Unix()
	row["px_src"] = "live"
	row["fill_kind"] = "maker"
	row["fill_rule"] = pendingFillRule(p)
	if p.relationReceiptID != "" {
		row["relation_receipt_id"] = p.relationReceiptID
	}
	if p.routeTag != "" {
		row["route_reason"] = p.routeTag // R122: post-time routing verdict on the ML lot
	}
	row["sig_px_at_fill"] = math.Round(cur*1000) / 1000
	// R115 Part 2: fill-moment re-score — decision-time vs fill-time p_win + drift on the lot.
	if p.decPWin > 0 {
		row["dec_pwin"] = math.Round(p.decPWin*1000) / 1000
		if pw, ok := s.mlResearchPWin(p.platform, p.ticker, p.side); ok {
			row["fill_pwin"] = math.Round(pw*1000) / 1000
			row["pwin_drift"] = math.Round((pw-p.decPWin)*1000) / 1000
		}
	}
	if p.queueKnown { // R115 queue model tags — the auditor grades sim realism off these
		row["queue_ahead"] = p.queueAhead
		row["queue_left"] = math.Round(p.queueLeft*10) / 10
	}
	line, err := json.Marshal(map[string]any{"seq": p.id, "ts": time.Now().Unix(), "row": row})
	if err != nil {
		return "conflict"
	}
	path := filepath.Join(s.cfg().DataDir, "ml_maker_fills.jsonl")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		s.log.Warn("ml maker fill append failed — fill NOT booked (fail-closed, mfs row stays canceled)", "err", err)
		return "io-error"
	}
	_, werr := f.Write(append(line, '\n'))
	_ = f.Close()
	if werr != nil {
		return "io-error"
	}
	_ = s.store.Audit(ctx, "info", "ml",
		fmt.Sprintf("ML MAKER FILL %s %s ×%.0f @ %.0f¢ (mark %.0f¢) queued for sidecar ingest fill_kind=maker fill_rule=%s", p.ticker, strings.ToUpper(p.side), p.contracts, p.px*100, cur*100, pendingFillRule(p)), "")
	return ""
}

// rotateMLMakerFills bounds the fill queue file at boot (~5MB → .1) — the sidecar's watermark is
// seq-based (mfs id, monotonic), so rotation never re-ingests.
func (s *Server) rotateMLMakerFills() {
	path := filepath.Join(s.cfg().DataDir, "ml_maker_fills.jsonl")
	if st, err := os.Stat(path); err == nil && st.Size() > 5<<20 {
		_ = os.Remove(path + ".1")
		_ = os.Rename(path, path+".1")
	}
}

// ---- late-fill watch (cost of patience) ------------------------------------------------------

type lateWatch struct {
	id                     int64
	platform, ticker, side string
	postPx                 float64
	canceledAt, until      time.Time
}

// lateWatchAdd queues a canceled post for the 30-min would-have-filled-later watch.
func (s *Server) lateWatchAdd(p *pendingMaker) {
	s.pendMu.Lock()
	defer s.pendMu.Unlock()
	if len(s.lateWatches) >= 2000 { // bound: drop the oldest (stats tool, not a ledger)
		s.lateWatches = s.lateWatches[1:]
	}
	now := time.Now()
	s.lateWatches = append(s.lateWatches, lateWatch{id: p.id, platform: p.platform, ticker: p.ticker,
		side: p.side, postPx: p.px, canceledAt: now, until: now.Add(30 * time.Minute)})
}

// sweepLateWatches — piggybacks on the maker sweep tick: a canceled level that trades through
// stamps late_fill=1 + the cancel→cross delay; expiry stamps late_fill=0. Memory-only across
// restarts (orphaned watches simply never conclude — late_fill stays NULL = "not measured").
func (s *Server) sweepLateWatches(ctx context.Context) {
	s.pendMu.Lock()
	watches := append([]lateWatch(nil), s.lateWatches...)
	s.pendMu.Unlock()
	if len(watches) == 0 {
		return
	}
	now := time.Now()
	keep := watches[:0:0]
	for _, w := range watches {
		if cur, ok := s.sideLivePriceAt(ctx, "late-watch", w.platform, w.ticker, w.side); ok && cur <= w.postPx-0.01+1e-9 {
			_ = s.store.SetMakerLateFill(ctx, w.id, int(now.Sub(w.canceledAt).Seconds()))
			continue
		}
		if now.After(w.until) {
			_ = s.store.SetMakerLateNo(ctx, w.id)
			continue
		}
		keep = append(keep, w)
	}
	s.pendMu.Lock()
	s.lateWatches = keep
	s.pendMu.Unlock()
}

// ---- ML sidecar endpoints --------------------------------------------------------------------

// handleMLPending (GET /api/mlpending) — the sidecar's view of its resting posts: counted toward
// deployed capital + one-lot-per-market at decision time.
func (s *Server) handleMLPending(w http.ResponseWriter, r *http.Request) {
	type pendingMLReservation struct {
		ticker, side, platform string
		px, contracts          float64
		posted                 int64
	}
	s.pendMu.Lock()
	rows := make([]pendingMLReservation, 0, 8)
	for _, p := range s.pendingMakers {
		if p.book != "ml" {
			continue
		}
		rows = append(rows, pendingMLReservation{ticker: p.ticker, side: p.side, platform: p.platform,
			px: p.px, contracts: p.contracts, posted: p.postedAt.Unix()})
	}
	s.pendMu.Unlock()
	out := make([]map[string]any, 0, len(rows))
	for _, p := range rows {
		// Fee lookup stays outside pendMu; venue metadata may own independent locks.
		fee := s.pureMakerFee(p.platform, p.ticker, p.contracts, p.px)
		out = append(out, map[string]any{"ticker": p.ticker, "side": p.side, "platform": p.platform,
			"post_px": p.px, "contracts": p.contracts, "fee": fee, "posted": p.posted})
	}
	writeJSON(w, http.StatusOK, map[string]any{"pending": out, "maker_sim": s.makerSimBooksOn()})
}

func mlPendingMakerConflict(rows []*pendingMaker, platform, ticker, twinKey, clusterKey string) (dup, trueDup bool, clusterStake float64) {
	for _, p := range rows {
		if p == nil || p.book == "subcent-golf" || p.platform != platform {
			continue // cohort-local research quotes have no paper portfolio/exposure authority
		}
		if p.ticker == ticker {
			return true, false, clusterStake
		}
		if twinKey != "" && trueRestatementKey(p.platform, p.ticker) == twinKey {
			return false, true, clusterStake
		}
		if clusterKey != "" && corrClusterKey(p.platform, p.ticker) == clusterKey {
			clusterStake += p.px * p.contracts
		}
	}
	return false, false, clusterStake
}

// handleMLPost (POST /api/mlpost, token-authed) — the sidecar's candidate observation endpoint.
// R166 keeps the endpoint for exact liveness receipts but rejects every new modeled Paper entry.
// Responses:
//
//	{"status":"pending","post_px":..}                — resting; fill arrives via ml_maker_fills.jsonl
//	{"status":"taker","px":..,"fee":..,"rule":..,"px_src":..} — sidecar books instantly at px (divert)
//	{"status":"reject","reason":..}                  — not a bet this cycle
func (s *Server) handleMLPost(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Row      map[string]any `json:"row"`
		RefPrice float64        `json:"ref_price"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Row == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "need row + ref_price"})
		return
	}
	gs := func(k string) string { v, _ := body.Row[k].(string); return v }
	gf := func(k string) float64 { v, _ := body.Row[k].(float64); return v }
	ticker, side, platform := gs("ticker"), strings.ToUpper(strings.TrimSpace(gs("side"))), strings.ToLower(gs("platform"))
	contracts := gf("contracts")
	if ticker == "" || (side != "YES" && side != "NO") || contracts < 1 || body.RefPrice <= 0 || body.RefPrice >= 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "row needs ticker/side/contracts and a sane ref_price"})
		return
	}
	if platform != "kalshi" && platform != "polyus" {
		// poly-int has no maker book surface here — the sidecar books those locally (legacy path).
		writeJSON(w, http.StatusOK, map[string]any{"status": "reject", "reason": "platform not maker-sim capable"})
		return
	}
	// The authenticated sidecar handoff is the ML book's real-time signal boundary. This is feed
	// observability only; every book, fee, depth, allocation and route gate below still applies.
	now := time.Now().UTC()
	topology := r147VenueCode(platform)
	for _, route := range []string{"maker", "taker"} {
		s.noteR147SignalContractRuntime("ml-book", platform, side, route, topology,
			ticker, "mlpost:validated-book-native-candidate", "signal", "", "",
			now, true, "")
	}
	if r166LegacyPaperNewEntriesRetired {
		const reason = "book-native-ml-paper-log-only"
		for _, route := range []string{"maker", "taker"} {
			s.noteR147SignalContractRuntime("ml-book", platform, side, route, topology,
				ticker, "mlpost:r166-paper-entry-retired", "excluded", "LOG-ONLY", reason,
				time.Now().UTC(), true, "")
		}
		s.logReject("auto-ml", platform, ticker, side, body.RefPrice, reason)
		writeJSON(w, http.StatusOK, map[string]any{"status": "reject", "reason": reason})
		return
	}
	ctx := r.Context()
	var relation *fundedRelationDecision
	relationFinished := false
	relationRejectReason := "mlpost-rejected-before-placement"
	mlReject := func(reason string) {
		relationRejectReason = strings.TrimSpace(reason)
		if relationRejectReason == "" {
			relationRejectReason = "mlpost-rejected-before-placement"
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "reject", "reason": relationRejectReason})
	}
	defer func() {
		if relationFinished {
			return
		}
		if relation == nil {
			relation = s.fundedSingleRelation(context.WithoutCancel(ctx), platform, ticker, side, "new-ml-v2", "candidate")
		}
		relation.ctx = context.WithoutCancel(ctx)
		relation.reject(relationRejectReason)
	}()
	if !s.makerSimBooksOn() {
		relationRejectReason = "maker_sim_books off — book locally"
		writeJSON(w, http.StatusOK, map[string]any{"status": "reject", "reason": "maker_sim_books off — book locally"})
		return
	}
	if s.ksBlocked() {
		mlReject("kill switch")
		return
	}
	// R128 timeout-inventory fix: the ML book had NO stale-feed placement gate (it relied on the
	// sidecar's no-live-price rule + the 3¢ chase guard) — fail closed on provably-dead feeds
	// exactly like the venue books (self-heals with the R108 watchdogs).
	if reason := s.pxOfflineReason(platform, ticker); reason != "" {
		mlReject(reason)
		return
	}
	takerQuote := func(rule, pxSrc string) {
		px, fillCt, quoteSrc, quoteOK := s.executableTaker(ctx, platform, ticker, side, contracts)
		if !quoteOK || fillCt+1e-9 < contracts {
			mlReject("insufficient executable ask depth")
			return
		}
		if px > body.RefPrice+0.03 { // autoPlace's staleness doctrine: never chase >3¢ past the decision price
			mlReject("moved-off-decision")
			return
		}
		fee, feeKnown, feeSource := s.blendedFee(platform, ticker, "", false, contracts, px), false, ""
		if exact, source, known := s.fillFeeReceipt(platform, ticker, false, contracts, px); known {
			fee, feeKnown, feeSource = exact, true, source
		}
		if quoteSrc != "" {
			pxSrc = quoteSrc
		}
		relation = s.fundedSingleRelation(ctx, platform, ticker, side, "new-ml-v2", "taker")
		relationID, err := relation.allowBeforePlacement("passed-current-new-ml-taker-checks", contracts, px, fee)
		if err != nil {
			_ = s.store.Audit(context.WithoutCancel(ctx), "error", "relation",
				"new-ml taker relation receipt failed", err.Error())
			mlReject("relation-receipt-failed")
			return
		}
		relationFinished = true
		writeJSON(w, http.StatusOK, map[string]any{"status": "taker", "px": math.Round(px*1000) / 1000,
			"fee": math.Round(fee*10000) / 10000, "fee_known": feeKnown, "fee_source": feeSource,
			"rule": rule, "px_src": pxSrc, "relation_receipt_id": relationID})
	}
	if s.pusMakerBlockedNow(platform) { // Part-1 exception path (default OFF per the R107 retest verdict)
		takerQuote("divert:venue_rule", "taker_venue_rule")
		return
	}
	if s.cfg().Auto.MakerAdverseGuard {
		if reason, dep, depSrc, momPtr, momGateC := s.makerPostUnsafe(platform, ticker, side); reason != "" {
			if s.makerGateStampAllowed(platform, ticker, "ml-book") {
				_ = s.store.InsertMakerGated(ctx, platform, ticker, side, "ml-book", body.RefPrice, 0, dep, momPtr, momGateC, reason, depSrc)
			}
			// R114 MAKER AUTOPSY (auditor r45 DO-THIS 8 / edge 1 — maker source/band gating):
			// EPOCH3 evidence says the divert-to-taker on INVISIBLE books was the single biggest
			// bleed: divert:depth0 lots n=150 → −$110.05 of the −$217 epoch (buying a MODELED ask
			// — the +2¢ fallback half-spread — in a market with no observable book is blind taker
			// entry). depth0 now SKIPS for the ML book; visible-but-thin/momentum diverts stay
			// (those weren't the bleed). Tagged skip:depth0 so the auditor can grade the switch.
			if strings.Contains(reason, "depth0") {
				mlReject("skip:depth0 (R114: no visible book — divert was -$110/150 lots in E3)")
				return
			}
			takerQuote("divert:"+reason, "live")
			return
		}
	}
	postPx := s.entryBid(ctx, platform, ticker, side, body.RefPrice)
	if postPx <= 0.01 || postPx >= 0.99 {
		mlReject("post px outside band")
		return
	}
	// R114 MAKER AUTOPSY (band leg of auditor r45 DO-THIS 8): sub-50¢ ML-book maker fills are near
	// pure adverse selection in the E3 data — price bands 10–40¢ won 12.5–25% of settled fills and
	// 71% of ml-book fills saw adverse 5m drift (avg −5.78¢). Below the band floor the book only
	// gets filled when informed flow runs it over. Config ml_maker_min_px_c (default 50; 0 = off).
	if minC := s.cfg().Auto.MLMakerMinPxC; minC >= 0 {
		if minC == 0 {
			minC = 50 // default; set the config key negative to disable
		}
		if postPx < minC/100 {
			mlReject(fmt.Sprintf("skip:maker-band (R114: post %.0f¢ < %.0f¢ floor — sub-band fills won 12-25%% in E3)", postPx*100, minC))
			return
		}
	}
	// R122 ROUTER (+ ML MAKER PAUSE — auditor r56 decision item 6 + 407, money policy): the
	// queue-aware maker/taker choice at the actual post price. While the sidecar's ece_gated
	// sensor is absent or above ml_maker_calib_gate_ece, EVERY ML candidate diverts to taker
	// (post-boot maker fill-EV was CI-negative ×2: −15.22¢ n=50). Queue-deep/no-flow books
	// divert too; the E3-protective depth0-SKIP above already ran.
	rd := s.routeMakerTaker(ctx, platform, ticker, side, postPx, "ml-book")
	if !rd.Maker {
		takerQuote("route:"+rd.Tag(), "live")
		return
	}
	raw, _ := json.Marshal(body.Row)
	// R111 (corrects R110): the ML book's dup policy is enforced HERE (its lots don't route
	// through betConflictReason's positions loop). Same ticker OR a TRUE restatement (identical
	// payoff — the crypto dual-series twins) is one bet → refused. CORRELATED-DISTINCT variants
	// (F3/F5/F7 vs full game, spread/strike ladders, corners bands — the operator's four-Dodgers
	// case was THIS class, not duplicates) are ALLOWED, tracked as a cluster and refused only
	// past the cluster_exposure_cap stake brake.
	// R112: twin dup-block only for API-VERIFIED identical twins (twins.go); the same-ticker
	// pending/dup check below is the one-lot-per-market position rule and stays.
	tkey := s.twinKeyGate(platform, ticker)
	ckey := corrClusterKey(platform, ticker)
	s.pendMu.Lock()
	dup, trueDup, clusterStake := mlPendingMakerConflict(s.pendingMakers, platform, ticker, tkey, ckey)
	s.pendMu.Unlock()
	if dup {
		mlReject("pending-maker")
		return
	}
	if trueDup {
		mlReject("dup-underlying")
		return
	}
	if tkey != "" || ckey != "" {
		stakes := s.mlBookOpenStakes()
		for k := range s.mlBookOpenSides() {
			if !strings.HasPrefix(k, platform+"|") {
				continue
			}
			tk := strings.TrimPrefix(k, platform+"|")
			if tkey != "" && trueRestatementKey(platform, tk) == tkey {
				mlReject("dup-underlying")
				return
			}
			if ckey != "" && corrClusterKey(platform, tk) == ckey {
				clusterStake += stakes[k]
			}
		}
	}
	if ckey != "" && clusterStake >= s.clusterExposureCap() {
		mlReject("corr-cluster-cap")
		return
	}
	sp, dep, depSrc := 0.0, 0.0, "none"
	if platform == "kalshi" {
		if s.kal != nil { // hermetic tests run without a venue client
			sp, _, _ = s.kalSigMeta(ctx, ticker)
		}
		dep, depSrc = s.kalDepthSrc(ticker)
	} else if s.polyUSWS != nil {
		if _, _, bs, as, ok := s.polyUSWS.Book3(ticker); ok {
			dep, depSrc = bs+as, "book3"
		}
	}
	mom, mok := s.pxRingMom5m(platform, ticker)
	var momPtr *float64
	if mok {
		m := mom
		momPtr = &m
	}
	postFee := s.pureMakerFee(platform, ticker, contracts, postPx)
	relation = s.fundedSingleRelation(ctx, platform, ticker, side, "new-ml-v2", "maker")
	relationID, relationErr := relation.allowBeforePlacement("passed-current-new-ml-maker-checks", contracts, postPx,
		postFee)
	if relationErr != nil {
		_ = s.store.Audit(context.WithoutCancel(ctx), "error", "relation",
			"new-ml maker relation receipt failed", relationErr.Error())
		mlReject("relation-receipt-failed")
		return
	}
	relationFinished = true
	id, err := s.store.InsertMakerAttempt(ctx, platform, ticker, side, "ml-book", postPx, sp, dep, 0, momPtr, s.cfg().Auto.MakerMomGateC, "", depSrc)
	if err != nil {
		_, _ = s.store.RecordFundedRelationPlacementFailure(context.WithoutCancel(ctx), relationID, "maker-attempt-insert-failed")
		mlReject("mfs insert failed")
		return
	}
	_ = s.store.SetMakerRoute(ctx, id, rd.Reason, rd.Queue, rd.RatePM, rd.ETASec, rd.HorizonSec) // R122
	qa, qk := s.queueAheadAt(platform, ticker, side, postPx)                                     // R115 queue model
	s.pendMu.Lock()
	s.pendingMakers = append(s.pendingMakers, &pendingMaker{
		id: id, platform: platform, ticker: ticker, title: gs("title"), side: side, source: "ml-book",
		note: "ml-book", px: postPx, contracts: contracts, postedAt: time.Now(), book: "ml", mlRow: raw,
		queueAhead: qa, queueLeft: qa, queueKnown: qk, tapeCutoff: time.Now(),
		decPWin:           gf("p_win"), // R115 Part 2: decision-time p_win — fill-time drift baseline + edge-died pull
		routeTag:          rd.Tag(),    // R122: rides onto the ML lot at fill
		relationReceiptID: relationID,
	})
	s.pendMu.Unlock()
	_ = s.store.Audit(ctx, "info", "ml", fmt.Sprintf("ML maker RESTING %s %s ×%.0f @ %.0f¢ — waiting for a real opposite trade print", strings.ToUpper(side), ticker, contracts, postPx*100), "")
	// R128 (operator Part 2: combos as an expression for the ML book): a Kalshi ML candidate with
	// a live comboable partner also expresses as a Combos-book synthetic leg-stack (xvComboTry's
	// own correlation/fee gate decides; src "ml" splits the stats).
	if platform == "kalshi" {
		if edge := gf("p_win") - postPx; edge > 0 {
			s.xvComboTry(ctx, "ml", ticker, side, postPx, edge, 0)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "pending", "post_px": math.Round(postPx*1000) / 1000,
		"fee": postFee, "relation_receipt_id": relationID})
}
