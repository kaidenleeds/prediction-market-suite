package server

// books.go — R127 FOUR-BOOK RESTRUCTURE (operator's exact order).
//
// Paper money consolidates into exactly FOUR portfolios:
//
//	Kalshi $600 — shared-auto-kalshi · kflow-pre · kflow-live · weather · freshinv
//	PolyUS $600 — shared-auto-polyus · xvgap
//	Combos $600 — the R126 combo overlay's MONEY lots (expr "synthetic" leg-stacks + the rare
//	               "rfq" venue-quote lots; the product-cost rows continue as the log-only
//	               "product-sim" study family and never spend this bank)
//	ML     $600 — the sidecar ML book (bank via the ml_alloc.json handshake)
//
// Every old per-strategy book folds into its venue book as a TAGGED SUB-STRATEGY: its ledger
// file/format, tags and verdict-engine grading are UNCHANGED (strategies remain the unit of
// proof); only the MONEY consolidates. Placement paths consult
//
//	venue-book available = (starting grant + current-epoch fee-net P&L) − Σ open exposure
//
// instead of their old per-book Bank. The old kfBook.Bank fields stay as journal/back-compat
// but no longer gate placement. If a venue book starts over-allocated (old open exposure >
// new bank), placement simply PAUSES until lots settle below the bank — no forced closes.
//
// DESIGN NOTES (decisions, documented):
//   - Each funded portfolio compounds independently. Realized fee-net P&L since Reset moves only
//     that portfolio's sizing equity; another portfolio never subsidizes it. Equity floors at zero.
//   - freshinv holds lots on BOTH venues; the whole book is mapped under the Kalshi venue book
//     (the operator's mapping), so its polyus lots count against Kalshi book exposure.
//   - rawflow is FOLDED (alloc 0, wind-down exemplar) — not a sub of any venue book; its
//     residual lots settle in place and never gate the new books.
//   - The shared-auto exposure read rides paperFillsCached; a read failure fails CLOSED
//     (available=0 → placements pause one tick) — fabricated-zero exposure would over-place.
//   - Per-book pending maker-post stake stays subtracted inside each sub's own placement path
//     (pendingBookStake), exactly as before — the venue-level formula counts FILLED lots.
//   - MIGRATION (kv latch r127_books_v1): one-time journal of every old book's {name, old bank,
//     lifetime net, open lots, open exposure} into the audit log AND data\r127_book_transfer.json.
//     Ledgers keep their lots; open lots settle in place, counting against the venue book.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	vbKalshi = "kalshi"
	vbPolyus = "polyus"
	vbCombos = "combos"
	vbML     = "ml"
)

// bookBankUSD — the venue book's starting/reset grant (0/unset ⇒ operator defaults).
func fixedPaperPortfolioUSD(configured float64) float64 {
	if configured > 0 {
		return configured
	}
	return 600
}

// bookBankUSD is the configured/reset starting grant, not current sizing equity. Money paths use
// bookCurrentEquityUSD or bookAvailableUSD so each portfolio compounds only its own realized P&L.
func (s *Server) bookBankUSD(book string) float64 {
	a := s.cfg().Auto
	switch book {
	case vbKalshi:
		return fixedPaperPortfolioUSD(a.BookKalshiUSD)
	case vbPolyus:
		return fixedPaperPortfolioUSD(a.BookPolyusUSD)
	case vbCombos:
		return fixedPaperPortfolioUSD(a.BookCombosUSD)
	case vbML:
		return fixedPaperPortfolioUSD(a.BookMLUSD)
	}
	return 0
}

// mlPaperVenueBanks splits the ONE ML Paper grant into its two executable venue sleeves. There is
// intentionally no second grant: the returned values add back to total after cent rounding. The
// operator has not selected a venue tilt, so equal risk capacity is the only neutral default.
func mlPaperVenueBanks(total float64) map[string]float64 {
	if total <= 0 || math.IsNaN(total) || math.IsInf(total, 0) {
		return map[string]float64{vbKalshi: 0, vbPolyus: 0}
	}
	k := math.Round(total*50) / 100 // round(total/2, cents)
	p := math.Round((total-k)*100) / 100
	return map[string]float64{vbKalshi: k, vbPolyus: p}
}

// bookSub is one tagged sub-strategy of a venue book (the briefing roster registry).
type bookSub struct {
	Book   string // parent venue book
	Plain  string // plain-language roster name (R91 plain-words law)
	Family string // verdict-engine family ("" ⇒ the shared auto executor: computed from closed trades)
	Promo  string // promotion-pipeline family when the sub is pipeline-managed ("" otherwise)
	// Seed — R130 automatic follower lines only: the verdict family whose scoreboard ¢ funds the
	// weight while the sub has no settled lots of its own ("<fam>" or "invert:<fam>"). Static
	// lines leave it "".
	Seed string
	// R132 dynamic followers seed from the SAME VENUE'S verdict, never a pooled cross-venue mean.
	SeedMean  float64
	SeedN     int
	SeedState string
}

// venueBookSubs — THE mapping (operator's exact order). Each sub keeps its own ledger/tags/
// grading; money consolidates per parent book.
func venueBookSubs() []bookSub {
	return []bookSub{
		{Book: vbKalshi, Plain: "kalshi auto", Family: "", Promo: ""},
		{Book: vbKalshi, Plain: "kflow book (pre)", Family: "kflow-pre"},
		{Book: vbKalshi, Plain: "kflow book (live)", Family: "kflow-live"},
		{Book: vbKalshi, Plain: "weather book", Family: "weather"},
		{Book: vbKalshi, Plain: "fresh-listing fade", Family: "book:freshinv-k", Promo: "invert:freshlist"},
		{Book: vbKalshi, Plain: "favorites >80¢ (maker)", Family: "book:favlong80"}, // R128: B1-study bettor
		{Book: vbKalshi, Plain: "cheap side <10¢", Family: "book:cheapband-k"},      // R129: band-study bettor (Kalshi 0-10¢ cell)
		{Book: vbPolyus, Plain: "polyus auto", Family: ""},
		{Book: vbPolyus, Plain: "fresh-listing follow", Family: "book:freshlist-p"},
		{Book: vbPolyus, Plain: "cross-venue gap book", Family: "book:xvgap", Promo: "xvgap"},
		{Book: vbPolyus, Plain: "cheap side <15¢", Family: "book:cheapband-p"}, // R129: band-study bettor (PolyUS <15¢ cells)
		{Book: vbCombos, Plain: "gap combo (product study)", Family: "combo-overlay"},
		{Book: vbCombos, Plain: "gap combo (leg-stack)", Family: "combo-synth"},
		{Book: vbCombos, Plain: "gap combo (venue quote)", Family: "combo-rfq"},
		{Book: vbCombos, Plain: "cross-venue lock (staked)", Family: "lockstack"}, // R129: depth-verified K↔PUS lock stakes
		{Book: vbML, Plain: "ML book", Family: "ml-book"},
	}
}

// kfLotsExposure sums open cost (contracts × entry + booked fee) of a kfBook-shaped lot list.
func kfLotsExposure(open []kfPos) float64 {
	usd := 0.0
	for _, p := range open {
		usd += p.Contracts*p.Price + p.Fee
	}
	return usd
}

// sharedAutoExposure — the shared auto executor's open cost basis + lot count on one venue
// (paper_fills through the R98 cached read). ok=false ⇒ the read failed with no cache — the
// caller must fail CLOSED (never treat a blind read as zero exposure).
func (s *Server) sharedAutoExposure(ctx context.Context, platform string) (usd float64, lots int, ok bool) {
	fills, _, _, err := s.paperFillsCached(ctx)
	if err != nil {
		return 0, 0, false
	}
	positions, _ := paper.Aggregate(fills)
	for _, p := range positions {
		if p.Contracts > 0 && p.Platform == platform {
			usd += p.CostBasis
			lots++
		}
	}
	return usd, lots, true
}

// bookOpenExposure — Σ open exposure of every sub in a venue book (+ total open lots).
// ok=false ⇒ a component read failed (shared-auto fills blind) — fail closed upstream.
// LOCK ORDER: takes each sub-book's own mutex briefly; callers must NOT hold any book mutex.
func (s *Server) bookOpenExposure(ctx context.Context, book string) (usd float64, lots int, ok bool) {
	ok = true
	switch book {
	case vbKalshi:
		u, n, aok := s.sharedAutoExposure(ctx, "kalshi")
		usd, lots, ok = u, n, aok
		s.kfBookMu.Lock()
		kb := s.kfLoadLocked()
		usd += kfLotsExposure(kb.Pre.Open) + kfLotsExposure(kb.Live.Open)
		lots += len(kb.Pre.Open) + len(kb.Live.Open)
		s.kfBookMu.Unlock()
		s.wxBookMu.Lock()
		wb := s.wxBookLoadLocked()
		usd += kfLotsExposure(wb.Open)
		lots += len(wb.Open)
		s.wxBookMu.Unlock()
		s.fiBookMu.Lock()
		fb := s.fiLoadLocked()
		fu, fn := fiExposure(fb.Open, vbKalshi)
		usd, lots = usd+fu, lots+fn
		s.fiBookMu.Unlock()
		s.flBookMu.Lock()
		flb := s.flLoadLocked() // R128: FavLong80 lots ride the Kalshi book wall too
		usd += kfLotsExposure(flb.Open)
		lots += len(flb.Open)
		s.flBookMu.Unlock()
		s.cbBookMu.Lock()
		cbb := s.cbLoadLocked() // R129: Cheapband's Kalshi half
		usd += kfLotsExposure(cbb.K.Open)
		lots += len(cbb.K.Open)
		s.cbBookMu.Unlock()
		gu, gl, gok := s.gfExposureByVenue(vbKalshi) // R130/R132: poisoned ledger fails the venue wall closed
		usd += gu
		lots += gl
		ok = ok && gok
	case vbPolyus:
		u, n, aok := s.sharedAutoExposure(ctx, "polyus")
		usd, lots, ok = u, n, aok
		s.xvgBookMu.Lock()
		xb := s.xvgLoadLocked()
		usd += kfLotsExposure(xb.Open)
		lots += len(xb.Open)
		s.xvgBookMu.Unlock()
		s.fiBookMu.Lock()
		fb := s.fiLoadLocked()
		fu, fn := fiExposure(fb.Open, vbPolyus)
		usd, lots = usd+fu, lots+fn
		s.fiBookMu.Unlock()
		s.cbBookMu.Lock()
		cbb := s.cbLoadLocked() // R129: Cheapband's PolyUS half
		usd += kfLotsExposure(cbb.P.Open)
		lots += len(cbb.P.Open)
		s.cbBookMu.Unlock()
		gu, gl, gok := s.gfExposureByVenue(vbPolyus)
		usd += gu
		lots += gl
		ok = ok && gok
	case vbCombos:
		s.xvcMu.Lock()
		cb := s.xvcLoadLocked()
		for _, p := range cb.Open {
			lots++
			if p.Expr == xvcExprSynth || p.Expr == xvcExprRFQ { // money expressions only —
				usd += p.Cost*p.Contracts + p.Fee // product-sim rows are the log-only study
			}
		}
		s.xvcMu.Unlock()
		s.xvlMu.Lock()
		for _, op := range s.xvlLoadLocked().Open { // R129: staked locks spend the Combos bank
			if op.Staked > 0 {
				usd += op.stakedCostUSD()
				lots++
			}
		}
		s.xvlMu.Unlock()
		// R148 staged additive packages are not conjunction parlays and therefore own a separate
		// append-only SQL ledger. Their all-in accepted cost shares this same Combo exposure wall.
		if s.store == nil {
			ok = false
		} else if staged, err := s.store.StagedPaperBundleFundsSince(ctx, time.Time{}); err != nil {
			ok = false
		} else {
			usd += staged.OpenCost
			lots += staged.OpenCount
		}
	case vbML:
		// Display only — Go never places ML lots (the sidecar owns that book + its own sizing).
		if pf, current := s.currentMLPaperBrief(); current {
			for _, o := range pf.Open {
				usd += o.Price * o.Contracts
				lots++
			}
		}
	}
	if book == vbKalshi || book == vbPolyus {
		pendingUSD, pendingN := s.pendingPortfolioExposure(book)
		usd += pendingUSD
		lots += pendingN
	}
	return usd, lots, ok
}

// bookAvailableUSD — independently compounded equity − all sub-strategy open exposure.
// A blind ledger read returns 0 (fail closed — placements pause one tick, never over-place).
func (s *Server) bookAvailableUSD(ctx context.Context, book string) float64 {
	equity, eqOK := s.bookCurrentEquityUSD(ctx, book)
	if !eqOK {
		return 0
	}
	exp, _, ok := s.bookOpenExposure(ctx, book)
	if !ok {
		return 0
	}
	return equity - exp
}

// promoShareCap — a sub-strategy's allocation share within its venue book.
// R128 (operator's core order): the SCOREBOARD WEIGHT is the share — weight × the venue book's
// bank (scoreweight.go), refreshed continuously from lifetime realized ¢/unit. The fixed $100
// promotion share + GROW checkpoints are superseded (records stay as journal); the legacy
// AllocUSD is only the fallback while the weight table is cold (first ~1 min of a boot).
func (s *Server) promoShareCap(family string) float64 {
	roster := map[string]string{
		"invert:freshlist": "book:freshinv-k",
		"xvgap":            "book:xvgap",
		"favlong80":        "book:favlong80",
	}[family]
	if roster == "" {
		roster = family
	}
	if share, ok := s.subShareUSD(roster); ok {
		return share
	}
	capUSD := s.promoCfg().BankrollUSD
	s.promoMu.Lock()
	if r, okR := s.promoLoadLocked().Records[family]; okR && r.AllocUSD > 0 {
		capUSD = r.AllocUSD
	}
	s.promoMu.Unlock()
	return capUSD
}

// bookAllTimeNet — the DISPLAY net of a venue book: Σ sub lifetime nets, all-time since first
// run (R127 item 8 — no *Base subtraction anywhere; resets stay operational but displays
// ignore the bases). ML is the explicit exception: every active ML surface reports only the
// current book-native-v2 cohort/epoch, while older model history remains archive-only.
func (s *Server) bookAllTimeNet(ctx context.Context, book string) (net float64, ok bool) {
	switch book {
	case vbKalshi, vbPolyus:
		fills, _, _, err := s.paperFillsCached(ctx)
		if err != nil {
			return 0, false
		}
		for _, ss := range paper.Stats(fills).BySource {
			if ss.Platform == book {
				net += ss.Realized - ss.Fees
			}
		}
		if book == vbKalshi {
			s.kfBookMu.Lock()
			kb := s.kfLoadLocked()
			net += kb.Pre.Net + kb.Live.Net
			s.kfBookMu.Unlock()
			s.wxBookMu.Lock()
			net += s.wxBookLoadLocked().Net
			s.wxBookMu.Unlock()
			s.fiBookMu.Lock()
			net += s.fiLoadLocked().Net
			s.fiBookMu.Unlock()
			s.flBookMu.Lock()
			net += s.flLoadLocked().Net // R129 fix: favlong80 was missing from the display net (R128 omission)
			s.flBookMu.Unlock()
			s.cbBookMu.Lock()
			net += s.cbLoadLocked().K.Net // R129: Cheapband Kalshi half
			s.cbBookMu.Unlock()
			net += s.gfNetByVenue(vbKalshi) // R130: the follower's kalshi subs
		} else {
			s.xvgBookMu.Lock()
			net += s.xvgLoadLocked().Net
			s.xvgBookMu.Unlock()
			s.cbBookMu.Lock()
			net += s.cbLoadLocked().P.Net // R129: Cheapband PolyUS half
			s.cbBookMu.Unlock()
			net += s.gfNetByVenue(vbPolyus) // R130: the follower's polyus subs
		}
		return net, true
	case vbCombos:
		s.xvcMu.Lock()
		net, _, _ = xvcCurrentEpochStats(s.xvcLoadLocked())
		s.xvcMu.Unlock()
		s.xvlMu.Lock()
		for _, op := range s.xvlLoadLocked().Closed { // R129: staked locks' realized $
			if op.Staked > 0 {
				net += op.stakedNetUSD()
			}
		}
		s.xvlMu.Unlock()
		if s.store == nil {
			return 0, false
		}
		staged, err := s.store.StagedPaperBundleFundsSince(ctx, time.Time{})
		if err != nil {
			return 0, false
		}
		net += staged.Realized
		return net, true
	case vbML:
		pf, current := s.currentMLPaperBrief()
		if !current {
			return 0, false
		}
		return pf.Stats.Net, true
	}
	return 0, false
}

// bookSessionMetrics is the briefing's exact process-epoch view. A bet is one closed
// position/lot. A unit is the amount that the strategy scoreboard grades: a contract/share for
// singles and ML, or one all-in combo/lock set for the Combos book. UnitsComplete is false if any
// included close lacks a trustworthy unit count; callers must render the per-unit value as n/a
// instead of dividing only part of the book's P&L by the known units.
type bookSessionMetrics struct {
	NetUSD        float64
	Bets          int
	Units         float64
	UnitsComplete bool
	Open          int
	OpenComplete  bool
}

// bookSessionClosedMetrics reads only positions after the durable Reset-P&L epoch when one exists,
// with bootAt as the fallback before the first reset. The reset boundary intentionally survives a
// process restart: current open positions and post-reset performance may not disappear merely
// because the suite was rebuilt. Strict timestamp comparison keeps the reset's own flattening
// trades out of the fresh portfolio. Dollar/bet is NetUSD/Bets; cents/unit is NetUSD/Units*100
// when UnitsComplete.
func (s *Server) bookSessionClosedMetrics(ctx context.Context, book string) (m bookSessionMetrics, ok bool) {
	epoch := s.bootAt
	comboAfterID := int64(0)
	s.pnlMu.Lock()
	if !s.pnlEpoch.IsZero() {
		epoch = s.pnlEpoch
	}
	comboAfterID = s.pnlComboAfterID
	s.pnlMu.Unlock()
	parseStamp := func(raw string) (time.Time, bool) {
		if raw == "" {
			return time.Time{}, false
		}
		t, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			t, err = time.Parse(time.RFC3339, raw)
		}
		return t, err == nil
	}
	after := func(raw string) bool {
		t, valid := parseStamp(raw)
		// Strictly after is intentional: Windows can stamp the final reset-close and the reset
		// epoch with the same 100ns wall-clock value. Equality belongs to the old epoch.
		return valid && t.After(epoch)
	}
	m.UnitsComplete, m.OpenComplete = true, true
	add := func(net, units float64) {
		m.NetUSD += net
		m.Bets++
		if units > 0 && !math.IsNaN(units) && !math.IsInf(units, 0) {
			m.Units += units
		} else {
			m.UnitsComplete = false
		}
	}
	addOpen := func(raw string) {
		t, valid := parseStamp(raw)
		if !valid {
			m.OpenComplete = false
			return
		}
		// Reset closes every funded open first. Therefore an open stamped exactly at the coarse
		// Windows boundary is a new/current position, unlike an equal-stamped close above.
		if !t.Before(epoch) {
			m.Open++
		}
	}
	addOpenUnix := func(ts int64) {
		if ts <= 0 {
			m.OpenComplete = false
			return
		}
		if ts >= epoch.Unix() {
			m.Open++
		}
	}
	addKF := func(rows []kfClosed, platform string) {
		for _, c := range rows {
			if platform != "" && fiLotPlatform(c.kfPos) != platform {
				continue
			}
			if after(c.SettledTS) {
				add(c.PnL, c.Contracts)
			}
		}
	}
	addKFOpen := func(rows []kfPos, platform string) {
		for _, p := range rows {
			if platform != "" && fiLotPlatform(p) != platform {
				continue
			}
			// FillTS is the actual funded-open time. Older ledgers only have TS, which was written
			// at the same placement decision; a row with neither is not silently counted lifetime.
			ts := p.FillTS
			if ts == "" {
				ts = p.TS
			}
			addOpen(ts)
		}
	}
	ok = true
	switch book {
	case vbKalshi, vbPolyus:
		fills, _, _, err := s.paperFillsCached(ctx)
		if err != nil {
			return bookSessionMetrics{}, false
		}
		for _, t := range paper.ClosedTrades(fills) {
			if t.Platform == book && after(t.ClosedAt) {
				add(t.Realized-t.Fees, t.Contracts)
			}
		}
		positions, _ := paper.Aggregate(fills)
		for _, p := range positions {
			if p.Platform == book {
				addOpen(p.Opened)
			}
		}
		if book == vbKalshi {
			s.kfBookMu.Lock()
			kb := s.kfLoadLocked()
			addKF(kb.Pre.Closed, "")
			addKF(kb.Live.Closed, "")
			addKFOpen(kb.Pre.Open, "")
			addKFOpen(kb.Live.Open, "")
			s.kfBookMu.Unlock()
			s.wxBookMu.Lock()
			wb := s.wxBookLoadLocked()
			addKF(wb.Closed, "")
			addKFOpen(wb.Open, "")
			s.wxBookMu.Unlock()
			s.flBookMu.Lock()
			fb := s.flLoadLocked()
			addKF(fb.Closed, "")
			addKFOpen(fb.Open, "")
			s.flBookMu.Unlock()
		} else {
			s.xvgBookMu.Lock()
			xb := s.xvgLoadLocked()
			addKF(xb.Closed, "")
			addKFOpen(xb.Open, "")
			s.xvgBookMu.Unlock()
		}
		s.fiBookMu.Lock()
		fib := s.fiLoadLocked()
		addKF(fib.Closed, book)
		addKFOpen(fib.Open, book)
		s.fiBookMu.Unlock()
		s.cbBookMu.Lock()
		if book == vbKalshi {
			cb := s.cbLoadLocked()
			addKF(cb.K.Closed, "")
			addKFOpen(cb.K.Open, "")
		} else {
			cb := s.cbLoadLocked()
			addKF(cb.P.Closed, "")
			addKFOpen(cb.P.Open, "")
		}
		s.cbBookMu.Unlock()
		s.gfBookMu.Lock()
		suffix := "-k"
		if book == vbPolyus {
			suffix = "-p"
		}
		for k, b := range s.gfLoadLocked().Subs {
			if strings.HasSuffix(k, suffix) {
				addKF(b.Closed, "")
				addKFOpen(b.Open, "")
			}
		}
		s.gfBookMu.Unlock()
		if (book == vbKalshi && (s.kfPoisoned.Load() || s.wxBookPoisoned.Load() || s.fiBookPoisoned.Load() ||
			s.flBookPoisoned.Load() || s.cbBookPoisoned.Load() || s.gfBookPoisoned.Load())) ||
			(book == vbPolyus && (s.xvgBookPoisoned.Load() || s.fiBookPoisoned.Load() ||
				s.cbBookPoisoned.Load() || s.gfBookPoisoned.Load())) {
			return bookSessionMetrics{}, false
		}
	case vbCombos:
		// A briefing is a read-only status surface. Never queue it behind a long-running combo
		// maintenance pass: the last-good per-portfolio cache below is a safer answer than holding the
		// whole briefing request open. TryLock also prevents a burst of previews from forming a waiter
		// convoy behind the same file-backed ledger.
		if !s.xvcMu.TryLock() {
			return bookSessionMetrics{}, false
		}
		xcb := s.xvcLoadLocked()
		for _, c := range xcb.Closed {
			if (c.Expr == xvcExprSynth || c.Expr == xvcExprRFQ) && after(c.SettledTS) {
				add(c.PnL, c.Contracts)
			}
		}
		for _, p := range xcb.Open {
			if p.Expr == xvcExprSynth || p.Expr == xvcExprRFQ {
				addOpen(p.TS)
			}
		}
		s.xvcMu.Unlock()
		// xvlock used to hold xvlMu while reading one resolution certificate per candidate. When that
		// scan stalled, this unconditional Lock made /api/briefing-preview ignore its 30-second context
		// and hang for more than a minute. Reads now fail fast and serve the last-good briefing value.
		if !s.xvlLockContext(ctx, 0) {
			return bookSessionMetrics{}, false
		}
		xlb := s.xvlLoadLocked()
		for _, c := range xlb.Closed {
			if c.Staked > 0 && after(c.SettledTS) {
				add(c.stakedNetUSD(), c.Staked)
			}
		}
		for _, p := range xlb.Open {
			if p.Staked > 0 {
				addOpen(p.FirstSeen)
			}
		}
		s.xvlMu.Unlock()
		// The funded positive-system route is the current Combos portfolio's only parlay lane.
		// It lives in SQLite rather than the older combo-overlay/lock books, so include its durable
		// provenance rows here while continuing to exclude every legacy/generic/RFQ parlay.
		aggregateEpoch := epoch
		if comboAfterID > 0 {
			aggregateEpoch = time.Time{} // the durable row-id boundary is authoritative when present
		}
		funds, err := s.store.RouteCohortParlayFunds(ctx, positiveSystemComboRouteSource,
			storage.ComboLabCohortRollingPositive, comboAfterID, aggregateEpoch)
		if err != nil {
			return bookSessionMetrics{}, false
		}
		m.NetUSD += funds.Realized
		m.Bets += funds.ClosedCount
		m.Open += funds.OpenCount
		if funds.ClosedCount > 0 {
			if funds.ClosedContracts > 0 && !math.IsNaN(funds.ClosedContracts) && !math.IsInf(funds.ClosedContracts, 0) {
				m.Units += funds.ClosedContracts
			} else {
				m.UnitsComplete = false
			}
		}
		// Additive staged packages are true Combo Paper positions but cannot be represented by
		// paper_parlays' product payoff. Fold their exact fee-net rows into the same independently
		// compounding portfolio and keep one accepted package as one bet.
		staged, err := s.store.StagedPaperBundleFundsSince(ctx, epoch)
		if err != nil {
			return bookSessionMetrics{}, false
		}
		m.NetUSD += staged.Realized
		m.Bets += staged.ClosedCount
		m.Open += staged.OpenCount
		if staged.ClosedCount > 0 {
			if staged.ClosedUnits > 0 && !math.IsNaN(staged.ClosedUnits) && !math.IsInf(staged.ClosedUnits, 0) {
				m.Units += staged.ClosedUnits
			} else {
				m.UnitsComplete = false
			}
		}
		if s.xvlPoisoned.Load() {
			return bookSessionMetrics{}, false
		}
	case vbML:
		// The portfolio header is part of the New-ML surface. Filter through the same current cohort
		// and epoch view as the detailed ML block so legacy losses cannot be presented as if the new
		// book-native-v2 model produced them.
		pf, current := s.currentMLPaperBrief()
		if !current {
			return bookSessionMetrics{}, false
		}
		for _, p := range pf.Open {
			if p.Model == currentMLCohort {
				addOpenUnix(int64(p.Opened))
			}
		}
		for _, c := range pf.Closed {
			if c.Model == currentMLCohort && !bool(c.ResetClose) &&
				time.Unix(int64(c.ClosedTS), 0).After(epoch) {
				add(c.PnL, c.Contracts)
			}
		}
	default:
		return bookSessionMetrics{}, false
	}
	if m.Bets == 0 {
		m.UnitsComplete = false // neither $/bet nor cents/unit has a denominator yet
	}
	return m, ok
}

// briefPortfolioSessionMetrics gives the phone/UI briefing a bounded read with last-good fallback.
// Portfolio accounting remains exact in its durable ledgers; this cache only prevents a transient
// maintenance lock from turning an informational preview into an unbounded request.
func (s *Server) briefPortfolioSessionMetrics(ctx context.Context, book string) (bookSessionMetrics, bool) {
	metrics, ok := s.bookSessionClosedMetrics(ctx, book)
	s.cacheBriefPortfolioMetrics(book, metrics, ok)
	if ok {
		return metrics, true
	}
	if cached, cachedOK, _ := s.cachedBriefPortfolioMetrics(book); cachedOK {
		return cached, true
	}
	return bookSessionMetrics{}, false
}

// bookSessionClosedStats keeps the pre-R133 internal contract for callers that only need the
// session net and number of closed bets. The richer briefing surface uses the metrics helper.
func (s *Server) bookSessionClosedStats(ctx context.Context, book string) (net float64, rounds int, ok bool) {
	m, ok := s.bookSessionClosedMetrics(ctx, book)
	return m.NetUSD, m.Bets, ok
}

// ---- R127 one-time transfer migration ----------------------------------------------------------

const r127MigLatch = "r127_books_v1"

// r127BookRow is one old book's journal entry in data\r127_book_transfer.json.
type r127BookRow struct {
	Name         string  `json:"name"`
	NewBook      string  `json:"new_book"` // the venue book it folds into ("" = folded/none)
	OldBankUSD   float64 `json:"old_bank_usd"`
	LifetimeNet  float64 `json:"lifetime_net_usd"`
	OpenLots     int     `json:"open_lots"`
	OpenExposure float64 `json:"open_exposure_usd"`
	Note         string  `json:"note,omitempty"`
}

// migrateBooksR127 — the one-time kv-latched transfer journal (latch r127_books_v1): every old
// book's {name, old bank, lifetime net, open-lot count, open exposure} into the audit log AND
// data\r127_book_transfer.json. NO money is force-moved: ledgers keep their lots (open lots
// settle in place, counting against the new venue book's exposure); an over-allocated venue book
// simply pauses placement until it settles below the bank. Also journals the promotion-record
// remapping (invert:freshlist → freshinv sub of the Kalshi book; xvgap → sub of the PolyUS book).
func (s *Server) migrateBooksR127(ctx context.Context) {
	if v, ok := s.store.KVGet(ctx, r127MigLatch); ok && v != "" {
		return // latched — the journal is one-time by design
	}
	rows := []r127BookRow{}
	kfRow := func(name, newBook string, bk *kfBook, note string) r127BookRow {
		return r127BookRow{Name: name, NewBook: newBook, OldBankUSD: math.Round(bk.Bank*100) / 100,
			LifetimeNet: math.Round(bk.Net*100) / 100, OpenLots: len(bk.Open),
			OpenExposure: math.Round(kfLotsExposure(bk.Open)*100) / 100, Note: note}
	}
	s.kfBookMu.Lock()
	kb := s.kfLoadLocked()
	rows = append(rows,
		kfRow("kflow-pre", vbKalshi, &kb.Pre, "sub-strategy of the Kalshi book"),
		kfRow("kflow-live", vbKalshi, &kb.Live, "sub-strategy of the Kalshi book"))
	s.kfBookMu.Unlock()
	s.wxBookMu.Lock()
	rows = append(rows, kfRow("weather", vbKalshi, s.wxBookLoadLocked(), "sub-strategy of the Kalshi book"))
	s.wxBookMu.Unlock()
	s.fiBookMu.Lock()
	rows = append(rows, kfRow("freshinv", vbKalshi, s.fiLoadLocked(), "promotion sub of the Kalshi book (invert:freshlist)"))
	s.fiBookMu.Unlock()
	s.xvgBookMu.Lock()
	rows = append(rows, kfRow("xvgap", vbPolyus, s.xvgLoadLocked(), "promotion sub of the PolyUS book"))
	s.xvgBookMu.Unlock()
	s.rfBookMu.Lock()
	rows = append(rows, kfRow("rawflow", "", s.rfLoadLocked(), "FOLDED (alloc 0) — wind-down only, not a venue-book sub"))
	s.rfBookMu.Unlock()
	s.xvcMu.Lock()
	cb := s.xvcLoadLocked()
	cexp := 0.0
	for _, p := range cb.Open {
		cexp += p.Cost*p.Contracts + p.Fee
	}
	rows = append(rows, r127BookRow{Name: "combo-overlay", NewBook: vbCombos, OldBankUSD: xvcBank,
		LifetimeNet: math.Round(cb.Net*100) / 100, OpenLots: len(cb.Open),
		OpenExposure: math.Round(cexp*100) / 100,
		Note:         "lifetime history only; new product-sim and other unverified multi-leg Paper P&L is retired"})
	s.xvcMu.Unlock()
	// ML book (sidecar-owned file; journal-only read).
	var pf struct {
		Bank0 float64 `json:"bank0"`
		Open  []struct {
			Price     float64 `json:"price"`
			Contracts float64 `json:"contracts"`
		} `json:"open"`
		Lifetime struct {
			Net float64 `json:"net"`
		} `json:"lifetime"`
	}
	if s.readJSONLoose(filepath.Join(s.cfg().DataDir, "ml_paper.json"), &pf) {
		mlExp := 0.0
		for _, o := range pf.Open {
			mlExp += o.Price * o.Contracts
		}
		rows = append(rows, r127BookRow{Name: "ml", NewBook: vbML, OldBankUSD: pf.Bank0,
			LifetimeNet: math.Round(pf.Lifetime.Net*100) / 100, OpenLots: len(pf.Open),
			OpenExposure: math.Round(mlExp*100) / 100,
			Note:         "bank moves to $600 via the ml_alloc.json handshake; bank0 re-anchors at the next reset"})
	}
	// Shared auto book, per venue (the SQLite paper_fills ledger).
	if fills, _, _, err := s.paperFillsCached(ctx); err == nil {
		netByVenue := map[string]float64{}
		for _, ss := range paper.Stats(fills).BySource {
			netByVenue[ss.Platform] += ss.Realized - ss.Fees
		}
		positions, _ := paper.Aggregate(fills)
		for _, venue := range []string{"kalshi", "polyus"} {
			exp, n := 0.0, 0
			for _, p := range positions {
				if p.Contracts > 0 && p.Platform == venue {
					exp += p.CostBasis
					n++
				}
			}
			oldBank := s.cfg().Auto.BankrollKalshiUSD
			if venue == "polyus" {
				oldBank = s.cfg().Auto.BankrollPolyUSUSD
			}
			rows = append(rows, r127BookRow{Name: "shared-auto-" + venue, NewBook: venue,
				OldBankUSD: oldBank, LifetimeNet: math.Round(netByVenue[venue]*100) / 100,
				OpenLots: n, OpenExposure: math.Round(exp*100) / 100,
				Note: "old equity-fraction budget (frac × NAV) superseded by the fixed venue-book bank"})
		}
	}
	blob, err := json.MarshalIndent(map[string]any{
		"ts":   time.Now().UTC().Format(time.RFC3339),
		"note": "R127 four-book transfer journal (one-time, kv latch r127_books_v1). No money force-moved: ledgers keep their lots; R143 current operator setting is four $600 paper portfolios.",
		"promotion_migration": []string{
			"invert:freshlist → freshinv runs as a sub-strategy of the Kalshi book (share cap = promotion AllocUSD)",
			"xvgap → runs as a sub-strategy of the PolyUS book (share cap = promotion AllocUSD)",
			"reserve machinery retired: promotion no longer moves reserve money (fields kept for history)",
		},
		"books": rows,
	}, "", " ")
	if err != nil {
		return
	}
	path := filepath.Join(s.cfg().DataDir, "r127_book_transfer.json")
	tmp := path + ".tmp"
	if os.WriteFile(tmp, blob, 0o644) != nil || os.Rename(tmp, path) != nil {
		s.log.Warn("R127 book-transfer journal write failed — migration NOT latched (retries next boot)", "path", path)
		return
	}
	for _, r := range rows {
		_ = s.store.Audit(ctx, "info", "books", fmt.Sprintf(
			"R127 transfer: %s → %s book · old bank $%.2f · lifetime net $%+.2f · %d open lots ($%.2f exposure) — ledger unchanged, lots settle in place",
			r.Name, orDash(r.NewBook), r.OldBankUSD, r.LifetimeNet, r.OpenLots, r.OpenExposure), "")
	}
	_ = s.store.Audit(ctx, "info", "books",
		"R127 four-book restructure latched (r127_books_v1); R143 current banks are four $600 paper portfolios — old books fold in as tagged sub-strategies; promotion records remap to portfolio shares", "")
	_ = s.store.KVSet(ctx, r127MigLatch, time.Now().UTC().Format(time.RFC3339))
}

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

// ---- R127 briefing roster ----------------------------------------------------------------------

// briefUnitTag maps a verdict-engine unit to the compact roster tag (¢ per WHAT — silently
// mixing units would be dishonest, R125 rule).
func briefUnitTag(unit string) string {
	switch unit {
	case "$/contract", "$/combo-ct":
		return "/ct"
	case "$/round":
		return "/bet"
	case "$/quote":
		return "/q"
	case "$/$1":
		return "/$1"
	case "$/lock":
		return "/lock"
	}
	return ""
}

// bookRosterLines — the four-book briefing roster (R127 punch-list A): one line per sub-strategy,
// plain name · realized ¢ per its honest unit · (n) — pulled from the verdict engine; the shared
// auto executor's venue lines are computed from its own closed rounds (all-time). Promotion-
// managed subs carry their pipeline state (promoted / paused (watch) / retiring).
func (s *Server) bookRosterLines(ctx context.Context) map[string][]string {
	verds := s.computeExperimentVerdicts(ctx)
	vmap := map[string]verdictEnt{}
	for _, v := range verds {
		vmap[v.Family] = v
	}
	type autoAgg struct {
		pnl, cts float64
		n        int
	}
	auto := map[string]*autoAgg{"kalshi": {}, "polyus": {}}
	if fills, _, _, err := s.paperFillsCached(ctx); err == nil {
		for _, t := range paper.ClosedTrades(fills) {
			a := auto[t.Platform]
			if a == nil {
				continue
			}
			ctr := t.Contracts
			if ctr < 1 {
				ctr = 1
			}
			a.pnl += t.Realized - t.Fees
			a.cts += ctr
			a.n++
		}
	}
	out := map[string][]string{}
	for _, sub := range s.venueBookSubsAll(verds) { // R130: static mapping + automatic follower lines
		line := ""
		if sub.Family == "" { // the shared auto executor's venue half
			if a := auto[sub.Book]; a != nil && a.n > 0 && a.cts > 0 {
				line = fmt.Sprintf("· %s %+.1f¢/ct (n=%d)", sub.Plain, a.pnl/a.cts*100, a.n)
			} else {
				line = fmt.Sprintf("· %s — collecting (n=0)", sub.Plain)
			}
		} else if v, ok := vmap[sub.Family]; ok && v.N > 0 {
			line = fmt.Sprintf("· %s %+.1f¢%s (n=%d)", sub.Plain, v.Mean*100, briefUnitTag(v.Unit), v.N)
		} else if sub.SeedN > 0 {
			line = fmt.Sprintf("· %s %+.1f¢/ct (n=%d, venue score)", sub.Plain, sub.SeedMean*100, sub.SeedN)
		} else if v, ok := vmap[sub.Seed]; sub.Seed != "" && ok && v.N > 0 {
			// R130: follower line with no settled lots yet — show the family score it rides on
			line = fmt.Sprintf("· %s %+.1f¢%s (n=%d, family score)", sub.Plain, v.Mean*100, briefUnitTag(v.Unit), v.N)
		} else {
			line = fmt.Sprintf("· %s — collecting (n=0)", sub.Plain)
		}
		if sub.Promo != "" {
			switch s.promoRecordState(sub.Promo) {
			case promoStatePromoted, promoStateGrown:
				line += " · promoted"
			case promoStateWatchD4:
				line += " · promoted (weight-run)" // R128: WATCH no longer pauses — migrates next sweep
			case promoStateRetired:
				line += " · retiring"
			}
		}
		// R128: the scoreboard-weight share (the allocation engine's live number, plain %).
		if w, ok := s.rosterWeight(sub.Book, sub.Family); ok {
			line += fmt.Sprintf(" · gets %.0f%% of book", w*100)
		}
		out[sub.Book] = append(out[sub.Book], line)
	}
	return out
}

// rosterWeight — cache-only lookup of one roster line's allocation weight (scoreweight.go).
func (s *Server) rosterWeight(book, family string) (float64, bool) {
	s.swMu.Lock()
	t := s.swTable
	s.swMu.Unlock()
	if t == nil {
		return 0, false
	}
	for _, e := range t.Books[book] {
		if e.Family == family {
			return e.Weight, true
		}
	}
	return 0, false
}

// activePortfolioSystemMetrics projects only authenticated, fill-conditioned exchange-profit
// routes into the two numbers useful on a phone. Paper/model/assumed-fill rows are excluded before
// the coverage mapper runs. Each
// system is weighted by sqrt(distinct settled venue+ticker contracts), so a tiny-sample extreme
// cannot dominate linearly; a missing contract count gets neutral weight 1 rather than borrowing
// repeated route rows.
// The canonical coverage mapper excludes locked venues, ML research, opposite-side diagnostics and
// non-positive cells while retaining both generic followers and named existing executors.
func activePortfolioSystemMetrics(verdicts []verdictEnt) (active int, avgFeeNetCents float64) {
	weightedTotal, weightTotal := 0.0, 0.0
	authenticated := make([]verdictEnt, 0, len(verdicts))
	for _, verdict := range verdicts {
		if verdictHasAuthenticatedProfitEvidence(verdict) {
			authenticated = append(authenticated, verdict)
		}
	}
	for _, row := range gfPaperExecutionCoverage(authenticated) {
		if row.Executor == "" {
			continue
		}
		weight := 1.0
		if row.Markets > 0 {
			weight = math.Sqrt(float64(row.Markets))
		}
		active++
		weightedTotal += row.CurrentCents * weight
		weightTotal += weight
	}
	if weightTotal > 0 {
		avgFeeNetCents = weightedTotal / weightTotal
	}
	return active, avgFeeNetCents
}

// activePortfolioSystemsLine is cache-only apart from the persisted verdict fallback. The regular
// verdict sweep owns heavyweight refresh work; a briefing request must never rebuild the roster.
func (s *Server) activePortfolioSystemsLine() string {
	verdicts, _, _ := s.leaderboardVerdictSnapshot()
	active, avg := activePortfolioSystemMetrics(verdicts)
	if active == 0 {
		return "🚀 Portfolio systems · 0 positive authenticated exchange-profit routes · avg n/a"
	}
	return fmt.Sprintf("🚀 Portfolio systems · %d positive authenticated exchange-profit routes · contract-sample-weighted avg %+.1f¢/share fee-net", active, avg)
}

// historicalPaperAllocationState is a display/migration reader only. R165 removed its last
// execution consumers because those persisted states were derived from assumed-fill history.
func (s *Server) historicalPaperAllocationState(family string) string {
	s.promoMu.Lock()
	defer s.promoMu.Unlock()
	if record := s.promoLoadLocked().Records[family]; record != nil {
		return record.State
	}
	return ""
}
