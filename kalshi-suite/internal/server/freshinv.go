package server

// R117 FRESHINV BOOK — the freshlist-fade paper book, PROMOTION #1 of the auto-promotion
// pipeline (promotion.go). Strategy under test: fade early moves on FRESHLY-LISTED markets —
// the inverted side of the freshlist family (invert:freshlist, PROVEN+ at +9.4¢/ct when built).
//
//   - Detection is EXACTLY the freshlist family's: Kalshi via the existing sweepR27Discovery
//     freshlist block (WS listing → REST-verified open_time ≤45m, book px 0.03–0.97); PolyUS via
//     a light first-seen detector over the CACHED PolyUS snapshot (zero new API load).
//   - Placement: NO at 1−px (the fade). Entry screens: freshlist's own 0.03–0.97 band on the YES
//     price PLUS entry (fade side) in [0.05, 0.95] and a sane spread (≤10¢ — rawflow carries no
//     spread screen, so this is the cheapest kalSigMeta-based mimic).
//   - Sizing: the SHARED Kelly engine (engineStake) on THIS book's own equity, family key
//     "auto-cons-freshinv", kedge "realized-only".
//   - NO ML GATE (by design): this is a raw-strategy book — the point is to measure the fade
//     edge PURE, uncontaminated by the ML pipeline. Safety rails still apply (one open lot per
//     market, betConflictReason cross-book hedge guards, capital reservation, maker sim).
//   - Bank is set by the PROMOTION PIPELINE (reserve accounting in promotion_state.json), never
//     by allocFracs — the book only places while its promotion record is PROMOTED/GROWN.
//   - Settlement mirrors settleRawFlowBook: resolution first (ResolvedYes off signal_log — the
//     detector logs freshlist/freshfade rows on BOTH venues so the authority exists), then the
//     frozen ratio stop against the cached venue mark. Settled lots feed the book's OWN verdict
//     ("book:freshinv" in computeExperimentVerdicts) — the pipeline's retire rule reads it.
//
// NEVER real money. LIVE arming stays operator-only FOREVER (see promotion.go header).

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

	"github.com/kalshi-suite/kalshi-suite/internal/signal"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	fiListBandLo = 0.03 // freshlist's own detection band (YES price)
	fiListBandHi = 0.97
	fiEntryLo    = 0.05 // entry band on the FADE (NO) side
	fiEntryHi    = 0.95
	fiSpreadCapC = 10.0 // sane-spread screen, cents (cheap kalSigMeta mimic — rawflow has none)
)

// fiEntrySide — R118 (pure, pinned in r118_invert_test.go): the executor's SIDE SELECTION for the
// invert:freshlist fade. Every shape freshlist detects is a single-slug binary instrument quoted
// as a YES price (plain YES/NO markets, above/below ladder strikes on Kalshi, and — per R106
// bug-255 flip semantics — two-sided winner/advance instruments, where the short leg is always
// expressed as side NO at 1−yes, never a synthetic YES). The inverted entry is therefore always
// NO at 1−yesPx, gated by the detection band, the fade-side entry band and the spread screen.
func fiEntrySide(yesPx, spreadC float64) (side string, price float64, ok bool) {
	if yesPx <= fiListBandLo || yesPx >= fiListBandHi {
		return "", 0, false // freshlist's own band — no real book yet
	}
	side, price = "NO", 1-yesPx // the fade side's entry
	if price < fiEntryLo || price > fiEntryHi {
		return "", 0, false
	}
	if spreadC > fiSpreadCapC {
		return "", 0, false // spread screen: a fresh book quoting >10¢ wide isn't a price, it's a guess
	}
	return side, price, true
}

// fiEntrySideFor applies the venue-specific edge found in R132: Kalshi fresh listings are faded,
// while PolyUS fresh listings are followed. Pooling the two venues had bet the profitable PUS
// cohort backwards.
func fiEntrySideFor(platform string, yesPx, spreadC float64) (side string, price float64, ok bool) {
	if !strings.EqualFold(platform, "polyus") {
		return fiEntrySide(yesPx, spreadC)
	}
	if yesPx <= fiListBandLo || yesPx >= fiListBandHi || yesPx < fiEntryLo || yesPx > fiEntryHi || spreadC > fiSpreadCapC {
		return "", 0, false
	}
	return "YES", yesPx, true
}

func fiVenueBook(platform string) (book, roster string) {
	if strings.EqualFold(platform, "polyus") {
		return vbPolyus, "book:freshlist-p"
	}
	return vbKalshi, "book:freshinv-k"
}

func (s *Server) fiVenueActive(platform string) bool {
	if strings.EqualFold(platform, "polyus") {
		return s.genfollowOn() // permanent research/execution lane; allocation weight controls size
	}
	return s.freshInvActive()
}

func (s *Server) fiVenueShare(platform string) float64 {
	_, roster := fiVenueBook(platform)
	if share, ok := s.subShareUSD(roster); ok {
		return share
	}
	if strings.EqualFold(platform, "kalshi") {
		return s.promoShareCap("invert:freshlist")
	}
	return 0 // cold allocation table: wait for same-venue evidence/weight rather than borrow K money
}

func fiLotPlatform(p kfPos) string {
	if p.Platform == "" {
		return "kalshi"
	}
	return strings.ToLower(p.Platform)
}

// fiLotFamily is the exact economic identity of a FreshInv lot.  The specialist ledger is shared
// only as storage: Kalshi expresses the independently quoted inverse `invert:freshlist` on NO,
// while PolyUS expresses direct `freshlist` on YES.  In particular, PolyUS
// `invert:freshfade` is a different generic-follower system and must never inherit this lot.
func fiLotFamily(p kfPos) string {
	if family := strings.ToLower(strings.TrimSpace(lotFamilyTag(p, ""))); family != "" {
		return family
	}
	if fiLotPlatform(p) == "polyus" {
		return "freshlist"
	}
	return "invert:freshlist"
}

func fiLotHasExactFamily(p kfPos) bool {
	return strings.Contains(strings.ToLower(p.RouteReason), "fam:")
}

// prepareFreshInvSignalReceipt stores the complete decision-time book as the exact system row
// before the JSON lot can become durable.  It intentionally calls storage directly: invoking the
// broad signal hook here would recursively trigger a second strategy decision.
func (s *Server) prepareFreshInvSignalReceipt(ctx context.Context, sig storage.Signal, p kfPos) error {
	family := fiLotFamily(p)
	sig.Platform = fiLotPlatform(p)
	sig.Ticker, sig.Title = p.Ticker, p.Title
	sig.Side, sig.SignalType = strings.ToUpper(p.Side), family
	sig.EntryPrice = p.Price
	feePC := 0.0
	if p.Contracts > 0 {
		feePC = p.Fee / p.Contracts
	}
	sig.FeePC = &feePC
	sig.ExecExpr = fmt.Sprintf("dedicated-freshinv-system/family=%s/side=%s/route=%s/book=%s",
		family, sig.Side, p.FillKind, sig.BookSource)
	if sig.PricingVersion == "" {
		return fmt.Errorf("freshinv exact receipt missing executable pricing version")
	}
	inserted, err := s.store.InsertSignalResult(ctx, sig)
	if err != nil {
		return fmt.Errorf("store freshinv exact signal receipt: %w", err)
	}
	if !inserted {
		refreshed, refreshErr := s.store.RefreshSignalFamilyExecutionReceipt(ctx, sig)
		if refreshErr != nil {
			return fmt.Errorf("refresh freshinv same-slot execution receipt: %w", refreshErr)
		}
		if !refreshed {
			return fmt.Errorf("freshinv same-slot receipt was not an exact unresolved unfilled family row")
		}
	}
	already, err := s.store.SignalFamilyFillMatches(ctx, sig.Platform, sig.Ticker, sig.Side, family, p.Price)
	if err != nil {
		return fmt.Errorf("check freshinv exact fill receipt: %w", err)
	}
	if already {
		return fmt.Errorf("freshinv exact family already carries a fill")
	}
	return nil
}

// stampFreshInvSignalFill completes the durable exact-system join after the lot file is safely on
// disk.  A zero-row update is a hard attribution failure, not a successful best-effort write.
func (s *Server) stampFreshInvSignalFill(ctx context.Context, p kfPos, includeResolved bool) error {
	feePC := 0.0
	if p.Contracts > 0 {
		feePC = p.Fee / p.Contracts
	}
	updated, err := s.store.UpdateSignalFamilyExecutionFill(ctx, fiLotPlatform(p), p.Ticker,
		p.Side, fiLotFamily(p), p.Price, feePC, p.SignalTS, includeResolved)
	if err != nil {
		return fmt.Errorf("stamp freshinv exact family fill: %w", err)
	}
	if !updated {
		return fmt.Errorf("stamp freshinv exact family fill: no matching unfilled row")
	}
	return nil
}

// ensureFreshInvLotAttribution repairs the only cross-store crash window: the JSON lot may have
// reached disk just before its signal-row fill stamp. New lots always have an explicit fam: tag;
// legacy lots remain settleable but are not rewritten as if they carried modern route proof.
func (s *Server) ensureFreshInvLotAttribution(ctx context.Context, p kfPos) error {
	if !fiLotHasExactFamily(p) {
		return nil
	}
	ok, err := s.store.SignalFamilyFillMatchesAt(ctx, fiLotPlatform(p), p.Ticker, p.Side,
		fiLotFamily(p), p.Price, p.SignalTS, true)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	return s.stampFreshInvSignalFill(ctx, p, true)
}

func (s *Server) rollbackFreshInvLot(p kfPos) error {
	s.fiBookMu.Lock()
	b := s.fiLoadLocked()
	for i := len(b.Open) - 1; i >= 0; i-- {
		candidate := b.Open[i]
		if candidate.TS == p.TS && candidate.Ticker == p.Ticker &&
			strings.EqualFold(candidate.Side, p.Side) && fiLotPlatform(candidate) == fiLotPlatform(p) &&
			fiLotFamily(candidate) == fiLotFamily(p) && math.Abs(candidate.Price-p.Price) < 1e-9 &&
			math.Abs(candidate.Contracts-p.Contracts) < 1e-9 {
			b.Open = append(b.Open[:i], b.Open[i+1:]...)
			s.fiBookDirty = true
			break
		}
	}
	s.fiBookMu.Unlock()
	return s.freshInvFlushChecked()
}

func fiExposure(open []kfPos, platform string) (usd float64, lots int) {
	for _, p := range open {
		if fiLotPlatform(p) == strings.ToLower(platform) {
			usd += p.Contracts*p.Price + p.Fee
			lots++
		}
	}
	return
}

// fiSettlePnL — R118 (pure, pinned in r118_invert_test.go): grade one lot at resolution. yv is
// the market's YES settle value (0, 1, or 0.5 on a half-settle). The side flip is applied HERE
// and only here (payout = 1−yv on the NO/DOWN side) — entry already carried the price flip, so
// settle-flip and price-flip each happen exactly once (no double negation).
func fiSettlePnL(side string, yv, price, contracts, fee float64) (payout, pnl float64, won bool) {
	payout = yv
	if strings.EqualFold(side, "NO") || strings.EqualFold(side, "DOWN") {
		payout = 1 - yv
	}
	pnl = contracts*(payout-price) - fee
	return payout, pnl, payout > price
}

// freshInvActive — R165 hard retirement for NEW Paper lots. Persisted WATCH/PROMOTED/GROWN labels
// came from assumed-fill history and cannot activate an executor. Detection keeps logging and
// freshInvWindingDown continues to settle existing lots.
func (s *Server) freshInvActive() bool {
	return false
}

// freshInvWindingDown — retired/paused with lots still open (or an un-rebased epoch net): keep
// settling + rendering, exactly the rawFlowWindingDown doctrine (bug 268 class).
func (s *Server) freshInvWindingDown() bool {
	s.fiBookMu.Lock()
	defer s.fiBookMu.Unlock()
	b := s.fiLoadLocked()
	return len(b.Open) > 0 || math.Abs(b.Net-b.NetBase) > 0.005
}

// fiLoadLocked lazy-loads freshinv_book.json. Caller holds fiBookMu. Bank seeds at 0 — the
// promotion pipeline sets it on promote/grow (fiSetBank). Same bug-52 poison guard as the twins.
func (s *Server) fiLoadLocked() *kfBook {
	if s.fiBook != nil {
		return s.fiBook
	}
	b := &kfBook{}
	path := filepath.Join(s.cfg().DataDir, "freshinv_book.json")
	if !s.readJSONLoose(path, b) {
		if st, err := os.Stat(path); err == nil && st.Size() > 0 {
			s.fiBookPoisoned.Store(true)
			s.log.Error("freshinv_book.json exists but failed to parse — FreshInv flushes DISABLED this session (bug-52 guard)", "path", path, "size", st.Size())
			_ = s.store.Audit(context.Background(), "error", "freshinv", "FreshInv book file unreadable — flushes disabled this session; inspect data\\freshinv_book.json", "")
		}
	}
	s.fiBook = b
	return b
}

// fiSetBank — the promotion pipeline's ONLY money hook: sets the book's bank to its current
// allocation (promote seeds it, grow doubles it, wind-down refund zeroes it).
func (s *Server) fiSetBank(v float64) {
	s.fiBookMu.Lock()
	b := s.fiLoadLocked()
	b.Bank = math.Round(v*100) / 100
	s.fiBookDirty = true
	s.fiBookMu.Unlock()
	s.freshInvFlush()
}

// fiBookBankNet returns (bank, net since epoch, settled lifetime rows, open lots) — the promotion
// sweep's book-stats read (grow checkpoints, retire wind-down refund).
func (s *Server) fiBookBankNet() (bank, netSince float64, settledN, openN int) {
	s.fiBookMu.Lock()
	defer s.fiBookMu.Unlock()
	b := s.fiLoadLocked()
	return b.Bank, b.Net - b.NetBase, b.Wins + b.Losses, len(b.Open)
}

// freshInvPlace is retained only as the historical call-site seam. R166 retired direct FreshInv
// entries on both venues: insertSignal already sends the exact freshlist/invert:freshlist identity
// to the shared delayed two-complete-book worker. Enqueuing again here would duplicate that same
// opportunity, while running the legacy body would create an immediate assumed Paper fill.
func (s *Server) freshInvPlace(ctx context.Context, platform, ticker, title string, yesPx, spreadC, resolveHours float64) {
	if r166LegacyPaperNewEntriesRetired {
		return
	}
	if s.fiBookPoisoned.Load() || !s.fiVenueActive(platform) || s.ksBlocked() {
		return
	}
	if !s.paperEntryHorizonOK(resolveHours, ticker, title) {
		return
	}
	// R128 timeout-inventory fix: fail closed on provably-dead feeds (the venue books' R126 gate;
	// this path only had the 3¢ chase guard). Self-heals with the R108 watchdogs.
	if s.pxOfflineReason(platform, ticker) != "" {
		return
	}
	side, price, entryOK := fiEntrySideFor(platform, yesPx, spreadC)
	if !entryOK {
		return
	}
	// Discovery may begin from a visible YES percentage, but money always uses a complete,
	// side-specific executable receipt. This is especially important for the Kalshi NO fade: the
	// entry is the actual NO ask/depth/fee, never the theoretical `1-yesPx` complement.
	routeFamily := "invert:freshlist"
	if strings.EqualFold(platform, "polyus") {
		routeFamily = "freshlist"
	}
	bookSignal, feeSource, bookOK := s.currentCompleteBookSignal(storage.Signal{
		Platform: strings.ToLower(strings.TrimSpace(platform)), Ticker: ticker, Title: title, Side: side,
		SignalType:   routeFamily,
		ResolveHours: resolveHours,
	})
	if !bookOK || bookSignal.EntryPrice > price+.03 || bookSignal.EntryPrice < fiEntryLo ||
		bookSignal.EntryPrice > fiEntryHi || bookSignal.BookDepth < 1 {
		return
	}
	price = bookSignal.EntryPrice
	// SAFETY (not part of the experiment): the shared cross-book conflict guard.
	if r := s.betConflictReason(platform, ticker, side, "auto-cons-freshinv", nil); r != "" {
		return
	}
	// R127 FOUR-BOOK GATE: freshinv is a promotion SUB-STRATEGY of the KALSHI venue book (both
	// venues' lots count there — the operator's mapping). Two rails, both computed OUTSIDE
	// fiBookMu (bookAvailableUSD takes each sub's own mutex):
	//   venueAvail — the Kalshi book's bank − Σ ALL subs' open exposure;
	//   subCap     — this sub's promotion share (AllocUSD; GROW raises it; default $100).
	// The old b.Bank field is journal-only now.
	venueBook, _ := fiVenueBook(platform)
	venueAvail := s.bookAvailableUSD(ctx, venueBook)
	subCap := s.fiVenueShare(platform)
	s.fiBookMu.Lock()
	subHeld := 0.0
	{
		b := s.fiLoadLocked()
		for _, p := range b.Open {
			pplat := p.Platform
			if pplat == "" {
				pplat = "kalshi"
			}
			if p.Ticker == ticker && strings.EqualFold(pplat, platform) {
				s.fiBookMu.Unlock()
				return // one open lot per MARKET (either side)
			}
		}
		nVenue := 0
		for _, p := range b.Open {
			pplat := p.Platform
			if pplat == "" {
				pplat = "kalshi"
			}
			if strings.EqualFold(pplat, platform) {
				nVenue++
			}
		}
		if nVenue >= 100 {
			s.fiBookMu.Unlock()
			return // runaway guard (rawflow's bound)
		}
		for _, p := range b.Open {
			pplat := p.Platform
			if pplat == "" {
				pplat = "kalshi"
			}
			if strings.EqualFold(pplat, platform) {
				subHeld += kfLotsExposure([]kfPos{p})
			}
		}
	}
	s.fiBookMu.Unlock()
	sizingEquity := math.Min(subCap, s.bookSizingEquityUSD(ctx, venueBook))
	if sizingEquity <= 0 || venueAvail <= 0 || subHeld >= subCap {
		return
	}
	// SHARED ENGINE on the sub's share cap; realized-only (no ML fallback, ever — rawflow rule).
	stake := s.engineStake(ctx, platform, ticker, side, "auto-cons-freshinv", price, 0, sizingEquity, false, "realized-only")
	contracts := math.Floor(stake / price)
	contracts = executableContracts(contracts, bookSignal.BookDepth)
	if contracts < 1 {
		return
	}
	// Frozen ratio stop, exactly the RawFlow rule: SL = p − r·p, r frozen NOW ("ride"/r≤0 ⇒ none).
	s.autoMu.Lock()
	slRatio, slMode := s.autoSLTPRatio, s.autoSLTPMode
	s.autoMu.Unlock()
	sl := 0.0
	if slMode != "ride" && slRatio > 0 {
		sl = math.Max(0.01, price-slRatio*price)
	}
	entryPx := price
	fee, sizedFeeSource, feeKnown := s.fillFeeReceipt(platform, ticker, false, contracts, entryPx)
	if !feeKnown || strings.TrimSpace(sizedFeeSource) == "" {
		return
	}
	feeSource = sizedFeeSource
	fillKind, fillRule := "taker", "complete-book-touch"
	cost := contracts*entryPx + fee
	nowTS := time.Now().UTC().Format(time.RFC3339)
	lot := kfPos{TS: nowTS, Ticker: ticker,
		Title: title, Side: side, Price: entryPx, Contracts: contracts, Fee: fee, SL: sl,
		FillKind: fillKind, FillRule: fillRule, Platform: strings.ToLower(platform), FeeKnown: true, FeeSource: feeSource,
		RouteReason: fmt.Sprintf("fam:%s origin=strategy route=taker book=%s ask=%.6f bid=%.6f depth=%.4f fee=%.6f fee_source=%s",
			routeFamily, bookSignal.BookSource, entryPx, *bookSignal.BookBid, bookSignal.BookDepth, fee, feeSource),
		SignalTS: nowTS, DecisionTS: nowTS, FillTS: nowTS}
	// Store the independently named, complete-book decision row before committing the file-backed
	// lot. A later cap race may leave an unfilled observation, but never an unattributed fill.
	if err := s.prepareFreshInvSignalReceipt(ctx, bookSignal, lot); err != nil {
		_ = s.store.Audit(ctx, "error", "freshinv", "FreshInv exact signal receipt rejected placement", err.Error())
		return
	}
	s.fiBookMu.Lock()
	b := s.fiLoadLocked()
	held, _ := fiExposure(b.Open, platform)
	// R127: sub share cap + venue-book available replace the old per-book Bank check.
	if held+cost > subCap || cost > venueAvail {
		s.fiBookMu.Unlock()
		return // fully invested (sub share or venue book) — skip rather than margin a paper experiment
	}
	for _, p := range b.Open { // re-check under the lock (sizing window)
		if p.Ticker == ticker && fiLotPlatform(p) == strings.ToLower(platform) {
			s.fiBookMu.Unlock()
			return
		}
	}
	// R120 provenance: instant-taker divert path — signal/decision/fill share one stamp.
	if !s.stampFundedKFRelation(ctx, &lot, routeFamily, "taker") {
		s.fiBookMu.Unlock()
		return
	}
	b.Open = append(b.Open, lot)
	s.fiBookDirty = true
	s.fiBookMu.Unlock()
	if err := s.freshInvFlushChecked(); err != nil {
		if rollbackErr := s.rollbackFreshInvLot(lot); rollbackErr != nil {
			s.fiBookPoisoned.Store(true)
		}
		_ = s.store.Audit(context.WithoutCancel(ctx), "error", "freshinv", "FreshInv placement ledger failed; lot rolled back", err.Error())
		return
	}
	if err := s.stampFreshInvSignalFill(ctx, lot, false); err != nil {
		if rollbackErr := s.rollbackFreshInvLot(lot); rollbackErr != nil {
			s.fiBookPoisoned.Store(true)
			_ = s.store.Audit(context.WithoutCancel(ctx), "error", "freshinv",
				"FreshInv attribution and rollback persistence both failed; executor poisoned", rollbackErr.Error())
		}
		_ = s.store.Audit(context.WithoutCancel(ctx), "error", "freshinv",
			"FreshInv exact family fill failed; lot rolled back", err.Error())
		return
	}
	_ = s.store.Audit(ctx, "info", "freshinv",
		fmt.Sprintf("FreshInv OPEN %s %s %s ×%.0f @ %.0f¢ ($%.2f, fee $%.2f, sl %.0f¢) fill_kind=%s fill_rule=%s", platform, ticker, strings.ToUpper(side), contracts, entryPx*100, cost, fee, sl*100, fillKind, fillRule), "")
	s.enqueueLiveMirrorCandidate(liveMirrorCandidate{
		Platform: platform, Ticker: ticker, Title: title, Side: side, Source: "freshinv",
		Price: entryPx, At: time.Now(), Inverted: !strings.EqualFold(platform, "polyus"),
	})
}

// fiMakerFill books a filled FreshInv post into the book (sweepPendingMakers "freshinv" route).
// Mirrors rfMakerFill; platform-aware.
func (s *Server) fiMakerFill(ctx context.Context, p *pendingMaker, cur float64) string {
	if !s.paperEntryHorizonNow(ctx, p.platform, p.ticker, p.title) {
		return "entry-horizon"
	}
	if r := s.betConflictReason(p.platform, p.ticker, p.side, "auto-cons-freshinv", nil); r != "" {
		return "conflict"
	}
	fee := s.pureMakerFee(p.platform, p.ticker, p.contracts, p.px)
	cost := p.contracts * p.px
	// R127: venue-book available + sub share cap (computed BEFORE fiBookMu — lock order).
	venueBook, _ := fiVenueBook(p.platform)
	// The portfolio wall includes this resting post. Release only its own reservation for the
	// pending->filled conversion; all other open/pending exposure remains subtracted.
	venueAvail := s.bookAvailableUSD(ctx, venueBook) + cost + fee
	subCap := s.fiVenueShare(p.platform)
	s.fiBookMu.Lock()
	b := s.fiLoadLocked()
	for _, o := range b.Open {
		if o.Ticker == p.ticker && fiLotPlatform(o) == strings.ToLower(p.platform) {
			s.fiBookMu.Unlock()
			return "conflict"
		}
	}
	held, _ := fiExposure(b.Open, p.platform)
	if held+cost > subCap || cost > venueAvail {
		s.fiBookMu.Unlock()
		return "capital"
	}
	// R120 provenance: maker fill — signal/decision/post were the post's creation pass (postedAt);
	// the fill is NOW. The post→fill gap is the honest maker latency.
	postTS := p.postedAt.UTC().Format(time.RFC3339)
	lot := kfPos{TS: time.Now().UTC().Format(time.RFC3339), Ticker: p.ticker,
		Title: p.title, Side: p.side, Price: p.px, Contracts: p.contracts, Fee: fee, SL: p.sl,
		FillKind: "maker", FillRule: pendingFillRule(p), Platform: p.platform, RouteReason: p.routeTag,
		SignalTS: postTS, DecisionTS: postTS, PostTS: postTS,
		FillTS: time.Now().UTC().Format(time.RFC3339), RelationReceiptID: p.relationReceiptID}
	s.stampKFExactFee(&lot)
	b.Open = append(b.Open, lot)
	s.fiBookDirty = true
	s.fiBookMu.Unlock()
	s.freshInvFlush()
	_ = s.store.Audit(ctx, "info", "freshinv",
		fmt.Sprintf("FreshInv MAKER FILL %s %s %s ×%.0f @ %.0f¢ (mark %.0f¢, fee $%.2f) fill_kind=maker fill_rule=%s", p.platform, p.ticker, strings.ToUpper(p.side), p.contracts, p.px*100, cur*100, fee, pendingFillRule(p)), "")
	return ""
}

// fiSideMark returns the CURRENT side price of an open lot from the zero-API caches only.
func (s *Server) fiSideMark(p kfPos) (float64, bool) {
	plat := p.Platform
	if plat == "" {
		plat = "kalshi"
	}
	var yes float64
	var ok bool
	if plat == "polyus" {
		yes, ok = s.curPolyUSYes(p.Ticker)
	} else {
		yes, ok = s.cachedYesMark(p.Ticker)
	}
	if !ok || yes <= 0 || yes >= 1 {
		return 0, false
	}
	if strings.EqualFold(p.Side, "NO") || strings.EqualFold(p.Side, "DOWN") {
		return 1 - yes, true
	}
	return yes, true
}

// settleFreshInvBook grades the book on the normal settlement tick: resolution first (the same
// signal_log authority — the detector logs freshlist rows on both venues), then the frozen
// stop-loss against the cached venue mark (taker exit fee). Runs beside settleRawFlowBook.
func (s *Server) settleFreshInvBook(ctx context.Context) {
	if s.fiBookPoisoned.Load() {
		return
	}
	if !s.fiVenueActive("kalshi") && !s.fiVenueActive("polyus") && !s.freshInvWindingDown() {
		return
	}
	s.fiBookMu.Lock()
	b := s.fiLoadLocked()
	if len(b.Open) == 0 {
		s.fiBookMu.Unlock()
		return
	}
	now := time.Now()
	still := b.Open[:0:0]
	changed := false
	var closedNotes []string
	type closeReceipt struct {
		Family, Platform, Ticker, Side, Reason string
		Payout, PnL, CLV                       float64
		CLVKnown                               bool
	}
	var closes []closeReceipt
	var relationOutcomes []fundedKFOutcome
	var attributionErrors []string
	for _, p := range b.Open {
		if err := s.ensureFreshInvLotAttribution(ctx, p); err != nil {
			// A modern lot may not close or emit marks under an ambiguous family. Keeping it open is
			// safer than silently lending its economics to freshfade or invert:freshfade.
			attributionErrors = append(attributionErrors, fmt.Sprintf("%s %s: %v", fiLotPlatform(p), p.Ticker, err))
			still = append(still, p)
			continue
		}
		// 1) RESOLUTION — the terminal truth wins over any stop.
		if yv, res := s.store.ResolvedYesForVenue(ctx, fiLotPlatform(p), p.Ticker); res {
			payout, pnl, won := fiSettlePnL(p.Side, yv, p.Price, p.Contracts, p.Fee)
			receipt := closeReceipt{Family: fiLotFamily(p), Platform: fiLotPlatform(p), Ticker: p.Ticker,
				Side: strings.ToUpper(p.Side), Reason: "settled", Payout: payout, PnL: pnl}
			if len(p.Marks) > 0 {
				receipt.CLV, receipt.CLVKnown = p.Marks[len(p.Marks)-1].Px-p.Price, true
			}
			closes = append(closes, receipt)
			kfMarksClose(&p, payout, "settle") // R120: terminal trajectory mark
			b.Net += pnl
			if won {
				b.Wins++
			} else {
				b.Losses++
			}
			b.Closed = append(b.Closed, kfClosed{kfPos: p, Payout: payout,
				PnL: math.Round(pnl*100) / 100, Won: won, SettledTS: now.UTC().Format(time.RFC3339), Reason: "settled"})
			relationOutcomes = append(relationOutcomes, fundedKFOutcome{Position: p, PnL: pnl, Source: "freshinv-settlement"})
			closedNotes = append(closedNotes, fmt.Sprintf("%s %s %s settled %+0.2f", fiLotFamily(p), fiLotPlatform(p), p.Ticker, pnl))
			s.fiBookDirty, changed = true, true
			continue
		}
		// 2) STOP-LOSS — frozen level vs the cached live mark (side price). No mark = no action.
		// R120 provenance: the cached venue mark stamps every open lot's trajectory.
		if sidePx, ok := s.fiSideMark(p); ok {
			nMk := len(p.Marks)
			kfStampMark(&p, sidePx, "mark")
			if len(p.Marks) != nMk {
				updated, pathErr := s.store.AppendSignalFamilyPostPathAtResult(ctx, fiLotPlatform(p),
					p.Ticker, p.Side, fiLotFamily(p), sidePx, p.SignalTS)
				if pathErr != nil || (!updated && fiLotHasExactFamily(p)) {
					attributionErrors = append(attributionErrors, fmt.Sprintf("%s %s %s post-path: updated=%v err=%v",
						fiLotPlatform(p), p.Ticker, fiLotFamily(p), updated, pathErr))
				}
				s.fiBookDirty = true
			}
		}
		if p.SL > 0 {
			if sidePx, ok := s.fiSideMark(p); ok && sidePx <= p.SL {
				kfMarksClose(&p, sidePx, "stop")
				plat := p.Platform
				if plat == "" {
					plat = "kalshi"
				}
				exitFee := s.blendedFeeAction(plat, p.Ticker, "", false, p.Contracts, sidePx, false) // taker exit
				pnl := p.Contracts*(sidePx-p.Price) - p.Fee - exitFee
				closes = append(closes, closeReceipt{Family: fiLotFamily(p), Platform: fiLotPlatform(p),
					Ticker: p.Ticker, Side: strings.ToUpper(p.Side), Reason: "stop-loss", Payout: sidePx,
					PnL: pnl, CLV: sidePx - p.Price, CLVKnown: true})
				b.Net += pnl
				b.Losses++ // a stop exit below entry is a loss by construction
				b.Closed = append(b.Closed, kfClosed{kfPos: p, Payout: sidePx,
					PnL: math.Round(pnl*100) / 100, Won: false, SettledTS: now.UTC().Format(time.RFC3339), Reason: "stop-loss"})
				relationOutcomes = append(relationOutcomes, fundedKFOutcome{Position: p, PnL: pnl, Source: "freshinv-stop-loss"})
				closedNotes = append(closedNotes, fmt.Sprintf("%s %s %s stop-loss %+0.2f", fiLotFamily(p), fiLotPlatform(p), p.Ticker, pnl))
				s.fiBookDirty, changed = true, true
				continue
			}
		}
		still = append(still, p)
	}
	b.Open = still
	if changed { // only a CLOSE moves the sparkline
		if len(b.Closed) > 300 {
			b.Closed = b.Closed[len(b.Closed)-300:]
		}
		b.Equity = append(b.Equity, [2]float64{float64(now.Unix()), math.Round((b.Net-b.NetBase)*100) / 100})
		if len(b.Equity) > 400 {
			b.Equity = b.Equity[len(b.Equity)-400:]
		}
	}
	s.fiBookMu.Unlock()
	if err := s.freshInvFlushChecked(); err != nil {
		s.fiBookPoisoned.Store(true)
		_ = s.store.Audit(context.WithoutCancel(ctx), "error", "freshinv",
			"FreshInv settlement ledger failed to persist; executor poisoned", err.Error())
		return
	}
	s.settleFundedKFOutcomes(context.WithoutCancel(ctx), relationOutcomes)
	for _, n := range closedNotes {
		_ = s.store.Audit(ctx, "info", "freshinv", "FreshInv CLOSE "+n, "")
	}
	for _, attributionErr := range attributionErrors {
		_ = s.store.Audit(context.WithoutCancel(ctx), "error", "freshinv",
			"FreshInv exact family attribution blocked mark/close", attributionErr)
	}
	for _, receipt := range closes {
		detail := map[string]any{"system": receipt.Family, "venue": receipt.Platform,
			"ticker": receipt.Ticker, "side": receipt.Side, "settlement_result": receipt.Reason,
			"payout": receipt.Payout, "realized_pnl": receipt.PnL, "clv": nil}
		if receipt.CLVKnown {
			detail["clv"] = receipt.CLV
		}
		encoded, _ := json.Marshal(detail)
		category := "system-execution"
		if strings.HasPrefix(receipt.Family, "invert:") {
			category = "inverse-system"
		}
		_ = s.store.Audit(ctx, "info", category, "CLOSED "+receipt.Family+" "+receipt.Ticker, string(encoded))
	}
}

// freshInvFlush persists the book if dirty (atomic tmp+rename; poison-guarded).
func (s *Server) freshInvFlush() {
	defer s.invalidatePortfolioEquityCache()
	s.fiBookMu.Lock()
	defer s.fiBookMu.Unlock()
	if s.fiBookPoisoned.Load() || !s.fiBookDirty || s.fiBook == nil {
		return
	}
	out, err := json.Marshal(s.fiBook)
	if err != nil {
		return
	}
	path := filepath.Join(s.cfg().DataDir, "freshinv_book.json")
	tmp := path + ".tmp"
	if os.WriteFile(tmp, out, 0o644) == nil {
		// R122 (auditor 438): the dirty flag clears ONLY on rename success — a failed rename used
		// to clear it anyway, silently dropping settled state on the next restart.
		if os.Rename(tmp, path) == nil {
			s.fiBookDirty = false
		}
	}
}

// freshInvFlushChecked is the placement form. The historical wrapper above remains for non-money
// state changes, while a new executable lot must receive a real persistence verdict.
func (s *Server) freshInvFlushChecked() error {
	s.fiBookMu.Lock()
	defer s.fiBookMu.Unlock()
	if s.fiBookPoisoned.Load() {
		return fmt.Errorf("freshinv ledger is poisoned")
	}
	if !s.fiBookDirty || s.fiBook == nil {
		return nil
	}
	out, err := json.Marshal(s.fiBook)
	if err != nil {
		return fmt.Errorf("marshal freshinv ledger: %w", err)
	}
	path := filepath.Join(s.cfg().DataDir, "freshinv_book.json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o644); err != nil {
		return fmt.Errorf("write freshinv ledger temp file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace freshinv ledger: %w", err)
	}
	s.fiBookDirty = false
	return nil
}

// resetFreshInvBook starts a flat current epoch. Pre-reset opens move to the durable reset archive;
// the bank stays at the promotion allocation and lifetime closed evidence remains.
func (s *Server) resetFreshInvBook(ctx context.Context) {
	res, err := s.resetFreshInvBookAt(time.Now())
	err = s.finishKFResetResult(ctx, res, err)
	if s.store != nil {
		level := "info"
		msg := fmt.Sprintf("reset P&L: FreshInv started flat; %d prior opens archived", len(res.ArchivedOpen))
		if err != nil {
			level, msg = "error", "reset P&L: FreshInv clean epoch failed: "+err.Error()
		}
		_ = s.store.Audit(context.WithoutCancel(ctx), level, "freshinv", msg, "")
	}
}

// fiBookOpenSide returns the side of an open FreshInv lot for a venue market ("" = none) — the
// symmetric cross-book hedge leg (rfBookOpenSide pattern; platform-aware).
func (s *Server) fiBookOpenSide(platform, ticker string) string {
	if !s.freshInvActive() && !s.freshInvWindingDown() {
		return ""
	}
	s.fiBookMu.Lock()
	defer s.fiBookMu.Unlock()
	b := s.fiLoadLocked()
	for _, p := range b.Open {
		plat := p.Platform
		if plat == "" {
			plat = "kalshi"
		}
		if plat == platform && p.Ticker == ticker {
			return p.Side
		}
	}
	return ""
}

// fiClusterStake — FreshInv leg of the correlated-cluster exposure math (rfClusterStake pattern;
// Kalshi lots only, matching the guard's kalshi-scoped cluster block).
func (s *Server) fiClusterStake(ckey, tkey string) (stake float64, trueDup string) {
	if ckey == "" || (!s.freshInvActive() && !s.freshInvWindingDown()) {
		return 0, ""
	}
	s.fiBookMu.Lock()
	defer s.fiBookMu.Unlock()
	b := s.fiLoadLocked()
	for _, p := range b.Open {
		if p.Platform != "" && p.Platform != "kalshi" {
			continue
		}
		if corrClusterKey("kalshi", p.Ticker) == ckey {
			stake += p.Price * p.Contracts
		}
		if tkey != "" && trueRestatementKey("kalshi", p.Ticker) == tkey {
			trueDup = p.Side
		}
	}
	return stake, trueDup
}

// sweepFreshInvPolyUS — the light PolyUS fresh-listing detector (5-min cadence): first-seen
// tracking over the CACHED PolyUS snapshot (polyUSSnapshot — zero new API calls). A slug first
// seen after the seed pass counts as freshly listed from its first-seen time; within 45 min and
// with a real book (yes 0.03–0.97) it logs freshlist/freshfade rows (platform polyus — the
// settlement authority for the book's PUS lots) and places the fade through freshInvPlace.
func (s *Server) sweepFreshInvPolyUS(ctx context.Context) {
	mkts := s.polyUSSnapshot()
	if len(mkts) == 0 {
		return // cold cache — never seed off an empty snapshot
	}
	now := time.Now()
	type cand struct {
		m     polyUSMarket
		since time.Duration
	}
	var cands []cand
	s.pusFreshMu.Lock()
	if s.pusFreshSeen == nil {
		s.pusFreshSeen = map[string]time.Time{}
	}
	seeded := s.pusFreshSeeded
	for _, m := range mkts {
		if m.Slug == "" {
			continue
		}
		first, known := s.pusFreshSeen[m.Slug]
		if !known {
			s.pusFreshSeen[m.Slug] = now
			first = now
		}
		// Only markets that showed up AFTER the boot seed pass count as fresh listings. TTL note:
		// 7d ≫ the 45-min window, so a prune can't recycle a live market back into "new" while it
		// still matters (a snapshot gap >7d would — accepted, documented risk of a cache-only feed).
		if seeded && now.Sub(first) <= 45*time.Minute {
			cands = append(cands, cand{m, now.Sub(first)})
		}
	}
	pruneTimeMap(s.pusFreshSeen, func(t time.Time) time.Time { return t }, 20000, 7*24*time.Hour)
	s.pusFreshSeeded = true
	s.pusFreshMu.Unlock()
	if !seeded {
		return // seed-only pass — detector state stays warm
	}
	// R122 (Part 3 logging-continuity — the audit's one hard book-state coupling): the
	// freshInvActive() gate is GONE from detection. It silenced the PolyUS half of
	// freshlist/freshfade whenever the book wasn't PROMOTED/GROWN — starving the very evidence
	// stream (FamilyEdgeStats, 90d window) a retired invert:freshlist would need to re-prove
	// itself, while the Kalshi half logged unconditionally. Book state gates PLACEMENT only:
	// freshInvPlace below self-gates on freshInvActive() + ksBlocked().
	logged := 0
	for _, c := range cands {
		m := c.m
		if m.Yes <= fiListBandLo || m.Yes >= fiListBandHi || m.Bid <= 0 || m.Ask <= 0 {
			continue // no real two-sided book yet — re-check next sweep inside the 45-min window
		}
		title := m.Game
		if m.TeamName != "" {
			title = m.TeamName + " — " + m.Game
		}
		if title == "" {
			title = m.Slug
		}
		sc := signal.SpreadCents(m.Bid, m.Ask)
		// Log the freshlist + fade rows ONCE per slug (the family's venue extension AND the
		// settlement authority ResolvedYes reads for the book's PUS lots).
		s.pusFreshMu.Lock()
		if s.pusFreshLogged == nil {
			s.pusFreshLogged = map[string]time.Time{}
		}
		_, already := s.pusFreshLogged[m.Slug]
		if !already && logged < 5 {
			s.pusFreshLogged[m.Slug] = now
			pruneTimeMap(s.pusFreshLogged, func(t time.Time) time.Time { return t }, 4000, 24*time.Hour)
		}
		s.pusFreshMu.Unlock()
		if !already && logged < 5 {
			mins := c.since.Minutes()
			rh := s.sigResolveHours(ctx, "polyus", m.Slug)
			_ = s.insertSignal(ctx, storage.Signal{Platform: "polyus", Ticker: m.Slug, Title: title, Side: "YES", SignalType: "freshlist", EntryPrice: m.Yes, Strength: mins, SpreadCents: sc, ResolveHours: rh, Confidence: s.signalConfidence("freshlist", m.Yes)})
			_ = s.insertSignal(ctx, storage.Signal{Platform: "polyus", Ticker: m.Slug, Title: title, Side: "NO", SignalType: "freshfade", EntryPrice: 1 - m.Yes, Strength: mins, SpreadCents: sc, ResolveHours: rh, Confidence: s.signalConfidence("freshfade", 1-m.Yes)})
			logged++
		}
		rh := s.sigResolveHours(ctx, "polyus", m.Slug)
		s.freshInvPlace(ctx, "polyus", m.Slug, title, m.Yes, sc, rh)
	}
}

// freshInvMLPayload — the /api/ml "freshinv" block behind the ML-tab panel + freshinv-book widget
// (rawFlowMLPayload pattern; platform-aware zero-API marks).
func (s *Server) freshInvMLPayload() map[string]any {
	active, winding := s.freshInvActive(), s.freshInvWindingDown()
	if !active && !winding {
		return map[string]any{"enabled": false}
	}
	s.fiBookMu.Lock()
	b := s.fiLoadLocked()
	netSince, wins, losses := kfCurrentEpochStats(b)
	wr := 0.0
	if n := wins + losses; n > 0 {
		wr = float64(wins) / float64(n)
	}
	openSnap := append([]kfPos(nil), b.Open...)
	bank, netLife := b.Bank, b.Net
	lifeWins, lifeLosses := b.Wins, b.Losses
	closedLife := b.Wins + b.Losses
	eqSeries := append([][2]float64(nil), b.Equity...)
	s.fiBookMu.Unlock()
	openCost := 0.0
	lots := make([]map[string]any, 0, len(openSnap))
	for _, p := range openSnap { // marks OUTSIDE the book lock
		openCost += p.Contracts*p.Price + p.Fee
		plat := p.Platform
		if plat == "" {
			plat = "kalshi"
		}
		lot := map[string]any{"ticker": p.Ticker, "title": p.Title, "side": p.Side,
			"price": p.Price, "contracts": p.Contracts, "platform": plat}
		if ts, err := time.Parse(time.RFC3339, p.TS); err == nil {
			lot["opened"] = ts.Unix()
		}
		if sidePx, ok := s.fiSideMark(p); ok {
			lot["cur_price"] = sidePx
			lot["unrealized"] = math.Round((p.Contracts*(sidePx-p.Price)-p.Fee)*100) / 100
		}
		lots = append(lots, lot)
	}
	return map[string]any{
		"enabled": true, "winding_down": winding && !active,
		"state":      s.promoRecordState("invert:freshlist"),
		"rules":      "venue-split fresh listings: fade on Kalshi (NO at 1−px), follow on PolyUS (YES at px) within 45min of open · yes 3–97¢ + entry 5–95¢ + spread ≤10¢ · same-venue evidence/weight · one lot per market · cross-book hedge blocks · paper only",
		"accounting": "current-reset-epoch",
		"bank":       sanF(math.Round(bank*100) / 100), "equity": sanF(math.Round((bank+netSince)*100) / 100),
		"net": sanF(math.Round(netSince*100) / 100), "closed": wins + losses, "wins": wins,
		"win_rate": sanF(wr), "open": len(openSnap), "open_cost": sanF(math.Round(openCost*100) / 100),
		"open_lots": lots, "equity_series": eqSeries,
		"net_lifetime": sanF(math.Round(netLife*100) / 100), "closed_lifetime": closedLife,
		"wins_lifetime": lifeWins, "losses_lifetime": lifeLosses,
	}
}

// handleFreshInv (GET /api/freshinv) — the book, its rules, promotion state and epoch stats.
func (s *Server) handleFreshInv(w http.ResponseWriter, r *http.Request) {
	if !s.freshInvActive() && !s.freshInvWindingDown() {
		writeJSON(w, http.StatusOK, map[string]any{"enabled": false,
			"note":  "FreshInv runs only while promoted by the auto-promotion pipeline (see /api/promotions)",
			"state": s.promoRecordState("invert:freshlist")})
		return
	}
	out := s.freshInvMLPayload()
	s.fiBookMu.Lock()
	b := s.fiLoadLocked()
	out["open_lots_full"] = append([]kfPos(nil), b.Open...)
	out["closed_tail"] = append([]kfClosed(nil), b.Closed...)
	out["lifetime"] = map[string]any{"net": math.Round(b.Net*100) / 100, "wins": b.Wins, "losses": b.Losses}
	s.fiBookMu.Unlock()
	writeJSON(w, http.StatusOK, out)
}
