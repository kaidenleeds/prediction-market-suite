package server

// R129 CHEAPBAND BOOK — operator order ("count that as a strat and DEF use it"): buy CHEAP +EV
// candidates in the price bands the R126 band study measured as profitable, per venue:
//
//	Kalshi 0–10¢ — +3.75¢/ct (ev_lo +2.36, n=1,416 positive-family rows; tools/r126_bands.py)
//	PolyUS <15¢  — ≈1.0 ROI per $ staked (every sub-15¢ band CI-positive within positive families)
//
// THE CRITICAL CONDITION (the study's own honesty note): raw cheap prices LOSE — kalshi 0-5¢ is
// −1.5¢/ct over ALL rows; the measured edge exists only WITHIN measured-positive families. So a
// candidate qualifies ONLY when its origin family's lifetime verdict is measured-positive
// (cache-only verdict read: signal family, N ≥ 100, mean > 0, not venue-locked; stale cache
// refuses — fail closed). EXCLUDED BY MEASUREMENT, BY NAME (roi-bands loader precedent):
//   - poly-int entirely (platform filter — its cheap bands graded trash even inside winners);
//   - crypto tickers (the <20¢ crypto cell is UNPROVEN, CI spans 0 — out until its own line proves).
//
//   - Candidate source: the insertSignal choke point — every family's detections flow through it,
//     which (a) makes the cohort EXACTLY the one the band study graded and (b) guarantees a
//     signal row exists on every ticker this book holds, so settlement authority is free
//     (signal_log resolutions, the freshinv doctrine).
//   - TAKER entries at the live ask (3¢ never-chase; ask must stay in-band): at sub-15¢ prices
//     the taker fee is <0.9¢/ct and a maker queue at a 1–5¢ level rarely fills — the band study
//     itself was graded at taker fees, so taker keeps the measurement honest.
//   - Sizing: the shared Kelly engine on the scoreboard-weight share of each venue book
//     (source "auto-cons-cheapband", realized-only). TWO roster families — book:cheapband-k
//     (Kalshi book) and book:cheapband-p (PolyUS book) — one ledger file, two graded halves
//     (the kflow pre/live shape), each with its own scoreboard ¢/ct line and venue-book wall.
//   - Lots carry the origin family as RouteReason ("fam:<signal_type>") — the auditor's tag.
//
// NEVER real money. LIVE arming stays operator-only FOREVER.

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

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	cbBandLo     = 0.01 // sub-1¢ has no book to speak of on either venue
	cbKalBandHi  = 0.10 // Kalshi: the measured 0-10¢ cell
	cbPusBandHi  = 0.15 // PolyUS: the measured <15¢ cells
	cbMinFamilyN = 100  // family-positive bar: enough graded rows that "positive" isn't noise
	cbSpreadCapC = 6.0  // sane-spread screen — a 6¢ spread on a ≤15¢ price is not a real book
)

// cbBook — one file, two graded halves (the kflow pre/live shape): K = Kalshi lots, P = PolyUS.
type cbBook struct {
	K kfBook `json:"k"`
	P kfBook `json:"p"`
}

// cheapbandOn — config kill for the book (cheapband_enabled, nil/absent = ON).
func (s *Server) cheapbandOn() bool {
	v := s.cfg().Auto.CheapbandEnabled
	return v == nil || *v
}

// cbLoadLocked lazy-loads cheapband_book.json. Caller holds cbBookMu (bug-52 poison guard).
func (s *Server) cbLoadLocked() *cbBook {
	if s.cbBook != nil {
		return s.cbBook
	}
	b := &cbBook{}
	path := filepath.Join(s.cfg().DataDir, "cheapband_book.json")
	if !s.readJSONLoose(path, b) {
		if st, err := os.Stat(path); err == nil && st.Size() > 0 {
			s.cbBookPoisoned.Store(true)
			s.log.Error("cheapband_book.json exists but failed to parse — Cheapband flushes DISABLED this session (bug-52 guard)", "path", path, "size", st.Size())
			_ = s.store.Audit(context.Background(), "error", "cheapband", "Cheapband book file unreadable — flushes disabled this session; inspect data\\cheapband_book.json", "")
		}
	}
	s.cbBook = b
	return b
}

// cheapbandFlush persists the book if dirty (atomic tmp+rename; poison-guarded; 438 rule).
func (s *Server) cheapbandFlush() {
	defer s.invalidatePortfolioEquityCache()
	s.cbBookMu.Lock()
	defer s.cbBookMu.Unlock()
	if s.cbBookPoisoned.Load() || !s.cbBookDirty || s.cbBook == nil {
		return
	}
	out, err := json.Marshal(s.cbBook)
	if err != nil {
		return
	}
	path := filepath.Join(s.cfg().DataDir, "cheapband_book.json")
	tmp := path + ".tmp"
	if os.WriteFile(tmp, out, 0o644) == nil {
		if os.Rename(tmp, path) == nil {
			s.cbBookDirty = false
		}
	}
}

// cheapbandWindingDown — disabled with lots still open: keep settling (268 doctrine).
func (s *Server) cheapbandWindingDown() bool {
	s.cbBookMu.Lock()
	defer s.cbBookMu.Unlock()
	b := s.cbLoadLocked()
	return len(b.K.Open) > 0 || len(b.P.Open) > 0
}

// cbHalf — the venue's ledger half + book/roster identity. Caller holds cbBookMu for the half.
func cbHalf(b *cbBook, platform string) *kfBook {
	if platform == "polyus" {
		return &b.P
	}
	return &b.K
}

// cbFamilyPositive — cache-only, VENUE-SPECIFIC verdict gate: the origin family must be
// measured-positive on the same venue where the Cheapband order would be placed. Pooling a good
// PolyUS cohort with a losing Kalshi cohort (or vice versa) is not execution evidence.
func (s *Server) cbFamilyPositive(fam, platform string) bool {
	if fam == "" {
		return false
	}
	s.verdMu.Lock()
	cache, at := s.verdCache, s.verdAt
	s.verdMu.Unlock()
	if cache == nil || time.Since(at) > swStale {
		return false
	}
	for _, v := range cache {
		if v.Family == fam {
			vv, ok := v.Venues[platform]
			return v.Group == "signal" && !v.Locked && ok && vv.N >= cbMinFamilyN && vv.Mean > 0
		}
	}
	return false
}

// cbCategoryOK is the evidence-backed R132 deny gate. The signal still lands in signal_log, so
// blocked cells keep researching and can earn their way back; only the money-book expression is
// stopped. These are measured large, confidence-bound-negative cells. Unknown PUS category is the
// measured "generic" cell and therefore fails closed.
func cbCategoryOK(platform, category string) bool {
	c := strings.ToLower(strings.TrimSpace(category))
	switch platform {
	case "kalshi":
		return !strings.Contains(c, "world cup")
	case "polyus":
		if c == "" || c == "generic" || c == "other" || c == "unknown" {
			return false
		}
		return !strings.Contains(c, "soccer") && !strings.Contains(c, "tennis") && !strings.Contains(c, "world cup")
	default:
		return false
	}
}

// cheapbandConsider routes one freshly-logged signal row into the Cheapband book when it sits in
// a measured-profitable cheap band. Called from the insertSignal choke point on EVERY row —
// the gate ladder is ordered cheapest-first and self-refuses almost always.
func (s *Server) cheapbandConsider(ctx context.Context, sig storage.Signal) {
	plat := strings.ToLower(strings.TrimSpace(sig.Platform))
	var bandHi float64
	switch plat {
	case "kalshi":
		bandHi = cbKalBandHi
	case "polyus":
		bandHi = cbPusBandHi
	default:
		return // poly-int ("polymarket") measured trash-cheap — excluded by name (R126)
	}
	if sig.EntryPrice < cbBandLo || sig.EntryPrice >= bandHi || sig.Ticker == "" {
		return
	}
	side := strings.ToUpper(strings.TrimSpace(sig.Side))
	if side != "YES" && side != "NO" {
		return
	}
	// cheapbandConsider is called only after insertSignal has a durable current or duplicate row.
	// Record the exact venue+side feed before allocation/economic gates without implying a fill.
	s.noteR147SignalRuntimeForSignal("cheapband", sig, "*",
		"cheapband:in-band-signal", time.Now(), true, "")
	if !s.paperEntryHorizonOK(sig.ResolveHours, sig.Ticker, sig.Title) {
		return
	}
	if isCryptoTicker(sig.Ticker) {
		return // <20¢ crypto is UNPROVEN (R126: CI −2.85 at n=265) — gated out until its own line proves
	}
	if !cbCategoryOK(plat, sig.Category) {
		return
	}
	if sig.SpreadCents > cbSpreadCapC {
		return
	}
	if r166LegacyPaperNewEntriesRetired {
		s.enqueueR166SpecialistTaker(ctx, sig, "cheapband", r147SignalInputTopology(sig),
			"cheapband:eligible-signal")
		return
	}
	if !s.cheapbandOn() || s.ksBlocked() || s.cbBookPoisoned.Load() {
		return
	}
	if !s.cbFamilyPositive(sig.SignalType, plat) {
		return
	}
	// R128 timeout doctrine: fail closed on provably-dead feeds.
	if s.pxOfflineReason(plat, sig.Ticker) != "" {
		return
	}
	if r := s.betConflictReason(plat, sig.Ticker, side, "auto-cons-cheapband", nil); r != "" {
		return
	}
	book, roster := vbKalshi, "book:cheapband-k"
	if plat == "polyus" {
		book, roster = vbPolyus, "book:cheapband-p"
	}
	// One open lot per market per half + runaway guard — BEFORE any pricing work.
	s.cbBookMu.Lock()
	subHeld := 0.0
	{
		h := cbHalf(s.cbLoadLocked(), plat)
		for _, p := range h.Open {
			if p.Ticker == sig.Ticker {
				s.cbBookMu.Unlock()
				return
			}
		}
		if len(h.Open) >= 100 {
			s.cbBookMu.Unlock()
			return
		}
		subHeld = kfLotsExposure(h.Open)
	}
	s.cbBookMu.Unlock()
	// R128 book gate: venue-book wall + the scoreboard-weight share (never under cbBookMu —
	// bookOpenExposure takes the sub-book mutexes, including ours).
	venueAvail := s.bookAvailableUSD(ctx, book)
	subCap := s.promoShareCap(roster)
	sizingEquity := math.Min(subCap, s.bookSizingEquityUSD(ctx, book))
	if sizingEquity <= 0 || venueAvail <= 0 || subHeld >= subCap {
		return
	}
	// TAKER at the live ask, 3¢ never-chase, band re-checked at the actual entry price.
	ask, depth, _, quoteOK := s.executableAsk(ctx, plat, sig.Ticker, side, true)
	if !quoteOK || ask <= 0 || ask > sig.EntryPrice+0.03 || ask < cbBandLo || ask >= bandHi {
		return
	}
	stake := s.engineStake(ctx, plat, sig.Ticker, side, "auto-cons-cheapband", ask, 0, sizingEquity, false, "realized-only")
	contracts := math.Floor(stake / ask)
	if contracts < 1 {
		contracts = 1
	}
	contracts = executableContracts(contracts, depth)
	if contracts < 1 {
		return
	}
	fee := s.blendedFee(plat, sig.Ticker, "", false, contracts, ask)
	s.autoMu.Lock()
	slRatio, slMode := s.autoSLTPRatio, s.autoSLTPMode
	s.autoMu.Unlock()
	sl := 0.0
	if slMode != "ride" && slRatio > 0 {
		sl = math.Max(0.01, ask-slRatio*ask)
	}
	cost := contracts*ask + fee
	nowTS := time.Now().UTC().Format(time.RFC3339)
	s.cbBookMu.Lock()
	h := cbHalf(s.cbLoadLocked(), plat)
	held := kfLotsExposure(h.Open)
	if held+cost > subCap || cost > venueAvail {
		s.cbBookMu.Unlock()
		return
	}
	for _, p := range h.Open {
		if p.Ticker == sig.Ticker {
			s.cbBookMu.Unlock()
			return
		}
	}
	pos := kfPos{TS: nowTS, Ticker: sig.Ticker,
		Title: sig.Title, Side: side, Price: ask, Contracts: contracts, Fee: fee, SL: sl,
		FillKind: "taker", FillRule: "cheapband", RouteReason: "fam:" + sig.SignalType,
		Platform: plat, SignalTS: nowTS, DecisionTS: nowTS, FillTS: nowTS}
	s.stampKFExactFee(&pos)
	if !s.stampFundedKFRelation(ctx, &pos, "auto-cons-"+sig.SignalType, "taker") {
		s.cbBookMu.Unlock()
		return
	}
	h.Open = append(h.Open, pos)
	s.cbBookDirty = true
	s.cbBookMu.Unlock()
	s.cheapbandFlush() // 454 rule: flush right after placement
	_ = s.store.Audit(ctx, "info", "cheapband",
		fmt.Sprintf("Cheapband OPEN %s %s %s ×%.0f @ %.1f¢ ($%.2f, fee $%.2f, sl %.1f¢) fam=%s", plat, sig.Ticker, side, contracts, ask*100, cost, fee, sl*100, sig.SignalType), "")
	s.enqueueLiveMirrorCandidate(liveMirrorCandidate{
		Platform: plat, Ticker: sig.Ticker, Title: sig.Title, Side: side,
		Source: "auto-cons-cheapband", Family: "cheapband", Price: ask, At: time.Now(),
		InputTopology: r147SignalInputTopology(sig), InputObservedAt: r147SignalInputObservedAt(sig),
	})
}

// settleCheapbandBook — resolutions first (signal_log authority — the origin families logged the
// rows), then the frozen ratio stop against the cached venue mark. Both halves, both venues.
func (s *Server) settleCheapbandBook(ctx context.Context) {
	if s.cbBookPoisoned.Load() {
		return
	}
	if !s.cheapbandOn() && !s.cheapbandWindingDown() {
		return
	}
	now := time.Now()
	var closedNotes []string
	var relationOutcomes []fundedKFOutcome
	s.cbBookMu.Lock()
	b := s.cbLoadLocked()
	for _, h := range []*kfBook{&b.K, &b.P} {
		if len(h.Open) == 0 {
			continue
		}
		still := h.Open[:0:0]
		changed := false
		for _, p := range h.Open {
			if yv, res := s.store.ResolvedYesForVenue(ctx, fiLotPlatform(p), p.Ticker); res {
				payout, pnl, won := fiSettlePnL(p.Side, yv, p.Price, p.Contracts, p.Fee)
				kfMarksClose(&p, payout, "settle")
				h.Net += pnl
				if won {
					h.Wins++
				} else {
					h.Losses++
				}
				h.Closed = append(h.Closed, kfClosed{kfPos: p, Payout: payout,
					PnL: math.Round(pnl*100) / 100, Won: won, SettledTS: now.UTC().Format(time.RFC3339), Reason: "settled"})
				closedNotes = append(closedNotes, fmt.Sprintf("%s settled %+0.2f", p.Ticker, pnl))
				relationOutcomes = append(relationOutcomes, fundedKFOutcome{p, pnl, "cheapband-settlement"})
				s.cbBookDirty, changed = true, true
				continue
			}
			if sidePx, ok := s.fiSideMark(p); ok { // platform-aware zero-API mark (freshinv helper)
				nMk := len(p.Marks)
				kfStampMark(&p, sidePx, "mark")
				if len(p.Marks) != nMk {
					s.cbBookDirty = true
				}
				if p.SL > 0 && sidePx <= p.SL {
					kfMarksClose(&p, sidePx, "stop")
					plat := p.Platform
					if plat == "" {
						plat = "kalshi"
					}
					exitFee := s.blendedFeeAction(plat, p.Ticker, "", false, p.Contracts, sidePx, false)
					pnl := p.Contracts*(sidePx-p.Price) - p.Fee - exitFee
					h.Net += pnl
					h.Losses++
					h.Closed = append(h.Closed, kfClosed{kfPos: p, Payout: sidePx,
						PnL: math.Round(pnl*100) / 100, Won: false, SettledTS: now.UTC().Format(time.RFC3339), Reason: "stop-loss"})
					closedNotes = append(closedNotes, fmt.Sprintf("%s stop-loss %+0.2f", p.Ticker, pnl))
					relationOutcomes = append(relationOutcomes, fundedKFOutcome{p, pnl, "cheapband-stop"})
					s.cbBookDirty, changed = true, true
					continue
				}
			}
			still = append(still, p)
		}
		h.Open = still
		if changed {
			if len(h.Closed) > 300 {
				h.Closed = h.Closed[len(h.Closed)-300:]
			}
			h.Equity = append(h.Equity, [2]float64{float64(now.Unix()), math.Round((h.Net-h.NetBase)*100) / 100})
			if len(h.Equity) > 400 {
				h.Equity = h.Equity[len(h.Equity)-400:]
			}
		}
	}
	s.cbBookMu.Unlock()
	s.settleFundedKFOutcomes(ctx, relationOutcomes)
	for _, n := range closedNotes {
		_ = s.store.Audit(ctx, "info", "cheapband", "Cheapband CLOSE "+n, "")
	}
	s.cheapbandFlush()
}

// cbBookOpenSide — cross-book hedge leg (platform-aware: each half guards its own venue).
func (s *Server) cbBookOpenSide(platform, ticker string) string {
	if platform != "kalshi" && platform != "polyus" {
		return ""
	}
	s.cbBookMu.Lock()
	defer s.cbBookMu.Unlock()
	h := cbHalf(s.cbLoadLocked(), platform)
	for _, p := range h.Open {
		if p.Ticker == ticker {
			return p.Side
		}
	}
	return ""
}

// handleCheapband (GET /api/cheapband) — both halves, rules, weight shares and lifetime stats.
func (s *Server) handleCheapband(w http.ResponseWriter, r *http.Request) {
	s.cbBookMu.Lock()
	b := s.cbLoadLocked()
	halves := map[string]*kfBook{"kalshi": &b.K, "polyus": &b.P}
	out := map[string]any{
		"enabled": s.cheapbandOn(),
		"rules":   "legacy Paper-only cheap-band hypothesis from the R126 assumed-fill study. Historical band returns and family verdicts are research diagnostics, not exchange profit evidence; they cannot promote, resize corrected one-share Paper, or authorize LIVE. Raw Paper ledger remains visible for audit.",
		"bands":   map[string][]float64{"kalshi": {cbBandLo, cbKalBandHi}, "polyus": {cbBandLo, cbPusBandHi}},
	}
	for name, h := range halves {
		netSince, wins, losses := kfCurrentEpochStats(h)
		wr := 0.0
		if n := wins + losses; n > 0 {
			wr = float64(wins) / float64(n)
		}
		openCost := 0.0
		lots := make([]map[string]any, 0, len(h.Open))
		for _, p := range h.Open {
			openCost += p.Contracts*p.Price + p.Fee
			lot := map[string]any{"ticker": p.Ticker, "title": p.Title, "side": p.Side,
				"price": p.Price, "contracts": p.Contracts, "fam": strings.TrimPrefix(p.RouteReason, "fam:")}
			if sidePx, ok := s.fiSideMark(p); ok {
				lot["cur_price"] = sidePx
				lot["unrealized"] = math.Round((p.Contracts*(sidePx-p.Price)-p.Fee)*100) / 100
			}
			lots = append(lots, lot)
		}
		roster := "book:cheapband-k"
		if name == "polyus" {
			roster = "book:cheapband-p"
		}
		blk := map[string]any{
			"accounting": "current-reset-epoch",
			"net":        sanF(math.Round(netSince*100) / 100), "closed": wins + losses, "wins": wins,
			"win_rate": sanF(wr), "open": len(h.Open), "open_cost": sanF(math.Round(openCost*100) / 100),
			"open_lots": lots, "equity_series": h.Equity,
			"lifetime": map[string]any{"net": sanF(math.Round(h.Net*100) / 100), "wins": h.Wins, "losses": h.Losses},
		}
		if share, ok := s.subShareUSD(roster); ok {
			blk["weight_share_usd"] = math.Round(share*100) / 100
		}
		out[name] = blk
	}
	s.cbBookMu.Unlock()
	writeJSON(w, http.StatusOK, out)
}
