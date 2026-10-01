package server

// R128 FAVLONG80 BOOK — buy expensive FAVORITES (>80¢) on Kalshi, maker-preferred. Fix-list
// item 4 (operator order), built on the B1 study (R127 tools/r127_favlong.py, n=10,072 graded
// favlong rows): the >80¢ band is +3.4¢/ct even at taker and +5.4¢/ct at maker fees, while
// 60–80¢ is the bleed. The favlong FAMILY keeps logging its full 60–92¢ band unconditionally;
// THIS book places only the ≥80¢ slice, tagged favlong80, judged by the verdict engine like
// everything else ("book:favlong80" — its own real fills).
//
//   - Detection: the existing favlong Kalshi generator (sweepR27Discovery block 3i.7 — liquid
//     non-crypto markets, favorite side). The generator's band tops out at 92¢, which is exactly
//     the band the B1 evidence graded — favlong80's entry band is [0.80, 0.92] by construction.
//   - NO ML GATE (raw-strategy book — measure the B1 edge pure, the freshinv doctrine).
//   - Maker-preferred through the queue-aware router (routeMakerTaker → bookMakerPost); taker
//     divert re-prices at the ask with the 3¢ never-chase doctrine.
//   - Sizing: the SHARED Kelly engine on the scoreboard-weight share of the Kalshi book
//     (R128 allocation engine; source "auto-cons-favlong80", kedge realized-only).
//   - Settlement: resolutions first (signal_log authority — favlong logs the rows), then the
//     frozen ratio stop against the cached venue mark. Kalshi-only.
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
	flBandLo     = 0.80 // B1: the profitable band starts at 80¢
	flBandHi     = 0.92 // the favlong generator's own ceiling (fee-trap guard) — the graded band
	flSpreadCapC = 10.0 // sane-spread screen (freshinv's cheap kalSigMeta mimic)
)

// favLong80On — config kill for the book (favlong80_enabled, nil/absent = ON).
func (s *Server) favLong80On() bool {
	v := s.cfg().Auto.Favlong80Enabled
	return v == nil || *v
}

// flLoadLocked lazy-loads favlong80_book.json. Caller holds flBookMu (bug-52 poison guard).
func (s *Server) flLoadLocked() *kfBook {
	if s.flBook != nil {
		return s.flBook
	}
	b := &kfBook{}
	path := filepath.Join(s.cfg().DataDir, "favlong80_book.json")
	if !s.readJSONLoose(path, b) {
		if st, err := os.Stat(path); err == nil && st.Size() > 0 {
			s.flBookPoisoned.Store(true)
			s.log.Error("favlong80_book.json exists but failed to parse — FavLong80 flushes DISABLED this session (bug-52 guard)", "path", path, "size", st.Size())
			_ = s.store.Audit(context.Background(), "error", "favlong80", "FavLong80 book file unreadable — flushes disabled this session; inspect data\\favlong80_book.json", "")
		}
	}
	s.flBook = b
	return b
}

// favLong80Flush persists the book if dirty (atomic tmp+rename; poison-guarded; 438 rule).
func (s *Server) favLong80Flush() {
	defer s.invalidatePortfolioEquityCache()
	s.flBookMu.Lock()
	defer s.flBookMu.Unlock()
	if s.flBookPoisoned.Load() || !s.flBookDirty || s.flBook == nil {
		return
	}
	out, err := json.Marshal(s.flBook)
	if err != nil {
		return
	}
	path := filepath.Join(s.cfg().DataDir, "favlong80_book.json")
	tmp := path + ".tmp"
	if os.WriteFile(tmp, out, 0o644) == nil {
		if os.Rename(tmp, path) == nil {
			s.flBookDirty = false
		}
	}
}

// favLong80WindingDown — disabled with lots still open: keep settling (268 doctrine).
func (s *Server) favLong80WindingDown() bool {
	s.flBookMu.Lock()
	defer s.flBookMu.Unlock()
	b := s.flLoadLocked()
	return len(b.Open) > 0
}

// favLong80Place routes one favlong detection at ≥80¢ into the FavLong80 book, maker-preferred.
// side/price are the FAVORITE side exactly as the favlong generator computed them.
func (s *Server) favLong80Place(ctx context.Context, ticker, title, side string, price, spreadC, resolveHours float64) {
	if !s.paperEntryHorizonOK(resolveHours, ticker, title) {
		return
	}
	if price < flBandLo || price > flBandHi || spreadC > flSpreadCapC {
		return
	}
	// R128 timeout doctrine: fail closed on provably-dead feeds (the venue books' R126 gate).
	if s.pxOfflineReason("kalshi", ticker) != "" {
		return
	}
	if r166LegacyPaperNewEntriesRetired {
		s.enqueueR166SpecialistTaker(ctx, storage.Signal{Platform: "kalshi", Ticker: ticker,
			Title: title, Side: strings.ToUpper(side), EntryPrice: price, SpreadCents: spreadC,
			ResolveHours: resolveHours}, "favlong80", "K", "favlong80:eligible-signal")
		return
	}
	if !s.favLong80On() || s.ksBlocked() || s.flBookPoisoned.Load() {
		return
	}
	if r := s.betConflictReason("kalshi", ticker, side, "auto-cons-favlong80", nil); r != "" {
		return
	}
	// R128 book gate: Kalshi venue book wall + the scoreboard-weight share.
	venueAvail := s.bookAvailableUSD(ctx, vbKalshi)
	subCap := s.promoShareCap("favlong80")
	s.flBookMu.Lock()
	subHeld := 0.0
	{
		b := s.flLoadLocked()
		for _, p := range b.Open {
			if p.Ticker == ticker {
				s.flBookMu.Unlock()
				return // one open lot per market
			}
		}
		if len(b.Open) >= 100 {
			s.flBookMu.Unlock()
			return // runaway guard
		}
		subHeld = kfLotsExposure(b.Open)
	}
	s.flBookMu.Unlock()
	sizingEquity := math.Min(subCap, s.bookSizingEquityUSD(ctx, vbKalshi))
	if sizingEquity <= 0 || venueAvail <= 0 || subHeld >= subCap {
		return
	}
	stake := s.engineStake(ctx, "kalshi", ticker, side, "auto-cons-favlong80", price, 0, sizingEquity, false, "realized-only")
	contracts := math.Floor(stake / price)
	if contracts < 1 {
		contracts = 1
	}
	s.autoMu.Lock()
	slRatio, slMode := s.autoSLTPRatio, s.autoSLTPMode
	s.autoMu.Unlock()
	sl := 0.0
	if slMode != "ride" && slRatio > 0 {
		sl = math.Max(0.01, price-slRatio*price)
	}
	entryPx, fee := price, s.pureMakerFee("kalshi", ticker, contracts, price)
	fillKind, fillRule := "maker", "legacy"
	if s.makerSimBooksOn() {
		divert := ""
		if s.cfg().Auto.MakerAdverseGuard {
			if reason, dep, depSrc, momPtr, momGateC := s.makerPostUnsafe("kalshi", ticker, side); reason != "" {
				divert = reason
				if s.makerGateStampAllowed("kalshi", ticker, "favlong80") {
					_ = s.store.InsertMakerGated(ctx, "kalshi", ticker, side, "favlong80", price, 0, dep, momPtr, momGateC, reason, depSrc)
				}
			}
		}
		if divert == "" { // MAKER-PREFERRED (B1: maker fees are the +5.4¢ cell)
			postPx := s.entryBid(ctx, "kalshi", ticker, side, price)
			if postPx < flBandLo || postPx > flBandHi {
				return // the band is an ENTRY rule — the posted price IS the entry if filled
			}
			rd := s.routeMakerTaker(ctx, "kalshi", ticker, side, postPx, "favlong80")
			if rd.Maker {
				pendingStake := s.pendingBookStake("favlong80") // pendMu only — before flBookMu (lock order)
				cost := contracts * postPx
				s.flBookMu.Lock()
				b := s.flLoadLocked()
				held := kfLotsExposure(b.Open)
				ok := held+pendingStake+cost <= subCap && cost <= venueAvail
				for _, p := range b.Open {
					if p.Ticker == ticker {
						ok = false
					}
				}
				s.flBookMu.Unlock()
				if !ok {
					return
				}
				if s.bookMakerPost(ctx, "favlong80", "kalshi", ticker, title, side, "favlong80", postPx, contracts, sl, rd) {
					return // resting; becomes a lot only on a REAL touch-through
				}
				return // a pending attempt already rests on this market
			}
			divert = rd.Tag()
		}
		// DIVERT to taker: re-price at the ask, 3¢ never-chase doctrine (B1: taker is still +3.4¢).
		px, fillCt, _, quoteOK := s.executableTaker(ctx, "kalshi", ticker, side, contracts)
		if !quoteOK || px > price+0.03 || px < flBandLo || px > flBandHi {
			return
		}
		contracts = fillCt
		entryPx, fee = px, s.blendedFee("kalshi", ticker, "", false, contracts, px)
		fillKind, fillRule = "taker", "divert:"+divert
	}
	cost := contracts * entryPx
	s.flBookMu.Lock()
	b := s.flLoadLocked()
	held := kfLotsExposure(b.Open)
	if held+cost > subCap || cost > venueAvail {
		s.flBookMu.Unlock()
		return
	}
	for _, p := range b.Open {
		if p.Ticker == ticker {
			s.flBookMu.Unlock()
			return
		}
	}
	nowTS := time.Now().UTC().Format(time.RFC3339)
	lot := kfPos{TS: nowTS, Ticker: ticker,
		Title: title, Side: side, Price: entryPx, Contracts: contracts, Fee: fee, SL: sl,
		FillKind: fillKind, FillRule: fillRule, Platform: "kalshi",
		SignalTS: nowTS, DecisionTS: nowTS, FillTS: nowTS}
	s.stampKFExactFee(&lot)
	if !s.stampFundedKFRelation(ctx, &lot, "auto-cons-favlong80", fillKind) {
		s.flBookMu.Unlock()
		return
	}
	b.Open = append(b.Open, lot)
	s.flBookDirty = true
	s.flBookMu.Unlock()
	s.favLong80Flush() // 454 rule: flush right after placement
	_ = s.store.Audit(ctx, "info", "favlong80",
		fmt.Sprintf("FavLong80 OPEN kalshi %s %s ×%.0f @ %.0f¢ ($%.2f, fee $%.2f, sl %.0f¢) fill_kind=%s fill_rule=%s", ticker, strings.ToUpper(side), contracts, entryPx*100, cost, fee, sl*100, fillKind, fillRule), "")
	s.enqueueLiveMirrorCandidate(liveMirrorCandidate{
		Platform: "kalshi", Ticker: ticker, Title: title, Side: side,
		Source: "auto-cons-favlong80", Family: "favlong80", Price: entryPx, At: time.Now(),
		InputTopology: "K",
	})
}

// flMakerFill books a filled FavLong80 post (sweepPendingMakers "favlong80" route).
func (s *Server) flMakerFill(ctx context.Context, p *pendingMaker, cur float64) string {
	if !s.paperEntryHorizonNow(ctx, p.platform, p.ticker, p.title) {
		return "entry-horizon"
	}
	if r := s.betConflictReason(p.platform, p.ticker, p.side, "auto-cons-favlong80", nil); r != "" {
		return "conflict"
	}
	fee := s.pureMakerFee(p.platform, p.ticker, p.contracts, p.px)
	cost := p.contracts * p.px
	// Convert this already-reserved maker post to a fill without charging its capital twice.
	venueAvail := s.bookAvailableUSD(ctx, vbKalshi) + cost + fee
	subCap := s.promoShareCap("favlong80")
	s.flBookMu.Lock()
	b := s.flLoadLocked()
	for _, o := range b.Open {
		if o.Ticker == p.ticker {
			s.flBookMu.Unlock()
			return "conflict"
		}
	}
	held := kfLotsExposure(b.Open)
	if held+cost > subCap || cost > venueAvail {
		s.flBookMu.Unlock()
		return "capital"
	}
	postTS := p.postedAt.UTC().Format(time.RFC3339)
	lot := kfPos{TS: time.Now().UTC().Format(time.RFC3339), Ticker: p.ticker,
		Title: p.title, Side: p.side, Price: p.px, Contracts: p.contracts, Fee: fee, SL: p.sl,
		FillKind: "maker", FillRule: pendingFillRule(p), Platform: p.platform, RouteReason: p.routeTag,
		SignalTS: postTS, DecisionTS: postTS, PostTS: postTS,
		FillTS: time.Now().UTC().Format(time.RFC3339), RelationReceiptID: p.relationReceiptID}
	s.stampKFExactFee(&lot)
	b.Open = append(b.Open, lot)
	s.flBookDirty = true
	s.flBookMu.Unlock()
	s.favLong80Flush()
	_ = s.store.Audit(ctx, "info", "favlong80",
		fmt.Sprintf("FavLong80 MAKER FILL %s %s ×%.0f @ %.0f¢ (mark %.0f¢, fee $%.2f) fill_rule=%s", p.ticker, strings.ToUpper(p.side), p.contracts, p.px*100, cur*100, fee, pendingFillRule(p)), "")
	s.enqueueLiveMirrorCandidate(liveMirrorCandidate{
		Platform: "kalshi", Ticker: p.ticker, Title: p.title, Side: p.side,
		Source: "auto-cons-favlong80", Family: "favlong80", Price: p.px, At: time.Now(),
		InputTopology: "K",
	})
	return ""
}

// flSideMark — current side price of an open lot from the zero-API Kalshi cache.
func (s *Server) flSideMark(p kfPos) (float64, bool) {
	yes, ok := s.cachedYesMark(p.Ticker)
	if !ok || yes <= 0 || yes >= 1 {
		return 0, false
	}
	if strings.EqualFold(p.Side, "NO") || strings.EqualFold(p.Side, "DOWN") {
		return 1 - yes, true
	}
	return yes, true
}

// settleFavLong80Book — resolutions first (signal_log authority), then the frozen ratio stop.
func (s *Server) settleFavLong80Book(ctx context.Context) {
	if !s.favLong80On() && !s.favLong80WindingDown() {
		return
	}
	s.flBookMu.Lock()
	b := s.flLoadLocked()
	if len(b.Open) == 0 {
		s.flBookMu.Unlock()
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
			closedNotes = append(closedNotes, fmt.Sprintf("%s settled %+0.2f", p.Ticker, pnl))
			relationOutcomes = append(relationOutcomes, fundedKFOutcome{p, pnl, "favlong80-settlement"})
			s.flBookDirty, changed = true, true
			continue
		}
		if sidePx, ok := s.flSideMark(p); ok {
			nMk := len(p.Marks)
			kfStampMark(&p, sidePx, "mark")
			if len(p.Marks) != nMk {
				s.flBookDirty = true
			}
		}
		if p.SL > 0 {
			if sidePx, ok := s.flSideMark(p); ok && sidePx <= p.SL {
				kfMarksClose(&p, sidePx, "stop")
				exitFee := s.blendedFeeAction("kalshi", p.Ticker, "", false, p.Contracts, sidePx, false)
				pnl := p.Contracts*(sidePx-p.Price) - p.Fee - exitFee
				b.Net += pnl
				b.Losses++
				b.Closed = append(b.Closed, kfClosed{kfPos: p, Payout: sidePx,
					PnL: math.Round(pnl*100) / 100, Won: false, SettledTS: now.UTC().Format(time.RFC3339), Reason: "stop-loss"})
				closedNotes = append(closedNotes, fmt.Sprintf("%s stop-loss %+0.2f", p.Ticker, pnl))
				relationOutcomes = append(relationOutcomes, fundedKFOutcome{p, pnl, "favlong80-stop"})
				s.flBookDirty, changed = true, true
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
	s.flBookMu.Unlock()
	s.settleFundedKFOutcomes(ctx, relationOutcomes)
	for _, n := range closedNotes {
		_ = s.store.Audit(ctx, "info", "favlong80", "FavLong80 CLOSE "+n, "")
	}
	s.favLong80Flush()
}

// flBookOpenSide — cross-book hedge leg (rfBookOpenSide pattern; Kalshi-only book).
func (s *Server) flBookOpenSide(platform, ticker string) string {
	if platform != "kalshi" {
		return ""
	}
	s.flBookMu.Lock()
	defer s.flBookMu.Unlock()
	b := s.flLoadLocked()
	for _, p := range b.Open {
		if p.Ticker == ticker {
			return p.Side
		}
	}
	return ""
}

// handleFavLong80 (GET /api/favlong80) — the book, rules, weight share and lifetime stats.
func (s *Server) handleFavLong80(w http.ResponseWriter, r *http.Request) {
	s.flBookMu.Lock()
	b := s.flLoadLocked()
	netSince, wins, losses := kfCurrentEpochStats(b)
	netLife := b.Net
	lifeWins, lifeLosses := b.Wins, b.Losses
	openSnap := append([]kfPos(nil), b.Open...)
	closedTail := append([]kfClosed(nil), b.Closed...)
	eqSeries := append([][2]float64(nil), b.Equity...)
	s.flBookMu.Unlock()
	wr := 0.0
	if n := wins + losses; n > 0 {
		wr = float64(wins) / float64(n)
	}
	openCost := 0.0
	lots := make([]map[string]any, 0, len(openSnap))
	for _, p := range openSnap {
		openCost += p.Contracts*p.Price + p.Fee
		lot := map[string]any{"ticker": p.Ticker, "title": p.Title, "side": p.Side,
			"price": p.Price, "contracts": p.Contracts}
		if sidePx, ok := s.flSideMark(p); ok {
			lot["cur_price"] = sidePx
			lot["unrealized"] = math.Round((p.Contracts*(sidePx-p.Price)-p.Fee)*100) / 100
		}
		lots = append(lots, lot)
	}
	share, shareOK := s.subShareUSD("book:favlong80")
	writeJSON(w, http.StatusOK, map[string]any{
		"enabled": s.favLong80On(),
		"rules":   "buy >80¢ favorites on Kalshi (B1 study: +3.4¢/ct taker, +5.4¢/ct maker at n=10,072) · band 80–92¢ (the graded band) · maker-preferred via the queue-aware router · NO ML gate (raw-strategy book) · realized-only Kelly on the scoreboard-weight share of the Kalshi book · one lot per market · frozen ratio stop · paper only",
		"band":    []float64{flBandLo, flBandHi},
		"weight_share_usd": func() any {
			if shareOK {
				return math.Round(share*100) / 100
			}
			return nil
		}(),
		"accounting": "current-reset-epoch",
		"net":        sanF(math.Round(netSince*100) / 100), "closed": wins + losses, "wins": wins,
		"win_rate": sanF(wr), "open": len(openSnap), "open_cost": sanF(math.Round(openCost*100) / 100),
		"open_lots": lots, "closed_tail": closedTail, "equity_series": eqSeries,
		"lifetime": map[string]any{"net": sanF(math.Round(netLife*100) / 100), "wins": lifeWins, "losses": lifeLosses},
	})
}
