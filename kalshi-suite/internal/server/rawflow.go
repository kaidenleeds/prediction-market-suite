package server

// R103 RAWFLOW BOOK — the signals-vs-ML experiment (epoch-3 rebalance, operator directive).
// Question under test: does the RAW kalshi-flow signal with fixed algorithmic rules beat the
// ML-gated pipeline on the same feed? Rules (FIXED by design — the absence of adaptive gates IS
// the experiment):
//
//   - kalshi-flow signals ONLY: the same rows the kflow family logger emits (one-sided sustained
//     flow clearing the family's own strength bar, whale prints and crypto-15M excluded upstream).
//   - Price band 10–50¢ (auditor-endorsed: the family's +3.7¢/ct band that clears maker fees).
//   - Sizing: the SHARED Kelly engine (engineStake) on THIS book's own equity — edge source is the
//     family's REALIZED per-contract net ("auto-cons-kflow" stats), never the ML.
//   - NO ML gate, NO haircut, NO borders. (Safety rails are NOT part of the experiment:
//     one-open-lot-per-MARKET within the book, and the R100 cross-book ML-hedge block still apply.)
//   - Stop-loss: the auto book's ratio rule SL = p − r·p with r FROZEN at entry (fixed, not
//     retuned mid-lot); no stop when the ratio is unset/ride-mode. Otherwise ride to settlement.
//   - Maker entry fee model (the live convention, same as the kflow twins); stop exits pay taker.
//
// Bank seeds at alloc_rawflow × paper_total_start each epoch (Reset P&L re-seeds); the book's net
// joins NAV (it is allocated portfolio money, unlike the research kflow twins). Lots are tagged in
// their own file (data/rawflow_book.json) + audit rows category "rawflow" so the auditor can grade
// RawFlow-vs-ML directly. NEVER real money.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	rawFlowBandLo = 0.10 // 10¢ — fixed experiment band (auditor r29: +3.7¢/ct in 10–50¢ clears fees)
	rawFlowBandHi = 0.50 // 50¢
)

// rawFlowEnabled: the book takes NEW lots iff it has an allocation (equity-fraction mode only).
func (s *Server) rawFlowEnabled() bool {
	return s.cfg().Auto.PaperTotalStart > 0 && s.cfg().Auto.AllocRawFlow > 0
}

// rawFlowWindingDown — R103 (auditor r31 bug 268): zeroing alloc_rawflow with lots still open must
// behave like the FROZEN kflow twins, not a cliff: open lots keep settling, the header line keeps
// rendering "(archived)" while they do, and the book's epoch net stays in NAV until a reset
// re-bases it — no discontinuous NAV step, no orphaned lots.
func (s *Server) rawFlowWindingDown() bool {
	// R105 (auditor A9, R104 queue): the old `PaperTotalStart <= 0 → false` gate meant flipping the
	// suite back to legacy FLAT bankrolls ORPHANED open RawFlow lots mid-position (wind-down went
	// false ⇒ settlement stopped ⇒ header vanished ⇒ NAV stepped). Wind-down now depends ONLY on
	// the book's own state: open lots (or an un-rebased epoch net) keep settling + rendering
	// "(archived)" regardless of the bankroll mode.
	s.rfBookMu.Lock()
	defer s.rfBookMu.Unlock()
	b := s.rfLoadLocked()
	return len(b.Open) > 0 || math.Abs(b.Net-b.NetBase) > 0.005
}

// rawFlowSeedBank is the epoch-start bank: NORMALIZED alloc_rawflow × paper_total_start.
func (s *Server) rawFlowSeedBank() float64 {
	_, _, _, rf, _, active := s.allocFracs()
	if !active {
		return 0
	}
	return math.Round(rf*s.cfg().Auto.PaperTotalStart*100) / 100
}

// rfLoadLocked lazy-loads rawflow_book.json. Caller holds rfBookMu. Same bug-52 poison guard as
// the kflow twins: an EXISTING file that fails to parse disables flushes for the session.
func (s *Server) rfLoadLocked() *kfBook {
	if s.rfBook != nil {
		return s.rfBook
	}
	b := &kfBook{Bank: s.rawFlowSeedBank()}
	path := filepath.Join(s.cfg().DataDir, "rawflow_book.json")
	if !s.readJSONLoose(path, b) {
		if st, err := os.Stat(path); err == nil && st.Size() > 0 {
			s.rfPoisoned.Store(true)
			s.log.Error("rawflow_book.json exists but failed to parse — RawFlow flushes DISABLED this session (bug-52 guard)", "path", path, "size", st.Size())
			_ = s.store.Audit(context.Background(), "error", "rawflow", "RawFlow book file unreadable — flushes disabled this session; inspect data\\rawflow_book.json", "")
		}
	}
	if b.Bank <= 0 {
		b.Bank = s.rawFlowSeedBank()
	}
	s.rfBook = b
	return b
}

// rawFlowPlace routes one just-logged kalshi-flow signal into the RawFlow book under the fixed
// rules. In-memory only — the caller batch-flushes via rawFlowFlush (one write per scan).
func (s *Server) rawFlowPlace(ctx context.Context, ticker, title, side string, price, resolveHours float64) {
	if !s.paperEntryHorizonOK(resolveHours, ticker, title) {
		return
	}
	if price < rawFlowBandLo || price > rawFlowBandHi {
		return // fixed 10–50¢ band — the experiment's only price rule
	}
	if r166LegacyPaperNewEntriesRetired {
		s.enqueueR166SpecialistTaker(ctx, storage.Signal{Platform: "kalshi", Ticker: ticker,
			Title: title, Side: strings.ToUpper(side), EntryPrice: price, ResolveHours: resolveHours},
			"rawflow", "K", "rawflow:eligible-signal")
		return
	}
	if !s.rawFlowEnabled() || s.ksBlocked() {
		return
	}
	// SAFETY (not part of the experiment): the R100 cross-book hedge guard — never bet the
	// opposite side of a market the ML book is riding. positions=nil: the RawFlow book is
	// standalone (its own one-lot-per-market rule below), not coupled to the shared paper book.
	if r := s.betConflictReason("kalshi", ticker, side, "auto-cons-rawflow", nil); r != "" {
		return
	}
	// Sizing OUTSIDE the book lock (engineStake may read family stats/feeds).
	// R105 (auditor A11): `poisonedEquity` renamed — it was ordinary book equity, nothing poisoned;
	// the misnomer was flagged as an audit-readability hazard on a money-adjacent path.
	s.rfBookMu.Lock()
	bookEquity := 0.0
	{
		b := s.rfLoadLocked()
		for _, p := range b.Open {
			if p.Ticker == ticker {
				s.rfBookMu.Unlock()
				return // one open lot per MARKET (either side) — fixed rule
			}
		}
		if len(b.Open) >= 100 {
			s.rfBookMu.Unlock()
			return // runaway guard (kflow keeps 200; this book is band-limited so 100 is generous)
		}
		bookEquity = b.Bank + (b.Net - b.NetBase) // book equity = epoch bank + net since epoch
	}
	s.rfBookMu.Unlock()
	if bookEquity <= 0 {
		return
	}
	// SHARED ENGINE on the book's own equity. Edge/Kelly stats key off the kflow FAMILY (the
	// signal family under test — "auto-cons-rawflow" has no realized history by construction);
	// allocMult=false (the R78 allocator is a paper-auto-book overlay); kedge "realized-only"
	// (auditor r31 bug 266: the default hybrid/realized modes fall back to PURE ML on thin
	// evidence, which would contaminate the signals-vs-ML experiment — no ML, ever, here).
	stake := s.engineStake(ctx, "kalshi", ticker, side, "auto-cons-kflow", price, 0, bookEquity, false, "realized-only")
	contracts := math.Floor(stake / price)
	if contracts < 1 {
		contracts = 1
	}
	// Fixed stop rule: ratio SL = p − r·p, r frozen NOW (mode "ride" or r<=0 ⇒ no stop).
	s.autoMu.Lock()
	slRatio, slMode := s.autoSLTPRatio, s.autoSLTPMode
	s.autoMu.Unlock()
	sl := 0.0
	if slMode != "ride" && slRatio > 0 {
		sl = math.Max(0.01, price-slRatio*price)
	}
	// R107 (operator: every paper book places via the MAKER SIM): the lot no longer books
	// instantly at the signal price. Post at entryBid (join-or-improve, the mfs prior art) and
	// let sweepPendingMakers fill it ONLY on ≥1¢ touch-through (R106 cancel parity applies).
	// The R105 option-c gate stays on top: an unsafe book DIVERTS to an instant taker fill at
	// the ask with taker fees — take-or-skip, never a blind post. Every lot is tagged
	// fill_kind/fill_rule so the auditor can grade maker vs taker directly.
	entryPx, fee := price, s.kalFee(ticker, true, int(contracts), price)
	fillKind, fillRule := "maker", "legacy"
	if s.makerSimBooksOn() {
		divert := ""
		if s.cfg().Auto.MakerAdverseGuard {
			if reason, dep, depSrc, momPtr, momGateC := s.makerPostUnsafe("kalshi", ticker, side); reason != "" {
				divert = reason
				if s.makerGateStampAllowed("kalshi", ticker, "rawflow") {
					_ = s.store.InsertMakerGated(ctx, "kalshi", ticker, side, "rawflow", price, 0, dep, momPtr, momGateC, reason, depSrc)
				}
			}
		}
		if divert == "" { // SAFE to post: reserve capital (open + resting posts), then rest the order
			postPx := s.entryBid(ctx, "kalshi", ticker, side, price)
			if postPx < rawFlowBandLo || postPx > rawFlowBandHi {
				return // the band is an ENTRY rule — the posted price IS the entry if filled
			}
			// R122 ROUTER: queue-aware maker/taker choice at the actual post price (option-c
			// above was the tactical safety gate; this is the strategic one).
			rd := s.routeMakerTaker(ctx, "kalshi", ticker, side, postPx, "rawflow")
			if rd.Maker {
				pendingStake := s.pendingBookStake("rawflow") // pendMu only — taken BEFORE rfBookMu (lock order)
				cost := contracts * postPx
				s.rfBookMu.Lock()
				b := s.rfLoadLocked()
				held := 0.0
				for _, p := range b.Open {
					held += p.Contracts*p.Price + p.Fee
				}
				ok := b.Bank+(b.Net-b.NetBase)-held-pendingStake >= cost
				for _, p := range b.Open {
					if p.Ticker == ticker {
						ok = false
					}
				}
				s.rfBookMu.Unlock()
				if !ok {
					return // fully invested (incl. resting posts) or dup — skip
				}
				if s.bookMakerPost(ctx, "rawflow", "kalshi", ticker, title, side, "rawflow", postPx, contracts, sl, rd) {
					return // resting; it becomes a lot only on a REAL touch-through
				}
				return // a pending attempt already rests on this market
			}
			divert = rd.Tag() // router chose taker — fall to the divert path, reason tagged
		}
		// DIVERT to taker: re-price at the ask, taker fee, same staleness doctrine as autoPlace.
		px, fillCt, _, quoteOK := s.executableTaker(ctx, "kalshi", ticker, side, contracts)
		if !quoteOK || px > price+0.03 || px < rawFlowBandLo || px > rawFlowBandHi {
			return // moved off the signal or out of band — skip, never chase
		}
		contracts = fillCt
		entryPx, fee = px, s.kalFee(ticker, false, int(contracts), px)
		fillKind, fillRule = "taker", "divert:"+divert
	}
	cost := contracts * entryPx
	s.rfBookMu.Lock()
	b := s.rfLoadLocked()
	held := 0.0
	for _, p := range b.Open {
		held += p.Contracts*p.Price + p.Fee
	}
	if b.Bank+(b.Net-b.NetBase)-held < cost {
		s.rfBookMu.Unlock()
		return // fully invested — skip rather than margin a paper experiment
	}
	for _, p := range b.Open { // re-check under the lock (sizing window)
		if p.Ticker == ticker {
			s.rfBookMu.Unlock()
			return
		}
	}
	// R120 provenance: this is the instant-taker divert path — placed in the same pass the signal
	// logged, so signal/decision/fill share one stamp (maker fills stamp post/fill in rfMakerFill).
	nowTS := time.Now().UTC().Format(time.RFC3339)
	lot := kfPos{TS: nowTS, Ticker: ticker,
		Title: title, Side: side, Price: entryPx, Contracts: contracts, Fee: fee, SL: sl,
		FillKind: fillKind, FillRule: fillRule,
		SignalTS: nowTS, DecisionTS: nowTS, FillTS: nowTS}
	s.stampKFExactFee(&lot)
	if !s.stampFundedKFRelation(ctx, &lot, "rawflow", fillKind) {
		s.rfBookMu.Unlock()
		return
	}
	b.Open = append(b.Open, lot)
	s.rfDirty = true
	s.rfBookMu.Unlock()
	_ = s.store.Audit(ctx, "info", "rawflow",
		fmt.Sprintf("RawFlow OPEN %s %s ×%.0f @ %.0f¢ ($%.2f, fee $%.2f, sl %.0f¢) fill_kind=%s fill_rule=%s", ticker, strings.ToUpper(side), contracts, entryPx*100, cost, fee, sl*100, fillKind, fillRule), "")
	s.enqueueLiveMirrorCandidate(liveMirrorCandidate{
		Platform: "kalshi", Ticker: ticker, Title: title, Side: side,
		Source: "auto-cons-rawflow", Price: entryPx, At: time.Now(),
	})
}

// settleRawFlowBook grades the book on the normal settlement tick: resolution first (payout via
// the same signal_log authority the ML/kflow books settle on), then the frozen stop-loss against
// the zero-API cached mark (exit at mark, taker exit fee). Runs beside settleKflowBooks.
func (s *Server) settleRawFlowBook(ctx context.Context) {
	if !s.rawFlowEnabled() && !s.rawFlowWindingDown() { // 268: wind-down keeps settling after un-allocation
		return
	}
	s.rfBookMu.Lock()
	b := s.rfLoadLocked()
	if len(b.Open) == 0 {
		s.rfBookMu.Unlock()
		return
	}
	now := time.Now()
	still := b.Open[:0:0]
	changed := false
	var closedNotes []string
	var relationOutcomes []fundedKFOutcome
	for _, p := range b.Open {
		// 1) RESOLUTION — the terminal truth wins over any stop.
		if yv, res := s.store.ResolvedYesForVenue(ctx, fiLotPlatform(p), p.Ticker); res {
			payout := yv
			if strings.EqualFold(p.Side, "NO") || strings.EqualFold(p.Side, "DOWN") {
				payout = 1 - yv
			}
			kfMarksClose(&p, payout, "settle") // R120: terminal trajectory mark
			pnl := p.Contracts*(payout-p.Price) - p.Fee
			won := payout > p.Price
			b.Net += pnl
			if won {
				b.Wins++
			} else {
				b.Losses++
			}
			b.Closed = append(b.Closed, kfClosed{kfPos: p, Payout: payout,
				PnL: math.Round(pnl*100) / 100, Won: won, SettledTS: now.UTC().Format(time.RFC3339), Reason: "settled"})
			closedNotes = append(closedNotes, fmt.Sprintf("%s settled %+0.2f", p.Ticker, pnl))
			relationOutcomes = append(relationOutcomes, fundedKFOutcome{p, pnl, "rawflow-settlement"})
			s.rfDirty, changed = true, true
			continue
		}
		// 2) STOP-LOSS — frozen level vs the cached live mark (side price). No mark = no action.
		// R120 provenance: the same cached mark stamps the lot's trajectory (all lots, not just
		// stopped ones), at the documented ≥1¢-or-≥60s resolution.
		if yes, ok := s.cachedYesMark(p.Ticker); ok && yes > 0 && yes < 1 {
			sidePx := yes
			if strings.EqualFold(p.Side, "NO") || strings.EqualFold(p.Side, "DOWN") {
				sidePx = 1 - yes
			}
			nMk := len(p.Marks)
			kfStampMark(&p, sidePx, "mark")
			if len(p.Marks) != nMk {
				s.rfDirty = true
			}
			if p.SL > 0 {
				if sidePx <= p.SL {
					kfMarksClose(&p, sidePx, "stop")
					exitFee := s.kalFee(p.Ticker, false, int(p.Contracts), sidePx) // taker exit
					pnl := p.Contracts*(sidePx-p.Price) - p.Fee - exitFee
					b.Net += pnl
					b.Losses++ // a stop exit below entry is a loss by construction
					b.Closed = append(b.Closed, kfClosed{kfPos: p, Payout: sidePx,
						PnL: math.Round(pnl*100) / 100, Won: false, SettledTS: now.UTC().Format(time.RFC3339), Reason: "stop-loss"})
					closedNotes = append(closedNotes, fmt.Sprintf("%s stop-loss %+0.2f", p.Ticker, pnl))
					relationOutcomes = append(relationOutcomes, fundedKFOutcome{p, pnl, "rawflow-stop"})
					s.rfDirty, changed = true, true
					continue
				}
			}
		}
		still = append(still, p)
	}
	b.Open = still
	if changed { // only a CLOSE moves the sparkline (a placement alone must not add a point)
		if len(b.Closed) > 300 {
			b.Closed = b.Closed[len(b.Closed)-300:]
		}
		b.Equity = append(b.Equity, [2]float64{float64(now.Unix()), math.Round((b.Net-b.NetBase)*100) / 100})
		if len(b.Equity) > 400 {
			b.Equity = b.Equity[len(b.Equity)-400:]
		}
	}
	s.rfBookMu.Unlock()
	s.settleFundedKFOutcomes(ctx, relationOutcomes)
	for _, n := range closedNotes {
		_ = s.store.Audit(ctx, "info", "rawflow", "RawFlow CLOSE "+n, "")
	}
	s.rawFlowFlush()
}

// rawFlowFlush persists the book if dirty (atomic tmp+rename; poison-guarded).
func (s *Server) rawFlowFlush() {
	s.rfBookMu.Lock()
	defer s.rfBookMu.Unlock()
	if s.rfPoisoned.Load() || !s.rfDirty || s.rfBook == nil {
		return
	}
	out, err := json.Marshal(s.rfBook)
	if err != nil {
		return
	}
	path := filepath.Join(s.cfg().DataDir, "rawflow_book.json")
	tmp := path + ".tmp"
	if os.WriteFile(tmp, out, 0o644) == nil {
		if os.Rename(tmp, path) == nil { // R122 (auditor 438): clear dirty only on rename success
			s.rfDirty = false
		}
	}
}

// resetRawFlowBook starts a flat current epoch. Pre-reset opens move to the durable reset archive;
// lifetime closed evidence remains and the bank re-seeds from the current allocation.
func (s *Server) resetRawFlowBook(ctx context.Context) {
	res, err := s.resetRawFlowBookAt(time.Now())
	err = s.finishKFResetResult(ctx, res, err)
	if s.store != nil {
		level := "info"
		msg := fmt.Sprintf("reset P&L: RawFlow started flat; %d prior opens archived; bank re-seeded to $%.2f", len(res.ArchivedOpen), s.rawFlowSeedBank())
		if err != nil {
			level, msg = "error", "reset P&L: RawFlow clean epoch failed: "+err.Error()
		}
		_ = s.store.Audit(context.WithoutCancel(ctx), level, "rawflow", msg, "")
	}
}

// rawFlowNetSinceEpoch is the NAV component (allocated portfolio money — joins refreshTotalEquity).
// 268: the component persists through a wind-down (alloc zeroed, lots settling) so NAV never
// steps discontinuously; the NEXT reset re-bases it to 0 like every book.
func (s *Server) rawFlowNetSinceEpoch() float64 {
	if !s.rawFlowEnabled() && !s.rawFlowWindingDown() {
		return 0
	}
	s.rfBookMu.Lock()
	defer s.rfBookMu.Unlock()
	b := s.rfLoadLocked()
	return b.Net - b.NetBase
}

// rawFlowMLPayload — R105 (operator: "RawFlow gets a panel like the kflow twins"): the /api/ml
// "rawflow" block behind the ML-tab panel + the rawflow-book widget. kflowBooksPayload pattern;
// open lots are marked from the ZERO-API cached tape only (no REST on the snapshot path).
func (s *Server) rawFlowMLPayload() map[string]any {
	enabled, winding := s.rawFlowEnabled(), s.rawFlowWindingDown()
	if !enabled && !winding {
		return map[string]any{"enabled": false}
	}
	s.rfBookMu.Lock()
	b := s.rfLoadLocked()
	wins, losses := b.Wins-b.WinsBase, b.Losses-b.LossesBase
	if wins < 0 {
		wins = 0
	}
	if losses < 0 {
		losses = 0
	}
	wr := 0.0
	if n := wins + losses; n > 0 {
		wr = float64(wins) / float64(n)
	}
	openSnap := append([]kfPos(nil), b.Open...)
	netSince := b.Net - b.NetBase
	bank, netLife := b.Bank, b.Net
	closedLife := b.Wins + b.Losses
	eqSeries := append([][2]float64(nil), b.Equity...)
	s.rfBookMu.Unlock()
	openCost := 0.0
	clStake := map[string]float64{}
	clN := map[string]int{}
	lots := make([]map[string]any, 0, len(openSnap))
	for _, p := range openSnap { // marks OUTSIDE the book lock (cachedYesMark takes its own locks)
		openCost += p.Contracts*p.Price + p.Fee
		if ck := corrClusterKey("kalshi", p.Ticker); ck != "" {
			clStake[ck] += p.Contracts * p.Price
			clN[ck]++
		}
		lot := map[string]any{"ticker": p.Ticker, "title": p.Title, "side": p.Side,
			"price": p.Price, "contracts": p.Contracts}
		if ts, err := time.Parse(time.RFC3339, p.TS); err == nil {
			lot["opened"] = ts.Unix()
		}
		if yes, ok := s.cachedYesMark(p.Ticker); ok && yes > 0 && yes < 1 {
			sidePx := yes
			if strings.EqualFold(p.Side, "NO") || strings.EqualFold(p.Side, "DOWN") {
				sidePx = 1 - yes
			}
			lot["cur_price"] = sidePx
			lot["unrealized"] = math.Round((p.Contracts*(sidePx-p.Price)-p.Fee)*100) / 100
		}
		lots = append(lots, lot)
	}
	return map[string]any{
		"enabled": true, "winding_down": winding && !enabled,
		"rules": "kalshi-flow signals only · 10–50¢ band · shared Kelly engine on own equity (realized family edge, no ML) · no ML gate/haircut/borders · one lot per market · ML-hedge block · ratio stop frozen at entry else ride to settlement · maker entry fee",
		"band":  "10–50¢",
		"bank":  sanF(math.Round(bank*100) / 100), "equity": sanF(math.Round((bank+netSince)*100) / 100),
		"net": sanF(math.Round(netSince*100) / 100), "closed": wins + losses, "wins": wins,
		"win_rate": sanF(wr), "open": len(openSnap), "open_cost": sanF(math.Round(openCost*100) / 100),
		"open_lots": lots, "equity_series": eqSeries,
		"net_lifetime": sanF(math.Round(netLife*100) / 100), "closed_lifetime": closedLife,
		"corr_clusters": func() []map[string]any {
			// R111: correlated-exposure clusters (2+ open lots on one underlying — allowed since
			// the restatement-taxonomy correction; entry-capped by cluster_exposure_cap).
			out := []map[string]any{}
			for ck, n := range clN {
				if n >= 2 {
					out = append(out, map[string]any{"cluster": ck, "lots": n,
						// R114 (auditor 331): cap is +Inf when the brake is OFF (R112 default);
						// raw Inf breaks json.Marshal and killed both this payload and buildML.
						"stake": sanF(math.Round(clStake[ck]*100) / 100), "cap": sanF(s.clusterExposureCap())})
				}
			}
			sort.Slice(out, func(i, j int) bool { return out[i]["stake"].(float64) > out[j]["stake"].(float64) })
			return out
		}(),
	}
}

// handleRawFlow (GET /api/rawflow) — the book, its fixed rules, and epoch-relative stats.
func (s *Server) handleRawFlow(w http.ResponseWriter, r *http.Request) {
	if !s.rawFlowEnabled() {
		writeJSON(w, http.StatusOK, map[string]any{"enabled": false, "note": "set alloc_rawflow > 0 (equity-fraction mode) to run the RawFlow experiment book"})
		return
	}
	s.rfBookMu.Lock()
	b := s.rfLoadLocked()
	openCost := 0.0
	clStake := map[string]float64{}
	clTicks := map[string][]string{}
	for _, p := range b.Open {
		openCost += p.Contracts*p.Price + p.Fee
		if ck := corrClusterKey("kalshi", p.Ticker); ck != "" {
			clStake[ck] += p.Contracts * p.Price
			clTicks[ck] = append(clTicks[ck], p.Ticker)
		}
	}
	// R111: correlated-exposure clusters (2+ open lots on one underlying — legitimate since the
	// restatement-taxonomy correction, but surfaced so concentration is visible; capped by
	// cluster_exposure_cap at entry).
	clusters := []map[string]any{}
	for ck, tks := range clTicks {
		if len(tks) >= 2 {
			clusters = append(clusters, map[string]any{
				"cluster": ck, "tickers": tks,
				// R114 (auditor 331): sanitize +Inf (brake OFF) — raw Inf 500s /api/rawflow.
				"stake": math.Round(clStake[ck]*100) / 100,
				"cap":   sanF(s.clusterExposureCap()),
			})
		}
	}
	sort.Slice(clusters, func(i, j int) bool { return clusters[i]["stake"].(float64) > clusters[j]["stake"].(float64) })
	netSince, wins, losses := kfCurrentEpochStats(b)
	out := map[string]any{
		"enabled":    true,
		"accounting": "current-reset-epoch",
		"rules":      "kalshi-flow signals only · 10–50¢ band · shared Kelly engine on own equity (realized family edge, no ML) · no ML gate/haircut/borders · one lot per market · ML-hedge block · ratio stop frozen at entry else ride to settlement · maker entry fee",
		"bank":       b.Bank, "net": math.Round(netSince*100) / 100, "net_epoch": math.Round(netSince*100) / 100,
		"wins": wins, "losses": losses,
		"open": b.Open, "open_n": len(b.Open), "open_cost": math.Round(openCost*100) / 100,
		"corr_clusters": clusters,
		"closed_tail":   b.Closed, "equity": b.Equity,
		"lifetime": map[string]any{"net": math.Round(b.Net*100) / 100, "wins": b.Wins, "losses": b.Losses},
	}
	s.rfBookMu.Unlock()
	writeJSON(w, http.StatusOK, out)
}
