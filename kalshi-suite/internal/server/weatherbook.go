package server

// R106 WEATHER BOOK — operator ask: a $300 paper book trading the wxedge signal family only
// (NWS-forecast-anchored Kalshi daily-high temperature ladders — weather.go's producer, revived
// R99 / instrumented R90+R101). Portfolio becomes $1,300: ML $350 · Kalshi auto $350 · PolyUS
// $100 · RawFlow $200 · Weather $300. Rules (rawflow.go pattern — same shared machinery, its own
// ledger):
//
//   - wxedge signals ONLY: the rows weather.go's wxScanSeries emits (forecast-covering ladder bin,
//     side always YES, NWS CLI-settled series only). Placement is driven INLINE from the producer.
//   - Price band 20–40¢ DEFAULT (the auditor's edge-24 endorsement), config-tunable via
//     weather_band_lo_c / weather_band_hi_c (cents; 0 ⇒ defaults).
//   - Sizing: the SHARED Kelly engine (engineStake) on THIS book's own equity — edge source
//     "auto-cons-wxedge" in realized-only mode. wxedge is log-only (no auto family accrues fills),
//     so betEdge consults the BOOK'S OWN settled lots (wxBookRealizedEdge, n≥5) as the realized
//     evidence; until that exists the engine sizes at its base (min-stake floor semantics — the
//     same honest cold-start RawFlow had). NO ML gate/haircut/borders, ever.
//   - One open lot per MARKET + the cross-book hedge guard both ways ("weather-hedge" mirrors
//     R105's symmetric rawflow-hedge).
//   - Stop-loss: house ratio rule, FROZEN at entry (r<=0/ride ⇒ ride to settlement — weather
//     ladders settle at the next-morning NWS CLI report, ~35h). Maker entry fee; stops exit taker.
//
// Bank seeds at alloc_weather × paper_total_start each epoch; the book's net joins NAV. Lots live
// in data/weather_book.json + audit rows category "weather" (book=weather tags for auditor
// grading). NEVER real money.

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
)

const (
	weatherBandLoDefC = 20.0 // default band low (cents) — auditor edge-24 endorsement
	weatherBandHiDefC = 40.0 // default band high (cents)
	// R132: the old point-forecast strategy paid 20–40¢ for whichever rounded NWS hourly
	// forecast bin happened to contain the point estimate. It never modeled P(bin), and forecast
	// revisions stacked mutually-exclusive YES bins in the same city/day event. Research logging
	// and settlement stay on; new capital stays off until the event-level probability model exists.
	weatherNewPlacementsFrozen   = true
	weatherPlacementFreezeReason = "R132 weather freeze: new lots disabled after the settled book lost $70.36 on $330.36 staked (-21.3%); forecast revisions also stacked mutually-exclusive bins. Research and settlement remain active pending an event-level probability model."
)

var weatherFreezeAuditOnce sync.Once

// weatherBand returns the configured entry band in DOLLARS (config knobs are cents; 0 ⇒ default).
func (s *Server) weatherBand() (lo, hi float64) {
	a := s.cfg().Auto
	loC, hiC := a.WeatherBandLoC, a.WeatherBandHiC
	if loC <= 0 {
		loC = weatherBandLoDefC
	}
	if hiC <= 0 {
		hiC = weatherBandHiDefC
	}
	if hiC < loC {
		loC, hiC = hiC, loC
	}
	return loC / 100, hiC / 100
}

// weatherBookEnabled: the book takes NEW lots iff it has an allocation (equity-fraction mode only).
func (s *Server) weatherBookEnabled() bool {
	return s.cfg().Auto.PaperTotalStart > 0 && s.cfg().Auto.AllocWeather > 0
}

// weatherWindingDown mirrors rawFlowWindingDown (bug-268 doctrine): zeroing alloc_weather with
// lots open keeps them settling + rendering "(archived)" on the book's own state alone.
func (s *Server) weatherWindingDown() bool {
	s.wxBookMu.Lock()
	defer s.wxBookMu.Unlock()
	b := s.wxBookLoadLocked()
	return len(b.Open) > 0 || math.Abs(b.Net-b.NetBase) > 0.005
}

// weatherSeedBank is the epoch-start bank: NORMALIZED alloc_weather × paper_total_start.
func (s *Server) weatherSeedBank() float64 {
	_, _, _, _, wx, active := s.allocFracs()
	if !active {
		return 0
	}
	return math.Round(wx*s.cfg().Auto.PaperTotalStart*100) / 100
}

// wxBookLoadLocked lazy-loads weather_book.json. Caller holds wxBookMu. Bug-52 poison guard.
// SELF-HEALING SEED (found live during the R106 deploy): the book can be touched (NAV sweep /
// endpoints) BEFORE alloc_weather is set — a load in that window minted Bank=0 and cached it, so
// enabling the allocation later left a book that could never place. A $0 bank is never valid
// while an allocation exists, so the seed re-checks on EVERY access (cheap: two float reads).
func (s *Server) wxBookLoadLocked() *kfBook {
	if s.wxBook != nil {
		if s.wxBook.Bank <= 0 {
			if seed := s.weatherSeedBank(); seed > 0 {
				s.wxBook.Bank, s.wxBookDirty = seed, true
			}
		}
		return s.wxBook
	}
	b := &kfBook{Bank: s.weatherSeedBank()}
	path := filepath.Join(s.cfg().DataDir, "weather_book.json")
	if !s.readJSONLoose(path, b) {
		if st, err := os.Stat(path); err == nil && st.Size() > 0 {
			s.wxBookPoisoned.Store(true)
			s.log.Error("weather_book.json exists but failed to parse — Weather flushes DISABLED this session (bug-52 guard)", "path", path, "size", st.Size())
			_ = s.store.Audit(context.Background(), "error", "weather", "Weather book file unreadable — flushes disabled this session; inspect data\\weather_book.json", "")
		}
	}
	if b.Bank <= 0 {
		b.Bank = s.weatherSeedBank()
	}
	s.wxBook = b
	return b
}

// wxBookRealizedEdge is the book's OWN realized per-contract net (mean over settled lots, n≥5) —
// the "auto-cons-wxedge" realized evidence for betEdge's realized-only mode. wxedge never
// auto-places into the shared paper book, so no fills-based verdict can exist; the book IS the
// family's track record. Returns ok=false below n=5 (engine then sizes at its base — honest
// cold-start, no ML fallback by construction).
func (s *Server) wxBookRealizedEdge() (float64, bool) {
	s.wxBookMu.Lock()
	defer s.wxBookMu.Unlock()
	b := s.wxBookLoadLocked()
	n, sumPnl, sumCt := 0, 0.0, 0.0
	for i := len(b.Closed) - 1; i >= 0 && n < 200; i-- {
		c := b.Closed[i]
		if c.Contracts <= 0 {
			continue
		}
		sumPnl += c.PnL
		sumCt += c.Contracts
		n++
	}
	if n < 5 || sumCt <= 0 {
		return 0, false
	}
	return sumPnl / sumCt, true
}

// weatherBookPlace is the production entry choke point. R132 freezes NEW lots while preserving
// wxedge research rows and settleWeatherBook. The warning is once per process so every forecast
// scan does not flood audit_log.
func (s *Server) weatherBookPlace(ctx context.Context, ticker, title, side string, price, resolveHours float64) {
	if weatherNewPlacementsFrozen {
		weatherFreezeAuditOnce.Do(func() {
			_ = s.store.Audit(context.WithoutCancel(ctx), "warn", "weather", weatherPlacementFreezeReason, "")
		})
		return
	}
	s.weatherBookPlaceUnfrozen(ctx, ticker, title, side, price, resolveHours)
}

// weatherBookPlaceUnfrozen retains the old book mechanics for settlement/regression tests and for
// a future evidence-backed re-arm. Production calls only weatherBookPlace above.
func (s *Server) weatherBookPlaceUnfrozen(ctx context.Context, ticker, title, side string, price, resolveHours float64) {
	if !s.weatherBookEnabled() || s.ksBlocked() {
		return
	}
	if !s.paperEntryHorizonOK(resolveHours, ticker, title) {
		return
	}
	lo, hi := s.weatherBand()
	if price < lo || price > hi {
		return // tunable entry band (default 20–40¢ — auditor edge-24)
	}
	// SAFETY: the cross-book hedge guard — never bet the opposite side of a market another book
	// is riding. positions=nil: the Weather book is standalone (its own one-lot rule below).
	if r := s.betConflictReason("kalshi", ticker, side, "auto-cons-weather", nil); r != "" {
		return
	}
	// R127 FOUR-BOOK GATE: weather is a SUB-STRATEGY of the KALSHI venue book — available = the
	// book's bank − Σ ALL Kalshi subs' open exposure (computed OUTSIDE wxBookMu; the old wx Bank
	// field is journal-only now). Sizing rides the venue book's bank.
	venueAvail := s.bookAvailableUSD(ctx, vbKalshi)
	s.wxBookMu.Lock()
	{
		b := s.wxBookLoadLocked()
		for _, p := range b.Open {
			if p.Ticker == ticker {
				s.wxBookMu.Unlock()
				return // one open lot per MARKET (either side) — fixed rule
			}
		}
		if len(b.Open) >= 100 {
			s.wxBookMu.Unlock()
			return // runaway guard
		}
	}
	s.wxBookMu.Unlock()
	if venueAvail <= 0 {
		return
	}
	// SHARED ENGINE on the venue book's bank; realized-only edge keyed "auto-cons-wxedge"
	// (betEdge consults wxBookRealizedEdge — no ML contamination possible, bug-266 doctrine).
	stake := s.engineStake(ctx, "kalshi", ticker, side, "auto-cons-wxedge", price, 0, s.bookSizingEquityUSD(ctx, vbKalshi), false, "realized-only")
	contracts := math.Floor(stake / price)
	if contracts < 1 {
		contracts = 1
	}
	// House stop rule: ratio SL = p − r·p, r FROZEN now (mode "ride" or r<=0 ⇒ ride to settlement).
	s.autoMu.Lock()
	slRatio, slMode := s.autoSLTPRatio, s.autoSLTPMode
	s.autoMu.Unlock()
	sl := 0.0
	if slMode != "ride" && slRatio > 0 {
		sl = math.Max(0.01, price-slRatio*price)
	}
	// R107 (operator: every paper book places via the MAKER SIM) — same routing as RawFlow:
	// post at entryBid, fill only on touch-through; option-c divert to an instant tagged taker
	// fill; legacy instant booking only when maker_sim_books is off.
	entryPx, fee := price, s.kalFee(ticker, true, int(contracts), price)
	fillKind, fillRule := "maker", "legacy"
	if s.makerSimBooksOn() {
		divert := ""
		if s.cfg().Auto.MakerAdverseGuard {
			if reason, dep, depSrc, momPtr, momGateC := s.makerPostUnsafe("kalshi", ticker, side); reason != "" {
				divert = reason
				if s.makerGateStampAllowed("kalshi", ticker, "weather") {
					_ = s.store.InsertMakerGated(ctx, "kalshi", ticker, side, "weather", price, 0, dep, momPtr, momGateC, reason, depSrc)
				}
			}
		}
		if divert == "" {
			postPx := s.entryBid(ctx, "kalshi", ticker, side, price)
			if postPx < lo || postPx > hi {
				return // tunable band is an ENTRY rule — applies to the posted price
			}
			// R122 ROUTER: queue-aware maker/taker choice at the actual post price.
			rd := s.routeMakerTaker(ctx, "kalshi", ticker, side, postPx, "weather")
			if rd.Maker {
				cost := contracts * postPx
				s.wxBookMu.Lock()
				b := s.wxBookLoadLocked()
				// venueAvail already subtracts every funded Kalshi open lot and resting post.
				ok := cost <= venueAvail
				for _, p := range b.Open {
					if p.Ticker == ticker {
						ok = false
					}
				}
				s.wxBookMu.Unlock()
				if !ok {
					return
				}
				if s.bookMakerPost(ctx, "weather", "kalshi", ticker, title, side, "weather", postPx, contracts, sl, rd) {
					return // resting; becomes a lot only on touch-through
				}
				return
			}
			divert = rd.Tag() // router chose taker — fall to the divert path, reason tagged
		}
		px, fillCt, _, quoteOK := s.executableTaker(ctx, "kalshi", ticker, side, contracts)
		if !quoteOK || px > price+0.03 || px < lo || px > hi {
			return // moved off the signal or out of band — skip, never chase
		}
		contracts = fillCt
		entryPx, fee = px, s.kalFee(ticker, false, int(contracts), px)
		fillKind, fillRule = "taker", "divert:"+divert
	}
	cost := contracts * entryPx
	s.wxBookMu.Lock()
	b := s.wxBookLoadLocked()
	// R127: venue-book available replaces the old per-book Bank check.
	if cost > venueAvail {
		s.wxBookMu.Unlock()
		return // Kalshi venue book fully invested — skip rather than margin a paper experiment
	}
	for _, p := range b.Open { // re-check under the lock (sizing window)
		if p.Ticker == ticker {
			s.wxBookMu.Unlock()
			return
		}
	}
	// R120 provenance: instant-taker divert path — signal/decision/fill share one stamp.
	nowTS := time.Now().UTC().Format(time.RFC3339)
	lot := kfPos{TS: nowTS, Ticker: ticker,
		Title: title, Side: side, Price: entryPx, Contracts: contracts, Fee: fee, SL: sl,
		FillKind: fillKind, FillRule: fillRule,
		SignalTS: nowTS, DecisionTS: nowTS, FillTS: nowTS}
	s.stampKFExactFee(&lot)
	if !s.stampFundedKFRelation(ctx, &lot, "weather", fillKind) {
		s.wxBookMu.Unlock()
		return
	}
	b.Open = append(b.Open, lot)
	s.wxBookDirty = true
	s.wxBookMu.Unlock()
	_ = s.store.Audit(ctx, "info", "weather",
		fmt.Sprintf("Weather OPEN %s %s ×%.0f @ %.0f¢ ($%.2f, fee $%.2f, sl %.0f¢) book=weather fill_kind=%s fill_rule=%s", ticker, strings.ToUpper(side), contracts, entryPx*100, cost, fee, sl*100, fillKind, fillRule), "")
	s.enqueueLiveMirrorCandidate(liveMirrorCandidate{
		Platform: "kalshi", Ticker: ticker, Title: title, Side: side,
		Source: "auto-cons-weather", Price: entryPx, At: time.Now(),
	})
}

// settleWeatherBook grades the book on the settlement tick: resolution first, then the frozen
// stop against the zero-API cached mark (taker exit fee). Runs beside settleRawFlowBook.
func (s *Server) settleWeatherBook(ctx context.Context) {
	if !s.weatherBookEnabled() && !s.weatherWindingDown() {
		return
	}
	s.wxBookMu.Lock()
	b := s.wxBookLoadLocked()
	if len(b.Open) == 0 {
		s.wxBookMu.Unlock()
		return
	}
	now := time.Now()
	still := b.Open[:0:0]
	changed := false
	var closedNotes []string
	var relationOutcomes []fundedKFOutcome
	for _, p := range b.Open {
		if yv, res := s.store.ResolvedYesForVenue(ctx, "kalshi", p.Ticker); res {
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
			relationOutcomes = append(relationOutcomes, fundedKFOutcome{p, pnl, "weather-settlement"})
			s.wxBookDirty, changed = true, true
			continue
		}
		// R120 provenance: the cached mark stamps every open lot's trajectory (not just SL lots).
		if yes, ok := s.cachedYesMark(p.Ticker); ok && yes > 0 && yes < 1 {
			sidePx := yes
			if strings.EqualFold(p.Side, "NO") || strings.EqualFold(p.Side, "DOWN") {
				sidePx = 1 - yes
			}
			nMk := len(p.Marks)
			kfStampMark(&p, sidePx, "mark")
			if len(p.Marks) != nMk {
				s.wxBookDirty = true
			}
			if p.SL > 0 && sidePx <= p.SL {
				kfMarksClose(&p, sidePx, "stop")
				exitFee := s.kalFee(p.Ticker, false, int(p.Contracts), sidePx) // taker exit
				pnl := p.Contracts*(sidePx-p.Price) - p.Fee - exitFee
				b.Net += pnl
				b.Losses++
				b.Closed = append(b.Closed, kfClosed{kfPos: p, Payout: sidePx,
					PnL: math.Round(pnl*100) / 100, Won: false, SettledTS: now.UTC().Format(time.RFC3339), Reason: "stop-loss"})
				closedNotes = append(closedNotes, fmt.Sprintf("%s stop-loss %+0.2f", p.Ticker, pnl))
				relationOutcomes = append(relationOutcomes, fundedKFOutcome{p, pnl, "weather-stop"})
				s.wxBookDirty, changed = true, true
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
	s.wxBookMu.Unlock()
	s.settleFundedKFOutcomes(ctx, relationOutcomes)
	for _, n := range closedNotes {
		_ = s.store.Audit(ctx, "info", "weather", "Weather CLOSE "+n+" book=weather", "")
	}
	s.weatherBookFlush()
}

// weatherBookFlush persists the book if dirty (atomic tmp+rename; poison-guarded).
func (s *Server) weatherBookFlush() {
	defer s.invalidatePortfolioEquityCache()
	s.wxBookMu.Lock()
	defer s.wxBookMu.Unlock()
	if s.wxBookPoisoned.Load() || !s.wxBookDirty || s.wxBook == nil {
		return
	}
	out, err := json.Marshal(s.wxBook)
	if err != nil {
		return
	}
	path := filepath.Join(s.cfg().DataDir, "weather_book.json")
	tmp := path + ".tmp"
	if os.WriteFile(tmp, out, 0o644) == nil {
		if os.Rename(tmp, path) == nil { // R122 (auditor 438): clear dirty only on rename success
			s.wxBookDirty = false
		}
	}
}

// resetWeatherBook starts a flat current epoch. Pre-reset opens move to the durable reset archive;
// lifetime closed evidence remains and the bank re-seeds from the current allocation.
func (s *Server) resetWeatherBook(ctx context.Context) {
	res, err := s.resetWeatherBookAt(time.Now())
	err = s.finishKFResetResult(ctx, res, err)
	if s.store != nil {
		level := "info"
		msg := fmt.Sprintf("reset P&L: Weather started flat; %d prior opens archived; bank re-seeded to $%.2f", len(res.ArchivedOpen), s.weatherSeedBank())
		if err != nil {
			level, msg = "error", "reset P&L: Weather clean epoch failed: "+err.Error()
		}
		_ = s.store.Audit(context.WithoutCancel(ctx), level, "weather", msg, "")
	}
}

// weatherNetSinceEpoch is the NAV component (allocated portfolio money). Persists through
// wind-down (bug-268 doctrine) so NAV never steps.
func (s *Server) weatherNetSinceEpoch() float64 {
	if !s.weatherBookEnabled() && !s.weatherWindingDown() {
		return 0
	}
	s.wxBookMu.Lock()
	defer s.wxBookMu.Unlock()
	b := s.wxBookLoadLocked()
	return b.Net - b.NetBase
}

// weatherRulesLine is the one-liner shown in the panel/endpoint.
func (s *Server) weatherRulesLine() string {
	lo, hi := s.weatherBand()
	return fmt.Sprintf("NEW LOTS FROZEN (R132); research + existing-lot settlement only · former wxedge %.0f–%.0f¢ band · re-arm requires an event-level probability model", lo*100, hi*100)
}

// weatherMLPayload — the /api/ml "weather" block behind the ML-tab panel + weather-book widget
// (rawFlowMLPayload pattern; zero-API marks).
func (s *Server) weatherMLPayload() map[string]any {
	enabled, winding := s.weatherBookEnabled(), s.weatherWindingDown()
	if !enabled && !winding {
		return map[string]any{"enabled": false}
	}
	s.wxBookMu.Lock()
	b := s.wxBookLoadLocked()
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
	s.wxBookMu.Unlock()
	openCost := 0.0
	lots := make([]map[string]any, 0, len(openSnap))
	for _, p := range openSnap {
		openCost += p.Contracts*p.Price + p.Fee
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
	lo, hi := s.weatherBand()
	return map[string]any{
		"enabled": true, "winding_down": winding && !enabled, "placements_frozen": weatherNewPlacementsFrozen,
		"rules":      s.weatherRulesLine(),
		"band":       fmt.Sprintf("%.0f–%.0f¢", lo*100, hi*100),
		"accounting": "current-reset-epoch",
		"bank":       sanF(math.Round(bank*100) / 100), "equity": sanF(math.Round((bank+netSince)*100) / 100),
		"net": sanF(math.Round(netSince*100) / 100), "closed": wins + losses, "wins": wins,
		"win_rate": sanF(wr), "open": len(openSnap), "open_cost": sanF(math.Round(openCost*100) / 100),
		"open_lots": lots, "equity_series": eqSeries,
		"net_lifetime": sanF(math.Round(netLife*100) / 100), "closed_lifetime": closedLife,
		"wins_lifetime": lifeWins, "losses_lifetime": lifeLosses,
	}
}

// wxBookOpenSide reports the side of an open Weather lot on a ticker ("" = none) — the symmetric
// cross-book hedge guard's input (betConflictReason "weather-hedge").
func (s *Server) wxBookOpenSide(ticker string) string {
	if !s.weatherBookEnabled() && !s.weatherWindingDown() {
		return ""
	}
	s.wxBookMu.Lock()
	defer s.wxBookMu.Unlock()
	b := s.wxBookLoadLocked()
	for _, p := range b.Open {
		if p.Ticker == ticker {
			return p.Side
		}
	}
	return ""
}

// handleWeatherBookReset (POST /api/weatherbook/reset) — the SINGLE-BOOK reset the R106 spec
// calls for ("reset of ONLY the new book"): re-seeds the Weather bank from the current
// allocation + stamps bases; NO other book is touched (handleParlayReset precedent).
func (s *Server) handleWeatherBookReset(w http.ResponseWriter, r *http.Request) {
	if !s.weatherBookEnabled() {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "weather book not enabled (alloc_weather=0)"})
		return
	}
	s.resetWeatherBook(r.Context())
	s.wxBookMu.Lock()
	bank := s.wxBookLoadLocked().Bank
	s.wxBookMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "bank": bank})
}

// handleWeatherBook (GET /api/weatherbook) — the book, its rules, and epoch-relative stats.
func (s *Server) handleWeatherBook(w http.ResponseWriter, r *http.Request) {
	if !s.weatherBookEnabled() {
		writeJSON(w, http.StatusOK, map[string]any{"enabled": false, "note": "set alloc_weather > 0 (equity-fraction mode) to run the Weather book"})
		return
	}
	s.wxBookMu.Lock()
	b := s.wxBookLoadLocked()
	openCost := 0.0
	for _, p := range b.Open {
		openCost += p.Contracts*p.Price + p.Fee
	}
	netSince, wins, losses := kfCurrentEpochStats(b)
	out := map[string]any{
		"enabled":           true,
		"placements_frozen": weatherNewPlacementsFrozen,
		"rules":             s.weatherRulesLine(),
		"accounting":        "current-reset-epoch",
		"bank":              b.Bank, "net": math.Round(netSince*100) / 100,
		"net_epoch": math.Round(netSince*100) / 100,
		"wins":      wins, "losses": losses,
		"open": b.Open, "open_n": len(b.Open), "open_cost": math.Round(openCost*100) / 100,
		"closed_tail": b.Closed, "equity": b.Equity,
		"lifetime": map[string]any{"net": math.Round(b.Net*100) / 100, "wins": b.Wins, "losses": b.Losses},
	}
	s.wxBookMu.Unlock()
	writeJSON(w, http.StatusOK, out)
}
