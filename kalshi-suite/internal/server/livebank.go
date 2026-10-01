package server

// R103 PER-VENUE LIVE BANKROLLS — operator directive: "money cannot move between venues", so live
// mode stops pretending both venues share one wallet. Each venue gets its own bankroll (venue
// truth for Kalshi, config-pinned for PolyUS — that venue exposes NO balance API, a documented
// client gap), its own deployed/available view, its own exposure ceiling, and its own daily-spend
// bucket. Live sizing runs the SAME engine the paper books use (engineStake below — the verbatim
// extraction of autoPlace's sizing block), differing ONLY in the bankroll fed in. Every pre-R103
// rail stays ON TOP of the split, unchanged: the combined live_exposure_cap_usd, the per-order
// live_max_order_usd, the daily live_max_daily_loss_usd window, the realized daily-loss kill-stop,
// the kill switch, session arming, and the R84 arm-sweep. The split can only tighten, never loosen.

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

const liveBankReceiptFreshFor = 2 * time.Minute

func livePolyUSExposureReceiptReason(cacheAt time.Time, cacheErr string, privateOrdersFresh bool, now time.Time) string {
	ttl := liveMirrorPUSRESTCacheTTL
	if privateOrdersFresh {
		ttl = liveMirrorPUSPushCacheTTL
	}
	age := now.Sub(cacheAt)
	if cacheAt.IsZero() || age < 0 || age > ttl || strings.TrimSpace(cacheErr) != "" {
		return "polyus positions/orders exposure stale or unreadable"
	}
	return ""
}

// noteLiveBankReceipt publishes one authenticated venue-NAV observation. Invalid observations are
// published too: once an armed process learns that venue truth is unavailable, it must not fall
// back to the rosier ARM receipt until the venue supplies a new valid balance.
func (s *Server) noteLiveBankReceipt(venue string, bank float64, ok bool) {
	if math.IsNaN(bank) || math.IsInf(bank, 0) || bank < 0 {
		bank, ok = 0, false
	}
	s.liveMu.Lock()
	if s.liveBankNow == nil {
		s.liveBankNow = map[string]float64{}
	}
	if s.liveBankNowAt == nil {
		s.liveBankNowAt = map[string]time.Time{}
	}
	if s.liveBankNowOK == nil {
		s.liveBankNowOK = map[string]bool{}
	}
	s.liveBankNow[venue] = bank
	s.liveBankNowAt[venue] = time.Now()
	s.liveBankNowOK[venue] = ok
	s.liveMu.Unlock()
}

func (s *Server) liveBankReceipt(venue string) (float64, bool, bool) {
	s.liveMu.Lock()
	bank, exists := s.liveBankNow[venue]
	at, atExists := s.liveBankNowAt[venue]
	valid := s.liveBankNowOK[venue]
	s.liveMu.Unlock()
	if !exists || !atExists {
		return 0, false, false
	}
	age := time.Since(at)
	if !valid || age < 0 || age > liveBankReceiptFreshFor {
		return 0, false, true
	}
	return bank, true, true
}

// liveVenueCash forces a new authenticated Kalshi receipt. PolyUS balance is push-authenticated,
// so its receipt is accepted only while that private socket is connected and fresh.
func (s *Server) liveVenueCash(ctx context.Context, venue string) (float64, bool) {
	if s.liveBankCashRead != nil {
		return s.liveBankCashRead(ctx, venue)
	}
	switch venue {
	case "kalshi":
		if s.kal == nil || !s.kal.HasCredentials() {
			return 0, false
		}
		bal, err := s.kal.GetBalance(ctx)
		if err != nil || bal == nil || bal.Balance < 0 {
			return 0, false
		}
		v := float64(bal.Balance) / 100
		// Keep the read-only display cache coherent, but never read that cache to authorize money.
		s.balMu.Lock()
		s.balUSD, s.balAt = v, time.Now()
		s.balMu.Unlock()
		return v, true
	case "polyus":
		if s.pusPriv == nil {
			return 0, false
		}
		bal, _, ok := s.pusPriv.Balance()
		return bal, ok && bal >= 0 && !math.IsNaN(bal) && !math.IsInf(bal, 0)
	default:
		return 0, false
	}
}

// liveVenueNAV returns the venue's raw current account value before any configured safety ceiling.
// Kalshi's authenticated cash balance plus portfolio_value (current positions value) is the sole
// production authority because cash plus position cost is not mark-to-market account value and can
// therefore hide an open-position drawdown. PolyUS has no equivalent current-positions-value
// field, so its authenticated cash plus current exposure remains the best available venue truth.
// The legacy cash seam intentionally keeps that reconstruction for hermetic tests which have no
// venue client; an injected NAV seam always wins and fails closed.
func (s *Server) liveVenueNAV(ctx context.Context, venue string, exposure float64) (float64, bool) {
	valid := func(v float64) bool {
		return v >= 0 && !math.IsNaN(v) && !math.IsInf(v, 0)
	}
	if !valid(exposure) {
		return 0, false
	}
	if s.liveBankNAVRead != nil {
		nav, ok := s.liveBankNAVRead(ctx, venue)
		return nav, ok && valid(nav)
	}
	if s.liveBankCashRead != nil {
		cash, ok := s.liveBankCashRead(ctx, venue)
		nav := cash + exposure
		return nav, ok && valid(cash) && valid(nav)
	}
	if venue == "kalshi" {
		if snapshot, ok := r154KalshiAdmissionSnapshotFromContext(ctx); ok {
			bal := snapshot.Balance()
			if bal == nil || bal.Balance < 0 {
				return 0, false
			}
			cash := float64(bal.Balance) / 100
			s.balMu.Lock()
			s.balUSD, s.balAt = cash, snapshot.ObservedAt()
			s.balMu.Unlock()
			nav, navOK := bal.AccountNAVUSD()
			return nav, navOK && valid(nav)
		}
	}
	switch venue {
	case "kalshi":
		if s.kal == nil || !s.kal.HasCredentials() {
			return 0, false
		}
		bal, err := s.kal.GetBalance(ctx)
		if err != nil || bal == nil || bal.Balance < 0 {
			return 0, false
		}
		cash := float64(bal.Balance) / 100
		// Keep the read-only cash display cache coherent with the same atomic account receipt.
		s.balMu.Lock()
		s.balUSD, s.balAt = cash, time.Now()
		s.balMu.Unlock()
		nav, ok := bal.AccountNAVUSD()
		return nav, ok && valid(nav)
	case "polyus":
		cash, ok := s.liveVenueCash(ctx, venue)
		nav := cash + exposure
		return nav, ok && valid(cash) && valid(nav)
	default:
		return 0, false
	}
}

func (s *Server) capLiveVenueNAV(venue string, nav float64) (float64, string) {
	pin := 0.0
	switch venue {
	case "kalshi":
		pin = s.cfg().Risk.KalshiLiveBankroll
	case "polyus":
		pin = s.cfg().Risk.PolyusLiveBankroll
	}
	if pin > 0 && pin < nav {
		return pin, "config-ceiling"
	}
	return nav, "venue"
}

// liveArmBankroll is the immutable receipt captured during the operator's ARM handshake. Percentage
// safety rails use this baseline for the whole armed session; actual Kelly sizing still re-reads
// venue NAV on every pass, so wins compound and losses de-risk without a fixed-dollar choke.
func (s *Server) liveArmBankroll(venue string) float64 {
	s.liveMu.Lock()
	v := s.liveBankArm[venue]
	s.liveMu.Unlock()
	return v
}

// liveCapitalVenueEnabled prevents a configured-but-disabled wallet from padding another venue's
// percentage rails or drawdown. Capital counts only where current settings could authorize a
// funded destination; account display may still show disabled wallets separately.
func (s *Server) liveCapitalVenueEnabled(venue string) bool {
	r := s.cfg().Risk
	switch venue {
	case "kalshi":
		// Kalshi is the suite's primary/manual LIVE account and is mandatory for every ARM.
		return true
	case "polyus":
		return (r.LiveProspectiveAllocation && r.LiveSystemPolyUS) || r.LiveNewMLPolyUS
	default:
		return false
	}
}

func (s *Server) liveEnabledNAV(kalshiNAV, polyUSNAV float64) float64 {
	total := 0.0
	if s.liveCapitalVenueEnabled("kalshi") {
		total += kalshiNAV
	}
	if s.liveCapitalVenueEnabled("polyus") {
		total += polyUSNAV
	}
	return total
}

func (s *Server) liveArmTotalBankroll() float64 {
	return s.liveEnabledNAV(s.liveArmBankroll("kalshi"), s.liveArmBankroll("polyus"))
}

// liveRiskBankroll compounds the percentage risk rails during the UTC day without moving money
// between venues. ARM remains the immutable starting receipt; realized fee-net profit on this
// venue expands its exposure room, while realized loss contracts it. Before the first 30-second
// venue sample (or after a day rollover), the exact ARM receipt is the conservative baseline.
func (s *Server) liveRiskBankroll(venue string) float64 {
	day := time.Now().UTC().Format("2006-01-02")
	s.liveMu.Lock()
	armed := s.liveArmed
	receiptsInitialized := s.liveBankNow != nil
	deltaC := int64(0)
	if s.liveLossHasBase && s.liveLossDay == day && s.liveLossDeltaV != nil {
		deltaC = s.liveLossDeltaV[venue]
	}
	s.liveMu.Unlock()
	if armed && receiptsInitialized {
		if current, ok, _ := s.liveBankReceipt(venue); ok {
			return math.Max(0, current)
		}
		return 0 // stale, missing, or unreadable current capital fails closed while armed
	}
	return math.Max(0, s.liveArmBankroll(venue)+liveLossUnitsToUSD(deltaC))
}

func (s *Server) liveRiskTotalBankroll() float64 {
	return s.liveEnabledNAV(s.liveRiskBankroll("kalshi"), s.liveRiskBankroll("polyus"))
}

// liveTurnoverAfterOperatorReset preserves the immutable UTC-day accepted-cost total while
// restarting only the available allowance when the operator explicitly opens a new intraday
// risk epoch. The offset expires automatically at the next UTC date.
func (s *Server) liveTurnoverAfterOperatorReset(dayTurnover float64) float64 {
	r := s.cfg().Risk
	at, err := time.Parse(time.RFC3339, r.LiveRiskEpochAt)
	if err == nil && at.UTC().Format("2006-01-02") == time.Now().UTC().Format("2006-01-02") {
		return math.Max(0, dayTurnover-r.LiveRiskEpochTurnoverUSD)
	}
	return dayTurnover
}

// liveRailLimit returns a bankroll-proportional limit, optionally tightened by a positive legacy
// dollar cap. pct<=0 means the proportional rail is not configured; fixed<0 explicitly disables
// the legacy dollar rail. With neither rail configured, ok=false so money paths can fail closed.
func liveRailLimit(bank, pct, fixed float64) (limit float64, ok bool) {
	if pct > 0 {
		if bank <= 0 || math.IsNaN(bank) || math.IsInf(bank, 0) {
			return 0, false
		}
		limit, ok = bank*pct, true
	}
	if fixed > 0 && (!ok || fixed < limit) {
		limit, ok = fixed, true
	}
	if !ok || limit <= 0 || math.IsNaN(limit) || math.IsInf(limit, 0) {
		return 0, false
	}
	return limit, true
}

func railValue(bank, pct, fixed float64) float64 {
	v, _ := liveRailLimit(bank, pct, fixed)
	return v
}

// liveRailsView exposes the effective dollars produced by the percentage formulas. This is a
// receipt, not a control path; all money handlers recompute the same canonical helpers.
func (s *Server) liveRailsView() map[string]any {
	r := s.cfg().Risk
	k, p, total := s.liveRiskBankroll("kalshi"), s.liveRiskBankroll("polyus"), s.liveRiskTotalBankroll()
	if !s.liveCapitalVenueEnabled("kalshi") {
		k = 0
	}
	if !s.liveCapitalVenueEnabled("polyus") {
		p = 0
	}
	armTotal := s.liveArmTotalBankroll()
	day := time.Now().UTC().Format("2006-01-02")
	s.liveMu.Lock()
	turnover := 0.0
	if s.liveDayKey == day {
		turnover = s.liveDaySpend
	}
	s.liveMu.Unlock()
	activeTurnover := s.liveTurnoverAfterOperatorReset(turnover)
	return map[string]any{
		"arm_total": armTotal, "current_risk_total": total,
		"current_risk_kalshi": k, "current_risk_polyus": p,
		"order_pct": r.LiveMaxOrderPct, "order_kalshi": railValue(k, r.LiveMaxOrderPct, r.LiveMaxOrderUSD),
		"order_polyus": railValue(p, r.LiveMaxOrderPct, r.LiveMaxOrderUSD),
		"exposure_pct": r.LiveExposureCapPct, "exposure_total": s.liveCap(),
		"crypto_pct": r.LiveCryptoCapPct, "crypto_cap": s.liveCryptoCapUSD(),
		"cluster_pct": r.LiveClusterCapPct, "cluster_kalshi": railValue(k, r.LiveClusterCapPct, -1),
		"cluster_polyus": railValue(p, r.LiveClusterCapPct, -1),
		"turnover_today": activeTurnover, "turnover_day_total": turnover,
		"turnover_limit": "none; reporting only",
		"daily_loss_pct": r.LiveDailyLossPct, "daily_loss": railValue(armTotal, r.LiveDailyLossPct, r.LiveMaxDailyLossUSD),
		"kelly_max_frac": math.Min(r.LiveKellyMaxFrac, 0.50),
	}
}

// liveVenueBankroll returns authenticated current account value. Kalshi forces an atomic REST
// cash + portfolio_value receipt; PolyUS requires a fresh private-WS balance plus current exposure.
// Optional config bankrolls are safety ceilings, never substitutes for unavailable venue truth.
// "unknown" therefore refuses sizing.
func (s *Server) liveVenueBankroll(ctx context.Context, venue string) (bank float64, src string) {
	exp, expErr := s.liveVenueExposureUSD(ctx, venue)
	if expErr != nil {
		s.noteLiveBankReceipt(venue, 0, false)
		return 0, "unknown"
	}
	rawNAV, navOK := s.liveVenueNAV(ctx, venue, exp)
	if !navOK {
		s.noteLiveBankReceipt(venue, 0, false)
		return 0, "unknown"
	}
	bank, src = s.capLiveVenueNAV(venue, rawNAV)
	s.noteLiveBankReceipt(venue, bank, true)
	return bank, src
}

// refreshLiveRiskReceiptFromExposure closes the proposal-bootstrap loop. The monitor learns
// positions/orders before it builds proposals; it must also publish a current authenticated NAV
// before asking the percentage rails for headroom. Otherwise the two-minute NAV receipt expires,
// liveRiskBankroll returns zero, and the zero cap prevents every candidate from reaching the later
// sizing/budget path that would have refreshed the receipt.
func (s *Server) refreshLiveRiskReceiptFromExposure(ctx context.Context, venue string, exposure float64) bool {
	// The AUTO loop asks for proposal headroom every seven seconds. Reuse the still-current
	// authenticated NAV instead of turning that read-only sizing pass into an unnecessary balance
	// API poll. A successful order path refreshes venue truth again before sizing/placement, and an
	// idle session refreshes here as soon as the two-minute receipt expires.
	if _, ok, _ := s.liveBankReceipt(venue); ok {
		return true
	}
	rawNAV, ok := s.liveVenueNAV(ctx, venue, exposure)
	if !ok {
		s.noteLiveBankReceipt(venue, 0, false)
		return false
	}
	nav, _ := s.capLiveVenueNAV(venue, rawNAV)
	s.noteLiveBankReceipt(venue, nav, true)
	return true
}

// liveSizingBankroll is liveVenueBankroll run through the SAME drawdown throttle the paper engine
// applies in platformBankroll (peak ratchet + halve when >20% under the peak; disabled when the
// operator sets drawdown_floor_pct to 100%). The ONE deliberate difference from paper: no floor-UP.
// Paper's floor lifts sizing equity back toward the epoch-start base after losses ("keep a
// toe-hold") — on real money that would mean sizing on dollars the venue says we no longer have,
// so live equity is truth-bounded: the throttle can only shrink it. Peaks live in the same
// session-scoped s.eqPeak map under "live|<venue>" keys (a paper reset re-anchors them; they
// rebuild from venue truth on the next read).
func (s *Server) liveSizingBankroll(ctx context.Context, venue string) (eq float64, src string) {
	eq, src = s.liveVenueBankroll(ctx, venue)
	return s.liveSizingEquityThrottle(venue, eq, src)
}

// liveCachedSizingBankroll is a read-only preliminary sizing receipt for the dispatcher. The final
// Kalshi handler replaces it with one fresh admission snapshot before any reservation or venue
// call. This keeps proposal sizing from repeating the same balance/positions/orders trip.
func (s *Server) liveCachedSizingBankroll(venue string) (eq float64, src string) {
	var ok bool
	eq, ok, _ = s.liveBankReceipt(venue)
	if !ok {
		return 0, "authenticated-receipt-stale"
	}
	return s.liveSizingEquityThrottle(venue, eq, "authenticated-receipt")
}

func (s *Server) liveSizingEquityThrottle(venue string, eq float64, src string) (float64, string) {
	if eq <= 0 {
		return eq, src
	}
	peakKey := "live|" + venue
	s.eqMu.Lock()
	if s.eqPeak == nil {
		s.eqPeak = map[string]float64{}
	}
	if eq > s.eqPeak[peakKey] {
		s.eqPeak[peakKey] = eq
	}
	peak := s.eqPeak[peakKey]
	s.eqMu.Unlock()
	if fp := s.cfg().Auto.DrawdownFloorPct; fp >= 1 {
		return eq, src // floor at 100% disables the breaker — same rule as platformBankroll
	}
	if peak > 0 && eq < peak*0.80 {
		eq *= 0.5 // drawdown breaker: same 20%-under-peak halving as the paper engine
	}
	return eq, src
}

// liveVenueExposureUSD is the per-venue split of the pre-R103 combined exposure: open live
// positions + resting order value for ONE venue. liveExposureUSD (the combined rail) now sums the
// two — behavior unchanged by construction.
// R105 (auditor A2, R104 queue): venue REST errors are RETURNED, not swallowed — an unreadable
// exposure used to read as $0 and every budget check failed OPEN during a REST outage. Money-path
// callers (liveVenueBudgetCheck / liveVenueRemaining) and the bankroll reader now fail CLOSED on
// any exposure error; an unreadable position/order set can never be treated as zero exposure.
func (s *Server) liveVenueExposureUSD(ctx context.Context, venue string) (float64, error) {
	exp, _, _, err := s.liveVenueExposureBreakdownUSD(ctx, venue)
	return exp, err
}

// liveVenueExposureBreakdownUSD performs one authenticated account read and returns both total
// outstanding exposure and its crypto subset. Keeping the two in one scan avoids adding another
// positions/orders round trip to the final real-money boundary.
func (s *Server) liveVenueExposureBreakdownUSD(ctx context.Context, venue string) (exp, cryptoExp float64,
	cryptoKnown bool, err error) {
	cryptoKnown = true
	restingByID := map[string]float64{}
	restingCryptoByID := map[string]float64{}
	switch venue {
	case "kalshi":
		if s.kal == nil {
			return 0, 0, true, nil // hermetic tests build a Server without a venue client
		}
		var (
			pos    []kalshi.MarketPosition
			orders []kalshi.Order
		)
		if snapshot, ok := r154KalshiAdmissionSnapshotFromContext(ctx); ok {
			pos, orders = snapshot.Positions(), snapshot.Orders()
		} else {
			var perr error
			pos, perr = s.kal.GetPositions(ctx)
			if perr != nil {
				return 0, 0, false, fmt.Errorf("kalshi positions unreadable: %w", perr)
			}
			if s.kal.Armed() {
				var oerr error
				orders, oerr = s.kal.GetOrders(ctx)
				if oerr != nil {
					return exp, cryptoExp, false, fmt.Errorf("kalshi orders unreadable: %w", oerr)
				}
			}
		}
		{
			s.liveMu.Lock()
			tbd := s.kalPosTBD // R48: closed-but-undetermined tickers don't eat the cap
			s.liveMu.Unlock()
			for _, p := range pos {
				if tbd[p.Ticker] {
					continue
				}
				risk := math.Abs(p.ExposureUSD()) // fixed-point truth via the accessor
				class, classErr := s.liveKalshiAccountInstrumentCryptoClass(ctx, p.Ticker)
				if classErr != nil {
					return exp, cryptoExp, false, fmt.Errorf("kalshi position crypto attribution: %w", classErr)
				}
				exp, cryptoExp, cryptoKnown = liveCryptoAddRisk(exp, cryptoExp, cryptoKnown, risk, class)
			}
		}
		// R84 (bug 35): resting order value counts too — armed sessions only (disarmed can't place).
		if s.kal.Armed() {
			for _, o := range orders {
				if o.RemainFP.Float() <= 0 {
					continue
				}
				risk, riskKnown := kalshiRestingRisk(o)
				if !riskKnown {
					return exp, cryptoExp, false, fmt.Errorf("kalshi resting order %q omitted current direction or price truth", strings.TrimSpace(o.OrderID))
				}
				class, classErr := s.liveKalshiAccountInstrumentCryptoClass(ctx, o.Ticker)
				if classErr != nil {
					return exp, cryptoExp, false, fmt.Errorf("kalshi resting-order crypto attribution: %w", classErr)
				}
				exp, cryptoExp, cryptoKnown = liveCryptoAddRisk(exp, cryptoExp, cryptoKnown, risk, class)
				if id := strings.TrimSpace(o.OrderID); id != "" {
					restingByID[id] = risk
					if class == liveCryptoYes {
						restingCryptoByID[liveCryptoRestingKey("kalshi", id)] = risk
					}
				}
			}
		}
	case "polyus":
		if s.polyUSAuth == nil && s.pusPriv == nil {
			return 0, 0, true, nil // venue not configured in this process; no PolyUS exposure can be opened here
		}
		privateOrders, privateOK := []polymarketus.PUSOrder(nil), false
		if s.pusPriv != nil {
			privateOrders, privateOK = s.pusPriv.Orders()
		}
		s.pusLiveMu.Lock()
		positions := append([]polymarketus.PUSPosition(nil), s.pusPosC...)
		orders := append([]polymarketus.PUSOrder(nil), s.pusOrdsC...)
		cacheAt, cacheErr := s.pusLiveAt, s.pusErrC
		s.pusLiveMu.Unlock()
		// Positions are REST-reconciled even when the private order stream is healthy. A mutation
		// deliberately zeros pusLiveAt, so no second order can borrow the pre-mutation exposure view.
		// Between mutations, the 75s push-health allowance matches buildLiveSnapshot's intentional
		// 60s position reconciliation cadence; without a healthy private order stream use the strict
		// 15s REST-only allowance.
		if why := livePolyUSExposureReceiptReason(cacheAt, cacheErr, privateOK, time.Now()); why != "" {
			return 0, 0, false, fmt.Errorf("%s", why)
		}
		if privateOK {
			orders = privateOrders
		}
		for _, p := range positions {
			if !p.Expired && p.Net != 0 {
				risk := math.Abs(p.Cost)
				exp, cryptoExp, cryptoKnown = liveCryptoAddRisk(exp, cryptoExp, cryptoKnown, risk,
					liveCryptoInstrumentClass("polyus", p.Slug, p.Title, ""))
			}
		}
		for _, o := range orders {
			if strings.EqualFold(o.Action, "BUY") && o.Leaves > 0 {
				risk := o.Leaves * o.Price
				class := liveCryptoInstrumentClass("polyus", o.Slug, o.Title, "")
				exp, cryptoExp, cryptoKnown = liveCryptoAddRisk(exp, cryptoExp, cryptoKnown, risk, class)
				if id := strings.TrimSpace(o.ID); id != "" {
					restingByID[id] = risk
				}
				switch class {
				case liveCryptoYes:
					if id := strings.TrimSpace(o.ID); id != "" {
						restingCryptoByID[liveCryptoRestingKey("polyus", id)] = risk
					}
				}
			}
		}
	default:
		return 0, 0, false, fmt.Errorf("unsupported LIVE destination %q", venue)
	}
	if s.store == nil {
		return exp, cryptoExp, false, fmt.Errorf("pending-risk store unavailable")
	}
	pendingRows, pendingErr := s.store.ActiveLivePendingRisk(ctx)
	if pendingErr != nil {
		return exp, cryptoExp, false, fmt.Errorf("pending crypto risk unreadable: %w", pendingErr)
	}
	pending, _, pendingErr := r148PendingRiskOverlayRows(pendingRows, venue, restingByID)
	if pendingErr != nil {
		return exp, cryptoExp, false, fmt.Errorf("%s pending risk unreadable: %w", venue, pendingErr)
	}
	exp += pending
	pendingCrypto, pendingKnown := livePendingCryptoOverlay(pendingRows, restingCryptoByID, venue)
	if !pendingKnown {
		cryptoKnown = false
	} else {
		cryptoExp += pendingCrypto
	}
	return exp, cryptoExp, cryptoKnown, nil
}

// liveVenueCap returns a venue's bankroll-proportional outstanding-exposure ceiling, optionally
// tightened by its positive legacy dollar cap and always bounded by the combined cap.
func (s *Server) liveVenueCap(venue string) float64 {
	if !s.liveCapitalVenueEnabled(venue) {
		return 0
	}
	var fixed float64
	switch venue {
	case "kalshi":
		fixed = s.cfg().Risk.LiveKalshiCapUSD
	case "polyus":
		fixed = s.cfg().Risk.LivePolyusCapUSD
	}
	if pct := s.cfg().Risk.LiveExposureCapPct; pct > 0 {
		cap, ok := liveRailLimit(s.liveRiskBankroll(venue), pct, fixed)
		if !ok {
			return 0
		}
		if combined := s.liveCap(); combined > 0 && cap > combined {
			cap = combined
		}
		return cap
	}
	if fixed > 0 {
		return fixed
	}
	return s.liveCap()
}

// liveVenueBudgetCheck enforces BOTH exposure ceilings for one prospective order: the combined
// live cap FIRST (the pre-R103 rail, kept verbatim), then the order's own venue cap — a proposal
// can only spend its own venue's budget. Returns "" when allowed, else the refusal. One exposure
// read per venue (the Kalshi read is the REST pair the old combined check already paid).
func (s *Server) liveVenueBudgetCheck(ctx context.Context, venue string, cost float64) string {
	return s.liveVenueBudgetCheckClassified(ctx, venue, cost, 0, true)
}

// liveVenueBudgetCheckFor adds the aggregate crypto sleeve to the canonical final account scan.
// A nil candidate preserves the legacy/manual-independent budget API; every actual order handler
// supplies its exact destination/ticker/side identity before mutation.
func (s *Server) liveVenueBudgetCheckFor(ctx context.Context, venue string, cost float64,
	candidate *liveMirrorCandidate) string {
	if candidate == nil {
		return s.liveVenueBudgetCheckClassified(ctx, venue, cost, 0, true)
	}
	cryptoCost, known := liveCryptoBundleCost([]liveMirrorCandidate{*candidate}, cost)
	return s.liveVenueBudgetCheckClassified(ctx, venue, cost, cryptoCost, known)
}

func (s *Server) liveVenueBudgetCheckForBundle(ctx context.Context, venue string, cost float64,
	candidates []liveMirrorCandidate) string {
	cryptoCost, known := liveCryptoBundleCost(candidates, cost)
	return s.liveVenueBudgetCheckClassified(ctx, venue, cost, cryptoCost, known)
}

// Staged legs are separately routed and unwindable, unlike one atomic KXMVE product. Their
// admission therefore carries the same principal-weighted fee share as its durable reservation,
// rather than charging the atomic-product rule or changing attribution after submission.
func (s *Server) liveVenueBudgetCheckForStagedBundle(ctx context.Context, venue string, cost,
	cryptoCost float64, cryptoKnown bool) string {
	return s.liveVenueBudgetCheckClassified(ctx, venue, cost, cryptoCost, cryptoKnown)
}

func (s *Server) liveVenueBudgetCheckClassified(ctx context.Context, venue string, cost, proposedCrypto float64,
	proposedCryptoKnown bool) string {
	venue = strings.ToLower(strings.TrimSpace(venue))
	if venue != "kalshi" && venue != "polyus" {
		return "unsupported LIVE destination"
	}
	if venue == "polyus" && !s.livePolyUSDestinationEnabled() {
		return "PolyUS LIVE destination is disabled"
	}
	kExp, kCrypto, kCryptoKnown, kErr := s.liveVenueExposureBreakdownUSD(ctx, "kalshi")
	pExp, pCrypto, pCryptoKnown, pErr := 0.0, 0.0, true, error(nil)
	// A disabled destination has no right to block or enlarge the active venue. Its credentials,
	// account polling, and intermittent 429s are operational telemetry only in Kalshi-only mode.
	if s.livePolyUSDestinationEnabled() {
		pExp, pCrypto, pCryptoKnown, pErr = s.liveVenueExposureBreakdownUSD(ctx, "polyus")
	}
	if kErr != nil || pErr != nil {
		s.noteLiveBankReceipt(venue, 0, false)
		// R105 (auditor A2): FAIL CLOSED — during a venue REST outage the exposure used to read $0
		// and this check waved orders through blind. No readable exposure ⇒ no new orders.
		e := kErr
		if e == nil {
			e = pErr
		}
		return fmt.Sprintf("live exposure unreadable (%v) — refusing to place while blind (fail-closed)", e)
	}
	targetExp := kExp
	if venue == "polyus" {
		targetExp = pExp
	}
	// Refresh the target venue's authenticated account value on every admission decision.
	// Withdrawals and mark-to-market losses shrink rails before another order; deposits cannot
	// enlarge the Adaptive Allocation Model until this receipt succeeds.
	rawNAV, navOK := s.liveVenueNAV(ctx, venue, targetExp)
	if !navOK {
		s.noteLiveBankReceipt(venue, 0, false)
		return venue + " authenticated account NAV is stale or unreadable — refusing new exposure (fail-closed)"
	}
	currentNAV, _ := s.capLiveVenueNAV(venue, rawNAV)
	s.noteLiveBankReceipt(venue, currentNAV, true)
	combinedCap := s.liveCap()
	if combinedCap <= 0 {
		return "combined live exposure cap unavailable — refusing while bankroll rails are unknown"
	}
	if kExp+pExp+cost > combinedCap+0.001 {
		return fmt.Sprintf("would exceed the combined live exposure cap $%.2f", combinedCap)
	}
	vExp := kExp
	if venue == "polyus" {
		vExp = pExp
	}
	if vc := s.liveVenueCap(venue); vExp+cost > vc+0.001 {
		return fmt.Sprintf("would exceed the %s live exposure cap $%.2f", venue, vc)
	}
	if msg := s.liveCryptoProposedBudgetReason(kCrypto+pCrypto, proposedCrypto,
		kCryptoKnown && pCryptoKnown, proposedCryptoKnown); msg != "" {
		return msg
	}
	return ""
}

// liveVenueRemaining returns each venue's proposal budget headroom: venue cap − venue exposure,
// outer-bounded by the COMBINED cap's remaining (the pre-R103 rail — the split can only tighten).
// One Kalshi REST pair (positions + orders, the same pair the old combined check paid); the
// PolyUS side reads the cached live view.
func (s *Server) liveVenueRemaining(ctx context.Context) (remKal, remPus float64) {
	kExp, kErr := s.liveVenueExposureUSD(ctx, "kalshi")
	pExp, pErr := 0.0, error(nil)
	polyUSEnabled := s.livePolyUSDestinationEnabled()
	if polyUSEnabled {
		pExp, pErr = s.liveVenueExposureUSD(ctx, "polyus")
	}
	if kErr != nil || pErr != nil {
		if kErr != nil {
			s.noteLiveBankReceipt("kalshi", 0, false)
		}
		if pErr != nil {
			s.noteLiveBankReceipt("polyus", 0, false)
		}
		return 0, 0 // R105 (A2): headroom unknowable ⇒ propose nothing (fail-closed, matches liveVenueBudgetCheck)
	}
	// Refresh capital BEFORE computing caps. This runs even when there are no proposals, so an
	// idle armed session cannot self-starve when the prior NAV receipt ages out. Venue truth is
	// still mandatory: a failed cash read returns no headroom rather than falling back to ARM cash.
	if !s.refreshLiveRiskReceiptFromExposure(ctx, "kalshi", kExp) {
		return 0, 0
	}
	if polyUSEnabled && !s.refreshLiveRiskReceiptFromExposure(ctx, "polyus", pExp) {
		return 0, 0
	}
	combinedCap := s.liveCap()
	if combinedCap <= 0 {
		return 0, 0
	}
	combined := combinedCap - (kExp + pExp)
	if combined < 0 {
		combined = 0
	}
	remKal = s.liveVenueCap("kalshi") - kExp
	if polyUSEnabled {
		remPus = s.liveVenueCap("polyus") - pExp
	}
	if remKal > combined {
		remKal = combined
	}
	if remPus > combined {
		remPus = combined
	}
	if remKal < 0 {
		remKal = 0
	}
	if remPus < 0 {
		remPus = 0
	}
	return remKal, remPus
}

// liveVenueDayTurnover returns today's gross accepted live order cost for one venue. It is an
// observation, not current exposure and not an admission rail.
func (s *Server) liveVenueDayTurnover(venue string) float64 {
	day := time.Now().UTC().Format("2006-01-02")
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	if s.liveDayKey != day {
		return 0
	}
	return s.liveDaySpendV[venue]
}

// engineStake is THE sizing engine — R103: the verbatim extraction of autoPlace's sizing block so
// paper and live cannot fork. Flat/pct base → fractional Kelly when authenticated money-facing
// evidence is known on a non-Paper route
// (stake = bank × edge/(1−price) × per-signal Kelly fraction) → cross-confirm multiplier →
// 25%-of-bankroll per-market cap → (allocMult) the R78 EV-share family scaling — a PAPER-auto-book
// overlay, so the paper caller passes true and live/RawFlow pass false — → min-stake floor
// (fee-bleed protection, clamped to 25% of the bankroll). stakeIn keeps autoPlace's exact
// semantics: >0 = explicit stake (skips base/Kelly), <0 = AMTKELLY sentinel (|stakeIn| becomes the
// Kelly multiplier), 0 = size from the engine. kedgeOverride "" = the configured KellyEdge mirror
// (paper + live); RawFlow passes "realized-only" (auditor r31 bug 266 — no ML in the experiment).
// Same bankroll in ⇒ same stake out — pinned by TestR103LiveSizingMatchesPaper.
func (s *Server) engineStake(ctx context.Context, platform, ticker, side, source string, price, stakeIn, bank float64, allocMult bool, kedgeOverride string) float64 {
	mult := 1.0
	stake := stakeIn
	if stake < 0 { // AMTKELLY sentinel: a NEGATIVE stake means "size by Kelly × |stake|"
		mult = -stake
		stake = 0
	}
	if stake <= 0 {
		s.autoMu.Lock()
		flat, pct, kf, kedge := s.autoStake, s.cfg().Auto.StakePct, s.autoKellyFrac, s.autoKellyEdge
		s.autoMu.Unlock()
		if kedgeOverride != "" {
			kedge = kedgeOverride
		}
		base := flat
		if pct > 0 {
			base = bank * pct // a slice of THIS bankroll
		}
		stake = base
		// KELLYSIZE — cash/demo routes may size from their independently authorized inputs. The
		// allocMult=true caller is legacy Paper: model, Paper-fill and signal-log results are not
		// authenticated profit evidence, so Phase 0 leaves its base size untouched. Corrected Paper's
		// dedicated executor bypasses this engine and always submits exactly one share.
		// stake = bankroll × (edge/(1−price)) × kf, with the per-signal probation/reliability
		// fraction (EVFLOORSIZE) exactly as the paper engine always did.
		if !allocMult && kf > 0 && price > 0 && price < 1 {
			if edge, ok := s.betEdge(ctx, platform, ticker, side, source, price, kedge); ok && edge > 0 {
				stake = bank * (edge / (1 - price)) * s.signalKellyFrac(platform, source, ticker, side, price, kf)
			}
		}
		stake *= mult
		if cap := 0.25 * bank; cap > 0 && stake > cap {
			stake = cap // never let one bet exceed 25% of the bankroll, even after the multiplier
		}
		if stake < 1 {
			stake = 1 // need at least ~1 contract
		}
	}
	// R128 SCOREBOARD-WEIGHT ALLOCATION (paper auto only; operator's core order): scale the
	// family's stake by its share of positive lifetime realized ¢/unit — the SAME numbers the
	// scoreboard shows, mean-normalized within the positive roster (average positive member ×1;
	// measured-negative ~0 — logging never stops; policy-inverted families ride their 🔄 edge).
	// This REPLACES the R78 LB-EV table as the live multiplier (that table still builds for
	// display). Live + RawFlow pass allocMult=false, exactly as before.
	if allocMult {
		if am := s.scoreWeightMult(source, platform); am != 1 {
			stake *= am
			if cap := 0.25 * bank; cap > 0 && stake > cap {
				stake = cap
			}
		}
	}
	// Phase 2b MIN-STAKE FLOOR: tiny bets pay a brutal fee %; lift to the floor but never above
	// 25% of the bankroll so a small bankroll can't be over-concentrated by the floor.
	if minS := s.cfg().Auto.MinStakeUsd; minS > 0 {
		if lim := 0.25 * bank; lim > 0 && minS > lim {
			minS = lim
		}
		if stake < minS {
			stake = minS
		}
	}
	return stake
}

// liveEngineOrderUSD sizes ONE live order for a venue through the shared engine and the live
// rails: engineStake on the venue's throttled live bankroll, then the bankroll-percentage order
// cap (optionally tightened by a legacy dollar cap). Returns the target
// order dollars and the bankroll source tag.
// R105 (operator, repeated ask + auditor A5): the flat $/bet fallback is GONE with the
// live_order_usd knob — live stakes come from the SAME engine as paper, full stop. An
// unknown/zero bankroll returns usd=0 ("cannot size") and callers SKIP the candidate with the
// reason logged; the old fallback both invented a stake and mislabeled it with the caller's src
// tag (A5). R103 (auditor r31 bug 264): the bankroll is resolved by the CALLER once per proposal
// pass (liveSizingBankroll fires venue REST) and threaded here per candidate.
func (s *Server) liveEngineOrderUSDWith(ctx context.Context, venue, ticker, side, source string, price, bank float64, src string) (float64, string) {
	if bank <= 0 {
		return 0, src // R105: refuse to size — no flat fallback, callers skip + say why
	}
	usd := s.engineStake(ctx, venue, ticker, side, source, price, 0, bank, false, "")
	r := s.cfg().Risk
	maxOrd, ok := liveRailLimit(bank, r.LiveMaxOrderPct, r.LiveMaxOrderUSD)
	if !ok {
		return 0, "rail-unknown"
	}
	if usd > maxOrd {
		usd = maxOrd
	}
	if usd < price { // less than one contract after the rails → the unit floor keeps proposals alive
		usd = price
	}
	return usd, src
}

// adaptiveProofKellyFrac turns the LIVE-ONLY Kelly setting into a ceiling, clamped to half-Kelly.
// The paper engine's auto.kelly_frac is deliberately unrelated and unchanged. The route-specific
// executable proof supplies two independent shrink factors:
// stability = lower/mean (confidence width) and strength = lower/(2×required floor). Each retains
// a conservative 75% base because the lower bound has already paid the statistical uncertainty
// penalty. The P&L throttle is continuous through the operator's -3%/-5%/-8% soft brakes; -8%
// admits no new directional exposure, before the separate -10% hard daily kill. Invalid evidence
// fails shut.
func livePnLRiskMultiplier(equityRatio float64) float64 {
	if math.IsNaN(equityRatio) || math.IsInf(equityRatio, 0) || equityRatio <= 0 {
		return 0
	}
	drawdown := math.Max(0, 1-equityRatio)
	switch {
	case drawdown <= 0.03:
		return 1 - 0.25*(drawdown/0.03) // 1.00 at flat/profit, 0.75 at -3%
	case drawdown <= 0.05:
		return 0.75 - 0.25*((drawdown-0.03)/0.02) // 0.50 at -5%
	case drawdown < 0.08:
		return 0.50 - 0.50*((drawdown-0.05)/0.03) // zero at -8%
	default:
		return 0
	}
}

func adaptiveProofKellyFrac(maxFrac, proofMean, proofLower, requiredFloor, drawdownRatio float64) float64 {
	vals := []float64{maxFrac, proofMean, proofLower, requiredFloor, drawdownRatio}
	for _, v := range vals {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0
		}
	}
	if maxFrac <= 0 || requiredFloor <= 0 || proofLower+1e-12 < requiredFloor ||
		proofMean <= 0 || proofLower <= 0 || proofLower > proofMean+1e-12 || drawdownRatio <= 0 {
		return 0
	}
	maxFrac = math.Min(maxFrac, 0.50)
	stability := clampF(proofLower/proofMean, 0, 1)
	strength := clampF(proofLower/(2*requiredFloor), 0, 1)
	proofMult := (0.75 + 0.25*stability) * (0.75 + 0.25*strength)
	dd := livePnLRiskMultiplier(drawdownRatio)
	return maxFrac * proofMult * dd
}

// liveProofOrderUSD is the Adaptive Allocation Model live-mirror sizing authority. The same always-valid,
// route-specific, current-fee-adjusted proof that authorizes the strategy supplies both the Kelly
// edge and the adaptive fraction. Raw/legacy ML proof and a flat/minimum-dollar fallback are
// intentionally absent; the opaque New-ML v2 bridge reaches this function only after its separate
// current-model, actual-Paper-result, route, book, fee, and operator-authority gates pass.
// Formula:
//
//	dollars = current venue NAV × adaptive fractional-Kelly × proof_lower / (1 - executable price)
//
// Current NAV compounds wins and shrinks after losses. The percentage order rail is a final
// ceiling; if the proof-sized amount cannot buy one contract, no order is emitted rather than
// rounding risk upward.
func (s *Server) liveProofOrderUSD(venue string, bank, price, proofMean, proofLower float64) (usd, effectiveFrac float64) {
	return s.liveProofOrderUSDForFloor(venue, bank, price, proofMean, proofLower, s.liveMirrorEdgeFloor())
}

// liveProofOrderUSDForFloor is the authorization-aware Adaptive Allocation Model entry point.
// Sealed/New-ML callers retain normal AUTO's floor through liveProofOrderUSD. The prospective
// allocation lane passes its separately disclosed positive-LB floor, so authorization and sizing
// cannot silently disagree after a route has already cleared proof.
func (s *Server) liveProofOrderUSDForFloor(venue string, bank, price, proofMean, proofLower, requiredFloor float64) (usd, effectiveFrac float64) {
	return s.liveProofOrderUSDWithReferenceAndFloor(bank, s.liveArmBankroll(venue), price, proofMean, proofLower, requiredFloor)
}

// liveProofOrderUSDWithReference keeps the compounding/drawdown reference in the same portfolio
// as bank. New ML passes its destination sleeve equity and that sleeve's grant; generic live
// portfolios keep using their venue NAV and arm-time venue NAV through liveProofOrderUSD.
func (s *Server) liveProofOrderUSDWithReference(bank, reference, price, proofMean, proofLower float64) (usd, effectiveFrac float64) {
	return s.liveProofOrderUSDWithReferenceAndFloor(bank, reference, price, proofMean, proofLower, s.liveMirrorEdgeFloor())
}

func (s *Server) liveProofOrderUSDWithReferenceAndFloor(bank, reference, price, proofMean, proofLower, requiredFloor float64) (usd, effectiveFrac float64) {
	if bank <= 0 || price <= 0 || price >= 1 || proofLower <= 0 ||
		reference <= 0 || math.IsNaN(bank) || math.IsInf(bank, 0) || math.IsNaN(reference) || math.IsInf(reference, 0) ||
		math.IsNaN(proofLower) || math.IsInf(proofLower, 0) || requiredFloor <= 0 ||
		math.IsNaN(requiredFloor) || math.IsInf(requiredFloor, 0) {
		return 0, 0
	}
	maxFrac := s.cfg().Risk.LiveKellyMaxFrac
	effectiveFrac = adaptiveProofKellyFrac(maxFrac, proofMean, proofLower, requiredFloor, bank/reference)
	if effectiveFrac <= 0 {
		return 0, 0
	}
	fstar := proofLower / (1 - price)
	if fstar > 1 {
		fstar = 1
	}
	usd = bank * effectiveFrac * fstar
	r := s.cfg().Risk
	if maxOrd, ok := liveRailLimit(bank, r.LiveMaxOrderPct, r.LiveMaxOrderUSD); ok {
		usd = math.Min(usd, maxOrd)
	} else {
		return 0, 0
	}
	if usd+1e-12 < price {
		return 0, effectiveFrac
	}
	return usd, effectiveFrac
}

// liveEngineOrderUSD is the resolve-then-size convenience (single-order callers + tests): ONE
// bankroll resolution, then the shared rails above.
func (s *Server) liveEngineOrderUSD(ctx context.Context, venue, ticker, side, source string, price float64) (usd float64, src string) {
	bank, src := s.liveSizingBankroll(ctx, venue)
	return s.liveEngineOrderUSDWith(ctx, venue, ticker, side, source, price, bank, src)
}
