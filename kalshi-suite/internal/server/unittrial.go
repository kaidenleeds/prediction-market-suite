package server

// R132 BANKROLL-NEUTRAL UNIT RESEARCH. Every signal family gets a one-share executable-ask
// observation before any paper-book allocation decision. This answers whether an opportunity has
// edge; portfolio books separately answer how much collateral can safely express it.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// executableAsk is the single side-aware quote boundary used by research and live proposals.
// It always returns the price of the outcome being BOUGHT, never a YES midpoint. Research passes
// allowREST=false to stay off the hot path; live proposals may use a bounded REST fallback.
func (s *Server) executableAsk(ctx context.Context, platform, ticker, side string, allowREST bool) (ask, depth float64, source string, ok bool) {
	platform = strings.ToLower(strings.TrimSpace(platform))
	side = strings.ToUpper(strings.TrimSpace(side))
	if side != "YES" && side != "NO" {
		return 0, 0, "", false
	}
	switch platform {
	case "kalshi":
		if s.kal == nil {
			return 0, 0, "", false
		}
		ob, _, fresh := s.kal.LiveBook(ticker, 5*time.Second)
		source = "kalshi-ws"
		if (!fresh || ob == nil) && allowREST {
			ob, _ = s.kal.GetOrderbook(ctx, ticker)
			source = "kalshi-rest"
		}
		if ob == nil || (!fresh && !allowREST) {
			// The all-market ticker WS carries the true YES BBO even when this
			// ticker is outside the priority orderbook-depth subset.  Use that
			// executable touch before considering REST; depth remains unknown.
			if bid, yesAsk, bboOK := s.kal.LiveBidAsk(ticker); bboOK {
				if side == "NO" {
					return 1 - bid, 0, "kalshi-ticker-ws", true
				}
				return yesAsk, 0, "kalshi-ticker-ws", true
			}
			return 0, 0, source, false
		}
		if side == "NO" {
			if len(ob.YesBids) == 0 {
				return 0, 0, source, false
			}
			ask, depth = 1-ob.YesBids[0].Price, ob.YesBids[0].Size
		} else {
			if len(ob.YesAsks) == 0 {
				return 0, 0, source, false
			}
			ask, depth = ob.YesAsks[0].Price, ob.YesAsks[0].Size
		}
		return ask, depth, source, ask > 0 && ask < 1
	case "polyus":
		if s.polyUSWS != nil {
			bid, yesAsk, fresh := s.polyUSWS.LiveBidAsk(ticker)
			if fresh && bid > 0 && yesAsk > bid && yesAsk < 1 {
				_, _, bidDepth, askDepth, bookOK := s.polyUSWS.Book3(ticker)
				if !bookOK {
					bidDepth, askDepth = 0, 0
				}
				if side == "NO" {
					return 1 - bid, bidDepth, "polyus-ws", true
				}
				return yesAsk, askDepth, "polyus-ws", true
			}
		}
		if !allowREST {
			return 0, 0, "polyus-ws", false
		}
		// First fallback is the suite's already-fetched market snapshot (no extra request).
		for _, m := range s.polyUSSnapshot() {
			if m.Slug == ticker && m.Bid > 0 && m.Ask > m.Bid && m.Ask < 1 {
				if side == "NO" {
					return 1 - m.Bid, m.BidSz, "polyus-snapshot", true
				}
				return m.Ask, m.AskSz, "polyus-snapshot", true
			}
		}
		if s.polyUS != nil {
			if b, _, code, _ := s.polyUS.BookFull(ctx, ticker); code == http.StatusOK && b != nil &&
				b.BestBid > 0 && b.BestAsk > b.BestBid && b.BestAsk < 1 && strings.Contains(strings.ToUpper(b.State), "OPEN") {
				if side == "NO" {
					return 1 - b.BestBid, b.BestBidQty, "polyus-rest", b.BestBidQty > 0
				}
				return b.BestAsk, b.BestAskQty, "polyus-rest", b.BestAskQty > 0
			}
		}
		return 0, 0, "polyus-rest", false
	case "polymarket":
		if s.poly == nil {
			return 0, 0, "", false
		}
		m, found := s.poly.CachedMarketByCondition(ticker)
		if !found || m.Closed || !m.Active || !m.AcceptingOrders {
			return 0, 0, "polyint-cache", false
		}
		outcome := 0
		if side == "NO" {
			outcome = 1
		}
		if _, a, _, askDepth, _, fresh := s.poly.CLOBOutcomeBBO(ticker, outcome); fresh {
			return a, askDepth, "polyint-ws", true
		}
		if m.BestBid <= 0 || m.BestAsk <= m.BestBid || m.BestAsk >= 1 {
			return 0, 0, "polyint-cache", false
		}
		if side == "NO" {
			return 1 - m.BestBid, 0, "polyint-cache", true
		}
		return m.BestAsk, 0, "polyint-cache", true
	}
	return 0, 0, "", false
}

// executableContracts limits an immediate paper/taker fill to whole contracts visibly offered at
// the quoted top level. A quote without size is not executable evidence and therefore fills zero.
func executableContracts(desired, touchDepth float64) float64 {
	if desired < 1 || touchDepth < 1 {
		return 0
	}
	return math.Floor(math.Min(desired, touchDepth))
}

func (s *Server) executableTaker(ctx context.Context, platform, ticker, side string, desired float64) (ask, contracts float64, source string, ok bool) {
	ask, depth, source, ok := s.executableAsk(ctx, platform, ticker, side, true)
	if !ok {
		return 0, 0, source, false
	}
	contracts = executableContracts(desired, depth)
	return ask, contracts, source, contracts >= 1
}

func (s *Server) unitExecutableQuote(sig storage.Signal) (ask, depth float64, source string, ok bool) {
	return s.executableAsk(context.Background(), sig.Platform, sig.Ticker, sig.Side, false)
}

// unitTrialFeeExact captures the venue's observation-time taker authority. A fallback schedule is
// useful for discovery displays, but it is not an economic receipt and can never enter route proof.
func (s *Server) unitTrialFeeExact(platform, ticker string, price float64) (float64, string, bool) {
	switch strings.ToLower(strings.TrimSpace(platform)) {
	case "kalshi":
		fee, known, source := s.kalFeeExact(ticker, false, 1, price)
		return fee, "kalshi:" + strings.TrimSpace(source), known && fee >= 0 && !math.IsNaN(fee) && !math.IsInf(fee, 0)
	case "polyus":
		fee, known := s.polyUSResearchFeeExact(ticker, price)
		return fee, "polyus:market.feeCoefficient", known && fee >= 0 && !math.IsNaN(fee) && !math.IsInf(fee, 0)
	case "polymarket":
		if s.poly == nil {
			return 0, "", false
		}
		fee, known := s.poly.ClobFeeUSD(ticker, 1, price, true)
		return fee, "polyint:clob-fee-schedule", known && fee >= 0 && !math.IsNaN(fee) && !math.IsInf(fee, 0)
	default:
		return 0, "", false
	}
}

func (s *Server) unitTrialConsiderOne(ctx context.Context, sig storage.Signal, sourcePrefix string) unitTrialFunnelResult {
	if sig.SignalType == "" || sig.Ticker == "" {
		return unitTrialFunnelResult{Reason: "missing_signal_or_ticker_identity"}
	}
	if strings.EqualFold(sig.Platform, "kalshi") && !s.kalFeeSupported(sig.Ticker) {
		return unitTrialFunnelResult{Reason: "unsupported_kalshi_fee_schema"}
	}
	ask, depth, source, ok := s.unitExecutableQuote(sig)
	// Full-board PolyUS LITE proves the current BBO but intentionally carries no depth. A genuine
	// system signal outside the fixed full-depth prefix gets one bounded authoritative full-book
	// overflow attempt before the route is labeled incomplete; the overflow helper rate-limits and
	// coalesces requests, so broad signal scans cannot recreate a subscription/REST flood.
	if strings.EqualFold(sig.Platform, "polyus") && (!ok || depth < 1) {
		if complete, _, completeOK := s.completeBookSignal(sig); completeOK {
			ask, depth, source, ok = complete.EntryPrice, complete.BookDepth, complete.BookSource, true
		}
	}
	if !ok {
		return unitTrialFunnelResult{QuoteAttempted: true, Reason: "fresh_side_specific_book_unavailable"}
	}
	fee, feeSource, feeKnown := s.unitTrialFeeExact(sig.Platform, sig.Ticker, ask)
	if strings.TrimSpace(sourcePrefix) != "" {
		source = strings.TrimSpace(sourcePrefix) + "/" + source
	}
	inserted, err := s.insertCanonicalUnitTrial(ctx, storage.UnitTrial{
		OpenedTS: time.Now().UTC(), Family: sig.SignalType, Platform: strings.ToLower(sig.Platform),
		Ticker: sig.Ticker, Side: strings.ToUpper(sig.Side), Episode: sig.Episode,
		Category: sig.Category, Ask: ask, FeePC: fee, FeeKnown: feeKnown, FeeSource: feeSource,
		Depth: depth, QuoteSource: source,
		ResolveHours: sig.ResolveHours,
	})
	result := unitTrialFunnelResult{QuoteAttempted: true, UnitRow: inserted, UnitRows: boolInt(inserted),
		MoneyTruth: ask > 0 && ask < 1 && depth >= 1 && feeKnown && strings.TrimSpace(feeSource) != ""}
	result.MoneyTruthRows = boolInt(result.MoneyTruth)
	if err != nil {
		result.Reason = "unit_trial_storage_error"
	} else if !inserted {
		result.Reason = "unit_trial_duplicate_or_no_upgrade"
	} else if !result.MoneyTruth {
		switch {
		case depth < 1:
			result.Reason = "book_depth_unknown_or_below_one_share"
		case !feeKnown || strings.TrimSpace(feeSource) == "":
			result.Reason = "exact_route_fee_unavailable"
		default:
			result.Reason = "incomplete_executable_money_truth"
		}
	}
	return result
}

// insertCanonicalUnitTrial freezes the exact entry-time event identity before economic evidence is
// written. The venue-local catalog groups related sub-bets under one event without guessing any
// cross-venue equivalence. If the catalog genuinely has no exact ticker, Store retains the row as
// explicitly unclustered; a registry/storage error fails closed instead of silently losing proof.
func (s *Server) insertCanonicalUnitTrial(ctx context.Context, trial storage.UnitTrial) (bool, error) {
	if s.store == nil {
		return false, errors.New("unit trial store unavailable")
	}
	venue, ticker := strings.ToLower(strings.TrimSpace(trial.Platform)), strings.TrimSpace(trial.Ticker)
	if ticker != "" && (venue == "kalshi" || venue == "polyus" || venue == "polymarket") {
		if _, _, err := s.ensureExactCatalogCanonicalInstrument(ctx, venue, ticker); err != nil {
			return false, fmt.Errorf("freeze unit-trial event identity: %w", err)
		}
	}
	return s.store.InsertUnitTrial(ctx, trial)
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func (s *Server) unitTrialConsider(ctx context.Context, sig storage.Signal) unitTrialFunnelResult {
	return s.unitTrialConsiderOne(ctx, sig, "emitted-side")
}

// completeBookSignal is the single injectable boundary used by the generic inverse collector and
// its Paper executor. Production always falls through to the current live/cache book implementation;
// the optional function supports immutable replay and hermetic end-to-end route tests without
// weakening the all-fields-required contract in currentCompleteBookSignal.
func (s *Server) completeBookSignal(sig storage.Signal) (storage.Signal, string, bool) {
	if s.completeBookSignalFn != nil {
		return s.completeBookSignalFn(sig)
	}
	return s.currentCompleteBookSignal(sig)
}

// completeBookSignalCached preserves the hermetic test seam while forbidding Paper from borrowing
// the real-money REST overflow lane. A missing subscribed book is an honest Paper zero-fill.
func (s *Server) completeBookSignalCached(sig storage.Signal) (storage.Signal, string, bool) {
	if s.completeBookSignalFn != nil {
		return s.completeBookSignalFn(sig)
	}
	return s.currentCompleteBookSignalCached(sig)
}

// collectIndependentInverseSystem gives a one-side detector's opposite expression its own named
// model route. The row uses the actual opposite ask/depth/fee and later settles from its actual
// side. It is not an algebraic child of the direct result, and it is reported through its own
// collector funnel. A Paper fill later creates a separate strategy-origin route through the same
// live-mirror observation boundary; LIVE authority still requires a sealed accepted proof.
func (s *Server) collectIndependentInverseSystem(ctx context.Context, sig storage.Signal) (storage.Signal, unitTrialFunnelResult, bool) {
	inverse, ok := independentInverseSignalIdentity(sig)
	if !ok {
		return storage.Signal{}, unitTrialFunnelResult{}, false
	}
	// Persist the inverse as a real signal family only after a complete opposite-side book receipt
	// exists. This gives the named system its own settlement and post-entry path (therefore its own
	// CLV), instead of leaving it as an ungraded arithmetic annotation on the original row.
	quoted, feeSource, quoteOK := s.completeBookSignal(inverse)
	if !quoteOK {
		return inverse, unitTrialFunnelResult{
			QuoteAttempted: true,
			Reason:         "independent_inverse_complete_book_unavailable",
		}, true
	}
	quoted.PricingVersion = "independent-inverse-executable-v1"
	quoted.ExecExpr += "/book=" + quoted.BookSource + "/fee=" + feeSource
	inserted, insertErr := s.store.InsertSignalResult(ctx, quoted)
	if insertErr != nil {
		return quoted, unitTrialFunnelResult{
			QuoteAttempted: true,
			Reason:         "independent_inverse_signal_storage_error",
		}, true
	}
	// A signal may retry its execution hook inside the same ten-minute storage slot. Refresh the
	// still-unfilled named inverse with this retry's atomic book receipt and timestamp; otherwise a
	// valid later Paper fill cannot prove which stale duplicate it came from. Filled/resolved rows
	// are immutable and deliberately do not refresh.
	refreshed := false
	if !inserted {
		var refreshErr error
		refreshed, refreshErr = s.store.RefreshSignalFamilyExecutionReceipt(ctx, quoted)
		if refreshErr != nil {
			return quoted, unitTrialFunnelResult{
				QuoteAttempted: true,
				Reason:         "independent_inverse_signal_refresh_error",
			}, true
		}
	}
	// Reuse this exact complete receipt for route proof. Re-querying through executableAsk here can
	// combine a newer PolyUS LITE price with older MARKET_DATA depth, which is not a real fillable
	// state. The signal row and one-share row therefore share one atomic price/depth/fee snapshot.
	fee := 0.0
	if quoted.FeePC != nil {
		fee = *quoted.FeePC
	}
	unitInserted, unitErr := s.insertCanonicalUnitTrial(ctx, storage.UnitTrial{
		OpenedTS: time.Now().UTC(), Family: quoted.SignalType, Platform: strings.ToLower(quoted.Platform),
		OriginLayer: "model", Ticker: quoted.Ticker, Side: strings.ToUpper(quoted.Side), Episode: quoted.Episode,
		Category: quoted.Category, Ask: quoted.EntryPrice, FeePC: fee, FeeKnown: true, FeeSource: feeSource,
		Depth: quoted.BookDepth, QuoteSource: "independent-inverse-system/base=" +
			strings.TrimSpace(sig.SignalType) + "/" + quoted.BookSource,
		ResolveHours: quoted.ResolveHours,
	})
	result := unitTrialFunnelResult{QuoteAttempted: true, UnitRow: unitInserted,
		UnitRows: boolInt(unitInserted), MoneyTruth: unitErr == nil && quoted.EntryPrice > 0 &&
			quoted.EntryPrice < 1 && quoted.BookDepth >= 1 && strings.TrimSpace(feeSource) != ""}
	result.MoneyTruthRows = boolInt(result.MoneyTruth)
	if unitErr != nil {
		result.Reason = "independent_inverse_unit_storage_error"
	} else if !unitInserted {
		result.Reason = "independent_inverse_unit_duplicate_or_no_upgrade"
	}
	if inserted && result.Reason == "" {
		result.Reason = "independent_inverse_signal_and_money_truth_stored"
	} else if refreshed && result.Reason == "" {
		result.Reason = "independent_inverse_signal_refreshed_and_money_truth_stored"
	} else if !inserted && result.Reason == "" {
		result.Reason = "independent_inverse_signal_duplicate_money_truth_stored"
	}
	return quoted, result, true
}

// collectAndNoteIndependentInverse keeps persistence and funnel accounting together. The emitted
// direct route is published first; this slower opposite-book step then returns a durable, complete
// inverse receipt that may safely enter its own delayed Paper route.
func (s *Server) collectAndNoteIndependentInverse(ctx context.Context, sig storage.Signal,
	triggerAt ...time.Time) (storage.Signal, bool) {
	inverse, result, eligible := s.collectIndependentInverseSystem(ctx, sig)
	if !eligible {
		return storage.Signal{}, false
	}
	// The inverse becomes runtime-current only after its own opposite-side complete-book read.
	// A base-side signal is never enough to infer that this independently priced opportunity lived.
	s.noteR147InverseOpportunity(inverse, result, time.Now().UTC())
	event := nativeSignalFunnelEvent{Eligible: 1, InputRows: 1, Reason: result.Reason}
	if result.QuoteAttempted {
		event.QuoteAttempts = 1
	}
	event.UnitRows = result.UnitRows
	event.MoneyTruth = result.MoneyTruthRows
	s.noteNativeSystemFunnel(context.WithoutCancel(ctx), inverse, event)
	if !result.MoneyTruth {
		at := time.Now().UTC()
		if len(triggerAt) > 0 && !triggerAt[0].IsZero() {
			at = triggerAt[0].UTC()
		}
		detail, _ := json.Marshal(map[string]any{
			"system": inverse.SignalType, "venue": strings.ToLower(strings.TrimSpace(inverse.Platform)),
			"ticker": inverse.Ticker, "side": strings.ToUpper(strings.TrimSpace(inverse.Side)),
			"route": "taker", "input_topology": r147SignalInputTopology(inverse),
			"state": "REJECTED", "order_result": "REJECTED", "reason": result.Reason,
			"trigger_unix_ms": at.UnixMilli(), "paper_execution": "not_observed",
		})
		auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		_ = s.store.Audit(auditCtx, "info", "inverse-system",
			fmt.Sprintf("REJECTED %s %s %s: %s", inverse.SignalType,
				strings.ToLower(strings.TrimSpace(inverse.Platform)), inverse.Ticker, result.Reason), string(detail))
		cancel()
	}
	return inverse, result.MoneyTruth
}

func (s *Server) settleUnitTrials(ctx context.Context) {
	if settled, err := s.store.ResolveUnitTrialsFromSignals(ctx, 2000); err == nil && settled > 0 {
		// A terminal row can turn a qualifying route negative. Fence both existing entries and
		// loads that began before the settlement commit so no order can reuse the prior 2s proof.
		s.liveRouteProofCache.invalidate()
	}
}

func (s *Server) handleUnitTrials(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.UnitTrialStatsAt(r.Context(), time.Now().UTC())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"rules": "one executable share per model/venue/market/side/gap episode; no bankroll or allocation gate; fee-inclusive; return/$ = net divided by entry cost; capital-day is a ratio of sums using exact elapsed seconds with a one-second guard; real and paper strategies retain collateral/exposure safety walls",
		"stats": stats,
	})
}
