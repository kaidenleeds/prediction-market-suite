package server

// whaleexitbridge.go turns the Poly-int whale-exit observation into an honest prospective
// tradeable-venue system. The original `whale-exit` row is a counterfactual: a measured-good
// wallet SOLD an outcome, and the row asks whether buying that same sold outcome at the observed
// print and holding to settlement would have paid. It does not know the wallet's acquisition
// basis, so it never claims to measure the seller's realized P&L.
//
// `whale-exit-hold-bridge` is the separately named executable expression. It buys the SOLD outcome
// (fades the early exit) on Kalshi or PolyUS only when a current normalized rule certificate proves
// the exact event/payoff orientation and a complete side-specific venue book proves ask, bid, both
// touch depths, quote age, ticks and exact fees. The normal signal choke point then supplies the
// prospective UnitTrial, generic Paper executor and future sealed LIVE route. A missing certificate
// or book is a durable rejection, never a fuzzy trade or a fabricated historical fill.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const whaleExitHoldBridgeFamily = "whale-exit-hold-bridge"

type whaleExitBridgeCandidate struct {
	Venue, Ticker, Title, Side string
}

// whaleExitSourceSide converts the exact two-token Polymarket outcome ordering into its binary
// economic side. Outcome zero is the condition's YES token and outcome one is its complement.
// N-way or ambiguous labels refuse: a missing bridge is safer than guessing the exposure.
func whaleExitSourceSide(outcomes []string, soldOutcome string) (string, bool) {
	if len(outcomes) != 2 || strings.TrimSpace(soldOutcome) == "" {
		return "", false
	}
	found := -1
	for i, outcome := range outcomes {
		if strings.EqualFold(strings.TrimSpace(outcome), strings.TrimSpace(soldOutcome)) {
			if found >= 0 {
				return "", false
			}
			found = i
		}
	}
	switch found {
	case 0:
		return "YES", true
	case 1:
		return "NO", true
	default:
		return "", false
	}
}

func whaleExitCertificateSide(sourceSide, orientation string) (string, bool) {
	sourceSide = strings.ToUpper(strings.TrimSpace(sourceSide))
	if sourceSide != "YES" && sourceSide != "NO" {
		return "", false
	}
	switch strings.ToLower(strings.TrimSpace(orientation)) {
	case "same":
		return sourceSide, true
	case "inverse":
		if sourceSide == "YES" {
			return "NO", true
		}
		return "YES", true
	default:
		return "", false
	}
}

func whaleExitBridgeIdentityOK(sourceSide string, candidate whaleExitBridgeCandidate,
	cert storage.RulePairCertificateSpec, current bool, certErr error) (bool, string) {
	if certErr != nil {
		return false, "rule-certificate-read-error"
	}
	if !current || strings.TrimSpace(cert.SpecHash) == "" {
		return false, "no-current-settlement-equivalence-certificate"
	}
	expected, ok := whaleExitCertificateSide(sourceSide, cert.Orientation)
	if !ok {
		return false, "certificate-orientation-invalid"
	}
	if strings.ToUpper(strings.TrimSpace(candidate.Side)) != expected {
		return false, "matched-side-conflicts-with-certificate-orientation"
	}
	return true, "current-settlement-equivalence-certificate"
}

func (s *Server) auditWhaleExitBridge(ctx context.Context, state, reason, conditionID,
	soldOutcome, sourceSide string, sourcePrice, notional float64, traders int,
	candidate whaleExitBridgeCandidate, cert storage.RulePairCertificateSpec, quoted *storage.Signal,
	feeSource string) {
	detail := map[string]any{
		"system": whaleExitHoldBridgeFamily, "state": state, "reason": reason,
		"source_venue": "polymarket", "source_condition_id": conditionID,
		"source_sold_outcome": soldOutcome, "source_binary_side": sourceSide,
		"source_sell_price": sourcePrice, "source_sell_notional": notional,
		"source_skilled_wallets":      traders,
		"predeclared_hypothesis":      "fade the skilled early exit: buy the same sold outcome and hold to settlement",
		"held_counterfactual_family":  "whale-exit",
		"seller_realized_pnl_known":   false,
		"seller_realized_pnl_blocker": "a sell print does not expose the wallet's acquisition lot or cost basis",
		"destination_venue":           candidate.Venue, "destination_ticker": candidate.Ticker,
		"destination_side": candidate.Side, "destination_title": candidate.Title,
		"certificate_hash": cert.SpecHash, "certificate_orientation": cert.Orientation,
		"canonical_event_id": cert.CanonicalEventID, "canonical_payoff_id": cert.CanonicalPayoffID,
		"receipt_grants_paper_authority": false, "receipt_grants_live_authority": false,
		"execution_pipeline": "signal -> exact one-share route -> generic Paper executor -> shared sealed LIVE mirror",
		"execution_note":     "collection is not promotion; every order still needs positive settled exact-route evidence and a final fresh book/fee/depth recheck",
	}
	if strings.EqualFold(state, "REJECTED") {
		emit, count, since := whaleExitBridgeRejectBatches.record(s, reason,
			strings.ToLower(strings.TrimSpace(candidate.Venue)), time.Now())
		if !emit {
			return
		}
		detail["aggregated_rejection_count"] = count
		detail["aggregation_window_started_at"] = since.UTC().Format(time.RFC3339Nano)
		detail["aggregation_key"] = reason + "/" + strings.ToLower(strings.TrimSpace(candidate.Venue))
	}
	if quoted != nil {
		detail["book_source"], detail["ask"], detail["bid"] = quoted.BookSource, quoted.BookAsk, quoted.BookBid
		detail["ask_depth"], detail["bid_depth"] = quoted.BookAskDepth, quoted.BookBidDepth
		detail["spread_cents"], detail["quote_age_s"] = quoted.SpreadCents, quoted.BookQuoteAgeS
		detail["taker_tick"], detail["maker_tick"] = quoted.BookTakerTick, quoted.BookMakerTick
		detail["taker_fee_pc"], detail["maker_fee_pc"] = quoted.BookTakerFeePC, quoted.BookMakerFeePC
		detail["fee_source"] = feeSource
	}
	b, _ := json.Marshal(detail)
	// The dispatcher owns a bounded worker. Give its final receipt one detached but still bounded
	// write attempt: producer/request cancellation cannot erase it, and a wedged DB cannot pin the
	// only bridge worker indefinitely.
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = s.store.Audit(auditCtx, "info", "whale-exit-bridge",
		fmt.Sprintf("%s %s %s %s: %s", state, whaleExitHoldBridgeFamily,
			candidate.Venue, candidate.Ticker, reason), string(b))
}

// logWhaleExitHoldBridge runs only from the already-throttled skilled SELL event. It evaluates
// both tradeable venues independently; a Kalshi identity rejection never hides an eligible PolyUS
// twin. The first rejection per reason/venue is durable and repeats are durably summarized in
// bounded batches; every collected signal keeps its exact individual receipt.
func (s *Server) logWhaleExitHoldBridge(ctx context.Context, conditionID, title, soldOutcome string,
	sourcePrice, notional float64, traders int) {
	if s.poly == nil || conditionID == "" || soldOutcome == "" || sourcePrice <= 0.02 || sourcePrice >= 0.98 {
		return
	}
	pm, found := s.poly.MarketByCondition(ctx, conditionID)
	if !found {
		s.auditWhaleExitBridge(ctx, "REJECTED", "source-market-unavailable", conditionID,
			soldOutcome, "", sourcePrice, notional, traders, whaleExitBridgeCandidate{},
			storage.RulePairCertificateSpec{}, nil, "")
		return
	}
	sourceSide, binary := whaleExitSourceSide(pm.Outcomes(), soldOutcome)
	if !binary {
		s.auditWhaleExitBridge(ctx, "REJECTED", "source-outcome-is-not-an-exact-binary-token", conditionID,
			soldOutcome, "", sourcePrice, notional, traders, whaleExitBridgeCandidate{},
			storage.RulePairCertificateSpec{}, nil, "")
		return
	}

	candidates := make([]whaleExitBridgeCandidate, 0, 2)
	seen := map[string]bool{}
	add := func(candidate whaleExitBridgeCandidate) {
		candidate.Venue = strings.ToLower(strings.TrimSpace(candidate.Venue))
		candidate.Ticker, candidate.Side = strings.TrimSpace(candidate.Ticker), strings.ToUpper(strings.TrimSpace(candidate.Side))
		key := candidate.Venue + "\x00" + candidate.Ticker + "\x00" + candidate.Side
		if candidate.Ticker == "" || (candidate.Side != "YES" && candidate.Side != "NO") || seen[key] {
			return
		}
		seen[key] = true
		candidates = append(candidates, candidate)
	}

	// Candidate discovery may use the existing broad matcher, but it can never authorize this
	// bridge. The current normalized rule certificate below is the mandatory identity authority.
	if s.kal != nil {
		kmarkets, _ := s.kal.GetTopLiquidMarkets(ctx)
		s.metaMu.Lock()
		known := make(map[string]bool, len(kmarkets))
		for _, market := range kmarkets {
			known[market.Ticker] = true
		}
		for ticker, market := range s.kmkts {
			if market.Result != "" || (market.Status != "" && !strings.EqualFold(market.Status, "active")) || known[ticker] {
				continue
			}
			known[ticker] = true
			kmarkets = append(kmarkets, market)
		}
		s.metaMu.Unlock()
		if ticker, matchedTitle, side, _, ok := s.consensusKalshiMatch(whaleExitHoldBridgeFamily, pm, soldOutcome, kmarkets); ok {
			add(whaleExitBridgeCandidate{Venue: "kalshi", Ticker: ticker, Title: matchedTitle, Side: side})
		}
	}
	if slug, _, flipped, ok := s.polyUSMatchForSide(whaleExitHoldBridgeFamily, title, soldOutcome, pm.Slug); ok {
		side := "YES"
		if flipped {
			side = "NO"
		}
		add(whaleExitBridgeCandidate{Venue: "polyus", Ticker: slug, Title: title, Side: side})
	}
	if len(candidates) == 0 {
		s.auditWhaleExitBridge(ctx, "REJECTED", "no-tradeable-twin-candidate", conditionID,
			soldOutcome, sourceSide, sourcePrice, notional, traders, whaleExitBridgeCandidate{},
			storage.RulePairCertificateSpec{}, nil, "")
		return
	}

	for _, candidate := range candidates {
		cert, current, certErr := s.store.CompatibleRulePairCertificateFor(ctx, "polymarket", conditionID,
			candidate.Venue, candidate.Ticker)
		if ok, reason := whaleExitBridgeIdentityOK(sourceSide, candidate, cert, current, certErr); !ok {
			s.auditWhaleExitBridge(ctx, "REJECTED", reason, conditionID, soldOutcome, sourceSide,
				sourcePrice, notional, traders, candidate, cert, nil, "")
			continue
		}
		resolveHours := s.sigResolveHours(ctx, candidate.Venue, candidate.Ticker)
		if !s.paperEntryHorizonOK(resolveHours, candidate.Ticker, candidate.Title) ||
			!s.paperEntryHorizonNow(ctx, candidate.Venue, candidate.Ticker, candidate.Title) {
			s.auditWhaleExitBridge(ctx, "REJECTED", "outside-current-paper-entry-horizon", conditionID,
				soldOutcome, sourceSide, sourcePrice, notional, traders, candidate, cert, nil, "")
			continue
		}
		sig := storage.Signal{Platform: candidate.Venue, Ticker: candidate.Ticker, Title: candidate.Title,
			Side: candidate.Side, SignalType: whaleExitHoldBridgeFamily, Underlying: sourcePrice,
			Notional: notional, TraderCount: traders, Strength: float64(traders), ResolveHours: resolveHours,
			ExecExpr: "source=polymarket:" + conditionID + "/action=fade-skilled-exit/buy=sold-outcome/hold=settlement/certificate=" + cert.SpecHash}
		quoted, feeSource, quoteOK := s.completeBookSignal(sig)
		if !quoteOK {
			s.auditWhaleExitBridge(ctx, "REJECTED", "fresh-complete-side-specific-book-unavailable",
				conditionID, soldOutcome, sourceSide, sourcePrice, notional, traders, candidate, cert, nil, "")
			continue
		}
		quoted.Confidence = s.signalConfidence(whaleExitHoldBridgeFamily, quoted.EntryPrice)
		if err := s.insertSignal(ctx, quoted); err != nil {
			s.auditWhaleExitBridge(ctx, "REJECTED", "tradeable-signal-storage-error", conditionID,
				soldOutcome, sourceSide, sourcePrice, notional, traders, candidate, cert, &quoted, feeSource)
			continue
		}
		s.auditWhaleExitBridge(ctx, "COLLECTED", "certified-twin-and-complete-book-processed", conditionID,
			soldOutcome, sourceSide, sourcePrice, notional, traders, candidate, cert, &quoted, feeSource)
	}
}
