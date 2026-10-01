package server

// Exact executable-book route for the funded Combo Paper portfolio. Combo Lab remains research;
// this file owns the stricter path that can create a Paper position.

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const fundedComboMaxQuoteAge = 5 * time.Second

func (s *Server) validateFreshFundedComboCollection(ctx context.Context, legs []paper.Leg, expected string) error {
	if err := s.validateFreshMLComboCollection(ctx, legs, expected); err != nil {
		// The shared checker predates positive-system Combo Paper and names its first caller. Keep
		// one authoritative implementation while reporting the route-neutral funded truth here.
		msg := strings.ReplaceAll(err.Error(), "Kalshi ML Combo", "Kalshi funded Combo")
		return fmt.Errorf("%s", msg)
	}
	return nil
}

type fundedComboLegQuote struct {
	Bid, Ask, Depth, Tick, QuoteAge, QtyStep, MinQty float64
	BookSource, FeeSource, QuantitySource            string
}

func validFundedComboQuote(q fundedComboLegQuote) bool {
	finite := func(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
	return finite(q.Bid) && finite(q.Ask) && finite(q.Depth) && finite(q.Tick) && finite(q.QuoteAge) &&
		q.Bid > 0 && q.Ask > q.Bid && q.Ask < 1 && q.Depth >= 1 && q.Tick > 0 && q.Tick < 1 &&
		finite(q.QtyStep) && finite(q.MinQty) && q.QtyStep > 0 && q.MinQty > 0 &&
		q.QuoteAge >= 0 && q.QuoteAge <= fundedComboMaxQuoteAge.Seconds() &&
		strings.TrimSpace(q.BookSource) != "" && strings.TrimSpace(q.FeeSource) != "" &&
		strings.TrimSpace(q.QuantitySource) != ""
}

func (s *Server) fundedComboQuantityRule(platform, ticker string) (step, minimum float64, source string, ok bool) {
	switch strings.ToLower(strings.TrimSpace(platform)) {
	case "kalshi", "":
		// Kalshi's combined-market RFQ contract endpoint is package-count sized. Keep product-price
		// Paper on whole package contracts until an exact RFQ itself supplies a different count_fp.
		return 1, 1, "kalshi-rfq-whole-package", true
	case "polyus":
		now := time.Now()
		s.polyUSMu.Lock()
		defer s.polyUSMu.Unlock()
		valid := func(slug string, minimum float64) bool {
			return strings.EqualFold(slug, ticker) && minimum > 0 &&
				!math.IsNaN(minimum) && !math.IsInf(minimum, 0)
		}
		if !s.polyUSAt.IsZero() && now.Sub(s.polyUSAt) <= 5*time.Minute {
			for _, market := range s.polyUSMkts {
				if valid(market.Slug, market.MinimumQty) {
					return market.MinimumQty, market.MinimumQty, "polyus-minimumTradeQty", true
				}
			}
		}
		if !s.pusSweepAt.IsZero() && now.Sub(s.pusSweepAt) <= 5*time.Minute {
			for _, market := range s.pusSweep {
				if valid(market.Slug, market.MinimumQty) {
					return market.MinimumQty, market.MinimumQty, "polyus-minimumTradeQty", true
				}
			}
		}
	}
	return 0, 0, "", false
}

// fundedComboExecutableQuote has no mark/mid/last-trade/REST fallback. The shared complete-book
// receipt requires a current lifecycle, both touch prices and depths, side tick, and exact fee
// authority before this adapter returns the requested side's executable ask.
func (s *Server) fundedComboExecutableQuote(ctx context.Context, leg paper.Leg) (fundedComboLegQuote, bool) {
	if s.fundedComboQuoteFn != nil {
		q, ok := s.fundedComboQuoteFn(ctx, leg)
		return q, ok && validFundedComboQuote(q)
	}
	receipt, feeSource, ok := s.currentCompleteBookSignal(storage.Signal{
		Platform: strings.ToLower(strings.TrimSpace(leg.Platform)), Ticker: strings.TrimSpace(leg.Ticker),
		Side: strings.ToUpper(strings.TrimSpace(leg.Side)),
	})
	if !ok || receipt.BookBid == nil || receipt.BookAsk == nil || receipt.BookAskDepth == nil ||
		receipt.BookQuoteAgeS == nil || receipt.BookTakerTick == nil {
		return fundedComboLegQuote{}, false
	}
	q := fundedComboLegQuote{Bid: *receipt.BookBid, Ask: *receipt.BookAsk,
		Depth: *receipt.BookAskDepth, Tick: *receipt.BookTakerTick, QuoteAge: *receipt.BookQuoteAgeS,
		BookSource: receipt.BookSource, FeeSource: feeSource}
	q.QtyStep, q.MinQty, q.QuantitySource, ok = s.fundedComboQuantityRule(leg.Platform, leg.Ticker)
	if !ok {
		return fundedComboLegQuote{}, false
	}
	return q, validFundedComboQuote(q)
}

func (s *Server) fundedComboExactFee(leg paper.Leg, contracts, price float64) (float64, string, bool) {
	if s.fundedComboFeeFn != nil {
		fee, source, ok := s.fundedComboFeeFn(leg, contracts, price)
		return fee, source, ok && fee >= 0 && !math.IsNaN(fee) && !math.IsInf(fee, 0) && strings.TrimSpace(source) != ""
	}
	fee, source, ok := s.fillFeeReceipt(leg.Platform, leg.Ticker, false, contracts, price)
	return fee, source, ok && fee >= 0 && !math.IsNaN(fee) && !math.IsInf(fee, 0) && strings.TrimSpace(source) != ""
}

func minFundedComboDepth(quotes []fundedComboLegQuote) float64 {
	depth := math.Inf(1)
	for _, q := range quotes {
		depth = math.Min(depth, q.Depth)
	}
	return depth
}

func gcd64(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// fundedComboQuantityGrid finds one package quantity accepted by every leg. Six decimal places
// match the venue fixed-point wire precision and avoid floating modulo guesses.
func fundedComboQuantityGrid(quotes []fundedComboLegQuote) (quantum, minimum float64, ok bool) {
	const scale int64 = 1_000_000
	maxInt64 := int64(^uint64(0) >> 1)
	var lcm int64 = 1
	for _, q := range quotes {
		step := int64(math.Round(q.QtyStep * float64(scale)))
		if step <= 0 || math.Abs(float64(step)/float64(scale)-q.QtyStep) > 1e-9 {
			return 0, 0, false
		}
		g := gcd64(lcm, step)
		if g <= 0 || lcm > maxInt64/(step/g) {
			return 0, 0, false
		}
		lcm *= step / g
		minimum = math.Max(minimum, q.MinQty)
	}
	quantum = float64(lcm) / float64(scale)
	return quantum, minimum, quantum > 0
}

func fundedComboRoundQuantity(raw float64, quotes []fundedComboLegQuote) (float64, bool) {
	quantum, minimum, ok := fundedComboQuantityGrid(quotes)
	if !ok || raw <= 0 {
		return 0, false
	}
	quantity := math.Floor((raw+1e-10)/quantum) * quantum
	quantity = math.Round(quantity*1e6) / 1e6
	return quantity, quantity+1e-10 >= minimum && quantity > 0
}

// fundedComboFit caps package contracts to every leg's current touch and the available Combo
// portfolio capital, then recomputes every exact quantity fee. Fee rounding is nonlinear, so the
// bounded loop re-evaluates authority after each capital shrink instead of scaling a one-share fee.
func (s *Server) fundedComboFit(legs []paper.Leg, quotes []fundedComboLegQuote, price, maxContracts, avail float64) (
	contracts, stake, fees float64, legFees []float64, feeSources []string, err error,
) {
	if len(legs) < 2 || len(legs) != len(quotes) || price <= 0 || price >= 1 || maxContracts <= 0 || avail <= 0 {
		return 0, 0, 0, nil, nil, fmt.Errorf("invalid funded combo sizing inputs")
	}
	contracts, ok := fundedComboRoundQuantity(math.Min(math.Min(maxContracts, minFundedComboDepth(quotes)), 4000), quotes)
	if !ok || contracts <= 0 || math.IsNaN(contracts) || math.IsInf(contracts, 0) {
		return 0, 0, 0, nil, nil, fmt.Errorf("funded combo has no common touch depth")
	}
	for attempt := 0; attempt < 8; attempt++ {
		stake = contracts * price
		fees = 0
		legFees = make([]float64, len(legs))
		feeSources = make([]string, len(legs))
		for i := range legs {
			fee, source, ok := s.fundedComboExactFee(legs[i], contracts, quotes[i].Ask)
			if !ok {
				return 0, 0, 0, nil, nil, fmt.Errorf("leg %s exact taker fee is unavailable at quantity %.4f", legs[i].Ticker, contracts)
			}
			legFees[i], feeSources[i] = fee, source
			fees += fee
		}
		need := stake + fees
		if stake >= 1 && need <= avail+1e-9 {
			return contracts, stake, fees, legFees, feeSources, nil
		}
		if need <= 0 || stake < 1 {
			return 0, 0, 0, nil, nil, fmt.Errorf("funded combo common depth supports only $%.2f of entry capital", stake)
		}
		contracts, ok = fundedComboRoundQuantity(contracts*math.Min(.999999, avail/need), quotes)
		if !ok {
			return 0, 0, 0, nil, nil, fmt.Errorf("funded combo capital cannot support the venue quantity grid")
		}
	}
	return 0, 0, 0, nil, nil, fmt.Errorf("funded combo could not fit exact rounded fees inside available capital")
}

func (s *Server) fundedComboQuoteLegs(ctx context.Context, legs []paper.Leg, reference []paper.Leg) (
	[]paper.Leg, []fundedComboLegQuote, float64, error,
) {
	if !validParlayLegCount(len(legs)) {
		return nil, nil, 0, fmt.Errorf("combo needs 2-%d legs", parlayLegLimit)
	}
	platform := strings.ToLower(strings.TrimSpace(legs[0].Platform))
	if platform == "" {
		return nil, nil, 0, fmt.Errorf("funded combo platform is missing")
	}
	out := append([]paper.Leg(nil), legs...)
	quotes := make([]fundedComboLegQuote, len(out))
	product := 1.0
	for i := range out {
		leg := &out[i]
		leg.Platform = strings.ToLower(strings.TrimSpace(leg.Platform))
		leg.Side = strings.ToUpper(strings.TrimSpace(leg.Side))
		if leg.Platform != platform {
			return nil, nil, 0, fmt.Errorf("combo legs must all be on the same platform")
		}
		if leg.Ticker == "" || (leg.Side != "YES" && leg.Side != "NO") || leg.Entry <= 0 || leg.Entry >= 1 {
			return nil, nil, 0, fmt.Errorf("bad funded combo leg identity/price")
		}
		if hours, limit, ok := s.paperComboEntryHorizonDecision(ctx, leg.Platform, leg.Ticker, leg.Title); !ok {
			return nil, nil, 0, fmt.Errorf("combo leg %s entry horizon %.3fh is unknown, elapsed, or beyond the %.3fh limit", leg.Ticker, hours, limit)
		}
		q, ok := s.fundedComboExecutableQuote(ctx, *leg)
		if !ok {
			return nil, nil, 0, fmt.Errorf("leg %s lacks a fresh complete executable side book", leg.Ticker)
		}
		anchor := leg.Entry
		if len(reference) == len(out) {
			anchor = reference[i].Entry
		}
		if q.Ask > anchor+.03 {
			return nil, nil, 0, fmt.Errorf("leg %s moved %.1f cents against the last executable ask", leg.Ticker, (q.Ask-anchor)*100)
		}
		if q.Ask < anchor-.10 || q.Ask < .02 || q.Ask > .98 {
			return nil, nil, 0, fmt.Errorf("leg %s executable ask %.4f is dislocated or outside the sane band", leg.Ticker, q.Ask)
		}
		leg.BookBid, leg.Entry, leg.TouchDepth = q.Bid, q.Ask, q.Depth
		leg.TickSize, leg.QuoteAgeS, leg.BookSource = q.Tick, q.QuoteAge, q.BookSource
		leg.QuantityStep, leg.MinimumQty, leg.QuantitySource = q.QtyStep, q.MinQty, q.QuantitySource
		quotes[i] = q
		product *= q.Ask
	}
	if product < .02 || product >= 1 || 1/product > 50 || math.IsNaN(product) || math.IsInf(product, 0) {
		return nil, nil, 0, fmt.Errorf("funded combo executable product %.6f fails the 50x payout fuse", product)
	}
	return out, quotes, product, nil
}

func (s *Server) placeFundedParlayLegs(ctx context.Context, legs []paper.Leg, stake float64, source string, contract *fundedComboContract) (positionID int64, retErr error) {
	system := fmt.Sprintf("parlay-%dleg", len(legs))
	producer := "unknown"
	if contract != nil && strings.TrimSpace(contract.ProducerFamily) != "" {
		producer = strings.TrimSpace(contract.ProducerFamily)
	}
	portfolio := vbCombos
	if contract != nil && strings.TrimSpace(contract.Portfolio) != "" {
		portfolio = strings.ToLower(strings.TrimSpace(contract.Portfolio))
	}
	relation := s.beginFundedRelation(ctx, portfolio, system, "combo-taker/"+producer, comboRelationLegs(legs))
	defer func() {
		if retErr != nil {
			relation.reject(retErr.Error())
		}
	}()
	if contract == nil || contract.RouteSource == "" || contract.Cohort == "" || len(contract.SystemIDs) == 0 ||
		!contract.ForceTaker || contract.JointP <= 0 || contract.JointP >= 1 || math.IsNaN(contract.JointP) || math.IsInf(contract.JointP, 0) {
		return 0, fmt.Errorf("funded combo contract is incomplete or not taker-priced")
	}
	producer = strings.TrimSpace(contract.ProducerFamily)
	if producer == "" {
		producer = "positive-system"
	}
	epoch := strings.TrimSpace(contract.ExperimentEpoch)
	if epoch == "" {
		epoch = contract.RouteSource + ":" + contract.Cohort
	}
	comboRelation := "independent"
	seenEvents := map[string]bool{}
	for _, leg := range relation.legs {
		if leg.CanonicalEventID == "" || leg.EventVersion <= 0 {
			comboRelation = "unknown"
			break
		}
		key := fmt.Sprintf("%s@%d", leg.CanonicalEventID, leg.EventVersion)
		if seenEvents[key] {
			comboRelation = "related"
		}
		seenEvents[key] = true
	}
	venue := strings.ToLower(strings.TrimSpace(legs[0].Platform))
	for _, leg := range legs[1:] {
		if !strings.EqualFold(leg.Platform, venue) {
			venue = "mixed"
			break
		}
	}
	if stake <= 0 {
		stake = 10
	}
	initial, quotes, product, err := s.fundedComboQuoteLegs(ctx, legs, nil)
	if err != nil {
		return 0, err
	}
	avail := s.fundedComboAvailableUSD(ctx, portfolio)
	requestedContracts := stake / product
	contracts, _, _, _, _, err := s.fundedComboFit(initial, quotes, product, requestedContracts, avail)
	if err != nil {
		return 0, err
	}

	// Fill-time decision: repeat the complete book and lifecycle checks, cap again to the new
	// common touch, and recompute exact fees at the final quantity immediately before insertion.
	finalLegs, finalQuotes, finalProduct, err := s.fundedComboQuoteLegs(ctx, initial, initial)
	if err != nil {
		return 0, err
	}
	contracts, stake, fees, legFees, feeSources, err := s.fundedComboFit(finalLegs, finalQuotes, finalProduct, contracts, avail)
	if err != nil {
		return 0, err
	}
	for i := range finalLegs {
		finalLegs[i].EntryFee = legFees[i]
		finalLegs[i].FeeSource = feeSources[i]
	}
	expectedNetPerDollar, economicsOK := plabAllInReturn(contract.JointP, finalProduct, fees/stake)
	if !economicsOK || expectedNetPerDollar <= 0 {
		return 0, fmt.Errorf("funded combo final fee-net expected value %.6f is not positive", expectedNetPerDollar)
	}
	if strings.EqualFold(finalLegs[0].Platform, "kalshi") {
		if err := s.validateFreshFundedComboCollection(ctx, finalLegs, contract.Collection); err != nil {
			return 0, err
		}
	}
	p := paper.Parlay{Stake: stake, Price: finalProduct, Contracts: contracts, Status: "open",
		Legs: finalLegs, Fees: fees, ExpectedNetPerDollar: expectedNetPerDollar,
		RouteSource: contract.RouteSource, Cohort: contract.Cohort,
		SystemIDs: append([]string(nil), contract.SystemIDs...), JointP: contract.JointP,
		CanonicalSystemID: fmt.Sprintf("parlay-%dleg", len(finalLegs)), ComboVenue: venue,
		LegCount: len(finalLegs), RelationClass: comboRelation, ProducerFamily: producer,
		ComboRoute: "paper-combo-taker", ExperimentEpoch: epoch, ComboKey: paperComboSignature(finalLegs)}
	receiptID, err := relation.allowBeforePlacement("passed-current-funded-combo-checks", contracts, finalProduct, fees)
	if err != nil {
		return 0, fmt.Errorf("funded combo relation receipt: %w", err)
	}
	id, err := s.store.InsertParlayWithRelation(ctx, p, receiptID)
	if err != nil {
		_, _ = s.store.RecordFundedRelationPlacementFailure(context.WithoutCancel(ctx), receiptID, "funded-combo-insert-failed")
		return 0, err
	}
	_ = s.store.Audit(ctx, "info", "parlay", fmt.Sprintf(
		"combo #%d placed (%s): %d legs · %.4f contracts capped by touch depth · stake $%.2f · executable product %.6f · exact fees $%.2f",
		id, source, len(finalLegs), contracts, stake, finalProduct, fees), "")
	return id, nil
}
