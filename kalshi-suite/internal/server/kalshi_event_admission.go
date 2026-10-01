package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// kalshiEventExposure is an already-resolved piece of money state under Kalshi's own
// event_ticker.  An event ticker is the exact outcome-sibling header (for example, the two
// players in one tennis winner market); it is deliberately narrower than our broader correlated
// game cluster, so totals/spreads/props with different event_tickers remain independently tradable.
type kalshiEventExposure struct {
	Ticker, EventTicker, Side, Kind string
}

func kalshiEventExposureConflict(candidateTicker, candidateEvent string, held []kalshiEventExposure) string {
	candidateTicker = strings.TrimSpace(candidateTicker)
	candidateEvent = strings.ToUpper(strings.TrimSpace(candidateEvent))
	if candidateTicker == "" || candidateEvent == "" {
		return "kalshi-event-identity-unavailable"
	}
	for _, exposure := range held {
		if !strings.EqualFold(strings.TrimSpace(exposure.EventTicker), candidateEvent) {
			continue
		}
		switch exposure.Kind {
		case "position":
			return "existing-kalshi-event-position"
		case "order":
			return "existing-kalshi-event-resting-order"
		case "reservation":
			return "existing-kalshi-event-pending-order"
		default:
			return "existing-kalshi-event-exposure"
		}
	}
	return ""
}

// kalshiEventTickerStrict uses the venue's event_ticker field, never ticker parsing.  The market
// catalog is the fast path because an event identity is immutable.  A cold/evicted ticker gets one
// authoritative market read and is put back into the shared catalog.  Unknown identity is a
// refusal: real money must never guess whether two outcomes share a header.
func (s *Server) kalshiEventTickerStrict(ctx context.Context, ticker string) (string, error) {
	ticker = strings.TrimSpace(ticker)
	if ticker == "" {
		return "", errors.New("empty Kalshi ticker")
	}
	if r159KalshiResidentAdmissionOnly(ctx) {
		if m, ok := s.r159KalshiResidentMarket(ticker); ok {
			return strings.ToUpper(strings.TrimSpace(m.EventTicker)), nil
		}
		return "", errors.New("resident Kalshi market event identity unavailable or stale")
	}
	if m, ok := s.kmkt(ticker); ok && strings.TrimSpace(m.EventTicker) != "" {
		return strings.ToUpper(strings.TrimSpace(m.EventTicker)), nil
	}
	if s.kal == nil {
		return "", errors.New("Kalshi market identity client unavailable")
	}
	m, err := s.kal.GetMarket(ctx, ticker)
	if err != nil {
		return "", fmt.Errorf("Kalshi market identity read failed: %w", err)
	}
	if strings.TrimSpace(m.Ticker) != "" && !strings.EqualFold(strings.TrimSpace(m.Ticker), ticker) {
		return "", fmt.Errorf("Kalshi market identity mismatch: asked %s got %s", ticker, m.Ticker)
	}
	if strings.TrimSpace(m.EventTicker) == "" {
		return "", errors.New("Kalshi market omitted event_ticker")
	}
	s.metaMu.Lock()
	s.kmktsPutLocked(ticker, m)
	s.metaMu.Unlock()
	return strings.ToUpper(strings.TrimSpace(m.EventTicker)), nil
}

func (s *Server) heldKalshiComboLegs(ctx context.Context, ticker string) ([]kalshi.MVELeg, bool, error) {
	ticker = strings.TrimSpace(ticker)
	s.liveMu.Lock()
	legs := append([]kalshi.MVELeg(nil), s.comboLegReg[ticker]...)
	s.liveMu.Unlock()
	if len(legs) > 0 {
		return legs, true, nil
	}
	if !strings.Contains(strings.ToUpper(ticker), "KXMVE") {
		return nil, false, nil
	}
	var market kalshi.Market
	var ok bool
	if r159KalshiResidentAdmissionOnly(ctx) {
		market, ok = s.r159KalshiResidentMarket(ticker)
	} else {
		market, ok = s.kmkt(ticker)
	}
	if !ok {
		if r159KalshiResidentAdmissionOnly(ctx) {
			return nil, true, errors.New("resident held combined-market leg metadata unavailable or stale")
		}
		if s.kal == nil {
			return nil, true, errors.New("held combined-market leg registry unavailable")
		}
		var err error
		market, err = s.kal.GetMarket(kalshi.WithPriority(ctx), ticker)
		if err != nil {
			return nil, true, fmt.Errorf("recover held combined-market legs: %w", err)
		}
		s.metaMu.Lock()
		s.kmktsPutLocked(ticker, market)
		s.metaMu.Unlock()
	}
	legs = append([]kalshi.MVELeg(nil), market.MVESelectedLegs...)
	if len(legs) == 0 {
		return nil, true, errors.New("held combined-market metadata omitted mve_selected_legs")
	}
	for _, leg := range legs {
		if strings.TrimSpace(leg.MarketTicker) == "" ||
			(!strings.EqualFold(leg.Side, "yes") && !strings.EqualFold(leg.Side, "no")) {
			return nil, true, errors.New("held combined-market metadata has invalid leg identity")
		}
	}
	encoded, err := json.Marshal(legs)
	if err != nil || s.store == nil {
		return nil, true, errors.New("held combined-market leg recovery is not durable")
	}
	if err := s.store.KVSet(ctx, "combo_legs_"+ticker, string(encoded)); err != nil {
		return nil, true, fmt.Errorf("persist recovered combined-market legs: %w", err)
	}
	s.liveMu.Lock()
	if s.comboLegReg == nil {
		s.comboLegReg = map[string][]kalshi.MVELeg{}
	}
	s.comboLegReg[ticker] = append([]kalshi.MVELeg(nil), legs...)
	s.liveMu.Unlock()
	return legs, true, nil
}

func (s *Server) resolvedKalshiEventExposures(ctx context.Context, positions []kalshi.MarketPosition,
	orders []kalshi.Order, risks []storage.LivePendingRiskReservation) ([]kalshiEventExposure, error) {
	held := make([]kalshiEventExposure, 0, len(positions)+len(orders)+len(risks))
	appendResolved := func(ticker, side, kind string) error {
		event, err := s.kalshiEventTickerStrict(ctx, ticker)
		if err != nil {
			return fmt.Errorf("%s %s event identity: %w", kind, ticker, err)
		}
		held = append(held, kalshiEventExposure{Ticker: ticker, EventTicker: event,
			Side: strings.ToUpper(strings.TrimSpace(side)), Kind: kind})
		return nil
	}
	for _, p := range positions {
		if qty := p.PositionQty(); qty != 0 {
			legs, combined, comboErr := s.heldKalshiComboLegs(ctx, p.Ticker)
			if comboErr != nil {
				return nil, comboErr
			}
			if combined {
				// The suite only buys the YES conjunction. A short/NO combined position is the
				// complement of a conjunction and cannot be decomposed into independent opposite legs.
				if qty < 0 {
					return nil, errors.New("held combined-market NO exposure is not decomposable")
				}
				for _, leg := range legs {
					if err := appendResolved(leg.MarketTicker, leg.Side, "position"); err != nil {
						return nil, err
					}
				}
				continue
			}
			side := "YES"
			if qty < 0 {
				side = "NO"
			}
			if err := appendResolved(p.Ticker, side, "position"); err != nil {
				return nil, err
			}
		}
	}
	for _, o := range orders { // GetOrders is the complete resting-only account view.
		legs, combined, comboErr := s.heldKalshiComboLegs(ctx, o.Ticker)
		if comboErr != nil {
			return nil, comboErr
		}
		if combined {
			if !strings.EqualFold(strings.TrimSpace(o.Action), "BUY") ||
				!strings.EqualFold(strings.TrimSpace(o.OutcomeSide), "YES") {
				return nil, errors.New("resting combined-market exposure is not a YES buy conjunction")
			}
			for _, leg := range legs {
				if err := appendResolved(leg.MarketTicker, leg.Side, "order"); err != nil {
					return nil, err
				}
			}
			continue
		}
		side := strings.ToUpper(strings.TrimSpace(o.OutcomeSide))
		// A resting SELL may reduce an owned side or create the opposite exposure depending on the
		// account at fill time. Preserve exact-header protection, but leave its semantic side unknown
		// so another related bet fails closed rather than inventing an exposure.
		if action := strings.ToUpper(strings.TrimSpace(o.Action)); action != "" && action != "BUY" {
			side = ""
		}
		if err := appendResolved(o.Ticker, side, "order"); err != nil {
			return nil, err
		}
	}
	for _, row := range risks {
		if strings.EqualFold(strings.TrimSpace(row.Intent.Product), "combo") {
			var proof struct {
				SemanticLegs []struct {
					Ticker string `json:"ticker"`
					Side   string `json:"side"`
				} `json:"semantic_legs"`
			}
			if json.Unmarshal([]byte(row.Intent.ProofJSON), &proof) != nil || len(proof.SemanticLegs) == 0 {
				return nil, errors.New("active combo reservation omitted underlying semantic legs")
			}
			for _, leg := range proof.SemanticLegs {
				if err := appendResolved(leg.Ticker, leg.Side, "reservation"); err != nil {
					return nil, err
				}
			}
			continue
		}
		for _, leg := range row.Legs {
			if strings.EqualFold(strings.TrimSpace(leg.Venue), "kalshi") &&
				strings.EqualFold(strings.TrimSpace(leg.Action), "BUY") {
				if err := appendResolved(leg.Ticker, leg.Side, "reservation"); err != nil {
					return nil, err
				}
			}
		}
	}
	return held, nil
}

// liveKalshiEventHeaderConflict is the exact LIVE sibling-outcome guard.  Callers provide the
// same fresh authenticated position/order receipt used by the normal account guard; durable risk
// closes the pre-ack/restart gap.  handleLivePlace's livePlaceMu serializes this check through the
// following reservation, so two suite dispatchers cannot both pass and reserve sibling outcomes.
func (s *Server) liveKalshiEventHeaderConflict(ctx context.Context, candidate, side string,
	positions []kalshi.MarketPosition, orders []kalshi.Order) string {
	event, err := s.kalshiEventTickerStrict(ctx, candidate)
	if err != nil {
		return "kalshi-event-identity-unavailable"
	}
	if s.store == nil {
		return "durable-pending-risk-unavailable"
	}
	risks, err := s.store.ActiveLivePendingRisk(ctx)
	if err != nil {
		return "durable-pending-risk-unreadable"
	}
	held, err := s.resolvedKalshiEventExposures(ctx, positions, orders, risks)
	if err != nil {
		return "kalshi-held-event-identity-unavailable"
	}
	if why := kalshiEventExposureConflict(candidate, event, held); why != "" {
		return why
	}
	// No funded exposure means there is no broader payoff vector to contradict. Exact resident
	// event identity above is still mandatory, but a cold Event/Milestone cache must not reject the
	// first standalone pick merely because Paper/background warming has not reached it yet.
	if len(held) == 0 {
		return ""
	}
	return s.kalshiSemanticExposureConflict(ctx, candidate, side, held)
}

// liveKalshiEventHeaderGuard refreshes account state at the last pre-submit boundary.  There is no
// lock/hedge exception: the operator requires one position/order/reservation per exact event_ticker.
func (s *Server) liveKalshiEventHeaderGuard(ctx context.Context, candidate, side string) string {
	if s.kal == nil {
		return "kalshi-account-client-unavailable"
	}
	if snapshot, ok := r154KalshiAdmissionSnapshotFromContext(ctx); ok {
		return s.liveKalshiEventHeaderConflict(ctx, candidate, side,
			snapshot.Positions(), snapshot.Orders())
	}
	var (
		positions []kalshi.MarketPosition
		orders    []kalshi.Order
		posErr    error
		ordErr    error
		wg        sync.WaitGroup
	)
	wg.Add(2)
	go func() { defer wg.Done(); positions, posErr = s.kal.GetPositions(ctx) }()
	go func() { defer wg.Done(); orders, ordErr = s.kal.GetOrders(ctx) }()
	wg.Wait()
	if posErr != nil {
		return "kalshi-position-read-unavailable"
	}
	if ordErr != nil {
		return "kalshi-order-read-unavailable"
	}
	return s.liveKalshiEventHeaderConflict(ctx, candidate, side, positions, orders)
}

type kalshiPackageLeg struct {
	Ticker string `json:"ticker"`
	Side   string `json:"side"`
}

// kalshiPackageEventTickersStrict resolves every component through the same API-native market
// identity used by single orders. A package cannot contain two different contracts under one
// event_ticker: that would recreate the sibling-outcome exposure the one-header rule forbids while
// making it look like a diversified multi-leg product.
func (s *Server) kalshiPackageEventTickersStrict(ctx context.Context, legs []kalshiPackageLeg) (map[string]string, string) {
	if len(legs) == 0 {
		return nil, "kalshi-package-has-no-components"
	}
	byTicker := make(map[string]string, len(legs))
	byEvent := make(map[string]string, len(legs))
	for _, leg := range legs {
		ticker := strings.TrimSpace(leg.Ticker)
		if ticker == "" {
			return nil, "kalshi-event-identity-unavailable"
		}
		if side := strings.ToUpper(strings.TrimSpace(leg.Side)); side != "YES" && side != "NO" {
			return nil, "kalshi-package-side-unavailable"
		}
		if _, duplicate := byTicker[strings.ToUpper(ticker)]; duplicate {
			return nil, "duplicate-kalshi-contract-in-package"
		}
		event, err := s.kalshiEventTickerStrict(ctx, ticker)
		if err != nil {
			return nil, "kalshi-event-identity-unavailable"
		}
		if prior, duplicate := byEvent[event]; duplicate && !strings.EqualFold(prior, ticker) {
			return nil, "duplicate-kalshi-event-header-in-package"
		}
		byTicker[strings.ToUpper(ticker)] = event
		byEvent[event] = ticker
	}
	return byTicker, ""
}

// liveKalshiPackageEventHeaderConflict is the package equivalent of the single-order guard. It
// refreshes authenticated positions/orders, includes every durable pending-risk reservation, and
// permits one explicitly named reservation to be ignored while its staged package advances. The
// ignore is identity-scoped; positions and venue orders are never ignored.
func (s *Server) liveKalshiPackageEventHeaderConflict(ctx context.Context, legs []kalshiPackageLeg,
	ignoreReservationID string) string {
	candidateEvents, why := s.kalshiPackageEventTickersStrict(ctx, legs)
	if why != "" {
		return why
	}
	if s.kal == nil {
		return "kalshi-account-client-unavailable"
	}
	var (
		positions []kalshi.MarketPosition
		orders    []kalshi.Order
		posErr    error
		ordErr    error
		wg        sync.WaitGroup
	)
	wg.Add(2)
	go func() { defer wg.Done(); positions, posErr = s.kal.GetPositions(ctx) }()
	go func() { defer wg.Done(); orders, ordErr = s.kal.GetOrders(ctx) }()
	wg.Wait()
	if posErr != nil {
		return "kalshi-position-read-unavailable"
	}
	if ordErr != nil {
		return "kalshi-order-read-unavailable"
	}
	if s.store == nil {
		return "durable-pending-risk-unavailable"
	}
	risks, err := s.store.ActiveLivePendingRisk(ctx)
	if err != nil {
		return "durable-pending-risk-unreadable"
	}
	if ignoreReservationID = strings.TrimSpace(ignoreReservationID); ignoreReservationID != "" {
		kept := risks[:0:0]
		for _, risk := range risks {
			if risk.Intent.ReservationID != ignoreReservationID {
				kept = append(kept, risk)
			}
		}
		risks = kept
	}
	held, err := s.resolvedKalshiEventExposures(ctx, positions, orders, risks)
	if err != nil {
		return "kalshi-held-event-identity-unavailable"
	}
	for ticker, event := range candidateEvents {
		if conflict := kalshiEventExposureConflict(ticker, event, held); conflict != "" {
			return conflict
		}
	}
	// Check the package as one payoff vector against the complete funded set. Add each preceding
	// package leg to held so internal three-leg contradictions cannot pass pairwise inspection.
	semanticHeld := append([]kalshiEventExposure(nil), held...)
	for _, leg := range legs {
		if why := s.kalshiSemanticExposureConflict(ctx, leg.Ticker, leg.Side, semanticHeld); why != "" {
			return why
		}
		event := candidateEvents[strings.ToUpper(strings.TrimSpace(leg.Ticker))]
		semanticHeld = append(semanticHeld, kalshiEventExposure{Ticker: leg.Ticker,
			EventTicker: event, Side: strings.ToUpper(strings.TrimSpace(leg.Side)), Kind: "package"})
	}
	return ""
}

// paperKalshiEventHeaderConflict gives funded Paper the same event-header admission rule.  Real
// Paper candidates are catalog-backed.  Hermetic legacy tests that intentionally have no Kalshi
// client/catalog retain their synthetic behavior; once either the candidate or held state carries
// venue identity, missing identity fails closed instead of parsing a ticker.
func (s *Server) paperKalshiEventHeaderConflict(ctx context.Context, candidate, side string,
	positions []paper.Position) string {
	if _, cached := s.kmkt(candidate); !cached && s.kal == nil {
		return ""
	}
	event, err := s.kalshiEventTickerStrict(ctx, candidate)
	if err != nil {
		return "kalshi-event-identity-unavailable"
	}
	held := make([]kalshiEventExposure, 0, len(positions))
	for _, p := range positions {
		if !strings.EqualFold(p.Platform, "kalshi") || p.Contracts <= 0 {
			continue
		}
		if _, cached := s.kmkt(p.Ticker); !cached && s.kal == nil {
			return "kalshi-held-event-identity-unavailable"
		}
		pEvent, pErr := s.kalshiEventTickerStrict(ctx, p.Ticker)
		if pErr != nil {
			return "kalshi-held-event-identity-unavailable"
		}
		held = append(held, kalshiEventExposure{Ticker: p.Ticker, EventTicker: pEvent,
			Side: strings.ToUpper(strings.TrimSpace(p.Side)), Kind: "position"})
	}
	if why := kalshiEventExposureConflict(candidate, event, held); why != "" {
		return why
	}
	return s.kalshiSemanticExposureConflict(ctx, candidate, side, held)
}
