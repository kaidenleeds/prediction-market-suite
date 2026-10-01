package server

// R153 crypto allocation rail.
//
// Crypto is an asset class, not one event cluster: BTC/ETH/SOL windows can be distinct venue
// contracts while still consuming the same highly correlated capital sleeve. The existing 5%
// order, 10% event/coin cluster, and total-exposure rails remain authoritative. This file adds a
// separate aggregate crypto share of deployable LIVE capital and keeps the accounting exact across
// held positions, resting orders, and the durable pre-account-visibility risk overlay.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type liveCryptoClass uint8

const (
	liveCryptoUnknown liveCryptoClass = iota
	liveCryptoNo
	liveCryptoYes
)

func liveCryptoFamily(family string) bool {
	family = strings.ToLower(strings.TrimSpace(family))
	family = strings.TrimPrefix(family, "auto-cons-")
	family = strings.TrimPrefix(family, "invert:")
	switch family {
	case "xmatch", "spotlag", "kcrypto", "pcrypto", "pbridge", "poly-pred-kalshi",
		"xinv-pcrypto", "kthresh":
		return true
	default:
		return false
	}
}

// liveCryptoInstrumentClass uses the venue instrument as the primary truth and the exact route
// identity as a consistency check. A crypto-only route attached to a non-crypto instrument is not
// silently treated as either class: the final money boundary refuses it.
func liveCryptoInstrumentClass(venue, ticker, title, system string) liveCryptoClass {
	venue = strings.ToLower(strings.TrimSpace(venue))
	ticker = strings.TrimSpace(ticker)
	if ticker == "" || (venue != "kalshi" && venue != "polyus") {
		return liveCryptoUnknown
	}
	tickerCrypto := isCryptoTicker(ticker + " " + title)
	if liveCryptoFamily(system) && !tickerCrypto {
		return liveCryptoUnknown
	}
	if tickerCrypto {
		return liveCryptoYes
	}
	return liveCryptoNo
}

// liveCryptoAddRisk is the shared held-position/resting-order accumulator. Keeping this tiny
// primitive pure makes the account-source coverage testable without substituting a fake venue
// client into the real-money handler.
func liveCryptoAddRisk(total, crypto float64, known bool, risk float64,
	class liveCryptoClass) (float64, float64, bool) {
	if risk < 0 || math.IsNaN(risk) || math.IsInf(risk, 0) {
		return total, crypto, false
	}
	total += risk
	switch class {
	case liveCryptoYes:
		crypto += risk
	case liveCryptoNo:
	case liveCryptoUnknown:
		known = false
	}
	return total, crypto, known
}

func liveCryptoCandidateClass(c liveMirrorCandidate) liveCryptoClass {
	if side := strings.ToUpper(strings.TrimSpace(c.Side)); side != "YES" && side != "NO" {
		return liveCryptoUnknown
	}
	system := liveMirrorFamily(c)
	if system == "" {
		system = c.Source
	}
	return liveCryptoInstrumentClass(c.Platform, c.Ticker, c.Title, system)
}

func liveCryptoRestingKey(venue, orderID string) string {
	return strings.ToLower(strings.TrimSpace(venue)) + "\x00" + strings.TrimSpace(orderID)
}

// livePendingComboCryptoClass expands an atomic RFQ/KXMVE reservation through its immutable
// semantic legs. Because Kalshi accepts and holds that conjunction as one product, any crypto
// component charges the complete product cost. Separately routed staged legs use their actual
// per-leg principal below instead.
func livePendingComboCryptoClass(row storage.LivePendingRiskReservation, venue string) (liveCryptoClass, bool) {
	venue = strings.ToLower(strings.TrimSpace(venue))
	if venue != "" && venue != "kalshi" {
		return liveCryptoNo, true
	}
	var proof struct {
		SemanticLegs []struct {
			Ticker string `json:"ticker"`
			Side   string `json:"side"`
		} `json:"semantic_legs"`
	}
	if json.Unmarshal([]byte(row.Intent.ProofJSON), &proof) != nil || len(proof.SemanticLegs) == 0 {
		return liveCryptoUnknown, false
	}
	anyCrypto := false
	for _, leg := range proof.SemanticLegs {
		if strings.TrimSpace(leg.Ticker) == "" || (strings.ToUpper(strings.TrimSpace(leg.Side)) != "YES" &&
			strings.ToUpper(strings.TrimSpace(leg.Side)) != "NO") {
			return liveCryptoUnknown, false
		}
		class := liveCryptoInstrumentClass("kalshi", leg.Ticker, "", row.Intent.SystemID)
		if class == liveCryptoUnknown {
			return class, false
		}
		anyCrypto = anyCrypto || class == liveCryptoYes
	}
	if anyCrypto {
		return liveCryptoYes, true
	}
	return liveCryptoNo, true
}

// liveKalshiAccountInstrumentCryptoClass expands a held/resting KXMVE product back through the
// immutable selected-leg registry. The registry is KV-backed and heldKalshiComboLegs can recover
// it from Kalshi's mve_selected_legs, so attribution survives acceptance and process restarts.
func (s *Server) liveKalshiAccountInstrumentCryptoClass(ctx context.Context, ticker string) (liveCryptoClass, error) {
	legs, combined, err := s.heldKalshiComboLegs(ctx, ticker)
	if err != nil {
		return liveCryptoUnknown, err
	}
	if !combined {
		class := liveCryptoInstrumentClass("kalshi", ticker, "", "")
		if class == liveCryptoUnknown {
			return class, errorsNewCryptoAttribution(ticker)
		}
		return class, nil
	}
	if len(legs) == 0 {
		return liveCryptoUnknown, errorsNewCryptoAttribution(ticker)
	}
	anyCrypto := false
	for _, leg := range legs {
		if strings.TrimSpace(leg.MarketTicker) == "" ||
			(!strings.EqualFold(leg.Side, "yes") && !strings.EqualFold(leg.Side, "no")) {
			return liveCryptoUnknown, errorsNewCryptoAttribution(ticker)
		}
		class := liveCryptoInstrumentClass("kalshi", leg.MarketTicker, "", "")
		if class == liveCryptoUnknown {
			return class, errorsNewCryptoAttribution(ticker)
		}
		anyCrypto = anyCrypto || class == liveCryptoYes
	}
	if anyCrypto {
		return liveCryptoYes, nil
	}
	return liveCryptoNo, nil
}

func errorsNewCryptoAttribution(ticker string) error {
	return fmt.Errorf("Kalshi instrument %q has no durable crypto attribution", strings.TrimSpace(ticker))
}

// liveCryptoProportionalCost is the one staged-bundle fee allocation rule used at admission and
// by the durable pending overlay. Fees are spread by immutable entry principal, so unequal leg
// fees cannot make the pre-submit and post-reservation crypto charge disagree.
func liveCryptoProportionalCost(totalCost, allPrincipal, cryptoPrincipal float64) (float64, bool) {
	for _, value := range []float64{totalCost, allPrincipal, cryptoPrincipal} {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, false
		}
	}
	if allPrincipal <= 0 || cryptoPrincipal > allPrincipal+1e-9 {
		return 0, false
	}
	return totalCost * cryptoPrincipal / allPrincipal, true
}

// livePendingCryptoOverlay returns only the still-unrepresented crypto share of durable risk.
// A single order already visible in the venue's resting set is subtracted exactly. Multi-leg risk
// remains reserved until its coordinator releases the package. Atomic KXMVE products retain full
// product attribution; separately routed staged legs retain their actual proportional principal.
func livePendingCryptoOverlay(rows []storage.LivePendingRiskReservation,
	restingByVenueOrder map[string]float64, venue string) (float64, bool) {
	venue = strings.ToLower(strings.TrimSpace(venue))
	total := 0.0
	for _, row := range rows {
		if r148RiskHasEvent(row, storage.LivePendingRiskReleased) {
			continue
		}
		if len(row.Legs) == 0 || row.Intent.CostUSD < 0 || math.IsNaN(row.Intent.CostUSD) || math.IsInf(row.Intent.CostUSD, 0) {
			return 0, false
		}
		reserved := 0.0
		if strings.EqualFold(strings.TrimSpace(row.Intent.Product), "combo") {
			class, known := livePendingComboCryptoClass(row, venue)
			if !known || class == liveCryptoUnknown {
				return 0, false
			}
			if class == liveCryptoNo {
				continue
			}
			reserved = row.Intent.CostUSD
		} else {
			allPrincipal, cryptoPrincipal := 0.0, 0.0
			for _, leg := range row.Legs {
				action := strings.ToUpper(strings.TrimSpace(leg.Action))
				if action == "SELL" {
					continue
				}
				if action != "BUY" || (strings.ToUpper(strings.TrimSpace(leg.Side)) != "YES" &&
					strings.ToUpper(strings.TrimSpace(leg.Side)) != "NO") || leg.Quantity <= 0 ||
					leg.LimitPrice <= 0 || leg.LimitPrice >= 1 || math.IsNaN(leg.Quantity) ||
					math.IsNaN(leg.LimitPrice) || math.IsInf(leg.Quantity, 0) || math.IsInf(leg.LimitPrice, 0) {
					return 0, false
				}
				principal := leg.Quantity * leg.LimitPrice
				class := liveCryptoInstrumentClass(leg.Venue, leg.Ticker, "", row.Intent.SystemID)
				if class == liveCryptoUnknown {
					return 0, false
				}
				allPrincipal += principal
				if class == liveCryptoYes && (venue == "" || strings.EqualFold(leg.Venue, venue)) {
					cryptoPrincipal += principal
				}
			}
			if cryptoPrincipal <= 0 {
				continue
			}
			var known bool
			reserved, known = liveCryptoProportionalCost(row.Intent.CostUSD, allPrincipal, cryptoPrincipal)
			if !known {
				return 0, false
			}
		}
		// Single-order account visibility is exact. Packages retain their full reservation until
		// the coordinator proves and releases the complete state vector.
		if len(row.Legs) == 1 {
			if r148RiskHasEvent(row, storage.LivePendingRiskAccountVisible) {
				reserved = 0
			} else {
				for i := len(row.Events) - 1; i >= 0; i-- {
					event := row.Events[i]
					if event.EventType != storage.LivePendingRiskAck {
						continue
					}
					represented := restingByVenueOrder[liveCryptoRestingKey(row.Legs[0].Venue, event.OrderID)]
					reserved = math.Max(0, reserved-represented)
					break
				}
			}
		}
		total += reserved
	}
	if total < 0 || math.IsNaN(total) || math.IsInf(total, 0) {
		return 0, false
	}
	return total, true
}

func liveCryptoCapReason(current, proposed, cap float64) string {
	for _, value := range []float64{current, proposed, cap} {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return "crypto exposure or cap is unavailable"
		}
	}
	if cap <= 0 {
		return "crypto exposure cap is unavailable"
	}
	if current+proposed > cap+0.001 {
		return fmt.Sprintf("crypto exposure $%.2f + $%.2f would exceed the aggregate crypto cap $%.2f", current, proposed, cap)
	}
	return ""
}

func (s *Server) liveCryptoCapUSD() float64 {
	pct := s.cfg().Risk.LiveCryptoCapPct
	// Kalshi is always a reachable manual LIVE destination. PolyUS only contributes capital when
	// its explicit money-authority switch is enabled; configured credentials or an old ARM receipt
	// from a disabled venue must never enlarge the Kalshi crypto sleeve.
	deployable := s.liveRiskBankroll("kalshi")
	if s.livePolyUSDestinationEnabled() {
		deployable += s.liveRiskBankroll("polyus")
	}
	if pct <= 0 || pct > 1 || deployable <= 0 || math.IsNaN(pct) || math.IsInf(pct, 0) {
		return 0
	}
	cap := deployable * pct
	// The asset-class sleeve cannot exceed the already-authoritative combined exposure rail. This
	// is min(total cap, 50% of enabled capital), not 50% of the cap: with a 50% total rail, crypto
	// may still use all deployed exposure when it is the strongest eligible class.
	if totalCap := s.liveCap(); totalCap > 0 && cap > totalCap {
		cap = totalCap
	}
	return cap
}

// liveCryptoBudgetReason is called only from the serialized final venue handler, using the same
// account snapshot that checks total and per-venue exposure. Non-crypto candidates are unaffected.
func (s *Server) liveCryptoBudgetReason(candidate *liveMirrorCandidate, currentCrypto, cost float64,
	classificationKnown bool) string {
	if candidate == nil {
		return ""
	}
	class := liveCryptoCandidateClass(*candidate)
	if class == liveCryptoNo {
		return ""
	}
	if class == liveCryptoUnknown {
		return "crypto exposure classification is unavailable; refusing new crypto exposure"
	}
	return s.liveCryptoProposedBudgetReason(currentCrypto, cost, classificationKnown, true)
}

func (s *Server) liveCryptoProposedBudgetReason(currentCrypto, proposedCrypto float64,
	currentKnown, proposedKnown bool) string {
	if !proposedKnown {
		return "crypto exposure classification is unavailable; refusing new crypto exposure"
	}
	if proposedCrypto <= 0 { // a proven non-crypto order cannot increase the crypto sleeve
		return ""
	}
	if !currentKnown {
		return "crypto exposure classification is unavailable; refusing new crypto exposure"
	}
	return liveCryptoCapReason(currentCrypto, proposedCrypto, s.liveCryptoCapUSD())
}

// liveCryptoBundleCost conservatively charges an entire atomic/staged product to the crypto sleeve
// when any component is crypto. A multi-leg payoff cannot split its purchase cost into separately
// withdrawable asset-class principal, so fractional leg allocation would understate exposure.
func liveCryptoBundleCost(candidates []liveMirrorCandidate, cost float64) (float64, bool) {
	if cost < 0 || math.IsNaN(cost) || math.IsInf(cost, 0) || len(candidates) == 0 {
		return 0, false
	}
	crypto := false
	for _, candidate := range candidates {
		switch liveCryptoCandidateClass(candidate) {
		case liveCryptoYes:
			crypto = true
		case liveCryptoNo:
		default:
			return 0, false
		}
	}
	if crypto {
		return cost, true
	}
	return 0, true
}

func liveMirrorXMatchPreferred(a, b liveMirrorRankedCandidate) (preferA bool, decided bool) {
	if liveCryptoCandidateClass(a.Candidate) != liveCryptoYes ||
		liveCryptoCandidateClass(b.Candidate) != liveCryptoYes {
		return false, false
	}
	aX := strings.EqualFold(liveMirrorFamily(a.Candidate), "xmatch")
	bX := strings.EqualFold(liveMirrorFamily(b.Candidate), "xmatch")
	if aX == bX {
		return false, false
	}
	return aX, true
}
