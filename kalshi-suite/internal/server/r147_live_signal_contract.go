package server

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

// r147LiveInputClockSnapshot is the small, testable transport receipt consumed by prospective
// LIVE proof.  The executable destination book is refreshed separately by liveMirrorExecutable;
// these fields cover only additional upstream venue inputs named by the canonical signal contract.
type r147LiveInputClockSnapshot struct {
	KalshiClient              bool
	KalshiSubscribed          int
	KalshiFresh               int
	KalshiGaps                int64
	KalshiError               string
	PINTClient, PINTConnected bool
	PINTFresh                 int
	PINTAgeSeconds            float64
	PUSClient, PUSFrameOK     bool
	PUSExecutable             int
	PUSFrameAge               time.Duration
	ExternalObservedAt        time.Time
	Now                       time.Time
}

func r147ParseInputObservedAt(raw string) time.Time {
	at, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw))
	if err != nil {
		return time.Time{}
	}
	return at
}

func r147BindSignalContract(c liveMirrorCandidate, route string) (
	liveMirrorCandidate, r147SystemVariantSignalContract, string) {
	family := strings.ToLower(strings.TrimSpace(liveMirrorFamily(c)))
	if family == "" {
		return c, r147SystemVariantSignalContract{}, "prospective-allocation-exact-system-family-unavailable"
	}
	matches := r147ExactSignalContracts(family, c.Platform, c.Side, route)
	action := strings.ToUpper(strings.TrimSpace(c.Action))
	if action == "" {
		action = "BUY"
	}
	kept := matches[:0]
	for _, row := range matches {
		contractAction := strings.ToUpper(strings.TrimSpace(row.Action))
		if contractAction == "" {
			contractAction = "BUY"
		}
		if contractAction == action {
			kept = append(kept, row)
		}
	}
	matches = kept
	if len(matches) == 0 {
		return c, r147SystemVariantSignalContract{}, "prospective-allocation-signal-contract-absent"
	}
	wantTopology := strings.ToUpper(strings.TrimSpace(c.InputTopology))
	wantID := strings.TrimSpace(c.SignalContractID)
	if wantTopology != "" || wantID != "" {
		for _, row := range matches {
			id := r147SignalContractIdentity(row)
			if wantTopology != "" && !strings.EqualFold(wantTopology, row.InputTopology) {
				continue
			}
			if wantID != "" && wantID != id {
				continue
			}
			c.InputTopology, c.SignalContractID = row.InputTopology, id
			return c, row, ""
		}
		return c, r147SystemVariantSignalContract{}, "prospective-allocation-signal-contract-mismatch"
	}
	if len(matches) != 1 {
		return c, r147SystemVariantSignalContract{}, "prospective-allocation-signal-topology-ambiguous"
	}
	row := matches[0]
	c.InputTopology, c.SignalContractID = row.InputTopology, r147SignalContractIdentity(row)
	return c, row, ""
}

func liveAllocationSignalBaseKey(c liveMirrorCandidate) string {
	action := strings.ToUpper(strings.TrimSpace(c.Action))
	if action == "" {
		action = "BUY"
	}
	return strings.Join([]string{strings.ToLower(strings.TrimSpace(liveMirrorFamily(c))), action,
		strings.ToLower(strings.TrimSpace(c.Platform)), strings.TrimSpace(c.Ticker),
		strings.ToUpper(strings.TrimSpace(c.Side))}, "\x1f")
}

func liveAllocationSignalKey(c liveMirrorCandidate) string {
	if strings.TrimSpace(c.SignalContractID) == "" || strings.TrimSpace(c.InputTopology) == "" {
		return ""
	}
	return liveAllocationSignalBaseKey(c) + "\x1f" + c.SignalContractID
}

// liveAllocationBindSignalContract recovers an omitted contract only from one still-fresh exact
// prospective receipt.  This is needed by the internal HTTP handler, whose legacy request body
// reconstructs the candidate from mirror_source+ticker+side.  Two fresh topology receipts are
// ambiguous and fail closed; their economics can never be pooled.
func (s *Server) liveAllocationBindSignalContract(c liveMirrorCandidate, route string,
	allowReceiptRecovery bool) (liveMirrorCandidate, r147SystemVariantSignalContract, string) {
	bound, contract, reason := r147BindSignalContract(c, route)
	if reason != "prospective-allocation-signal-topology-ambiguous" || !allowReceiptRecovery {
		return bound, contract, reason
	}
	family := strings.ToLower(strings.TrimSpace(liveMirrorFamily(c)))
	matches := r147ExactSignalContracts(family, c.Platform, c.Side, route)
	now := time.Now()
	found := 0
	for _, row := range matches {
		candidate := c
		candidate.InputTopology = row.InputTopology
		candidate.SignalContractID = r147SignalContractIdentity(row)
		key := liveAllocationSignalKey(candidate)
		s.liveMirrorMu.Lock()
		at, ok := s.liveAllocationSignal[key]
		s.liveMirrorMu.Unlock()
		if !ok || at.IsZero() || now.Sub(at) < 0 || now.Sub(at) > liveMirrorTTL {
			continue
		}
		bound, contract, found = candidate, row, found+1
	}
	if found == 1 {
		return bound, contract, ""
	}
	if found > 1 {
		return c, r147SystemVariantSignalContract{}, "prospective-allocation-multiple-fresh-signal-topologies"
	}
	return c, r147SystemVariantSignalContract{}, reason
}

func liveAllocationInputClockReason(contract r147SystemVariantSignalContract,
	clocks r147LiveInputClockSnapshot) string {
	execution := r147VenueCode(contract.ExecutionVenue)
	if execution != "K" && execution != "PUS" {
		return "prospective-allocation-pint-is-input-only"
	}
	if strings.TrimSpace(contract.InputTopology) == "" || len(contract.RequiredInputs) == 0 ||
		r147SignalContractIdentity(contract) == "" {
		return "prospective-allocation-signal-topology-absent"
	}
	for _, required := range contract.RequiredInputs {
		required = strings.ToUpper(strings.TrimSpace(required))
		if required == execution {
			// liveMirrorExecutable owns the destination lifecycle, full book, tick and depth clock.
			continue
		}
		switch required {
		case "PINT":
			if !clocks.PINTClient {
				return "prospective-allocation-polyint-input-client-unavailable"
			}
			if !clocks.PINTConnected || clocks.PINTFresh < 1 || clocks.PINTAgeSeconds < 0 ||
				clocks.PINTAgeSeconds > 45 || math.IsNaN(clocks.PINTAgeSeconds) || math.IsInf(clocks.PINTAgeSeconds, 0) {
				return "prospective-allocation-polyint-input-books-not-fresh"
			}
		case "PUS":
			if !clocks.PUSClient {
				return "prospective-allocation-polyus-input-client-unavailable"
			}
			if !clocks.PUSFrameOK || clocks.PUSFrameAge < 0 ||
				clocks.PUSFrameAge > polymarketus.MarketsWSPrimaryTransportMaxAge || clocks.PUSExecutable < 1 {
				return "prospective-allocation-polyus-input-books-not-fresh"
			}
		case "K":
			// For a PolyUS destination, the exact signal receipt below proves which market fired;
			// this clock proves the required Kalshi book transport was current and gap-free.
			if !clocks.KalshiClient {
				return "prospective-allocation-kalshi-input-client-unavailable"
			}
			if clocks.KalshiSubscribed < 1 || clocks.KalshiFresh < 1 || clocks.KalshiGaps != 0 ||
				strings.TrimSpace(clocks.KalshiError) != "" {
				return "prospective-allocation-kalshi-input-books-not-fresh"
			}
		case "SPOT", "OKX", "SPORTSBOOK", "NOAA":
			now := clocks.Now
			if now.IsZero() {
				now = time.Now()
			}
			age := now.Sub(clocks.ExternalObservedAt)
			maxAge := map[string]time.Duration{
				"SPOT": 20 * time.Second, "OKX": 15 * time.Minute,
				"SPORTSBOOK": 5 * time.Minute, "NOAA": 90 * time.Minute,
			}[required]
			if clocks.ExternalObservedAt.IsZero() || age < -2*time.Second || age > maxAge {
				return fmt.Sprintf("prospective-allocation-%s-input-receipt-not-fresh", strings.ToLower(required))
			}
		default:
			return fmt.Sprintf("prospective-allocation-required-input-clock-unavailable:%s", required)
		}
	}
	return ""
}

func (s *Server) liveAllocationInputFreshReason(contract r147SystemVariantSignalContract, candidate liveMirrorCandidate) string {
	clocks := r147LiveInputClockSnapshot{ExternalObservedAt: candidate.InputObservedAt, Now: time.Now()}
	if s.kal != nil {
		clocks.KalshiClient = true
		clocks.KalshiSubscribed, clocks.KalshiFresh, clocks.KalshiGaps, clocks.KalshiError = s.kal.BookStats()
	}
	if s.poly != nil {
		clocks.PINTClient = true
		clocks.PINTConnected, _, clocks.PINTFresh, clocks.PINTAgeSeconds = s.poly.CLOBStats()
	}
	if s.polyUSWS != nil {
		clocks.PUSClient = true
		clocks.PUSFrameAge, clocks.PUSFrameOK = s.polyUSWS.PrimaryFrameAge()
		clocks.PUSExecutable = s.polyUSWS.ExecutableCount()
	}
	return liveAllocationInputClockReason(contract, clocks)
}
