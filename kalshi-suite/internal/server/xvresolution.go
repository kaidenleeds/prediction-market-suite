package server

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/resolutionbasis"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type xvlResolutionCacheEntry struct {
	Input storage.ResolutionBasisInput
	Found bool
	At    time.Time
}

type xvlResolutionCacheState struct {
	sync.Mutex
	Rows map[string]xvlResolutionCacheEntry
}

var xvlResolutionCaches sync.Map // *Server -> *xvlResolutionCacheState

func (s *Server) xvlResolutionCache() *xvlResolutionCacheState {
	v, _ := xvlResolutionCaches.LoadOrStore(s, &xvlResolutionCacheState{Rows: map[string]xvlResolutionCacheEntry{}})
	return v.(*xvlResolutionCacheState)
}

func xvlResolutionLeg(fallbackVenue, fallbackID string, in storage.ResolutionBasisInput, ok bool) resolutionbasis.LegTerms {
	if !ok {
		return resolutionbasis.LegTerms{Venue: fallbackVenue, InstrumentID: fallbackID}
	}
	return resolutionbasis.LegTerms{Venue: in.Venue, InstrumentID: in.Ticker, EventID: in.EventID,
		PayoffID: in.PayoffID, BasisID: in.CrossVenueBasis, NativeSide: in.NativeSide,
		InstrumentOrientation: in.Orientation, SettlementSource: in.SettlementSource,
		RulesHash: in.RulesHash, VoidPolicy: in.VoidPolicy, ScalarPolicy: in.ScalarPolicy,
		UnknownPolicy: in.UnknownPolicy, IdentityStatus: in.IdentityStatus,
		InstrumentVersion: in.InstrumentVersion, EventVersion: in.EventVersion,
		PayoffVersion: in.PayoffVersion}
}

// xvlResolutionCertificate freezes the exact immutable semantic versions that existed at first
// sight. A structural twin without two complete receipts still receives a hashed blocked
// certificate, so it remains observable research but can never become proof or a staked lock.
func xvlResolutionCertificate(c xvlCandidate, o xvlOrient, inputs map[string]storage.ResolutionBasisInput) resolutionbasis.Certificate {
	left, leftOK := inputs[storage.ResolutionBasisKey(c.aVenue, c.aID)]
	right, rightOK := inputs[storage.ResolutionBasisKey(c.bVenue, c.bID)]
	orientation := "same"
	if !c.sameSide {
		orientation = "inverse"
	}
	pairID := strings.TrimSpace(c.pair) + "|" + strings.TrimSpace(c.aID) + "|" + strings.TrimSpace(c.bID)
	return resolutionbasis.Issue(pairID, orientation, o.aSide, o.bSide,
		xvlResolutionLeg(c.aVenue, c.aID, left, leftOK),
		xvlResolutionLeg(c.bVenue, c.bID, right, rightOK))
}

// xvlPairResolutionInputs reduces the batch lookup to the only two rows a single certificate can
// consume. The lock scanner can carry thousands of catalog inputs in one pass; cloning that whole
// map once per candidate made the scan O(candidates^2) and, before R146, kept xvlMu locked for
// minutes. A certificate is deliberately incapable of observing any unrelated instrument.
func xvlPairResolutionInputs(inputs map[string]storage.ResolutionBasisInput, keys ...string) map[string]storage.ResolutionBasisInput {
	out := make(map[string]storage.ResolutionBasisInput, len(keys))
	for _, key := range keys {
		if input, ok := inputs[key]; ok {
			out[key] = input
		}
	}
	return out
}

// xvlCertifiedResolutionCertificate is the runtime boundary for every supported venue pair.
// Structural canonical rows are discovery only; they are upgraded to verified normalized terms
// exclusively when an immutable independently reviewed orientation certificate still references
// both current raw venue artifacts. Unsupported, stale, or mis-oriented pairs fail closed.
func (s *Server) xvlCertifiedResolutionCertificate(ctx context.Context, c xvlCandidate, o xvlOrient,
	inputs map[string]storage.ResolutionBasisInput) resolutionbasis.Certificate {
	leftCandidateKey := storage.ResolutionBasisKey(c.aVenue, c.aID)
	rightCandidateKey := storage.ResolutionBasisKey(c.bVenue, c.bID)
	pairInputs := xvlPairResolutionInputs(inputs, leftCandidateKey, rightCandidateKey)
	cert, current, err := s.store.CompatibleRulePairCertificateFor(ctx, c.aVenue, c.aID, c.bVenue, c.bID)
	expectedSame := cert.Orientation == "same"
	if err != nil || !current || c.sameSide != expectedSame {
		// Keep the structural identity fields for diagnostics but remove the semantic fields that
		// could otherwise make a raw-text coincidence look compatible.
		for key, in := range pairInputs {
			in.IdentityStatus, in.SettlementSource, in.RulesHash = "structural", "", ""
			in.VoidPolicy, in.ScalarPolicy, in.UnknownPolicy, in.CrossVenueBasis = "", "", "", ""
			pairInputs[key] = in
		}
		return xvlResolutionCertificate(c, o, pairInputs)
	}
	leftKey := storage.ResolutionBasisKey(cert.LeftVenue, cert.LeftInstrumentID)
	rightKey := storage.ResolutionBasisKey(cert.RightVenue, cert.RightInstrumentID)
	// A stored certificate may normalize the venue order. Re-select exactly those two keys, still
	// never carrying the unrelated batch into this per-pair operation.
	pairInputs = xvlPairResolutionInputs(inputs, leftKey, rightKey)
	canonical, canonicalOK := pairInputs[leftKey]
	if !canonicalOK || canonical.EventID != cert.CanonicalEventID || canonical.PayoffID != cert.LeftPayoffID {
		return xvlResolutionCertificate(c, o, map[string]storage.ResolutionBasisInput{})
	}
	for key, in := range pairInputs {
		expectedPayoff := cert.LeftPayoffID
		instrumentOrientation := "same"
		if key == rightKey {
			expectedPayoff = cert.RightPayoffID
			instrumentOrientation = cert.Orientation
		}
		if in.EventID != cert.CanonicalEventID || in.PayoffID != expectedPayoff || in.EventVersion != canonical.EventVersion {
			in.IdentityStatus, in.SettlementSource, in.RulesHash, in.CrossVenueBasis = "structural", "", "", ""
			in.VoidPolicy, in.ScalarPolicy, in.UnknownPolicy = "", "", ""
		} else {
			in.IdentityStatus = "verified"
			in.EventID, in.PayoffID = cert.CanonicalEventID, cert.CanonicalPayoffID
			in.PayoffVersion = canonical.PayoffVersion
			in.NativeSide, in.Orientation = "YES", instrumentOrientation
			in.CrossVenueBasis = cert.BasisID
			in.SettlementSource = cert.Left.SettlementSourceID + "|" + cert.Left.SettlementSourceURL
			in.RulesHash = cert.Left.RulesHash
			in.VoidPolicy, in.ScalarPolicy, in.UnknownPolicy = cert.Left.VoidPolicy, cert.Left.ScalarPolicy, cert.Left.UnknownPolicy
		}
		pairInputs[key] = in
	}
	return xvlResolutionCertificate(c, o, pairInputs)
}

func (s *Server) xvlResolutionInputs(ctx context.Context, candidates ...xvlCandidate) map[string]storage.ResolutionBasisInput {
	keys := make([]storage.ResolutionBasisInstrumentKey, 0, len(candidates)*2)
	wanted := make(map[string]storage.ResolutionBasisInstrumentKey, len(candidates)*2)
	for _, candidate := range candidates {
		for _, key := range []storage.ResolutionBasisInstrumentKey{
			{Venue: candidate.aVenue, Ticker: candidate.aID}, {Venue: candidate.bVenue, Ticker: candidate.bID},
		} {
			wanted[storage.ResolutionBasisKey(key.Venue, key.Ticker)] = key
		}
	}
	now := time.Now().UTC()
	cache := s.xvlResolutionCache()
	out := make(map[string]storage.ResolutionBasisInput, len(wanted))
	cache.Lock()
	for normalized, key := range wanted {
		entry, ok := cache.Rows[normalized]
		if ok && now.Sub(entry.At) >= 0 && now.Sub(entry.At) <= 30*time.Second {
			if entry.Found {
				out[normalized] = entry.Input
			}
			continue
		}
		keys = append(keys, key)
	}
	cache.Unlock()
	if len(keys) == 0 {
		return out
	}
	inputs, err := s.store.ResolutionBasisInputsForKeys(ctx, keys)
	if err != nil {
		// Empty input makes every certificate explicitly blocked. The lock scan remains research
		// observable and does not turn a transient registry read failure into a trading grant.
		return map[string]storage.ResolutionBasisInput{}
	}
	cache.Lock()
	for _, key := range keys {
		normalized := storage.ResolutionBasisKey(key.Venue, key.Ticker)
		input, found := inputs[normalized]
		cache.Rows[normalized] = xvlResolutionCacheEntry{Input: input, Found: found, At: now}
		if found {
			out[normalized] = input
		}
	}
	// Candidate churn is bounded, but remove old entries so long-lived market churn cannot grow
	// this cache indefinitely.
	if len(cache.Rows) > 10000 {
		for key, entry := range cache.Rows {
			if now.Sub(entry.At) > 10*time.Minute {
				delete(cache.Rows, key)
			}
		}
	}
	cache.Unlock()
	return out
}

// xvgapResolutionCertificate uses a canonical complementary orientation solely as an immutable
// witness that the two venue instruments describe the same payoff under the same settlement,
// void, scalar, and unknown-result rules. The directional gap trade may buy either side later;
// it cannot be logged into an economic family until this identity witness verifies.
func (s *Server) xvgapResolutionCertificate(ctx context.Context, kalshiTicker, polyUSSlug string, flip bool) resolutionbasis.Certificate {
	candidate := xvlCandidate{pair: "K-PUS", aVenue: "kalshi", aID: kalshiTicker,
		bVenue: "polyus", bID: polyUSSlug, sameSide: !flip}
	orientation := xvlOrient{aSide: "YES", bSide: "NO"}
	if flip {
		orientation.bSide = "YES"
	}
	inputs := s.xvlResolutionInputs(ctx, candidate)
	return s.xvlCertifiedResolutionCertificate(ctx, candidate, orientation, inputs)
}

type xvgapResolutionControl struct {
	KalshiTicker, PolyUSSlug, KalshiSide, PolyUSSide string
	KalshiAsk, PolyUSAsk, KalshiDepth, PolyUSDepth   float64
	KalshiBookSource, PolyUSBookSource               string
	KalshiFee, PolyUSFee                             float64
	KalshiFeeSource, PolyUSFeeSource                 string
	Gap, Spread                                      float64
	Flip                                             bool
}

// recordXVGAPResolutionControl keeps every certified or rejected same-book comparison visible in
// the identity-challenged system funnel. It is an observer control, never a trade candidate; the
// regular xvgap one-share ledger is reached only after the caller separately verifies the receipt.
func (s *Server) recordXVGAPResolutionControl(ctx context.Context, v xvgapResolutionControl,
	certificate resolutionbasis.Certificate, blocker string) {
	status := "rejected"
	if resolutionbasis.Verify(certificate) == nil {
		status = "verified"
	}
	if strings.TrimSpace(blocker) == "" && status == "rejected" {
		blocker = certificate.Blocker
	}
	_, _, _ = s.store.InsertResearchSystemObservation(context.WithoutCancel(ctx), storage.ResearchSystemObservation{
		Observed: time.Now().UTC(), SystemID: "identity-challenged-cross-venue-lock",
		OpportunityID: "xvgap|" + v.KalshiTicker + "|" + v.PolyUSSlug + "|" + v.KalshiSide + "|" + v.PolyUSSide,
		Kind:          "control", Cohort: "directional-gap-resolution-audit", CanonicalEventID: certificate.Left.EventID,
		Venue: "multi", Ticker: v.PolyUSSlug, Route: "observer", Side: v.PolyUSSide,
		CertificateStatus: status, CertificateHash: certificate.ContentHash,
		SourceClockID: "kalshi-ticker+polyus-market-ws", SourceArtifact: "simultaneous executable cross-venue books",
		PayoutLower: 0, PayoutUpper: 0, NetLower: 0, NetUpper: 0, OutcomeStatus: "open", Blocker: blocker,
		Inputs: map[string]any{
			"kalshi_ticker": v.KalshiTicker, "polyus_slug": v.PolyUSSlug, "flip": v.Flip,
			"kalshi_side": v.KalshiSide, "polyus_side": v.PolyUSSide,
			"kalshi_ask": v.KalshiAsk, "polyus_ask": v.PolyUSAsk,
			"kalshi_depth": v.KalshiDepth, "polyus_depth": v.PolyUSDepth,
			"kalshi_book_source": v.KalshiBookSource, "polyus_book_source": v.PolyUSBookSource,
			"kalshi_fee": v.KalshiFee, "polyus_fee": v.PolyUSFee,
			"kalshi_fee_source": v.KalshiFeeSource, "polyus_fee_source": v.PolyUSFeeSource,
			"gap_probability": v.Gap, "polyus_spread_probability": v.Spread,
			"resolution_certificate": certificate,
		},
	})
}
