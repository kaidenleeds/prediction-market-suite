package server

// R148 certified cross-venue signal variants.
//
// A signal discovered on Kalshi or PolyUS may be expressed on the other venue only when an
// independently normalized, still-current rule certificate identifies the exact destination
// instrument.  The certificate (not a title) owns side orientation.  A complete destination book,
// exact fee schedule, quantity rule, lifecycle and horizon are then required before the derived
// signal enters the same insertSignal -> unit trial -> generic Paper -> sealed LIVE-ready route as
// a native signal.  Every missing prerequisite is an explicit refusal; there is no blind mirror.

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	r148CrossVenueSignalMarker = "xv-exec-variant=1"
	r148CrossVenueCertTTL      = time.Minute
)

// These are portable single-instrument hypotheses. Their source observation may be useful on an
// exactly equivalent contract at the other venue. Systems that already own a cross-venue bridge,
// a dedicated portfolio/ML route, or a multi-leg product are deliberately absent: manufacturing a
// second generic signal would duplicate or change their strategy rather than add a venue variant.
var r148PortableCrossVenueSignalSystems = map[string]bool{
	"bookskew": true, "favlong": true, "freshfade": true, "freshlist": true,
	"fundtilt": true, "kalshi-flow": true, "kalshi-whale": true,
	"kcrypto": true, "kflow": true, "kthresh": true, "meanrev": true, "notail": true,
	"polyus-consensus": true, "polyus-flow": true, "polyus-whale": true,
	"sharpline": true, "spotlag": true,
}

type r148CrossVenueCertificateCache struct {
	at    time.Time
	certs []storage.RulePairCertificateSpec
	err   string
}

func r148CrossVenueSignalPortable(systemID string) bool {
	systemID = strings.ToLower(strings.TrimSpace(systemID))
	if strings.HasPrefix(systemID, "invert:") {
		systemID = strings.TrimPrefix(systemID, "invert:")
	}
	return r148PortableCrossVenueSignalSystems[systemID]
}

func r148OtherExecutionVenue(venue string) string {
	switch strings.ToLower(strings.TrimSpace(venue)) {
	case "kalshi":
		return "polyus"
	case "polyus":
		return "kalshi"
	default:
		return ""
	}
}

func r148CrossVenueSignalAlreadyDerived(sig storage.Signal) bool {
	return strings.Contains(strings.ToLower(sig.ExecExpr), r148CrossVenueSignalMarker)
}

// r148CrossVenueProducerCapable is a code-path claim, not a freshness or match claim. It says an
// exact source-side producer can reach this destination side through the certified variant adapter.
// Both destination sides are possible because a current certificate may be same or inverse.
func r148CrossVenueProducerCapable(systemID, destinationVenue, _ string) bool {
	base := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(systemID)), "invert:")
	if !r148CrossVenueSignalPortable(base) {
		return false
	}
	sourceVenue := r148OtherExecutionVenue(destinationVenue)
	return sourceVenue != "" && (r147NativeSignalProducerCells[base+"|"+sourceVenue+"|YES"] ||
		r147NativeSignalProducerCells[base+"|"+sourceVenue+"|NO"])
}

func r148CrossVenueTopologyName(sourceTopology, destinationVenue string) string {
	seen := map[string]bool{}
	for _, part := range strings.Split(strings.ToUpper(strings.TrimSpace(sourceTopology)), "-") {
		if part != "" {
			seen[part] = true
		}
	}
	if code := r147VenueCode(destinationVenue); code != "" {
		seen[code] = true
	}
	ordered := []string{"K", "PUS", "PINT", "SPOT", "OKX", "SPORTSBOOK", "NOAA"}
	out := make([]string, 0, len(seen))
	for _, part := range ordered {
		if seen[part] {
			out = append(out, part)
			delete(seen, part)
		}
	}
	// Unknown inputs are retained deterministically rather than silently discarded.
	for len(seen) > 0 {
		best := ""
		for part := range seen {
			if best == "" || part < best {
				best = part
			}
		}
		out = append(out, best)
		delete(seen, best)
	}
	return strings.Join(out, "-")
}

func r148CrossVenueInputTopologies(systemID, destinationVenue string) []r147InputTopology {
	if !r148CrossVenueProducerCapable(systemID, destinationVenue, "") {
		return nil
	}
	sourceVenue := r148OtherExecutionVenue(destinationVenue)
	base := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(systemID)), "invert:")
	source := r147NativeInputTopologies(base, sourceVenue)
	out := make([]r147InputTopology, 0, len(source))
	seen := map[string]bool{}
	for _, topology := range source {
		name := r148CrossVenueTopologyName(topology.name, destinationVenue)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		required := append([]string(nil), topology.required...)
		destinationCode := r147VenueCode(destinationVenue)
		found := false
		for _, input := range required {
			found = found || input == destinationCode
		}
		if !found {
			required = append(required, destinationCode)
		}
		out = append(out, r147Topology(name, required...))
	}
	return out
}

func r148CrossVenueDestination(cert storage.RulePairCertificateSpec, sourceVenue, sourceID string) (string, string, bool) {
	sourceVenue, sourceID = strings.ToLower(strings.TrimSpace(sourceVenue)), strings.TrimSpace(sourceID)
	switch {
	case strings.EqualFold(cert.LeftVenue, sourceVenue) && cert.LeftInstrumentID == sourceID:
		return strings.ToLower(cert.RightVenue), cert.RightInstrumentID, true
	case strings.EqualFold(cert.RightVenue, sourceVenue) && cert.RightInstrumentID == sourceID:
		return strings.ToLower(cert.LeftVenue), cert.LeftInstrumentID, true
	default:
		return "", "", false
	}
}

func r148PrepareCrossVenueSignal(source storage.Signal, cert storage.RulePairCertificateSpec,
	destinationVenue, destinationID, destinationSide, title, topology string, sourceObservedAt time.Time,
	resolveHours float64) storage.Signal {
	out := source
	out.Platform, out.Ticker, out.Side = destinationVenue, destinationID, destinationSide
	out.Title, out.MarketType, out.Kind = title, "", ""
	out.EntryPrice, out.SpreadCents, out.BookDepth = 0, 0, 0
	out.FeePC, out.PxAgeS = nil, nil
	out.SecsToStart, out.IsLive, out.ResolveHours = nil, 0, resolveHours
	resetExecutableBookSnapshot(&out)
	out.ExecExpr = fmt.Sprintf("%s/source=%s:%s/source-side=%s/destination=%s:%s/destination-side=%s/certificate=%s/orientation=%s",
		r148CrossVenueSignalMarker, strings.ToLower(source.Platform), source.Ticker,
		strings.ToUpper(source.Side), destinationVenue, destinationID, destinationSide,
		cert.SpecHash, strings.ToLower(cert.Orientation))
	if topology != "" {
		// The adapter must carry the detector's actual external-input clock. Minting a fresh
		// timestamp here would turn a stale spot/sportsbook/NOAA observation into apparently fresh
		// LIVE authority merely because its equivalent contract was re-quoted on another venue.
		out.ExecExpr += "/" + r147InputReceiptExpr(topology, sourceObservedAt)
	}
	return out
}

func r148TopologyNeedsInputTimestamp(topology string) bool {
	for _, input := range strings.Split(strings.ToUpper(strings.TrimSpace(topology)), "-") {
		switch input {
		case "SPOT", "OKX", "SPORTSBOOK", "NOAA":
			return true
		}
	}
	return false
}

func (s *Server) r148CrossVenueCertificates(ctx context.Context, sourceVenue, sourceID,
	destinationVenue string) ([]storage.RulePairCertificateSpec, error) {
	key := strings.ToLower(sourceVenue) + "\x00" + sourceID + "\x00" + strings.ToLower(destinationVenue)
	now := time.Now()
	s.xvSignalVariantMu.Lock()
	if cached, ok := s.xvSignalVariantCert[key]; ok && now.Sub(cached.at) <= r148CrossVenueCertTTL {
		out := append([]storage.RulePairCertificateSpec(nil), cached.certs...)
		errText := cached.err
		s.xvSignalVariantMu.Unlock()
		if errText != "" {
			return nil, fmt.Errorf("%s", errText)
		}
		// Cache only destination discovery. Current raw-rule and canonical versions are rechecked
		// on every candidate; a venue rules revision may revoke a certificate immediately.
		current := make([]storage.RulePairCertificateSpec, 0, len(out))
		for _, candidate := range out {
			cert, valid, err := s.store.CompatibleRulePairCertificateFor(ctx, candidate.LeftVenue,
				candidate.LeftInstrumentID, candidate.RightVenue, candidate.RightInstrumentID)
			if err != nil {
				return nil, err
			}
			if valid && cert.SpecHash == candidate.SpecHash {
				current = append(current, cert)
			}
		}
		return current, nil
	}
	s.xvSignalVariantMu.Unlock()
	certs, err := s.store.CompatibleRulePairCertificatesForInstrument(ctx, sourceVenue, sourceID, destinationVenue, 3)
	entry := r148CrossVenueCertificateCache{at: now, certs: append([]storage.RulePairCertificateSpec(nil), certs...)}
	if err != nil {
		entry.err = err.Error()
	}
	s.xvSignalVariantMu.Lock()
	if s.xvSignalVariantCert == nil {
		s.xvSignalVariantCert = map[string]r148CrossVenueCertificateCache{}
	}
	if len(s.xvSignalVariantCert) >= 12000 {
		s.xvSignalVariantCert = map[string]r148CrossVenueCertificateCache{}
	}
	s.xvSignalVariantCert[key] = entry
	s.xvSignalVariantMu.Unlock()
	return certs, err
}

type r148CrossVenueSignalLineage struct {
	SourceVenue, SourceTicker, SourceSide, CertificateHash string
}

func r148CrossVenueLineage(sig storage.Signal) (r148CrossVenueSignalLineage, bool) {
	if !r148CrossVenueSignalAlreadyDerived(sig) {
		return r148CrossVenueSignalLineage{}, false
	}
	values := map[string]string{}
	for _, token := range strings.Split(sig.ExecExpr, "/") {
		key, value, ok := strings.Cut(strings.TrimSpace(token), "=")
		if ok {
			values[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
		}
	}
	sourceVenue, sourceTicker, ok := strings.Cut(values["source"], ":")
	if !ok {
		return r148CrossVenueSignalLineage{}, false
	}
	out := r148CrossVenueSignalLineage{SourceVenue: strings.ToLower(strings.TrimSpace(sourceVenue)),
		SourceTicker: strings.TrimSpace(sourceTicker), SourceSide: strings.ToUpper(strings.TrimSpace(values["source-side"])),
		CertificateHash: strings.TrimSpace(values["certificate"])}
	if r148OtherExecutionVenue(out.SourceVenue) == "" || out.SourceTicker == "" ||
		(out.SourceSide != "YES" && out.SourceSide != "NO") || out.CertificateHash == "" {
		return r148CrossVenueSignalLineage{}, false
	}
	return out, true
}

func (s *Server) liveR148CrossVenueVariantReason(ctx context.Context, c liveMirrorCandidate) string {
	if strings.TrimSpace(c.CrossVenueCertificateHash) == "" {
		return ""
	}
	if s.store == nil || c.CrossVenueSourceVenue == "" || c.CrossVenueSourceTicker == "" ||
		(c.CrossVenueSourceSide != "YES" && c.CrossVenueSourceSide != "NO") {
		return "cross-venue-certificate-lineage-incomplete"
	}
	certs, err := s.store.CompatibleRulePairCertificatesForInstrument(ctx, c.CrossVenueSourceVenue,
		c.CrossVenueSourceTicker, c.Platform, 3)
	if err != nil {
		return "cross-venue-certificate-current-read-error"
	}
	if len(certs) != 1 || certs[0].SpecHash != c.CrossVenueCertificateHash {
		return "cross-venue-certificate-missing-stale-or-ambiguous"
	}
	destinationVenue, destinationID, ok := r148CrossVenueDestination(certs[0],
		c.CrossVenueSourceVenue, c.CrossVenueSourceTicker)
	if !ok || !strings.EqualFold(destinationVenue, c.Platform) || destinationID != c.Ticker {
		return "cross-venue-certificate-destination-drift"
	}
	wantSide, ok := whaleExitCertificateSide(c.CrossVenueSourceSide, certs[0].Orientation)
	if ok && c.Inverted {
		wantSide = concreteOppositeSide(wantSide)
	}
	if !ok || !strings.EqualFold(wantSide, c.Side) {
		return "cross-venue-certificate-side-drift"
	}
	return ""
}

// paperR148CrossVenueVariantReason applies the same current immutable identity check at the final
// Paper boundary. Paper is the evidence that may later qualify LIVE, so allowing it to fill after
// a raw-rule revision would poison the supposedly 1:1 route even though LIVE correctly failed.
func (s *Server) paperR148CrossVenueVariantReason(ctx context.Context, sig storage.Signal, inverted bool) string {
	if !r148CrossVenueSignalAlreadyDerived(sig) {
		return ""
	}
	lineage, ok := r148CrossVenueLineage(sig)
	if !ok {
		return "cross-venue-certificate-lineage-incomplete"
	}
	return s.liveR148CrossVenueVariantReason(ctx, liveMirrorCandidate{
		Platform: sig.Platform, Ticker: sig.Ticker, Side: sig.Side, Inverted: inverted,
		CrossVenueSourceVenue: lineage.SourceVenue, CrossVenueSourceTicker: lineage.SourceTicker,
		CrossVenueSourceSide: lineage.SourceSide, CrossVenueCertificateHash: lineage.CertificateHash,
	})
}

func (s *Server) auditR148CrossVenueSignalVariant(ctx context.Context, source storage.Signal,
	state, reason, destinationVenue, destinationID, destinationSide string,
	cert storage.RulePairCertificateSpec, quoted *storage.Signal, quantitySource string) {
	now := time.Now()
	observationSlot := r148CrossVenueDecisionSlot(now)
	fingerprint, sourceObservation := r148CrossVenueDecisionFingerprint(source, state, reason,
		destinationVenue, destinationID, destinationSide, cert, quoted, quantitySource, observationSlot)
	s.xvSignalVariantMu.Lock()
	if s.xvSignalVariantAudit == nil {
		s.xvSignalVariantAudit = map[string]time.Time{}
	}
	if at := s.xvSignalVariantAudit[fingerprint]; !at.IsZero() && now.Sub(at) < 10*time.Minute {
		s.xvSignalVariantMu.Unlock()
		return
	}
	if len(s.xvSignalVariantAudit) >= 12000 {
		s.xvSignalVariantAudit = map[string]time.Time{}
	}
	s.xvSignalVariantAudit[fingerprint] = now
	s.xvSignalVariantMu.Unlock()
	detail := map[string]any{
		"system": source.SignalType, "state": state, "reason": reason,
		"source_venue": strings.ToLower(source.Platform), "source_ticker": source.Ticker,
		"source_side": strings.ToUpper(source.Side), "source_episode": source.Episode,
		"source_entry_price": source.EntryPrice, "source_spread_cents": source.SpreadCents,
		"source_strength": source.Strength, "source_notional": source.Notional,
		"source_exec_expr": source.ExecExpr, "source_observation_hash": sourceObservation,
		"source_signal_slot": observationSlot,
		"destination_venue":  destinationVenue,
		"destination_ticker": destinationID, "destination_side": destinationSide,
		"certificate_hash": cert.SpecHash, "certificate_orientation": cert.Orientation,
		"canonical_event_id": cert.CanonicalEventID, "canonical_payoff_id": cert.CanonicalPayoffID,
		"quantity_source":    quantitySource,
		"execution_pipeline": "certified destination signal -> exact one-share route -> generic Paper -> shared sealed LIVE mirror",
	}
	if quoted != nil {
		detail["book_source"], detail["ask"], detail["bid"] = quoted.BookSource, quoted.BookAsk, quoted.BookBid
		detail["ask_depth"], detail["bid_depth"] = quoted.BookAskDepth, quoted.BookBidDepth
		detail["spread_cents"], detail["quote_age_s"] = quoted.SpreadCents, quoted.BookQuoteAgeS
		detail["taker_tick"], detail["maker_tick"] = quoted.BookTakerTick, quoted.BookMakerTick
		detail["taker_fee_pc"], detail["maker_fee_pc"] = quoted.BookTakerFeePC, quoted.BookMakerFeePC
	}
	b, _ := json.Marshal(detail)
	_ = s.store.Audit(context.WithoutCancel(ctx), "info", "cross-venue-system-variant",
		fmt.Sprintf("%s %s %s:%s -> %s:%s: %s", state, source.SignalType,
			strings.ToLower(source.Platform), source.Ticker, destinationVenue, destinationID, reason), string(b))
}

// r148CrossVenueDecisionFingerprint deduplicates only an identical economic decision. The former
// family+ticker fingerprint hid a changed source observation or destination quote for ten minutes.
// Volatile age/latency measurements are deliberately excluded: retrying the same frame 30 seconds
// later is not a new economic decision, while a changed price, size, fee, topology, episode,
// certificate or outcome is.
func r148CrossVenueDecisionFingerprint(source storage.Signal, state, reason, destinationVenue,
	destinationID, destinationSide string, cert storage.RulePairCertificateSpec,
	quoted *storage.Signal, quantitySource, observationSlot string) (string, string) {
	normalize := func(sig storage.Signal) storage.Signal {
		sig.PxAgeS, sig.BookQuoteAgeS, sig.BookLatencyMS = nil, nil, nil
		return sig
	}
	source = normalize(source)
	sourceJSON, _ := json.Marshal(source)
	sourceHash := fmt.Sprintf("%x", sha256.Sum256(sourceJSON))
	quotedHash := ""
	if quoted != nil {
		q := normalize(*quoted)
		quotedJSON, _ := json.Marshal(q)
		quotedHash = fmt.Sprintf("%x", sha256.Sum256(quotedJSON))
	}
	route := strings.Join([]string{state, reason, strings.ToLower(destinationVenue), destinationID,
		strings.ToUpper(destinationSide), cert.SpecHash, quantitySource, observationSlot, sourceHash, quotedHash}, "\x00")
	return fmt.Sprintf("%x", sha256.Sum256([]byte(route))), sourceHash
}

func r148CrossVenueDecisionSlot(now time.Time) string {
	slot := now.UTC().Format(time.RFC3339Nano)
	if len(slot) >= 15 {
		return slot[:15] // same immutable ten-minute observation identity as signal_log.slot
	}
	return slot
}

// collectR148CrossVenueSignalVariant is called from insertSignal after the source signal is durable.
// The derived row comes back through insertSignal so it cannot bypass unit evidence, Paper gates,
// LIVE mirror checks, settlement, CLV, relation tracking, or runtime liveness accounting.
func (s *Server) collectR148CrossVenueSignalVariant(ctx context.Context, source storage.Signal) {
	if s == nil || s.store == nil || r148CrossVenueSignalAlreadyDerived(source) ||
		strings.HasPrefix(strings.ToLower(strings.TrimSpace(source.SignalType)), "invert:") ||
		!r148CrossVenueSignalPortable(source.SignalType) {
		return
	}
	sourceVenue := strings.ToLower(strings.TrimSpace(source.Platform))
	destinationVenue := r148OtherExecutionVenue(sourceVenue)
	if destinationVenue == "" || source.Ticker == "" {
		return
	}
	certs, err := s.r148CrossVenueCertificates(ctx, sourceVenue, source.Ticker, destinationVenue)
	if err != nil {
		s.auditR148CrossVenueSignalVariant(ctx, source, "REJECTED", "rule-certificate-read-error",
			destinationVenue, "", "", storage.RulePairCertificateSpec{}, nil, "")
		return
	}
	if len(certs) == 0 {
		s.auditR148CrossVenueSignalVariant(ctx, source, "REJECTED", "no-current-normalized-settlement-certificate",
			destinationVenue, "", "", storage.RulePairCertificateSpec{}, nil, "")
		return
	}
	if len(certs) != 1 {
		s.auditR148CrossVenueSignalVariant(ctx, source, "REJECTED", "ambiguous-multiple-current-destinations",
			destinationVenue, "", "", storage.RulePairCertificateSpec{}, nil, "")
		return
	}
	cert := certs[0]
	actualVenue, destinationID, ok := r148CrossVenueDestination(cert, sourceVenue, source.Ticker)
	if !ok || actualVenue != destinationVenue || destinationID == "" {
		s.auditR148CrossVenueSignalVariant(ctx, source, "REJECTED", "certificate-does-not-bind-source-and-destination",
			destinationVenue, destinationID, "", cert, nil, "")
		return
	}
	destinationSide, ok := whaleExitCertificateSide(source.Side, cert.Orientation)
	if !ok {
		s.auditR148CrossVenueSignalVariant(ctx, source, "REJECTED", "certificate-side-orientation-invalid",
			destinationVenue, destinationID, "", cert, nil, "")
		return
	}
	resolveHours, _, _ := s.sigUrgency(ctx, destinationVenue, destinationID)
	if resolveHours <= 0 {
		s.auditR148CrossVenueSignalVariant(ctx, source, "REJECTED", "destination-time-remaining-unknown",
			destinationVenue, destinationID, destinationSide, cert, nil, "")
		return
	}
	sourceTopology := r147SignalInputTopology(source)
	allowedTopologies := r147NativeInputTopologies(source.SignalType, sourceVenue)
	if sourceTopology == "" {
		if len(allowedTopologies) == 1 {
			sourceTopology = allowedTopologies[0].name
		}
	}
	if sourceTopology == "" {
		s.auditR148CrossVenueSignalVariant(ctx, source, "REJECTED", "source-input-topology-unavailable",
			destinationVenue, destinationID, destinationSide, cert, nil, "")
		return
	}
	allowed := false
	for _, candidate := range allowedTopologies {
		if strings.EqualFold(candidate.name, sourceTopology) {
			allowed = true
			break
		}
	}
	if !allowed {
		s.auditR148CrossVenueSignalVariant(ctx, source, "REJECTED", "source-input-topology-not-valid-for-system",
			destinationVenue, destinationID, destinationSide, cert, nil, "")
		return
	}
	topology := r148CrossVenueTopologyName(sourceTopology, destinationVenue)
	sourceObservedAt := r147SignalInputObservedAt(source)
	if r148TopologyNeedsInputTimestamp(topology) && sourceObservedAt.IsZero() {
		s.auditR148CrossVenueSignalVariant(ctx, source, "REJECTED", "source-external-input-timestamp-unavailable",
			destinationVenue, destinationID, destinationSide, cert, nil, "")
		return
	}
	derived := r148PrepareCrossVenueSignal(source, cert, destinationVenue, destinationID,
		destinationSide, s.xvlFriendlyName(destinationVenue, destinationID), topology, sourceObservedAt, resolveHours)
	quoted, feeSource, quoteOK := s.completeBookSignal(derived)
	if !quoteOK || quoted.BookAskDepth == nil || quoted.BookBidDepth == nil || quoted.FeePC == nil {
		s.auditR148CrossVenueSignalVariant(ctx, source, "REJECTED", "destination-complete-book-depth-tick-or-fee-unavailable",
			destinationVenue, destinationID, destinationSide, cert, nil, "")
		return
	}
	_, minimum, quantitySource, quantityOK := s.fundedComboQuantityRule(destinationVenue, destinationID)
	if !quantityOK || minimum <= 0 || quoted.BookDepth+1e-9 < minimum {
		s.auditR148CrossVenueSignalVariant(ctx, source, "REJECTED", "destination-minimum-quantity-or-depth-unavailable",
			destinationVenue, destinationID, destinationSide, cert, &quoted, quantitySource)
		return
	}
	quoted.ExecExpr = derived.ExecExpr + "/book=" + quoted.BookSource + "/fee=" + feeSource +
		"/minimum-qty-source=" + quantitySource
	quoted.PricingVersion = mlBookFeatureSchema
	if err := s.insertSignal(ctx, quoted); err != nil {
		s.auditR148CrossVenueSignalVariant(ctx, source, "REJECTED", "destination-signal-storage-error",
			destinationVenue, destinationID, destinationSide, cert, &quoted, quantitySource)
		return
	}
	s.auditR148CrossVenueSignalVariant(ctx, source, "COLLECTED", "certified-variant-entered-shared-execution-pipeline",
		destinationVenue, destinationID, destinationSide, cert, &quoted, quantitySource)
}
