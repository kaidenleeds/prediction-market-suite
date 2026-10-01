package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// ResolutionBasisInput is the newest immutable semantic receipt for one venue instrument.
// It is deliberately price-free: executable books and fees remain separate money-truth gates.
// Empty fields are meaningful and must fail closed at the cross-venue certificate issuer.
type ResolutionBasisInput struct {
	Venue, Ticker, EventID, PayoffID               string
	NativeSide, Orientation                        string
	SettlementSource, RulesHash, CrossVenueBasis   string
	VoidPolicy, ScalarPolicy, UnknownPolicy        string
	IdentityStatus                                 string
	InstrumentVersion, EventVersion, PayoffVersion int
}

// ResolutionBasisKey preserves the venue-native instrument id while normalizing only the venue.
func ResolutionBasisKey(venue, ticker string) string {
	return strings.ToLower(strings.TrimSpace(venue)) + "|" + strings.TrimSpace(ticker)
}

func evidenceString(raw, key string) string {
	var values map[string]any
	if json.Unmarshal([]byte(raw), &values) != nil {
		return ""
	}
	v, ok := values[key].(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(v)
}

// consistentTerm returns the one normalized term represented by every nonempty authority. Any
// disagreement is returned as empty so the caller cannot silently choose the convenient source.
func consistentTerm(values ...string) string {
	var out string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if out == "" {
			out = value
			continue
		}
		if !strings.EqualFold(out, value) {
			return ""
		}
	}
	return out
}

// ResolutionBasisInputs joins each newest immutable instrument version to the exact event/payoff
// versions it cited. Venue rule semantics may be carried in instrument evidence; event/payoff
// evidence can corroborate them but may never override a disagreement. No title/slug inference is
// permitted here.
func (s *Store) resolutionBasisInputs(ctx context.Context, filter string, args []any, limit int) (map[string]ResolutionBasisInput, error) {
	query := `SELECT i.venue,i.ticker,i.version,i.event_id,i.event_version,
i.payoff_id,i.payoff_version,i.native_side,i.orientation,i.settlement_source,i.rules_hash,
i.cross_venue_basis,i.identity_status,i.evidence_json,
e.settlement_source,e.void_policy,e.evidence_json,
p.settlement_source,p.identity_status,p.evidence_json
FROM research_instrument_specs i
JOIN research_event_specs e ON e.event_id=i.event_id AND e.version=i.event_version
JOIN research_payoff_specs p ON p.event_id=i.event_id AND p.event_version=i.event_version
 AND p.payoff_id=i.payoff_id AND p.version=i.payoff_version
WHERE `
	if strings.TrimSpace(filter) != "" {
		query += "(" + filter + ") AND "
	}
	query += `i.version=(SELECT MAX(v.version) FROM research_instrument_specs v
 WHERE v.venue=i.venue AND v.ticker=i.ticker)
ORDER BY i.venue,i.ticker LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]ResolutionBasisInput)
	for rows.Next() {
		var v ResolutionBasisInput
		var instrumentIdentity, payoffIdentity string
		var instrumentEvidence, eventEvidence, payoffEvidence string
		var instrumentSource, eventSource, payoffSource, eventVoid string
		if err := rows.Scan(&v.Venue, &v.Ticker, &v.InstrumentVersion, &v.EventID, &v.EventVersion,
			&v.PayoffID, &v.PayoffVersion, &v.NativeSide, &v.Orientation, &instrumentSource,
			&v.RulesHash, &v.CrossVenueBasis, &instrumentIdentity, &instrumentEvidence,
			&eventSource, &eventVoid, &eventEvidence, &payoffSource, &payoffIdentity,
			&payoffEvidence); err != nil {
			return nil, err
		}
		v.SettlementSource = consistentTerm(instrumentSource, eventSource, payoffSource)
		v.VoidPolicy = consistentTerm(evidenceString(instrumentEvidence, "void_policy"),
			eventVoid, evidenceString(eventEvidence, "void_policy"), evidenceString(payoffEvidence, "void_policy"))
		v.ScalarPolicy = consistentTerm(evidenceString(instrumentEvidence, "scalar_policy"),
			evidenceString(eventEvidence, "scalar_policy"), evidenceString(payoffEvidence, "scalar_policy"))
		v.UnknownPolicy = consistentTerm(evidenceString(instrumentEvidence, "unknown_policy"),
			evidenceString(eventEvidence, "unknown_policy"), evidenceString(payoffEvidence, "unknown_policy"))
		if strings.EqualFold(strings.TrimSpace(instrumentIdentity), "verified") &&
			strings.EqualFold(strings.TrimSpace(payoffIdentity), "verified") {
			v.IdentityStatus = "verified"
		}
		out[ResolutionBasisKey(v.Venue, v.Ticker)] = v
	}
	return out, rows.Err()
}

func (s *Store) ResolutionBasisInputs(ctx context.Context, limit int) (map[string]ResolutionBasisInput, error) {
	if limit <= 0 || limit > 100000 {
		limit = 50000
	}
	return s.resolutionBasisInputs(ctx, "", nil, limit)
}

// ResolutionBasisInstrumentKey names one venue-native instrument without weakening its native ID.
// It lets hot cross-venue scanners read only their bounded candidate set instead of repeatedly
// materializing the production-size canonical registry.
type ResolutionBasisInstrumentKey struct {
	Venue, Ticker string
}

// ResolutionBasisInputsForKeys returns the newest immutable semantic receipt for exactly the
// requested instruments. Queries are chunked below SQLite's bind-variable ceiling; a missing key
// remains missing and therefore fails closed at certificate issuance.
func (s *Store) ResolutionBasisInputsForKeys(ctx context.Context, keys []ResolutionBasisInstrumentKey) (map[string]ResolutionBasisInput, error) {
	dedup := make([]ResolutionBasisInstrumentKey, 0, len(keys))
	seen := make(map[string]bool, len(keys))
	for _, key := range keys {
		key.Venue = strings.ToLower(strings.TrimSpace(key.Venue))
		key.Ticker = strings.TrimSpace(key.Ticker)
		if key.Venue == "" || key.Ticker == "" {
			continue
		}
		normalized := ResolutionBasisKey(key.Venue, key.Ticker)
		if seen[normalized] {
			continue
		}
		seen[normalized] = true
		dedup = append(dedup, key)
	}
	out := make(map[string]ResolutionBasisInput, len(dedup))
	const chunkSize = 200 // 400 bind values plus LIMIT, safely below SQLite's common 999 limit.
	for start := 0; start < len(dedup); start += chunkSize {
		end := start + chunkSize
		if end > len(dedup) {
			end = len(dedup)
		}
		filter := make([]string, 0, end-start)
		args := make([]any, 0, 2*(end-start))
		for _, key := range dedup[start:end] {
			filter = append(filter, "(i.venue=? AND i.ticker=?)")
			args = append(args, key.Venue, key.Ticker)
		}
		rows, err := s.resolutionBasisInputs(ctx, strings.Join(filter, " OR "), args, end-start)
		if err != nil {
			return nil, fmt.Errorf("resolution basis key chunk: %w", err)
		}
		for key, value := range rows {
			out[key] = value
		}
	}
	return out, nil
}
