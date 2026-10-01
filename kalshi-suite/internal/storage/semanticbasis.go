package storage

import (
	"context"
	"sort"
	"strings"
)

type SemanticInstrumentView struct {
	Venue, Ticker, EventID, PayoffID             string
	NativeSide, Orientation                      string
	SettlementSource, RulesHash, CrossVenueBasis string
	FeeAuthority, IdentityStatus                 string
	EventVersion, PayoffVersion                  int
}

type SemanticRelationView struct {
	EventID, LeftPayoffID, RightPayoffID  string
	RelationType, IdentityStatus          string
	EventVersion                          int
	LeftPayoffVersion, RightPayoffVersion int
	PayoutFloor                           *float64
}

// SemanticBasisInputs returns only the newest immutable identity versions. The server's pure
// semantic router may compare these; no price, order or authority table reads this method.
func (s *Store) SemanticBasisInputs(ctx context.Context, limit int) ([]SemanticInstrumentView, []SemanticRelationView, error) {
	if limit <= 0 || limit > 100000 {
		limit = 50000
	}
	// Venue-local catalog identities can never form a cross-venue route. Loading all of them first
	// exhausted the 50k input/pair bounds before the small structurally shared subset was reached.
	// Filter to current events that actually contain at least two venues before applying the bound.
	rows, err := s.db.QueryContext(ctx, `SELECT i.venue,i.ticker,i.event_id,i.event_version,
i.payoff_id,i.payoff_version,i.native_side,i.orientation,i.settlement_source,i.rules_hash,
i.cross_venue_basis,i.fee_authority,i.identity_status
FROM research_instrument_specs i
WHERE i.version=(SELECT MAX(v.version) FROM research_instrument_specs v
 WHERE v.venue=i.venue AND v.ticker=i.ticker)
 AND EXISTS (SELECT 1 FROM research_instrument_specs j
  WHERE j.event_id=i.event_id AND j.venue!=i.venue
    AND j.version=(SELECT MAX(jv.version) FROM research_instrument_specs jv
      WHERE jv.venue=j.venue AND jv.ticker=j.ticker))
ORDER BY i.event_id,i.cross_venue_basis,i.payoff_id,i.venue,i.ticker LIMIT ?`, limit)
	if err != nil {
		return nil, nil, err
	}
	var instruments []SemanticInstrumentView
	for rows.Next() {
		var v SemanticInstrumentView
		if err := rows.Scan(&v.Venue, &v.Ticker, &v.EventID, &v.EventVersion, &v.PayoffID,
			&v.PayoffVersion, &v.NativeSide, &v.Orientation, &v.SettlementSource, &v.RulesHash,
			&v.CrossVenueBasis, &v.FeeAuthority, &v.IdentityStatus); err != nil {
			rows.Close()
			return nil, nil, err
		}
		instruments = append(instruments, v)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, err
	}
	rows.Close()

	var relations []SemanticRelationView
	eventSet := map[string]bool{}
	for _, instrument := range instruments {
		eventSet[instrument.EventID] = true
	}
	events := make([]string, 0, len(eventSet))
	for eventID := range eventSet {
		events = append(events, eventID)
	}
	sort.Strings(events)
	// Keep SQLite parameter counts and read locks bounded while loading only relations that can
	// affect the selected cross-venue events. Venue-local complement rows are irrelevant here.
	for start := 0; start < len(events); start += 400 {
		end := start + 400
		if end > len(events) {
			end = len(events)
		}
		args := make([]any, 0, end-start)
		marks := make([]string, 0, end-start)
		for _, eventID := range events[start:end] {
			args = append(args, eventID)
			marks = append(marks, "?")
		}
		rows, err = s.db.QueryContext(ctx, `SELECT r.event_id,r.event_version,r.left_payoff_id,r.left_payoff_version,
r.right_payoff_id,r.right_payoff_version,r.relation_type,r.identity_status,r.payout_floor
FROM research_payoff_relations r
WHERE r.event_id IN (`+strings.Join(marks, ",")+`)
 AND r.version=(SELECT MAX(v.version) FROM research_payoff_relations v
  WHERE v.relation_id=r.relation_id)
ORDER BY r.event_id,r.relation_id`, args...)
		if err != nil {
			return nil, nil, err
		}
		for rows.Next() {
			var v SemanticRelationView
			if err := rows.Scan(&v.EventID, &v.EventVersion, &v.LeftPayoffID, &v.LeftPayoffVersion,
				&v.RightPayoffID, &v.RightPayoffVersion, &v.RelationType,
				&v.IdentityStatus, &v.PayoutFloor); err != nil {
				rows.Close()
				return nil, nil, err
			}
			relations = append(relations, v)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, nil, err
		}
		rows.Close()
	}
	return instruments, relations, nil
}
