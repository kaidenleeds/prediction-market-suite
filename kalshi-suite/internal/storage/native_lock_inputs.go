package storage

// Bounded read models for the three R139 native research systems. These readers expose only
// already prospective, book-native source rows; they never turn a historical verdict or a
// discovery-price signal into an execution candidate.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type NativeBasketInput struct {
	Observed          time.Time
	Venue, EventID    string
	Route             string
	IdentitySource    string
	IdentityComplete  bool
	MutuallyExclusive bool
	Exhaustive        bool
	PayoutLower       float64
	Legs              []BasketLeg
}

func (s *Store) RecentNativeBasketInputs(ctx context.Context, since time.Time, limit int) ([]NativeBasketInput, error) {
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	if since.IsZero() {
		since = time.Now().UTC().Add(-15 * time.Minute)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT observed_ts,venue,event_id,route,identity_source,
identity_complete,mutually_exclusive,exhaustive,payout_lower_bound,legs_json
FROM research_event_baskets WHERE observed_ts>=? ORDER BY id DESC LIMIT ?`,
		since.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NativeBasketInput
	for rows.Next() {
		var v NativeBasketInput
		var observed, legs string
		var identity, exclusive, exhaustive int
		if err := rows.Scan(&observed, &v.Venue, &v.EventID, &v.Route, &v.IdentitySource,
			&identity, &exclusive, &exhaustive, &v.PayoutLower, &legs); err != nil {
			return nil, err
		}
		v.Observed, _ = time.Parse(time.RFC3339Nano, observed)
		v.IdentityComplete, v.MutuallyExclusive, v.Exhaustive = identity != 0, exclusive != 0, exhaustive != 0
		if json.Unmarshal([]byte(legs), &v.Legs) != nil || len(v.Legs) < 2 {
			continue
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

type NativeUnitCandidateInput struct {
	Opened                                 time.Time
	Family, Platform, Origin, Ticker, Side string
	Category, FeeSource, QuoteSource       string
	Episode                                int
	Ask, FeePC, Depth, ResolveHours        float64
}

func (s *Store) RecentNativeUnitCandidates(ctx context.Context, since time.Time, limit int) ([]NativeUnitCandidateInput, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if since.IsZero() {
		since = time.Now().UTC().Add(-2 * time.Hour)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT opened_ts,family,platform,origin_layer,ticker,side,
episode,category,ask,fee_pc,depth,fee_source,quote_source,resolve_hours
FROM unit_trials
WHERE opened_ts>=? AND settled=0 AND platform='kalshi' AND fee_known=1
  AND TRIM(fee_source)!='' AND TRIM(quote_source)!='' AND ask>0 AND ask<1 AND depth>=2
ORDER BY id DESC LIMIT ?`, since.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NativeUnitCandidateInput
	for rows.Next() {
		var v NativeUnitCandidateInput
		var opened string
		if err := rows.Scan(&opened, &v.Family, &v.Platform, &v.Origin, &v.Ticker, &v.Side,
			&v.Episode, &v.Category, &v.Ask, &v.FeePC, &v.Depth, &v.FeeSource,
			&v.QuoteSource, &v.ResolveHours); err != nil {
			return nil, err
		}
		v.Opened, _ = time.Parse(time.RFC3339Nano, opened)
		out = append(out, v)
	}
	return out, rows.Err()
}

type NativeRouteSystemStat struct {
	SystemID                            string
	Rows, Candidates, Controls, Blocked int
	Grades                              int
}

func (s *Store) NativeRouteSystemStats(ctx context.Context, systems []string) (map[string]NativeRouteSystemStat, error) {
	out := make(map[string]NativeRouteSystemStat, len(systems))
	for _, system := range systems {
		var row NativeRouteSystemStat
		row.SystemID = system
		err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),
COALESCE(SUM(decision='candidate' OR instr(evidence_json,'"optimization_candidate":true')>0),0),
COALESCE(SUM(decision='control'),0),COALESCE(SUM(decision='blocked'),0),
(SELECT COUNT(*) FROM research_route_events e
 JOIN research_route_opportunities g ON g.opportunity_id=e.opportunity_id AND g.route_id=e.route_id
 WHERE g.system_name=? AND e.event_type='grade')
FROM research_route_opportunities WHERE system_name=?`, system, system).
			Scan(&row.Rows, &row.Candidates, &row.Controls, &row.Blocked, &row.Grades)
		if err != nil {
			return nil, err
		}
		out[system] = row
	}
	return out, nil
}

type NativeCollectorCycleStat struct {
	CollectorID, Status, ZeroReason, ErrorText string
	Cycles                                     int
	NeverRan, Alert                            bool
}

// NativeCollectorCurrentStat is the compact current-cycle receipt used by the default Systems
// digest. It intentionally contains no lifetime COUNT: one indexed latest-receipt lookup exposes
// whether the collector is current and what this bounded cycle actually saw/stored. Full lifetime
// totals remain available through NativeRouteSystemStats on explicit diagnostics only.
type NativeCollectorCurrentStat struct {
	CollectorID, Status, ZeroReason, ErrorText string
	Completed                                  time.Time
	Eligible, Attempted, Inserted, Duplicates  int
	Excluded                                   int
	Metrics                                    map[string]any
	NeverRan, Alert                            bool
}

func (s *Store) NativeCollectorCurrentStats(ctx context.Context, collectorIDs []string, now time.Time) (map[string]NativeCollectorCurrentStat, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	out := make(map[string]NativeCollectorCurrentStat, len(collectorIDs))
	for _, id := range collectorIDs {
		var v NativeCollectorCurrentStat
		v.CollectorID = strings.TrimSpace(id)
		var cadence float64
		var completed, metricsJSON string
		err := s.db.QueryRowContext(ctx, `SELECT c.expected_cadence_s,
COALESCE(r.status,''),COALESCE(r.completed_ts,''),COALESCE(r.zero_reason,''),COALESCE(r.error_text,''),
COALESCE(r.eligible,0),COALESCE(r.attempted,0),COALESCE(r.inserted,0),COALESCE(r.duplicates,0),
COALESCE(r.excluded_total,0),COALESCE(r.metrics_json,'{}')
FROM research_collector_specs c
LEFT JOIN research_collector_receipts r ON r.id=(
 SELECT x.id FROM research_collector_receipts x WHERE x.collector_id=c.collector_id ORDER BY x.id DESC LIMIT 1)
WHERE c.collector_id=? AND c.active=1 ORDER BY c.version DESC LIMIT 1`, v.CollectorID).
			Scan(&cadence, &v.Status, &completed, &v.ZeroReason, &v.ErrorText,
				&v.Eligible, &v.Attempted, &v.Inserted, &v.Duplicates, &v.Excluded, &metricsJSON)
		if errors.Is(err, sql.ErrNoRows) {
			v.NeverRan, v.Alert, v.ErrorText = true, true, "collector specification missing"
			out[v.CollectorID] = v
			continue
		}
		if err != nil {
			return nil, err
		}
		v.Metrics = map[string]any{}
		if err := json.Unmarshal([]byte(metricsJSON), &v.Metrics); err != nil {
			v.Alert, v.ErrorText = true, "latest collector metrics are invalid JSON"
		}
		v.NeverRan = completed == ""
		v.Alert = v.Alert || v.NeverRan || v.Status == "error" || v.Status == "blocked" || v.Status == "starved"
		if completed != "" {
			completedAt, parseErr := time.Parse(time.RFC3339Nano, completed)
			if parseErr != nil {
				v.Alert = true
				if v.ErrorText == "" {
					v.ErrorText = "latest collector completion timestamp is invalid"
				}
			} else {
				v.Completed = completedAt
				if cadence > 0 && now.Sub(completedAt) > time.Duration(3*cadence*float64(time.Second)) {
					v.Alert = true
				}
			}
		}
		out[v.CollectorID] = v
	}
	return out, nil
}

// NativeCollectorCycleStats is the hot Systems/briefing read for the three native collectors.
// It intentionally avoids CollectorLivenessReport's all-collector N+1 summary on a dashboard poll.
func (s *Store) NativeCollectorCycleStats(ctx context.Context, collectorIDs []string, now time.Time) (map[string]NativeCollectorCycleStat, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	out := make(map[string]NativeCollectorCycleStat, len(collectorIDs))
	for _, id := range collectorIDs {
		var v NativeCollectorCycleStat
		v.CollectorID = strings.TrimSpace(id)
		var cadence float64
		var completed string
		err := s.db.QueryRowContext(ctx, `SELECT c.expected_cadence_s,
COALESCE(r.status,''),COALESCE(r.completed_ts,''),COALESCE(r.zero_reason,''),COALESCE(r.error_text,''),
(SELECT COUNT(*) FROM research_collector_receipts n WHERE n.collector_id=c.collector_id)
FROM research_collector_specs c
LEFT JOIN research_collector_receipts r ON r.id=(SELECT MAX(x.id) FROM research_collector_receipts x WHERE x.collector_id=c.collector_id)
WHERE c.collector_id=? AND c.version=(SELECT MAX(v.version) FROM research_collector_specs v WHERE v.collector_id=c.collector_id)
LIMIT 1`, v.CollectorID).Scan(&cadence, &v.Status, &completed, &v.ZeroReason, &v.ErrorText, &v.Cycles)
		if errors.Is(err, sql.ErrNoRows) {
			v.NeverRan, v.Alert, v.ErrorText = true, true, "collector specification missing"
			out[v.CollectorID] = v
			continue
		}
		if err != nil {
			return nil, err
		}
		v.NeverRan = completed == ""
		v.Alert = v.NeverRan || v.Status == "error" || v.Status == "blocked" || v.Status == "starved"
		if completedAt, parseErr := time.Parse(time.RFC3339Nano, completed); parseErr == nil && cadence > 0 && now.Sub(completedAt) > time.Duration(3*cadence*float64(time.Second)) {
			v.Alert = true
		}
		out[v.CollectorID] = v
	}
	return out, nil
}
