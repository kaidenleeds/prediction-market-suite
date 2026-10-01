package storage

// R148 bundle leaderboard adapter. Multi-leg systems own an exact economic unit (one complete
// staged bundle), not a synthetic single share. This reader collapses repeated quote snapshots to
// the first prospectively recorded system+opportunity cell, then exposes the same time/rate fields
// used by the Systems leaderboard. It is reporting evidence only; LIVE proof continues to use the
// separately sealed route contract and can never be granted by this aggregate.

import (
	"context"
	"database/sql"
	"math"
	"sort"
	"strings"
	"time"
)

type bundleLeaderboardAgg struct {
	stat                    UnitTrialLeaderboardStat
	first, last             time.Time
	settledValues           []float64
	events, settledEvents   map[string]struct{}
	occupiedSeconds         float64
	settledCost, settledFee float64
}

// ResearchRouteBundleLeaderboard returns one row per exact system+venue-shape. N and
// SettledMarkets both mean distinct graded bundle opportunities; repeated snapshots or alternate
// solver rows for the same opportunity do not manufacture sample size.
func (s *Store) ResearchRouteBundleLeaderboard(ctx context.Context, now time.Time) ([]UnitTrialLeaderboardStat, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	rows, err := s.db.QueryContext(ctx, `
WITH shaped AS (
 SELECT b.*,
		CASE
		  WHEN NOT EXISTS(SELECT 1 FROM research_route_bundle_legs l WHERE l.bundle_id=b.bundle_id AND l.venue<>'kalshi') THEN 'kalshi'
		  WHEN NOT EXISTS(SELECT 1 FROM research_route_bundle_legs l WHERE l.bundle_id=b.bundle_id AND l.venue<>'polyus') THEN 'polyus'
		  ELSE 'crossvenue'
		END platform
	FROM research_route_bundles b
	WHERE b.certificate_status='verified'
	  AND julianday(b.observed_ts) IS NOT NULL AND julianday(b.observed_ts)<=julianday(?)
), ranked AS (
	SELECT b.*,
		   ROW_NUMBER() OVER (
			 PARTITION BY b.system_id,b.platform,b.opportunity_id
			 ORDER BY julianday(b.observed_ts),b.observed_ts,b.bundle_id
		   ) AS opportunity_rank
	FROM shaped b
), cells AS (
	SELECT b.system_id,b.opportunity_id,b.canonical_event_id,b.observed_ts,
		   b.platform,
        b.size_units,b.total_cost,b.total_fee,
        e.observed_ts grade_ts,e.realized_net,e.capital_seconds
 FROM ranked b
 LEFT JOIN research_route_bundle_events e
   ON e.bundle_id=b.bundle_id AND e.event_type='grade'
  AND julianday(e.observed_ts) IS NOT NULL AND julianday(e.observed_ts)<=julianday(?)
 WHERE b.opportunity_rank=1
)
SELECT system_id,platform,opportunity_id,canonical_event_id,observed_ts,grade_ts,
       size_units,total_cost,total_fee,realized_net,capital_seconds
FROM cells ORDER BY observed_ts,system_id,opportunity_id`, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	aggs := make(map[string]*bundleLeaderboardAgg)
	for rows.Next() {
		var system, platform, opportunity, eventID, observedS string
		var gradeS sql.NullString
		var size, cost, fee float64
		var realized, capital sql.NullFloat64
		if err := rows.Scan(&system, &platform, &opportunity, &eventID, &observedS, &gradeS,
			&size, &cost, &fee, &realized, &capital); err != nil {
			return nil, err
		}
		observed, ok := parseUnitTrialTime(observedS)
		if !ok || observed.After(now) || size <= 0 || !finiteBundle(size) || !finiteBundle(cost) ||
			!finiteBundle(fee) || cost <= 0 || fee < 0 {
			continue
		}
		key := strings.TrimSpace(system) + "\x00" + strings.ToLower(strings.TrimSpace(platform))
		a := aggs[key]
		if a == nil {
			a = &bundleLeaderboardAgg{events: make(map[string]struct{}), settledEvents: make(map[string]struct{})}
			a.stat.Family = strings.TrimSpace(system)
			a.stat.Platform = strings.ToLower(strings.TrimSpace(platform))
			a.stat.OriginLayer = "strategy"
			a.stat.Side = "BUNDLE"
			a.first, a.last = observed, observed
			aggs[key] = a
		}
		a.stat.Total++
		a.stat.UniqueMarkets++
		if observed.Before(a.first) {
			a.first = observed
		}
		if observed.After(a.last) {
			a.last = observed
		}
		if eventID != "" {
			a.events[eventID] = struct{}{}
		}
		if !gradeS.Valid || !realized.Valid || !finiteBundle(realized.Float64) {
			a.stat.Open++
			a.stat.OpenMarkets++
			continue
		}
		graded, ok := parseUnitTrialTime(gradeS.String)
		if !ok || graded.Before(observed) || graded.After(now) {
			a.stat.Open++
			a.stat.OpenMarkets++
			continue
		}
		value := realized.Float64 / size
		if !finiteBundle(value) {
			a.stat.Open++
			a.stat.OpenMarkets++
			continue
		}
		a.stat.N++
		a.stat.SettledMarkets++
		a.stat.TotalPnL += value
		a.settledCost += cost / size
		a.settledFee += fee / size
		a.settledValues = append(a.settledValues, value)
		if eventID != "" {
			a.settledEvents[eventID] = struct{}{}
		}
		if graded.After(a.last) {
			a.last = graded
		}
		if capital.Valid && capital.Float64 > 0 && finiteBundle(capital.Float64) {
			a.occupiedSeconds += capital.Float64
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]UnitTrialLeaderboardStat, 0, len(aggs))
	for _, a := range aggs {
		if a.stat.Total == 0 {
			continue
		}
		if a.stat.N > 0 {
			a.stat.MeanAsk = a.settledCost / float64(a.stat.N)
			a.stat.MeanFeePC = a.settledFee / float64(a.stat.N)
		}
		_, a.stat.MeanPC, a.stat.SDPC = unitTrialMeanSD(a.settledValues)
		a.stat.FirstOpened = a.first.UTC().Format(time.RFC3339Nano)
		a.stat.LastActivity = a.last.UTC().Format(time.RFC3339Nano)
		a.stat.TrackedSeconds = unitTrialElapsedSeconds(now, a.first)
		a.stat.TrackedDays = a.stat.TrackedSeconds / (24 * time.Hour).Seconds()
		a.stat.OpportunitiesPerDay = float64(a.stat.Total) / a.stat.TrackedDays
		a.stat.SettledPerDay = float64(a.stat.N) / a.stat.TrackedDays
		a.stat.NetPerCalendarDay = a.stat.TotalPnL / a.stat.TrackedDays
		a.stat.OccupiedShareSeconds = a.occupiedSeconds
		a.stat.OccupiedShareDays = a.occupiedSeconds / (24 * time.Hour).Seconds()
		if a.stat.OccupiedShareDays > 0 {
			a.stat.NetPerOccupiedShareDay = a.stat.TotalPnL / a.stat.OccupiedShareDays
		}
		a.stat.SettledEventClusters = len(a.settledEvents)
		a.stat.OpenCanonicalEventClusters = len(a.events) - len(a.settledEvents)
		if a.stat.OpenCanonicalEventClusters < 0 {
			a.stat.OpenCanonicalEventClusters = 0
		}
		out = append(out, a.stat)
	}
	sort.Slice(out, func(i, j int) bool {
		if math.Abs(out[i].NetPerCalendarDay-out[j].NetPerCalendarDay) > 1e-12 {
			return out[i].NetPerCalendarDay > out[j].NetPerCalendarDay
		}
		return out[i].Family+out[i].Platform < out[j].Family+out[j].Platform
	})
	return out, nil
}
