package storage

// Canonical attribution and economics for funded Combo Paper and Combo Lab. The source ledgers
// remain authoritative; this adapter only groups rows whose immutable identity is complete.

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

type comboIdentityLeg struct {
	Platform string `json:"platform"`
	Ticker   string `json:"ticker"`
	Side     string `json:"side"`
}

func comboIdentityFromLegJSON(raw string) (venue, key string, n int, ok bool) {
	var legs []comboIdentityLeg
	if json.Unmarshal([]byte(raw), &legs) != nil || len(legs) < 2 || len(legs) > 6 {
		return "", "", 0, false
	}
	parts := make([]string, 0, len(legs))
	venue = strings.ToLower(strings.TrimSpace(legs[0].Platform))
	if venue != "kalshi" && venue != "polyus" {
		return "", "", 0, false
	}
	seen := map[string]struct{}{}
	for _, leg := range legs {
		platform := strings.ToLower(strings.TrimSpace(leg.Platform))
		ticker := strings.ToUpper(strings.TrimSpace(leg.Ticker))
		side := strings.ToUpper(strings.TrimSpace(leg.Side))
		if platform == "" || ticker == "" || (side != "YES" && side != "NO") {
			return "", "", 0, false
		}
		if platform != venue {
			venue = "mixed"
		}
		contract := platform + "|" + ticker
		if _, duplicate := seen[contract]; duplicate {
			return "", "", 0, false
		}
		seen[contract] = struct{}{}
		parts = append(parts, contract+"|"+side)
	}
	sort.Strings(parts)
	h := sha1.Sum([]byte(strings.Join(parts, "&")))
	return venue, hex.EncodeToString(h[:])[:16], len(legs), true
}

// backfillPaperComboAttribution upgrades only rows whose immutable route/cohort or typed RFQ link
// identifies one owner. Blank legacy/manual rows remain deliberately unattributed.
func backfillPaperComboAttribution(db *sql.DB) error {
	rows, err := db.Query(`SELECT p.id,p.route_source,p.cohort,p.legs,
COALESCE(i.system_id,''),COALESCE(i.cohort,'')
FROM paper_parlays p
LEFT JOIN research_route_bundle_promotion_intents i ON i.paper_parlay_id=p.id
WHERE COALESCE(p.canonical_system_id,'')=''`)
	if err != nil {
		return err
	}
	type update struct {
		id, legs                                             int64
		system, venue, relation, producer, route, epoch, key string
	}
	var updates []update
	for rows.Next() {
		var id int64
		var routeSource, cohort, legsJSON, typedSystem, typedCohort string
		if err := rows.Scan(&id, &routeSource, &cohort, &legsJSON, &typedSystem, &typedCohort); err != nil {
			rows.Close()
			return err
		}
		venue, key, n, valid := comboIdentityFromLegJSON(legsJSON)
		if !valid || venue == "mixed" {
			continue
		}
		u := update{id: id, legs: int64(n), venue: venue, key: key}
		switch {
		case strings.TrimSpace(typedSystem) != "":
			u.system, u.relation, u.producer, u.route = strings.TrimSpace(typedSystem), "related", "rfq", "kalshi-rfq"
			u.epoch = "typed-rfq:" + strings.TrimSpace(typedCohort)
		case routeSource == FundedPositiveSystemComboRoute && cohort == ComboLabCohortRollingPositive:
			u.system, u.relation, u.producer, u.route = "parlay-"+strconv.Itoa(n)+"leg", "independent", "positive-system", "paper-combo-taker"
			u.epoch = routeSource + ":" + cohort
		case routeSource == "new-ml-combo-paper-v1" && cohort == "book-native-v2-ml-combo":
			u.system, u.relation, u.producer, u.route = "parlay-"+strconv.Itoa(n)+"leg", "independent", "ml", "paper-combo-taker"
			u.epoch = routeSource + ":" + cohort
		default:
			continue
		}
		updates = append(updates, u)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, u := range updates {
		if _, err := db.Exec(`UPDATE paper_parlays SET canonical_system_id=?,combo_venue=?,leg_count=?,
relation_class=?,producer_family=?,combo_route=?,experiment_epoch=?,combo_key=?
WHERE id=? AND canonical_system_id=''`, u.system, u.venue, u.legs, u.relation, u.producer,
			u.route, u.epoch, u.key, u.id); err != nil {
			return err
		}
	}
	return nil
}

type comboLeaderboardRow struct {
	family, venue, relation, producer, route, epoch, source, unit, comboKey string
	opened, closed                                                          time.Time
	open                                                                    bool
	value, entry, fee                                                       float64
}

type comboLeaderboardMarket struct {
	values     []float64
	entry, fee float64
}

type comboLeaderboardAgg struct {
	stat        UnitTrialLeaderboardStat
	first, last time.Time
	settledRaw  int
	markets     map[string]*comboLeaderboardMarket
	openMarkets map[string]struct{}
}

func comboLeaderboardGroupKey(r comboLeaderboardRow) string {
	return strings.Join([]string{r.family, r.venue, r.relation, r.producer, r.route, r.epoch, r.source}, "\x00")
}

func appendComboLeaderboardRow(aggs map[string]*comboLeaderboardAgg, r comboLeaderboardRow, now time.Time) {
	if r.family == "" || r.comboKey == "" || r.opened.IsZero() || r.opened.After(now) ||
		(r.venue != "kalshi" && r.venue != "polyus" && r.venue != "mixed" && r.venue != "crossvenue") ||
		(r.relation != "independent" && r.relation != "related" && r.relation != "unknown") ||
		r.producer == "" || r.route == "" || r.epoch == "" {
		return
	}
	key := comboLeaderboardGroupKey(r)
	a := aggs[key]
	if a == nil {
		a = &comboLeaderboardAgg{markets: map[string]*comboLeaderboardMarket{}, openMarkets: map[string]struct{}{}, first: r.opened, last: r.opened}
		a.stat = UnitTrialLeaderboardStat{Family: r.family, Platform: r.venue, OriginLayer: "strategy", Side: "BUNDLE",
			Route: r.route, ProducerFamily: r.producer, RelationClass: r.relation,
			ExperimentEpoch: r.epoch, EconomicSource: r.source, EconomicUnit: r.unit,
			EconomicsScope: comboEconomicsScope(r.source)}
		aggs[key] = a
	}
	a.stat.Total++
	if r.opened.Before(a.first) {
		a.first = r.opened
	}
	if r.opened.After(a.last) {
		a.last = r.opened
	}
	if r.open {
		a.stat.Open++
		a.openMarkets[r.comboKey] = struct{}{}
		return
	}
	if r.closed.IsZero() || r.closed.Before(r.opened) || r.closed.After(now) || math.IsNaN(r.value) || math.IsInf(r.value, 0) {
		return
	}
	if r.closed.After(a.last) {
		a.last = r.closed
	}
	a.settledRaw++
	m := a.markets[r.comboKey]
	if m == nil {
		m = &comboLeaderboardMarket{}
		a.markets[r.comboKey] = m
	}
	m.values = append(m.values, r.value)
	m.entry += r.entry
	m.fee += r.fee
}

func comboEconomicsScope(source string) string {
	if strings.EqualFold(strings.TrimSpace(source), "combo-paper") {
		return "funded-paper"
	}
	return "research-unit"
}

// ComboSystemLeaderboard exposes exact attributed package units. Source rows are keyed once by
// their immutable position/grade primary key, then repeated observations of the same combo key are
// averaged before confidence/sample calculations.
func (s *Store) ComboSystemLeaderboard(ctx context.Context, now time.Time) ([]UnitTrialLeaderboardStat, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	aggs := map[string]*comboLeaderboardAgg{}
	rows, err := s.db.QueryContext(ctx, `SELECT canonical_system_id,combo_venue,relation_class,
producer_family,combo_route,experiment_epoch,combo_key,ts,COALESCE(settled_ts,''),status,
contracts,price,COALESCE(fees,0),COALESCE(realized,0)
FROM paper_parlays WHERE COALESCE(artifact,0)=0 AND canonical_system_id<>'' AND leg_count BETWEEN 2 AND 6`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var r comboLeaderboardRow
		var openedS, closedS, status string
		var contracts, realized float64
		if err := rows.Scan(&r.family, &r.venue, &r.relation, &r.producer, &r.route, &r.epoch, &r.comboKey,
			&openedS, &closedS, &status, &contracts, &r.entry, &r.fee, &realized); err != nil {
			rows.Close()
			return nil, err
		}
		r.opened, _ = parseUnitTrialTime(openedS)
		r.closed, _ = parseUnitTrialTime(closedS)
		r.open = status == "open"
		r.source, r.unit = "combo-paper", "package_unit"
		if contracts > 0 {
			r.value, r.fee = realized/contracts, r.fee/contracts
		}
		appendComboLeaderboardRow(aggs, r, now)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	grades, err := s.PlabGradeReceipts(ctx)
	if err != nil {
		return nil, err
	}
	for _, g := range grades {
		opened := time.Unix(g.CandidateAt, 0).UTC()
		closed, _ := parseUnitTrialTime(g.GradedTS)
		appendComboLeaderboardRow(aggs, comboLeaderboardRow{family: g.CanonicalSystemID, venue: g.ComboVenue,
			relation: g.RelationClass, producer: g.ProducerFamily, route: g.ComboRoute, epoch: g.ExperimentEpoch,
			source: "combo-lab", unit: "entry_dollar", comboKey: g.ComboKey, opened: opened, closed: closed,
			value: g.RealizedReturn, entry: g.EntryCapital, fee: g.Fees}, now)
	}

	// Named staged locks already persist accepted/rejected/open/settled lifecycle events. Only the
	// accepted package is economic; rejected attempts remain visible in their dedicated funnel.
	staged, err := s.db.QueryContext(ctx, `SELECT a.system_id,a.venue_shape,a.leg_count,a.cohort,
a.opportunity_id,a.candidate_ts,accept.observed_ts,settled.observed_ts,a.size_units,
a.total_cost,a.total_fee,settled.realized_net
FROM staged_paper_bundle_attempts a
JOIN staged_paper_bundle_events accept ON accept.package_id=a.package_id AND accept.event_type='accepted'
LEFT JOIN staged_paper_bundle_events settled ON settled.package_id=a.package_id AND settled.event_type='settled'
WHERE a.leg_count BETWEEN 2 AND 6`)
	if err != nil {
		return nil, err
	}
	for staged.Next() {
		var r comboLeaderboardRow
		var legs int
		var candidateS, acceptedS string
		var settledS sql.NullString
		var size, cost, fee float64
		var realized sql.NullFloat64
		if err := staged.Scan(&r.family, &r.venue, &legs, &r.epoch, &r.comboKey, &candidateS, &acceptedS,
			&settledS, &size, &cost, &fee, &realized); err != nil {
			staged.Close()
			return nil, err
		}
		r.opened, _ = parseUnitTrialTime(acceptedS)
		if r.opened.IsZero() {
			r.opened, _ = parseUnitTrialTime(candidateS)
		}
		r.relation, r.producer, r.route, r.source, r.unit = "related", "lock", "staged-paper", "combo-paper", "package_unit"
		r.open = !settledS.Valid
		if settledS.Valid {
			r.closed, _ = parseUnitTrialTime(settledS.String)
		}
		if size > 0 {
			r.entry = (cost + fee) / size
			r.fee = fee / size
			if realized.Valid {
				r.value = realized.Float64 / size
			}
		}
		appendComboLeaderboardRow(aggs, r, now)
	}
	if err := staged.Close(); err != nil {
		return nil, err
	}

	out := make([]UnitTrialLeaderboardStat, 0, len(aggs))
	for _, a := range aggs {
		if a.stat.Total == 0 {
			continue
		}
		values := make([]float64, 0, len(a.markets))
		entry, fee := 0.0, 0.0
		for _, m := range a.markets {
			_, mean, _ := unitTrialMeanSD(m.values)
			values = append(values, mean)
			entry += m.entry / float64(len(m.values))
			fee += m.fee / float64(len(m.values))
		}
		a.stat.N, a.stat.SettledMarkets = a.settledRaw, len(a.markets)
		a.stat.OpenMarkets = len(a.openMarkets)
		allKeys := map[string]struct{}{}
		for k := range a.markets {
			allKeys[k] = struct{}{}
		}
		for k := range a.openMarkets {
			allKeys[k] = struct{}{}
		}
		a.stat.UniqueMarkets = len(allKeys)
		_, a.stat.MeanPC, a.stat.SDPC = unitTrialMeanSD(values)
		for _, v := range values {
			a.stat.TotalPnL += v
		}
		if len(values) > 0 {
			a.stat.MeanAsk = entry / float64(len(values))
			a.stat.MeanFeePC = fee / float64(len(values))
		}
		a.stat.FirstOpened = a.first.Format(time.RFC3339Nano)
		a.stat.LastActivity = a.last.Format(time.RFC3339Nano)
		a.stat.TrackedSeconds = unitTrialElapsedSeconds(now, a.first)
		a.stat.TrackedDays = a.stat.TrackedSeconds / (24 * time.Hour).Seconds()
		a.stat.OpportunitiesPerDay = float64(a.stat.Total) / a.stat.TrackedDays
		a.stat.SettledPerDay = float64(a.stat.SettledMarkets) / a.stat.TrackedDays
		a.stat.NetPerCalendarDay = a.stat.TotalPnL / a.stat.TrackedDays
		out = append(out, a.stat)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].NetPerCalendarDay != out[j].NetPerCalendarDay {
			return out[i].NetPerCalendarDay > out[j].NetPerCalendarDay
		}
		return comboLeaderboardGroupKey(comboLeaderboardRow{family: out[i].Family, venue: out[i].Platform, relation: out[i].RelationClass, producer: out[i].ProducerFamily, route: out[i].Route, epoch: out[i].ExperimentEpoch, source: out[i].EconomicSource}) < comboLeaderboardGroupKey(comboLeaderboardRow{family: out[j].Family, venue: out[j].Platform, relation: out[j].RelationClass, producer: out[j].ProducerFamily, route: out[j].Route, epoch: out[j].ExperimentEpoch, source: out[j].EconomicSource})
	})
	return out, nil
}
