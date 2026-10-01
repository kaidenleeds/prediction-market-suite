package storage

import (
	"context"
	"database/sql"
	"fmt"
)

// ResearchSystemFunnelInput is a bounded, source-native input to one R138 system. These rows are
// diagnostic controls, not trades: the server must still attach executable books/fees and a
// positive lower bound before it may call anything a candidate.
type ResearchSystemFunnelInput struct {
	Observed, OpportunityID, CanonicalEventID string
	Venue, Ticker, Source                     string
	Inputs                                    map[string]any
}

func (s *Store) ResearchSystemFunnelInputs(ctx context.Context, systemID string, limit int) ([]ResearchSystemFunnelInput, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	var out []ResearchSystemFunnelInput
	switch systemID {
	case "attention-spillover-graph", "series-roll-anchor":
		rows, err := s.db.QueryContext(ctx, `SELECT observed_ts,ticker,event_type,prior_event_type,cohort,
COALESCE(pre_bid,0),COALESCE(post0_bid,0),COALESCE(pre_ask,0),COALESCE(post0_ask,0)
FROM research_lifecycle_events ORDER BY id DESC LIMIT ?`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var observed, ticker, eventType, prior string
			var cohort int
			var preBid, postBid, preAsk, postAsk float64
			if err := rows.Scan(&observed, &ticker, &eventType, &prior, &cohort, &preBid, &postBid, &preAsk, &postAsk); err != nil {
				return nil, err
			}
			source := "lifecycle transition control; parent-child graph is not joined"
			if systemID == "series-roll-anchor" {
				source = "lifecycle transition control; prior-issue fair anchor is not joined"
			}
			out = append(out, ResearchSystemFunnelInput{Observed: observed, OpportunityID: fmt.Sprintf("%s|%s|%d", ticker, eventType, cohort),
				CanonicalEventID: "venue:kalshi:" + ticker, Venue: "kalshi", Ticker: ticker, Source: source,
				Inputs: map[string]any{"event_type": eventType, "prior_event_type": prior, "cohort": cohort,
					"pre_bid": preBid, "post_bid": postBid, "pre_ask": preAsk, "post_ask": postAsk}})
		}
		return out, rows.Err()

	case "clientele-clock-basis":
		rows, err := s.db.QueryContext(ctx, `SELECT MAX(last_seen),game_id,GROUP_CONCAT(venue||':'||ticker),COUNT(DISTINCT venue)
FROM market_game WHERE src='struct' GROUP BY game_id HAVING COUNT(DISTINCT venue)>=2
ORDER BY MAX(last_seen) DESC LIMIT ?`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var observed, gameID, instruments string
			var venues int
			if err := rows.Scan(&observed, &gameID, &instruments, &venues); err != nil {
				return nil, err
			}
			out = append(out, ResearchSystemFunnelInput{Observed: observed, OpportunityID: gameID,
				CanonicalEventID: "sports:" + gameID, Venue: "multi", Source: "market_game structural cross-venue identity; last_seen is UTC and no audience cell is joined",
				Inputs: map[string]any{"instruments": instruments, "venue_count": venues,
					"observation_clock": "UTC catalog last_seen", "venue_local_hour_joined": false, "audience_cell_joined": false}})
		}
		return out, rows.Err()

	case "collateral-release-rotation", "settlement-latency-carry":
		rows, err := s.db.QueryContext(ctx, `SELECT id,closed_ts,platform,ticker,family,side,opened_ts,resolve_hours,
COALESCE(pnl_pc,0),ask,fee_pc FROM unit_trials
WHERE settled=1 AND closed_ts!='' ORDER BY id DESC LIMIT ?`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var trialID int64
			var closed, venue, ticker, family, side, opened string
			var resolveHours, pnl, ask, fee float64
			if err := rows.Scan(&trialID, &closed, &venue, &ticker, &family, &side, &opened,
				&resolveHours, &pnl, &ask, &fee); err != nil {
				return nil, err
			}
			out = append(out, ResearchSystemFunnelInput{Observed: closed,
				OpportunityID:    fmt.Sprintf("%s|unit-trial|%d", systemID, trialID),
				CanonicalEventID: "venue:" + venue + ":" + ticker, Venue: venue, Ticker: ticker, Source: "settled unit_trials grade close; authoritative collateral credit clock and amount are absent",
				Inputs: map[string]any{"family": family, "side": side, "opened_ts": opened, "closed_ts": closed,
					"source_unit_trial_id": trialID, "funnel_input_identity_version": 2,
					"closed_ts_semantics":                      "grade_or_settlement_close_not_collateral_credit",
					"authoritative_credit_timestamp_available": false, "authoritative_credit_amount_available": false,
					"resolve_hours_at_entry": resolveHours, "realized_pnl_pc": pnl, "entry_ask": ask, "entry_fee": fee}})
		}
		return out, rows.Err()

	case "deadline-hazard-surface":
		rows, err := s.db.QueryContext(ctx, `SELECT observed_ts,source_id,artifact_id,source_ts,valid_ts,
schema_version,clock_status,settlement_compatible,blocker
FROM research_official_release_frames ORDER BY id DESC LIMIT ?`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var observed, sourceID, artifactID, sourceTS, validTS, schemaVersion, clockStatus, blocker string
			var settlementCompatible int
			if err := rows.Scan(&observed, &sourceID, &artifactID, &sourceTS, &validTS, &schemaVersion,
				&clockStatus, &settlementCompatible, &blocker); err != nil {
				return nil, err
			}
			out = append(out, ResearchSystemFunnelInput{Observed: observed,
				OpportunityID: sourceID + "|" + artifactID, CanonicalEventID: "official-release:" + sourceID + ":" + artifactID,
				Venue: "none", Source: "official release frame; verified payoff relation and executable market-book join are absent",
				Inputs: map[string]any{"source_id": sourceID, "artifact_id": artifactID, "source_ts": sourceTS,
					"valid_ts": validTS, "schema_version": schemaVersion, "clock_status": clockStatus,
					"settlement_compatible": settlementCompatible == 1, "source_blocker": blocker,
					"verified_payoff_relation_joined": false, "executable_book_joined": false}})
		}
		return out, rows.Err()

	case "proper-score-executor", "forecast-persona-router":
		rows, err := s.db.QueryContext(ctx, `SELECT observed_ts,system_name,transform,platform,ticker,
forecast_signal,forecast_source,forecast_version,model_backend,calibration,book_source,quote_age_s,
selected_side,entry_price,entry_depth,exact_fee_total,fee_source,expected_net,abstain_reason,
settled,grade_status,realized_net,closed_ts
FROM research_proper_score_trials ORDER BY id DESC LIMIT ?`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var observed, sourceSystem, transform, venue, ticker, forecastSignal, forecastSource, forecastVersion string
			var modelBackend, calibration, bookSource, selectedSide, feeSource, abstainReason, gradeStatus, closed string
			var quoteAge, entryPrice, entryDepth, fee, expected float64
			var settled int
			var realized sql.NullFloat64
			if err := rows.Scan(&observed, &sourceSystem, &transform, &venue, &ticker, &forecastSignal,
				&forecastSource, &forecastVersion, &modelBackend, &calibration, &bookSource, &quoteAge,
				&selectedSide, &entryPrice, &entryDepth, &fee, &feeSource, &expected, &abstainReason,
				&settled, &gradeStatus, &realized, &closed); err != nil {
				return nil, err
			}
			inputs := map[string]any{"source_system": sourceSystem, "transform": transform,
				"forecast_signal": forecastSignal, "forecast_source": forecastSource,
				"forecast_version": forecastVersion, "model_backend": modelBackend, "calibration": calibration,
				"book_source": bookSource, "quote_age_s": quoteAge, "selected_side": selectedSide,
				"entry_price": entryPrice, "entry_depth": entryDepth, "exact_fee": fee,
				"fee_source": feeSource, "point_expected_net": expected, "abstain_reason": abstainReason,
				"settled": settled, "grade_status": gradeStatus, "closed_ts": closed}
			if realized.Valid {
				inputs["realized_net"] = realized.Float64
			}
			if systemID == "forecast-persona-router" {
				inputs["frozen_persona_policy_applied"] = false
			}
			out = append(out, ResearchSystemFunnelInput{Observed: observed,
				OpportunityID:    systemID + "|" + venue + "|" + ticker + "|" + sourceSystem + "|" + observed,
				CanonicalEventID: "venue:" + venue + ":" + ticker, Venue: venue, Ticker: ticker,
				Source: "book-native proper-score trial mirror; no additional trading authority", Inputs: inputs})
		}
		return out, rows.Err()

	case "semantic-complexity-premium":
		rows, err := s.db.QueryContext(ctx, `SELECT first_seen_ts,venue,source_id,version,title,summary,categories_json,
change_classes_json,severity,artifact_hash FROM research_venue_notices ORDER BY id DESC LIMIT ?`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var observed, venue, sourceID, title, summary, categories, changes, severity, hash string
			var version int
			if err := rows.Scan(&observed, &venue, &sourceID, &version, &title, &summary, &categories, &changes, &severity, &hash); err != nil {
				return nil, err
			}
			out = append(out, ResearchSystemFunnelInput{Observed: observed, OpportunityID: sourceID + "|" + fmt.Sprint(version),
				CanonicalEventID: "notice:" + venue + ":" + sourceID, Venue: venue, Source: "research_venue_notices",
				Inputs: map[string]any{"title": title, "summary": summary, "categories_json": categories,
					"change_classes_json": changes, "severity": severity, "artifact_hash": hash}})
		}
		return out, rows.Err()

	case "paired-bridge-inversion":
		rows, err := s.db.QueryContext(ctx, `SELECT opened_ts,platform,ticker,family,side,ask,fee_pc,depth,
fee_known,fee_source,quote_source FROM unit_trials
WHERE family LIKE '%bridge%' OR family LIKE 'xvgap%' OR family IN ('xmatch','pmatch')
ORDER BY id DESC LIMIT ?`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var observed, venue, ticker, family, side, feeSource, quoteSource string
			var ask, fee, depth float64
			var feeKnown int
			if err := rows.Scan(&observed, &venue, &ticker, &family, &side, &ask, &fee, &depth, &feeKnown, &feeSource, &quoteSource); err != nil {
				return nil, err
			}
			out = append(out, ResearchSystemFunnelInput{Observed: observed,
				OpportunityID:    venue + "|" + ticker + "|" + side + "|" + family,
				CanonicalEventID: "venue:" + venue + ":" + ticker, Venue: venue, Ticker: ticker, Source: "bridge unit_trials",
				Inputs: map[string]any{"family": family, "side": side, "ask": ask, "fee": fee, "depth": depth,
					"fee_known": feeKnown == 1, "fee_source": feeSource, "quote_source": quoteSource}})
		}
		return out, rows.Err()

	case "side-normalized-crowding-fade":
		rows, err := s.db.QueryContext(ctx, `SELECT ts,platform,ticker,side,signal_type,entry_price,
COALESCE(flow_ratio_15m,0),COALESCE(flow_n_15m,0),COALESCE(holders_hhi,0),COALESCE(holders_skill,0),
COALESCE(book_bid,0),COALESCE(book_ask,0),COALESCE(book_bid_depth,0),COALESCE(book_ask_depth,0),book_source
FROM signal_log WHERE book_feature_ver>=1 AND (flow_n_15m IS NOT NULL OR holders_hhi IS NOT NULL OR trader_count>0)
ORDER BY id DESC LIMIT ?`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var observed, venue, ticker, side, family, bookSource string
			var entry, flowRatio, flowN, hhi, skill, bid, ask, bidDepth, askDepth float64
			if err := rows.Scan(&observed, &venue, &ticker, &side, &family, &entry, &flowRatio, &flowN, &hhi, &skill,
				&bid, &ask, &bidDepth, &askDepth, &bookSource); err != nil {
				return nil, err
			}
			out = append(out, ResearchSystemFunnelInput{Observed: observed,
				OpportunityID:    venue + "|" + ticker + "|" + side + "|" + family,
				CanonicalEventID: "venue:" + venue + ":" + ticker, Venue: venue, Ticker: ticker, Source: "book-native signal_log crowding inputs",
				Inputs: map[string]any{"bought_side": side, "source_family": family, "entry": entry,
					"flow_ratio_15m": flowRatio, "flow_n_15m": flowN, "holders_hhi": hhi, "holders_skill": skill,
					"book_bid": bid, "book_ask": ask, "book_bid_depth": bidDepth, "book_ask_depth": askDepth, "book_source": bookSource}})
		}
		return out, rows.Err()

	case "identity-challenged-cross-venue-lock":
		rows, err := s.db.QueryContext(ctx, `SELECT opportunity_id,route_id,observed_ts,system_name,canonical_event_id,venue,ticker,side,
identity_status,quote_source,fee_authority,decision,decision_reason,evidence_json
FROM research_route_opportunities
WHERE system_name='identity-challenged-cross-venue-lock'
   OR system_name GLOB 'xvlock*' OR system_name GLOB 'xvgap*'
ORDER BY observed_ts DESC LIMIT ?`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var sourceOpportunityID, sourceRouteID, observed, sourceSystem, eventID, venue, ticker string
			var side, identity, quote, fee, decision, reason, evidence string
			if err := rows.Scan(&sourceOpportunityID, &sourceRouteID, &observed, &sourceSystem, &eventID,
				&venue, &ticker, &side, &identity, &quote, &fee, &decision, &reason, &evidence); err != nil {
				return nil, err
			}
			if eventID == "" {
				eventID = "venue:" + venue + ":" + ticker
			}
			out = append(out, ResearchSystemFunnelInput{Observed: observed,
				OpportunityID: systemID + "|route|" +
					ResearchRouteStableID(sourceOpportunityID, sourceRouteID),
				CanonicalEventID: eventID, Venue: venue, Ticker: ticker, Source: "unified cross-venue route ledger",
				Inputs: map[string]any{"source_system": sourceSystem, "side": side, "identity_status": identity,
					"quote_source": quote, "fee_authority": fee, "decision": decision, "decision_reason": reason,
					"route_evidence_json": evidence, "source_route_opportunity_id": sourceOpportunityID,
					"source_route_id": sourceRouteID, "funnel_input_identity_version": 2}})
		}
		return out, rows.Err()
	default:
		return nil, fmt.Errorf("unsupported R139 system funnel %q", systemID)
	}
}
