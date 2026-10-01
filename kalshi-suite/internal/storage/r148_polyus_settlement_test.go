package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestR148PolyUSFractionalSettlementIsQuarantinedAndRegradable(t *testing.T) {
	st := nativeLockTestStore(t)
	ctx := context.Background()
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := st.db.Exec(`INSERT INTO venue_settlements(
platform,ticker,yes_value,resolved_at,source_artifact) VALUES('polyus','FRACTIONAL',.53,?,'legacy BookFull settlementPx')`, stamp); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO signal_log(
ts,day,slot,platform,ticker,title,side,signal_type,entry_price,resolved,won,settle_val,resolved_at)
VALUES(?,?,?,?,?,?,?,?,?,1,1,.53,?)`, stamp, stamp[:10], "r148-fractional", "polyus", "FRACTIONAL",
		"fixture", "YES", "fixture", .5, stamp); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO maker_fill_stats(
ts,platform,ticker,side,source,post_px,filled,settle_val,outcome_won,settle_mirrored_at)
VALUES(?,'polyus','FRACTIONAL','YES','fixture',.5,1,.53,1,?)`, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO unit_trials(
opened_ts,closed_ts,family,platform,origin_layer,ticker,side,ask,fee_pc,fee_known,
fee_source,depth,quote_source,resolve_hours,settled,settle_val,pnl_pc,return_per_dollar,capital_day)
VALUES(?,?,'fixture','polyus','model','FRACTIONAL','YES',.5,.01,1,'fixture',1,'fixture',1,1,.53,.02,.04,.96)`, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	if err := migratePolyUSFinalSettlementAuthority(st.db); err != nil {
		t.Fatal(err)
	}
	var active, receiptQ, gradeQ int
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM venue_settlements WHERE platform='polyus' AND ticker='FRACTIONAL'`).Scan(&active)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM polyus_settlement_quarantine WHERE platform='polyus' AND ticker='FRACTIONAL'`).Scan(&receiptQ)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM polyus_settlement_grade_quarantine WHERE platform='polyus' AND ticker='FRACTIONAL'`).Scan(&gradeQ)
	if active != 0 || receiptQ != 1 || gradeQ != 3 {
		t.Fatalf("active=%d receipt quarantine=%d grade quarantine=%d", active, receiptQ, gradeQ)
	}
	var resolved int
	var won, resolvedAt sql.NullString
	var settle float64
	if err := st.db.QueryRow(`SELECT resolved,CAST(won AS TEXT),settle_val,resolved_at FROM signal_log
WHERE platform='polyus' AND ticker='FRACTIONAL'`).Scan(&resolved, &won, &settle, &resolvedAt); err != nil {
		t.Fatal(err)
	}
	if resolved != 0 || won.Valid || settle != -1 || resolvedAt.Valid {
		t.Fatalf("signal not reset for regrade: resolved=%d won=%+v settle=%g at=%+v", resolved, won, settle, resolvedAt)
	}
	var makerSettle, makerWon, makerAt sql.NullString
	if err := st.db.QueryRow(`SELECT CAST(settle_val AS TEXT),CAST(outcome_won AS TEXT),settle_mirrored_at
FROM maker_fill_stats WHERE platform='polyus' AND ticker='FRACTIONAL'`).Scan(&makerSettle, &makerWon, &makerAt); err != nil {
		t.Fatal(err)
	}
	if makerSettle.Valid || makerWon.Valid || !makerAt.Valid || makerAt.String != "" {
		t.Fatalf("maker grade not reset: settle=%+v won=%+v at=%+v", makerSettle, makerWon, makerAt)
	}
	var unitSettled int
	var unitClosed string
	var unitSettle, unitPNL sql.NullFloat64
	if err := st.db.QueryRow(`SELECT settled,closed_ts,settle_val,pnl_pc FROM unit_trials
WHERE platform='polyus' AND ticker='FRACTIONAL'`).Scan(&unitSettled, &unitClosed, &unitSettle, &unitPNL); err != nil {
		t.Fatal(err)
	}
	if unitSettled != 0 || unitClosed != "" || unitSettle.Valid || unitPNL.Valid {
		t.Fatalf("unit grade not reset: settled=%d closed=%q settle=%+v pnl=%+v", unitSettled, unitClosed, unitSettle, unitPNL)
	}
	if _, err := st.RecordVenueSettlement(ctx, "polyus", "FRACTIONAL", .61, time.Now(), "preliminary"); err == nil {
		t.Fatal("fractional PolyUS payout was accepted as final")
	}
	if inserted, err := st.RecordVenueSettlement(ctx, "polyus", "FRACTIONAL", 1, time.Now(), PolyUSFinalBookSettlementV2); err != nil || !inserted {
		t.Fatalf("binary final not accepted inserted=%v err=%v", inserted, err)
	}
	if _, err := st.db.Exec(`DELETE FROM polyus_settlement_quarantine`); err == nil {
		t.Fatal("quarantine evidence was deletable")
	}
}

func TestR148PolyUSLegacyExactBinaryReceiptsAndDependentGradesAreQuarantinedIdempotently(t *testing.T) {
	st := nativeLockTestStore(t)
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	for _, row := range []struct {
		ticker, source string
		value          float64
	}{
		{"LEGACY-ZERO", "polyus broad settlement poll", 0},
		{"LEGACY-ONE", "polyus BookFull settlementPx", 1},
		{"TRUSTED-ONE", PolyUSFinalEndpointSettlementV2, 1},
	} {
		if _, err := st.db.Exec(`INSERT INTO venue_settlements(
platform,ticker,yes_value,resolved_at,source_artifact) VALUES('polyus',?,?,?,?)`,
			row.ticker, row.value, stamp, row.source); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range []struct {
		ticker string
		value  float64
	}{
		{"LEGACY-ZERO", 0}, {"LEGACY-ONE", 1}, {"TRUSTED-ONE", 1},
	} {
		won := 0
		if row.value == 1 {
			won = 1
		}
		if _, err := st.db.Exec(`INSERT INTO signal_log(
ts,day,slot,platform,ticker,title,side,signal_type,entry_price,resolved,won,settle_val,resolved_at)
VALUES(?,?,?,?,?,?,?,?,?,1,?,?,?)`, stamp, stamp[:10], "r148-exact", "polyus", row.ticker,
			"fixture", "YES", "fixture", .5, won, row.value, stamp); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.db.Exec(`INSERT INTO maker_fill_stats(
ts,platform,ticker,side,source,post_px,filled,settle_val,outcome_won,settle_mirrored_at)
VALUES(?,'polyus','LEGACY-ZERO','YES','fixture',.5,1,0,0,?)`, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO unit_trials(
opened_ts,closed_ts,family,platform,origin_layer,ticker,side,ask,fee_pc,fee_known,
fee_source,depth,quote_source,resolve_hours,settled,settle_val,pnl_pc,return_per_dollar,capital_day)
VALUES(?,?,'fixture','polyus','model','LEGACY-ZERO','YES',.5,.01,1,'fixture',1,'fixture',1,1,0,-.51,-1.02,.96)`, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO paper_fills(
ts,platform,ticker,title,side,action,price,contracts,fee,source,note,fill_kind,route_reason)
VALUES(?,'polyus','LEGACY-ZERO','fixture','YES','BUY',.4,2,.02,'fixture','entry','taker','fixture'),
(?,'polyus','LEGACY-ZERO','fixture','YES','SELL',0,2,0,'settled-polyus','auto-settled at resolution','','')`,
		stamp, stamp); err != nil {
		t.Fatal(err)
	}
	parlayLegs := `[{"platform":"kalshi","ticker":"K-COMBO","side":"YES"},{"platform":"polyus","ticker":"LEGACY-ONE","side":"YES"}]`
	if _, err := st.db.Exec(`INSERT INTO paper_parlays(
ts,stake,price,contracts,status,legs,payout,realized,settled_ts,fees,route_source,cohort,system_ids)
VALUES(?,2,.2,10,'settled',?,1,7.9,?,.1,'fixture','fixture','["fixture"]')`,
		stamp, parlayLegs, stamp); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	direct := fundedRelationTestReceipt("ez", "polyus", "polyus", "LEGACY-ZERO", "YES", "event-zero", 1, "independent")
	directID, inserted, err := st.InsertFundedRelationReceipt(ctx, direct)
	if err != nil || !inserted {
		t.Fatalf("direct relation receipt=%q inserted=%v err=%v", directID, inserted, err)
	}
	combo := fundedRelationTestReceipt("eo", "combos", "kalshi", "K-COMBO", "YES", "event-combo", 1, "dependent")
	combo.Legs = append(combo.Legs, FundedRelationLeg{Venue: "polyus", Ticker: "LEGACY-ONE", Side: "YES",
		CanonicalEventID: "event-one", EventVersion: 1, RelationState: "dependent"})
	comboID, inserted, err := st.InsertFundedRelationReceipt(ctx, combo)
	if err != nil || !inserted {
		t.Fatalf("combo relation receipt=%q inserted=%v err=%v", comboID, inserted, err)
	}
	for _, id := range []string{directID, comboID} {
		if inserted, err := st.RecordFundedRelationOutcome(ctx, id, time.Now(), -2, "legacy PolyUS settlement"); err != nil || !inserted {
			t.Fatalf("relation outcome %s inserted=%v err=%v", id, inserted, err)
		}
	}

	if err := migratePolyUSFinalSettlementAuthority(st.db); err != nil {
		t.Fatal(err)
	}
	var activeLegacy, activeTrusted, receiptQ, gradeQ int
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM venue_settlements
WHERE platform='polyus' AND ticker LIKE 'LEGACY-%'`).Scan(&activeLegacy)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM venue_settlements
WHERE platform='polyus' AND ticker='TRUSTED-ONE'`).Scan(&activeTrusted)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM polyus_settlement_quarantine
WHERE platform='polyus' AND ticker LIKE 'LEGACY-%'`).Scan(&receiptQ)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM polyus_settlement_grade_quarantine
WHERE platform='polyus' AND ticker LIKE 'LEGACY-%'`).Scan(&gradeQ)
	if activeLegacy != 0 || activeTrusted != 1 || receiptQ != 2 || gradeQ != 4 {
		t.Fatalf("active legacy=%d trusted=%d receipt quarantine=%d grade quarantine=%d",
			activeLegacy, activeTrusted, receiptQ, gradeQ)
	}
	var trustedResolved, legacyResolved int
	_ = st.db.QueryRow(`SELECT resolved FROM signal_log
WHERE platform='polyus' AND ticker='TRUSTED-ONE'`).Scan(&trustedResolved)
	_ = st.db.QueryRow(`SELECT SUM(resolved) FROM signal_log
WHERE platform='polyus' AND ticker LIKE 'LEGACY-%'`).Scan(&legacyResolved)
	if trustedResolved != 1 || legacyResolved != 0 {
		t.Fatalf("trusted resolved=%d legacy resolved sum=%d", trustedResolved, legacyResolved)
	}
	var paperBuy, paperSell, paperQ, parlayQ, relationActive, relationQ int
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM paper_fills WHERE platform='polyus' AND ticker='LEGACY-ZERO' AND UPPER(action)='BUY'`).Scan(&paperBuy)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM paper_fills WHERE platform='polyus' AND ticker='LEGACY-ZERO' AND UPPER(action)='SELL'`).Scan(&paperSell)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM polyus_paper_fill_settlement_quarantine`).Scan(&paperQ)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM polyus_parlay_settlement_quarantine`).Scan(&parlayQ)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM funded_relation_outcomes WHERE receipt_id IN (?,?)`, directID, comboID).Scan(&relationActive)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM polyus_ml_relation_outcome_quarantine WHERE receipt_id IN (?,?)`, directID, comboID).Scan(&relationQ)
	var parlayStatus string
	_ = st.db.QueryRow(`SELECT status FROM paper_parlays WHERE legs=?`, parlayLegs).Scan(&parlayStatus)
	if paperBuy != 1 || paperSell != 0 || paperQ != 1 || parlayQ != 1 || parlayStatus != "open" ||
		relationActive != 0 || relationQ != 2 {
		t.Fatalf("funded repair buy=%d sell=%d fillQ=%d parlayQ=%d status=%q relation active=%d q=%d",
			paperBuy, paperSell, paperQ, parlayQ, parlayStatus, relationActive, relationQ)
	}

	// A restart replay must neither add evidence nor reset the trusted replacement.
	if inserted, err := st.RecordFundedRelationOutcome(ctx, directID, time.Now(), 3, "corrected final"); err != nil || !inserted {
		t.Fatalf("corrected relation outcome inserted=%v err=%v", inserted, err)
	}
	if err := migratePolyUSFinalSettlementAuthority(st.db); err != nil {
		t.Fatal(err)
	}
	var receiptQ2, gradeQ2 int
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM polyus_settlement_quarantine
WHERE platform='polyus' AND ticker LIKE 'LEGACY-%'`).Scan(&receiptQ2)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM polyus_settlement_grade_quarantine
WHERE platform='polyus' AND ticker LIKE 'LEGACY-%'`).Scan(&gradeQ2)
	if receiptQ2 != receiptQ || gradeQ2 != gradeQ {
		t.Fatalf("migration replay duplicated quarantine: receipts %d->%d grades %d->%d",
			receiptQ, receiptQ2, gradeQ, gradeQ2)
	}
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM funded_relation_outcomes WHERE receipt_id=?`, directID).Scan(&relationActive)
	if relationActive != 1 {
		t.Fatalf("migration replay deleted corrected relation outcome: active=%d", relationActive)
	}
	if _, err := st.RecordVenueSettlement(context.Background(), "polyus", "UNTRUSTED-EXACT", 1,
		time.Now(), "legacy exact fixture"); err == nil {
		t.Fatal("exact 0/1 PolyUS receipt without a trusted v2 source was accepted")
	}
}

func TestR148PolyUSDerivedResearchGradesAreQuarantinedAndReopened(t *testing.T) {
	st := nativeLockTestStore(t)
	ctx := context.Background()
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	const ticker = "R148-RESEARCH-BAD"
	if _, err := st.db.Exec(`INSERT INTO venue_settlements(
platform,ticker,yes_value,resolved_at,source_artifact)
VALUES('polyus',?,1,?,'legacy exact settlementPx')`, ticker, stamp); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO research_proper_score_trials(
observed_ts,slot,system_name,transform,strategy_mode,cohort,platform,ticker,
forecast_yes,forecast_origin_side,forecast_source,forecast_version,route,
yes_bid,yes_ask,yes_bid_depth,yes_ask_depth,no_bid,no_ask,no_bid_depth,no_ask_depth,
book_source,quote_age_s,q_yes,settled,grade_status,settle_yes,forecast_score,book_score,
score_delta,realized_net,closed_ts)
VALUES(?,?,'proper-score-brier','brier','fundamental','fixture','polyus',?,
.6,'YES','fixture','v1','taker',.49,.51,10,10,.49,.51,10,10,
'fixture',0,.6,1,'graded',1,.84,.74,.10,.49,?)`, stamp, "r148-research", ticker, stamp); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO research_event_specs(
event_id,version,spec_hash,created_ts) VALUES('r148-research-event',1,?,?)`,
		strings.Repeat("a", 64), stamp); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO research_payoff_specs(
payoff_id,version,spec_hash,event_id,event_version,created_ts)
VALUES('r148-research-payoff',1,?,'r148-research-event',1,?)`,
		strings.Repeat("b", 64), stamp); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO research_instrument_specs(
venue,ticker,version,spec_hash,event_id,event_version,payoff_id,payoff_version,created_ts)
VALUES('polyus',?,1,?,'r148-research-event',1,'r148-research-payoff',1,?)`,
		ticker, strings.Repeat("c", 64), stamp); err != nil {
		t.Fatal(err)
	}
	res, err := st.db.Exec(`INSERT INTO research_system_observations(
observed_ts,decision_ts,observed_slot,system_id,opportunity_id,observation_kind,cohort,venue,ticker,
canonical_event_id,event_version,canonical_payoff_id,payoff_version,instrument_version,
route,side,certificate_status,source_clock_id,book_source,fee_source,quote_age_max_s,
tick_min,size_units,executable_cost,exact_fee,payout_lower,payout_upper,net_lower,net_upper,
visible_capacity,latency_known,quote_age_known,tick_known,depth_known,fee_known,inputs_json)
VALUES(?,?,?,'fixture-system','fixture-opportunity','negative','fixture','polyus',?,
'r148-research-event',1,'r148-research-payoff',1,1,
'taker','YES','verified','clock','book','fee',0,.01,1,.5,.01,0,1,-.51,.49,
1,1,1,1,1,1,'{}')`, stamp, stamp, "r148-research", ticker)
	if err != nil {
		t.Fatal(err)
	}
	observationID, _ := res.LastInsertId()
	if _, err := st.db.Exec(`INSERT INTO research_system_payoff_updates(
observation_id,observed_ts,status,payout_lower,payout_upper,realized_net,
source_artifact,source_hash,reason)
VALUES(?,?,'settled',1,1,.49,'legacy exact settlement','legacy-hash','fixture')`,
		observationID, stamp); err != nil {
		t.Fatal(err)
	}
	if inserted, err := st.InsertResearchRouteOpportunity(ctx, ResearchRouteOpportunity{
		OpportunityID: "r148-research-route", RouteID: "taker", Observed: time.Now(),
		SystemName: "fixture-system", IdentityStatus: "verified", Venue: "polyus", Ticker: ticker,
		Side: "YES", Route: "taker", Action: "buy", QuoteSource: "book", QuoteAgeKnown: true,
		LatencyKnown: true, TickSize: .01, TickKnown: true, ExecutablePrice: .5,
		ExecutableDepth: 1, DepthKnown: true, RequestedQty: 1, FeeAmount: .01,
		FeeAuthority: "fee", FeeKnown: true, ExpectedPayoutLow: 0, ExpectedPayoutHigh: 1,
		ExpectedNetLow: -.51, ExpectedNetHigh: .49, Decision: "candidate",
		DecisionReason: "fixture", EvidenceJSON: `{"ticker":"R148-RESEARCH-BAD"}`,
	}); err != nil || !inserted {
		t.Fatalf("route inserted=%v err=%v", inserted, err)
	}
	payout, realized := 1.0, .49
	if _, err := st.AppendResearchRouteEvent(ctx, ResearchRouteEvent{
		OpportunityID: "r148-research-route", RouteID: "taker", EventType: "grade",
		Payout: &payout, RealizedNet: &realized, OutcomeStatus: "settled",
		Reason: "legacy exact settlement", EvidenceJSON: `{"settle_yes":1}`,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO research_route_bundles(
bundle_id,observed_ts,system_id,cohort,experiment_version,opportunity_id,
canonical_event_id,event_version,certificate_hash,certificate_status,route_kind,atomic_route,
leg_count,state_count,size_units,total_cost,total_fee,payout_floor,net_floor,partial_fill_worst,
unwind_worst,unwind_known,decision_latency_ms,latency_known,state_vector_hash,blocker,evidence_json)
VALUES('r148-bundle',?,'fixture-system','fixture',1,'fixture-bundle-opportunity',
'fixture-event',1,'fixture-certificate','verified','cross_venue_non_atomic',0,
2,2,1,.8,.02,0,-.82,-.82,-.82,1,1,1,'fixture-state','fixture','{}')`, stamp); err != nil {
		t.Fatal(err)
	}
	for i, leg := range []struct{ venue, ticker string }{{"kalshi", "R148-K"}, {"polyus", ticker}} {
		if _, err := st.db.Exec(`INSERT INTO research_route_bundle_legs(
bundle_id,leg_index,leg_id,venue,ticker,side,payoff_id,quantity,integrated_cost,exact_fee,
visible_depth,tick_size,quote_age_s,book_source,source_clock_id,fee_source,levels_json,
payoff_json,unwind_known,unwind_book_source,unwind_fee_source,unwind_levels_json)
VALUES('r148-bundle',?,?,?,?,'YES','fixture-payoff',1,.4,.01,1,.01,0,
'book','clock','fee','[]','{}',1,'book','fee','[]')`, i, "leg-"+leg.venue, leg.venue, leg.ticker); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.db.Exec(`INSERT INTO research_route_bundle_events(
bundle_id,observed_ts,event_type,outcome_status,payout,realized_net,reason,evidence_json)
VALUES('r148-bundle',?,'grade','settled',1,.18,'legacy exact settlement','{}')`, stamp); err != nil {
		t.Fatal(err)
	}

	if err := migratePolyUSFinalSettlementAuthority(st.db); err != nil {
		t.Fatal(err)
	}
	var properSettled, payoffRows, routeGrades, bundleGrades, quarantineRows, openEconomic int
	var properSettle, properNet sql.NullFloat64
	if err := st.db.QueryRow(`SELECT settled,settle_yes,realized_net FROM research_proper_score_trials
WHERE platform='polyus' AND ticker=?`, ticker).Scan(&properSettled, &properSettle, &properNet); err != nil {
		t.Fatal(err)
	}
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM research_system_payoff_updates WHERE observation_id=?`, observationID).Scan(&payoffRows)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM research_route_events WHERE opportunity_id='r148-research-route' AND event_type='grade'`).Scan(&routeGrades)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM research_route_bundle_events WHERE bundle_id='r148-bundle' AND event_type='grade'`).Scan(&bundleGrades)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM polyus_research_grade_quarantine`).Scan(&quarantineRows)
	_ = st.db.QueryRow(`SELECT open_economic FROM research_system_collection_totals WHERE system_id='fixture-system'`).Scan(&openEconomic)
	if properSettled != 0 || properSettle.Valid || properNet.Valid || payoffRows != 0 || routeGrades != 0 ||
		bundleGrades != 0 || quarantineRows != 4 || openEconomic != 1 {
		t.Fatalf("proper=%d settle=%+v net=%+v payoff=%d route=%d bundle=%d quarantine=%d open=%d",
			properSettled, properSettle, properNet, payoffRows, routeGrades, bundleGrades, quarantineRows, openEconomic)
	}
	if _, err := st.db.Exec(`DELETE FROM polyus_research_grade_quarantine`); err == nil {
		t.Fatal("research-grade quarantine evidence was deletable")
	}
	if err := migratePolyUSFinalSettlementAuthority(st.db); err != nil {
		t.Fatal(err)
	}
	var quarantineRows2 int
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM polyus_research_grade_quarantine`).Scan(&quarantineRows2)
	if quarantineRows2 != quarantineRows {
		t.Fatalf("research quarantine replay duplicated rows: %d -> %d", quarantineRows, quarantineRows2)
	}
}

func TestR148PolyUSMLFractionalSettlementReopensLotAndReversesCounters(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	receipt := fundedRelationTestReceipt("r148", "ml-polyus", "polyus", "FRACTIONAL-ML", "YES", "event-r148", 1, "independent")
	receipt.DecisionFingerprint = strings.Repeat("a", 64)
	receipt.PositionFingerprint = strings.Repeat("b", 64)
	receiptID, inserted, err := st.InsertFundedRelationReceipt(ctx, receipt)
	if err != nil || !inserted {
		t.Fatalf("receipt id=%s inserted=%v err=%v", receiptID, inserted, err)
	}
	if inserted, err := st.RecordFundedRelationOutcome(ctx, receiptID, time.Now(), -2, "false fractional settlement"); err != nil || !inserted {
		t.Fatalf("outcome inserted=%v err=%v", inserted, err)
	}
	reset := time.Now().Add(-time.Hour)
	closedAt := float64(time.Now().Unix())
	book := map[string]any{
		"bank0": 600.0, "reset_at": reset.Format(time.RFC3339Nano),
		"open": []any{}, "closed": []any{map[string]any{
			"ticker": "FRACTIONAL-ML", "side": "YES", "platform": "polyus", "opened": float64(time.Now().Add(-2 * time.Hour).Unix()),
			"price": .58, "contracts": 10.0, "fee": .2, "won": 0.0, "pnl": -2.0, "closed_ts": closedAt,
			"settlement_payout": .53, "terminal_reason": "settlement", "relation_receipt_id": receiptID,
			"relation_outcome_synced": true,
		}},
		"lifetime": map[string]any{
			"net": -2.0, "net_base": 0.0, "closed": 1.0, "closed_base": 0.0, "wins": 0.0, "wins_base": 0.0,
			"venue_net": map[string]any{"kalshi": 0.0, "polyus": -2.0}, "venue_net_base": map[string]any{"kalshi": 0.0, "polyus": 0.0},
			"venue_closed": map[string]any{"kalshi": 0.0, "polyus": 1.0}, "venue_closed_base": map[string]any{"kalshi": 0.0, "polyus": 0.0},
			"venue_contracts": map[string]any{"kalshi": 0.0, "polyus": 10.0}, "venue_contracts_base": map[string]any{"kalshi": 0.0, "polyus": 0.0},
		},
		"stats": map[string]any{"net": -2.0, "closed": 1.0, "wins": 0.0, "bank0": 600.0, "equity": 598.0},
		"venue_sleeves": map[string]any{
			"kalshi": map[string]any{"grant": 300.0}, "polyus": map[string]any{"grant": 300.0},
		},
	}
	raw, _ := json.Marshal(book)
	if err := os.WriteFile(filepath.Join(dir, "ml_paper.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := repairPolyUSMLFractionalSettlements(st.db, dir); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(filepath.Join(dir, "ml_paper.json"))
	if err != nil || json.Unmarshal(raw, &book) != nil {
		t.Fatalf("read repaired ML book err=%v", err)
	}
	if got := len(book["closed"].([]any)); got != 0 {
		t.Fatalf("false close remains: %d", got)
	}
	opened := book["open"].([]any)
	if len(opened) != 1 {
		t.Fatalf("reopened=%d", len(opened))
	}
	stats := book["stats"].(map[string]any)
	if jsonFloat(stats, "net") != 0 || jsonFloat(stats, "closed") != 0 || jsonFloat(stats, "open_n") != 1 {
		t.Fatalf("stats not reversed: %+v", stats)
	}
	var activeOutcome, quarantinedOutcome int
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM funded_relation_outcomes WHERE receipt_id=?`, receiptID).Scan(&activeOutcome)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM polyus_ml_relation_outcome_quarantine WHERE receipt_id=?`, receiptID).Scan(&quarantinedOutcome)
	if activeOutcome != 0 || quarantinedOutcome != 1 {
		t.Fatalf("active outcome=%d quarantined=%d", activeOutcome, quarantinedOutcome)
	}
	if inserted, err := st.RecordFundedRelationOutcome(ctx, receiptID, time.Now(), 3, "corrected final"); err != nil || !inserted {
		t.Fatalf("corrected outcome inserted=%v err=%v", inserted, err)
	}
	// A later restart replays the durable quarantine file. It must not mistake the corrected
	// authoritative outcome for the old fractional one merely because both share a receipt id.
	if err := repairPolyUSMLFractionalSettlements(st.db, dir); err != nil {
		t.Fatal(err)
	}
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM funded_relation_outcomes WHERE receipt_id=?`, receiptID).Scan(&activeOutcome)
	if activeOutcome != 1 {
		t.Fatalf("restart replay deleted corrected final outcome: active=%d", activeOutcome)
	}
	if qRaw, err := os.ReadFile(filepath.Join(dir, "ml_paper.r148-settlement-quarantine.json")); err != nil || !strings.Contains(string(qRaw), "0.53") {
		t.Fatalf("quarantine evidence missing err=%v raw=%s", err, qRaw)
	}
}

func TestR148PolyUSMLLegacyExactClosureReopensButLaterTrustedClosureSurvives(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	ticker := "LEGACY-EXACT-ML"
	legacyAt := time.Now().UTC().Add(-time.Hour)
	if _, err := st.db.Exec(`INSERT INTO polyus_settlement_quarantine(
platform,ticker,yes_value,resolved_at,source_artifact,reason,quarantined_at)
VALUES('polyus',?,1,?,'legacy BookFull settlementPx','fixture',?)`, ticker,
		legacyAt.Format(time.RFC3339Nano), legacyAt.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	lot := map[string]any{"ticker": ticker, "side": "YES", "platform": "polyus",
		"opened": float64(legacyAt.Add(-time.Hour).Unix()), "price": .40, "contracts": 2.0,
		"fee": .02, "won": 1.0, "pnl": 1.18, "closed_ts": float64(legacyAt.Unix()),
		"settlement_payout": 1.0, "terminal_reason": "settlement"}
	book := map[string]any{
		"bank0": 600.0, "open": []any{}, "closed": []any{lot},
		"lifetime": map[string]any{
			"net": 1.18, "net_base": 0.0, "closed": 1.0, "closed_base": 0.0, "wins": 1.0, "wins_base": 0.0,
			"venue_net": map[string]any{"kalshi": 0.0, "polyus": 1.18}, "venue_net_base": map[string]any{"kalshi": 0.0, "polyus": 0.0},
			"venue_closed": map[string]any{"kalshi": 0.0, "polyus": 1.0}, "venue_closed_base": map[string]any{"kalshi": 0.0, "polyus": 0.0},
			"venue_contracts": map[string]any{"kalshi": 0.0, "polyus": 2.0}, "venue_contracts_base": map[string]any{"kalshi": 0.0, "polyus": 0.0},
		},
		"stats": map[string]any{"net": 1.18, "closed": 1.0, "wins": 1.0, "bank0": 600.0, "equity": 601.18},
		"venue_sleeves": map[string]any{
			"kalshi": map[string]any{"grant": 300.0}, "polyus": map[string]any{"grant": 300.0},
		},
	}
	if err := atomicWriteJSON(filepath.Join(dir, "ml_paper.json"), book); err != nil {
		t.Fatal(err)
	}
	if err := repairPolyUSMLFractionalSettlements(st.db, dir); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "ml_paper.json"))
	if err := json.Unmarshal(raw, &book); err != nil {
		t.Fatal(err)
	}
	if len(book["closed"].([]any)) != 0 || len(book["open"].([]any)) != 1 {
		t.Fatalf("legacy exact closure was not reopened: %s", raw)
	}

	trustedAt := time.Now().UTC()
	if inserted, err := st.RecordVenueSettlement(ctx, "polyus", ticker, 1, trustedAt,
		PolyUSFinalEndpointSettlementV2); err != nil || !inserted {
		t.Fatalf("trusted replacement inserted=%v err=%v", inserted, err)
	}
	corrected := cloneJSONMap(lot)
	corrected["closed_ts"] = float64(trustedAt.Add(time.Second).Unix())
	book["open"], book["closed"] = []any{}, []any{corrected}
	book["lifetime"] = map[string]any{
		"net": 1.18, "net_base": 0.0, "closed": 1.0, "closed_base": 0.0, "wins": 1.0, "wins_base": 0.0,
		"venue_net": map[string]any{"kalshi": 0.0, "polyus": 1.18}, "venue_net_base": map[string]any{"kalshi": 0.0, "polyus": 0.0},
		"venue_closed": map[string]any{"kalshi": 0.0, "polyus": 1.0}, "venue_closed_base": map[string]any{"kalshi": 0.0, "polyus": 0.0},
		"venue_contracts": map[string]any{"kalshi": 0.0, "polyus": 2.0}, "venue_contracts_base": map[string]any{"kalshi": 0.0, "polyus": 0.0},
	}
	if err := atomicWriteJSON(filepath.Join(dir, "ml_paper.json"), book); err != nil {
		t.Fatal(err)
	}
	if err := repairPolyUSMLFractionalSettlements(st.db, dir); err != nil {
		t.Fatal(err)
	}
	raw, _ = os.ReadFile(filepath.Join(dir, "ml_paper.json"))
	if err := json.Unmarshal(raw, &book); err != nil {
		t.Fatal(err)
	}
	if len(book["closed"].([]any)) != 1 || len(book["open"].([]any)) != 0 {
		t.Fatalf("later trusted closure was incorrectly reopened: %s", raw)
	}
}

func TestR148PolyUSMLTrimmedHistoryStartsCleanEpochAndKeepsCollecting(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ticker := "R148-ML-TRIMMED"
	stamp := time.Now().UTC().Add(-time.Hour)
	if _, err := st.db.Exec(`INSERT INTO polyus_settlement_quarantine(
platform,ticker,yes_value,resolved_at,source_artifact,reason,quarantined_at)
VALUES('polyus',?,1,?,'legacy BookFull settlementPx','fixture',?)`, ticker,
		stamp.Format(time.RFC3339Nano), stamp.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	book := map[string]any{
		"bank0": 600.0, "open": []any{}, "closed": []any{map[string]any{
			"ticker": ticker, "side": "YES", "platform": "polyus", "opened": float64(stamp.Add(-time.Hour).Unix()),
			"price": .4, "contracts": 2.0, "fee": .02, "won": 1.0, "pnl": 1.18,
			"closed_ts": float64(stamp.Unix()), "settlement_payout": 1.0, "terminal_reason": "settlement",
		}},
		"lifetime": map[string]any{
			"net": 8.0, "net_base": 2.0, "closed": 7.0, "closed_base": 2.0, "wins": 5.0, "wins_base": 1.0,
			"venue_net": map[string]any{"kalshi": 2.0, "polyus": 6.0}, "venue_net_base": map[string]any{"kalshi": 1.0, "polyus": 1.0},
			"venue_closed": map[string]any{"kalshi": 2.0, "polyus": 5.0}, "venue_closed_base": map[string]any{"kalshi": 1.0, "polyus": 1.0},
			"venue_contracts": map[string]any{"kalshi": 4.0, "polyus": 10.0}, "venue_contracts_base": map[string]any{"kalshi": 2.0, "polyus": 2.0},
		},
		"stats": map[string]any{"net": 6.0, "closed": 5.0, "wins": 4.0, "bank0": 600.0},
		"venue_sleeves": map[string]any{
			"kalshi": map[string]any{"grant": 300.0}, "polyus": map[string]any{"grant": 300.0},
		},
	}
	path := filepath.Join(dir, "ml_paper.json")
	if err := atomicWriteJSON(path, book); err != nil {
		t.Fatal(err)
	}
	if err := repairPolyUSMLFractionalSettlements(st.db, dir); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(raw, &book) != nil {
		t.Fatalf("read clean ML epoch err=%v", err)
	}
	life := book["lifetime"].(map[string]any)
	_, hasBlocked := book["settlement_repair_blocked"]
	if len(book["open"].([]any)) != 1 || len(book["closed"].([]any)) != 0 ||
		jsonFloat(life, "net") != 0 || jsonFloat(life, "closed") != 0 ||
		fmt.Sprint(book["settlement_epoch"]) != "polyus-final-v2-clean-epoch-1" ||
		book["settlement_legacy_excluded"] != true || hasBlocked {
		t.Fatalf("ML clean epoch did not exclude legacy authority: %s", raw)
	}
	hash := fmt.Sprint(book["settlement_legacy_archive_sha256"])
	var archives int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM polyus_funded_json_epoch_quarantine
WHERE ledger_name='ml_paper.json' AND source_sha256=?`, hash).Scan(&archives); err != nil || archives != 1 {
		t.Fatalf("ML immutable epoch archive count=%d err=%v", archives, err)
	}
	archivePath := path + ".r148-pre-clean-epoch-" + hash[:12] + ".json"
	if archived, err := os.ReadFile(archivePath); err != nil || !strings.Contains(string(archived), `"closed":7`) {
		t.Fatalf("ML readable archive missing original aggregate err=%v raw=%s", err, archived)
	}
	// A healthy clean epoch eventually rotates its own 500-row tail. Its version stamp prevents
	// that ordinary bounded history from being interpreted as legacy contamination again.
	book["closed"] = []any{map[string]any{
		"ticker": "R148-ML-NEW-FINAL", "side": "YES", "platform": "polyus",
		"opened": float64(time.Now().Add(-time.Hour).Unix()), "price": .4, "contracts": 1.0,
		"fee": .01, "won": 1.0, "pnl": .59, "closed_ts": float64(time.Now().Unix()),
		"settlement_payout": 1.0, "terminal_reason": "settlement",
	}}
	life["net"], life["closed"], life["wins"] = 12.5, 501.0, 301.0
	jsonMap(life, "venue_net")["polyus"] = 12.5
	jsonMap(life, "venue_closed")["polyus"] = 501.0
	if err := atomicWriteJSON(path, book); err != nil {
		t.Fatal(err)
	}
	if err := repairPolyUSMLFractionalSettlements(st.db, dir); err != nil {
		t.Fatal(err)
	}
	raw, _ = os.ReadFile(path)
	if err := json.Unmarshal(raw, &book); err != nil {
		t.Fatal(err)
	}
	life = book["lifetime"].(map[string]any)
	if len(book["closed"].([]any)) != 1 || jsonFloat(life, "net") != 12.5 || jsonFloat(life, "closed") != 501 {
		t.Fatalf("post-clean ML tail rotation triggered a second reset: %s", raw)
	}
}

func TestR148PolyUSReceiptReadersRejectFractionalDefenseInDepth(t *testing.T) {
	st := nativeLockTestStore(t)
	ctx := context.Background()
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := st.db.Exec(`INSERT INTO venue_settlements(
platform,ticker,yes_value,resolved_at,source_artifact) VALUES('polyus','DIRECT-BYPASS',.61,?,'direct fixture')`, stamp); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.ResolvedYesForVenue(ctx, "polyus", "DIRECT-BYPASS"); ok {
		t.Fatal("ResolvedYesForVenue trusted fractional PolyUS bypass")
	}
	if _, ok, err := st.VenueSettlementForTicker(ctx, "polyus", "DIRECT-BYPASS"); err != nil || ok {
		t.Fatalf("VenueSettlementForTicker trusted fractional bypass ok=%v err=%v", ok, err)
	}
	if _, err := st.db.Exec(`INSERT INTO venue_settlements(
platform,ticker,yes_value,resolved_at,source_artifact) VALUES('polyus','DIRECT-EXACT',1,?,'legacy exact bypass')`, stamp); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.ResolvedYesForVenue(ctx, "polyus", "DIRECT-EXACT"); ok {
		t.Fatal("ResolvedYesForVenue trusted unversioned exact PolyUS bypass")
	}
	if _, ok, err := st.VenueSettlementForTicker(ctx, "polyus", "DIRECT-EXACT"); err != nil || ok {
		t.Fatalf("VenueSettlementForTicker trusted unversioned exact bypass ok=%v err=%v", ok, err)
	}
}

func TestR148MLSettlementQuarantineKeyDoesNotMergeSimultaneousLots(t *testing.T) {
	base := map[string]any{"platform": "polyus", "ticker": "SAME", "side": "YES",
		"opened": 100.0, "closed_ts": 200.0, "contracts": 1.0, "price": .4, "settlement_payout": .53}
	other := cloneJSONMap(base)
	other["side"] = "NO"
	if mlSettlementQuarantineKey(base) == mlSettlementQuarantineKey(other) {
		t.Fatal("simultaneous YES/NO lots collapsed to one quarantine identity")
	}
	base["relation_receipt_id"] = strings.Repeat("a", 64)
	other = cloneJSONMap(base)
	other["ticker"], other["closed_ts"] = "RENAMED", 999.0
	if mlSettlementQuarantineKey(base) != mlSettlementQuarantineKey(other) {
		t.Fatal("the same durable relation receipt produced multiple quarantine identities")
	}
}
