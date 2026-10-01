package storage

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

func TestFundedSystemPerformanceSeparatesActualPaperMoneyAndExactClocks(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Date(2026, 7, 16, 18, 0, 0, 0, time.UTC)
	makeReceipt := func(seed, system, ticker string, decision time.Time, quantity float64) FundedRelationReceipt {
		r := fundedRelationTestReceipt(seed, "kalshi", "kalshi", ticker, "YES", "event-"+ticker, 1, "independent")
		r.Decision, r.SystemID, r.Quantity = decision, system, quantity
		return r
	}
	one := makeReceipt("1", "alpha", "A", now.Add(-48*time.Hour), 2)
	two := makeReceipt("2", "alpha", "A", now.Add(-36*time.Hour), 3)
	// Repeated funded entries into the same frozen position remain two settled decisions (n=2),
	// while unique_positions and unique_contracts both stay one.
	two.PositionFingerprint = one.PositionFingerprint
	open := makeReceipt("3", "alpha", "A", now.Add(-12*time.Hour), 4)
	open.PositionFingerprint = one.PositionFingerprint
	negative := makeReceipt("4", "beta", "B", now.Add(-24*time.Hour), 1)
	ids := make([]string, 0, 4)
	for _, receipt := range []FundedRelationReceipt{one, two, open, negative} {
		id, inserted, insertErr := st.InsertFundedRelationReceipt(ctx, receipt)
		if insertErr != nil || !inserted {
			t.Fatalf("insert %s: id=%q inserted=%v err=%v", receipt.SystemID, id, inserted, insertErr)
		}
		ids = append(ids, id)
	}
	for i, outcome := range []struct {
		when time.Time
		pnl  float64
	}{{now.Add(-24 * time.Hour), 2}, {now.Add(-6 * time.Hour), 1}, {now.Add(-time.Hour), -.5}} {
		id := ids[i]
		if i == 2 { // ids[2] is deliberately open; beta is ids[3].
			id = ids[3]
		}
		if inserted, insertErr := st.RecordFundedRelationOutcome(ctx, id, outcome.when, outcome.pnl, "fixture"); insertErr != nil || !inserted {
			t.Fatalf("outcome %d: inserted=%v err=%v", i, inserted, insertErr)
		}
	}
	rows, err := st.FundedSystemPerformanceRows(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	bySystem := map[string]FundedSystemPerformance{}
	for _, row := range rows {
		bySystem[row.SystemID] = row
	}
	alpha := bySystem["alpha"]
	if alpha.EconomicsScope != "funded-paper-simulation" || alpha.EvidenceTier != "funded_paper_simulation" ||
		alpha.FillConditioned || alpha.ProfitEvidence || alpha.LiveAuthorizes || alpha.AcceptedReceipts != 3 || alpha.N != 2 ||
		alpha.UniquePositions != 1 || alpha.UniqueContracts != 1 || alpha.OpenReceipts != 1 ||
		alpha.SettledContracts != 5 || math.Abs(alpha.TotalRealizedProfitDollars-3) > 1e-12 ||
		math.Abs(alpha.ElapsedSeconds-48*time.Hour.Seconds()) > 1e-9 ||
		math.Abs(alpha.ElapsedDays-2) > 1e-12 || math.Abs(alpha.NetPerCalendarDay-1.5) > 1e-12 {
		t.Fatalf("alpha=%+v", alpha)
	}
	if alpha.FirstDecision != now.Add(-48*time.Hour).Format(time.RFC3339Nano) ||
		alpha.LastActivity != now.Add(-6*time.Hour).Format(time.RFC3339Nano) {
		t.Fatalf("alpha clocks=%+v", alpha)
	}
	if beta := bySystem["beta"]; beta.N != 1 || beta.TotalRealizedProfitDollars != -.5 || beta.NetPerCalendarDay != -.5 {
		t.Fatalf("beta=%+v", beta)
	}
}

func TestFundedContractSettlementKeepsSpecialistPositionsSeparateAndReconcilesLegacyRace(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	makeReceipt := func(seed, system, ticker string, quantity, price, fee float64) FundedRelationReceipt {
		r := fundedRelationTestReceipt(seed, "kalshi", "kalshi", ticker, "YES",
			"event-"+ticker, 1, "independent")
		r.Decision, r.SystemID = now.Add(-time.Hour), system
		r.Quantity, r.EntryPrice, r.EntryFee = quantity, price, fee
		return r
	}

	generic := makeReceipt("g", "auto-cons-xmatch", "SHARED", 20, .50, .35)
	specialist := makeReceipt("s", "poly-pred-kalshi:yes:taker-k", "SHARED", 20, .50, .35)
	// Economic position identity intentionally stays shared. Settlement ownership comes from the
	// exact system/receipt, not from inventing a second fingerprint for the same contract.
	specialist.PositionFingerprint = generic.PositionFingerprint
	genericID, _, err := st.InsertFundedRelationReceipt(ctx, generic)
	if err != nil {
		t.Fatal(err)
	}
	specialistID, _, err := st.InsertFundedRelationReceipt(ctx, specialist)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.RecordFundedRelationOutcomesForContract(ctx, "kalshi", "kalshi",
		"SHARED", "YES", now, 9.65, "settled-win"); err == nil ||
		!strings.Contains(err.Error(), "exact system identity") {
		t.Fatalf("ambiguous compatibility settlement error=%v", err)
	}
	if n, err := st.RecordFundedRelationOutcomesForSystemContract(ctx, "kalshi", "kalshi",
		"SHARED", "YES", generic.SystemID, now, 9.65, "settled-win"); err != nil || n != 1 {
		t.Fatalf("generic exact-system settlement n=%d err=%v", n, err)
	}
	var genericPnL float64
	if err := st.db.QueryRow(`SELECT pnl_dollars FROM funded_relation_outcomes WHERE receipt_id=?`,
		genericID).Scan(&genericPnL); err != nil || genericPnL != 9.65 {
		t.Fatalf("generic pnl=%v err=%v", genericPnL, err)
	}
	var specialistOutcomes int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM funded_relation_outcomes WHERE receipt_id=?`,
		specialistID).Scan(&specialistOutcomes); err != nil || specialistOutcomes != 0 {
		t.Fatalf("generic settlement captured specialist position: count=%d err=%v", specialistOutcomes, err)
	}
	if inserted, corrected, err := st.ReconcileFundedSpecialistRelationOutcome(ctx, specialistID,
		"kalshi", "SHARED", "YES", 20, .50, .35, now, 9.65,
		"genfollow-settlement"); err != nil || !inserted || corrected {
		t.Fatalf("specialist direct settlement inserted=%v corrected=%v err=%v", inserted, corrected, err)
	}

	// The retained specialist ledger rounds closed P&L to cents. An exact first write of -0.195
	// therefore replays as -0.20 on boot; that ordinary half-cent boundary is idempotent, not a
	// conflicting second settlement.
	rounded := makeReceipt("r", "freshlist:yes:taker-p", "ROUNDING", 1, .185, .01)
	roundedID, _, err := st.InsertFundedRelationReceipt(ctx, rounded)
	if err != nil {
		t.Fatal(err)
	}
	if inserted, err := st.RecordFundedRelationOutcome(ctx, roundedID, now, -.195,
		"genfollow-settlement"); err != nil || !inserted {
		t.Fatalf("rounded exact outcome inserted=%v err=%v", inserted, err)
	}
	if inserted, corrected, err := st.ReconcileFundedSpecialistRelationOutcome(ctx, roundedID,
		"kalshi", "ROUNDING", "YES", 1, .185, .01, now.Add(time.Minute), -.20,
		"genfollow-settlement"); err != nil || inserted || corrected {
		t.Fatalf("half-cent replay inserted=%v corrected=%v err=%v", inserted, corrected, err)
	}
	if inserted, corrected, err := st.ReconcileFundedSpecialistRelationOutcome(ctx, roundedID,
		"kalshi", "ROUNDING", "YES", 1, .185, .01, now.Add(2*time.Minute), -.20,
		"other-specialist-settlement"); err == nil || inserted || corrected ||
		!strings.Contains(err.Error(), "conflicting funded specialist outcome") {
		t.Fatalf("different specialist source hidden by rounding tolerance: inserted=%v corrected=%v err=%v",
			inserted, corrected, err)
	}
	roundedGeneric := makeReceipt("q", "freshlist:yes:taker-p", "ROUNDING-GENERIC", 1, .185, .01)
	roundedGenericID, _, err := st.InsertFundedRelationReceipt(ctx, roundedGeneric)
	if err != nil {
		t.Fatal(err)
	}
	if inserted, err := st.RecordFundedRelationOutcome(ctx, roundedGenericID, now, -.195,
		"settled-loss"); err != nil || !inserted {
		t.Fatalf("rounded generic outcome inserted=%v err=%v", inserted, err)
	}
	if inserted, corrected, err := st.ReconcileFundedSpecialistRelationOutcome(ctx,
		roundedGenericID, "kalshi", "ROUNDING-GENERIC", "YES", 1, .185, .01,
		now.Add(time.Minute), -.20, "genfollow-settlement"); err != nil || inserted || corrected {
		t.Fatalf("generic half-cent replay inserted=%v corrected=%v err=%v",
			inserted, corrected, err)
	}

	legacy := makeReceipt("l", "poly-pred-kalshi:yes:taker-k", "LEGACY-RACE", 20, .50, .35)
	legacyID, _, err := st.InsertFundedRelationReceipt(ctx, legacy)
	if err != nil {
		t.Fatal(err)
	}
	// Reproduce the old contract-wide first writer. The immutable original is deliberately wrong
	// for this specialist lot and must remain preserved after reconciliation.
	if inserted, err := st.RecordFundedRelationOutcome(ctx, legacyID, now, 4.825, "settled-win"); err != nil || !inserted {
		t.Fatalf("legacy first outcome inserted=%v err=%v", inserted, err)
	}
	if inserted, corrected, err := st.ReconcileFundedSpecialistRelationOutcome(ctx, legacyID,
		"kalshi", "LEGACY-RACE", "YES", 20, .50, .35, now.Add(time.Minute), 9.65,
		"genfollow-settlement"); err != nil || inserted || !corrected {
		t.Fatalf("legacy correction inserted=%v corrected=%v err=%v", inserted, corrected, err)
	}
	// Idempotent boot replays neither duplicate nor mutate the append-only correction.
	if inserted, corrected, err := st.ReconcileFundedSpecialistRelationOutcome(ctx, legacyID,
		"kalshi", "LEGACY-RACE", "YES", 20, .50, .35, now.Add(time.Minute), 9.65,
		"genfollow-settlement"); err != nil || inserted || corrected {
		t.Fatalf("idempotent correction inserted=%v corrected=%v err=%v", inserted, corrected, err)
	}
	var original, repaired float64
	var correctionRows int
	if err := st.db.QueryRow(`SELECT pnl_dollars FROM funded_relation_outcomes WHERE receipt_id=?`,
		legacyID).Scan(&original); err != nil || original != 4.825 {
		t.Fatalf("immutable original pnl=%v err=%v", original, err)
	}
	if err := st.db.QueryRow(`SELECT corrected_pnl_dollars FROM funded_relation_outcome_corrections
WHERE receipt_id=?`, legacyID).Scan(&repaired); err != nil || repaired != 9.65 {
		t.Fatalf("corrected pnl=%v err=%v", repaired, err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM funded_relation_outcome_corrections WHERE receipt_id=?`,
		legacyID).Scan(&correctionRows); err != nil || correctionRows != 1 {
		t.Fatalf("correction rows=%d err=%v", correctionRows, err)
	}
	rows, err := st.FundedSystemPerformanceRows(ctx, now.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var legacySystem *FundedSystemPerformance
	for i := range rows {
		if rows[i].SystemID == legacy.SystemID {
			legacySystem = &rows[i]
			break
		}
	}
	if legacySystem == nil || legacySystem.N != 2 || legacySystem.ReconciledReceipts != 1 ||
		math.Abs(legacySystem.TotalRealizedProfitDollars-19.30) > 1e-9 {
		t.Fatalf("corrected funded performance=%+v", legacySystem)
	}
}

func paperParlayFixture() paper.Parlay {
	return paper.Parlay{Stake: 2, Price: .2, Contracts: 10, Status: "open", Fees: .1,
		RouteSource: "fixture", Cohort: "fixture", SystemIDs: []string{"fixture-system"}, JointP: .3,
		ExpectedNetPerDollar: .1, Legs: []paper.Leg{{Platform: "kalshi", Ticker: "A", Side: "YES", Entry: .4},
			{Platform: "kalshi", Ticker: "D", Side: "NO", Entry: .5}}}
}

func fundedRelationTestReceipt(seed, portfolio, venue, ticker, side, event string, version int, state string) FundedRelationReceipt {
	fingerprint := strings.Repeat(seed, 64/len(seed)+1)[:64]
	position := strings.Repeat(seed+"f", 64/(len(seed)+1)+1)[:64]
	return FundedRelationReceipt{DecisionFingerprint: fingerprint, PositionFingerprint: position,
		Decision: time.Now().UTC(), Portfolio: portfolio, CandidateVenue: venue, CandidateTicker: ticker,
		CandidateSide: side, SystemID: "fixture-system", RouteKind: "taker", CanonicalEventID: event,
		EventVersion: version, RelationState: state, Allowed: true, Reason: "fixture-approved",
		Quantity: 1, EntryPrice: .4, EntryFee: .01, Legs: []FundedRelationLeg{{Venue: venue,
			Ticker: ticker, Side: side, CanonicalEventID: event, EventVersion: version, RelationState: state}}}
}

func TestFundedRelationReceiptsAreImmutableAndUnknownNeverCountsIndependent(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	epoch := time.Now().Add(-time.Hour)
	a := fundedRelationTestReceipt("a", "kalshi", "kalshi", "A", "YES", "event-1", 1, "independent")
	b := fundedRelationTestReceipt("b", "kalshi", "kalshi", "B", "NO", "event-1", 1, "dependent")
	c := fundedRelationTestReceipt("c", "kalshi", "kalshi", "C", "YES", "event-2", 1, "independent")
	u := fundedRelationTestReceipt("d", "kalshi", "kalshi", "U", "YES", "", 0, "unknown")
	u.EntryFee = -.002 // signed fee truth: a maker rebate is not an invalid negative cost
	ids := make([]string, 0, 4)
	for _, receipt := range []FundedRelationReceipt{a, b, c, u} {
		id, inserted, err := st.InsertFundedRelationReceipt(ctx, receipt)
		if err != nil || !inserted {
			t.Fatalf("insert %+v: id=%q inserted=%v err=%v", receipt, id, inserted, err)
		}
		ids = append(ids, id)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE funded_relation_receipts SET relation_state='independent' WHERE receipt_id=?`, ids[3]); err == nil {
		t.Fatal("immutable receipt accepted an update")
	}
	var rebate float64
	if err := st.db.QueryRowContext(ctx, `SELECT entry_fee FROM funded_relation_receipts WHERE receipt_id=?`, ids[3]).Scan(&rebate); err != nil || rebate != -.002 {
		t.Fatalf("signed maker rebate=%v err=%v", rebate, err)
	}
	for i, pnl := range []float64{2, -1, 3, 4} {
		if inserted, err := st.RecordFundedRelationOutcome(ctx, ids[i], time.Now(), pnl, "fixture-settlement"); err != nil || !inserted {
			t.Fatalf("outcome %d inserted=%v err=%v", i, inserted, err)
		}
	}
	report, err := st.FundedRelationPerformance(ctx, epoch)
	if err != nil || len(report) != 1 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	got := map[string]RelationPerformanceCell{}
	for _, cell := range report[0].Cells {
		got[cell.Relation] = cell
	}
	// A and B share one frozen event but are distinct contracts, so both are dependent even though
	// A's decision-time state was independent before B arrived. C stays independent. U stays unknown.
	if got["dependent"].UniqueBets != 2 || got["dependent"].NetDollars != 1 ||
		got["independent"].UniqueBets != 1 || got["independent"].NetDollars != 3 ||
		got["unknown"].UniqueBets != 1 || got["unknown"].NetDollars != 4 {
		t.Fatalf("relation cells=%+v", got)
	}
}

func TestFundedRelationIdentityRejectionPreservesRawCandidateWithoutFundedClassification(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	decision := time.Now().UTC()
	in := FundedRelationIdentityRejection{Decision: decision, Venue: "polymarket",
		Ticker: "0xfixture", Side: "DOWN", SystemID: "auto-cons-pcrypto", RouteKind: "candidate",
		Reason: "track-only", IdentityIssue: "nonbinary-side,nonfunded-venue",
		RawLegs: []FundedRelationLeg{{Venue: "polymarket", Ticker: "0xfixture", Side: "DOWN",
			RelationState: "unknown"}}}
	id, inserted, err := st.InsertFundedRelationIdentityRejection(ctx, in)
	if err != nil || !inserted || len(id) != 64 {
		t.Fatalf("first identity rejection id=%q inserted=%v err=%v", id, inserted, err)
	}
	id2, inserted, err := st.InsertFundedRelationIdentityRejection(ctx, in)
	if err != nil || inserted || id2 != id {
		t.Fatalf("dedupe identity rejection id=%q inserted=%v err=%v", id2, inserted, err)
	}
	var venue, ticker, side, issue, raw string
	if err := st.db.QueryRowContext(ctx, `SELECT candidate_venue,candidate_ticker,candidate_side,
identity_issue,raw_legs_json FROM funded_relation_identity_rejections WHERE rejection_id=?`, id).
		Scan(&venue, &ticker, &side, &issue, &raw); err != nil {
		t.Fatal(err)
	}
	if venue != "polymarket" || ticker != "0xfixture" || side != "DOWN" ||
		issue != "nonbinary-side,nonfunded-venue" || !strings.Contains(raw, `"RelationState":"unknown"`) {
		t.Fatalf("raw rejection mutated venue=%q ticker=%q side=%q issue=%q raw=%s", venue, ticker, side, issue, raw)
	}
	var funded int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM funded_relation_receipts`).Scan(&funded); err != nil || funded != 0 {
		t.Fatalf("unclassified rejection entered funded receipts: count=%d err=%v", funded, err)
	}
	if _, _, err := st.InsertFundedRelationReceipt(ctx,
		FundedRelationReceipt{DecisionFingerprint: strings.Repeat("f", 64), PositionFingerprint: strings.Repeat("e", 64),
			Decision: decision, Portfolio: "kalshi", CandidateVenue: "polymarket", CandidateTicker: "0xfixture",
			CandidateSide: "DOWN", SystemID: "bad-funded", RouteKind: "taker", RelationState: "unknown",
			Allowed: true, Reason: "must-stay-strict", Quantity: 1, EntryPrice: .5,
			Legs: []FundedRelationLeg{{Venue: "polymarket", Ticker: "0xfixture", Side: "DOWN", RelationState: "unknown"}}}); err == nil {
		t.Fatal("funded receipt accepted a nonbinary execution side")
	}
	if _, _, err := st.InsertFundedRelationReceipt(ctx,
		FundedRelationReceipt{DecisionFingerprint: strings.Repeat("1", 64), PositionFingerprint: strings.Repeat("2", 64),
			Decision: decision, Portfolio: "kalshi", CandidateVenue: "polymarket", CandidateTicker: "0xbinary",
			CandidateSide: "YES", SystemID: "bad-funded-binary", RouteKind: "taker", RelationState: "unknown",
			Allowed: true, Reason: "nonfunded-venue-must-stay-strict", Quantity: 1, EntryPrice: .5,
			Legs: []FundedRelationLeg{{Venue: "polymarket", Ticker: "0xbinary", Side: "YES", RelationState: "unknown"}}}); err == nil {
		t.Fatal("funded receipt accepted a binary side from a nonfunded venue")
	}
	misaligned := fundedRelationTestReceipt("9", "kalshi", "polyus", "PUS-TICKER", "YES", "", 0, "unknown")
	if _, _, err := st.InsertFundedRelationReceipt(ctx, misaligned); err == nil {
		t.Fatal("Kalshi funded portfolio accepted a PolyUS candidate")
	}
	mixedLegs := fundedRelationTestReceipt("8", "kalshi", "kalshi", "K-TICKER", "YES", "", 0, "unknown")
	mixedLegs.Legs = append(mixedLegs.Legs, FundedRelationLeg{Venue: "polyus", Ticker: "PUS-TICKER",
		Side: "NO", RelationState: "unknown"})
	if _, _, err := st.InsertFundedRelationReceipt(ctx, mixedLegs); err == nil {
		t.Fatal("Kalshi funded portfolio accepted a PolyUS leg")
	}
}

func TestInsertFundedParlayWithRelationIsAtomicAndSettlesOutcome(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	r := fundedRelationTestReceipt("e", "combos", "kalshi", "A", "YES", "event-3", 1, "independent")
	r.Legs = append(r.Legs, FundedRelationLeg{Venue: "kalshi", Ticker: "D", Side: "NO",
		CanonicalEventID: "event-4", EventVersion: 1, RelationState: "independent"})
	id, _, err := st.InsertFundedRelationReceipt(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	parlayID, err := st.InsertParlayWithRelation(ctx, paperParlayFixture(), id)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SettleParlay(ctx, parlayID, 1, 7.25); err != nil {
		t.Fatal(err)
	}
	var pnl float64
	if err := st.db.QueryRowContext(ctx, `SELECT pnl_dollars FROM funded_relation_outcomes WHERE receipt_id=?`, id).Scan(&pnl); err != nil || pnl != 7.25 {
		t.Fatalf("pnl=%v err=%v", pnl, err)
	}
}

func TestR144ProperRelationIsClassifiedWithinEachScoringTransform(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	makeTrial := func(transform, ticker string) ProperScoreTrial {
		t.Helper()
		observationID, inserted, insertErr := st.InsertResearchSystemObservation(ctx, ResearchSystemObservation{
			Observed: now, SystemID: "proper-score-executor", ExperimentVersion: 1,
			OpportunityID: "proper-relation-" + transform + "-" + ticker, Kind: "control",
			Cohort: "proper-relation-fixture", CanonicalEventID: "event-1", EventVersion: 1,
			Venue: "kalshi", Ticker: ticker, Route: "observer", Side: "YES",
			CertificateStatus: "not_applicable", SourceArtifact: "proper relation fixture",
			OutcomeStatus: "open", Blocker: "test fixture",
		})
		if insertErr != nil || !inserted {
			t.Fatalf("insert observation %s/%s: id=%d inserted=%v err=%v", transform, ticker, observationID, inserted, insertErr)
		}
		trial := properStorageFixture(transform)
		trial.Ticker = ticker
		trial.ForecastSignal = "relation-" + transform + "-" + ticker
		trial.ResearchObservationID = observationID
		return trial
	}
	trials := []ProperScoreTrial{makeTrial("brier", "A"), makeTrial("log", "A"), makeTrial("log", "B")}
	freezeProperStorageManifests(t, st, trials...)
	for _, trial := range trials {
		if inserted, insertErr := st.InsertProperScoreTrial(ctx, trial); insertErr != nil || !inserted {
			t.Fatalf("insert proper trial %s/%s: inserted=%v err=%v", trial.Transform, trial.Ticker, inserted, insertErr)
		}
	}
	closed := now.Add(time.Minute).Format(time.RFC3339Nano)
	if _, err := st.db.ExecContext(ctx, `UPDATE research_proper_score_trials
SET settled=1,grade_status='graded',realized_net=.10,closed_ts=?`, closed); err != nil {
		t.Fatal(err)
	}
	rows, err := st.ProperScoreRelationPerformance(ctx, now.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	type key struct{ transform, relation string }
	got := map[key]int{}
	for _, row := range rows {
		got[key{row.Transform, row.Relation}] = row.UniqueBets
	}
	if got[key{"brier", "independent"}] != 1 || got[key{"brier", "dependent"}] != 0 ||
		got[key{"log", "independent"}] != 0 || got[key{"log", "dependent"}] != 2 {
		t.Fatalf("cross-transform contracts contaminated relation labels: %+v", got)
	}
}
