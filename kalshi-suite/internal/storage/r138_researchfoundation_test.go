package storage

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
)

func TestR138ImmutableResearchBlueprints(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()

	var n, funded, paper, live int
	if err := st.db.QueryRow(`SELECT COUNT(*),COALESCE(SUM(funded),0),COALESCE(SUM(paper_authority),0),
COALESCE(SUM(live_authority),0) FROM research_experiment_specs`).Scan(&n, &funded, &paper, &live); err != nil {
		t.Fatal(err)
	}
	if n != 23 || funded != 0 || paper != 0 || live != 0 {
		t.Fatalf("blueprint authority/count = n%d funded%d paper%d live%d", n, funded, paper, live)
	}
	var timingCohort, horizonRule string
	if err := st.db.QueryRow(`SELECT cohort_json,horizon_rule FROM research_experiment_specs
WHERE experiment_id='replenishment-fingerprint' AND version=2`).Scan(&timingCohort, &horizonRule); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(timingCohort, "ram-fixed-horizon-v2") ||
		!strings.Contains(horizonRule, "version-1 gate-delayed marks are non-comparable") {
		t.Fatalf("timing v2 contract does not isolate legacy marks: cohort=%s horizon=%s", timingCohort, horizonRule)
	}
	if err := st.db.QueryRow(`SELECT cohort_json,horizon_rule FROM research_experiment_specs
WHERE experiment_id='replenishment-fingerprint' AND version=3`).Scan(&timingCohort, &horizonRule); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(timingCohort, "ram-fixed-horizon-atomic-v3") ||
		!strings.Contains(horizonRule, "versions 1-2 cannot enter") {
		t.Fatalf("timing v3 contract does not isolate non-atomic history: cohort=%s horizon=%s", timingCohort, horizonRule)
	}
	var flowCohort string
	if err := st.db.QueryRow(`SELECT cohort_json FROM research_experiment_specs
WHERE experiment_id='flow-direction-integrity' AND version=2`).Scan(&flowCohort); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(flowCohort, `"projection_contract":"step7-frozen-outbox-v1"`) ||
		!strings.Contains(flowCohort, `"inferred_side_can_select":false`) {
		t.Fatalf("flow v2 atomic contract missing: %s", flowCohort)
	}
	if err := st.db.QueryRow(`SELECT cohort_json FROM research_experiment_specs
WHERE experiment_id='flow-direction-integrity' AND version=3`).Scan(&flowCohort); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(flowCohort, `"flow_projection_semantics":"kalshi-venue-local-structural-identity-v3"`) ||
		!strings.Contains(flowCohort, `"structural_identity_action":"blocked_negative"`) ||
		!strings.Contains(flowCohort, `"cross_venue_equivalence_verified":false`) ||
		!strings.Contains(flowCohort, `"paper_authority":false`) ||
		!strings.Contains(flowCohort, `"live_authority":false`) {
		t.Fatalf("flow v3 structural contract missing or over-authorized: %s", flowCohort)
	}
	ids := ResearchExperimentIDs()
	wantIDs := []string{
		"attention-spillover-graph", "clientele-clock-basis", "collateral-release-rotation",
		"deadline-hazard-surface", "flow-direction-integrity", "forecast-persona-router",
		"identity-challenged-cross-venue-lock", "incentive-subsidized-structural-lock",
		"maker-salvage-matched-cohort", "outcome-set-expansion-shock", "paired-bridge-inversion",
		"payoff-constraint-solver", "proper-score-executor", "replenishment-fingerprint",
		"score-state-surface", "semantic-complexity-premium", "series-roll-anchor",
		"settlement-latency-carry", "side-normalized-crowding-fade",
	}
	if !reflect.DeepEqual(ids, wantIDs) {
		t.Fatalf("experiment ids=%v want=%v", ids, wantIDs)
	}

	// Opening the same database again must validate the exact immutable hashes, not duplicate rows.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_experiment_specs`).Scan(&n); err != nil || n != 23 {
		t.Fatalf("reopen registry n=%d err=%v", n, err)
	}

	// A semantic edit under v1 is refused; callers must append a deliberate v2.
	bad := r137ExperimentBlueprints()[0]
	bad.Hypothesis = "rewritten after seeing results"
	if _, err := st.RegisterExperimentSpec(ctx, bad); err == nil || !strings.Contains(err.Error(), "bump version") {
		t.Fatalf("experiment drift should fail with version guidance, got %v", err)
	}
	if _, err := st.db.Exec(`UPDATE research_experiment_specs SET funded=1 WHERE experiment_id='proper-score-executor'`); err == nil {
		t.Fatal("immutable experiment trigger allowed update")
	}
	if _, err := st.db.Exec(`DELETE FROM research_experiment_events`); err == nil {
		t.Fatal("append-only experiment-event trigger allowed delete")
	}
	var codeHash, dataHash string
	if err := st.db.QueryRow(`SELECT code_manifest_hash,data_manifest_hash FROM research_experiment_specs
WHERE experiment_id='forecast-persona-router' AND version=1`).Scan(&codeHash, &dataHash); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendResearchExperimentEvent(ctx, ResearchExperimentEventInput{
		ExperimentID: "forecast-persona-router", ExperimentVersion: 1, EventType: "collecting",
		State: "COLLECTING", Message: "collector implementation started",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendResearchExperimentEvent(ctx, ResearchExperimentEventInput{
		ExperimentID: "forecast-persona-router", ExperimentVersion: 1, EventType: "result",
		State: "TRAIN_RESULT_NEGATIVE", Message: "negative train result retained",
		EvidenceJSON: `{"lower_bound":-0.1}`, ResultClass: "negative", CodeManifestHash: codeHash,
		DataManifestHash: dataHash, SourceManifestHash: "sha256:fixture-source",
	}); err != nil {
		t.Fatalf("negative result append: %v", err)
	}
	if _, err := st.AppendResearchExperimentEvent(ctx, ResearchExperimentEventInput{
		ExperimentID: "proper-score-executor", ExperimentVersion: 1, EventType: "holdout_pass",
		State: "REPLICATED_UNTOUCHED", Message: "illegal shortcut", EvidenceJSON: `{"lb":1}`,
		ResultClass: "positive", CodeManifestHash: codeHash, DataManifestHash: dataHash,
		SourceManifestHash: "sha256:fixture-source",
	}); err == nil {
		t.Fatal("holdout pass bypassed legal state/provenance checks")
	}

	report, err := st.ResearchFoundationReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report["funded"] != false || report["paper_authority"] != false || report["live_authority"] != false {
		t.Fatalf("report granted authority: %#v", report)
	}
}

func TestR138CanonicalIdentityVersionsAndRelations(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	event := CanonicalEventSpec{EventID: "sports:nba:2026-07-11:AAA@BBB", EventType: "sports-game",
		Domain: "nba", Title: "AAA at BBB", StartTS: "2026-07-12T00:00:00Z", Timezone: "UTC",
		SourceArtifact: "fixture", OutcomeSetStatus: "incomplete", EvidenceJSON: `{"title_match":false}`,
		SourceObservedTS: "2020-01-01T00:00:00Z"}
	payA := CanonicalPayoffSpec{PayoffID: event.EventID + "|winner|AAA", EventID: event.EventID,
		Label: "AAA wins", PredicateJSON: `{"kind":"winner","team":"AAA"}`,
		BoundaryRule: "AAA wins full game", PayoutFloor: 0, PayoutCeiling: 1,
		SourceArtifact: "fixture", IdentityStatus: "structural", SourceObservedTS: "2020-01-01T00:00:00Z"}
	payB := CanonicalPayoffSpec{PayoffID: event.EventID + "|winner|BBB", EventID: event.EventID,
		Label: "BBB wins", PredicateJSON: `{"kind":"winner","team":"BBB"}`,
		BoundaryRule: "BBB wins full game", PayoutFloor: 0, PayoutCeiling: 1,
		SourceArtifact: "fixture", IdentityStatus: "structural", SourceObservedTS: "2020-01-01T00:00:00Z"}
	inst := CanonicalInstrumentSpec{Venue: "kalshi", Ticker: "KXTEST-A", EventID: event.EventID,
		PayoffID: payA.PayoffID, NativeSide: "YES", Orientation: "same", MarketKind: "winner",
		Scope: "full_game", RulesArtifact: "fixture", CrossVenueBasis: "rule docs still required",
		IdentityStatus: "structural", SourceObservedTS: "2020-01-01T00:00:00Z"}
	r, err := st.RegisterCanonicalBatch(ctx, []CanonicalEventSpec{event}, []CanonicalPayoffSpec{payA, payB}, []CanonicalInstrumentSpec{inst})
	if err != nil {
		t.Fatal(err)
	}
	if r.EventsInserted != 1 || r.PayoffsInserted != 2 || r.InstrumentsInserted != 1 {
		t.Fatalf("first batch=%+v", r)
	}
	r, err = st.RegisterCanonicalBatch(ctx, []CanonicalEventSpec{event}, []CanonicalPayoffSpec{payA, payB}, []CanonicalInstrumentSpec{inst})
	if err != nil || r.EventsInserted+r.PayoffsInserted+r.InstrumentsInserted != 0 {
		t.Fatalf("idempotent batch=%+v err=%v", r, err)
	}
	var observed string
	if err := st.db.QueryRow(`SELECT MAX(observed_ts) FROM research_identity_sightings WHERE object_type='event'`).Scan(&observed); err != nil || !strings.HasPrefix(observed, "2020-01-01T00:00:00") {
		t.Fatalf("source sighting=%q err=%v; scan time must not masquerade as source freshness", observed, err)
	}

	changed := event
	changed.VoidPolicy = "void on abandonment"
	r, err = st.RegisterCanonicalBatch(ctx, []CanonicalEventSpec{changed}, []CanonicalPayoffSpec{payA, payB}, []CanonicalInstrumentSpec{inst})
	if err != nil {
		t.Fatal(err)
	}
	if r.EventsInserted != 1 || r.PayoffsInserted != 2 || r.InstrumentsInserted != 1 {
		t.Fatalf("semantic version propagation=%+v", r)
	}
	for table, want := range map[string]int{"research_event_specs": 2, "research_payoff_specs": 4, "research_instrument_specs": 2} {
		var got int
		if err := st.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil || got != want {
			t.Fatalf("%s count=%d want=%d err=%v", table, got, want, err)
		}
	}

	zero := 0.0
	rel := CanonicalRelationSpec{RelationID: "rel-winner-exclusive", EventID: event.EventID,
		LeftPayoffID: payA.PayoffID, RightPayoffID: payB.PayoffID, RelationType: "excludes",
		PayoutFloor: &zero, IdentityStatus: "structural", EvidenceJSON: `{"source":"fixture"}`}
	if err := st.RegisterPayoffRelation(ctx, rel); err != nil {
		t.Fatal(err)
	}
	if err := st.RegisterPayoffRelation(ctx, rel); err != nil {
		t.Fatalf("identical relation should be idempotent: %v", err)
	}
	rel.RelationType = "implies"
	if err := st.RegisterPayoffRelation(ctx, rel); err != nil {
		t.Fatalf("relation semantic change should append v2: %v", err)
	}
	var relationVersions int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_payoff_relations WHERE relation_id=?`, rel.RelationID).Scan(&relationVersions); err != nil || relationVersions != 2 {
		t.Fatalf("relation versions=%d err=%v", relationVersions, err)
	}
	if _, err := st.db.Exec(`DELETE FROM research_event_specs WHERE event_id=?`, event.EventID); err == nil {
		t.Fatal("immutable event trigger allowed delete")
	}
	other := CanonicalEventSpec{EventID: "sports:nba:other", EventType: "sports-game", SourceArtifact: "fixture"}
	otherPayoff := CanonicalPayoffSpec{PayoffID: "sports:nba:other|winner|CCC", EventID: other.EventID,
		PredicateJSON: `{"team":"CCC"}`, PayoutCeiling: 1, SourceArtifact: "fixture"}
	if _, err := st.RegisterCanonicalBatch(ctx, []CanonicalEventSpec{other}, []CanonicalPayoffSpec{otherPayoff}, nil); err != nil {
		t.Fatal(err)
	}
	cross := inst
	cross.Ticker, cross.PayoffID = "KX-CROSS", otherPayoff.PayoffID
	if _, err := st.RegisterCanonicalBatch(ctx, nil, nil, []CanonicalInstrumentSpec{cross}); err == nil || !strings.Contains(err.Error(), "belongs to") {
		t.Fatalf("cross-event instrument accepted: %v", err)
	}
	crossRel := CanonicalRelationSpec{RelationID: "cross", EventID: event.EventID,
		LeftPayoffID: payA.PayoffID, RightPayoffID: otherPayoff.PayoffID, RelationType: "correlated"}
	if err := st.RegisterPayoffRelation(ctx, crossRel); err == nil || !strings.Contains(err.Error(), "crosses event") {
		t.Fatalf("cross-event relation accepted: %v", err)
	}
}

func TestR138StructuralIdentityPageExcludesFuzzy(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if err := st.UpsertGameIdentity(ctx, []GameIdentityRow{{GameID: "nba:2026-07-11:AAA@BBB", League: "nba", Away: "AAA", Home: "BBB"}}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertMarketGame(ctx, []MarketGameRow{
		{Venue: "kalshi", Ticker: "KX-STRUCT", GameID: "nba:2026-07-11:AAA@BBB", MktType: "winner", YesTeam: "AAA", Src: "struct"},
		{Venue: "polyus", Ticker: "pus-fuzzy", GameID: "nba:2026-07-11:AAA@BBB", MktType: "winner", YesTeam: "AAA", Src: "fuzzy"},
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := st.StructuralIdentityPage(ctx, "", "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Ticker != "KX-STRUCT" {
		t.Fatalf("structural-only page=%+v", rows)
	}
	var fk int
	if err := st.db.QueryRow(`PRAGMA foreign_keys`).Scan(&fk); err != nil && err != sql.ErrNoRows {
		t.Fatal(err)
	}
	if fk != 1 {
		t.Fatalf("foreign_keys=%d want 1", fk)
	}
}
