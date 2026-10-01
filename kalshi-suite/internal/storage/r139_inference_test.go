package storage

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func insertR139Event(t *testing.T, tx execer, id string, created time.Time) {
	t.Helper()
	if _, err := tx.Exec(`INSERT INTO research_event_specs(event_id,version,spec_hash,event_type,domain,created_ts)
VALUES(?,1,?,'binary','fixture',?)`, id, fmt.Sprintf("sha256:%064x", len(id)*7919), created.UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
}

type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

func insertR139Observation(t *testing.T, tx execer, system, opportunity, event, venue string,
	observed time.Time, latencyKnown bool, experimentVersions ...int) int64 {
	t.Helper()
	ticker := venue + "|" + event
	payoffID := event + "|YES"
	identityKnown := !strings.Contains(event, "missing")
	var payoffArg, payoffVersionArg any
	instrumentVersion := 0
	if identityKnown {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO research_payoff_specs(
payoff_id,version,spec_hash,event_id,event_version,identity_status,created_ts)
VALUES(?,1,?,?,1,'verified',?)`, payoffID, fmt.Sprintf("sha256:%064x", len(payoffID)*3571), event,
			observed.UTC().Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`INSERT OR IGNORE INTO research_instrument_specs(
venue,ticker,version,spec_hash,event_id,event_version,payoff_id,payoff_version,native_side,
orientation,identity_status,created_ts) VALUES(?,?,1,?,?,1,?,1,'YES','same','verified',?)`,
			venue, ticker, fmt.Sprintf("sha256:%064x", len(ticker)*4567), event, payoffID,
			observed.UTC().Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
		payoffArg, payoffVersionArg = payoffID, 1
		instrumentVersion = 1
	}
	flag := 0
	if latencyKnown {
		flag = 1
	}
	experimentVersion := 1
	if len(experimentVersions) > 0 {
		experimentVersion = experimentVersions[0]
	}
	res, err := tx.Exec(`INSERT INTO research_system_observations(
observed_ts,decision_ts,observed_slot,system_id,experiment_version,opportunity_id,observation_kind,canonical_event_id,event_version,
canonical_payoff_id,payoff_version,instrument_version,venue,ticker,route,side,certificate_status,source_clock_id,source_artifact,book_source,fee_source,
quote_age_max_s,tick_min,size_units,executable_cost,exact_fee,payout_lower,payout_upper,net_lower,
net_upper,visible_capacity,capital_seconds,decision_latency_ms,latency_known,quote_age_known,tick_known,
depth_known,fee_known,outcome_status,candidate)
	VALUES(?,?,?,?,?,?, 'negative',?,1,?,?,?,?,?, 'taker','YES','verified','fixture-clock','fixture-settlement',
	'fixture-book','fixture-fee',0.1,0.01,1,0.40,0.01,0,1,-0.41,0.59,10,3600,5,?,1,1,1,1,'open',0)`,
		observed.UTC().Format(time.RFC3339Nano), observed.UTC().Format(time.RFC3339Nano),
		observed.UTC().Truncate(5*time.Minute).Format(time.RFC3339),
		system, experimentVersion, opportunity, event, payoffArg, payoffVersionArg, instrumentVersion, venue, ticker, flag)
	if err != nil {
		t.Fatal(err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func insertR139Payoff(t *testing.T, tx execer, observationID int64, observed time.Time, status string,
	lower, upper float64, realized *float64, suffix string) {
	t.Helper()
	if _, err := tx.Exec(`INSERT INTO research_system_payoff_updates(
observation_id,observed_ts,status,payout_lower,payout_upper,realized_net,source_artifact,source_hash,reason)
VALUES(?,?,?,?,?,?, 'fixture-settlement',?, 'fixture')`, observationID,
		observed.UTC().Format(time.RFC3339Nano), status, lower, upper, realized,
		fmt.Sprintf("sha256:%064s", suffix)); err != nil {
		t.Fatal(err)
	}
}

func TestR139InferenceAlwaysReturnsExactlyNineteenAndDeduplicatesManifest(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	asOf := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	a, inserted, err := st.RunR139ResearchInference(context.Background(), asOf)
	if err != nil || !inserted {
		t.Fatalf("first run inserted=%v err=%v", inserted, err)
	}
	b, inserted, err := st.RunR139ResearchInference(context.Background(), asOf)
	if err != nil || inserted {
		t.Fatalf("duplicate run inserted=%v err=%v", inserted, err)
	}
	if a.RunID != b.RunID || a.InputManifestHash != b.InputManifestHash || a.ResultHash != b.ResultHash ||
		len(a.Results) != 19 || !reflect.DeepEqual(a.Results, b.Results) {
		t.Fatalf("non-deterministic receipt: a=%+v b=%+v", a, b)
	}
	for _, r := range a.Results {
		if r.State != "INSUFFICIENT_NO_EXACT_TERMINAL_ROWS" || r.ExecutionCandidate ||
			r.PreregisteredUntouchedGatePass || r.Funded || r.PaperAuthority || r.LiveAuthority {
			t.Fatalf("empty system result overclaimed authority/evidence: %+v", r)
		}
	}
	candidate, funded, paper, live, err := st.R139InferenceAuthorityCounts(context.Background())
	if err != nil || candidate != 0 || funded != 0 || paper != 0 || live != 0 {
		t.Fatalf("authority=%d/%d/%d/%d err=%v", candidate, funded, paper, live, err)
	}
	if _, err := st.db.Exec(`UPDATE research_inference_runs SET status='complete' WHERE id=?`, a.RunID); err == nil {
		t.Fatal("append-only inference receipt allowed update")
	}
}

func TestR174LateZeroOutputProjectionChangesInferenceManifest(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	asOf := time.Now().UTC()
	first, inserted, err := st.RunR139ResearchInference(ctx, asOf)
	if err != nil || !inserted {
		t.Fatalf("first inference inserted=%v err=%v", inserted, err)
	}
	flow := step7ProjectionTestFlow("late-zero-output-manifest",
		asOf.Truncate(24*time.Hour).Add(-time.Hour))
	job, err := NewStep7FlowProjectionJob(flow, 1, "terminal no eligible projection", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, flowN, jobN, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{flow}, []Step7ProjectionJob{job}); err != nil ||
		flowN != 1 || jobN != 1 {
		t.Fatalf("late zero-output raw/job=%d/%d err=%v", flowN, jobN, err)
	}
	projectionManifest, err := st.Step7ProjectionCompletionManifest(ctx, asOf.Truncate(24*time.Hour))
	if err != nil || projectionManifest.Count != 1 || projectionManifest.Hash == "" ||
		projectionManifest.MaxCompletedTS == "" {
		t.Fatalf("completion manifest=%+v err=%v", projectionManifest, err)
	}
	second, inserted, err := st.RunR139ResearchInference(ctx, asOf)
	if err != nil || !inserted {
		t.Fatalf("second inference inserted=%v err=%v", inserted, err)
	}
	if first.ObservedRows != second.ObservedRows || first.ObservedRows != 0 {
		t.Fatalf("zero-output projection changed observation rows %d -> %d",
			first.ObservedRows, second.ObservedRows)
	}
	if first.RunID == second.RunID || first.InputManifestHash == second.InputManifestHash {
		t.Fatalf("late zero-output completion reused stale receipt: first=%d/%s second=%d/%s",
			first.RunID, first.InputManifestHash, second.RunID, second.InputManifestHash)
	}
	third, inserted, err := st.RunR139ResearchInference(ctx, asOf)
	if err != nil || inserted || third.RunID != second.RunID {
		t.Fatalf("unchanged completion manifest did not dedupe: inserted=%v run=%d want=%d err=%v",
			inserted, third.RunID, second.RunID, err)
	}
}

func TestR174InferenceCutoffsUseChronologicalRFC3339(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	cutoff := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
	asOf := cutoff.Add(12 * time.Hour)
	tx, err := st.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	insertR139Event(t, tx, "before-cutoff", cutoff.Add(-time.Hour))
	beforeID := insertR139Observation(t, tx, "proper-score-executor", "before-cutoff",
		"before-cutoff", "kalshi", cutoff.Add(-time.Millisecond), true)
	insertR139Event(t, tx, "after-cutoff-fraction", cutoff.Add(123*time.Millisecond))
	_ = insertR139Observation(t, tx, "proper-score-executor", "after-cutoff-fraction",
		"after-cutoff-fraction", "kalshi", cutoff.Add(123*time.Millisecond), true)
	value := .25
	insertR139Payoff(t, tx, beforeID, asOf.Add(123*time.Millisecond), "settled", 1, 1,
		&value, "post-asof-fraction")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	rows, _, _, observed, maxObservationID, maxUpdateID, err :=
		st.r139LoadInferenceSnapshot(ctx, cutoff, asOf, 10)
	if err != nil {
		t.Fatal(err)
	}
	if observed != 1 || maxObservationID != beforeID || maxUpdateID != 0 ||
		len(rows) != 0 {
		t.Fatalf("chronological cutoff leaked same-second fractional row/update: observed=%d max_obs=%d want=%d max_update=%d terminal=%d",
			observed, maxObservationID, beforeID, maxUpdateID, len(rows))
	}
}

func TestR174InferenceCannotMixSupersededStep7TimingOrProjectionVersion(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	tx, err := st.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	value := .10
	for version := 1; version <= 3; version++ {
		event := fmt.Sprintf("event:step7-timing-v%d", version)
		insertR139Event(t, tx, event, base)
		id := insertR139Observation(t, tx, "replenishment-fingerprint",
			fmt.Sprintf("step7-timing-v%d", version), event, "kalshi",
			base.Add(time.Duration(version)*time.Minute), true, version)
		insertR139Payoff(t, tx, id, base.Add(24*time.Hour), "settled", 1, 1, &value,
			fmt.Sprintf("timing-v%d", version))
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	report, _, err := st.RunR139ResearchInference(context.Background(), base.AddDate(0, 0, 3))
	if err != nil {
		t.Fatal(err)
	}
	var replenishment ResearchInferenceSystemResult
	for _, result := range report.Results {
		if result.SystemID == "replenishment-fingerprint" {
			replenishment = result
			break
		}
	}
	if replenishment.ExperimentVersion != 3 || replenishment.TerminalRows != 1 ||
		report.ExactTerminalRows != 1 {
		t.Fatalf("superseded timing cohort mixed into inference: result=%+v exact=%d",
			replenishment, report.ExactTerminalRows)
	}
}

func TestR140InferenceKeysetPagesCompleteSnapshotWithoutRowLimit(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	base := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	tx, err := st.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	insertR139Event(t, tx, "event:paged", base)
	var lastID int64
	for i := 0; i < 7; i++ {
		lastID = insertR139Observation(t, tx, "deadline-hazard-surface", fmt.Sprintf("paged:%d", i),
			"event:paged", "kalshi", base.Add(time.Duration(i)*time.Minute), true)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	cutoff, asOf := base.Add(24*time.Hour), base.Add(36*time.Hour)
	a, ax, ah, observed, maxObs, _, err := st.r139LoadInferenceSnapshot(context.Background(), cutoff, asOf, 2)
	if err != nil {
		t.Fatal(err)
	}
	b, bx, bh, observedB, maxObsB, _, err := st.r139LoadInferenceSnapshot(context.Background(), cutoff, asOf, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 0 || len(b) != 0 || ax.Open != 7 || bx.Open != 7 || observed != 7 || observedB != 7 ||
		maxObs != lastID || maxObsB != lastID || ah == "" || ah != bh || ax != bx {
		t.Fatalf("paged snapshot changed by page size: a=%d/%+v/%s/%d/%d b=%d/%+v/%s/%d/%d",
			len(a), ax, ah, observed, maxObs, len(b), bx, bh, observedB, maxObsB)
	}
}

func TestR139InferenceCountsOpenVoidCensorNonexactIdentityAndRouteExclusions(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	// One deliberately legacy identity-broken row exercises the classifier. Current inserts are
	// protected by this trigger and the central Go boundary; dropping it is test-only migration
	// simulation, not a supported production path.
	if _, err := st.db.Exec(`DROP TRIGGER research_system_observations_exact_identity_insert`); err != nil {
		t.Fatal(err)
	}
	tx, err := st.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	insertR139Event(t, tx, "event:valid", base)
	ids := make([]int64, 7)
	for i := range ids {
		event := "event:valid"
		if i == 4 {
			event = "event:missing"
		}
		ids[i] = insertR139Observation(t, tx, "deadline-hazard-surface", fmt.Sprintf("opp:%d", i),
			event, "kalshi", base.Add(time.Duration(i)*time.Hour), i != 5)
	}
	zero := 0.0
	insertR139Payoff(t, tx, ids[1], base.Add(24*time.Hour), "voided", 0, 0, nil, "void")
	insertR139Payoff(t, tx, ids[2], base.Add(24*time.Hour), "censored", 0, 1, nil, "censor")
	insertR139Payoff(t, tx, ids[3], base.Add(24*time.Hour), "settled", 0, 1, &zero, "interval")
	insertR139Payoff(t, tx, ids[4], base.Add(24*time.Hour), "settled", 1, 1, &zero, "identity")
	insertR139Payoff(t, tx, ids[5], base.Add(24*time.Hour), "settled", 1, 1, &zero, "route")
	// An authoritative scalar void/refund with realized net is terminal money truth, not a dropped
	// member. Only a nonexact void remains in the exclusion count above.
	insertR139Payoff(t, tx, ids[6], base.Add(24*time.Hour), "voided", 0, 0, &zero, "exact-void")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	report, _, err := st.RunR139ResearchInference(context.Background(), base.AddDate(0, 0, 3))
	if err != nil {
		t.Fatal(err)
	}
	want := ResearchInferenceExclusions{Open: 1, Void: 1, Censored: 1, NonExact: 1, Identity: 1, RouteTruth: 1}
	if report.ObservedRows != 7 || report.ExactTerminalRows != 1 || report.Exclusions != want {
		t.Fatalf("classification observed=%d exact=%d exclusions=%+v want=%+v",
			report.ObservedRows, report.ExactTerminalRows, report.Exclusions, want)
	}
}

func TestR139InferenceUsesEventSplitQuietDaysShrinkageAndMultiplicityAsMonitoringOnly(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	tx, err := st.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	positive := .20
	for i := 0; i < 160; i++ {
		event := fmt.Sprintf("event:positive:%03d", i)
		when := base.AddDate(0, 0, i)
		insertR139Event(t, tx, event, when)
		venue := "kalshi"
		if i%2 == 1 {
			venue = "polyus"
		}
		id := insertR139Observation(t, tx, "proper-score-executor", fmt.Sprintf("positive:%03d", i),
			event, venue, when, true)
		insertR139Payoff(t, tx, id, when.Add(12*time.Hour), "settled", 1, 1, &positive, fmt.Sprintf("settled-%03d", i))
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Ten later completed quiet days are intentionally included in the rolling final-slice monitor.
	report, inserted, err := st.RunR139ResearchInference(context.Background(), base.AddDate(0, 0, 170))
	if err != nil || !inserted || len(report.Results) != 19 {
		t.Fatalf("run inserted=%v systems=%d err=%v", inserted, len(report.Results), err)
	}
	var got ResearchInferenceSystemResult
	for _, r := range report.Results {
		if r.SystemID == "proper-score-executor" {
			got = r
		}
		if r.RawP > r.HolmP+1e-12 || r.RawP > r.BYQ+1e-12 {
			t.Fatalf("dependence-safe correction became less conservative: %+v", r)
		}
		if r.ExecutionCandidate || r.PreregisteredUntouchedGatePass || r.Funded || r.PaperAuthority || r.LiveAuthority {
			t.Fatalf("inference granted execution authority: %+v", r)
		}
	}
	if got.State != "AWAITING_PREREGISTERED_UNTOUCHED_REPLICATION" || !got.MonitoringLowerBoundPositive ||
		got.PreregisteredUntouchedGatePass || !got.ValidationBoundsKnown || !got.MonitoringBoundsKnown ||
		got.ValidationLower <= 0 || got.MonitoringLower <= 0 || got.MonitoringDays < 40 ||
		len(got.Cells) != 2 || got.HolmP > .05 || got.BYQ > .10 {
		t.Fatalf("strict positive monitoring fixture overclaimed or lost its diagnostics: %+v", got)
	}
	if !report.TerminalOnlyMonitoring || report.PreregisteredFreezePresent ||
		!strings.Contains(report.InferenceTruth, "not untouched") || !strings.Contains(report.MoneyTruth, "exact scalar void") {
		t.Fatalf("API truth missing: %+v", report)
	}
}
