package storage

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func canonicalCollectorReceipt(cycle string) CollectorReceipt {
	return CollectorReceipt{CollectorID: "canonical-identity", CycleID: cycle, Status: "healthy",
		Started: time.Now(), Completed: time.Now(), Eligible: 1, Attempted: 1, Inserted: 1,
		Source: "game_identity+market_game structural joins", SchemaVersion: "r138-v1",
		ExpectedCadence: 5 * time.Minute, Systems: ResearchExperimentIDs(), Metrics: map[string]any{}}
}

func TestCollectorReceiptMustMatchImmutableActiveSpec(t *testing.T) {
	st, _ := openTemp(t)
	ctx := context.Background()
	good := canonicalCollectorReceipt("good")
	if inserted, err := st.InsertCollectorReceipt(ctx, good); err != nil || !inserted {
		t.Fatalf("valid receipt inserted=%v err=%v", inserted, err)
	}
	bad := canonicalCollectorReceipt("bad-schema")
	bad.SchemaVersion = "made-up-green-schema"
	if inserted, err := st.InsertCollectorReceipt(ctx, bad); err != nil || !inserted {
		t.Fatalf("mismatch evidence inserted=%v err=%v", inserted, err)
	}
	var status, class, schema string
	if err := st.db.QueryRow(`SELECT status,error_class,schema_version FROM research_collector_receipts
WHERE collector_id='canonical-identity' AND cycle_id='bad-schema'`).Scan(&status, &class, &schema); err != nil {
		t.Fatal(err)
	}
	if status != "error" || class != "collector_contract_mismatch" || schema != "r138-v1" {
		t.Fatalf("forged green receipt persisted as status=%q class=%q schema=%q", status, class, schema)
	}
}

func TestCollectorReceiptZeroAndErrorTruthCannotBeBlank(t *testing.T) {
	st, _ := openTemp(t)
	ctx := context.Background()
	zero := canonicalCollectorReceipt("blank-zero")
	zero.Status, zero.Eligible, zero.Attempted, zero.Inserted = "healthy_empty", 0, 0, 0
	zero.ExpectedZero, zero.ZeroReason = true, ""
	if _, err := st.InsertCollectorReceipt(ctx, zero); err == nil {
		t.Fatal("blank healthy-empty reason was accepted")
	}
	errRow := canonicalCollectorReceipt("blank-error")
	errRow.Status, errRow.Inserted = "error", 0
	if _, err := st.InsertCollectorReceipt(ctx, errRow); err == nil {
		t.Fatal("blank error class/text was accepted")
	}
}

func TestCollectorLivenessReportUsesOneConnectionAndPreservesLifetimeFacts(t *testing.T) {
	st, _ := openTemp(t)
	ctx := context.Background()
	for _, cycle := range []string{"single-connection-1", "single-connection-2"} {
		receipt := canonicalCollectorReceipt(cycle)
		if inserted, err := st.InsertCollectorReceipt(ctx, receipt); err != nil || !inserted {
			t.Fatalf("seed receipt %s inserted=%v err=%v", cycle, inserted, err)
		}
	}

	// Regression for the production WAL pin: an outer report cursor plus a per-collector nested
	// query deadlocked at one connection and could wait behind all eight production connections.
	// A single-statement report must complete even when only one connection exists.
	st.db.SetMaxOpenConns(1)
	st.db.SetMaxIdleConns(1)
	reportCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	report, err := st.CollectorLivenessReport(reportCtx)
	if err != nil {
		t.Fatalf("single-connection report: %v", err)
	}
	rows, ok := report["collectors"].([]CollectorLivenessView)
	if !ok {
		t.Fatalf("collector report type=%T", report["collectors"])
	}
	for _, row := range rows {
		if row.CollectorID != "canonical-identity" {
			continue
		}
		if row.LifetimeCycles != 2 || row.LifetimeInserted != 2 || row.LastSuccessTS == "" || row.LastInsertTS == "" {
			t.Fatalf("lifetime facts not preserved: %+v", row)
		}
		return
	}
	t.Fatal("canonical-identity missing from collector report")
}

func collectorLivenessViewForTest(t *testing.T, st *Store, id string) CollectorLivenessView {
	t.Helper()
	report, err := st.CollectorLivenessReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rows, ok := report["collectors"].([]CollectorLivenessView)
	if !ok {
		t.Fatalf("collector report type=%T", report["collectors"])
	}
	for _, row := range rows {
		if row.CollectorID == id {
			return row
		}
	}
	t.Fatalf("collector %s missing", id)
	return CollectorLivenessView{}
}

func TestCollectorLivenessUsesRealRotatingCadenceWithoutHidingMissedRotations(t *testing.T) {
	for _, id := range []string{"sealed-paper-promotion", "concrete-attention-horizons", "replenishment-toxicity", "step7-event-microstructure"} {
		if got := effectiveCollectorLivenessCadence(id, 5); got != 60 {
			t.Fatalf("%s effective cadence=%v, want 60", id, got)
		}
	}
	newStore := func(t *testing.T, completed time.Time) *Store {
		t.Helper()
		st, _ := openTemp(t)
		if err := st.EnsureR138Step6To9CollectorBlueprints(context.Background()); err != nil {
			t.Fatal(err)
		}
		_, err := st.InsertCollectorReceipt(context.Background(), CollectorReceipt{
			CollectorID: "concrete-attention-horizons", CycleID: completed.Format(time.RFC3339Nano),
			ExperimentID: "attention-spillover-graph", ExperimentVersion: 1,
			Status: "healthy_empty", Started: completed.Add(-time.Second), Completed: completed,
			ExpectedZero: true, ZeroReason: "no fixed-horizon mark due", Exclusions: map[string]int{},
			Source: R139ConcreteAttentionSource, SchemaVersion: "attention-executable-markout-v1",
			ExpectedCadence: 5 * time.Second, Systems: []string{"attention-spillover-graph"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return st
	}

	t.Run("ordinary rotation gap stays healthy", func(t *testing.T) {
		st := newStore(t, time.Now().UTC().Add(-49*time.Second))
		row := collectorLivenessViewForTest(t, st, "concrete-attention-horizons")
		if row.Alert || row.ExpectedCadenceS != 5 || row.EffectiveCadenceS != 60 {
			t.Fatalf("ordinary rotating gap misclassified: %+v", row)
		}
	})
	t.Run("three missed real rotations alert", func(t *testing.T) {
		st := newStore(t, time.Now().UTC().Add(-4*time.Minute))
		row := collectorLivenessViewForTest(t, st, "concrete-attention-horizons")
		if !row.Alert || row.EffectiveCadenceS != 60 {
			t.Fatalf("missed rotations hidden: %+v", row)
		}
	})
}

func TestCollectorLivenessRequiresCurrentBootCompletionAfterBoundedGrace(t *testing.T) {
	st, _ := openTemp(t)
	now := time.Now().UTC()
	old := canonicalCollectorReceipt("preboot-success")
	old.Started, old.Completed = now.Add(-21*time.Minute), now.Add(-20*time.Minute)
	if inserted, err := st.InsertCollectorReceipt(context.Background(), old); err != nil || !inserted {
		t.Fatalf("seed preboot receipt inserted=%v err=%v", inserted, err)
	}

	st.SetCollectorRuntimeStart(now.Add(-time.Minute))
	warming := collectorLivenessViewForTest(t, st, "canonical-identity")
	if warming.Alert || !warming.BootWarming || warming.CompletedThisBoot || !warming.NeverRanThisBoot ||
		warming.Status != "warming" || warming.ReceiptStatus != "healthy" {
		t.Fatalf("preboot receipt masqueraded as current health: %+v", warming)
	}

	// Canonical identity's declared/effective cadence is five minutes, so the bounded boot grace is
	// fifteen minutes. Once that expires, absence of a current-process completion is actionable.
	st.SetCollectorRuntimeStart(now.Add(-16 * time.Minute))
	stale := collectorLivenessViewForTest(t, st, "canonical-identity")
	if !stale.Alert || stale.BootWarming || !stale.NeverRanThisBoot || stale.BootGraceS != 900 ||
		stale.Status != "stale" || stale.ReceiptStatus != "healthy" {
		t.Fatalf("missing current-boot completion stayed hidden: %+v", stale)
	}
}

func TestCollectorLivenessCurrentBootErrorAlwaysAlertsAndSuccessClearsWarmup(t *testing.T) {
	st, _ := openTemp(t)
	now := time.Now().UTC()
	st.SetCollectorRuntimeStart(now.Add(-time.Minute))
	errReceipt := canonicalCollectorReceipt("current-error")
	errReceipt.Started, errReceipt.Completed = now.Add(-time.Second), now
	errReceipt.Status, errReceipt.Inserted = "error", 0
	errReceipt.ErrorClass, errReceipt.ErrorText = "source_api", "authoritative source timed out"
	if inserted, err := st.InsertCollectorReceipt(context.Background(), errReceipt); err != nil || !inserted {
		t.Fatalf("seed current error inserted=%v err=%v", inserted, err)
	}
	errView := collectorLivenessViewForTest(t, st, "canonical-identity")
	if !errView.Alert || errView.BootWarming || !errView.CompletedThisBoot || errView.Status != "error" {
		t.Fatalf("current source error was suppressed by boot grace: %+v", errView)
	}

	good := canonicalCollectorReceipt("current-success")
	good.Started, good.Completed = now.Add(time.Second), now.Add(2*time.Second)
	if inserted, err := st.InsertCollectorReceipt(context.Background(), good); err != nil || !inserted {
		t.Fatalf("seed current success inserted=%v err=%v", inserted, err)
	}
	goodView := collectorLivenessViewForTest(t, st, "canonical-identity")
	if goodView.Alert || goodView.BootWarming || !goodView.CompletedThisBoot || goodView.Status != "healthy" {
		t.Fatalf("current success did not clear runtime state: %+v", goodView)
	}
}

func TestCollectorBootWarmupIsNotAnAlertOrCurrentRuntimeProof(t *testing.T) {
	view := CollectorLivenessView{CollectorID: "proper-score", Status: "warming", ReceiptStatus: "healthy_empty",
		BootWarming: true, NeverRanThisBoot: true, BootGraceS: 900}
	state, reason, blockers := effectiveResearchSystemRuntimeState("COLLECTING", "historical receipt exists",
		[]string{"proper-score"}, map[string]CollectorLivenessView{"proper-score": view})
	if view.Alert || state != "BLOCKED" || len(blockers) != 1 ||
		!strings.Contains(reason, "warming: no completion since process boot") {
		t.Fatalf("warmup became an alert or current runtime proof: state=%s reason=%q blockers=%v", state, reason, blockers)
	}
}

func TestVenueNoticeCollectorUpgradeCoexistsWithImmutableV1(t *testing.T) {
	st, _ := openTemp(t)
	ctx := context.Background()
	legacy := CollectorSpec{CollectorID: "venue-notices", Version: 1,
		Source:          "official Kalshi/Polymarket/PolyUS changelogs, status feeds, maintenance docs, and regulatory notices",
		SchemaVersion:   "venue-notices-r138-v1",
		ZeroPolicy:      "zero new versions is healthy only after every pinned official source returns unchanged or all parsed notices deduplicate; any source/schema error is blocked",
		ExpectedCadence: 15 * time.Minute, Systems: ResearchExperimentIDs()}
	if inserted, err := st.RegisterCollectorSpec(ctx, legacy); err != nil || !inserted {
		t.Fatalf("seed legacy venue watcher inserted=%v err=%v", inserted, err)
	}
	if err := st.EnsureR138CollectorBlueprints(ctx); err != nil {
		t.Fatalf("v2 watcher contract did not migrate beside immutable v1: %v", err)
	}
	current, err := st.currentCollectorSpec(ctx, "venue-notices")
	if err != nil || current.Version != 2 || current.SchemaVersion != "venue-notices-r138-v2" {
		t.Fatalf("current watcher spec=%+v err=%v", current, err)
	}
}

func TestProperScoreCollectorUpgradeCoexistsWithImmutableV1(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "legacy.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE research_collector_specs (
 collector_id TEXT NOT NULL, version INTEGER NOT NULL, spec_hash TEXT NOT NULL,
 experiment_id TEXT NOT NULL DEFAULT '', experiment_version INTEGER NOT NULL DEFAULT 0,
 expected_cadence_s REAL NOT NULL, source TEXT NOT NULL, schema_version TEXT NOT NULL,
 systems_json TEXT NOT NULL DEFAULT '[]', zero_policy TEXT NOT NULL,
 active INTEGER NOT NULL DEFAULT 1, created_ts TEXT NOT NULL,
 PRIMARY KEY(collector_id,version), UNIQUE(collector_id,spec_hash))`); err != nil {
		t.Fatal(err)
	}
	st := &Store{db: db}
	ctx := context.Background()
	legacy := CollectorSpec{CollectorID: "proper-score", Version: 1,
		ExperimentID: "proper-score-executor", ExperimentVersion: 1,
		Source:          "ml_predictions book-native-v2 all_scored + current complete venue books",
		SchemaVersion:   "r138-book-native-v2",
		ZeroPolicy:      "zero is healthy only when source forecasts are absent or every row has an explicit exclusion",
		ExpectedCadence: 5 * time.Minute, Systems: []string{"proper-score-executor", "forecast-persona-router"}}
	if inserted, err := st.RegisterCollectorSpec(ctx, legacy); err != nil || !inserted {
		t.Fatalf("seed legacy proper-score inserted=%v err=%v", inserted, err)
	}
	if err := st.EnsureR138CollectorBlueprints(ctx); err != nil {
		t.Fatalf("v2 proper-score contract did not migrate beside immutable v1: %v", err)
	}
	current, err := st.currentCollectorSpec(ctx, "proper-score")
	if err != nil || current.Version != 2 || current.SchemaVersion != "proper-score-paper-v2" {
		t.Fatalf("current proper-score spec=%+v err=%v", current, err)
	}
}

func TestR139NewCollectorContractsAreRegistered(t *testing.T) {
	st, _ := openTemp(t)
	ctx := context.Background()
	if err := st.EnsureR138Step6To9CollectorBlueprints(ctx); err != nil {
		t.Fatal(err)
	}
	want := map[string]struct {
		source  string
		cadence time.Duration
	}{
		"cross-venue-rule-artifacts":  {R139CrossVenueRuleArtifactSource, 5 * time.Minute},
		"sealed-paper-promotion":      {R139SealedPaperPromotionSource, 5 * time.Second},
		"step7-event-microstructure":  {R139Step7EventMicrostructureSource, 5 * time.Second},
		"step7-maker-salvage":         {R139Step7MakerSalvageSource, time.Minute},
		"concrete-attention-horizons": {R139ConcreteAttentionSource, 5 * time.Second},
	}
	for id, expected := range want {
		spec, err := st.currentCollectorSpec(ctx, id)
		if err != nil || spec.Source != expected.source || spec.ExpectedCadence != expected.cadence {
			t.Fatalf("%s spec=%+v err=%v", id, spec, err)
		}
	}
}
