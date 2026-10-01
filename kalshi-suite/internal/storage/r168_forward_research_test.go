package storage

import (
	"context"
	"strings"
	"testing"
	"time"
)

func r168StorageTestContracts() []ForwardResearchContract {
	return []ForwardResearchContract{
		{ContractID: "r147sc-v1|spotlag|kalshi|yes|taker|k-spot", SystemID: "spotlag",
			Venue: "kalshi", Side: "YES", Action: "BUY", Route: "taker",
			InputTopology: "K-SPOT", ExecutionClass: "delayed_two_touch", Realistic: true},
		{ContractID: "r147sc-v1|book-model|kalshi|no|maker|k", SystemID: "book-model",
			Venue: "kalshi", Side: "NO", Action: "BUY", Route: "maker",
			InputTopology: "K", ExecutionClass: "log_only_book_native_ml", Realistic: false},
	}
}

func r168StorageTestGeneration(id string, opened time.Time,
	contracts []ForwardResearchContract) ForwardResearchGeneration {
	return ForwardResearchGeneration{
		GenerationID:           id,
		DetectorReceiptVersion: "test-detector-receipt-v1",
		EventIdentityVersion:   "test-event-identity-v1",
		OpenedAt:               opened,
		Contracts:              contracts,
	}
}

func TestForwardResearchSchemaMigrationLeavesV1V2ReadableAndImmutable(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	db, err := st.executionShadowHandle()
	if err != nil {
		t.Fatal(err)
	}
	// Reproduce the pre-v3 shape: neither a generation nor its attempt links had versioned
	// detector/identity receipts. The upgrade must add blank legacy columns, never rewrite them.
	const legacySchema = `
CREATE TABLE forward_research_generations (
 generation_id TEXT PRIMARY KEY, manifest_hash TEXT NOT NULL, opened_ts TEXT NOT NULL,
 contract_count INTEGER NOT NULL, cash_authority INTEGER NOT NULL DEFAULT 0, created_ts TEXT NOT NULL
);
CREATE TABLE forward_research_contracts (
 generation_id TEXT NOT NULL, contract_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
 system_id TEXT NOT NULL, venue TEXT NOT NULL, side TEXT NOT NULL, action TEXT NOT NULL,
 route TEXT NOT NULL, input_topology TEXT NOT NULL, execution_class TEXT NOT NULL,
 realistic INTEGER NOT NULL, cash_authority INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(generation_id,contract_id), UNIQUE(generation_id,ordinal)
);
CREATE TABLE forward_research_attempts (
 generation_id TEXT NOT NULL, attempt_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL,
 detector_event_id TEXT NOT NULL UNIQUE, model_version TEXT NOT NULL,
 trigger_unix_ms INTEGER NOT NULL, event_cluster_id TEXT NOT NULL DEFAULT '',
 book_transport TEXT NOT NULL DEFAULT '', book_generation INTEGER,
 book_subscription_id INTEGER, book_sequence INTEGER, observed_ts TEXT NOT NULL,
 cash_authority INTEGER NOT NULL DEFAULT 0
);`
	if _, err = db.ExecContext(ctx, legacySchema); err != nil {
		t.Fatal(err)
	}
	contracts := r168StorageTestContracts()[:1]
	hash, err := ForwardResearchManifestHash(contracts)
	if err != nil {
		t.Fatal(err)
	}
	opened := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	for _, generationID := range []string{"legacy-forward-v1", "legacy-forward-v2"} {
		if _, err = db.ExecContext(ctx, `INSERT INTO forward_research_generations(
generation_id,manifest_hash,opened_ts,contract_count,cash_authority,created_ts)
VALUES(?,?,?,?,0,?)`, generationID, hash, opened.Format(time.RFC3339Nano), 1,
			opened.Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
		if _, err = db.ExecContext(ctx, `INSERT INTO forward_research_contracts(
generation_id,contract_id,ordinal,system_id,venue,side,action,route,input_topology,
execution_class,realistic,cash_authority) VALUES(?,?,?,?,?,?,?,?,?,?,?,0)`,
			generationID, contracts[0].ContractID, 0, contracts[0].SystemID,
			contracts[0].Venue, contracts[0].Side, contracts[0].Action, contracts[0].Route,
			contracts[0].InputTopology, contracts[0].ExecutionClass,
			contracts[0].Realistic); err != nil {
			t.Fatal(err)
		}
	}
	current := r168StorageTestGeneration("current-forward-v3", opened.Add(time.Hour), contracts)
	if _, err = st.PrepareForwardResearchGeneration(ctx, current); err != nil {
		t.Fatal(err)
	}
	for _, generationID := range []string{"legacy-forward-v1", "legacy-forward-v2"} {
		report, reportErr := st.ForwardResearchReport(ctx, generationID)
		if reportErr != nil {
			t.Fatalf("%s report: %v", generationID, reportErr)
		}
		if report.GenerationID != generationID || report.ContractCount != 1 ||
			len(report.Rows) != 1 || report.DetectorReceiptVersion != "" ||
			report.EventIdentityVersion != "" || report.Gates.GenerationProof ||
			report.Verdict != "NOT_PROVEN" {
			t.Fatalf("%s was rewritten or unreadable: %+v", generationID, report)
		}
	}
	if _, err = db.ExecContext(ctx, `UPDATE forward_research_generations
SET detector_receipt_version='forged' WHERE generation_id='legacy-forward-v2'`); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "immutable") {
		t.Fatalf("legacy generation became mutable after migration: %v", err)
	}
}

func TestForwardResearchManifestIsImmutableAndReportsUnseenFailClosed(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	opened := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	contracts := r168StorageTestContracts()
	generation := r168StorageTestGeneration("test-forward-v1", opened, contracts)
	prepared, err := st.PrepareForwardResearchGeneration(ctx, generation)
	if err != nil {
		t.Fatal(err)
	}
	reversed := []ForwardResearchContract{contracts[1], contracts[0]}
	retryInput := r168StorageTestGeneration(generation.GenerationID, opened.Add(time.Hour), reversed)
	retry, err := st.PrepareForwardResearchGeneration(ctx, retryInput)
	if err != nil {
		t.Fatalf("idempotent prepare: %v", err)
	}
	if prepared.ManifestHash == "" || retry.ManifestHash != prepared.ManifestHash ||
		!retry.OpenedAt.Equal(opened) || retry.CashAuthority {
		t.Fatalf("unstable generation: first=%+v retry=%+v", prepared, retry)
	}
	db, err := st.executionShadowHandle()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `UPDATE forward_research_contracts SET realistic=0
WHERE generation_id=? AND contract_id=?`, generation.GenerationID, contracts[0].ContractID); err == nil ||
		!strings.Contains(err.Error(), "immutable") {
		t.Fatalf("manifest update was not refused: %v", err)
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO forward_research_contracts(
generation_id,contract_id,ordinal,system_id,venue,side,action,route,input_topology,
execution_class,realistic,cash_authority) VALUES(?,?,?,?,?,?,?,?,?,?,?,0)`, generation.GenerationID,
		"extra-contract", 2, "extra", "kalshi", "YES", "BUY", "taker", "K",
		"delayed_two_touch", true); err == nil || !strings.Contains(err.Error(), "sealed") {
		t.Fatalf("late manifest insert was not refused: %v", err)
	}

	oldAt := opened.Add(-time.Second)
	oldAttempt := ExecutionShadowAttempt{AttemptID: "pre-generation-attempt", SignalDecisionID: "pre-generation-decision",
		ObservedAt: oldAt, TriggerUnixMS: oldAt.UnixMilli(), Venue: "kalshi", Ticker: "KXOLD",
		Title: "Old event", Side: "YES", Action: "BUY", SystemID: "spotlag", Route: "taker",
		SignalSource: "test", InputTopology: "K-SPOT", SignalContract: contracts[0].ContractID,
		SignalPrice: .4, QualificationBasis: "pre-generation test", CreatedAt: oldAt}
	if inserted, insertErr := st.InsertExecutionShadowAttempt(ctx, oldAttempt); insertErr != nil || !inserted {
		t.Fatalf("insert pre-generation attempt inserted=%v err=%v", inserted, insertErr)
	}
	oldDetector := ExecutionShadowEvent{EventID: "pre-generation-detector", AttemptID: oldAttempt.AttemptID,
		At: oldAt, ElapsedFromTriggerMS: 0, Stage: "detector", Outcome: "seen",
		Evidence: map[string]any{"forward_generation_id": generation.GenerationID,
			"signal_contract_id": contracts[0].ContractID, "execution_model_version": "test-v1",
			"trigger_unix_ms": oldAt.UnixMilli()}}
	if inserted, appendErr := st.AppendExecutionShadowEvent(ctx, oldDetector); appendErr != nil || !inserted {
		t.Fatalf("append pre-generation detector inserted=%v err=%v", inserted, appendErr)
	}
	var oldLinks int
	if err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM forward_research_attempts
WHERE attempt_id=?`, oldAttempt.AttemptID).Scan(&oldLinks); err != nil || oldLinks != 0 {
		t.Fatalf("pre-generation evidence entered forward cohort: links=%d err=%v", oldLinks, err)
	}

	now := opened.Add(time.Minute)
	attempt := ExecutionShadowAttempt{AttemptID: "forward-attempt-1", SignalDecisionID: "decision-1",
		ObservedAt: now, TriggerUnixMS: now.UnixMilli(), Venue: "kalshi", Ticker: "KXTEST",
		Title: "Test event", Side: "YES", Action: "BUY", SystemID: "spotlag", Route: "taker",
		SignalSource: "test", InputTopology: "K-SPOT", SignalContract: contracts[0].ContractID,
		SignalPrice: .4, QualificationBasis: "forward test", CreatedAt: now}
	if inserted, insertErr := st.InsertExecutionShadowAttempt(ctx, attempt); insertErr != nil || !inserted {
		t.Fatalf("insert attempt inserted=%v err=%v", inserted, insertErr)
	}
	detector := ExecutionShadowEvent{EventID: "forward-detector-1", AttemptID: attempt.AttemptID,
		At: now, ElapsedFromTriggerMS: 0, Stage: "any-immutable-detector-stage", Outcome: "seen",
		Evidence: map[string]any{"forward_generation_id": generation.GenerationID,
			"signal_contract_id": contracts[0].ContractID, "execution_model_version": "test-v1",
			"trigger_unix_ms": now.UnixMilli(), "event_cluster_id": ""}}
	if inserted, appendErr := st.AppendExecutionShadowEvent(ctx, detector); appendErr != nil || !inserted {
		t.Fatalf("append detector inserted=%v err=%v", inserted, appendErr)
	}
	report, err := st.ForwardResearchReport(ctx, generation.GenerationID)
	if err != nil {
		t.Fatal(err)
	}
	if report.ContractCount != 2 || len(report.Rows) != 2 || report.ObservedAttempts != 1 ||
		report.UnseenContracts != 1 || report.PositiveVerdict || report.CashAuthority ||
		report.Verdict != "NOT_PROVEN" || report.Gates.SettlementProof {
		t.Fatalf("unexpected fail-closed report: %+v", report)
	}
	if report.Rows[0].ContractID != contracts[1].ContractID && report.Rows[1].ContractID != contracts[1].ContractID {
		t.Fatalf("log-only manifest row disappeared: %+v", report.Rows)
	}

	g, sub, seq := int64(1), int64(2), int64(3)
	terminalAt := now.Add(time.Second)
	terminal := ExecutionShadowEvent{EventID: "forward-terminal-1", AttemptID: attempt.AttemptID,
		At: terminalAt, ElapsedFromTriggerMS: 1000, Stage: "counterfactual-execution-terminal",
		Outcome: "modeled_fill", ShadowState: "modeled_fill",
		BookSource: "kalshi_ws_full_orderbook:g1:s2:q3", BookGeneration: &g,
		BookSubscriptionID: &sub, BookSequence: &seq}
	if _, err = st.AppendExecutionShadowEvent(ctx, terminal); err != nil {
		t.Fatal(err)
	}
	settleAt, settlement, net := terminalAt.Add(time.Hour), 1.0, .59
	settled := ExecutionShadowEvent{EventID: "forward-settlement-1", AttemptID: attempt.AttemptID,
		At: settleAt, ElapsedFromTriggerMS: int64(time.Hour/time.Millisecond) + 1000,
		Stage: "settlement", Outcome: "settled", SettlementKnown: true,
		SettlementValue: &settlement, SettledAt: settleAt, SettlementSource: "test",
		SettlementHash: "sha256:test", PaperNet: &net}
	if _, err = st.AppendExecutionShadowEvent(ctx, settled); err != nil {
		t.Fatal(err)
	}
	report, err = st.ForwardResearchReport(ctx, generation.GenerationID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Gates.SettlementProof || report.EconomicsRequired != 1 ||
		report.EconomicsKnown != 0 {
		t.Fatalf("Paper net incorrectly proved canonical Shadow economics: %+v", report)
	}
	settled.EventID = "forward-shadow-settlement-1"
	settled.ShadowNet, settled.PaperNet = &net, nil
	if _, err = st.AppendExecutionShadowEvent(ctx, settled); err != nil {
		t.Fatal(err)
	}
	report, err = st.ForwardResearchReport(ctx, generation.GenerationID)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Gates.SettlementProof || report.AttemptsMissingBook != 0 ||
		report.AttemptsMissingSequence != 0 {
		t.Fatalf("final execution/settlement proof not recognized: %+v", report)
	}
}

func TestForwardResearchSettlementProofRequiresEveryAttempt(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	contracts := r168StorageTestContracts()[:1]
	now := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	if _, err = st.PrepareForwardResearchGeneration(ctx,
		r168StorageTestGeneration("test-unsettled-v1", now.Add(-time.Second), contracts)); err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"settled-attempt", "unsettled-attempt"} {
		// Keep the fixtures in distinct 30-second economic episodes. Forward research correctly
		// coalesces repeated callbacks inside one episode, while this test needs two independent
		// attempts to prove that one settlement cannot cover the other.
		at := now.Add(time.Duration(i) * 31 * time.Second)
		attempt := ExecutionShadowAttempt{AttemptID: id, SignalDecisionID: "decision-" + id,
			ObservedAt: at, TriggerUnixMS: at.UnixMilli(), Venue: "kalshi", Ticker: "KX" + id,
			Title: id, Side: "YES", Action: "BUY", SystemID: "spotlag", Route: "taker",
			SignalSource: "test", InputTopology: "K-SPOT", SignalContract: contracts[0].ContractID,
			SignalPrice: .4, QualificationBasis: "forward test", CreatedAt: at}
		if _, err = st.InsertExecutionShadowAttempt(ctx, attempt); err != nil {
			t.Fatal(err)
		}
		event := ExecutionShadowEvent{EventID: "detector-" + id, AttemptID: id, At: at,
			ElapsedFromTriggerMS: 0, Stage: "detector", Outcome: "seen", Evidence: map[string]any{
				"forward_generation_id": "test-unsettled-v1", "signal_contract_id": contracts[0].ContractID,
				"execution_model_version": "test-v1", "trigger_unix_ms": at.UnixMilli()}}
		if _, err = st.AppendExecutionShadowEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	settlement, net := 1.0, .5
	settledAt := now.Add(45 * time.Second)
	if _, err = st.AppendExecutionShadowEvent(ctx, ExecutionShadowEvent{EventID: "one-settlement",
		AttemptID: "settled-attempt", At: settledAt, ElapsedFromTriggerMS: 45000,
		Stage: "settlement", Outcome: "settled", SettlementKnown: true,
		SettlementValue: &settlement, SettledAt: settledAt, SettlementSource: "test",
		SettlementHash: "sha256:one", ShadowNet: &net}); err != nil {
		t.Fatal(err)
	}
	report, err := st.ForwardResearchReport(ctx, "test-unsettled-v1")
	if err != nil {
		t.Fatal(err)
	}
	if report.Settled != 1 || report.ObservedAttempts != 2 || report.Gates.SettlementProof {
		t.Fatalf("partial settlement incorrectly passed: %+v", report)
	}
}

func TestForwardResearchEventClustersRequireVerifiedCurrentIdentityReceipts(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	contracts := r168StorageTestContracts()[:1]
	now := time.Now().UTC().Add(-5 * time.Minute).Truncate(time.Millisecond)
	generation := r168StorageTestGeneration("test-cluster-proof-v1",
		now.Add(-time.Second), contracts)
	if _, err = st.PrepareForwardResearchGeneration(ctx, generation); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name            string
		cluster         string
		verified        bool
		detectorVersion string
		identityVersion string
	}{
		{
			name: "unverified-proxy", cluster: "proxy:same-looking-title", verified: false,
			detectorVersion: generation.DetectorReceiptVersion,
			identityVersion: generation.EventIdentityVersion,
		},
		{
			name: "wrong-detector-version", cluster: "sports:wrong-detector", verified: true,
			detectorVersion: "other-detector-receipt",
			identityVersion: generation.EventIdentityVersion,
		},
		{
			name: "wrong-identity-version", cluster: "sports:wrong-identity", verified: true,
			detectorVersion: generation.DetectorReceiptVersion,
			identityVersion: "other-event-identity",
		},
		{
			name: "exact-current-receipts", cluster: "sports:verified-game", verified: true,
			detectorVersion: generation.DetectorReceiptVersion,
			identityVersion: generation.EventIdentityVersion,
		},
	}
	for i, tc := range cases {
		at := now.Add(time.Duration(i) * 31 * time.Second)
		attemptID := "cluster-proof-" + tc.name
		attempt := ExecutionShadowAttempt{
			AttemptID: attemptID, SignalDecisionID: "decision-" + tc.name,
			ObservedAt: at, TriggerUnixMS: at.UnixMilli(), Venue: "kalshi",
			Ticker: "KX-" + strings.ToUpper(tc.name), Title: tc.name,
			Side: "YES", Action: "BUY", SystemID: "spotlag", Route: "taker",
			SignalSource: "test", InputTopology: "K-SPOT",
			SignalContract: contracts[0].ContractID, SignalPrice: .4,
			QualificationBasis: "cluster receipt proof", CreatedAt: at,
		}
		if inserted, insertErr := st.InsertExecutionShadowAttempt(ctx, attempt); insertErr != nil || !inserted {
			t.Fatalf("%s insert attempt=%v err=%v", tc.name, inserted, insertErr)
		}
		event := ExecutionShadowEvent{
			EventID: "cluster-detector-" + tc.name, AttemptID: attemptID, At: at,
			ElapsedFromTriggerMS: 0, Stage: "detector", Outcome: "seen",
			Evidence: map[string]any{
				"forward_generation_id":            generation.GenerationID,
				"signal_contract_id":               contracts[0].ContractID,
				"execution_model_version":          "test-model",
				"forward_detector_receipt_version": tc.detectorVersion,
				"event_cluster_identity_version":   tc.identityVersion,
				"trigger_unix_ms":                  at.UnixMilli(),
				"event_cluster_id":                 tc.cluster,
				"event_cluster_verified":           tc.verified,
			},
		}
		if inserted, appendErr := st.AppendExecutionShadowEvent(ctx, event); appendErr != nil || !inserted {
			t.Fatalf("%s append detector=%v err=%v", tc.name, inserted, appendErr)
		}
	}
	db, err := st.executionShadowHandle()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, `SELECT attempt_id,event_cluster_id,event_cluster_verified,
detector_receipt_version,event_identity_version FROM forward_research_attempts
WHERE generation_id=? ORDER BY attempt_id`, generation.GenerationID)
	if err != nil {
		t.Fatal(err)
	}
	stored := map[string]struct {
		cluster, detector, identity string
		verified                    bool
	}{}
	for rows.Next() {
		var attemptID string
		var row struct {
			cluster, detector, identity string
			verified                    bool
		}
		if err = rows.Scan(&attemptID, &row.cluster, &row.verified,
			&row.detector, &row.identity); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		stored[attemptID] = row
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		row := stored["cluster-proof-"+tc.name]
		if tc.name == "exact-current-receipts" {
			if row.cluster != tc.cluster || !row.verified {
				t.Fatalf("valid structural identity was not frozen: %+v", row)
			}
		} else if row.cluster != "" || row.verified {
			t.Fatalf("%s untrusted identity survived in attempt contract: %+v", tc.name, row)
		}
		if row.detector != tc.detectorVersion || row.identity != tc.identityVersion {
			t.Fatalf("%s receipt versions were not frozen diagnostically: %+v", tc.name, row)
		}
	}
	report, err := st.ForwardResearchReport(ctx, generation.GenerationID)
	if err != nil {
		t.Fatal(err)
	}
	if report.ObservedAttempts != int64(len(cases)) || report.EventClusters != 1 ||
		report.AttemptsMissingCluster != 3 || report.Rows[0].EventClusters != 1 ||
		report.Rows[0].MissingCluster != 3 || report.Gates.EventProof {
		t.Fatalf("unverified or wrong-version cluster became proof: %+v", report)
	}
}

func TestForwardResearchReportScopesEventsToSelectedGenerationBeforeGrouping(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	contracts := r168StorageTestContracts()[:1]
	now := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	if _, err = st.PrepareForwardResearchGeneration(ctx,
		r168StorageTestGeneration("test-plan-v1", now.Add(-time.Second), contracts)); err != nil {
		t.Fatal(err)
	}
	db, err := st.executionShadowHandle()
	if err != nil {
		t.Fatal(err)
	}
	planRows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+forwardResearchReportSQL,
		"test-plan-v1", "test-plan-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer planRows.Close()
	var plan strings.Builder
	for planRows.Next() {
		var id, parent, unused int
		var detail string
		if err = planRows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(detail)
		plan.WriteByte('\n')
	}
	if err = planRows.Err(); err != nil {
		t.Fatal(err)
	}
	got := strings.ToLower(plan.String())
	if strings.Contains(got, "scan execution_shadow_events") {
		t.Fatalf("report regressed to a full historical event scan:\n%s", plan.String())
	}
	if !strings.Contains(got, "idx_execution_shadow_events_attempt") {
		t.Fatalf("report does not probe events by selected attempt:\n%s", plan.String())
	}
}
