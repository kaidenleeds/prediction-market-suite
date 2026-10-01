package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

type researchPromotionFreshnessFixture struct {
	proof     ResearchPromotionProof
	candidate ResearchPromotionCandidate
}

func researchPromotionFreshnessHash(label string) string {
	sum := sha256.Sum256([]byte(label))
	return hex.EncodeToString(sum[:])
}

func insertResearchPromotionFreshnessFixture(t *testing.T, st *Store,
	pipeline int, suffix string) researchPromotionFreshnessFixture {
	t.Helper()
	ctx := context.Background()
	system, cohort, venue, route := "proper-score-executor", "freshness-"+suffix, "kalshi", "taker"
	now := time.Now().UTC()
	start, end := now.Add(-72*time.Hour), now.Add(-24*time.Hour)
	observed := start.Add(time.Hour)
	currentVersion, _, experimentSpecHash, inferenceContractHash, err :=
		st.currentInferenceContract(ctx, system)
	if err != nil {
		t.Fatal(err)
	}

	eventID, ticker := "promotion-freshness-event-"+suffix, "KXFRESH"+strings.ToUpper(suffix)
	eventVersion, payoffID, payoffVersion := registerR138EvidenceInstrument(t, st, eventID, venue, ticker)
	row := r138EvidenceFixture("candidate", true)
	row.Observed, row.SystemID, row.OpportunityID = observed, system, "promotion-freshness-"+suffix
	row.Cohort, row.CanonicalEventID, row.EventVersion = cohort, eventID, eventVersion
	row.CanonicalPayoffID, row.PayoffVersion = payoffID, payoffVersion
	row.Venue, row.Ticker, row.Route = venue, ticker, route
	observationID, inserted, err := st.InsertResearchSystemObservation(ctx, row)
	if err != nil || !inserted {
		t.Fatalf("fixture observation inserted=%v err=%v", inserted, err)
	}

	projectionManifest, err := st.Step7ProjectionCompletionManifest(ctx, end)
	if err != nil {
		t.Fatal(err)
	}
	payoffManifest, err := researchPayoffUpdateManifest(ctx, st.db, ResearchInferencePreregistration{
		SystemID: system, ExperimentVersion: currentVersion, Cohort: cohort, Venue: venue, Route: route,
		UntouchedStart: start, UntouchedEnd: end,
	}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	inputManifest := map[string]any{
		"preregistration_id":          "promotion-freshness-prereg-" + suffix,
		"projection_completed_count":  projectionManifest.Count,
		"projection_max_completed_ts": projectionManifest.MaxCompletedTS,
		"projection_manifest_hash":    projectionManifest.Hash,
		"payoff_observation_count":    payoffManifest.ObservationCount,
		"payoff_update_count":         payoffManifest.UpdateCount,
		"payoff_update_max_id":        payoffManifest.MaxUpdateID,
		"payoff_update_manifest_hash": payoffManifest.Hash,
	}
	inputManifestJSON, err := json.Marshal(inputManifest)
	if err != nil {
		t.Fatal(err)
	}
	inputManifestHash, err := r139Hash(inputManifest)
	if err != nil {
		t.Fatal(err)
	}
	preregistrationID := "promotion-freshness-prereg-" + suffix
	resultHash := researchPromotionFreshnessHash("result-" + suffix)
	specHash := researchPromotionFreshnessHash("prereg-spec-" + suffix)
	manifestHash := func(name string) string {
		return researchPromotionFreshnessHash(name + "-" + suffix)
	}
	_, err = st.db.ExecContext(ctx, `INSERT INTO research_inference_preregistrations(
preregistration_id,created_ts,system_id,experiment_version,experiment_spec_hash,cohort,venue,route,
train_validation_cutoff_ts,untouched_start_ts,untouched_end_ts,seal_at_ts,embargo_seconds,
required_days,required_events,pipeline_version,code_manifest_hash,data_manifest_hash,
source_manifest_hash,inference_contract_hash,frozen_input_manifest_hash,spec_hash)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		preregistrationID, start.Add(-72*time.Hour).Format(time.RFC3339Nano), system, currentVersion,
		experimentSpecHash, cohort, venue, route, start.Add(-48*time.Hour).Format(time.RFC3339Nano),
		start.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano),
		86400, 2, 2, pipeline, manifestHash("code"), manifestHash("data"), manifestHash("source"),
		inferenceContractHash, manifestHash("frozen-input"), specHash)
	if err != nil {
		t.Fatal(err)
	}
	res, err := st.db.ExecContext(ctx, `INSERT INTO research_inference_runs(
created_ts,as_of_completed_ts,pipeline_version,input_manifest_hash,input_manifest_json,result_hash,
preregistration_id,status,observed_rows,exact_terminal_rows,excluded_open,excluded_void,
excluded_censored,excluded_nonexact,excluded_identity,excluded_route_truth,systems_reported)
VALUES(?,?,?,?,?,?,?,'sealed_preregistered_untouched',1,1,0,0,0,0,0,0,19)`,
		now.Add(-time.Hour).Format(time.RFC3339Nano), end.Format(time.RFC3339Nano), pipeline,
		inputManifestHash, string(inputManifestJSON), resultHash, preregistrationID)
	if err != nil {
		t.Fatal(err)
	}
	runID, err := res.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.db.ExecContext(ctx, `INSERT INTO research_inference_system_results(
run_id,system_id,experiment_version,state,reason,terminal_rows,train_events,validation_events,
monitoring_events,purged_events,validation_days,validation_mean,validation_lower,validation_upper,
validation_bounds_known,monitoring_days,monitoring_mean,monitoring_lower,monitoring_upper,
monitoring_bounds_known,raw_p,holm_p,by_q,monitoring_lower_bound_positive,
preregistered_untouched_gate_pass,cells_json,untouched_events,untouched_days,untouched_mean,
untouched_lower,untouched_upper,untouched_p,untouched_bounds_known,execution_candidate)
VALUES(?,?,?,'PREREGISTERED_UNTOUCHED_PASS','fixture sealed pass',1,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,.001,.001,.001,0,1,'[]',2,2,.10,.05,.15,.001,1,1)`,
		runID, system, currentVersion)
	if err != nil {
		t.Fatal(err)
	}

	routeEconomicsID := "promotion-freshness-route-" + suffix
	_, err = st.db.ExecContext(ctx, `INSERT INTO research_sealed_route_economics(
receipt_id,sealed_inference_run_id,sealed_result_hash,system_id,route_id,venue,net_per_day_lower,
capacity,capital_dollar_hours_per_day,conversion_evidence_hash,created_ts,
funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, routeEconomicsID, runID, resultHash, system, route, venue,
		.02, 1.0, 24.0, manifestHash("conversion"), now.Add(-time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	cause, causeInserted, err := st.RegisterFrozenCauseExposure(ctx, FrozenCauseExposure{
		ExposureID: "promotion-freshness-cause-" + suffix, InputState: "active",
		SystemID: system, RouteID: route, Venue: venue, CauseIDs: []string{"freshness-fixture"},
		Provenance: "promotion freshness fixture", EvidenceHash: resultHash,
		SealedInferenceRunID: runID, SealedRouteEconomicsReceiptID: routeEconomicsID,
		EvidenceObserved: now.Add(-time.Hour), ValidUntil: now.Add(365 * 24 * time.Hour),
		ReplicationID:    "promotion-freshness-replication-" + suffix,
		LowerBoundMethod: "fixture deterministic bound", ReplicatedUntouched: true,
		ExecutableFeeNetLowerBound: true, NetPerDayLower: .02, Capacity: 1,
		CapitalDollarHoursPerDay: 24, Version: 1,
	})
	if err != nil || !causeInserted {
		t.Fatalf("cause inserted=%v row=%+v err=%v", causeInserted, cause, err)
	}
	entries := []string{"valid:" + cause.SpecHash}
	causeGraphHash, err := ResearchPortfolioManifestHash("cause-graph-v1", entries)
	if err != nil {
		t.Fatal(err)
	}
	_, graphInserted, err := st.InsertCauseGraphRun(ctx, ResearchPortfolioRun{
		ManifestHash: causeGraphHash, Observed: now.Add(-time.Hour), State: "READY",
		InputHashes: entries, InputCount: 1, ValidInputCount: 1,
		ResultJSON: `[{"Systems":["proper-score-executor"],"ConservativeSharedCauseNetPerDay":0.02}]`,
		Reason:     "promotion freshness fixture",
	})
	if err != nil || !graphInserted {
		t.Fatalf("cause graph inserted=%v err=%v", graphInserted, err)
	}

	proof := ResearchPromotionProof{
		RunID: runID, ResultHash: resultHash, PreregistrationID: preregistrationID,
		SystemID: system, ExperimentVersion: int64(currentVersion), Cohort: cohort,
		Venue: venue, Route: route, Created: now.Add(-time.Hour),
		UntouchedStart: start, UntouchedEnd: end, ObservedRows: 1,
		MeanAllIn: .41, MeanPC: .10, LowerPC: .05,
		Governance: ResearchPromotionGovernance{
			RouteEconomicsReceiptID: routeEconomicsID, CauseExposureID: cause.ExposureID,
			CauseExposureVersion: cause.Version, CauseSpecHash: cause.SpecHash,
			CauseGraphManifestHash: causeGraphHash, NetPerDayLower: .02,
			Capacity: 1, CapitalDollarHoursPerDay: 24,
		},
	}
	candidate := ResearchPromotionCandidate{
		ObservationID: observationID, ExperimentVersion: int64(currentVersion),
		Observed: observed, SystemID: system, Cohort: cohort, Venue: venue, Route: route,
		Ticker: ticker, Title: ticker, Side: "YES", CertificateHash: row.CertificateHash,
		CanonicalEventID: eventID, EventVersion: eventVersion,
		CanonicalPayoffID: payoffID, PayoffVersion: payoffVersion,
		ObservedPrice: .40, ObservedFeePC: .01, VisibleCapacity: 10, ExpectedNetLower: .09,
	}
	return researchPromotionFreshnessFixture{proof: proof, candidate: candidate}
}

func TestR174PromotionRejectsLegacyPipelineProofs(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	fixture := insertResearchPromotionFreshnessFixture(t, st, 3, "pipeline-v3")
	if fresh, reason, err := st.researchPromotionProofFresh(ctx, fixture.proof); err != nil ||
		fresh || !strings.Contains(reason, "pipeline is v3/v3") {
		t.Fatalf("legacy pipeline freshness=%v reason=%q err=%v", fresh, reason, err)
	}
	if proofs, err := st.CurrentResearchPromotionProofs(ctx); err != nil || len(proofs) != 0 {
		t.Fatalf("legacy pipeline proof enumerated: proofs=%+v err=%v", proofs, err)
	}
	if _, inserted, err := st.InsertResearchPromotionIntent(ctx, fixture.proof,
		fixture.candidate, .40, .01, .005); err == nil || inserted ||
		!strings.Contains(err.Error(), "pipeline is v3/v3") {
		t.Fatalf("legacy pipeline intent inserted=%v err=%v", inserted, err)
	}
}

func TestR174PromotionRejectsSupersededExperimentProofs(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	fixture := insertResearchPromotionFreshnessFixture(t, st, R139InferencePipelineVersion, "old-experiment")
	if fresh, reason, err := st.researchPromotionProofFresh(ctx, fixture.proof); err != nil || !fresh {
		t.Fatalf("current proof was not fresh before version bump: fresh=%v reason=%q err=%v", fresh, reason, err)
	}
	var next ResearchExperimentSpec
	for _, spec := range r137ExperimentBlueprints() {
		if spec.ExperimentID == fixture.proof.SystemID {
			next = spec
			break
		}
	}
	if next.ExperimentID == "" {
		t.Fatal("proper-score experiment blueprint missing")
	}
	next.Version = int(fixture.proof.ExperimentVersion) + 1
	next.Hypothesis += " Superseding fixture contract."
	if inserted, err := st.RegisterExperimentSpec(ctx, next); err != nil || !inserted {
		t.Fatalf("superseding experiment inserted=%v err=%v", inserted, err)
	}
	if fresh, reason, err := st.researchPromotionProofFresh(ctx, fixture.proof); err != nil ||
		fresh || !strings.Contains(reason, fmt.Sprintf("current v%d", next.Version)) {
		t.Fatalf("superseded experiment freshness=%v reason=%q err=%v", fresh, reason, err)
	}
	if proofs, err := st.CurrentResearchPromotionProofs(ctx); err != nil || len(proofs) != 0 {
		t.Fatalf("superseded experiment proof enumerated: proofs=%+v err=%v", proofs, err)
	}
	if _, inserted, err := st.InsertResearchPromotionIntent(ctx, fixture.proof,
		fixture.candidate, .40, .01, .005); err == nil || inserted ||
		!strings.Contains(err.Error(), "no longer the current") {
		t.Fatalf("superseded experiment intent inserted=%v err=%v", inserted, err)
	}
}

func TestR174PromotionManifestDriftBlocksNewIntentButPreservesSubmittedReconciliation(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	insertResearchPromotionFreshnessFixture(t, st, R139InferencePipelineVersion, "manifest-drift")
	proofs, err := st.CurrentResearchPromotionProofs(ctx)
	if err != nil || len(proofs) != 1 {
		t.Fatalf("current promotion proofs=%+v err=%v", proofs, err)
	}
	proof := proofs[0]
	candidate, found, err := st.LatestResearchPromotionCandidate(ctx, proof, proof.UntouchedStart)
	if err != nil || !found {
		t.Fatalf("current promotion candidate=%+v found=%v err=%v", candidate, found, err)
	}
	intent, inserted, err := st.InsertResearchPromotionIntent(ctx, proof, candidate, .40, .01, .005)
	if err != nil || !inserted {
		t.Fatalf("current intent inserted=%v err=%v", inserted, err)
	}
	if err := st.AppendResearchPromotionEvent(ctx, intent, "paper_accepted", proof.Route,
		.40, .01, "freshness fixture Paper fill"); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendResearchPromotionEvent(ctx, intent, "live_dispatched", proof.Route,
		.40, .01, "freshness fixture LIVE submit"); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendResearchLiveExecutionReceipt(ctx, intent, ResearchLiveExecutionReceipt{
		State: "pending", OrderID: "freshness-pending-order", RequestedQty: 1, RemainingQty: 1,
		AveragePrice: .40, ReceiptSource: "venue-submit-ack",
	}); err != nil {
		t.Fatal(err)
	}

	lateRaw := step7ProjectionTestFlow("promotion-manifest-drift", proof.UntouchedStart.Add(2*time.Hour))
	lateJob, err := NewStep7FlowProjectionJob(lateRaw, 1, "no-qualified-observation", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, jobs, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{lateRaw}, []Step7ProjectionJob{lateJob}); err != nil || jobs != 1 {
		t.Fatalf("late zero-output projection jobs=%d err=%v", jobs, err)
	}
	if fresh, reason, err := st.researchPromotionProofFresh(ctx, proof); err != nil ||
		fresh || !strings.Contains(reason, "manifest changed") {
		t.Fatalf("manifest-drift freshness=%v reason=%q err=%v", fresh, reason, err)
	}
	if proofs, err := st.CurrentResearchPromotionProofs(ctx); err != nil || len(proofs) != 0 {
		t.Fatalf("manifest-stale proof enumerated: proofs=%+v err=%v", proofs, err)
	}
	if _, inserted, err := st.InsertResearchPromotionIntent(ctx, proof, candidate,
		.40, .01, .005); err == nil || inserted || !strings.Contains(err.Error(), "manifest changed") {
		t.Fatalf("manifest-stale intent inserted=%v err=%v", inserted, err)
	}
	pending, err := st.PendingResearchLiveExecutions(ctx, 10)
	if err != nil || len(pending) != 1 || pending[0].Intent.IntentID != intent.IntentID ||
		pending[0].Receipt.OrderID != "freshness-pending-order" {
		t.Fatalf("submitted-order reconciliation was lost after proof drift: pending=%+v err=%v",
			pending, err)
	}
}

func TestR174PromotionRejectsPendingProjectionBeforeUntouchedWindow(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	fixture := insertResearchPromotionFreshnessFixture(t, st,
		R139InferencePipelineVersion, "prewindow-pending")
	if fresh, reason, err := st.researchPromotionProofFresh(ctx, fixture.proof); err != nil || !fresh {
		t.Fatalf("fixture proof not fresh: fresh=%v reason=%q err=%v", fresh, reason, err)
	}
	oldRaw := step7ProjectionTestFlow("promotion-prewindow-pending",
		fixture.proof.UntouchedStart.Add(-time.Hour))
	oldJob, err := NewStep7FlowProjectionJob(oldRaw, 1, "",
		[]Step7FrozenObservation{{Observation: step7ProjectionObserver(
			"promotion-prewindow-pending", oldRaw.Observed)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, jobs, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{oldRaw}, []Step7ProjectionJob{oldJob}); err != nil || jobs != 1 {
		t.Fatalf("pre-window pending projection jobs=%d err=%v", jobs, err)
	}
	if pending, err := st.Step7ProjectionPendingInWindow(ctx, fixture.proof.UntouchedStart,
		fixture.proof.UntouchedEnd); err != nil || pending != 0 {
		t.Fatalf("fixture pending unexpectedly lies in untouched-only window: pending=%d err=%v",
			pending, err)
	}
	if pending, err := st.Step7ProjectionPendingInWindow(ctx, time.Time{},
		fixture.proof.UntouchedEnd); err != nil || pending != 1 {
		t.Fatalf("all-history completion manifest missed old pending raw: pending=%d err=%v",
			pending, err)
	}
	if proofs, err := st.CurrentResearchPromotionProofs(ctx); err != nil || len(proofs) != 0 {
		t.Fatalf("proof enumerated over old pending raw: proofs=%+v err=%v", proofs, err)
	}
	if _, inserted, err := st.InsertResearchPromotionIntent(ctx, fixture.proof,
		fixture.candidate, .40, .01, .005); err == nil || inserted ||
		!strings.Contains(err.Error(), "all-history manifest") {
		t.Fatalf("intent admitted old pending raw inserted=%v err=%v", inserted, err)
	}
}

func TestR174PromotionIntentRejectsCallerEconomicsMutation(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	insertResearchPromotionFreshnessFixture(t, st,
		R139InferencePipelineVersion, "economics-mutation")
	proofs, err := st.CurrentResearchPromotionProofs(ctx)
	if err != nil || len(proofs) != 1 {
		t.Fatalf("current proof=%+v err=%v", proofs, err)
	}
	proof := proofs[0]
	candidate, found, err := st.LatestResearchPromotionCandidate(ctx, proof, proof.UntouchedStart)
	if err != nil || !found {
		t.Fatalf("current candidate=%+v found=%v err=%v", candidate, found, err)
	}
	proofMutations := []struct {
		name   string
		mutate func(*ResearchPromotionProof)
	}{
		{"lower", func(p *ResearchPromotionProof) { p.LowerPC += .10 }},
		{"mean", func(p *ResearchPromotionProof) { p.MeanPC += .10 }},
		{"all-in", func(p *ResearchPromotionProof) { p.MeanAllIn -= .10 }},
		{"rows", func(p *ResearchPromotionProof) { p.ObservedRows++ }},
	}
	for _, tc := range proofMutations {
		t.Run("proof-"+tc.name, func(t *testing.T) {
			changed := proof
			tc.mutate(&changed)
			if _, inserted, err := st.InsertResearchPromotionIntent(ctx, changed, candidate,
				.40, .01, .005); err == nil || inserted ||
				!strings.Contains(err.Error(), "proof economics differ") {
				t.Fatalf("mutated proof inserted=%v err=%v", inserted, err)
			}
		})
	}
	candidateMutations := []struct {
		name   string
		mutate func(*ResearchPromotionCandidate)
	}{
		{"price", func(c *ResearchPromotionCandidate) { c.ObservedPrice += .01 }},
		{"fee", func(c *ResearchPromotionCandidate) { c.ObservedFeePC += .01 }},
		{"capacity", func(c *ResearchPromotionCandidate) { c.VisibleCapacity++ }},
		{"certificate", func(c *ResearchPromotionCandidate) {
			c.CertificateHash = researchPromotionFreshnessHash("forged-certificate")
		}},
	}
	for _, tc := range candidateMutations {
		t.Run("candidate-"+tc.name, func(t *testing.T) {
			changed := candidate
			tc.mutate(&changed)
			if _, inserted, err := st.InsertResearchPromotionIntent(ctx, proof, changed,
				.40, .01, .005); err == nil || inserted ||
				!strings.Contains(err.Error(), "candidate economics differ") {
				t.Fatalf("mutated candidate inserted=%v err=%v", inserted, err)
			}
		})
	}
	if _, inserted, err := st.InsertResearchPromotionIntent(ctx, proof, candidate,
		.40, .01, .005); err != nil || !inserted {
		t.Fatalf("authoritative unchanged intent inserted=%v err=%v", inserted, err)
	}
}

func TestR174NewDispatchFreshnessIsSeparateFromSubmittedReconciliation(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	insertResearchPromotionFreshnessFixture(t, st,
		R139InferencePipelineVersion, "dispatch-manifest")
	proofs, err := st.CurrentResearchPromotionProofs(ctx)
	if err != nil || len(proofs) != 1 {
		t.Fatalf("current proof=%+v err=%v", proofs, err)
	}
	proof := proofs[0]
	candidate, found, err := st.LatestResearchPromotionCandidate(ctx, proof, proof.UntouchedStart)
	if err != nil || !found {
		t.Fatalf("candidate=%+v found=%v err=%v", candidate, found, err)
	}
	intent, inserted, err := st.InsertResearchPromotionIntent(ctx, proof, candidate, .40, .01, .005)
	if err != nil || !inserted {
		t.Fatalf("intent inserted=%v err=%v", inserted, err)
	}
	if err := st.AppendResearchPromotionEvent(ctx, intent, "paper_accepted", proof.Route,
		.40, .01, "dispatch freshness fixture"); err != nil {
		t.Fatal(err)
	}
	if fresh, reason, err := st.ResearchPromotionIntentFreshForNewDispatch(ctx, intent); err != nil ||
		!fresh || reason != "" {
		t.Fatalf("current accepted intent fresh=%v reason=%q err=%v", fresh, reason, err)
	}

	lateRaw := step7ProjectionTestFlow("dispatch-manifest-drift",
		proof.UntouchedStart.Add(3*time.Hour))
	lateJob, err := NewStep7FlowProjectionJob(lateRaw, 1, "no-qualified-observation", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, jobs, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{lateRaw}, []Step7ProjectionJob{lateJob}); err != nil || jobs != 1 {
		t.Fatalf("late completion jobs=%d err=%v", jobs, err)
	}
	if fresh, reason, err := st.ResearchPromotionIntentFreshForNewDispatch(ctx, intent); err != nil ||
		fresh || !strings.Contains(reason, "manifest changed") {
		t.Fatalf("stale intent fresh=%v reason=%q err=%v", fresh, reason, err)
	}
	if accepted, ok, err := st.AcceptedResearchPromotionIntent(ctx, intent.SourceID); err != nil ||
		!ok || accepted.IntentID != intent.IntentID {
		t.Fatalf("stale proof hid durable accepted intent: accepted=%+v ok=%v err=%v",
			accepted, ok, err)
	}
}

func TestR174NewDispatchRejectsPendingBeforeUntouchedWindow(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	insertResearchPromotionFreshnessFixture(t, st,
		R139InferencePipelineVersion, "dispatch-prewindow")
	proofs, err := st.CurrentResearchPromotionProofs(ctx)
	if err != nil || len(proofs) != 1 {
		t.Fatalf("current proof=%+v err=%v", proofs, err)
	}
	proof := proofs[0]
	candidate, found, err := st.LatestResearchPromotionCandidate(ctx, proof, proof.UntouchedStart)
	if err != nil || !found {
		t.Fatalf("candidate=%+v found=%v err=%v", candidate, found, err)
	}
	intent, inserted, err := st.InsertResearchPromotionIntent(ctx, proof, candidate, .40, .01, .005)
	if err != nil || !inserted {
		t.Fatalf("intent inserted=%v err=%v", inserted, err)
	}
	if err := st.AppendResearchPromotionEvent(ctx, intent, "paper_accepted", proof.Route,
		.40, .01, "dispatch pending fixture"); err != nil {
		t.Fatal(err)
	}
	oldRaw := step7ProjectionTestFlow("dispatch-prewindow-pending",
		proof.UntouchedStart.Add(-time.Hour))
	oldJob, err := NewStep7FlowProjectionJob(oldRaw, 1, "",
		[]Step7FrozenObservation{{Observation: step7ProjectionObserver(
			"dispatch-prewindow-pending", oldRaw.Observed)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, jobs, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{oldRaw}, []Step7ProjectionJob{oldJob}); err != nil || jobs != 1 {
		t.Fatalf("old pending jobs=%d err=%v", jobs, err)
	}
	if fresh, reason, err := st.ResearchPromotionIntentFreshForNewDispatch(ctx, intent); err != nil ||
		fresh || !strings.Contains(reason, "all-history manifest") {
		t.Fatalf("pending-old intent fresh=%v reason=%q err=%v", fresh, reason, err)
	}
}

func TestR174PromotionIntentRejectsSupersededCauseVersion(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	insertResearchPromotionFreshnessFixture(t, st,
		R139InferencePipelineVersion, "superseded-cause")
	proofs, err := st.CurrentResearchPromotionProofs(ctx)
	if err != nil || len(proofs) != 1 {
		t.Fatalf("current proof=%+v err=%v", proofs, err)
	}
	proof := proofs[0]
	candidate, found, err := st.LatestResearchPromotionCandidate(ctx, proof, proof.UntouchedStart)
	if err != nil || !found {
		t.Fatalf("current candidate=%+v found=%v err=%v", candidate, found, err)
	}
	newer, inserted, err := st.RegisterFrozenCauseExposure(ctx, FrozenCauseExposure{
		ExposureID: proof.Governance.CauseExposureID, InputState: "active",
		SystemID: proof.SystemID, RouteID: proof.Route, Venue: proof.Venue,
		CauseIDs: []string{"freshness-fixture"}, Provenance: "superseding cause fixture",
		EvidenceHash:                  strings.TrimPrefix(proof.ResultHash, "sha256:"),
		SealedInferenceRunID:          proof.RunID,
		SealedRouteEconomicsReceiptID: proof.Governance.RouteEconomicsReceiptID,
		EvidenceObserved:              time.Now().UTC().Add(-time.Minute),
		ValidUntil:                    time.Now().UTC().Add(365 * 24 * time.Hour),
		ReplicationID:                 "superseding-cause-replication",
		LowerBoundMethod:              "fixture deterministic bound", ReplicatedUntouched: true,
		ExecutableFeeNetLowerBound: true, NetPerDayLower: proof.Governance.NetPerDayLower,
		Capacity:                 proof.Governance.Capacity,
		CapitalDollarHoursPerDay: proof.Governance.CapitalDollarHoursPerDay,
		Version:                  proof.Governance.CauseExposureVersion + 1,
	})
	if err != nil || !inserted || newer.Version != proof.Governance.CauseExposureVersion+1 {
		t.Fatalf("superseding cause inserted=%v row=%+v err=%v", inserted, newer, err)
	}
	if _, inserted, err := st.InsertResearchPromotionIntent(ctx, proof, candidate,
		.40, .01, .005); err == nil || inserted ||
		!strings.Contains(err.Error(), "current frozen cause") {
		t.Fatalf("superseded cause created intent=%v err=%v", inserted, err)
	}
}

func TestR174PostSealPayoffCorrectionInvalidatesNewIntentAndDispatchOnly(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	insertResearchPromotionFreshnessFixture(t, st,
		R139InferencePipelineVersion, "payoff-correction")
	proofs, err := st.CurrentResearchPromotionProofs(ctx)
	if err != nil || len(proofs) != 1 {
		t.Fatalf("current proof=%+v err=%v", proofs, err)
	}
	proof := proofs[0]
	candidate, found, err := st.LatestResearchPromotionCandidate(ctx, proof, proof.UntouchedStart)
	if err != nil || !found {
		t.Fatalf("current candidate=%+v found=%v err=%v", candidate, found, err)
	}
	intent, inserted, err := st.InsertResearchPromotionIntent(ctx, proof, candidate, .40, .01, .005)
	if err != nil || !inserted {
		t.Fatalf("intent inserted=%v err=%v", inserted, err)
	}
	if err := st.AppendResearchPromotionEvent(ctx, intent, "paper_accepted", proof.Route,
		.40, .01, "payoff correction fixture"); err != nil {
		t.Fatal(err)
	}
	realized := -.41
	if inserted, err := st.AppendResearchPayoffUpdate(ctx, ResearchPayoffUpdate{
		ObservationID: candidate.ObservationID, Observed: time.Now().UTC(), Status: "settled",
		PayoutLower: 0, PayoutUpper: 0, RealizedNet: &realized,
		SourceArtifact: "authoritative post-seal correction",
		SourceHash:     researchPromotionFreshnessHash("post-seal-correction"),
	}); err != nil || !inserted {
		t.Fatalf("post-seal payoff correction inserted=%v err=%v", inserted, err)
	}
	if proofs, err := st.CurrentResearchPromotionProofs(ctx); err != nil || len(proofs) != 0 {
		t.Fatalf("corrected payoff left proof current: proofs=%+v err=%v", proofs, err)
	}
	if _, inserted, err := st.InsertResearchPromotionIntent(ctx, proof, candidate,
		.40, .01, .005); err == nil || inserted ||
		!strings.Contains(err.Error(), "payoff-update manifest changed") {
		t.Fatalf("corrected payoff admitted new intent=%v err=%v", inserted, err)
	}
	if fresh, reason, err := st.ResearchPromotionIntentFreshForNewDispatch(ctx, intent); err != nil ||
		fresh || !strings.Contains(reason, "payoff-update manifest changed") {
		t.Fatalf("corrected payoff admitted new dispatch fresh=%v reason=%q err=%v",
			fresh, reason, err)
	}
	if accepted, ok, err := st.AcceptedResearchPromotionIntent(ctx, intent.SourceID); err != nil ||
		!ok || accepted.IntentID != intent.IntentID {
		t.Fatalf("payoff correction hid durable accepted intent: accepted=%+v ok=%v err=%v",
			accepted, ok, err)
	}
}
