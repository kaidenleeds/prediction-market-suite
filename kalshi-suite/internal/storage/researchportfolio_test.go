package storage

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"
)

func validFrozenEVI(now time.Time) FrozenEVIInput {
	return FrozenEVIInput{TaskID: "repair-feed", Version: 1, InputState: "active",
		SystemID: "flow-direction-integrity", RouteID: "research:collector", CauseIDs: []string{"feed", "flow"},
		Provenance: "immutable experiment review r139", EvidenceHash: strings.Repeat("a", 64),
		EvidenceObserved: now.Add(-time.Hour), ValidUntil: now.Add(24 * time.Hour),
		ProbabilityChangesDecision: .7, DecisionValueDollarsPerDay: 12, CollectionCostDollars: 1,
		ComputeMinutes: 5, APICalls: 10, ComputeBudget: 20, APIBudget: 100,
		ComputeDollarPerMinute: .02, APIDollarPerCall: .001}
}

func validFrozenCause(now time.Time) FrozenCauseExposure {
	return FrozenCauseExposure{ExposureID: "flow-k-maker", Version: 1, InputState: "active",
		SystemID: "flow-direction-integrity", RouteID: "kalshi:maker", Venue: "kalshi",
		CauseIDs: []string{"flow", "event:abc"}, Provenance: "untouched event-day holdout r139",
		EvidenceHash: strings.Repeat("b", 64), EvidenceObserved: now.Add(-time.Hour),
		ValidUntil: now.Add(24 * time.Hour), ReplicationID: "holdout-2026q3",
		LowerBoundMethod:    "clustered one-sided 95% fee-net executable Net/day lower bound",
		ReplicatedUntouched: true, ExecutableFeeNetLowerBound: true,
		NetPerDayLower: 1.25, Capacity: 20, CapitalDollarHoursPerDay: 30}
}

func TestR139FrozenPortfolioInputsRequireSealedPreregisteredInferenceAndRouteEconomics(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx, now := context.Background(), time.Now().UTC()
	report, _, err := st.RunR139ResearchInference(ctx, now)
	if err != nil || report.RunID <= 0 || report.ResultHash == "" {
		t.Fatalf("monitoring inference receipt=%+v err=%v", report, err)
	}
	in := validFrozenEVI(now)
	in.SealedInferenceRunID, in.EvidenceHash = report.RunID, strings.TrimPrefix(report.ResultHash, "sha256:")
	if _, _, err := st.RegisterFrozenEVIInput(ctx, in); err == nil || !strings.Contains(err.Error(), "rolling monitoring") {
		t.Fatalf("monitoring-only inference self-qualified EVI: %v", err)
	}
	missing := in
	missing.SealedInferenceRunID = report.RunID + 999
	if _, _, err := st.RegisterFrozenEVIInput(ctx, missing); err == nil || !strings.Contains(err.Error(), "no immutable inference result") {
		t.Fatalf("missing FK inference self-qualified EVI: %v", err)
	}
	wrongHash := in
	wrongHash.EvidenceHash = strings.Repeat("f", 64)
	if _, _, err := st.RegisterFrozenEVIInput(ctx, wrongHash); err == nil || !strings.Contains(err.Error(), "hash") {
		t.Fatalf("arbitrary EVI evidence hash accepted: %v", err)
	}
	bad := in
	bad.Version, bad.Funded = 2, true
	if _, _, err := st.RegisterFrozenEVIInput(ctx, bad); err == nil {
		t.Fatal("funded EVI input accepted")
	}
	bad = in
	bad.Version, bad.ProbabilityChangesDecision = 2, math.NaN()
	if _, _, err := st.RegisterFrozenEVIInput(ctx, bad); err == nil {
		t.Fatal("non-finite EVI input accepted")
	}

	cause := validFrozenCause(now)
	cause.SealedInferenceRunID, cause.EvidenceHash = report.RunID, strings.TrimPrefix(report.ResultHash, "sha256:")
	cause.SealedRouteEconomicsReceiptID = "missing-sealed-route-receipt"
	if _, _, err := st.RegisterFrozenCauseExposure(ctx, cause); err == nil || !strings.Contains(err.Error(), "rolling monitoring") {
		t.Fatalf("monitoring-only inference self-qualified cause exposure: %v", err)
	}
	point := cause
	point.Version, point.ReplicatedUntouched = 2, false
	if _, _, err := st.RegisterFrozenCauseExposure(ctx, point); err == nil {
		t.Fatal("point-estimate cause input accepted without untouched replication")
	}
	point = cause
	point.Version, point.NetPerDayLower = 2, math.Inf(1)
	if _, _, err := st.RegisterFrozenCauseExposure(ctx, point); err == nil {
		t.Fatal("non-finite route lower bound accepted")
	}
	// The route conversion table itself cannot be populated from a rolling monitor: its DB trigger
	// requires the future sealed status plus gate=1 and candidate=1.
	if _, err := st.db.Exec(`INSERT INTO research_sealed_route_economics(receipt_id,sealed_inference_run_id,
sealed_result_hash,system_id,route_id,venue,net_per_day_lower,capacity,capital_dollar_hours_per_day,
conversion_evidence_hash,created_ts,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, "forged-route", report.RunID, strings.TrimPrefix(report.ResultHash, "sha256:"),
		cause.SystemID, cause.RouteID, cause.Venue, cause.NetPerDayLower, cause.Capacity,
		cause.CapitalDollarHoursPerDay, strings.Repeat("e", 64), now.Format(time.RFC3339Nano)); err == nil ||
		!strings.Contains(err.Error(), "sealed preregistered") {
		t.Fatalf("rolling monitor created sealed route economics: %v", err)
	}
	// Schema-level foreign keys supplement the logical gate validation.
	for table, want := range map[string]string{"research_evi_input_specs": "research_inference_runs",
		"research_cause_exposure_specs": "research_sealed_route_economics"} {
		rows, err := st.db.Query(`PRAGMA foreign_key_list(` + table + `)`)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for rows.Next() {
			var id, seq int
			var refTable, from, to, onUpdate, onDelete, match string
			if err := rows.Scan(&id, &seq, &refTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
				t.Fatal(err)
			}
			found = found || refTable == want
		}
		rows.Close()
		if !found {
			t.Fatalf("%s lacks FK to %s", table, want)
		}
	}
	var funded, paper, live int
	if err := st.db.QueryRow(`SELECT COALESCE(SUM(funded),0),COALESCE(SUM(paper_authority),0),COALESCE(SUM(live_authority),0)
FROM (SELECT funded,paper_authority,live_authority FROM research_evi_input_specs
UNION ALL SELECT funded,paper_authority,live_authority FROM research_cause_exposure_specs)`).Scan(&funded, &paper, &live); err != nil {
		t.Fatal(err)
	}
	if funded != 0 || paper != 0 || live != 0 {
		t.Fatalf("frozen inputs gained authority: %d/%d/%d", funded, paper, live)
	}
}

func TestR139PortfolioManifestRejectsStaleInputsAndIsOrderDeterministic(t *testing.T) {
	now := time.Now().UTC()
	a := validFrozenEVI(now)
	a.SpecHash = strings.Repeat("1", 64)
	b := a
	b.TaskID, b.SpecHash = "second", strings.Repeat("2", 64)
	entriesAB, active, valid := FrozenEVIManifestEntries([]FrozenEVIInput{a, b}, now)
	entriesBA, active2, valid2 := FrozenEVIManifestEntries([]FrozenEVIInput{b, a}, now)
	h1, err := ResearchPortfolioManifestHash("evi-schedule-v1", entriesAB)
	if err != nil {
		t.Fatal(err)
	}
	h2, _ := ResearchPortfolioManifestHash("evi-schedule-v1", entriesBA)
	if h1 != h2 || active != 2 || valid != 2 || active2 != 2 || valid2 != 2 {
		t.Fatalf("manifest not deterministic h1=%s h2=%s counts=%d/%d %d/%d", h1, h2, active, valid, active2, valid2)
	}
	b.ValidUntil = now.Add(-time.Second)
	_, active, valid = FrozenEVIManifestEntries([]FrozenEVIInput{a, b}, now)
	if active != 2 || valid != 1 {
		t.Fatalf("stale frozen task was treated as valid: active=%d valid=%d", active, valid)
	}
	cause := validFrozenCause(now)
	cause.SpecHash = strings.Repeat("3", 64)
	cause.ValidUntil = now.Add(-time.Second)
	entries, active, valid := FrozenCauseManifestEntries([]FrozenCauseExposure{cause}, now)
	if active != 1 || valid != 0 || len(entries) != 1 || !strings.HasPrefix(entries[0], "stale:") {
		t.Fatalf("stale cause qualification=%v %d/%d", entries, active, valid)
	}
}

func TestR139PortfolioRunReceiptIsDeterministicAppendOnly(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	manifest, _ := ResearchPortfolioManifestHash("evi-schedule-v1", []string{"valid:" + strings.Repeat("4", 64)})
	run := ResearchPortfolioRun{ManifestHash: manifest, State: "SCHEDULED",
		InputHashes: []string{"valid:" + strings.Repeat("4", 64)}, InputCount: 1, ValidInputCount: 1,
		ResultJSON: `{"Selected":[{"ID":"a"}]}`, Reason: "frozen deterministic test"}
	if _, inserted, err := st.InsertEVIScheduleRun(ctx, run); err != nil || !inserted {
		t.Fatalf("first receipt inserted=%v err=%v", inserted, err)
	}
	if _, inserted, err := st.InsertEVIScheduleRun(ctx, run); err != nil || inserted {
		t.Fatalf("same receipt inserted=%v err=%v", inserted, err)
	}
	drift := run
	drift.ResultJSON = `{"Selected":[]}`
	if _, _, err := st.InsertEVIScheduleRun(ctx, drift); err == nil || !strings.Contains(err.Error(), "drift") {
		t.Fatalf("same-manifest nondeterminism accepted: %v", err)
	}
	for _, q := range []string{
		`UPDATE research_evi_schedule_runs SET state='WAITING_FOR_FROZEN_INPUTS' WHERE manifest_hash='` + manifest + `'`,
		`DELETE FROM research_evi_schedule_runs WHERE manifest_hash='` + manifest + `'`,
	} {
		if _, err := st.db.Exec(q); err == nil {
			t.Fatalf("portfolio receipt mutation succeeded: %s", q)
		}
	}
}
