package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestR139PreregisteredUntouchedFreezesFutureWindowAndSealsOnce(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	created := time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC)
	cutoff := created.Add(24 * time.Hour)
	start := cutoff.Add(24 * time.Hour)
	end := start.Add(40 * 24 * time.Hour)
	sealAt := end.Add(2 * 24 * time.Hour)
	h := func(c string) string { return strings.Repeat(c, 64) }
	p, inserted, err := st.registerResearchInferencePreregistrationAt(ctx, ResearchInferencePreregistration{
		PreregistrationID: "proper-score-v1-sealed-fixture", SystemID: "proper-score-executor",
		Cohort: "sealed-fixture", Venue: "kalshi", Route: "taker", TrainValidationCutoff: cutoff,
		UntouchedStart: start, UntouchedEnd: end, SealAt: sealAt, Embargo: 24 * time.Hour,
		RequiredDays: 30, RequiredEvents: 30, CodeManifestHash: h("a"), DataManifestHash: h("b"),
		SourceManifestHash: h("c"),
	}, created)
	if err != nil || !inserted || p.FrozenInputManifestHash == "" || p.InferenceContractHash == "" {
		t.Fatalf("preregister inserted=%v row=%+v err=%v", inserted, p, err)
	}
	repeat := p
	repeat.PreregistrationID = "proper-score-v1-second-look"
	if _, _, err := st.registerResearchInferencePreregistrationAt(ctx, repeat, created.Add(time.Minute)); err == nil ||
		!strings.Contains(err.Error(), "sequential") {
		t.Fatalf("unchanged contract acquired a second sequential look: %v", err)
	}
	if _, err := st.db.Exec(`UPDATE research_inference_preregistrations SET required_days=2 WHERE preregistration_id=?`,
		p.PreregistrationID); err == nil {
		t.Fatal("immutable preregistration allowed update")
	}
	for i := 0; i < 40; i++ {
		observed := start.Add(time.Duration(i)*24*time.Hour + time.Hour)
		eventID := "sealed-event-" + time.Unix(int64(i+1), 0).UTC().Format("150405")
		ticker := "KXSEALED" + time.Unix(int64(i+1), 0).UTC().Format("150405")
		registerR138EvidenceInstrument(t, st, eventID, "kalshi", ticker)
		row := r138EvidenceFixture("candidate", true)
		row.Observed, row.SystemID, row.OpportunityID = observed, p.SystemID, "sealed-opp-"+ticker
		row.Cohort, row.CanonicalEventID, row.EventVersion = p.Cohort, eventID, 0
		row.CanonicalPayoffID, row.PayoffVersion = "", 0
		row.Venue, row.Ticker, row.Route = p.Venue, ticker, p.Route
		id, ok, insertErr := st.InsertResearchSystemObservation(ctx, row)
		if insertErr != nil || !ok {
			t.Fatalf("observation %d inserted=%v err=%v", i, ok, insertErr)
		}
		realized := .20
		if ok, updateErr := st.AppendResearchPayoffUpdate(ctx, ResearchPayoffUpdate{ObservationID: id,
			Observed: observed.Add(12 * time.Hour), Status: "settled", PayoutLower: 1, PayoutUpper: 1,
			RealizedNet: &realized, SourceArtifact: "authoritative fixture settlement",
			SourceHash: h("d") + time.Unix(int64(i), 0).UTC().Format("150405")}); updateErr != nil || !ok {
			// SourceHash need not be SHA-shaped in the payoff ledger, but it must be unique.
			t.Fatalf("payoff %d inserted=%v err=%v", i, ok, updateErr)
		}
	}
	req := PreregisteredUntouchedSealRequest{PreregistrationID: p.PreregistrationID,
		CodeManifestHash: p.CodeManifestHash, DataManifestHash: p.DataManifestHash,
		SourceManifestHash: p.SourceManifestHash, InferenceContractHash: p.InferenceContractHash}
	report, sealed, err := st.sealPreregisteredUntouchedAt(ctx, req, sealAt.Add(time.Hour))
	if err != nil || !sealed || report.Status != "sealed_preregistered_untouched" ||
		!report.PreregisteredFreezePresent || report.PreregistrationID != p.PreregistrationID {
		t.Fatalf("seal inserted=%v report=%+v err=%v", sealed, report, err)
	}
	var target ResearchInferenceSystemResult
	for _, result := range report.Results {
		if result.SystemID == p.SystemID {
			target = result
		}
	}
	if target.State != "PREREGISTERED_UNTOUCHED_PASS" || !target.PreregisteredUntouchedGatePass ||
		!target.ExecutionCandidate || target.UntouchedEvents != 40 || target.UntouchedDays != 40 ||
		target.UntouchedLower <= 0 || target.UntouchedP > .05/19 || target.HolmP > .05 {
		t.Fatalf("untouched target failed strict gate: %+v", target)
	}
	second, sealed, err := st.sealPreregisteredUntouchedAt(ctx, req, sealAt.Add(2*time.Hour))
	if err != nil || sealed || second.RunID != report.RunID {
		t.Fatalf("one-shot seal duplicated: inserted=%v second=%+v err=%v", sealed, second, err)
	}
	// The DB-level EVI bridge normalizes the run's sha256: prefix to its stored 64-hex form.
	if _, err := st.db.Exec(`INSERT INTO research_sealed_route_economics(receipt_id,sealed_inference_run_id,
sealed_result_hash,system_id,route_id,venue,net_per_day_lower,capacity,capital_dollar_hours_per_day,
conversion_evidence_hash,created_ts,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, "sealed-route-fixture", report.RunID,
		strings.TrimPrefix(report.ResultHash, "sha256:"), p.SystemID, "taker-fixture", p.Venue,
		target.UntouchedLower, 1.0, 24.0, h("e"), sealAt.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("sealed EVI route hash/link rejected: %v", err)
	}
	// Promotion additionally needs immutable Net/day/capacity/capital-time conversion, a fresh
	// cause exposure, and a READY cause graph. Collector rows cannot manufacture these inputs.
	if _, err := st.db.Exec(`UPDATE research_sealed_route_economics SET route_id=? WHERE receipt_id=?`,
		p.Route, "sealed-route-fixture"); err == nil {
		t.Fatal("immutable route economics unexpectedly allowed route rewrite")
	}
	// The attempted UPDATE above correctly fails; create the exact route under a separate receipt.
	if _, err := st.db.Exec(`INSERT INTO research_sealed_route_economics(receipt_id,sealed_inference_run_id,
sealed_result_hash,system_id,route_id,venue,net_per_day_lower,capacity,capital_dollar_hours_per_day,
conversion_evidence_hash,created_ts,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, "sealed-route-promotion-fixture", report.RunID,
		strings.TrimPrefix(report.ResultHash, "sha256:"), p.SystemID, p.Route, p.Venue,
		target.UntouchedLower, 1.0, 24.0, h("f"), sealAt.Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("exact promotion route economics rejected: %v", err)
	}
	cause, insertedCause, causeErr := st.RegisterFrozenCauseExposure(ctx, FrozenCauseExposure{
		ExposureID: "sealed-promotion-cause", InputState: "active", SystemID: p.SystemID,
		RouteID: p.Route, Venue: p.Venue, CauseIDs: []string{"forecast-calibration"},
		Provenance: "sealed fixture", EvidenceHash: strings.TrimPrefix(report.ResultHash, "sha256:"),
		SealedInferenceRunID: report.RunID, SealedRouteEconomicsReceiptID: "sealed-route-promotion-fixture",
		EvidenceObserved: sealAt, ValidUntil: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
		ReplicationID: "untouched-fixture", LowerBoundMethod: "deterministic day block",
		ReplicatedUntouched: true, ExecutableFeeNetLowerBound: true,
		NetPerDayLower: target.UntouchedLower, Capacity: 1, CapitalDollarHoursPerDay: 24, Version: 1,
	})
	if causeErr != nil || !insertedCause {
		t.Fatalf("frozen cause inserted=%v row=%+v err=%v", insertedCause, cause, causeErr)
	}
	entries := []string{"valid:" + cause.SpecHash}
	manifest, _ := ResearchPortfolioManifestHash("cause-graph-v1", entries)
	if _, insertedGraph, graphErr := st.InsertCauseGraphRun(ctx, ResearchPortfolioRun{
		ManifestHash: manifest, Observed: sealAt, State: "READY", InputHashes: entries,
		InputCount: 1, ValidInputCount: 1,
		ResultJSON: `[{"Systems":["proper-score-executor"],"ConservativeSharedCauseNetPerDay":0.01}]`,
		Reason:     "fixture frozen cause graph",
	}); graphErr != nil || !insertedGraph {
		t.Fatalf("frozen cause graph inserted=%v err=%v", insertedGraph, graphErr)
	}
	// A later exact candidate may enter the separate Paper bridge, but only under the identical
	// frozen system/version/cohort/venue/route contract and the sealed all-in ceiling.
	currentEvent, currentTicker := "sealed-current-event", "KXSEALEDCURRENT"
	registerR138EvidenceInstrument(t, st, currentEvent, p.Venue, currentTicker)
	current := r138EvidenceFixture("candidate", true)
	current.Observed, current.SystemID, current.OpportunityID = sealAt.Add(2*time.Hour), p.SystemID, "current-candidate"
	current.Cohort, current.CanonicalEventID, current.EventVersion = p.Cohort, currentEvent, 0
	current.CanonicalPayoffID, current.PayoffVersion = "", 0
	current.Venue, current.Ticker, current.Route = p.Venue, currentTicker, p.Route
	current.PayoutLower, current.PayoutUpper = 0, 1
	current.NetLower, current.NetUpper, current.Blocker = -.41, .59, ""
	current.CapacityCurve = []CapacityPoint{{Size: 1, Cost: .40, Fee: .01, PayoutFloor: 0, NetFloor: -.41}}
	currentID, ok, insertErr := st.InsertResearchSystemObservation(ctx, current)
	if insertErr != nil || !ok {
		t.Fatalf("current promotion candidate inserted=%v err=%v", ok, insertErr)
	}
	proofs, proofErr := st.CurrentResearchPromotionProofs(ctx)
	if proofErr != nil || len(proofs) != 1 || proofs[0].SystemID != p.SystemID || proofs[0].LowerPC <= 0 {
		t.Fatalf("promotion proofs=%+v err=%v", proofs, proofErr)
	}
	candidate, found, candidateErr := st.LatestResearchPromotionCandidate(ctx, proofs[0], sealAt)
	if candidateErr != nil || !found || candidate.ObservationID != currentID ||
		candidate.ExpectedNetLower != current.NetLower || candidate.CertificateHash != current.CertificateHash {
		t.Fatalf("promotion candidate=%+v found=%v err=%v", candidate, found, candidateErr)
	}
	exploration, explorationErr := st.RecentResearchPaperCandidates(ctx, sealAt, 10)
	if explorationErr != nil || len(exploration) != 1 || exploration[0].ObservationID != currentID ||
		exploration[0].ExpectedNetLower != current.NetLower || exploration[0].CertificateHash != current.CertificateHash {
		t.Fatalf("Paper exploration candidates=%+v err=%v", exploration, explorationErr)
	}
	byID, byIDFound, byIDErr := st.ResearchPaperCandidateByID(ctx, currentID)
	if byIDErr != nil || !byIDFound || byID.ObservationID != currentID ||
		byID.ExpectedNetLower != current.NetLower || byID.CertificateHash != current.CertificateHash {
		t.Fatalf("Paper candidate by id=%+v found=%v err=%v", byID, byIDFound, byIDErr)
	}
	driftedProof := proofs[0]
	driftedProof.Governance.CauseSpecHash = h("9")
	if _, _, err := st.InsertResearchPromotionIntent(ctx, driftedProof, candidate, .40, .01, .005); err == nil {
		t.Fatal("self-asserted cause/portfolio governance drift created a promotion intent")
	}
	intent, createdIntent, intentErr := st.InsertResearchPromotionIntent(ctx, proofs[0], candidate, .40, .01, .005)
	if intentErr != nil || !createdIntent || ResearchPromotionIntentFromSource(intent.SourceID) != intent.IntentID {
		t.Fatalf("promotion intent=%+v inserted=%v err=%v", intent, createdIntent, intentErr)
	}
	if _, accepted, err := st.AcceptedResearchPromotionIntent(ctx, intent.SourceID); err != nil || accepted {
		t.Fatalf("intent became accepted before Paper event: accepted=%v err=%v", accepted, err)
	}
	if err := st.AppendResearchPromotionEvent(ctx, intent, "paper_accepted", p.Route, .40, .01, "fixture Paper fill"); err != nil {
		t.Fatal(err)
	}
	acceptedIntent, accepted, err := st.AcceptedResearchPromotionIntent(ctx, intent.SourceID)
	if err != nil || !accepted || acceptedIntent.Proof.RunID != report.RunID {
		t.Fatalf("accepted intent=%+v accepted=%v err=%v", acceptedIntent, accepted, err)
	}
	if err := st.AppendResearchPromotionEvent(ctx, intent, "live_dispatched", p.Route, .40, .01, "fixture armed LIVE"); err != nil {
		t.Fatalf("accepted Paper intent did not permit LIVE receipt: %v", err)
	}
	if err := st.AppendResearchLiveExecutionReceipt(ctx, intent, ResearchLiveExecutionReceipt{
		State: "pending", OrderID: "order-fixture", RequestedQty: 1, RemainingQty: 1,
		AveragePrice: .40, ReceiptSource: "venue-submit-ack", Detail: "dispatch is not a fill",
	}); err != nil {
		t.Fatalf("pending dispatch receipt rejected: %v", err)
	}
	if pending, err := st.PendingResearchLiveExecutions(ctx, 10); err != nil || len(pending) != 1 ||
		pending[0].Receipt.OrderID != "order-fixture" || pending[0].Intent.IntentID != intent.IntentID {
		t.Fatalf("restart-safe pending execution queue=%+v err=%v", pending, err)
	}
	if pending, err := st.PendingResearchLiveExecutionsForVenue(ctx, "polyus", 10); err != nil || len(pending) != 0 {
		t.Fatalf("venue-isolated PolyUS queue leaked Kalshi receipt: %+v err=%v", pending, err)
	}
	if _, err := st.PendingResearchLiveExecutionsForVenue(ctx, "unsupported", 10); err == nil {
		t.Fatal("unsupported LIVE venue was accepted by restart queue")
	}
	if err := st.AppendResearchLiveExecutionReceipt(ctx, intent, ResearchLiveExecutionReceipt{
		State: "full", OrderID: "order-fixture", RequestedQty: 1, FilledQty: 1,
		AveragePrice: .40, FeeTotal: .01, ReceiptSource: "self-asserted", Detail: "forged full",
	}); err == nil {
		t.Fatal("non-authoritative dispatch acknowledgement manufactured a full fill")
	}
	if err := st.AppendResearchLiveExecutionReceipt(ctx, intent, ResearchLiveExecutionReceipt{
		State: "partial", OrderID: "order-fixture", RequestedQty: 1, FilledQty: .5, RemainingQty: .5,
		AveragePrice: .40, FeeTotal: .005, Authoritative: true, ReceiptSource: "venue-order-receipt", Detail: "partial",
	}); err != nil {
		t.Fatalf("authoritative partial receipt rejected: %v", err)
	}
	if err := st.AppendResearchLiveExecutionReceipt(ctx, intent, ResearchLiveExecutionReceipt{
		State: "full", OrderID: "order-fixture", RequestedQty: 1, FilledQty: 1, RemainingQty: 0,
		AveragePrice: .40, FeeTotal: .01, FeeKnown: true, Authoritative: true, ReceiptSource: "kalshi-create-order-receipt", Detail: "complete",
	}); err != nil {
		t.Fatalf("authoritative full receipt rejected: %v", err)
	}
	if pending, err := st.PendingResearchLiveExecutions(ctx, 10); err != nil || len(pending) != 0 {
		t.Fatalf("terminal full receipt stayed in restart-safe pending queue: %+v err=%v", pending, err)
	}
	if _, err := st.db.Exec(`UPDATE research_live_execution_receipts SET state='full' WHERE order_id='order-fixture'`); err == nil {
		t.Fatal("immutable LIVE execution receipt allowed update")
	}
	bridge, err := st.ResearchPromotionStatus(ctx)
	if err != nil || bridge.State != "PAPER_PROMOTIONS_ACTIVE" || bridge.SealedEligible != 1 ||
		bridge.PaperAccepted != 1 || bridge.LiveDispatched != 1 {
		t.Fatalf("promotion bridge status=%+v err=%v", bridge, err)
	}
	if err := st.AppendResearchPromotionEvent(ctx, intent, "paper_rejected", p.Route, .40, .01, "forged second outcome"); err == nil {
		t.Fatal("one immutable promotion intent accepted both Paper outcomes")
	}
}

func TestR139PreregistrationRejectsBackdatingObserverAndContractDrift(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h := strings.Repeat("a", 64)
	base := ResearchInferencePreregistration{PreregistrationID: "bad", SystemID: "proper-score-executor",
		Cohort: "x", Venue: "kalshi", Route: "taker", TrainValidationCutoff: now,
		UntouchedStart: now, UntouchedEnd: now.Add(40 * 24 * time.Hour), SealAt: now.Add(41 * 24 * time.Hour),
		Embargo: 24 * time.Hour, RequiredDays: 30, RequiredEvents: 30,
		CodeManifestHash: h, DataManifestHash: h, SourceManifestHash: h}
	if _, _, err := st.registerResearchInferencePreregistrationAt(ctx, base, now); err == nil {
		t.Fatal("backdated/non-embargoed preregistration accepted")
	}
	base.PreregistrationID, base.Route = "observer", "observer"
	base.UntouchedStart = now.Add(24 * time.Hour)
	base.UntouchedEnd = base.UntouchedStart.Add(40 * 24 * time.Hour)
	base.SealAt = base.UntouchedEnd.Add(24 * time.Hour)
	if _, _, err := st.registerResearchInferencePreregistrationAt(ctx, base, now); err == nil {
		t.Fatal("observer-only funnel was preregistered as economic proof")
	}
}

func TestR139PreregisteredObservationLoaderKeysetPagesWithoutTruncation(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	start := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	p := ResearchInferencePreregistration{SystemID: "proper-score-executor", ExperimentVersion: 1,
		Cohort: "paged-fixture", Venue: "kalshi", Route: "taker", UntouchedStart: start.Add(-time.Second),
		UntouchedEnd: start.Add(24 * time.Hour), SealAt: start.Add(48 * time.Hour)}
	registerR138EvidenceInstrument(t, st, "paged-event", p.Venue, "KXPAGED")
	for i := 0; i < 5001; i++ {
		row := r138EvidenceFixture("candidate", true)
		row.Observed, row.SystemID, row.Cohort = start.Add(time.Duration(i)*time.Millisecond), p.SystemID, p.Cohort
		row.OpportunityID = fmt.Sprintf("paged-%05d", i)
		row.CanonicalEventID, row.EventVersion = "paged-event", 0
		row.CanonicalPayoffID, row.PayoffVersion = "", 0
		row.Venue, row.Ticker, row.Route = p.Venue, "KXPAGED", p.Route
		if _, inserted, insertErr := st.InsertResearchSystemObservation(ctx, row); insertErr != nil || !inserted {
			t.Fatalf("row %d inserted=%v err=%v", i, inserted, insertErr)
		}
	}
	rows, err := st.loadPreregisteredRows(ctx, p)
	if err != nil || len(rows) != 5001 || rows[0].ObservationID >= rows[len(rows)-1].ObservationID {
		t.Fatalf("paged rows=%d err=%v", len(rows), err)
	}
}

func TestR174PreregisteredObservationLoaderUsesFrozenExperimentVersion(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	start := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	tx, err := st.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	insertR139Event(t, tx, "frozen-version-event", start)
	frozenID := insertR139Observation(t, tx, "proper-score-executor", "frozen-version-v1",
		"frozen-version-event", "kalshi", start.Add(time.Hour), true, 1)
	_ = insertR139Observation(t, tx, "proper-score-executor", "superseding-version-v2",
		"frozen-version-event", "kalshi", start.Add(2*time.Hour), true, 2)
	sealAt := start.Add(48 * time.Hour)
	value := .25
	insertR139Payoff(t, tx, frozenID, sealAt.Add(123*time.Millisecond), "settled", 1, 1,
		&value, "post-seal-fraction")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	p := ResearchInferencePreregistration{SystemID: "proper-score-executor",
		ExperimentVersion: 1, Cohort: "", Venue: "kalshi", Route: "taker",
		UntouchedStart: start, UntouchedEnd: start.Add(24 * time.Hour),
		SealAt: sealAt}
	rows, err := st.loadPreregisteredRows(ctx, p)
	if err != nil || len(rows) != 1 || rows[0].ObservationID != frozenID ||
		rows[0].ExperimentVersion != 1 || rows[0].PayoffUpdateID != 0 {
		t.Fatalf("frozen experiment loader rows=%+v want id=%d version=1 err=%v",
			rows, frozenID, err)
	}
}

func TestR139PreregisteredBundleLoaderKeysetPagesWithoutTruncation(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	start := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	p := ResearchInferencePreregistration{SystemID: "payoff-constraint-solver", ExperimentVersion: 1,
		Cohort: "paged-bundles", Venue: "kalshi", Route: "rfq"}
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("paged-bundle-%d", i)
		observed := start.Add(time.Duration(i+1) * time.Hour)
		b := conjunctionBundleFixture(id, "event:"+id, p.Cohort, observed)
		if ok, insertErr := st.InsertResearchRouteBundle(ctx, b); insertErr != nil || !ok {
			t.Fatalf("bundle %d inserted=%v err=%v", i, ok, insertErr)
		}
		if ok, gradeErr := st.AppendResearchRouteBundleGrade(ctx, ResearchRouteBundleGrade{
			BundleID: id, Observed: observed.Add(time.Minute), OutcomeStatus: "settled",
			Payout: 1, RealizedNet: .6, CapitalSeconds: 60, Reason: "paged fixture",
			EvidenceJSON: `{"exact":true}`,
		}); gradeErr != nil || !ok {
			t.Fatalf("bundle %d graded=%v err=%v", i, ok, gradeErr)
		}
	}
	rows, err := st.preregisteredBundleRawRowsPageSize(ctx, p, start, start.Add(24*time.Hour),
		start.Add(48*time.Hour), 0, 2)
	if err != nil || len(rows) != 3 || rows[0].ObservationID >= rows[2].ObservationID {
		t.Fatalf("paged bundle rows=%d err=%v rows=%+v", len(rows), err, rows)
	}
}

func TestR174PreregisteredEquivalentBundleEventsReturnsFirstPage(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	observed := time.Date(2026, 3, 2, 0, 0, 0, 123000000, time.UTC)
	p := ResearchInferencePreregistration{SystemID: "payoff-constraint-solver",
		ExperimentVersion: 1, Cohort: "first-page-bundles", Venue: "kalshi", Route: "rfq"}
	b := conjunctionBundleFixture("first-page-bundle", "event:first-page", p.Cohort, observed)
	if ok, insertErr := st.InsertResearchRouteBundle(ctx, b); insertErr != nil || !ok {
		t.Fatalf("bundle inserted=%v err=%v", ok, insertErr)
	}
	events, err := st.preregisteredEquivalentBundleEvents(ctx, p, time.Time{}, time.Time{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !events[b.CanonicalEventID] || len(events) != 1 {
		t.Fatalf("first page events=%v want only %q", events, b.CanonicalEventID)
	}
}

func r174SealFencePreregistration(id, system, cohort, route string,
	created time.Time) ResearchInferencePreregistration {
	hash := func(c string) string { return strings.Repeat(c, 64) }
	cutoff := created.Add(24 * time.Hour)
	start := cutoff.Add(24 * time.Hour)
	return ResearchInferencePreregistration{PreregistrationID: id, SystemID: system,
		Cohort: cohort, Venue: "kalshi", Route: route, TrainValidationCutoff: cutoff,
		UntouchedStart: start, UntouchedEnd: start.Add(40 * 24 * time.Hour),
		SealAt: start.Add(42 * 24 * time.Hour), Embargo: 24 * time.Hour,
		RequiredDays: 30, RequiredEvents: 30, CodeManifestHash: hash("a"),
		DataManifestHash: hash("b"), SourceManifestHash: hash("c")}
}

func r174InsertSealFenceObservation(t *testing.T, st *Store, p ResearchInferencePreregistration,
	eventID, ticker string, observed time.Time) int64 {
	t.Helper()
	eventVersion, payoffID, payoffVersion := registerR138EvidenceInstrument(t, st,
		eventID, p.Venue, ticker)
	row := r138EvidenceFixture("candidate", true)
	row.Observed, row.DecisionAt = observed, observed.Add(12*time.Millisecond)
	row.SystemID, row.OpportunityID, row.Cohort = p.SystemID, "seal-fence:"+ticker, p.Cohort
	row.CanonicalEventID, row.EventVersion = eventID, eventVersion
	row.CanonicalPayoffID, row.PayoffVersion = payoffID, payoffVersion
	row.Venue, row.Ticker, row.Route = p.Venue, ticker, p.Route
	id, inserted, err := st.InsertResearchSystemObservation(context.Background(), row)
	if err != nil || !inserted {
		t.Fatalf("seal-fence observation inserted=%v id=%d err=%v", inserted, id, err)
	}
	return id
}

func r174AssertSealFenceChanged(t *testing.T, st *Store, p ResearchInferencePreregistration,
	wantPrior preregisteredPriorEventManifest, wantRFQUntouched, contains string) {
	t.Helper()
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	err = validatePreregisteredImmutableSealInputsTx(context.Background(), tx, p, wantPrior,
		wantRFQUntouched)
	if err == nil || !strings.Contains(err.Error(), contains) {
		t.Fatalf("final seal fence err=%v want substring %q", err, contains)
	}
}

func TestR174PreregisteredFinalSealTransactionRejectsLateFrozenInputs(t *testing.T) {
	created := time.Date(2025, 11, 1, 0, 0, 0, 0, time.UTC)
	t.Run("ordinary observation before training cutoff", func(t *testing.T) {
		st, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		ctx := context.Background()
		request := r174SealFencePreregistration("late-training-observation",
			"proper-score-executor", "late-training-observation", "taker", created)
		p, inserted, err := st.registerResearchInferencePreregistrationAt(ctx, request, created)
		if err != nil || !inserted {
			t.Fatalf("preregister inserted=%v err=%v", inserted, err)
		}
		_, prior, err := preregisteredPriorEvents(ctx, st.db, p)
		if err != nil {
			t.Fatal(err)
		}
		r174InsertSealFenceObservation(t, st, p, "event:late-training-observation",
			"KXLATETRAINOBS", p.TrainValidationCutoff.Add(-time.Hour))
		r174AssertSealFenceChanged(t, st, p, prior, "", "training/validation input manifest changed")
	})
	t.Run("ordinary payoff before training cutoff", func(t *testing.T) {
		st, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		ctx := context.Background()
		request := r174SealFencePreregistration("late-training-payoff",
			"proper-score-executor", "late-training-payoff", "taker", created)
		observationID := r174InsertSealFenceObservation(t, st, request,
			"event:late-training-payoff", "KXLATETRAINPAY", request.TrainValidationCutoff.Add(-2*time.Hour))
		p, inserted, err := st.registerResearchInferencePreregistrationAt(ctx, request, created)
		if err != nil || !inserted {
			t.Fatalf("preregister inserted=%v err=%v", inserted, err)
		}
		_, prior, err := preregisteredPriorEvents(ctx, st.db, p)
		if err != nil {
			t.Fatal(err)
		}
		realized := .2
		if ok, err := st.AppendResearchPayoffUpdate(ctx, ResearchPayoffUpdate{
			ObservationID: observationID, Observed: p.TrainValidationCutoff.Add(-time.Hour),
			Status: "settled", PayoutLower: 1, PayoutUpper: 1, RealizedNet: &realized,
			SourceArtifact: "late authoritative training payoff", SourceHash: "late-training-payoff",
		}); err != nil || !ok {
			t.Fatalf("late payoff inserted=%v err=%v", ok, err)
		}
		r174AssertSealFenceChanged(t, st, p, prior, "", "training/validation input manifest changed")
	})
	t.Run("ordinary prior event during embargo", func(t *testing.T) {
		st, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		ctx := context.Background()
		request := r174SealFencePreregistration("late-prior-observation",
			"proper-score-executor", "late-prior-observation", "taker", created)
		p, inserted, err := st.registerResearchInferencePreregistrationAt(ctx, request, created)
		if err != nil || !inserted {
			t.Fatalf("preregister inserted=%v err=%v", inserted, err)
		}
		_, prior, err := preregisteredPriorEvents(ctx, st.db, p)
		if err != nil {
			t.Fatal(err)
		}
		r174InsertSealFenceObservation(t, st, p, "event:late-prior-observation",
			"KXLATEPRIOR", p.TrainValidationCutoff.Add(time.Hour))
		r174AssertSealFenceChanged(t, st, p, prior, "", "prior-event membership changed")
	})
	t.Run("rfq prior equivalent bundle during embargo", func(t *testing.T) {
		st, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		ctx := context.Background()
		request := r174SealFencePreregistration("late-prior-rfq",
			"payoff-constraint-solver", "late-prior-rfq", "rfq", created)
		p, inserted, err := st.registerResearchInferencePreregistrationAt(ctx, request, created)
		if err != nil || !inserted {
			t.Fatalf("preregister inserted=%v err=%v", inserted, err)
		}
		_, prior, err := preregisteredPriorEvents(ctx, st.db, p)
		if err != nil {
			t.Fatal(err)
		}
		rfqUntouched, err := preregisteredRFQUntouchedManifest(ctx, st.db, p)
		if err != nil {
			t.Fatal(err)
		}
		bundle := conjunctionBundleFixture("late-prior-rfq-bundle",
			"event:late-prior-rfq", p.Cohort, p.TrainValidationCutoff.Add(time.Hour))
		if ok, err := st.InsertResearchRouteBundle(ctx, bundle); err != nil || !ok {
			t.Fatalf("late RFQ bundle inserted=%v err=%v", ok, err)
		}
		r174AssertSealFenceChanged(t, st, p, prior, rfqUntouched, "prior-event membership changed")
	})
	t.Run("rfq untouched bundle after initial load", func(t *testing.T) {
		st, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		ctx := context.Background()
		request := r174SealFencePreregistration("late-untouched-rfq-bundle",
			"payoff-constraint-solver", "late-untouched-rfq-bundle", "rfq", created)
		p, inserted, err := st.registerResearchInferencePreregistrationAt(ctx, request, created)
		if err != nil || !inserted {
			t.Fatalf("preregister inserted=%v err=%v", inserted, err)
		}
		_, prior, err := preregisteredPriorEvents(ctx, st.db, p)
		if err != nil {
			t.Fatal(err)
		}
		rfqUntouched, err := preregisteredRFQUntouchedManifest(ctx, st.db, p)
		if err != nil {
			t.Fatal(err)
		}
		bundle := conjunctionBundleFixture("late-untouched-rfq-bundle-row",
			"event:late-untouched-rfq-bundle", p.Cohort, p.UntouchedStart.Add(time.Hour))
		if ok, err := st.InsertResearchRouteBundle(ctx, bundle); err != nil || !ok {
			t.Fatalf("late RFQ bundle inserted=%v err=%v", ok, err)
		}
		r174AssertSealFenceChanged(t, st, p, prior, rfqUntouched,
			"untouched RFQ bundle/grade manifest changed")
	})
	t.Run("rfq untouched grade after initial load", func(t *testing.T) {
		st, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		ctx := context.Background()
		request := r174SealFencePreregistration("late-untouched-rfq-grade",
			"payoff-constraint-solver", "late-untouched-rfq-grade", "rfq", created)
		p, inserted, err := st.registerResearchInferencePreregistrationAt(ctx, request, created)
		if err != nil || !inserted {
			t.Fatalf("preregister inserted=%v err=%v", inserted, err)
		}
		bundle := conjunctionBundleFixture("late-untouched-rfq-grade-row",
			"event:late-untouched-rfq-grade", p.Cohort, p.UntouchedStart.Add(time.Hour))
		if ok, err := st.InsertResearchRouteBundle(ctx, bundle); err != nil || !ok {
			t.Fatalf("RFQ bundle inserted=%v err=%v", ok, err)
		}
		_, prior, err := preregisteredPriorEvents(ctx, st.db, p)
		if err != nil {
			t.Fatal(err)
		}
		rfqUntouched, err := preregisteredRFQUntouchedManifest(ctx, st.db, p)
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := st.AppendResearchRouteBundleGrade(ctx, ResearchRouteBundleGrade{
			BundleID: bundle.BundleID, Observed: p.UntouchedStart.Add(2 * time.Hour),
			OutcomeStatus: "settled", Payout: 1, RealizedNet: .6, CapitalSeconds: 60,
			Reason: "late authoritative grade", EvidenceJSON: `{"late":true}`,
		}); err != nil || !ok {
			t.Fatalf("late RFQ grade inserted=%v err=%v", ok, err)
		}
		r174AssertSealFenceChanged(t, st, p, prior, rfqUntouched,
			"untouched RFQ bundle/grade manifest changed")
	})
}

func TestResearchPromotionMakerReceiptWaitsForTerminalExactFeeState(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	source := "r139p:" + strings.Repeat("a", 64)
	id, err := st.InsertMakerAttempt(ctx, "kalshi", "KXMAKER", "YES", source, .40, 2, 10, 0, nil, 1, "", "bookws")
	if err != nil {
		t.Fatal(err)
	}
	state, err := st.ResearchPromotionPaperRouteState(ctx, source, "maker", time.Now().Add(-time.Minute))
	if err != nil || state.State != "pending" {
		t.Fatalf("resting maker state=%+v err=%v", state, err)
	}
	if err := st.MarkMakerFilledWithFee(ctx, id, -.0025, "fixture-rebate-authority"); err != nil {
		t.Fatal(err)
	}
	state, err = st.ResearchPromotionPaperRouteState(ctx, source, "maker", time.Now().Add(-time.Minute))
	if err != nil || state.State != "filled" || state.FeePC != -.0025 || state.FeeSource == "" {
		t.Fatalf("filled maker state=%+v err=%v", state, err)
	}
	source2 := "r139p:" + strings.Repeat("b", 64)
	id2, err := st.InsertMakerAttempt(ctx, "kalshi", "KXMAKER2", "YES", source2, .40, 2, 10, 0, nil, 1, "", "bookws")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.MarkMakerExpiredRule(ctx, id2, "moved-one-tick"); err != nil {
		t.Fatal(err)
	}
	state, err = st.ResearchPromotionPaperRouteState(ctx, source2, "maker", time.Now().Add(-time.Minute))
	if err != nil || state.State != "terminal_rejected" || state.Reason != "moved-one-tick" {
		t.Fatalf("expired maker state=%+v err=%v", state, err)
	}
}
