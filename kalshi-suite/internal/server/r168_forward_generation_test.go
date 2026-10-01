package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR168ForwardGenerationFreezesEveryR147Contract(t *testing.T) {
	generation := r168ForwardResearchGeneration()
	if generation.GenerationID != r168ForwardGenerationID || generation.CashAuthority ||
		generation.DetectorReceiptVersion != r168ForwardDetectorModelVersion ||
		generation.EventIdentityVersion != r168ForwardEventIdentityVersion ||
		len(generation.Contracts) != 385 {
		t.Fatalf("generation identity/count drifted: id=%q detector=%q identity=%q cash=%v contracts=%d",
			generation.GenerationID, generation.DetectorReceiptVersion,
			generation.EventIdentityVersion, generation.CashAuthority, len(generation.Contracts))
	}
	realistic, logOnly := 0, 0
	seen := map[string]bool{}
	for _, contract := range generation.Contracts {
		if contract.ContractID == "" || seen[contract.ContractID] || contract.CashAuthority {
			t.Fatalf("invalid manifest contract: %+v", contract)
		}
		seen[contract.ContractID] = true
		if contract.Realistic {
			realistic++
		} else {
			logOnly++
		}
	}
	if realistic != 354 || logOnly != 31 {
		t.Fatalf("execution-class inventory drifted: realistic=%d log_only=%d", realistic, logOnly)
	}

	s := testServer(t)
	if err := s.PrepareForwardResearchGeneration(context.Background()); err != nil {
		t.Fatal(err)
	}
	report, err := s.store.ForwardResearchReport(context.Background(), r168ForwardGenerationID)
	if err != nil {
		t.Fatal(err)
	}
	if report.ContractCount != 385 ||
		report.DetectorReceiptVersion != r168ForwardDetectorModelVersion ||
		report.EventIdentityVersion != r168ForwardEventIdentityVersion ||
		report.RealisticContracts != 354 ||
		report.LogOnlyContracts != 31 || report.UnseenContracts != 385 ||
		report.PositiveVerdict || report.CashAuthority || report.Verdict != "NOT_PROVEN" {
		t.Fatalf("initial manifest report is not complete/fail-closed: %+v", report)
	}
	rr := httptest.NewRecorder()
	s.handleForwardResearch(rr, httptest.NewRequest(http.MethodGet, "/api/forward-research", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("report http=%d body=%s", rr.Code, rr.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["generation_id"] != r168ForwardGenerationID || payload["positive_verdict"] != false ||
		payload["GenerationID"] != nil {
		t.Fatalf("report JSON contract drifted: %+v", payload)
	}
}

func TestR168DetectorStampUsesExactContractAndOnlyCachedProof(t *testing.T) {
	s := testServer(t)
	var exact r147SystemVariantSignalContract
	for _, candidate := range r147SystemVariantSignalContracts() {
		if candidate.SystemID == "spotlag" && candidate.ExecutionVenue == "kalshi" &&
			candidate.Side == "YES" && candidate.Route == "taker" {
			exact = candidate
			break
		}
	}
	if exact.SystemID == "" {
		t.Fatal("spotlag exact contract missing")
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	attempt := storage.ExecutionShadowAttempt{AttemptID: "stamp-test", TriggerUnixMS: now.UnixMilli(),
		Venue: "kalshi", Ticker: "KXSTAMP", Title: "Stamp test", Side: "YES", Action: "BUY",
		SystemID: "spotlag", Route: "taker", SignalContract: r147SignalContractIdentity(exact)}
	event := storage.ExecutionShadowEvent{BookSource: "kalshi_ws_full_orderbook:g11:s22:q33",
		Evidence: map[string]any{"existing": true}}
	s.r168StampForwardDetectorEvent(&event, attempt, "pricing-v9")
	if event.BookGeneration == nil || *event.BookGeneration != 11 || event.BookSubscriptionID == nil ||
		*event.BookSubscriptionID != 22 || event.BookSequence == nil || *event.BookSequence != 33 {
		t.Fatalf("cached sequence receipt not parsed: %+v", event)
	}
	if event.Evidence["forward_generation_id"] != r168ForwardGenerationID ||
		event.Evidence["signal_contract_id"] != attempt.SignalContract ||
		event.Evidence["execution_model_version"] != "pricing-v9" ||
		event.Evidence["forward_detector_receipt_version"] != r168ForwardDetectorModelVersion ||
		event.Evidence["event_cluster_identity_version"] != r168ForwardEventIdentityVersion ||
		event.Evidence["trigger_unix_ms"] != now.UnixMilli() ||
		event.Evidence["event_cluster_id"] != "" || event.Evidence["event_cluster_verified"] != false ||
		event.Evidence["cash_authority"] != false {
		t.Fatalf("forward detector stamp incomplete or unsafe: %+v", event.Evidence)
	}
}

func TestR173ForwardGenerationV3CannotMixLegacyV2Rows(t *testing.T) {
	_, st := newExecutionShadowTestServer(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	current := r168ForwardResearchGeneration()
	if current.GenerationID != "r168-forward-all-contracts-v3" {
		t.Fatalf("current generation=%q, want clean v3 boundary", current.GenerationID)
	}
	current.OpenedAt = now.Add(-time.Minute)
	legacy := current
	legacy.GenerationID = "r168-forward-all-contracts-v2"
	legacy.DetectorReceiptVersion = "r168-forward-detector-receipt-v2"
	legacy.EventIdentityVersion = "legacy-event-identity-v2"
	legacy.OpenedAt = now.Add(-2 * time.Minute)
	for _, generation := range []storage.ForwardResearchGeneration{legacy, current} {
		if _, err := st.PrepareForwardResearchGeneration(context.Background(), generation); err != nil {
			t.Fatalf("prepare %s: %v", generation.GenerationID, err)
		}
	}
	contract := current.Contracts[0]
	appendDetector := func(generationID, suffix string, at time.Time) {
		t.Helper()
		attempt := storage.ExecutionShadowAttempt{
			AttemptID: "generation-boundary-" + suffix, SignalDecisionID: "decision-" + suffix,
			ObservedAt: at, TriggerUnixMS: at.UnixMilli(), Venue: contract.Venue,
			Ticker: "KXR173-" + suffix, Title: suffix, Side: contract.Side, Action: contract.Action,
			SystemID: contract.SystemID, Route: contract.Route, SignalSource: "test",
			InputTopology: contract.InputTopology, SignalContract: contract.ContractID,
			SignalPrice: .4, QualificationBasis: "generation isolation test", CreatedAt: at,
		}
		if inserted, err := st.InsertExecutionShadowAttempt(context.Background(), attempt); err != nil || !inserted {
			t.Fatalf("insert %s attempt=%v err=%v", suffix, inserted, err)
		}
		event := storage.ExecutionShadowEvent{
			EventID: "generation-boundary-detector-" + suffix, AttemptID: attempt.AttemptID,
			At: at, ElapsedFromTriggerMS: 0, Stage: "detector-branch", Outcome: "qualified",
			Evidence: map[string]any{
				"forward_generation_id": generationID, "signal_contract_id": contract.ContractID,
				"execution_model_version":          r168ForwardDetectorModelVersion,
				"forward_detector_receipt_version": r168ForwardDetectorModelVersion,
				"event_cluster_identity_version":   r168ForwardEventIdentityVersion,
				"trigger_unix_ms":                  at.UnixMilli(),
			},
		}
		if inserted, err := st.AppendExecutionShadowEvent(context.Background(), event); err != nil || !inserted {
			t.Fatalf("append %s detector=%v err=%v", suffix, inserted, err)
		}
	}

	appendDetector(legacy.GenerationID, "legacy-v2", now)
	v3Before, err := st.ForwardResearchReport(context.Background(), current.GenerationID)
	if err != nil {
		t.Fatal(err)
	}
	if v3Before.ObservedAttempts != 0 || v3Before.UnseenContracts != int64(len(current.Contracts)) {
		t.Fatalf("legacy v2 row leaked into v3 before its first event: %+v", v3Before)
	}
	appendDetector(current.GenerationID, "current-v3", now.Add(time.Second))
	v2, err := st.ForwardResearchReport(context.Background(), legacy.GenerationID)
	if err != nil {
		t.Fatal(err)
	}
	v3, err := st.ForwardResearchReport(context.Background(), current.GenerationID)
	if err != nil {
		t.Fatal(err)
	}
	contractAttempts := func(report storage.ForwardResearchReport) int64 {
		t.Helper()
		for _, row := range report.Rows {
			if row.ContractID == contract.ContractID {
				return row.ObservedAttempts
			}
		}
		t.Fatalf("contract %q missing from %s", contract.ContractID, report.GenerationID)
		return 0
	}
	if v2.ObservedAttempts != 1 || v3.ObservedAttempts != 1 ||
		contractAttempts(v2) != 1 || contractAttempts(v3) != 1 {
		t.Fatalf("generation rows mixed or disappeared: v2=%+v v3=%+v", v2, v3)
	}
}

func TestR168DetectorStampSharesOnlyResidentStructuralGameCluster(t *testing.T) {
	s := testServer(t)
	cubsSlug, _ := anchorChiGame(t, s)
	kalshiTicker := "KXMLBGAME-26JUL091910CHCCWS-CHC"
	gameID, ok := s.giGameOfRef(kalshiTicker)
	if !ok {
		t.Fatal("structural game fixture missing")
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	stamp := func(venue, ticker, title string) map[string]any {
		attempt := storage.ExecutionShadowAttempt{
			AttemptID: "structural-" + venue, TriggerUnixMS: now.UnixMilli(),
			Venue: venue, Ticker: ticker, Title: title, Side: "YES", Action: "BUY",
			SystemID: "spotlag", Route: "taker", SignalContract: "test-contract",
		}
		event := storage.ExecutionShadowEvent{Evidence: map[string]any{}}
		s.r168StampForwardDetectorEvent(&event, attempt, "test-model")
		return event.Evidence
	}
	kalshiEvidence := stamp("kalshi", kalshiTicker, "Chicago game")
	polyUSEvidence := stamp("polyus", cubsSlug, "Completely different display title")
	want := "sports:" + gameID
	for _, evidence := range []map[string]any{kalshiEvidence, polyUSEvidence} {
		if evidence["event_cluster_id"] != want ||
			evidence["event_cluster_verified"] != true ||
			evidence["event_cluster_source"] != "resident-structural-game-identity" {
			t.Fatalf("structural identity not frozen exactly: %+v", evidence)
		}
	}

	// A similar-looking title without a structural venue join must remain unknown.
	unknown := stamp("kalshi", "KX-NOT-STRUCTURALLY-ANCHORED", "Chicago game")
	if unknown["event_cluster_id"] != "" || unknown["event_cluster_verified"] != false ||
		unknown["event_cluster_source"] != "" {
		t.Fatalf("title/proxy guess became false event proof: %+v", unknown)
	}
}

func TestR168AllSixProperMomentumContractsEnterGeneration(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	if err := s.PrepareForwardResearchGeneration(context.Background()); err != nil {
		t.Fatal(err)
	}
	releaseWriter := holdExecutionShadowWriterStart(t, s)
	now := time.Now().UTC().Truncate(time.Millisecond)
	for i, transform := range []string{"brier", "log", "spherical"} {
		for j, side := range []string{"NO", "YES"} {
			index := i*2 + j
			prior := -1.0
			if side == "YES" {
				prior = 1
			}
			in := storage.ProperMomentumSellIntent{
				IntentID: strings.Repeat(string(rune('a'+index)), 64),
				SourceID: "r168-forward-proper-" + transform + "-" + side,
				Created:  now.Add(time.Duration(index) * time.Millisecond),
				Candidate: storage.ProperMomentumSellCandidate{
					Observed: now, Cohort: "proper-score-v2|transform=" + transform,
					Ticker: "KXR168-" + strings.ToUpper(transform) + "-" + side,
					Title:  "proper " + transform + " " + side, OwnedSide: side,
					SourceClockID: "sealed-clock-" + transform + "-" + side,
					RequiredPrior: prior, ObservedSellPrice: .6, ObservedSellFee: .01,
				},
			}
			candidate := s.ensureProperMomentumCanonicalAttempt(in)
			if candidate.SignalContractID == "" {
				t.Fatalf("%s %s did not bind exact contract", transform, side)
			}
		}
	}
	releaseWriter()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	report, err := st.ForwardResearchReport(ctx, r168ForwardGenerationID)
	if err != nil {
		t.Fatal(err)
	}
	observed := 0
	for _, row := range report.Rows {
		if row.SystemID == "proper-score-momentum" {
			if row.ObservedAttempts != 1 {
				t.Fatalf("proper manifest row was not indexed exactly once: %+v", row)
			}
			observed++
		}
	}
	if observed != 6 || report.ObservedAttempts != 6 {
		t.Fatalf("proper generation coverage=%d total_attempts=%d", observed, report.ObservedAttempts)
	}
}

func TestR168InverseKflowDetectorEntersGeneration(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	if err := s.PrepareForwardResearchGeneration(context.Background()); err != nil {
		t.Fatal(err)
	}
	releaseWriter := holdExecutionShadowWriterStart(t, s)
	at := time.Now().UTC().Truncate(time.Millisecond)
	work := livePolicyMirrorWork{
		Signal: storage.Signal{Platform: "kalshi", Ticker: "KXR168-INVERSE", Title: "inverse",
			Side: "NO", SignalType: "invert:kalshi-flow", ResolveHours: 1,
			ExecExpr: "independent-inverse-system/base=kalshi-flow/emitted=YES/" +
				r147InputReceiptExpr("K", at.Add(-time.Millisecond))},
		Point: .62, SignalAt: at,
		Candidate: liveMirrorCandidate{Platform: "kalshi", Ticker: "KXR168-INVERSE",
			Title: "inverse", Side: "NO", Action: "BUY", Family: "invert:kalshi-flow",
			Source: inverseKflowShadowSignalSource, At: at},
		ExperimentKind: inverseKflowShadowExperimentKind,
	}
	candidate, ok := s.beginInverseKflowShadowAttempt(work)
	if !ok || candidate.SignalContractID == "" {
		t.Fatal("inverse producer did not create an exact forward attempt")
	}
	releaseWriter()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	report, err := st.ForwardResearchReport(ctx, r168ForwardGenerationID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range report.Rows {
		if row.ContractID == candidate.SignalContractID {
			found = row.ObservedAttempts == 1
			break
		}
	}
	if !found || report.ObservedAttempts != 1 {
		t.Fatalf("inverse detector did not enter manifest: attempts=%d contract=%s",
			report.ObservedAttempts, candidate.SignalContractID)
	}
}
