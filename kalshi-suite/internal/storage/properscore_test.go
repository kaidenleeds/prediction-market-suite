package storage

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"
)

func properStorageFixture(transform string) ProperScoreTrial {
	return ProperScoreTrial{Observed: time.Now().UTC(), Slot: "2026-07-11T20:00:00Z",
		System: "proper-score-" + transform, Transform: transform, Platform: "kalshi", Ticker: "KX-PROPER",
		Title: "test", Category: "test", ForecastYes: .70, ForecastOriginSide: "YES",
		ForecastSignal: "test-signal", ForecastSource: "ml-book-v1-calibrated", ForecastVersion: "v1",
		ModelBackend: "test", Calibration: "sigmoid", GeneratedAt: time.Now().Unix(), Route: "taker",
		StrategyMode: "fundamental", Cohort: "proper-score-v2|transform=" + transform + "|side=YES|route=taker",
		YesBid: .45, YesAsk: .50, YesBidDepth: 10, YesAskDepth: 10,
		NoBid: .50, NoAsk: .55, NoBidDepth: 10, NoAskDepth: 10,
		BookSource: "kalshi_book_ws", SourceClockID: "test-source-clock", QuoteAge: .2, DecisionLatencyMS: 1, QYes: .50,
		RawYes: .4, RawNo: -.4, CanonicalQty: .8, ExecutableQty: .8,
		RawVector: []float64{.4, -.4}, ConstantShift: .4, Rescale: 1, NormalizedWeight: .8,
		RequestedQty: .8, SelectedSide: "YES", TickSize: .01, LotSize: .1,
		DepthCurve: []map[string]any{{"price": .5, "quantity": .8, "tick": .01}},
		EntryPrice: .50, EntryDepth: 10, IntegratedCost: .4, SpotCost: .4,
		ExactFeeTotal: .02, FeeSource: "kalshi:test", ExpectedNet: .14,
		TargetPosition: .8, CancelledDelta: .8, FillStatus: "counterfactual"}
}

func freezeProperStorageManifests(t *testing.T, st *Store, trials ...ProperScoreTrial) {
	t.Helper()
	type group struct {
		manifest ProperScoreManifest
	}
	groups := map[string]*group{}
	for _, trial := range trials {
		key := trial.Slot + "|" + trial.Transform + "|" + trial.StrategyMode
		g := groups[key]
		if g == nil {
			g = &group{manifest: ProperScoreManifest{Created: trial.Observed, Slot: trial.Slot,
				Transform: trial.Transform, StrategyMode: trial.StrategyMode,
				ForecastSource: trial.ForecastSource, ForecastVersion: trial.ForecastVersion}}
			groups[key] = g
		}
		g.manifest.Coordinates = append(g.manifest.Coordinates, ProperScoreCoordinateFromTrial(trial))
	}
	for _, g := range groups {
		if _, _, err := st.RegisterProperScoreManifest(context.Background(), g.manifest); err != nil {
			t.Fatalf("freeze proper-score manifest: %v", err)
		}
	}
}

func TestProperScoreResearchLedgerDedupGradesAndReports(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	trial := properStorageFixture("brier")
	freezeProperStorageManifests(t, st, trial)
	if inserted, err := st.InsertProperScoreTrial(ctx, trial); err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	if inserted, err := st.InsertProperScoreTrial(ctx, trial); err != nil || inserted {
		t.Fatalf("duplicate inserted=%v err=%v", inserted, err)
	}
	abstain := properStorageFixture("log")
	abstain.ForecastYes, abstain.QYes = .48, .475
	abstain.RawYes, abstain.RawNo = math.Log(.48/.475), math.Log(.52/.525)
	abstain.CanonicalQty = math.Abs(abstain.RawYes - abstain.RawNo)
	abstain.ExecutableQty, abstain.SelectedSide, abstain.EntryPrice, abstain.EntryDepth = 0, "", 0, 0
	abstain.ExactFeeTotal, abstain.FeeSource, abstain.ExpectedNet = 0, "", 0
	abstain.AbstainReason = "inside_bid_ask_dead_zone"
	freezeProperStorageManifests(t, st, abstain)
	if inserted, err := st.InsertProperScoreTrial(ctx, abstain); err != nil || !inserted {
		t.Fatalf("abstain inserted=%v err=%v", inserted, err)
	}
	if err := st.InsertSignal(ctx, Signal{Platform: "kalshi", Ticker: trial.Ticker, Side: "YES",
		SignalType: "settlement", EntryPrice: .5}); err != nil {
		t.Fatal(err)
	}
	closed := time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE signal_log SET resolved=1,won=1,settle_val=1,resolved_at=? WHERE ticker=?`, closed, trial.Ticker); err != nil {
		t.Fatal(err)
	}
	if n, err := st.ResolveProperScoreTrials(ctx, 10); err != nil || n != 2 {
		t.Fatalf("graded=%d err=%v", n, err)
	}
	var net float64
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT realized_net FROM research_proper_score_trials WHERE transform='brier'`).Scan(&net); err != nil {
		t.Fatal(err)
	}
	if math.Abs(net-.38) > 1e-9 {
		t.Fatalf("realized net=%v want .38", net)
	}
	report, err := st.ProperScoreReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s := report["summary"].(map[string]any)
	if s["rows"].(int) != 2 || s["actions"].(int) != 1 || s["abstains"].(int) != 1 || s["settled"].(int) != 2 {
		t.Fatalf("summary=%#v", s)
	}
	if s["normalized_executable_rows"].(int) != 1 || s["normalized_executable_settled"].(int) != 1 ||
		s["normalized_executable_completed_slots"].(int) != 1 ||
		s["normalized_executable_unique_settled_markets"].(int) != 1 ||
		s["legacy_unnormalized_rows"].(int) != 0 || s["legacy_unnormalized_settled"].(int) != 0 {
		t.Fatalf("normalized cohort counts=%#v", s)
	}
	if math.Abs(s["l1_weighted_expected_net"].(float64)-.112) > 1e-9 ||
		math.Abs(s["l1_weighted_realized_net"].(float64)-.304) > 1e-9 {
		t.Fatalf("portfolio weights did not affect research return metrics: %#v", s)
	}
	if p, err := st.ProperScoreActualPosition(ctx, "fundamental", "brier", "kalshi",
		trial.Ticker, trial.ForecastSource, trial.ForecastVersion); err != nil || p != 0 {
		t.Fatalf("counterfactual row moved actual position: position=%v err=%v", p, err)
	}
	routes, err := st.ResearchRouteReport(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	routeCounts := routes["counts"].(map[string]int)
	if routeCounts["routes"] != 2 || routeCounts["grade_events"] != 2 {
		t.Fatalf("proper-score route lifecycle missing: %+v", routes)
	}
}

func TestProperOutcomeScoreSupportsAllPaperTransforms(t *testing.T) {
	for _, transform := range []string{"brier", "log", "spherical"} {
		yes, yesOK := properOutcomeScore(transform, .7, 1)
		no, noOK := properOutcomeScore(transform, .7, 0)
		if !yesOK || !noOK || !validFinite(yes) || !validFinite(no) || yes <= no {
			t.Fatalf("%s score yes=%v/%v no=%v/%v", transform, yes, yesOK, no, noOK)
		}
	}
	if _, ok := properOutcomeScore("log", 1, 1); ok {
		t.Fatal("undefined boundary log score admitted")
	}
}

func TestFrozenPersonaCandidatesSeparateModeAndIncludeSpherical(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx, now := context.Background(), time.Now().UTC()
	for _, row := range []struct {
		transform string
		net       float64
	}{{"brier", .05}, {"log", .10}, {"spherical", .20}} {
		for i := 0; i < 20; i++ {
			trial := properStorageFixture(row.transform)
			trial.Observed = now.Add(-10*24*time.Hour + time.Duration(i)*time.Minute)
			trial.Slot = trial.Observed.Truncate(time.Minute).Format(time.RFC3339Nano)
			trial.Ticker = fmt.Sprintf("PRIOR-%s-%02d", row.transform, i)
			trial.Cohort = "persona-mode-fixture"
			freezeProperStorageManifests(t, st, trial)
			if inserted, err := st.InsertProperScoreTrial(ctx, trial); err != nil || !inserted {
				t.Fatalf("prior insert %s/%d inserted=%v err=%v", row.transform, i, inserted, err)
			}
			if _, err := st.DBForTest().ExecContext(ctx, `UPDATE research_proper_score_trials
SET settled=1,realized_net=?,closed_ts=? WHERE ticker=?`, row.net,
				trial.Observed.Add(time.Hour).Format(time.RFC3339Nano), trial.Ticker); err != nil {
				t.Fatal(err)
			}
		}
	}
	current := properStorageFixture("spherical")
	current.Observed, current.Slot, current.Ticker = now.Add(-time.Hour), now.Add(-time.Hour).Format(time.RFC3339Nano), "CURRENT-SPHERICAL"
	current.Cohort = "persona-mode-fixture"
	freezeProperStorageManifests(t, st, current)
	if inserted, err := st.InsertProperScoreTrial(ctx, current); err != nil || !inserted {
		t.Fatalf("current insert=%v err=%v", inserted, err)
	}
	candidates, err := st.FrozenPersonaCandidates(ctx, now, 10)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("candidates=%+v err=%v", candidates, err)
	}
	got := candidates[0]
	if got.SelectedTransform != "spherical" || got.StrategyMode != "fundamental" ||
		got.BrierN != 20 || got.LogN != 20 || got.SphericalN != 20 || got.PriorN != 60 ||
		got.SphericalMean <= got.LogMean {
		t.Fatalf("three-transform frozen persona=%+v", got)
	}
}

func TestProperScoreResearchLedgerRejectsUnknownVersion(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	trial := properStorageFixture("brier")
	trial.ForecastVersion = ""
	if inserted, err := st.InsertProperScoreTrial(context.Background(), trial); err == nil || inserted {
		t.Fatalf("unknown forecast version admitted: inserted=%v err=%v", inserted, err)
	}
}

func TestProperScoreReportExcludesLegacyRowEvenWithNonzeroWeight(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	trial := properStorageFixture("brier")
	trial.Cohort = "proper-score-legacy-v1"
	freezeProperStorageManifests(t, st, trial)
	if inserted, err := st.InsertProperScoreTrial(ctx, trial); err != nil || !inserted {
		t.Fatalf("legacy fixture inserted=%v err=%v", inserted, err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE research_proper_score_trials
SET settled=1,realized_net=.25,closed_ts=? WHERE ticker=?`, time.Now().UTC().Format(time.RFC3339Nano), trial.Ticker); err != nil {
		t.Fatal(err)
	}
	report, err := st.ProperScoreReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	row := report["transforms"].(map[string]any)["brier"].(map[string]any)
	if row["normalized_executable_settled"].(int) != 0 ||
		row["legacy_unnormalized_settled"].(int) != 1 ||
		math.Abs(row["legacy_unnormalized_realized_net"].(float64)-.25) > 1e-9 ||
		row["l1_weighted_realized_net"].(float64) != 0 {
		t.Fatalf("legacy row leaked into normalized effectiveness: %#v", row)
	}
}

func TestProperScoreReportExcludesPartialVectorEconomics(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	first := properStorageFixture("brier")
	first.Ticker, first.NormalizedWeight, first.ExpectedNet = "KX-PROPER-A", .4, .10
	first.ExecutableQty, first.RequestedQty = .4, .4
	first.IntegratedCost, first.SpotCost = .20, .20
	second := properStorageFixture("brier")
	second.Ticker, second.NormalizedWeight, second.ExpectedNet = "KX-PROPER-B", .6, .20
	second.ExecutableQty, second.RequestedQty = .6, .6
	second.IntegratedCost, second.SpotCost = .30, .30
	freezeProperStorageManifests(t, st, first, second)
	for _, trial := range []ProperScoreTrial{first, second} {
		if inserted, insertErr := st.InsertProperScoreTrial(ctx, trial); insertErr != nil || !inserted {
			t.Fatalf("insert %s: inserted=%v err=%v", trial.Ticker, inserted, insertErr)
		}
	}
	closed := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE research_proper_score_trials
SET settled=1,realized_net=.30,closed_ts=? WHERE ticker=?`, closed, first.Ticker); err != nil {
		t.Fatal(err)
	}
	report, err := st.ProperScoreReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	partial := report["transforms"].(map[string]any)["brier"].(map[string]any)
	if partial["normalized_executable_rows"].(int) != 2 ||
		partial["normalized_executable_settled"].(int) != 1 ||
		partial["normalized_executable_completed_slots"].(int) != 0 ||
		partial["normalized_executable_unique_settled_markets"].(int) != 0 ||
		partial["normalized_executable_settled_l1"].(float64) != 0 ||
		partial["expected_net"].(float64) != 0 ||
		partial["realized_net"].(float64) != 0 ||
		partial["l1_weighted_expected_net"].(float64) != 0 ||
		partial["l1_weighted_realized_net"].(float64) != 0 {
		t.Fatalf("open/partial vector leaked into economic headlines: %#v", partial)
	}
	brief, err := st.ProperScoreBriefReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	partialBrief := brief["transforms"].(map[string]any)["brier"].(map[string]any)
	if partialBrief["normalized_executable_completed_slots"].(int) != 0 ||
		partialBrief["normalized_executable_unique_settled_markets"].(int) != 0 ||
		partialBrief["normalized_executable_settled_l1"].(float64) != 0 ||
		partialBrief["l1_weighted_realized_net"].(float64) != 0 {
		t.Fatalf("compact report admitted partial vector: %#v", partialBrief)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE research_proper_score_trials
SET settled=1,realized_net=-.10,closed_ts=? WHERE ticker=?`, closed, second.Ticker); err != nil {
		t.Fatal(err)
	}
	report, err = st.ProperScoreReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	complete := report["transforms"].(map[string]any)["brier"].(map[string]any)
	if complete["normalized_executable_settled"].(int) != 2 ||
		complete["normalized_executable_completed_slots"].(int) != 1 ||
		complete["normalized_executable_unique_settled_markets"].(int) != 2 ||
		math.Abs(complete["normalized_executable_settled_l1"].(float64)-1) > 1e-9 ||
		math.Abs(complete["expected_net"].(float64)-.30) > 1e-9 ||
		math.Abs(complete["realized_net"].(float64)-.20) > 1e-9 ||
		math.Abs(complete["l1_weighted_expected_net"].(float64)-.16) > 1e-9 ||
		math.Abs(complete["l1_weighted_realized_net"].(float64)-.06) > 1e-9 {
		t.Fatalf("completed vector metrics are wrong: %#v", complete)
	}
	brief, err = st.ProperScoreBriefReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	completeBrief := brief["transforms"].(map[string]any)["brier"].(map[string]any)
	if completeBrief["normalized_executable_completed_slots"].(int) != 1 ||
		completeBrief["normalized_executable_unique_settled_markets"].(int) != 2 ||
		math.Abs(completeBrief["normalized_executable_settled_l1"].(float64)-1) > 1e-9 ||
		math.Abs(completeBrief["l1_weighted_realized_net"].(float64)-.06) > 1e-9 {
		t.Fatalf("compact report diverged from completed-vector economics: %#v", completeBrief)
	}
}

func TestProperScoreManifestMissingCoordinateCannotBecomeCompletedVector(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	first := properStorageFixture("log")
	first.Ticker, first.NormalizedWeight = "KX-MANIFEST-A", .4
	second := properStorageFixture("log")
	second.Ticker, second.NormalizedWeight = "KX-MANIFEST-B", .6
	freezeProperStorageManifests(t, st, first, second)
	if inserted, insertErr := st.InsertProperScoreTrial(ctx, first); insertErr != nil || !inserted {
		t.Fatalf("insert surviving coordinate: inserted=%v err=%v", inserted, insertErr)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE research_proper_score_trials
SET settled=1,realized_net=.25,closed_ts=? WHERE ticker=?`,
		time.Now().UTC().Format(time.RFC3339Nano), first.Ticker); err != nil {
		t.Fatal(err)
	}
	report, err := st.ProperScoreReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	row := report["transforms"].(map[string]any)["log"].(map[string]any)
	if row["manifest_slots"].(int) != 1 || row["manifest_incomplete_slots"].(int) != 1 ||
		row["manifest_missing_coordinates"].(int) != 1 || row["manifest_hash_proven_slots"].(int) != 0 ||
		row["normalized_executable_completed_slots"].(int) != 0 ||
		row["expected_net"].(float64) != 0 || row["realized_net"].(float64) != 0 ||
		row["l1_weighted_realized_net"].(float64) != 0 {
		t.Fatalf("missing coordinate was not fail-closed: %#v", row)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE research_proper_score_manifests
SET expected_coordinate_count=1 WHERE slot=? AND transform=? AND strategy_mode=?`,
		first.Slot, first.Transform, first.StrategyMode); err == nil {
		t.Fatal("immutable expected-coordinate manifest was rewritten")
	}
	unexpected := properStorageFixture("log")
	unexpected.Ticker = "KX-MANIFEST-C"
	if inserted, err := st.InsertProperScoreTrial(ctx, unexpected); err == nil || inserted {
		t.Fatalf("coordinate outside frozen manifest admitted: inserted=%v err=%v", inserted, err)
	}
}

func TestProperScoreReportExcludesUnsupportedTerminalVectorEconomics(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	first := properStorageFixture("spherical")
	first.Ticker, first.NormalizedWeight, first.ExpectedNet = "KX-UNSUPPORTED-A", .4, .10
	second := properStorageFixture("spherical")
	second.Ticker, second.NormalizedWeight, second.ExpectedNet = "KX-UNSUPPORTED-B", .6, .20
	freezeProperStorageManifests(t, st, first, second)
	for _, trial := range []ProperScoreTrial{first, second} {
		if inserted, insertErr := st.InsertProperScoreTrial(ctx, trial); insertErr != nil || !inserted {
			t.Fatalf("insert %s: inserted=%v err=%v", trial.Ticker, inserted, insertErr)
		}
	}
	closed := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE research_proper_score_trials
SET settled=1,realized_net=.30,closed_ts=? WHERE ticker=?`, closed, first.Ticker); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE research_proper_score_trials
SET settled=-1,realized_net=.70,closed_ts=? WHERE ticker=?`, closed, second.Ticker); err != nil {
		t.Fatal(err)
	}
	report, err := st.ProperScoreReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	row := report["transforms"].(map[string]any)["spherical"].(map[string]any)
	if row["manifest_unsupported_slots"].(int) != 1 ||
		row["normalized_executable_completed_slots"].(int) != 0 ||
		row["expected_net"].(float64) != 0 || row["realized_net"].(float64) != 0 ||
		row["l1_weighted_expected_net"].(float64) != 0 ||
		row["l1_weighted_realized_net"].(float64) != 0 {
		t.Fatalf("unsupported terminal vector leaked into economic headlines: %#v", row)
	}
}

func TestProperScoreComparatorReportExcludesPartialVectorEconomics(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	slot := "2026-07-13T20:00:00Z"
	coordinate := func(name string) ProperScoreCoordinate {
		return ProperScoreCoordinate{Platform: "kalshi", Ticker: name, ForecastOriginSide: "YES",
			ForecastSignal: "comparator-fixture", ForecastSource: "ml-book-v2-calibrated",
			ForecastVersion: "fixture-v1", Route: "taker"}
	}
	if _, _, err := st.RegisterProperScoreManifest(ctx, ProperScoreManifest{
		Slot: slot, Transform: "log", StrategyMode: "fundamental",
		ForecastSource: "ml-book-v2-calibrated", ForecastVersion: "fixture-v1",
		Coordinates: []ProperScoreCoordinate{coordinate("a"), coordinate("b")},
	}); err != nil {
		t.Fatalf("freeze comparator manifest: %v", err)
	}
	insert := func(name string, target, expected float64) int64 {
		t.Helper()
		id, inserted, insertErr := st.InsertResearchSystemObservation(ctx, ResearchSystemObservation{
			Observed: time.Now().UTC(), SystemID: "proper-score-executor", ExperimentVersion: 1,
			OpportunityID: "proper-comparator-" + name, Kind: "control",
			Cohort:            "proper-score-comparator-v1|arm=equal-share|mode=fundamental|transform=log",
			Venue:             "kalshi",
			Ticker:            name,
			Route:             "observer",
			CertificateStatus: "not_applicable",
			SourceArtifact:    "proper-score storage fixture",
			OutcomeStatus:     "open",
			Blocker:           "zero-authority comparator fixture",
			Inputs: map[string]any{
				"comparator_arm":             "equal-share",
				"collection_slot":            slot,
				"matched_strategy_mode":      "fundamental",
				"matched_transform":          "log",
				"proper_coordinate_key":      ProperScoreCoordinateKey(coordinate(name)),
				"normalized_one_unit_target": target,
				"one_share_expected_net":     expected,
			},
		})
		if insertErr != nil || !inserted || id <= 0 {
			t.Fatalf("insert comparator %s: id=%d inserted=%v err=%v", name, id, inserted, insertErr)
		}
		return id
	}
	firstID := insert("a", .4, .10)
	secondID := insert("b", .6, .20)
	firstRealized := .30
	if inserted, err := st.AppendResearchPayoffUpdate(ctx, ResearchPayoffUpdate{
		ObservationID: firstID, Status: "settled", PayoutLower: 1, PayoutUpper: 1,
		RealizedNet: &firstRealized, SourceArtifact: "proper-score storage fixture",
		SourceHash: "comparator-a", Reason: "fixture settlement",
	}); err != nil || !inserted {
		t.Fatalf("settle first comparator: inserted=%v err=%v", inserted, err)
	}
	report, err := st.ProperScoreReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	partial := report["comparators"].(map[string]any)["metrics"].(map[string]any)["equal-share"].(map[string]any)
	if partial["rows"].(int) != 2 || partial["settled_rows"].(int) != 1 ||
		partial["completed_slots"].(int) != 0 || partial["target_l1"].(float64) != 0 ||
		partial["settled_l1"].(float64) != 0 || partial["l1_weighted_expected_net"].(float64) != 0 ||
		partial["l1_weighted_realized_net"].(float64) != 0 {
		t.Fatalf("partial comparator vector leaked into effectiveness: %#v", partial)
	}
	secondRealized := -.10
	if inserted, err := st.AppendResearchPayoffUpdate(ctx, ResearchPayoffUpdate{
		ObservationID: secondID, Status: "settled", PayoutLower: 0, PayoutUpper: 0,
		RealizedNet: &secondRealized, SourceArtifact: "proper-score storage fixture",
		SourceHash: "comparator-b", Reason: "fixture settlement",
	}); err != nil || !inserted {
		t.Fatalf("settle second comparator: inserted=%v err=%v", inserted, err)
	}
	report, err = st.ProperScoreReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	complete := report["comparators"].(map[string]any)["metrics"].(map[string]any)["equal-share"].(map[string]any)
	if complete["settled_rows"].(int) != 2 || complete["completed_slots"].(int) != 1 ||
		math.Abs(complete["target_l1"].(float64)-1) > 1e-9 ||
		math.Abs(complete["settled_l1"].(float64)-1) > 1e-9 ||
		math.Abs(complete["l1_weighted_expected_net"].(float64)-.16) > 1e-9 ||
		math.Abs(complete["l1_weighted_realized_net"].(float64)-.06) > 1e-9 {
		t.Fatalf("completed comparator vector metrics are wrong: %#v", complete)
	}
}
