package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestR145OperatorSystemCountUsesCanonicalCatalogNotVisibleRows(t *testing.T) {
	s := testServer(t)
	if err := s.store.Close(); err != nil {
		t.Fatal(err)
	}
	// These intentionally contain duplicate aliases and an unknown row. Neither the number of
	// currently visible verdicts nor database availability may change the immutable roster.
	verdicts := []verdictEnt{
		{Family: "kalshi-flow", Platform: "kalshi", Side: "YES", Route: "taker"},
		{Family: "taker:kalshi-flow@kalshi [YES]", Platform: "kalshi", Side: "YES", Route: "taker"},
		{Family: "temporary-row-that-is-not-catalogued", Platform: "polyus", Side: "NO", Route: "taker"},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	variants, systems, ok := s.trackedSystemCounts(ctx, verdicts)
	if !ok || systems != 119 || variants != 302 {
		t.Fatalf("operator roster = %d systems / %d variants / ok=%v; want 119 / 302 / true",
			systems, variants, ok)
	}
}

func TestR145SystemsAPIExposesStableCatalogAndLowerBoundContract(t *testing.T) {
	s := testServer(t)
	// Keep the endpoint cache-only. The count contract itself must not start or await a digest.
	s.researchDigestMu.Lock()
	s.researchDigestJSON = []byte(`{"state":"READY"}`)
	s.researchDigestAt = time.Now()
	s.researchDigestNext = time.Now().Add(time.Hour)
	s.researchDigestMu.Unlock()

	rec := httptest.NewRecorder()
	s.handleLeaderboardBacktest(rec, httptest.NewRequest(http.MethodGet, "/api/systems-leaderboard", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("systems endpoint = %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Counts struct {
			BaseSystems                int      `json:"base_systems"`
			OrderSystems               int      `json:"order_originating_base_systems"`
			ChildOverlays              int      `json:"executable_child_overlays"`
			ClassifiedNonOrder         int      `json:"classified_non_order_bases"`
			DeclaredTyped              int      `json:"declared_typed_variants"`
			CodePathConnected          int      `json:"code_path_connected_variants"`
			DeclaredDisconnected       int      `json:"declared_without_code_path_variants"`
			SignalContracts            int      `json:"signal_contract_variants"`
			ExecutionDerivatives       int      `json:"execution_derivative_contracts"`
			ProducerCapable            int      `json:"producer_capable_typed_variants"`
			ProducerBlocked            int      `json:"producer_blocked_typed_variants"`
			CrossVenueInputs           int      `json:"cross_venue_input_variants"`
			BundleSystems              int      `json:"bundle_product_systems"`
			BundleVariants             int      `json:"declared_bundle_product_variants"`
			ConnectedBundles           int      `json:"connected_staged_bundle_variants"`
			BlockedBundles             int      `json:"externally_blocked_bundle_variants"`
			KnownVariantsCompatibility int      `json:"known_exact_execution_variants"`
			ExactVariantsCompatibility int      `json:"exact_execution_variants"`
			MissingTyped               int      `json:"missing_typed_variant_systems"`
			MissingIDs                 []string `json:"missing_typed_variant_system_ids"`
			MissingExecutable          int      `json:"missing_executable_variants"`
			MissingExecutableIDs       []string `json:"missing_executable_system_ids"`
			ExternallyBlocked          int      `json:"externally_blocked_decision_systems"`
			ExternallyBlockedIDs       []string `json:"externally_blocked_decision_system_ids"`
			AliasesExcluded            int      `json:"aliases_excluded"`
			ComponentsExcluded         int      `json:"components_excluded"`
			LowerBound                 bool     `json:"variant_count_is_lower_bound"`
			Contract                   string   `json:"contract"`
			VisibleLeaderboardRows     int      `json:"visible_leaderboard_rows"`
		} `json:"counts"`
		SystemCatalog   []r145SystemCatalogEntry `json:"system_catalog"`
		SignalContracts struct {
			Total                  int            `json:"signal_contracts"`
			TypedExecutionVariants int            `json:"typed_execution_variants"`
			DerivativeContracts    int            `json:"derivative_contracts"`
			ProducerCapable        int            `json:"producer_capable_typed_variants"`
			Blocked                int            `json:"producer_blocked_typed_variants"`
			CrossVenue             int            `json:"cross_venue_input_variants"`
			FreshFedRuntime        string         `json:"fresh_fed_runtime"`
			ByTopology             map[string]int `json:"by_input_topology"`
		} `json:"variant_signal_contracts"`
		SignalMatrix []struct {
			SystemID        string `json:"system_id"`
			ExecutionVenue  string `json:"execution_venue"`
			InputTopology   string `json:"input_topology"`
			CrossVenue      bool   `json:"cross_venue"`
			Producer        bool   `json:"producer_capable"`
			FreshFedRuntime string `json:"fresh_fed_runtime"`
			Blocker         string `json:"blocker"`
		} `json:"variant_signal_matrix"`
		SignalRuntime struct {
			TypedVariants           int            `json:"typed_variants"`
			SignalContracts         int            `json:"signal_contracts"`
			DerivativeContracts     int            `json:"derivative_contracts"`
			CurrentSignal           int            `json:"current_signal"`
			Terminal                int            `json:"terminal"`
			Excluded                int            `json:"excluded"`
			NotApplicable           int            `json:"not_applicable"`
			RealisticPaperContracts int            `json:"realistic_paper_contracts"`
			LegacyOrOtherContracts  int            `json:"legacy_or_other_paper_contracts"`
			ByPaperExecution        map[string]int `json:"by_paper_execution"`
			Rows                    []struct {
				InputTopology    string `json:"input_topology"`
				SignalContractID string `json:"signal_contract_id"`
				State            string `json:"state"`
			} `json:"rows"`
		} `json:"variant_signal_runtime"`
		CountSemantics map[string]string          `json:"count_semantics"`
		Coverage       map[string]json.RawMessage `json:"coverage"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	c := body.Counts
	if c.BaseSystems != 119 || c.DeclaredTyped != 302 || c.CodePathConnected != 302 ||
		c.DeclaredDisconnected != 0 || c.KnownVariantsCompatibility != 302 ||
		c.ExactVariantsCompatibility != 302 ||
		c.MissingTyped != 28 || len(c.MissingIDs) != 28 || !c.LowerBound ||
		c.BundleSystems != 4 || c.BundleVariants != 10 || c.ConnectedBundles != 4 || c.BlockedBundles != 6 ||
		c.AliasesExcluded == 0 || c.ComponentsExcluded == 0 {
		t.Fatalf("canonical API counts are incomplete: %+v", c)
	}
	if c.OrderSystems != 89 || c.ChildOverlays != 1 || c.ClassifiedNonOrder != 30 ||
		c.MissingExecutable != 0 || len(c.MissingExecutableIDs) != 0 || c.ExternallyBlocked != 1 ||
		len(c.ExternallyBlockedIDs) != 1 || body.Counts.ExternallyBlockedIDs[0] != "xvlock" || len(body.SystemCatalog) != 119 {
		t.Fatalf("execution classification is incomplete: counts=%+v catalog=%d", c, len(body.SystemCatalog))
	}
	if c.SignalContracts != body.SignalContracts.Total ||
		c.ExecutionDerivatives != 6 ||
		body.SignalContracts.DerivativeContracts != 6 ||
		c.DeclaredTyped != body.SignalContracts.TypedExecutionVariants ||
		c.ProducerCapable != body.SignalContracts.ProducerCapable ||
		c.ProducerBlocked != body.SignalContracts.Blocked ||
		c.CrossVenueInputs != body.SignalContracts.CrossVenue ||
		c.SignalContracts != len(body.SignalMatrix) ||
		c.ProducerCapable+c.ProducerBlocked != c.DeclaredTyped ||
		c.SignalContracts < c.DeclaredTyped || c.CrossVenueInputs <= 0 ||
		body.SignalContracts.FreshFedRuntime != "not_measured" {
		t.Fatalf("signal-contract denominators disagree: counts=%+v contracts=%+v rows=%d",
			c, body.SignalContracts, len(body.SignalMatrix))
	}
	for _, key := range []string{"base_system", "order_system", "typed_variant", "signal_contract",
		"execution_derivative", "producer_capable", "fresh_fed", "runtime_state", "cross_venue"} {
		if strings.TrimSpace(body.CountSemantics[key]) == "" {
			t.Fatalf("count semantics omitted %q: %+v", key, body.CountSemantics)
		}
	}
	for _, topology := range []string{"K-PUS", "K-PINT", "PUS-PINT", "K-PUS-PINT"} {
		if body.SignalContracts.ByTopology[topology] <= 0 {
			t.Fatalf("cross-venue topology %q has no explicit variants: %+v",
				topology, body.SignalContracts.ByTopology)
		}
	}
	for _, row := range body.SignalMatrix {
		if row.ExecutionVenue == "polymarket" {
			t.Fatalf("PINT leaked into a money destination: %+v", row)
		}
		if row.CrossVenue && strings.TrimSpace(row.InputTopology) == "" {
			t.Fatalf("cross-venue variant lost its input topology: %+v", row)
		}
		if !row.Producer && strings.TrimSpace(row.Blocker) == "" {
			t.Fatalf("producer-blocked variant has no exact reason: %+v", row)
		}
		if row.FreshFedRuntime != "not_measured" {
			t.Fatalf("static connectivity was mislabeled as runtime freshness: %+v", row)
		}
	}
	runtime := body.SignalRuntime
	if runtime.TypedVariants != c.DeclaredTyped || runtime.SignalContracts != c.SignalContracts ||
		runtime.DerivativeContracts != 6 ||
		len(runtime.Rows) != runtime.SignalContracts ||
		runtime.CurrentSignal+runtime.Terminal+runtime.Excluded+runtime.NotApplicable != runtime.SignalContracts ||
		runtime.RealisticPaperContracts+runtime.LegacyOrOtherContracts != runtime.SignalContracts {
		t.Fatalf("exact runtime denominator is incomplete: %+v", runtime)
	}
	executionClassTotal := 0
	for _, n := range runtime.ByPaperExecution {
		executionClassTotal += n
	}
	if executionClassTotal != runtime.SignalContracts ||
		runtime.ByPaperExecution["delayed_two_touch"]+
			runtime.ByPaperExecution["delayed_newer_bid_sell_fok"] != runtime.RealisticPaperContracts ||
		runtime.ByPaperExecution["delayed_newer_bid_sell_fok"] != 6 ||
		runtime.RealisticPaperContracts != 354 || runtime.LegacyOrOtherContracts != 31 ||
		runtime.ByPaperExecution["log_only_maker"] != 18 ||
		runtime.ByPaperExecution["log_only_book_native_ml"] != 8 ||
		runtime.ByPaperExecution["log_only_registered_non_taker"] != 4 ||
		runtime.ByPaperExecution["log_only_rfq_not_observed"] != 1 {
		t.Fatalf("Paper execution classes are incomplete: %+v", runtime.ByPaperExecution)
	}
	for _, row := range runtime.Rows {
		if row.InputTopology == "" || row.SignalContractID == "" ||
			(row.State != "current_signal" && row.State != "terminal" &&
				row.State != "excluded" && row.State != "not_applicable") {
			t.Fatalf("runtime row lacks exact identity/state: %+v", row)
		}
	}
	if !strings.Contains(c.Contract, "lower bound") || !strings.Contains(c.Contract, "aliases") ||
		!strings.Contains(c.Contract, "code-path-connected") || !strings.Contains(c.Contract, "fresh collector") {
		t.Fatalf("count contract does not explain its denominator: %q", c.Contract)
	}
	if _, misleading := body.Coverage["scoreboard_strategies"]; misleading {
		t.Fatal("coverage-row count is still exposed as a system count")
	}
	if _, ok := body.Coverage["coverage_rows"]; !ok {
		t.Fatal("exact coverage-row denominator is missing")
	}
}

func TestR145BriefingAndDashboardLabelCatalogCountsTruthfully(t *testing.T) {
	s := testServer(t)
	brief := s.briefScoreboard(context.Background())
	for _, want := range []string{
		"119 bases · 89 order systems",
		"typed variants",
		"producer-capable /",
		"fresh-fed not inferred",
		"cross-venue-input variants",
		"input topology ≠ order venue · PINT input-only; orders K/PUS",
		"1 multi-leg decisions externally blocked",
		"xvlock",
	} {
		if !strings.Contains(brief, want) {
			t.Fatalf("briefing omitted canonical count label %q:\n%s", want, brief)
		}
	}
	for _, want := range []string{
		"System catalog",
		"base names",
		"order systems",
		"typed venue/side/order variants",
		"multi-leg product systems",
		"exact bundle variants",
		"fresh-fed is runtime-only",
		"Different execution venue, side, or order type = a different variant",
		"Cross-venue inputs are separate from the order venue; PINT is input-only",
		"Controls, cohorts, and ledgers are not executable systems",
		"does not claim a fresh signal, positive economics, Paper permission, or LIVE authority",
		"The table below is evidence/coverage, so its row count is not the catalog total",
		"Exact runtime paths",
		"delayed two-touch paths",
		"no current opportunity",
	} {
		if !strings.Contains(dashboardHTML, want) {
			t.Fatalf("dashboard omitted canonical count label %q", want)
		}
	}
}

func TestR147CatalogSeparatesCrossVenueInputsFromMoneyDestinations(t *testing.T) {
	byID := map[string]r145SystemCatalogEntry{}
	for _, row := range r145CanonicalBaseSystems() {
		byID[row.ID] = row
	}
	xv := byID["xvlock"]
	if xv.ExecutionRole != r145ExecutionUnsupported || xv.RequiresOrderRoute ||
		len(xv.ProductVariants) != 4 {
		t.Fatalf("xvlock was painted as an executable single-order system: %+v", xv)
	}
	hasInput := map[string]bool{}
	for _, venue := range xv.InputVenues {
		hasInput[venue] = true
	}
	if !hasInput["kalshi"] || !hasInput["polyus"] || !hasInput["polymarket"] {
		t.Fatalf("xvlock input topology is incomplete: %v", xv.InputVenues)
	}
	for _, venue := range xv.ExecutionVenues {
		if venue == "polymarket" {
			t.Fatalf("PINT leaked into executable destinations: %v", xv.ExecutionVenues)
		}
	}
	control := byID["identity-challenged-cross-venue-lock"]
	if control.ExecutionRole != r145ExecutionDataControl || control.RequiresOrderRoute {
		t.Fatalf("identity control masquerades as an order system: %+v", control)
	}
}
