package server

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r147RuntimeRow(rows []r147SystemVariantRuntimeRow, system, venue, side, route string) (r147SystemVariantRuntimeRow, bool) {
	for _, row := range rows {
		if row.SystemID == system && row.ExecutionVenue == venue && row.Side == side && row.Route == route {
			return row, true
		}
	}
	return r147SystemVariantRuntimeRow{}, false
}

func r147RuntimeContractRow(rows []r147SystemVariantRuntimeRow, system, venue, side, route,
	topology string) (r147SystemVariantRuntimeRow, bool) {
	for _, row := range rows {
		if row.SystemID == system && row.ExecutionVenue == venue && row.Side == side &&
			row.Route == route && row.InputTopology == topology {
			return row, true
		}
	}
	return r147SystemVariantRuntimeRow{}, false
}

func TestR147DedicatedProducerBoundariesStampExactRuntimeFeeds(t *testing.T) {
	s := testServer(t)
	now := time.Now()
	s.cheapbandConsider(context.Background(), storage.Signal{Platform: "polyus", Ticker: "pus-r147-cheap",
		Title: "cheap", Side: "YES", SignalType: "favlong", EntryPrice: .05, ResolveHours: 1,
		ExecExpr: r147InputReceiptExpr("PUS", now)})
	req := httptest.NewRequest("POST", "/api/mlpost", bytes.NewBufferString(
		`{"row":{"ticker":"pus-r147-ml","side":"NO","platform":"polyus","contracts":1},"ref_price":0.4}`))
	s.handleMLPost(httptest.NewRecorder(), req)
	snapshot := s.r147SystemVariantRuntimeSnapshot(context.Background(), now.Add(time.Second))
	for _, want := range []struct {
		system, venue, side, route, topology, state string
	}{
		{"cheapband", "polyus", "YES", "taker", "PUS", "current_signal"},
		{"ml-book", "polyus", "NO", "maker", "PUS", "excluded"},
		{"ml-book", "polyus", "NO", "taker", "PUS", "excluded"},
	} {
		row, ok := r147RuntimeContractRow(snapshot.Rows, want.system, want.venue, want.side,
			want.route, want.topology)
		if !ok || !row.FreshFed || row.State != want.state {
			t.Fatalf("dedicated runtime feed %v missing: %+v ok=%t", want, row, ok)
		}
	}
}

func TestR147RuntimeReceiptsSeparateDirectInverseAndResearchVariants(t *testing.T) {
	s := testServer(t)
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	s.noteR147NativeSignalRuntime(storage.Signal{Platform: "kalshi", SignalType: "kalshi-flow",
		Side: "YES", Ticker: "KX-R147"}, now, true, "")
	s.noteR147ResearchCandidateRuntime(storage.ResearchPromotionCandidate{SystemID: "proper-score-executor",
		Venue: "polyus", Side: "NO", Route: "taker", Ticker: "pus-r147", Observed: now,
		Cohort: "proper-score-v2|transform=log|fixture=true"})
	snapshot := s.r147SystemVariantRuntimeSnapshot(context.Background(), now.Add(time.Second))

	direct, ok := r147RuntimeRow(snapshot.Rows, "kalshi-flow", "kalshi", "YES", "taker")
	if !ok || !direct.FreshFed || direct.State != "current_signal" {
		t.Fatalf("direct runtime receipt missing: %+v ok=%t", direct, ok)
	}
	// An inverse YES action consumes the base system's NO signal, not the base YES receipt.
	inverse, ok := r147RuntimeRow(snapshot.Rows, "invert:kalshi-flow", "kalshi", "YES", "taker")
	if !ok || inverse.FreshFed || inverse.State != "not_applicable" {
		t.Fatalf("inverse copied direct-side freshness: %+v ok=%t", inverse, ok)
	}
	research, ok := r147RuntimeContractRow(snapshot.Rows, "proper-score-executor", "polyus", "NO",
		"taker", "PUS-PROPER-LOG")
	if !ok || !research.FreshFed || research.ReceiptSource != "research_system_observations:candidate" {
		t.Fatalf("research candidate runtime receipt missing: %+v ok=%t", research, ok)
	}
	for _, topology := range []string{"PUS-PROPER-BRIER", "PUS-PROPER-SPHERICAL"} {
		sibling, found := r147RuntimeContractRow(snapshot.Rows, "proper-score-executor", "polyus",
			"NO", "taker", topology)
		if !found || sibling.FreshFed {
			t.Fatalf("proper transform receipt leaked into %s: %+v", topology, sibling)
		}
	}
}

func TestR147RuntimeReceiptExpiresWithoutBecomingStaticCapability(t *testing.T) {
	s := testServer(t)
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	s.noteR147NativeSignalRuntime(storage.Signal{Platform: "kalshi", SignalType: "kalshi-flow",
		Side: "YES", Ticker: "KX-R147"}, now, true, "")
	snapshot := s.r147SystemVariantRuntimeSnapshot(context.Background(), now.Add(r147SignalRuntimeFreshAge+time.Second))
	row, ok := r147RuntimeRow(snapshot.Rows, "kalshi-flow", "kalshi", "YES", "taker")
	if !ok || row.FreshFed || row.State != "not_applicable" || snapshot.Stale == 0 {
		t.Fatalf("stale receipt remained fresh: %+v snapshot=%+v", row, snapshot)
	}
}

func TestR147EveryExecutableRegisteredSystemStampsOnlyItsExactCandidateCell(t *testing.T) {
	s := testServer(t)
	now := time.Date(2026, 7, 15, 15, 0, 0, 0, time.UTC)
	selected := map[string]r145SystemCatalogVariant{}
	for _, variant := range r145KnownSystemVariants() {
		if _, registered := storage.SystemExecutionCapability(variant.SystemID); !registered {
			continue
		}
		if _, exists := selected[variant.SystemID]; !exists {
			selected[variant.SystemID] = variant
		}
	}
	executableSpecs := 0
	for _, spec := range storage.SystemExecutionSpecs() {
		if len(spec.ExactVariants()) > 0 {
			executableSpecs++
		}
	}
	if got, want := len(selected), executableSpecs; got != want {
		t.Fatalf("registered systems represented by catalog=%d, want %d", got, want)
	}
	wantFresh := map[string]bool{}
	for systemID, variant := range selected {
		s.noteR147ResearchCandidateRuntime(storage.ResearchPromotionCandidate{
			SystemID: systemID, Venue: variant.Venue, Side: variant.Side, Route: variant.Route,
			Ticker: "R147-" + systemID, Observed: now,
		})
		matches := r147ExactSignalContracts(systemID, variant.Venue, variant.Side, variant.Route)
		if len(matches) != 1 {
			continue // no input topology was present, so a multi-topology cell must stay unmeasured
		}
		wantFresh[r147SignalRuntimeKey(systemID, variant.Venue, variant.Side, variant.Route,
			matches[0].InputTopology)] = true
	}
	snapshot := s.r147SystemVariantRuntimeSnapshot(context.Background(), now.Add(time.Second))
	for _, row := range snapshot.Rows {
		if _, registered := storage.SystemExecutionCapability(row.SystemID); !registered {
			continue
		}
		key := r147SignalRuntimeKey(row.SystemID, row.ExecutionVenue, row.Side, row.Route, row.InputTopology)
		if wantFresh[key] {
			if !row.FreshFed || row.State != "current_signal" {
				t.Fatalf("selected exact cell did not become fresh: %+v", row)
			}
		} else if row.FreshFed {
			t.Fatalf("candidate receipt leaked into an unselected sibling cell: %+v", row)
		}
	}
}

func TestR166RuntimeCoverageRefreshChecksEveryExactContractWithoutInventingSignals(t *testing.T) {
	s := testServer(t)
	now := time.Date(2026, 7, 21, 18, 0, 0, 0, time.UTC)
	s.refreshR147SystemContractCoverage(now)
	snapshot := s.r147SystemVariantRuntimeSnapshot(context.Background(), now.Add(time.Second))
	if snapshot.TypedVariants != len(r145KnownSystemVariants()) ||
		snapshot.SignalContracts != len(r147SystemVariantSignalContracts()) ||
		len(snapshot.Rows) != snapshot.SignalContracts {
		t.Fatalf("exact denominators disagree: %+v rows=%d", snapshot, len(snapshot.Rows))
	}
	if !snapshot.RefreshCurrent || snapshot.LastRefreshAt == "" || snapshot.NotApplicable != snapshot.SignalContracts ||
		snapshot.CurrentSignal+snapshot.Terminal+snapshot.Excluded != 0 || snapshot.ObservedContracts != 0 ||
		snapshot.UnobservedContracts != snapshot.SignalContracts || snapshot.ObservedEvents != 0 {
		t.Fatalf("empty refresh invented activity or stayed stale: %+v", snapshot)
	}
	for _, row := range snapshot.Rows {
		if row.SignalContractID == "" || row.InputTopology == "" || row.CoverageCheckedAt == "" ||
			row.State != "not_applicable" {
			t.Fatalf("contract was not explicitly checked: %+v", row)
		}
	}
}

func TestR166RuntimeCoverageSeparatesTopologyAndRoute(t *testing.T) {
	s := testServer(t)
	now := time.Date(2026, 7, 21, 19, 0, 0, 0, time.UTC)
	s.noteR147SignalContractRuntime("arb", "kalshi", "YES", "taker", "K-PINT",
		"KX-ARB", "fixture", "signal", "", "", now, true, "")
	s.noteR147SignalContractRuntime("kalshi-flow", "kalshi", "YES", "taker", "K",
		"KX-FLOW", "fixture", "signal", "", "", now, true, "")
	snapshot := s.r147SystemVariantRuntimeSnapshot(context.Background(), now.Add(time.Second))
	got, ok := r147RuntimeContractRow(snapshot.Rows, "arb", "kalshi", "YES", "taker", "K-PINT")
	if !ok || got.State != "current_signal" {
		t.Fatalf("exact K-PINT signal missing: %+v ok=%t", got, ok)
	}
	other, ok := r147RuntimeContractRow(snapshot.Rows, "arb", "kalshi", "YES", "taker", "K-PUS")
	if !ok || other.State != "not_applicable" || other.FreshFed {
		t.Fatalf("K-PINT leaked into K-PUS: %+v ok=%t", other, ok)
	}
	maker, ok := r147RuntimeContractRow(snapshot.Rows, "kalshi-flow", "kalshi", "YES", "maker", "K")
	if !ok || maker.State != "not_applicable" || maker.FreshFed {
		t.Fatalf("taker signal leaked into maker route: %+v ok=%t", maker, ok)
	}
	if !got.Observed || got.ObservedEvents != 1 || maker.Observed ||
		snapshot.ObservedContracts != 2 || snapshot.ObservedEvents != 2 {
		t.Fatalf("observed runtime counts do not separate static rows from actual receipts: got=%+v maker=%+v snapshot=%+v",
			got, maker, snapshot)
	}
}

func TestR166WildcardRuntimeCannotPaintMultipleRoutesCurrent(t *testing.T) {
	s := testServer(t)
	now := time.Date(2026, 7, 21, 19, 30, 0, 0, time.UTC)
	s.noteR147SignalRuntime("kalshi-flow", "kalshi", "YES", "*", "KX-WILDCARD",
		"route-less-fixture", now, true, "")
	snapshot := s.r147SystemVariantRuntimeSnapshot(context.Background(), now.Add(time.Second))
	for _, route := range []string{"maker", "taker"} {
		row, ok := r147RuntimeContractRow(snapshot.Rows, "kalshi-flow", "kalshi", "YES", route, "K")
		if !ok || row.Observed || row.State != "not_applicable" {
			t.Fatalf("route-less receipt overclaimed %s: %+v ok=%t", route, row, ok)
		}
	}
}

func TestR166RuntimeCoverageRecordsRealisticPaperTerminalAndExclusion(t *testing.T) {
	s := testServer(t)
	now := time.Date(2026, 7, 21, 20, 0, 0, 0, time.UTC)
	sig := storage.Signal{Platform: "kalshi", SignalType: "spotlag", Side: "YES", Ticker: "KX-SPOT",
		ExecExpr: r147InputReceiptExpr("K-SPOT", now)}
	s.noteR147NativeSignalRuntime(sig, now, true, "")
	s.noteR147PaperRouteOutcome(sig, "taker", "REJECTED", "book-missing", now.Add(time.Second))
	snapshot := s.r147SystemVariantRuntimeSnapshot(context.Background(), now.Add(2*time.Second))
	row, ok := r147RuntimeContractRow(snapshot.Rows, "spotlag", "kalshi", "YES", "taker", "K-SPOT")
	if !ok || row.State != "excluded" || row.Outcome != "REJECTED" || row.Blocker != "book-missing" {
		t.Fatalf("exact exclusion missing: %+v ok=%t", row, ok)
	}
	s.noteR147PaperRouteOutcome(sig, "taker", "PAPER-ZERO-FILL", "price-moved",
		now.Add(3*time.Second))
	snapshot = s.r147SystemVariantRuntimeSnapshot(context.Background(), now.Add(4*time.Second))
	row, _ = r147RuntimeContractRow(snapshot.Rows, "spotlag", "kalshi", "YES", "taker", "K-SPOT")
	if row.State != "terminal" || row.Outcome != "PAPER-ZERO-FILL" || row.Blocker != "price-moved" {
		t.Fatalf("exact terminal missing: %+v", row)
	}
}

func TestR166InverseRuntimeRequiresItsOwnCompleteBook(t *testing.T) {
	s := testServer(t)
	now := time.Date(2026, 7, 21, 21, 0, 0, 0, time.UTC)
	base := storage.Signal{Platform: "kalshi", SignalType: "kalshi-flow", Side: "NO", Ticker: "KX-INV",
		ExecExpr: r147InputReceiptExpr("K", now)}
	s.noteR147NativeSignalRuntime(base, now, true, "")
	snapshot := s.r147SystemVariantRuntimeSnapshot(context.Background(), now.Add(time.Second))
	inverse, ok := r147RuntimeContractRow(snapshot.Rows, "invert:kalshi-flow", "kalshi", "YES", "taker", "K")
	if !ok || inverse.State != "not_applicable" || inverse.FreshFed {
		t.Fatalf("base signal invented inverse liveness: %+v ok=%t", inverse, ok)
	}
	inverseSig := base
	inverseSig.SignalType, inverseSig.Side = "invert:kalshi-flow", "YES"
	s.noteR147InverseOpportunity(inverseSig, unitTrialFunnelResult{
		QuoteAttempted: true, Reason: "independent_inverse_complete_book_unavailable"}, now.Add(2*time.Second))
	snapshot = s.r147SystemVariantRuntimeSnapshot(context.Background(), now.Add(3*time.Second))
	inverse, _ = r147RuntimeContractRow(snapshot.Rows, "invert:kalshi-flow", "kalshi", "YES", "taker", "K")
	if inverse.State != "excluded" {
		t.Fatalf("failed inverse book was not an exact exclusion: %+v", inverse)
	}
	s.noteR147InverseOpportunity(inverseSig, unitTrialFunnelResult{
		QuoteAttempted: true, MoneyTruth: true, Reason: "independent_inverse_signal_and_money_truth_stored"},
		now.Add(4*time.Second))
	snapshot = s.r147SystemVariantRuntimeSnapshot(context.Background(), now.Add(5*time.Second))
	inverse, _ = r147RuntimeContractRow(snapshot.Rows, "invert:kalshi-flow", "kalshi", "YES", "taker", "K")
	if inverse.State != "current_signal" || !inverse.FreshFed || inverse.Outcome != "complete_book_current" {
		t.Fatalf("genuine inverse complete book did not become current: %+v", inverse)
	}
}

func TestR166RuntimeCoverageLabelsEveryPaperContractRealisticOrLogOnly(t *testing.T) {
	s := testServer(t)
	snapshot := s.r147SystemVariantRuntimeSnapshot(context.Background(), time.Now())
	t.Logf("runtime contracts=%d realistic=%d other=%d classes=%v", snapshot.SignalContracts,
		snapshot.RealisticPaperContracts, snapshot.LegacyOrOtherContracts, snapshot.ByPaperExecution)
	if snapshot.SignalContracts != 385 || snapshot.RealisticPaperContracts != 354 ||
		snapshot.LegacyOrOtherContracts != 31 ||
		snapshot.DerivativeContracts != 6 ||
		snapshot.RealisticPaperContracts+snapshot.LegacyOrOtherContracts != snapshot.SignalContracts {
		t.Fatalf("Paper execution categories do not cover every contract: %+v", snapshot)
	}
	for _, row := range snapshot.Rows {
		if strings.HasPrefix(row.PaperExecution, "log_only_") && row.RealisticPaper {
			t.Fatalf("log-only route was mislabeled realistic: %+v", row)
		}
		if row.SystemID == "proper-score-executor" && row.Route == "taker" && !row.RealisticPaper {
			t.Fatalf("migrated research taker lost its delayed execution label: %+v", row)
		}
		if row.SystemID == "proper-score-momentum" &&
			(row.Action != "SELL" || row.TimeInForce != "fill_or_kill" || !row.ReduceOnly ||
				!row.ExecutionDerivative || row.LiveAuthority || !row.RealisticPaper ||
				row.PaperExecution != "delayed_newer_bid_sell_fok") {
			t.Fatalf("proper-score SELL derivative truth drifted: %+v", row)
		}
		if row.SystemID == "spotlag" && row.Route == "taker" && !row.RealisticPaper {
			t.Fatalf("generic corrected Paper route lost its execution label: %+v", row)
		}
		if row.SystemID == "payoff-constraint-solver" &&
			(row.Side != "YES" || row.Route != "rfq" || row.PaperExecution != "log_only_rfq_not_observed" ||
				row.RealisticPaper) {
			t.Fatalf("payoff RFQ was not represented as a log-only YES conjunction route: %+v", row)
		}
	}
}
