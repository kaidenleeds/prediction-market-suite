package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r146ComboLegs(platform string, tickers ...string) []paper.Leg {
	out := make([]paper.Leg, 0, len(tickers))
	for _, ticker := range tickers {
		out = append(out, paper.Leg{Platform: platform, Ticker: ticker, Side: "YES", Entry: .70})
	}
	return out
}

func r146EnableMLComboExecutionFixture(s *Server) {
	r166AllowUnverifiedMultiLegPaperForTest(s)
	s.plabQuoteFn = func(_ context.Context, _ plabLeg) (float64, float64, string, bool) {
		return .70, 100, "fixture-current-ask", true
	}
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }
	s.fundedComboQuoteFn = func(_ context.Context, _ paper.Leg) (fundedComboLegQuote, bool) {
		return fundedComboLegQuote{Bid: .69, Ask: .70, Depth: 100, Tick: .01, QuoteAge: .01,
			QtyStep: 1, MinQty: 1, BookSource: "fixture-current-book", FeeSource: "fixture-exact-fee",
			QuantitySource: "fixture-whole-package"}, true
	}
	s.fundedComboFeeFn = func(paper.Leg, float64, float64) (float64, string, bool) {
		return 0, "fixture-exact-fee", true
	}
}

func TestR146MLComboPaperGrantLedgerAndResetAreIsolated(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.mutateCfg(func(c *config.Config) { c.Auto.BookMLCombosUSD, c.Auto.BookCombosUSD = 600, 600 })
	mlSettled, err := s.store.InsertParlay(ctx, paper.Parlay{Stake: 10, Fees: 1, Price: .49,
		Contracts: 20, Legs: r146ComboLegs("kalshi", "ML-K-A", "ML-K-B"), RouteSource: mlComboPaperRoute,
		Cohort: mlComboPaperCohort, SystemIDs: []string{"new-ml"}, JointP: .70, ExpectedNetPerDollar: .10})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.store.SettleParlay(ctx, mlSettled, 1, 20); err != nil {
		t.Fatal(err)
	}
	if _, err := s.store.InsertParlay(ctx, paper.Parlay{Stake: 10, Fees: 1, Price: .49,
		Contracts: 20, Legs: r146ComboLegs("polyus", "ML-P-A", "ML-P-B"), RouteSource: mlComboPaperRoute,
		Cohort: mlComboPaperCohort, SystemIDs: []string{"new-ml"}, JointP: .70, ExpectedNetPerDollar: .10}); err != nil {
		t.Fatal(err)
	}
	systemID, err := s.store.InsertParlay(ctx, paper.Parlay{Stake: 10, Price: .49, Contracts: 20,
		Legs: r146ComboLegs("kalshi", "SYS-A", "SYS-B"), RouteSource: positiveSystemComboRouteSource,
		Cohort: storage.ComboLabCohortRollingPositive, SystemIDs: []string{"system-a"}, JointP: .70,
		ExpectedNetPerDollar: .10})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.store.SettleParlay(ctx, systemID, 1, 100); err != nil {
		t.Fatal(err)
	}
	legacyID, err := s.store.InsertParlay(ctx, paper.Parlay{Stake: 5, Price: .49, Contracts: 10,
		Legs: r146ComboLegs("kalshi", "LEGACY-A", "LEGACY-B")})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.store.SettleParlay(ctx, legacyID, 0, -5); err != nil {
		t.Fatal(err)
	}

	st := s.mlComboPaperStatus(ctx)
	if st.Grant != 600 || math.Abs(st.Equity-620) > 1e-9 || math.Abs(st.Available-609) > 1e-9 ||
		st.Open != 1 || st.Closed != 1 || st.ByVenue["kalshi"].Realized != 20 || st.ByVenue["polyus"].Open != 1 {
		t.Fatalf("ML Combo money wall included another portfolio or lost its own rows: %+v", st)
	}
	if got := s.parlayOpenCost(ctx); got != 0 {
		t.Fatalf("ML Combo exposure leaked into System Combo availability: %.2f", got)
	}

	closed, err := s.resetMLComboPaperLocked(ctx)
	if err != nil || closed != 1 {
		t.Fatalf("ML Combo reset closed=%d err=%v", closed, err)
	}
	st = s.mlComboPaperStatus(ctx)
	if st.Equity != 600 || st.Available != 600 || st.Open != 0 || st.Closed != 0 || st.RealizedNet != 0 {
		t.Fatalf("ML Combo reset did not establish an independent clean epoch: %+v", st)
	}
	rows, err := s.store.ListParlays(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	mlRows, systemRows := 0, 0
	for _, row := range rows {
		if isMLComboPaper(row) {
			mlRows++
		}
		if isFundedPositiveSystemCombo(row) {
			systemRows++
		}
	}
	if mlRows != 2 || systemRows != 1 || len(rows) != 4 {
		t.Fatalf("portfolio reset deleted cross-route history: ml=%d system=%d", mlRows, systemRows)
	}
	if n, err := s.store.ResetParlayPnLExceptRouteCohort(ctx, mlComboPaperRoute, mlComboPaperCohort); err != nil || n != 2 {
		t.Fatalf("System/legacy Combo reset deleted=%d err=%v", n, err)
	}
	rows, _ = s.store.ListParlays(ctx, "")
	for _, row := range rows {
		if !isMLComboPaper(row) {
			t.Fatal("System/legacy Combo reset left a non-ML settlement")
		}
	}
	if len(rows) != 2 || !isMLComboPaper(rows[0]) || !isMLComboPaper(rows[1]) {
		t.Fatalf("System Combo route reset deleted ML Combo history: %+v", rows)
	}
}

func TestR146MLComboCandidateAndPlacementUseOwnKellyWall(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	r146EnableMLComboExecutionFixture(s)
	pool := []plabLeg{
		{Platform: "polyus", Ticker: "A", Side: "YES", Price: .70, PWin: .85, EVNet: .15, Depth: 100, EventKey: "event-a"},
		{Platform: "polyus", Ticker: "B", Side: "YES", Price: .70, PWin: .85, EVNet: .15, Depth: 100, EventKey: "event-b"},
		{Platform: "polyus", Ticker: "A", Side: "NO", Price: .30, PWin: .10, EVNet: -.20, Depth: 100, EventKey: "event-a"},
		{Platform: "kalshi", Ticker: "K", Side: "YES", Price: .70, PWin: .85, EVNet: .15, Depth: 100, EventKey: "event-k"},
	}
	st := newMLComboStatus()
	candidates := s.mlComboCandidates(ctx, pool, &st)
	if len(candidates) == 0 {
		t.Fatal("fresh same-venue New-ML legs produced no candidate")
	}
	for _, candidate := range candidates {
		seen := map[string]bool{}
		if len(candidate.Legs) < 2 || len(candidate.Legs) > 6 {
			t.Fatalf("candidate has invalid leg count: %+v", candidate)
		}
		for _, leg := range candidate.Legs {
			if leg.Platform != "polyus" || seen[leg.Ticker] {
				t.Fatalf("mixed venue or repeated venue+ticker survived: %+v", candidate.Legs)
			}
			seen[leg.Ticker] = true
		}
	}

	cand := candidates[0]
	legs := make([]paper.Leg, 0, len(cand.Legs))
	for _, leg := range cand.Legs {
		legs = append(legs, paper.Leg{Platform: leg.Platform, Ticker: leg.Ticker, Side: leg.Side, Entry: leg.Price})
	}
	contract := &fundedComboContract{RouteSource: mlComboPaperRoute, Cohort: mlComboPaperCohort,
		SystemIDs: []string{"new-ml"}, JointP: cand.JointP, ForceTaker: true, Portfolio: mlComboPaperPortfolio}
	id, err := s.placeParlayLegsWithContract(ctx, legs, 15, "new-ml-combo", contract)
	if err != nil || id == 0 {
		t.Fatalf("exact ML Combo placement id=%d err=%v", id, err)
	}
	rows, err := s.mlComboPaperRows(ctx)
	if err != nil || len(rows) != 1 {
		t.Fatalf("ML Combo ledger rows=%d err=%v", len(rows), err)
	}
	row := rows[0]
	if row.Stake > 15.000001 || row.Stake <= 0 || row.RouteSource != mlComboPaperRoute ||
		row.Cohort != mlComboPaperCohort || row.ExpectedNetPerDollar <= 0 {
		t.Fatalf("placement lost Kelly cap/provenance/economics: %+v", row)
	}
	for _, leg := range row.Legs {
		if leg.BookSource != "fixture-current-book" || leg.TouchDepth != 100 || leg.QuantityStep != 1 {
			t.Fatalf("final book/depth/quantity receipt missing: %+v", leg)
		}
	}
	if got := s.mlComboPaperAvailable(ctx); math.Abs(got-(600-row.Stake-row.Fees)) > 1e-9 {
		t.Fatalf("ML Combo available=%.4f does not reserve only its own position", got)
	}
	if got := s.parlayAvailableUSD(ctx); math.Abs(got-600) > 1e-9 {
		t.Fatalf("ML Combo position borrowed from System Combo bank: %.4f", got)
	}
}

func TestR146MLComboKellyUsesAllInFeesAndNeverBreaksExposureCap(t *testing.T) {
	feeFree, ok := mlComboPrincipalKelly(.60, .50, 0)
	if !ok || math.Abs(feeFree-.20) > 1e-9 {
		t.Fatalf("fee-free Kelly got %.9f ok=%v", feeFree, ok)
	}
	withFees, ok := mlComboPrincipalKelly(.60, .50, .10)
	want := (.60 - .50*1.10) / (1.10 * (1 - .50*1.10))
	if !ok || math.Abs(withFees-want) > 1e-9 || withFees >= feeFree {
		t.Fatalf("all-in fee Kelly got %.9f want %.9f feeFree %.9f", withFees, want, feeFree)
	}
	if stake, reason, ok := mlComboKellyStake(20, .90, .50, 0, .25); ok || stake != 0 || reason != "kelly_below_minimum_order" {
		t.Fatalf("$1 minimum overrode 2.5%% cap: stake %.2f reason %q ok=%v", stake, reason, ok)
	}
	if stake, reason, ok := mlComboKellyStake(600, .90, .50, 0, .25); !ok || reason != "" || stake > 15.0000001 {
		t.Fatalf("$600 Kelly stake escaped 2.5%% cap: stake %.4f reason %q ok=%v", stake, reason, ok)
	}
}

func TestR146MLComboAutoUsesOnlyFreshAuthorizedBookNativePredictions(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	r146EnableMLComboExecutionFixture(s)
	s.autoMu.Lock()
	s.autoOn = true
	s.autoMu.Unlock()
	predictions := make([]map[string]any, 0, 8)
	for i := 0; i < 8; i++ {
		predictions = append(predictions, map[string]any{"ticker": "PUS-" + string(rune('A'+i)), "side": "YES",
			"platform": "polyus", "title": fmt.Sprintf("distinct event %d", i), "price": .70, "p_win": .85, "ev_net": .15})
	}
	body, _ := json.Marshal(map[string]any{"execution_enabled": true, "paper_authority": true,
		"feature_schema": currentMLCohort, "model_version": "fixture-v2", "predictions": predictions})
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, "ml_predictions.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	s.autoPlaceMLComboPaper(ctx)
	st := s.mlComboPaperStatus(ctx)
	if st.State != "collecting" || st.Predictions != 8 || st.ExactLegs != 8 || st.Placed == 0 || st.Placed > mlComboPaperMaxPerTurn {
		t.Fatalf("fresh authorized New-ML collector did not place: %+v", st)
	}
	rows, err := s.store.ListParlays(ctx, "")
	if err != nil || len(rows) != st.Placed {
		t.Fatalf("collector rows=%d placed=%d err=%v", len(rows), st.Placed, err)
	}
	for _, row := range rows {
		if !isMLComboPaper(row) || len(row.Legs) < 2 || len(row.Legs) > 6 {
			t.Fatalf("collector leaked into legacy/System Combo route: %+v", row)
		}
	}
	if ps := s.positiveSystemComboStatus(ctx); ps.Open != 0 || ps.Closed != 0 {
		t.Fatalf("New-ML Combo rows polluted System Combo status: %+v", ps)
	}
	if before := len(rows); before > 0 {
		s.autoPlaceMLComboPaper(ctx) // 30-second collector receipt prevents duplicate churn.
		after, _ := s.store.ListParlays(ctx, "")
		if len(after) != before {
			t.Fatalf("same-cycle collector duplicated positions: before=%d after=%d", before, len(after))
		}
	}

	// A fresh file without the unanimous Paper receipt must not create another portfolio row.
	blocked, _ := json.Marshal(map[string]any{"execution_enabled": true, "paper_authority": false,
		"feature_schema": currentMLCohort, "predictions": predictions})
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, "ml_predictions.json"), blocked, 0o600); err != nil {
		t.Fatal(err)
	}
	_ = s.store.KVSet(ctx, mlComboPaperStatusKV, "{}")
	s.autoPlaceMLComboPaper(ctx)
	after, _ := s.store.ListParlays(ctx, "")
	if len(after) != len(rows) {
		t.Fatal("unauthorized New-ML predictions placed a combo")
	}
}

func TestR146MLComboKalshiRequiresRealCollectionShape(t *testing.T) {
	s := testServer(t)
	s.kmkts = map[string]kalshi.Market{
		"K-A": {Ticker: "K-A", EventTicker: "EV-A"},
		"K-B": {Ticker: "K-B", EventTicker: "EV-B"},
	}
	s.comboCols = []kalshi.MVCollection{{CollectionTicker: "COLL", SizeMin: 2, Events: map[string]bool{"EV-A": false, "EV-B": false},
		EventCfg: map[string]kalshi.MVEventCfg{"EV-A": {IsYesOnly: true, SizeMax: 1}, "EV-B": {IsYesOnly: true, SizeMax: 1}}}}
	s.comboColsAt = time.Now()
	legs := []plabLeg{{Platform: "kalshi", Ticker: "K-A", Side: "YES"}, {Platform: "kalshi", Ticker: "K-B", Side: "YES"}}
	if collection, ok := s.mlComboKalshiCollection(context.Background(), legs); !ok || collection != "COLL" {
		t.Fatalf("legal RFQ-shaped leg set was rejected: %q %v", collection, ok)
	}
	legs[0].Side = "NO"
	if collection, ok := s.mlComboKalshiCollection(context.Background(), legs); ok || collection != "" {
		t.Fatalf("NO on yes-only collection incorrectly claimed RFQ compatibility: %q %v", collection, ok)
	}
}

func TestR146MLComboKalshiCollectionIsFreshAndRevalidated(t *testing.T) {
	s := testServer(t)
	s.kmkts = map[string]kalshi.Market{
		"K-A": {Ticker: "K-A", EventTicker: "EV-A"},
		"K-B": {Ticker: "K-B", EventTicker: "EV-B"},
	}
	legal := kalshi.MVCollection{CollectionTicker: "COLL", SizeMin: 2,
		Events:   map[string]bool{"EV-A": false, "EV-B": false},
		EventCfg: map[string]kalshi.MVEventCfg{"EV-A": {IsYesOnly: true, SizeMax: 1}, "EV-B": {IsYesOnly: true, SizeMax: 1}}}
	s.comboCols, s.comboColsAt = []kalshi.MVCollection{legal}, time.Now()
	legs := r146ComboLegs("kalshi", "K-A", "K-B")
	if err := s.validateFreshMLComboCollection(context.Background(), legs, "COLL"); err != nil {
		t.Fatalf("fresh unchanged collection rejected: %v", err)
	}
	s.comboCols = []kalshi.MVCollection{{CollectionTicker: "OTHER", SizeMin: 2,
		Events: legal.Events, EventCfg: legal.EventCfg}}
	s.comboColsAt = time.Now()
	if err := s.validateFreshMLComboCollection(context.Background(), legs, "COLL"); err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("changed collection was not rejected: %v", err)
	}
	s.comboColsAt = time.Now().Add(-time.Minute)
	if _, err := s.freshComboCollections(context.Background(), 15*time.Second); err == nil {
		t.Fatal("stale collection cache was served on the money path")
	}
}

func TestR146MLComboPortfolioIsVisibleAndTruthfullyLabeled(t *testing.T) {
	for _, want := range []string{"New-ML Combo Paper", "book_ml_combos_usd", "five simulated portfolios",
		"ML Combo Paper bank $", "/api/ml-combo-paper", "not LIVE-transferable"} {
		if !strings.Contains(dashboardHTML, want) && !strings.Contains(newMLComboStatus().PolyUSLiveShape, want) {
			t.Fatalf("ML Combo UI/live-boundary truth missing %q", want)
		}
	}
}

func TestR146MLComboBriefingPreservesGrantAfterParentDeadline(t *testing.T) {
	s := testServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	line := s.mlComboPaperBriefLine(ctx)
	if !strings.Contains(line, "$600.00 eq") || !strings.Contains(line, "$600.00 avail") {
		t.Fatalf("canceled parent fabricated a zero ML Combo bankroll: %q", line)
	}
}
