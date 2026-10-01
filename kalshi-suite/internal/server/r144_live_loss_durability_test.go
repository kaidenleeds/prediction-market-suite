package server

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/killswitch"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

func testKalshiCumulativePosition(t *testing.T, ticker string, realized, fees float64) kalshi.MarketPosition {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"ticker": ticker, "position_fp": "0", "total_traded_dollars": "1.00",
		"realized_pnl_dollars": realized, "fees_paid_dollars": fees,
	})
	if err != nil {
		t.Fatal(err)
	}
	var row kalshi.MarketPosition
	if err := json.Unmarshal(b, &row); err != nil {
		t.Fatalf("decode test settlement: %v", err)
	}
	return row
}

func testKalshiLossSettlement(t *testing.T, ticker string, cost, revenue, fee float64, settled time.Time) kalshi.Settlement {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"ticker": ticker, "market_result": "no", "yes_count_fp": "1.00", "no_count_fp": "0.00",
		"yes_total_cost_dollars": cost, "no_total_cost_dollars": 0,
		"revenue": revenue * 100, "fee_cost": fee, "settled_time": settled.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatal(err)
	}
	var row kalshi.Settlement
	if err := json.Unmarshal(b, &row); err != nil {
		t.Fatalf("decode test settlement: %v", err)
	}
	return row
}

func TestR144LiveLossLedgerSurvivesRestart(t *testing.T) {
	s := testServer(t)
	day := liveLossDay(time.Now())
	s.liveLossDay, s.liveLossHasBase = day, true
	s.liveLossDeltaV = map[string]int64{"kalshi": liveLossUSDToUnits(-17.50), "polyus": liveLossUSDToUnits(4.25)}
	s.liveLossSeenV = map[string]map[string]bool{
		"kalshi": {"settlement-a": true}, "polyus": {"activity-b": true},
	}
	s.liveLossLastV = map[string]map[string]int64{"kalshi": {"KX-CLOSED": liveLossUSDToUnits(-17.50)}}
	s.liveLossFinalV = map[string]map[string]int64{"kalshi": {"KX-FINAL": liveLossUSDToUnits(4.25)}, "polyus": {}}
	s.liveLossBootV = map[string]bool{"kalshi": true, "polyus": true}
	if err := s.persistLiveLossLedger(context.Background()); err != nil {
		t.Fatalf("persist durable ledger: %v", err)
	}

	restarted := &Server{store: s.store, ks: killswitch.New(nil)}
	cfg := config.Default()
	cfg.DataDir = s.cfg().DataDir
	restarted.cfgP.Store(&cfg)
	if err := restarted.loadLatches(context.Background()); err != nil {
		t.Fatalf("load durable ledger: %v", err)
	}
	if got := restarted.liveLossDeltaV["kalshi"]; got != liveLossUSDToUnits(-17.50) {
		t.Fatalf("restart lost Kalshi realized P&L: got %d cents", got)
	}
	if got := restarted.liveLossDeltaV["polyus"]; got != liveLossUSDToUnits(4.25) {
		t.Fatalf("restart lost PolyUS realized P&L: got %d cents", got)
	}
	if !restarted.liveLossSeenV["kalshi"]["settlement-a"] || !restarted.liveLossSeenV["polyus"]["activity-b"] {
		t.Fatal("restart lost receipt watermarks")
	}
	if restarted.liveLossLastV["kalshi"]["KX-CLOSED"] != liveLossUSDToUnits(-17.50) || !restarted.liveLossBootV["kalshi"] {
		t.Fatal("restart lost Kalshi cumulative total-traded watermark")
	}
	if restarted.liveLossFinalV["kalshi"]["KX-FINAL"] != liveLossUSDToUnits(4.25) {
		t.Fatal("restart lost Kalshi settlement tombstone")
	}
	if restarted.liveLossTruthV["kalshi"] || restarted.liveLossTruthV["polyus"] {
		t.Fatal("restart must restore money but not invent fresh venue truth")
	}
}

func TestR153LegacySettlementBaselineMigrationRebuildsCurrentDayLoss(t *testing.T) {
	s := testServer(t)
	day := liveLossDay(time.Now())
	legacy := liveLossPersisted{Day: day,
		DeltaV: map[string]int64{"kalshi": -227, "polyus": 0},
		SeenV:  map[string]map[string]bool{"kalshi": {}, "polyus": {}},
		LastV:  map[string]map[string]int64{"kalshi": {"OPEN-FEES": -227}, "polyus": {}},
		FinalV: map[string]map[string]int64{"kalshi": {"OLD-BASELINED": -2192}, "polyus": {}},
		BootV:  map[string]bool{"kalshi": true, "kalshi-settlements": true}}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.store.KVSet(context.Background(), kvLiveLossLedger, string(raw)); err != nil {
		t.Fatal(err)
	}
	restarted := &Server{store: s.store, ks: killswitch.New(nil)}
	cfg := config.Default()
	cfg.DataDir = s.cfg().DataDir
	restarted.cfgP.Store(&cfg)
	if err := restarted.loadLatches(context.Background()); err != nil {
		t.Fatal(err)
	}
	if restarted.liveLossDeltaV["kalshi"] != 0 || len(restarted.liveLossLastV["kalshi"]) != 0 ||
		len(restarted.liveLossFinalV["kalshi"]) != 0 || len(restarted.liveLossBootV) != 0 {
		t.Fatalf("legacy ledger was not cleared for exact UTC-day rebuild: delta=%d boot=%v",
			restarted.liveLossDeltaV["kalshi"], restarted.liveLossBootV)
	}
	settledAt := time.Now().UTC()
	finals := map[string]kalshiLossSettlementFinal{
		"OLD-BASELINED":     {Cents: liveLossUSDToUnits(-21.92), Day: day, At: settledAt},
		"ALL-SETTLED-TODAY": {Cents: liveLossUSDToUnits(-51.56), Day: day, At: settledAt},
	}
	if err := restarted.applyKalshiSettlementWindow(day, finals, true); err != nil {
		t.Fatal(err)
	}
	openFee := liveLossUSDToUnits(-2.27)
	if err := restarted.applyKalshiCumulativeLossBootstrap(day,
		map[string]int64{"OPEN-FEES": openFee}, map[string]int64{"OPEN-FEES": openFee}); err != nil {
		t.Fatal(err)
	}
	if got := restarted.liveLossDeltaV["kalshi"]; got != liveLossUSDToUnits(-75.75) {
		t.Fatalf("rebuilt daily loss=%f USD, want -75.75", liveLossUnitsToUSD(got))
	}
}

func TestR144ClosedPositionDisappearanceCannotEraseRealizedLoss(t *testing.T) {
	s := testServer(t)
	day := liveLossDay(time.Now())
	baseline, err := kalshiCumulativeLossValues([]kalshi.MarketPosition{
		testKalshiCumulativePosition(t, "KX-CLOSED", 0, 0),
	})
	if err != nil {
		t.Fatalf("baseline cumulative ledger: %v", err)
	}
	if err := s.applyKalshiCumulativeLoss(day, baseline); err != nil {
		t.Fatal(err)
	}
	closed, err := kalshiCumulativeLossValues([]kalshi.MarketPosition{
		testKalshiCumulativePosition(t, "KX-CLOSED", -0.70, 0.01),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.applyKalshiCumulativeLoss(day, closed); err != nil {
		t.Fatal(err)
	}
	if got := s.liveLossDeltaV["kalshi"]; got != liveLossUSDToUnits(-.71) {
		t.Fatalf("closed fee-net loss=%f USD, want -0.71", liveLossUnitsToUSD(got))
	}

	// A missing row remains fail-closed until its authenticated, fee-known settlement arrives.
	if err := s.applyKalshiCumulativeLoss(day, map[string]int64{}); err == nil {
		t.Fatal("disappeared total-traded row must fail closed")
	}
	if got := s.liveLossDeltaV["kalshi"]; got != liveLossUSDToUnits(-.71) {
		t.Fatalf("disappeared closed row erased/changed loss: got %d cents", got)
	}
	missing := s.kalshiMissingCumulativeTickers(map[string]int64{})
	finals, err := kalshiLossSettlementFinals([]kalshi.Settlement{
		testKalshiLossSettlement(t, "KX-CLOSED", .70, 0, .01, time.Now()),
	}, missing)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.applyKalshiSettlementFinals(day, missing, finals); err != nil {
		t.Fatal(err)
	}
	if err := s.applyKalshiCumulativeLoss(day, map[string]int64{}); err != nil {
		t.Fatalf("authenticated settlement did not finalize disappeared row: %v", err)
	}
	if got := s.liveLossDeltaV["kalshi"]; got != liveLossUSDToUnits(-.71) {
		t.Fatalf("settlement final double-counted/changed loss: got %d cents", got)
	}
	// A later stale total_traded echo must not resurrect or double-count the finalized market.
	if err := s.applyKalshiCumulativeLoss(day, map[string]int64{"KX-CLOSED": -71}); err != nil {
		t.Fatal(err)
	}
	if got := s.liveLossDeltaV["kalshi"]; got != liveLossUSDToUnits(-.71) {
		t.Fatalf("stale cumulative echo double-counted final loss: got %d cents", got)
	}
}

func TestR152MissingKalshiRowWithoutExactSettlementRemainsBlocked(t *testing.T) {
	s := testServer(t)
	day := liveLossDay(time.Now())
	if err := s.applyKalshiCumulativeLoss(day, map[string]int64{"KX-MISSING": -12}); err != nil {
		t.Fatal(err)
	}
	missing := s.kalshiMissingCumulativeTickers(map[string]int64{})
	if len(missing) != 1 || missing[0] != "KX-MISSING" {
		t.Fatalf("missing=%v", missing)
	}
	if err := s.applyKalshiSettlementFinals(day, missing, map[string]kalshiLossSettlementFinal{}); err == nil {
		t.Fatal("missing exact settlement was allowed to finalize")
	}
	if err := s.applyKalshiCumulativeLoss(day, map[string]int64{}); err == nil {
		t.Fatal("unproved disappearance stopped failing closed")
	}
}

func TestR152KalshiFillAndSettlementEntirelyBetweenPositionPollsIsCounted(t *testing.T) {
	s := testServer(t)
	day := liveLossDay(time.Now())
	// Establish both authenticated baselines before the fast trade exists.
	if err := s.applyKalshiCumulativeLoss(day, map[string]int64{}); err != nil {
		t.Fatal(err)
	}
	if err := s.applyKalshiSettlementWindow(day, map[string]kalshiLossSettlementFinal{}, true); err != nil {
		t.Fatal(err)
	}
	// The next position snapshot is still empty, but the venue settlement feed now contains the
	// terminal receipt: 70c cost + 1c fee and no payout = -71c. It must not vanish between polls.
	rows := []kalshi.Settlement{testKalshiLossSettlement(t, "KX-FAST", .70, 0, .01, time.Now())}
	finals, complete, err := kalshiLossSettlementWindow(day, rows, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.applyKalshiSettlementWindow(day, finals, complete); err != nil {
		t.Fatal(err)
	}
	if err := s.applyKalshiCumulativeLoss(day, map[string]int64{}); err != nil {
		t.Fatal(err)
	}
	if got := s.liveLossDeltaV["kalshi"]; got != liveLossUSDToUnits(-.71) {
		t.Fatalf("between-poll settlement delta=%f USD, want -0.71", liveLossUnitsToUSD(got))
	}
	// Re-reading the same authenticated page is idempotent.
	if err := s.applyKalshiSettlementWindow(day, finals, complete); err != nil {
		t.Fatal(err)
	}
	if got := s.liveLossDeltaV["kalshi"]; got != liveLossUSDToUnits(-.71) {
		t.Fatalf("replayed settlement double-counted: %d cents", got)
	}
}

func TestR153InitialKalshiSettlementRebuildRequiresCompleteUTCDay(t *testing.T) {
	s := testServer(t)
	day := liveLossDay(time.Now())
	s.liveLossDay, s.liveLossHasBase = day, true
	s.liveLossEpochAt = liveLossDayStart(day)
	s.liveLossDeltaV = map[string]int64{"kalshi": 0, "polyus": 0}
	s.liveLossLastV = map[string]map[string]int64{"kalshi": {}, "polyus": {}}
	s.liveLossFinalV = map[string]map[string]int64{"kalshi": {}, "polyus": {}}
	s.liveLossBootV = map[string]bool{"kalshi": true}
	if err := s.applyKalshiSettlementWindow(day, map[string]kalshiLossSettlementFinal{}, false); err == nil {
		t.Fatal("truncated first settlement window was accepted as full-day loss truth")
	}
}

func TestR144OneVenueOutageDoesNotBlindOrFundTheOther(t *testing.T) {
	s := testServer(t)
	s.liveArmed = true
	s.liveBankArm = map[string]float64{"kalshi": 500, "polyus": 500}
	s.prepareLiveLossLedgerForArm()
	s.liveLossKalshiRead = func(context.Context) ([]kalshi.MarketPosition, error) {
		return nil, errors.New("Kalshi unavailable")
	}
	s.liveLossPolyUSRead = func(context.Context) ([]polymarketus.PUSActivity, error) {
		return []polymarketus.PUSActivity{{
			Type: "POSITION_RESOLUTION", Slug: "pus-settled", PnL: -10,
			Time: time.Now().UTC().Format(time.RFC3339Nano),
		}}, nil
	}
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveMaxOrderUSD = -1
		c.Risk.LiveMaxOrderPct = 0.05
		c.Risk.LiveMaxDailyLossUSD = -1
		c.Risk.LiveDailyLossPct = 0.10
		c.Risk.LiveProspectiveAllocation = true
		c.Risk.LiveSystemPolyUS = true
	})
	s.pollLiveLossLedger(context.Background())

	if s.liveLossTruthV["kalshi"] {
		t.Fatal("failed Kalshi read was marked healthy")
	}
	if !s.liveLossTruthV["polyus"] || s.liveLossDeltaV["polyus"] != liveLossUSDToUnits(-10) {
		t.Fatalf("PolyUS was blinded by Kalshi outage: truth=%v delta=%d", s.liveLossTruthV["polyus"], s.liveLossDeltaV["polyus"])
	}
	if why := s.liveRiskCheck(1, "kalshi"); !strings.Contains(why, "Kalshi unavailable") {
		t.Fatalf("Kalshi must fail closed with its own reason: %q", why)
	}
	if why := s.liveRiskCheck(1, "polyus"); why != "" {
		t.Fatalf("healthy PolyUS should remain independently available: %q", why)
	}
	if got := s.liveRiskBankroll("kalshi"); math.Abs(got-500) > 1e-9 {
		t.Fatalf("PolyUS loss moved Kalshi money: %v", got)
	}
	if got := s.liveRiskBankroll("polyus"); math.Abs(got-490) > 1e-9 {
		t.Fatalf("PolyUS own loss did not de-risk its own bankroll: %v", got)
	}
}
