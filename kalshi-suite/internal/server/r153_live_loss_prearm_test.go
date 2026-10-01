package server

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

func TestR153PreArmRebuildRefusesAlreadyBreachedDay(t *testing.T) {
	s := testServer(t)
	day := liveLossDay(time.Now())
	s.liveLossDay, s.liveLossHasBase = day, true
	s.liveLossEpochAt = liveLossDayStart(day)
	s.liveLossDeltaV = map[string]int64{"kalshi": liveLossUSDToUnits(-2.27), "polyus": 0}
	s.liveLossSeenV = map[string]map[string]bool{"kalshi": {}, "polyus": {}}
	s.liveLossLastV = map[string]map[string]int64{"kalshi": {"KX-OPEN": liveLossUSDToUnits(-2.27)}, "polyus": {}}
	s.liveLossFinalV = map[string]map[string]int64{"kalshi": {}, "polyus": {}}
	s.liveLossBootV = map[string]bool{"kalshi": true}
	s.liveLossTruthV = map[string]bool{}
	s.liveLossTruthAtV = map[string]time.Time{}
	s.liveLossErrV = map[string]string{}
	s.liveLossKalshiRead = func(context.Context) ([]kalshi.MarketPosition, error) {
		return []kalshi.MarketPosition{testKalshiCumulativePosition(t, "KX-OPEN", 0, 2.27)}, nil
	}
	s.liveLossKalshiSettlementRead = func(context.Context) ([]kalshi.Settlement, error) {
		return []kalshi.Settlement{
			testKalshiLossSettlement(t, "KX-SETTLED", 51.56, 0, 0, time.Now()),
		}, nil
	}
	s.liveLossPolyUSRead = func(context.Context) ([]polymarketus.PUSActivity, error) { return nil, nil }
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveDailyLossPct = 0.10
		c.Risk.LiveMaxDailyLossUSD = -1
	})

	why := s.preArmLiveLossReason(context.Background(), 490.0875, []string{"kalshi"})
	if !strings.Contains(why, "-53.83") || !strings.Contains(why, "49.01") {
		t.Fatalf("pre-ARM refusal=%q, want rebuilt -53.83 against 49.01 rail", why)
	}
	if got := s.liveLossDeltaV["kalshi"]; got != liveLossUSDToUnits(-53.83) {
		t.Fatalf("pre-ARM rebuilt loss=%f USD, want -53.83", liveLossUnitsToUSD(got))
	}
	if s.liveArmed || s.liveAuto {
		t.Fatal("loss refresh granted order authority")
	}
}

func TestR153FreshLossLedgerIncludesEarlierTodaySettlements(t *testing.T) {
	s := testServer(t)
	s.liveLossDay = ""
	s.liveLossHasBase = false
	s.prepareLiveLossLedgerForArm()
	day := liveLossDay(time.Now())
	if !s.liveLossEpochAt.Equal(liveLossDayStart(day)) {
		t.Fatalf("fresh daily-loss epoch=%s want UTC day start %s", s.liveLossEpochAt, liveLossDayStart(day))
	}
	now := time.Now().UTC()
	settledAt := liveLossDayStart(day).Add(now.Sub(liveLossDayStart(day)) / 2)
	finals := map[string]kalshiLossSettlementFinal{
		"KX-EARLIER-TODAY": {Cents: liveLossUSDToUnits(-51), Day: day, At: settledAt},
	}
	if err := s.applyKalshiSettlementWindow(day, finals, true); err != nil {
		t.Fatal(err)
	}
	if got := s.liveLossDeltaV["kalshi"]; got != liveLossUSDToUnits(-51) {
		t.Fatalf("fresh-state earlier-today loss=%f USD, want -51", liveLossUnitsToUSD(got))
	}
}

func TestR153FreshPreArmCountsEarlierTodayOpenFees(t *testing.T) {
	s := testServer(t)
	var open kalshi.MarketPosition
	raw, err := json.Marshal(map[string]any{
		"ticker": "KX-OPEN-TODAY", "position_fp": "1.00", "total_traded_dollars": "0.40",
		"realized_pnl_dollars": "0.00", "fees_paid_dollars": "2.27",
		"last_updated_ts": time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil || json.Unmarshal(raw, &open) != nil {
		t.Fatalf("encode/decode open position: %v", err)
	}
	var fill kalshi.Fill
	fillRaw := []byte(`{"fill_id":"fill-open-today","ticker":"KX-OPEN-TODAY","order_id":"order-open-today","outcome_side":"yes","count_fp":"1.00","yes_price_dollars":"0.40","no_price_dollars":"0.60","fee_cost":"2.27","created_time":"` + time.Now().UTC().Format(time.RFC3339Nano) + `"}`)
	if err := json.Unmarshal(fillRaw, &fill); err != nil {
		t.Fatal(err)
	}
	s.liveLossKalshiRead = func(context.Context) ([]kalshi.MarketPosition, error) {
		return []kalshi.MarketPosition{open}, nil
	}
	s.liveLossKalshiSettlementRead = func(context.Context) ([]kalshi.Settlement, error) { return nil, nil }
	s.liveLossKalshiFillRead = func(context.Context) ([]kalshi.Fill, error) { return []kalshi.Fill{fill}, nil }
	s.liveLossPolyUSRead = func(context.Context) ([]polymarketus.PUSActivity, error) { return nil, nil }
	s.prepareLiveLossLedgerForArm()
	if got := s.refreshLiveLossLedger(context.Background()); got != -2.27 {
		t.Fatalf("fresh-state current-day open fee result=%+.2f, want -2.27", got)
	}
	if s.liveLossDeltaV["kalshi"] != liveLossUSDToUnits(-2.27) {
		t.Fatalf("fresh-state current-day open fee delta=%f USD, want -2.27", liveLossUnitsToUSD(s.liveLossDeltaV["kalshi"]))
	}
}

func TestR153FreshPreArmRejectsLifetimeRealizedAmbiguity(t *testing.T) {
	s := testServer(t)
	var row kalshi.MarketPosition
	// Lifetime realized P&L can be exactly zero after prior +$5 and today's -$5; zero is not proof
	// that the ticker is new today. Lifetime notional must reconcile to today's timestamped fills.
	raw := []byte(`{"ticker":"KX-REUSED","position_fp":"1.00","total_traded_dollars":"200.00","realized_pnl_dollars":"0.00","fees_paid_dollars":"0.01","last_updated_ts":"` + time.Now().UTC().Format(time.RFC3339Nano) + `"}`)
	if err := json.Unmarshal(raw, &row); err != nil {
		t.Fatal(err)
	}
	var fill kalshi.Fill
	fillRaw := []byte(`{"fill_id":"fill-reused-today","ticker":"KX-REUSED","order_id":"order-reused","outcome_side":"yes","count_fp":"1.00","yes_price_dollars":"0.40","no_price_dollars":"0.60","fee_cost":"0.01","created_time":"` + time.Now().UTC().Format(time.RFC3339Nano) + `"}`)
	if err := json.Unmarshal(fillRaw, &fill); err != nil {
		t.Fatal(err)
	}
	s.liveLossKalshiRead = func(context.Context) ([]kalshi.MarketPosition, error) { return []kalshi.MarketPosition{row}, nil }
	s.liveLossKalshiSettlementRead = func(context.Context) ([]kalshi.Settlement, error) { return nil, nil }
	s.liveLossKalshiFillRead = func(context.Context) ([]kalshi.Fill, error) { return []kalshi.Fill{fill}, nil }
	s.liveLossPolyUSRead = func(context.Context) ([]polymarketus.PUSActivity, error) { return nil, nil }
	s.prepareLiveLossLedgerForArm()
	_ = s.refreshLiveLossLedger(context.Background())
	if why := s.liveLossTruthReason("kalshi"); !strings.Contains(why, "pre-day or incomplete lifetime notional") {
		t.Fatalf("ambiguous lifetime/current-day row did not fail closed: %q", why)
	}
}

func TestR153DailyLossPreservesManySubCentReceipts(t *testing.T) {
	day := liveLossDay(time.Now())
	rows := make([]polymarketus.PUSActivity, 50)
	for i := range rows {
		rows[i] = polymarketus.PUSActivity{Type: "TRADE", Slug: "subcent-" + strconv.Itoa(i),
			Price: .001, Qty: .01, PnL: 0, CommissionKnown: true, CommissionUSD: .004,
			Time: time.Now().UTC().Add(time.Duration(i) * time.Microsecond).Format(time.RFC3339Nano)}
	}
	receipts, err := polyUSLossReceipts(day, rows, map[string]bool{})
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, receipt := range receipts {
		total += receipt.DeltaC
	}
	if got := liveLossUnitsToUSD(total); math.Abs(got-(-.20)) > 0.0000005 {
		t.Fatalf("50 x -$0.004 receipts=%+.6f, want -0.200000", got)
	}
	if got := liveLossUnitsToUSD(liveLossUSDToUnits(-.004)); math.Abs(got-(-.004)) > 0.0000005 {
		t.Fatalf("sub-cent unit/display round trip=%+.6f", got)
	}
}
