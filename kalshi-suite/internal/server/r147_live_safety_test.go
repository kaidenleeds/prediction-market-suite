package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/killswitch"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

func TestR147ArmCleanSlateVerificationFailsClosedOnUnreadableOrSurvivingOrders(t *testing.T) {
	if why := kalshiArmCleanSlateReason(nil, errors.New("read outage")); !strings.Contains(why, "unreadable") {
		t.Fatalf("Kalshi unreadable verification passed: %q", why)
	}
	if why := kalshiArmCleanSlateReason([]kalshi.Order{{OrderID: "k-rest", Status: "resting"}}, nil); !strings.Contains(why, "k-rest") {
		t.Fatalf("Kalshi resting survivor passed: %q", why)
	}
	if why := kalshiArmCleanSlateReason([]kalshi.Order{{OrderID: "k-blank-status"}}, nil); !strings.Contains(why, "k-blank-status") {
		t.Fatalf("GetOrders row with omitted status escaped: %q", why)
	}
	if why := polyUSArmCleanSlateReason(nil, errors.New("read outage")); !strings.Contains(why, "unreadable") {
		t.Fatalf("PolyUS unreadable verification passed: %q", why)
	}
	if why := polyUSArmCleanSlateReason([]polymarketus.PUSOrder{{ID: "pus-rest"}}, nil); !strings.Contains(why, "1 open") {
		t.Fatalf("PolyUS open survivor passed: %q", why)
	}
	if why := polyUSArmCleanSlateReason(nil, nil); why != "" {
		t.Fatalf("empty PolyUS account rejected: %q", why)
	}
}

func TestR147KalshiGetOrdersRowsAreAllRestingTruth(t *testing.T) {
	ids, missing := kalshiRestingOrderIDs([]kalshi.Order{
		{OrderID: "blank-status"},
		{OrderID: "unexpected-status", Status: "executed"},
		{},
	})
	if len(ids) != 2 || ids[0] != "blank-status" || ids[1] != "unexpected-status" || missing != 1 {
		t.Fatalf("GetOrders rows were response-status filtered or missing IDs ignored: ids=%v missing=%d", ids, missing)
	}
}

func TestR147LiveContractLimitCoversFractionalAndIntegerRoutes(t *testing.T) {
	s := testServer(t)
	s.maxContracts = 1
	if got := s.liveContractLimitReason(1); got != "" {
		t.Fatalf("one contract should pass: %q", got)
	}
	if got := s.liveContractLimitReason(1.01); !strings.Contains(got, "max_order_contracts 1") {
		t.Fatalf("fractional PolyUS overflow escaped: %q", got)
	}
	if got := s.liveContractLimitReason(2); !strings.Contains(got, "max_order_contracts 1") {
		t.Fatalf("integer Kalshi/combo overflow escaped: %q", got)
	}
}

func TestR147FinalWriteBoundarySeesStateChangesWhileQueued(t *testing.T) {
	s := testServer(t)
	s.liveArmed, s.liveAuto = true, true
	if got := s.liveWriteBoundaryReason(true); got != "" {
		t.Fatalf("healthy armed AUTO boundary refused: %q", got)
	}

	// Hold the writer fence as the kill sweep does. A queued venue write cannot cross until the
	// fence opens, at which point it must observe the already-tripped switch and refuse.
	s.liveWriteFence.Lock()
	done := make(chan string, 1)
	go func() {
		s.liveWriteFence.RLock()
		done <- s.liveWriteBoundaryReason(true)
		s.liveWriteFence.RUnlock()
	}()
	s.ks.Trip("test halt")
	s.liveWriteFence.Unlock()
	select {
	case got := <-done:
		if !strings.Contains(got, "kill switch") {
			t.Fatalf("queued write did not see trip: %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("queued write did not drain")
	}

	s.ks.Reset()
	s.liveArmed = false
	if got := s.liveWriteBoundaryReason(false); !strings.Contains(got, "disarmed") {
		t.Fatalf("manual write ignored disarm: %q", got)
	}
	s.liveArmed, s.liveAuto = true, false
	if got := s.liveWriteBoundaryReason(true); !strings.Contains(got, "AUTO disabled") {
		t.Fatalf("AUTO write ignored AUTO-off: %q", got)
	}
}

func TestR147KillSwitchTripSurvivesRestartUntilDurableReset(t *testing.T) {
	s := testServer(t)
	s.persistManualKillSwitchTrip("operator halt")

	restarted := &Server{store: s.store, ks: killswitch.New(nil), log: s.log}
	restarted.restorePersistedKillSwitch(context.Background())
	state := restarted.ks.State()
	if !state.Tripped || state.Reason != "operator halt" {
		t.Fatalf("restart lost durable trip: %+v", state)
	}
	if err := restarted.resetPersistedKillSwitch(context.Background()); err != nil {
		t.Fatalf("durable reset: %v", err)
	}
	third := &Server{store: s.store, ks: killswitch.New(nil), log: s.log}
	third.restorePersistedKillSwitch(context.Background())
	if third.ks.Tripped() {
		t.Fatal("durably reset trip returned on next restart")
	}
}

func TestR147AuthenticatedBalanceReceiptsDriveDynamicRails(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveExposureCapPct = 0.50
	cfg.Risk.LiveExposureCapUSD = -1
	cfg.Risk.LiveKalshiCapUSD = -1
	cfg.Risk.LivePolyusCapUSD = -1
	s.cfgP.Store(&cfg)
	s.liveArmed = true
	s.liveBankArm = map[string]float64{"kalshi": 100, "polyus": 0}
	s.liveBankNow = map[string]float64{}
	s.liveBankNowAt = map[string]time.Time{}
	s.liveBankNowOK = map[string]bool{}

	cash, cashOK := 100.0, true
	s.liveBankCashRead = func(context.Context, string) (float64, bool) { return cash, cashOK }
	if why := s.liveVenueBudgetCheck(context.Background(), "kalshi", 40); why != "" {
		t.Fatalf("$40 must fit fresh $100 NAV at 50%%: %q", why)
	}
	if got := s.liveRiskBankroll("kalshi"); got != 100 {
		t.Fatalf("fresh NAV = %.2f, want 100", got)
	}

	// A withdrawal is observed before the next admission decision and immediately tightens 50%
	// exposure to $5; the old $100 ARM receipt cannot leak back in.
	cash = 10
	if why := s.liveVenueBudgetCheck(context.Background(), "kalshi", 6); !strings.Contains(why, "$5.00") {
		t.Fatalf("withdrawal did not shrink the rail: %q", why)
	}
	if got := s.liveRiskBankroll("kalshi"); got != 10 {
		t.Fatalf("post-withdrawal NAV = %.2f, want 10", got)
	}

	// A deposit enlarges the rail only after this new authenticated receipt.
	cash = 200
	if why := s.liveVenueBudgetCheck(context.Background(), "kalshi", 75); why != "" {
		t.Fatalf("freshly acknowledged deposit did not update the 50%% rail: %q", why)
	}
	if got := s.liveRiskBankroll("kalshi"); got != 200 {
		t.Fatalf("post-deposit NAV = %.2f, want 200", got)
	}

	cashOK = false
	if why := s.liveVenueBudgetCheck(context.Background(), "kalshi", 1); !strings.Contains(why, "fail-closed") {
		t.Fatalf("unreadable authenticated balance escaped: %q", why)
	}
	if got := s.liveRiskBankroll("kalshi"); got != 0 {
		t.Fatalf("unreadable balance retained buying power %.2f", got)
	}

	cashOK, cash = true, 50
	if why := s.liveVenueBudgetCheck(context.Background(), "kalshi", 1); why != "" {
		t.Fatalf("fresh recovery receipt refused: %q", why)
	}
	s.liveMu.Lock()
	s.liveBankNowAt["kalshi"] = time.Now().Add(-liveBankReceiptFreshFor - time.Second)
	s.liveMu.Unlock()
	if got := s.liveRiskBankroll("kalshi"); got != 0 {
		t.Fatalf("aged receipt retained buying power %.2f", got)
	}
}

func TestR148KalshiDepositImmediatelyResizesProspectiveAllocationFromFreshNAV(t *testing.T) {
	s := testServer(t)
	r147EnableProspectiveAllocation(s)
	cfg := *s.cfg()
	cfg.Risk.LiveMaxOrderPct = .05
	cfg.Risk.LiveMaxOrderUSD = -1
	cfg.Risk.LiveExposureCapPct = .50
	cfg.Risk.LiveExposureCapUSD = -1
	cfg.Risk.LiveClusterCapPct = .10
	cfg.Risk.LiveKellyMaxFrac = .50
	s.cfgP.Store(&cfg)
	s.liveBankArm = map[string]float64{"kalshi": 100}
	s.maxContracts = 1000

	cash, cashOK := 100.0, true
	s.liveBankCashRead = func(context.Context, string) (float64, bool) { return cash, cashOK }
	bank, _ := s.liveSizingBankroll(context.Background(), "kalshi")
	before, beforeUSD, _, _, why := s.liveProspectiveAllocationSize("kalshi", bank, .40, .20, .10, .01, 1000, 1, true)
	if why != "" || bank != 100 || before != 12 || beforeUSD != 5 {
		t.Fatalf("pre-deposit allocation bank/count/usd/why = %.2f/%.0f/%.2f/%q", bank, before, beforeUSD, why)
	}

	cash = 200
	bank, _ = s.liveSizingBankroll(context.Background(), "kalshi")
	after, afterUSD, _, _, why := s.liveProspectiveAllocationSize("kalshi", bank, .40, .20, .10, .01, 1000, 1, true)
	if why != "" || bank != 200 || after != 24 || afterUSD != 10 {
		t.Fatalf("post-deposit allocation bank/count/usd/why = %.2f/%.0f/%.2f/%q", bank, after, afterUSD, why)
	}
	if after <= before || afterUSD <= beforeUSD {
		t.Fatalf("fresh deposit did not increase bounded allocation: before %.0f/$%.2f after %.0f/$%.2f", before, beforeUSD, after, afterUSD)
	}

	cashOK = false
	if bank, _ := s.liveSizingBankroll(context.Background(), "kalshi"); bank != 0 {
		t.Fatalf("failed authenticated balance read retained allocation NAV %.2f", bank)
	}
}

func TestR147PolyUSExposureFailsClosedAfterAccountMutation(t *testing.T) {
	s := testServer(t)
	// The zero-value client is only a configured-venue marker here; exposure reads use the
	// already-authenticated cache and never call its transport in this unit test.
	s.polyUSAuth = &polymarketus.Client{}
	s.pusLiveMu.Lock()
	s.pusPosC = []polymarketus.PUSPosition{{Slug: "match", Net: 10, Cost: 4}}
	s.pusOrdsC = []polymarketus.PUSOrder{{ID: "rest", Action: "BUY", Leaves: 2, Price: 0.50}}
	s.pusLiveAt = time.Now()
	s.pusErrC = ""
	s.pusLiveMu.Unlock()

	exposure, err := s.liveVenueExposureUSD(context.Background(), "polyus")
	if err != nil {
		t.Fatalf("fresh exposure rejected: %v", err)
	}
	if exposure != 5 {
		t.Fatalf("fresh exposure = %.2f, want 5.00", exposure)
	}

	// A successful order/cancel changes account truth. Until REST reconciliation completes, the
	// previous positions/orders receipt cannot authorize a second order under the 50%% cap.
	s.pusMutated(nil, false)
	if _, err := s.liveVenueExposureUSD(context.Background(), "polyus"); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("post-mutation stale exposure passed: %v", err)
	}
}

func TestR147PolyUSExposureReceiptUsesPushAndRESTCadences(t *testing.T) {
	now := time.Now()
	// REST positions intentionally refresh every ~60s while authenticated private order push is
	// healthy, so that combined receipt may remain usable for 75s.
	if why := livePolyUSExposureReceiptReason(now.Add(-60*time.Second), "", true, now); why != "" {
		t.Fatalf("healthy push receipt rejected before 75s: %q", why)
	}
	if why := livePolyUSExposureReceiptReason(now.Add(-76*time.Second), "", true, now); !strings.Contains(why, "stale") {
		t.Fatalf("aged push receipt passed: %q", why)
	}
	// Without a healthy private order stream, REST-only truth keeps the strict 15s allowance.
	if why := livePolyUSExposureReceiptReason(now.Add(-14*time.Second), "", false, now); why != "" {
		t.Fatalf("fresh REST receipt rejected: %q", why)
	}
	if why := livePolyUSExposureReceiptReason(now.Add(-16*time.Second), "", false, now); !strings.Contains(why, "stale") {
		t.Fatalf("aged REST-only receipt passed: %q", why)
	}
}
