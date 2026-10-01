package server

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
)

func TestR150IdleArmedSessionRefreshesPercentageRailsBeforeProposalBudget(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveExposureCapPct = 0.50
		c.Risk.LiveExposureCapUSD = -1
		c.Risk.LiveMaxOrderPct = 0.05
		c.Risk.LiveMaxOrderUSD = -1
		c.Risk.LiveSystemPolyUS = false
		c.Risk.LiveNewMLPolyUS = false
	})
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveBankArm = map[string]float64{"kalshi": 514.24}
	// Reproduce the overnight state: the last authenticated receipt aged out while ARM+AUTO
	// stayed on and no candidate reached the later order-admission refresh.
	s.liveBankNow = map[string]float64{"kalshi": 514.24}
	s.liveBankNowAt = map[string]time.Time{"kalshi": time.Now().Add(-liveBankReceiptFreshFor - time.Second)}
	s.liveBankNowOK = map[string]bool{"kalshi": true}
	s.liveMu.Unlock()

	reads := 0
	s.liveBankCashRead = func(_ context.Context, venue string) (float64, bool) {
		reads++
		return 514.24, venue == "kalshi"
	}
	remKal, remPUS := s.liveVenueRemaining(context.Background())
	if reads != 1 {
		t.Fatalf("authenticated cash reads=%d want 1", reads)
	}
	if math.Abs(remKal-257.12) > 1e-9 || remPUS != 0 {
		t.Fatalf("remaining Kalshi/PUS = %.6f/%.6f want 257.12/0", remKal, remPUS)
	}
	if got := s.liveCap(); math.Abs(got-257.12) > 1e-9 {
		t.Fatalf("combined cap=%.6f want 50%% of $514.24 = $257.12", got)
	}
	if got := s.liveVenueCap("kalshi"); math.Abs(got-257.12) > 1e-9 {
		t.Fatalf("Kalshi cap=%.6f want $257.12", got)
	}
	if got := s.liveRailsView()["order_kalshi"].(float64); math.Abs(got-25.712) > 1e-9 {
		t.Fatalf("Kalshi order rail=%.6f want 5%% of $514.24 = $25.712", got)
	}
}

func TestR150ProposalBudgetStillFailsClosedWhenAuthenticatedCashIsUnreadable(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveExposureCapPct = 0.50
		c.Risk.LiveExposureCapUSD = -1
		c.Risk.LiveMaxOrderPct = 0.05
		c.Risk.LiveMaxOrderUSD = -1
		c.Risk.LiveSystemPolyUS = false
		c.Risk.LiveNewMLPolyUS = false
	})
	s.liveMu.Lock()
	s.liveArmed = true
	s.liveBankArm = map[string]float64{"kalshi": 514.24}
	s.liveMu.Unlock()
	s.liveBankCashRead = func(context.Context, string) (float64, bool) { return 0, false }

	remKal, remPUS := s.liveVenueRemaining(context.Background())
	if remKal != 0 || remPUS != 0 || s.liveCap() != 0 {
		t.Fatalf("unreadable venue truth must fail closed: remaining=%.2f/%.2f cap=%.2f", remKal, remPUS, s.liveCap())
	}
}

func TestR150ProposalBudgetReusesFreshAuthenticatedReceipt(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveExposureCapPct = 0.50
		c.Risk.LiveExposureCapUSD = -1
		c.Risk.LiveMaxOrderPct = 0.05
		c.Risk.LiveMaxOrderUSD = -1
		c.Risk.LiveSystemPolyUS = false
		c.Risk.LiveNewMLPolyUS = false
	})
	s.liveMu.Lock()
	s.liveArmed = true
	s.liveBankNow = map[string]float64{"kalshi": 514.24}
	s.liveBankNowAt = map[string]time.Time{"kalshi": time.Now()}
	s.liveBankNowOK = map[string]bool{"kalshi": true}
	s.liveMu.Unlock()

	reads := 0
	s.liveBankCashRead = func(context.Context, string) (float64, bool) {
		reads++
		return 514.24, true
	}
	remKal, remPUS := s.liveVenueRemaining(context.Background())
	if reads != 0 {
		t.Fatalf("fresh seven-second proposal loop polled balance %d times; want 0", reads)
	}
	if math.Abs(remKal-257.12) > 1e-9 || remPUS != 0 {
		t.Fatalf("remaining Kalshi/PUS = %.6f/%.6f want 257.12/0", remKal, remPUS)
	}
}
