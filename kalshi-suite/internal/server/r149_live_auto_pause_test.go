package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

func r149EnableLiveDestinations(s *Server, kalshiOn, polyUSOn bool) {
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveProspectiveAllocation = kalshiOn || polyUSOn
		c.Risk.LiveSystemKalshi = kalshiOn
		c.Risk.LiveSystemPolyUS = polyUSOn
		c.Risk.LiveNewMLKalshi = false
		c.Risk.LiveNewMLPolyUS = false
		routes := make([]string, 0, 2)
		if kalshiOn {
			routes = append(routes, "kalshi|spotlag|YES|taker")
		}
		if polyUSOn {
			routes = append(routes, "polyus|polyus-flow|YES|taker")
		}
		c.Risk.LiveSystemAllowlist = strings.Join(routes, ",")
	})
}

func TestR149RunningAutoBecomesPausedWithoutChangingOperatorControls(t *testing.T) {
	in := greenLiveAutoGoInput()
	in.Armed, in.Auto = true, true
	in.DBLastMS = 900
	got := evaluateLiveAutoGo(in)
	if got.State != "PAUSED" || got.Headline != "LIVE AUTO PAUSED" || got.Armed != true || got.Auto != true {
		t.Fatalf("transient failure changed operator state instead of PAUSED: %+v", got)
	}
	if got.SafeToArm || got.ReadyToEnableAuto {
		t.Fatalf("paused runtime was advertised as ready: %+v", got)
	}
}

func TestR149PauseDecisionSuppressesDispatchThenResumesWithoutRetoggle(t *testing.T) {
	armed, auto, killClear := true, true, true
	dispatched := 0
	run := func(d liveAutoRuntimeDecision) {
		if d.Dispatch {
			dispatched++
		}
	}
	paused := decideLiveAutoRuntime(armed, auto, killClear, "book feed temporarily stale")
	run(paused)
	if !paused.Paused || paused.Dispatch || dispatched != 0 {
		t.Fatalf("paused cycle dispatched: decision=%+v dispatched=%d", paused, dispatched)
	}
	resumed := decideLiveAutoRuntime(armed, auto, killClear, "")
	run(resumed)
	if resumed.Paused || !resumed.Dispatch || dispatched != 1 || !armed || !auto || !killClear {
		t.Fatalf("healthy cycle did not resume under the same controls: decision=%+v dispatched=%d", resumed, dispatched)
	}
}

func TestR151eAutomaticSafetyPauseNeverTripsManualKillOrChangesIntent(t *testing.T) {
	s := testServer(t)
	s.liveArmed, s.liveAuto = true, true
	s.setLiveAutoSafetyPause("pending-risk", "exact fill is still reconciling")
	if s.ksBlocked() || !s.liveArmed || !s.liveAuto {
		t.Fatalf("automatic pause changed manual controls: kill=%v arm=%v auto=%v",
			s.ksBlocked(), s.liveArmed, s.liveAuto)
	}
	if reason := s.liveAutoSafetyPauseReason(); !strings.Contains(reason, "pending-risk") {
		t.Fatalf("automatic safety reason missing: %q", reason)
	}
	s.clearLiveAutoSafetyPause("pending-risk", "exact fill and account position agree")
	if reason := s.liveAutoSafetyPauseReason(); reason != "" || s.ksBlocked() || !s.liveArmed || !s.liveAuto {
		t.Fatalf("healthy proof did not auto-resume: reason=%q kill=%v arm=%v auto=%v",
			reason, s.ksBlocked(), s.liveArmed, s.liveAuto)
	}
}

func TestR151ePendingRiskLedgerReadFailurePausesWithoutManualKill(t *testing.T) {
	s := testServer(t)
	s.liveArmed, s.liveAuto = true, true
	if err := s.store.Close(); err != nil {
		t.Fatal(err)
	}
	s.reconcileR148SinglePendingRisks(context.Background())
	if s.ksBlocked() || !s.liveArmed || !s.liveAuto {
		t.Fatalf("ledger read failure changed manual controls: kill=%v arm=%v auto=%v",
			s.ksBlocked(), s.liveArmed, s.liveAuto)
	}
	if reason := s.liveAutoSafetyPauseReason(); !strings.Contains(reason, "pending-risk") ||
		!strings.Contains(reason, "temporarily unreadable") {
		t.Fatalf("ledger read failure did not create an automatic retry pause: %q", reason)
	}
}

func TestR151eOnlyManualEndpointLatchesKillSwitch(t *testing.T) {
	s := testServer(t)
	s.liveArmed, s.liveAuto = true, true
	s.setLiveAutoSafetyPause("account", "temporary account read failure")
	if s.ksBlocked() {
		t.Fatal("automatic safety pause latched the manual kill switch")
	}
	req := httptest.NewRequest(http.MethodPost, "/api/killswitch",
		strings.NewReader(`{"action":"trip","reason":"operator test"}`))
	rec := httptest.NewRecorder()
	s.handleKillSwitch(rec, req)
	if rec.Code != http.StatusOK || !s.ksBlocked() {
		t.Fatalf("manual endpoint did not latch kill: code=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestR149ArmDestinationsComeFromAuthorityNotConfiguredCredentials(t *testing.T) {
	venues := []string{"kalshi"}
	if !liveVenueRequired(venues, "kalshi") || liveVenueRequired(venues, "polyus") {
		t.Fatalf("Kalshi-only authority was widened to PolyUS: %v", venues)
	}
	venues = append(venues, "polyus")
	if !liveVenueRequired(venues, "polyus") {
		t.Fatalf("explicit PolyUS authority was not required: %v", venues)
	}
}

func TestR149PauseLogsTransitionsOnlyAndNeverTripsOrDisarms(t *testing.T) {
	s := testServer(t)
	s.liveArmed, s.liveAuto = true, true
	s.setLiveAutoPaused(true, "feed stale")
	s.setLiveAutoPaused(true, "different repeated detail")
	s.setLiveAutoPaused(false, "")
	s.setLiveAutoPaused(false, "")
	s.liveMu.Lock()
	armed, auto := s.liveArmed, s.liveAuto
	logs := append([]map[string]any(nil), s.liveLog...)
	s.liveMu.Unlock()
	if !armed || !auto || s.ksBlocked() {
		t.Fatalf("pause transition mutated money controls: arm=%t auto=%t kill=%t", armed, auto, s.ksBlocked())
	}
	events := []string{}
	for _, row := range logs {
		if event := strings.TrimSpace(fmt.Sprint(row["event"])); strings.HasPrefix(event, "LIVE-AUTO-") {
			events = append(events, event)
		}
	}
	if strings.Join(events, ",") != "LIVE-AUTO-PAUSED,LIVE-AUTO-RESUMED" {
		t.Fatalf("expected transition-only log, got %v", events)
	}
}

func TestR149KalshiOnlyBudgetIgnoresDisabledPolyUSReceipt(t *testing.T) {
	s := testServer(t)
	s.polyUSAuth = &polymarketus.Client{} // configured account with deliberately absent exposure receipt
	s.liveArmed = true
	s.liveBankArm = map[string]float64{"kalshi": 100}
	s.liveBankNow = map[string]float64{}
	s.liveBankNowAt = map[string]time.Time{}
	s.liveBankNowOK = map[string]bool{}
	s.liveBankCashRead = func(_ context.Context, venue string) (float64, bool) {
		if venue == "kalshi" {
			return 100, true
		}
		return 0, false
	}
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveProspectiveAllocation = true
		c.Risk.LiveSystemKalshi = true
		c.Risk.LiveSystemPolyUS = false
		c.Risk.LiveNewMLPolyUS = false
		c.Risk.LiveExposureCapPct = .50
		c.Risk.LiveExposureCapUSD = -1
		c.Risk.LiveKalshiCapUSD = -1
		c.Risk.LivePolyusCapUSD = -1
	})
	if why := s.liveVenueBudgetCheck(context.Background(), "kalshi", 5); why != "" {
		t.Fatalf("disabled PolyUS account blocked Kalshi-only money path: %q", why)
	}
	k, p := s.liveVenueRemaining(context.Background())
	if k <= 0 || p != 0 {
		t.Fatalf("Kalshi-only headroom k=%.2f p=%.2f, want k>0 p=0", k, p)
	}

	s.mutateCfg(func(c *config.Config) { c.Risk.LiveSystemPolyUS = true })
	if why := s.liveVenueBudgetCheck(context.Background(), "kalshi", 5); !strings.Contains(why, "polyus") {
		t.Fatalf("enabled PolyUS destination did not restore fail-closed shared exposure read: %q", why)
	}
}

func TestR149DisabledPolyUSDestinationCannotPlaceManualOrAutoOrder(t *testing.T) {
	s := testServer(t)
	s.maxContracts = 1000
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveProspectiveAllocation = true
		c.Risk.LiveSystemKalshi = true
		c.Risk.LiveSystemPolyUS = false
		c.Risk.LiveNewMLPolyUS = false
	})
	for _, auto := range []bool{false, true} {
		body, _ := json.Marshal(map[string]any{"slug": "fixture", "outcome": "YES", "price_dollars": .40, "count": 1, "auto": auto})
		req := httptest.NewRequest(http.MethodPost, "/api/live/polyus/place", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		s.handlePolyUSLiveOrder(rec, req)
		if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "destination is disabled") {
			t.Fatalf("auto=%t disabled PolyUS write code=%d body=%s", auto, rec.Code, rec.Body.String())
		}
	}
}

func TestR149ArmedOperatorCanEnableAutoThenRuntimePausesWithoutRetoggle(t *testing.T) {
	s := testServer(t)
	s.liveArmed = true
	req := httptest.NewRequest(http.MethodPost, "/api/live/auto", strings.NewReader(`{"on":1}`))
	rec := httptest.NewRecorder()
	s.handleLiveAuto(rec, req)
	s.liveMu.Lock()
	armed, auto := s.liveArmed, s.liveAuto
	s.liveMu.Unlock()
	if rec.Code != http.StatusOK || !armed || !auto {
		t.Fatalf("armed AUTO toggle did not remain available for PAUSED operation: code=%d arm=%t auto=%t body=%s", rec.Code, armed, auto, rec.Body.String())
	}

	s.setLiveAutoPaused(true, "fixture transient failure")
	s.liveMu.Lock()
	armed, auto, paused := s.liveArmed, s.liveAuto, s.liveAutoPaused
	s.liveMu.Unlock()
	if !armed || !auto || !paused || s.ksBlocked() {
		t.Fatalf("runtime pause changed controls: arm=%t auto=%t paused=%t kill=%t", armed, auto, paused, s.ksBlocked())
	}
}

func TestR149LiveControlsPinPausedUXAndNoOptimisticArm(t *testing.T) {
	for _, want := range []string{"g.state==='PAUSED'", "LIVE AUTO PAUSED", "if(d.auto){toggleLiveAuto(0);return;}"} {
		if !strings.Contains(dashboardHTML, want) {
			t.Fatalf("dashboard missing %q", want)
		}
	}
	if strings.Contains(dashboardHTML, "window._liveData.armed=true") {
		t.Fatal("ARM still paints an optimistic local success before the backend receipt")
	}
}
