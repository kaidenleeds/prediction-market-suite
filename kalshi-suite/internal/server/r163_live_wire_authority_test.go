package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r163WireRiskReservation(t *testing.T, st *storage.Store, id, venue, ticker, system string) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	clientOrderID := ""
	if venue == "kalshi" {
		clientOrderID = "r163-wire-client"
	}
	inserted, err := st.InsertLivePendingRisk(context.Background(), storage.LivePendingRiskIntent{
		ReservationID: id, Created: now, BaselineObserved: now.Add(-time.Second),
		Product: "single", DispatchSource: "test", SystemID: system, Route: "taker",
		PrincipalUSD: .5, FeeUSD: .03, CostUSD: .53,
		RequestHash: strings.Repeat("c", 64), ProofJSON: `{}`,
		BaselineReceiptJSON: `{"baseline_clock":"venue-account-watermark-v1","baseline_target_position_found":true}`,
	}, []storage.LivePendingRiskLeg{{
		Index: 0, Venue: venue, Ticker: ticker, Side: "YES", Action: "BUY",
		ClientOrderID: clientOrderID, Quantity: 1, LimitPrice: .50,
	}}, []storage.LivePendingRiskCluster{{
		Index: 0, Venue: venue, ClusterKey: "event:r163-wire",
		MappingVersion: r148LiveRiskMappingVersion, ReservedUSD: .53,
	}})
	if err != nil || !inserted {
		t.Fatalf("insert wire risk=%v err=%v", inserted, err)
	}
}

func TestR163NormalSystemAuthorityChangeAfterReservationIsKnownNoSend(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*config.Config)
		want   string
	}{
		{
			name: "exact identity removed",
			mutate: func(cfg *config.Config) {
				cfg.Risk.LiveSystemAllowlist = "kalshi|spotlag|NO|taker"
			},
			want: "live-system-identity-not-allowlisted:kalshi|spotlag|YES|taker",
		},
		{
			name: "destination venue disabled",
			mutate: func(cfg *config.Config) {
				cfg.Risk.LiveSystemKalshi = false
			},
			want: "live-system-disabled-for-destination-venue",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, st := r148RiskTestServer(t)
			cfg := config.Default()
			cfg.Risk.LiveProspectiveAllocation = true
			cfg.Risk.LiveSystemKalshi = true
			cfg.Risk.LiveSystemAllowlist = "kalshi|spotlag|YES|taker"
			s.cfgP.Store(&cfg)

			candidate := liveMirrorCandidate{
				Platform: "kalshi",
				Ticker:   "KXTEST",
				Side:     "YES",
				Source:   "auto-cons-spotlag",
				Family:   "spotlag",
			}
			if why := s.r163LiveWireSystemAuthorityReason(
				true, false, candidate, "taker"); why != "" {
				t.Fatalf("initial normal authority refused: %q", why)
			}

			intent := r148RiskTestReservation(t, st, "risk-r163-wire-authority")

			changed := *s.cfg()
			tc.mutate(&changed)
			s.cfgP.Store(&changed)

			venueAttempted := false
			why := s.r163LiveWireSystemAuthorityReason(
				true, false, candidate, "taker")
			if why == "" {
				venueAttempted = true
				t.Fatal("changed current authority remained usable after reservation")
			}
			if why != tc.want {
				t.Fatalf("current authority reason=%q, want %q", why, tc.want)
			}
			response, err := s.r163RejectKalshiWireSystemAuthority(
				context.Background(), intent.ReservationID, candidate, "taker", why)
			if err != nil {
				t.Fatal(err)
			}
			if attempted, ok := response["venue_attempted"].(bool); !ok || attempted {
				t.Fatalf("venue_attempted=%v (%T), want explicit false",
					response["venue_attempted"], response["venue_attempted"])
			}
			if venueAttempted {
				t.Fatal("venue mutation ran after current normal authority changed")
			}

			active, err := st.ActiveLivePendingRisk(context.Background())
			if err != nil || len(active) != 0 {
				t.Fatalf("known no-send stayed risk-reserved: active=%d err=%v",
					len(active), err)
			}
			row, found, err := st.LivePendingRiskByID(
				context.Background(), intent.ReservationID)
			if err != nil || !found || len(row.Events) < 1 {
				t.Fatalf("terminal no-send receipt missing: found=%v events=%d err=%v",
					found, len(row.Events), err)
			}
			released := row.Events[len(row.Events)-1]
			if released.EventType != storage.LivePendingRiskReleased ||
				released.ReceiptSource != "wire-system-authority" {
				t.Fatalf("terminal no-send receipt=%+v", released)
			}
			var evidence map[string]any
			if err := json.Unmarshal([]byte(released.EvidenceJSON), &evidence); err != nil {
				t.Fatal(err)
			}
			if evidence["reason"] != tc.want ||
				evidence["system_identity"] != "kalshi|spotlag|YES|taker" {
				t.Fatalf("terminal authority evidence=%v", evidence)
			}
		})
	}
}

func TestR163PolyUSNormalSystemAuthorityChangeAfterReservationIsKnownNoSend(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*config.Config)
		want   string
	}{
		{
			name: "exact identity removed",
			mutate: func(cfg *config.Config) {
				cfg.Risk.LiveSystemAllowlist = "polyus|polyus-flow|NO|taker"
			},
			want: "live-system-identity-not-allowlisted:polyus|polyus-flow|YES|taker",
		},
		{
			name: "destination venue disabled",
			mutate: func(cfg *config.Config) {
				cfg.Risk.LiveSystemPolyUS = false
			},
			want: "live-system-disabled-for-destination-venue",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, st := r148RiskTestServer(t)
			cfg := config.Default()
			cfg.Risk.LiveProspectiveAllocation = true
			cfg.Risk.LiveSystemPolyUS = true
			cfg.Risk.LiveSystemAllowlist = "polyus|polyus-flow|YES|taker"
			s.cfgP.Store(&cfg)

			candidate := liveMirrorCandidate{
				Platform: "polyus",
				Ticker:   "r163-polyus",
				Side:     "YES",
				Source:   "auto-cons-polyus-flow",
				Family:   "polyus-flow",
			}
			if why := s.r163LiveWireSystemAuthorityReason(
				true, false, candidate, "taker"); why != "" {
				t.Fatalf("initial normal authority refused: %q", why)
			}

			const riskID = "risk-r163-polyus-wire-authority"
			r163WireRiskReservation(t, st, riskID, "polyus", candidate.Ticker, candidate.Family)
			leg := 0
			if err := s.r148AppendRiskEvent(context.Background(), riskID,
				storage.LivePendingRiskSubmitStarted, "entry", "BUY", "", "",
				"polyus-create-order", "network mutation begins", &leg, 0, .5, .03,
				nil); err != nil {
				t.Fatal(err)
			}

			changed := *s.cfg()
			tc.mutate(&changed)
			s.cfgP.Store(&changed)

			venueAttempted := false
			why := s.r163LiveWireSystemAuthorityReason(
				true, false, candidate, "taker")
			if why == "" {
				venueAttempted = true
				t.Fatal("changed current PolyUS authority remained usable after reservation")
			}
			if why != tc.want {
				t.Fatalf("current authority reason=%q, want %q", why, tc.want)
			}
			response, err := s.r163RejectPolyUSWireSystemAuthority(
				context.Background(), riskID, candidate, "taker", why, 1, .5)
			if err != nil {
				t.Fatal(err)
			}
			if attempted, ok := response["venue_attempted"].(bool); !ok || attempted {
				t.Fatalf("venue_attempted=%v (%T), want explicit false",
					response["venue_attempted"], response["venue_attempted"])
			}
			if venueAttempted {
				t.Fatal("PolyUS venue mutation ran after current normal authority changed")
			}

			active, err := st.ActiveLivePendingRisk(context.Background())
			if err != nil || len(active) != 0 {
				t.Fatalf("known PolyUS no-send stayed risk-reserved: active=%d err=%v",
					len(active), err)
			}
			row, found, err := st.LivePendingRiskByID(context.Background(), riskID)
			if err != nil || !found || len(row.Events) < 2 {
				t.Fatalf("terminal PolyUS no-send receipt missing: found=%v events=%d err=%v",
					found, len(row.Events), err)
			}
			clean := row.Events[len(row.Events)-2]
			released := row.Events[len(row.Events)-1]
			if clean.EventType != storage.LivePendingRiskCleanRejected ||
				clean.ReceiptSource != "polyus-wire-system-authority" ||
				released.EventType != storage.LivePendingRiskReleased {
				t.Fatalf("terminal PolyUS no-send sequence=%+v then %+v", clean, released)
			}
		})
	}
}

func TestR163NormalSystemAuthorityRecheckIsAtFinalKalshiWireBoundary(t *testing.T) {
	raw, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	start := strings.Index(src, "func (s *Server) handleLivePlace")
	if start < 0 {
		t.Fatal("handleLivePlace source missing")
	}
	handler := src[start:]
	reservation := strings.Index(handler, "s.r148ReserveSingleRisk")
	recheck := strings.Index(handler, "s.r163LiveWireSystemAuthorityReason")
	policyRecheck := strings.Index(handler, "r163LiveMoneyPolicyGenerationReason")
	createOrder := strings.Index(handler, "s.kal.CreateOrder(postCtx, req)")
	if reservation < 0 || recheck <= reservation || policyRecheck <= recheck ||
		createOrder <= policyRecheck {
		t.Fatalf("Kalshi wire ordering invalid: reservation=%d authority=%d policy=%d create=%d",
			reservation, recheck, policyRecheck, createOrder)
	}
	afterRecheck := handler[recheck:createOrder]
	if !strings.Contains(afterRecheck, "r163RejectKalshiWireSystemAuthority") ||
		!strings.Contains(afterRecheck, "return") {
		t.Fatal("normal authority refusal does not return a known no-send receipt before CreateOrder")
	}
}

func TestR163NormalSystemAuthorityRecheckIsAtFinalPolyUSWireBoundary(t *testing.T) {
	raw, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	start := strings.Index(src, "func (s *Server) handlePolyUSLiveOrder")
	if start < 0 {
		t.Fatal("handlePolyUSLiveOrder source missing")
	}
	handler := src[start:]
	reservation := strings.Index(handler, "s.r148ReserveSingleRisk")
	recheck := strings.Index(handler, "s.r163LiveWireSystemAuthorityReason")
	policyRecheck := strings.Index(handler, "r163LiveMoneyPolicyGenerationReason")
	placeOrder := strings.Index(handler, "s.polyUSAuth.PlaceLiveOrder")
	if reservation < 0 || recheck <= reservation || policyRecheck <= recheck ||
		placeOrder <= policyRecheck {
		t.Fatalf("PolyUS wire ordering invalid: reservation=%d authority=%d policy=%d place=%d",
			reservation, recheck, policyRecheck, placeOrder)
	}
	afterRecheck := handler[recheck:placeOrder]
	if !strings.Contains(afterRecheck, "r163RejectPolyUSWireSystemAuthority") ||
		!strings.Contains(afterRecheck, "return") {
		t.Fatal("PolyUS authority refusal does not return a known no-send receipt before PlaceLiveOrder")
	}
}

func TestR163SettingsAuthorityRemovalCannotPublishInsideFinalVenueFence(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = "kalshi|spotlag|YES|taker"
	cfg.Risk.LiveSystemCanaryAllowlist = ""
	s.cfgP.Store(&cfg)
	candidate := liveMirrorCandidate{
		Platform: "kalshi", Ticker: "KXTEST", Side: "YES",
		Source: "auto-cons-spotlag", Family: "spotlag",
	}
	if why := s.r163LiveWireSystemAuthorityReason(
		true, false, candidate, "taker"); why != "" {
		t.Fatalf("initial authority refused: %q", why)
	}

	// This read lock models the exact interval from the handler's final current-config reread
	// through the venue call. Settings may parse concurrently, but its removal must not publish
	// during that interval.
	s.liveWriteFence.RLock()
	fenceHeld := true
	defer func() {
		if fenceHeld {
			s.liveWriteFence.RUnlock()
		}
	}()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/settings",
		strings.NewReader(`{"live_system_allowlist":"kalshi|spotlag|NO|taker"}`))
	done := make(chan struct{})
	go func() {
		s.handleSettings(rec, req)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	settingsAtPublish := false
	for time.Now().Before(deadline) {
		if !s.cfgMu.TryLock() {
			settingsAtPublish = true
			break
		}
		s.cfgMu.Unlock()
		runtime.Gosched()
	}
	if !settingsAtPublish {
		t.Fatal("settings request did not reach its serialized publish boundary")
	}
	select {
	case <-done:
		t.Fatal("authority removal published while a final venue-call read fence was held")
	default:
	}
	if why := s.r163LiveWireSystemAuthorityReason(
		true, false, candidate, "taker"); why != "" {
		t.Fatalf("blocked settings removal became visible inside venue fence: %q", why)
	}

	s.liveWriteFence.RUnlock()
	fenceHeld = false
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("authority settings did not publish after venue fence released")
	}
	if rec.Code != 200 {
		t.Fatalf("settings status=%d body=%s", rec.Code, rec.Body.String())
	}
	if why := s.r163LiveWireSystemAuthorityReason(
		true, false, candidate, "taker"); why !=
		"live-system-identity-not-allowlisted:kalshi|spotlag|YES|taker" {
		t.Fatalf("published removal reason=%q", why)
	}
}

func TestR163UnrelatedSettingsDoNotWaitForFinalVenueFence(t *testing.T) {
	s := testServer(t)
	s.liveWriteFence.RLock()
	defer s.liveWriteFence.RUnlock()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/settings",
		strings.NewReader(`{"stake_usd":3}`))
	done := make(chan struct{})
	go func() {
		s.handleSettings(rec, req)
		close(done)
	}()
	select {
	case <-done:
		if rec.Code != 200 {
			t.Fatalf("unrelated settings status=%d body=%s", rec.Code, rec.Body.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unrelated settings waited on the real-money authority fence")
	}
}

func TestR163SettingsRiskReductionCannotPublishInsideFinalVenueFence(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveMaxOrderUSD = 10
	s.cfgP.Store(&cfg)
	admittedGeneration := s.liveMoneyPolicyGeneration.Load()

	s.liveWriteFence.RLock()
	fenceHeld := true
	defer func() {
		if fenceHeld {
			s.liveWriteFence.RUnlock()
		}
	}()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/settings",
		strings.NewReader(`{"live_max_order_usd":0.25}`))
	done := make(chan struct{})
	go func() {
		s.handleSettings(rec, req)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	reachedPublish := false
	for time.Now().Before(deadline) {
		if !s.cfgMu.TryLock() {
			reachedPublish = true
			break
		}
		s.cfgMu.Unlock()
		runtime.Gosched()
	}
	if !reachedPublish {
		t.Fatal("risk settings request did not reach its serialized publish boundary")
	}
	select {
	case <-done:
		t.Fatal("risk reduction published while a final venue-call read fence was held")
	default:
	}
	if got := s.cfg().Risk.LiveMaxOrderUSD; got != 10 {
		t.Fatalf("risk reduction %.2f became visible inside venue fence", got)
	}
	if got := s.liveMoneyPolicyGeneration.Load(); got != admittedGeneration {
		t.Fatalf("money-policy generation changed inside venue fence: got=%d want=%d",
			got, admittedGeneration)
	}

	s.liveWriteFence.RUnlock()
	fenceHeld = false
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("risk settings did not publish after venue fence released")
	}
	if rec.Code != 200 || s.cfg().Risk.LiveMaxOrderUSD != .25 {
		t.Fatalf("risk settings status=%d max_order=%v body=%s",
			rec.Code, s.cfg().Risk.LiveMaxOrderUSD, rec.Body.String())
	}
	if got := s.liveMoneyPolicyGeneration.Load(); got != admittedGeneration+1 {
		t.Fatalf("money-policy generation=%d want=%d", got, admittedGeneration+1)
	}
}

func TestR163RiskReductionAfterReservationIsDurableKnownNoSend(t *testing.T) {
	for _, venue := range []string{"kalshi", "polyus"} {
		t.Run(venue, func(t *testing.T) {
			s := testServer(t)
			cfg := *s.cfg()
			cfg.Risk.LiveMaxOrderUSD = 10
			s.cfgP.Store(&cfg)
			admittedGeneration := s.liveMoneyPolicyGeneration.Load()
			riskID := "risk-r163-policy-" + venue
			r163WireRiskReservation(t, s.store, riskID, venue, "R163-POLICY", "spotlag")
			leg := 0
			clientOrderID := ""
			if venue == "kalshi" {
				clientOrderID = "r163-wire-client"
			}
			if venue != "kalshi" {
				if err := s.r148AppendRiskEvent(context.Background(), riskID,
					storage.LivePendingRiskSubmitStarted, "entry", "BUY", clientOrderID, "",
					venue+"-create-order", "network mutation begins", &leg, 0, .5, .03,
					nil); err != nil {
					t.Fatal(err)
				}
			}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest("POST", "/api/settings",
				strings.NewReader(`{"live_max_order_usd":0.25}`))
			s.handleSettings(rec, req)
			if rec.Code != 200 {
				t.Fatalf("risk settings status=%d body=%s", rec.Code, rec.Body.String())
			}
			currentGeneration := s.liveMoneyPolicyGeneration.Load()
			if why := r163LiveMoneyPolicyGenerationReason(
				admittedGeneration, currentGeneration); why == "" {
				t.Fatal("post-reservation risk reduction did not invalidate admission")
			}

			var response map[string]any
			var err error
			if venue == "kalshi" {
				response, err = s.r163RejectKalshiWireMoneyPolicy(
					context.Background(), riskID, admittedGeneration, currentGeneration)
			} else {
				response, err = s.r163RejectPolyUSWireMoneyPolicy(
					context.Background(), riskID, admittedGeneration, currentGeneration, 1, .5)
			}
			if err != nil {
				t.Fatal(err)
			}
			if attempted, ok := response["venue_attempted"].(bool); !ok || attempted {
				t.Fatalf("venue_attempted=%v (%T), want explicit false",
					response["venue_attempted"], response["venue_attempted"])
			}
			active, activeErr := s.store.ActiveLivePendingRisk(context.Background())
			if activeErr != nil || len(active) != 0 {
				t.Fatalf("changed-policy no-send stayed reserved: active=%d err=%v",
					len(active), activeErr)
			}
		})
	}
}

func TestR163ManualKillPublicationBlocksQueuedCashReadersUntilTripIsVisible(t *testing.T) {
	s := testServer(t)
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()

	// Model an already-in-flight venue write. The manual trip must queue as the next writer, and
	// Go's writer preference must keep every later cash reader behind it.
	s.liveWriteFence.RLock()
	tripDone := make(chan struct{})
	go func() {
		s.persistManualKillSwitchTrip("r163 queued manual halt")
		close(tripDone)
	}()

	deadline := time.Now().Add(2 * time.Second)
	writerQueued := false
	for time.Now().Before(deadline) {
		if s.liveWriteFence.TryRLock() {
			s.liveWriteFence.RUnlock()
			runtime.Gosched()
			continue
		}
		writerQueued = true
		break
	}
	if !writerQueued {
		s.liveWriteFence.RUnlock()
		t.Fatal("manual trip did not queue at the cash-authority writer")
	}

	readerResult := make(chan string, 1)
	go func() {
		s.liveWriteFence.RLock()
		readerResult <- s.liveWriteBoundaryReason(true)
		s.liveWriteFence.RUnlock()
	}()
	select {
	case got := <-readerResult:
		s.liveWriteFence.RUnlock()
		t.Fatalf("new cash reader crossed a queued manual trip: %q", got)
	default:
	}

	s.liveWriteFence.RUnlock()
	select {
	case got := <-readerResult:
		if !strings.Contains(got, "kill switch tripped") {
			t.Fatalf("queued cash reader did not observe the linearized trip: %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued cash reader did not drain after manual trip")
	}
	select {
	case <-tripDone:
	case <-time.After(2 * time.Second):
		t.Fatal("manual trip did not finish its durable latch")
	}
}

func TestR163ComboWireAuthorityRejectsABAAndMoneyPolicyChange(t *testing.T) {
	tests := []struct {
		name          string
		auto, enabled bool
		admitIntent   uint64
		nowIntent     uint64
		admitPolicy   uint64
		nowPolicy     uint64
		want          string
	}{
		{
			name: "combo off-on ABA invalidates admitted signal",
			auto: true, enabled: true, admitIntent: 20, nowIntent: 22,
			admitPolicy: 7, nowPolicy: 7, want: liveIntentGenerationChangedReason,
		},
		{
			name: "combo currently disabled",
			auto: true, enabled: false, admitIntent: 20, nowIntent: 20,
			admitPolicy: 7, nowPolicy: 7, want: "AUTO combo belt was disabled before RFQ acceptance",
		},
		{
			name: "risk policy changed",
			auto: true, enabled: true, admitIntent: 20, nowIntent: 20,
			admitPolicy: 7, nowPolicy: 8, want: "live-money-policy-changed-after-handler-admission",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := r163LiveComboWireReason(tc.auto, tc.enabled,
				tc.admitIntent, tc.nowIntent, tc.admitPolicy, tc.nowPolicy); got != tc.want {
				t.Fatalf("combo wire reason=%q want=%q", got, tc.want)
			}
		})
	}
}

func TestR163ComboGenerationRecheckDurablyReturnsBeforeVenueAccept(t *testing.T) {
	raw, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	start := strings.Index(src, "func (s *Server) handleLiveComboPlace")
	if start < 0 {
		t.Fatal("combo handler source missing")
	}
	handler := src[start:]
	intentCapture := strings.Index(handler, "admittedIntentGeneration :=")
	policyCapture := strings.Index(handler, "admittedMoneyPolicyGeneration :=")
	recheck := strings.Index(handler, "r163LiveComboWireReason")
	release := strings.Index(handler[recheck:], "s.r148ReleaseRisk")
	accept := strings.Index(handler, "s.kal.AcceptQuoteRFQ")
	if intentCapture < 0 || policyCapture < 0 || recheck <= intentCapture ||
		recheck <= policyCapture || release < 0 || accept <= recheck+release {
		t.Fatalf("combo wire ordering invalid: intent=%d policy=%d recheck=%d release=%d accept=%d",
			intentCapture, policyCapture, recheck, release, accept)
	}
	between := handler[recheck:accept]
	if !strings.Contains(between, "s.liveAutoCombosEnabled()") ||
		!strings.Contains(between, "r163ComboWireNoSendSource") ||
		!strings.Contains(between, "return") {
		t.Fatal("combo authority change lacks current-enable reread and durable no-send return")
	}
}

func TestR163MoneyPolicyClassifierIsConservativeAndEnumerated(t *testing.T) {
	// Every known cash knob that can change eligibility, sizing, route, price, horizon or venue
	// must publish a new generation. This includes all funded combo horizon families.
	cashSettings := []string{
		"min_ev_per_contract",
		"parlay_enabled",
		"parlay_max_hours_out",
		"paper_combo_max_hours_out",
		"paper_combo_crypto_max_hours_out",
		"paper_ml_max_hours_out",
		"paper_ml_crypto_max_hours_out",
		"consensus_max_hours_out",
		"consensus_crypto_max_hours_out",
		"live_system_allowlist",
		"live_system_canary_allowlist",
		"live_system_kalshi",
		"live_system_polyus",
		"live_staged_bundles",
		"live_staged_bundle_allowlist",
		"live_max_order_usd",
		"live_max_order_pct",
		"live_kelly_max_frac",
		"live_exposure_cap_usd",
		"live_exposure_cap_pct",
		"live_cluster_cap_pct",
		"max_order_contracts",
		"max_entry_price",
		"min_entry_price",
		"max_spread",
		"live_prospective_allocation",
		"live_signal_inversion",
		"odds_api_key",
		"kalshi_rate_limit_per_sec",
		"kalshi_request_timeout_ms",
		"kalshi_book_ws_cap",
		"polyus_book_ws_cap",
		"orders_poll_ms",
		"markets_max_pull",
		"markets_refresh_s",
	}
	for _, key := range cashSettings {
		if !r163SettingsPublishesLiveMoneyPolicy(map[string]any{key: true}) {
			t.Errorf("cash setting %q bypassed money-policy generation", key)
		}
	}
	for key := range r163MoneyInertSettings {
		if r163SettingsPublishesLiveMoneyPolicy(map[string]any{key: true}) {
			t.Errorf("explicitly inert setting %q unexpectedly fenced", key)
		}
	}
	if !r163SettingsPublishesLiveMoneyPolicy(map[string]any{
		"future_cash_setting_not_yet_known": true,
	}) {
		t.Fatal("future Settings field did not fail safe to money-affecting")
	}
}
