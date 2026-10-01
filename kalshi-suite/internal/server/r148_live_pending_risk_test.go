package server

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/killswitch"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r148RiskTestServer(t *testing.T) (*Server, *storage.Store) {
	t.Helper()
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return &Server{store: st, ks: killswitch.New(nil)}, st
}

func TestR151OnlyOpenKalshiReceiptsEnterCancelTracking(t *testing.T) {
	for _, tc := range []struct {
		state     string
		remaining float64
		want      bool
	}{
		{"full", 0, false},
		{"unfilled", 0, false},
		{"partial", 0, false}, // terminal IOC partial
		{"partial", 3, true},
		{"pending", 12, true},
		{"ambiguous", 12, false},
	} {
		if got := liveKalshiReceiptNeedsCancelTracking(tc.state, tc.remaining); got != tc.want {
			t.Fatalf("state=%s remaining=%v got=%v want=%v", tc.state, tc.remaining, got, tc.want)
		}
	}
}

func TestR151ePendingRiskPreAckAmbiguityIsDurableAndAutoPaused(t *testing.T) {
	s, st := r148RiskTestServer(t)
	in := r148RiskTestReservation(t, st, "risk-server-1")
	ctx := context.Background()
	leg := 0
	if err := s.r148AppendRiskEvent(ctx, in.ReservationID, storage.LivePendingRiskSubmitStarted,
		"entry", "BUY", "client-1", "", "test", "submit", &leg, 0, 0, 0, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	s.r148RiskAmbiguous(ctx, in.ReservationID, "entry", "unpersisted-order-id", "test",
		"transport result unknown", map[string]any{"possible_order_id": "unpersisted-order-id"})
	if s.ks.Tripped() {
		t.Fatal("automatic ambiguity must never trip the operator-only kill switch")
	}
	if reason := s.liveAutoSafetyPauseReason(); !strings.Contains(reason, liveSafetyPausePendingRisk) {
		t.Fatalf("ambiguous venue mutation did not pause new orders: %q", reason)
	}
	row, found, err := st.LivePendingRiskByID(ctx, in.ReservationID)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	last := row.Events[len(row.Events)-1]
	if last.EventType != storage.LivePendingRiskAmbiguous || last.AttemptKey != "entry" ||
		last.LegIndex == nil || *last.LegIndex != 0 || last.OrderID != "" {
		t.Fatalf("pre-ACK ambiguity was not durably bound to its submit: %+v", last)
	}
	active, err := st.ActiveLivePendingRisk(ctx)
	if err != nil || len(active) != 1 {
		t.Fatalf("ambiguous reservation was not retained: len=%d err=%v", len(active), err)
	}
}

func r148RiskTestReservation(t *testing.T, st *storage.Store, id string) storage.LivePendingRiskIntent {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	in := storage.LivePendingRiskIntent{ReservationID: id, Created: now,
		BaselineObserved: now.Add(-time.Second), Product: "single", DispatchSource: "test",
		SystemID: "spotlag", Route: "taker", PrincipalUSD: 1, FeeUSD: .03, CostUSD: 1.03,
		RequestHash: strings.Repeat("a", 64), ProofJSON: `{}`,
		BaselineReceiptJSON: `{"baseline_clock":"venue-account-watermark-v1","baseline_target_position_found":true}`}
	if id != "risk-server-1" {
		in.RequestHash = strings.Repeat("b", 64)
	}
	inserted, err := st.InsertLivePendingRisk(context.Background(), in,
		[]storage.LivePendingRiskLeg{{Index: 0, Venue: "kalshi", Ticker: "KXTEST",
			Side: "YES", Action: "BUY", ClientOrderID: "client-1", Quantity: 2,
			LimitPrice: .50, ExpectedPositionQty: 2}},
		[]storage.LivePendingRiskCluster{{Index: 0, Venue: "kalshi", ClusterKey: "event:test",
			MappingVersion: r148LiveRiskMappingVersion, ReservedUSD: 1.03}})
	if err != nil || !inserted {
		t.Fatalf("insert=%v err=%v", inserted, err)
	}
	return in
}

func TestR151PendingRiskBaselineUsesAuthenticatedVenueWatermark(t *testing.T) {
	venueAt := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Microsecond)
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/portfolio/positions":
			_, _ = w.Write([]byte(`{"market_positions":[{"ticker":"KXTEST","position_fp":"3","last_updated_ts":"` +
				venueAt.Format(time.RFC3339Nano) + `"}],"cursor":""}`))
		case "/portfolio/orders":
			time.Sleep(50 * time.Millisecond) // exposes the old, incorrect post-REST local end-time
			_, _ = w.Write([]byte(`{"orders":[],"cursor":""}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer venue.Close()
	s, _ := r148RiskTestServer(t)
	s.kal = kalshi.NewClient(venue.URL, queueTestSigner(t), 10000, 2*time.Second)
	baseline, err := s.r148KalshiRiskBaseline(context.Background(), "KXTEST")
	if err != nil {
		t.Fatal(err)
	}
	if !baseline.Observed.Equal(venueAt) || math.Abs(baseline.PositionQty-3) > 1e-9 {
		t.Fatalf("baseline observed=%s qty=%v, want venue watermark=%s qty=3",
			baseline.Observed, baseline.PositionQty, venueAt)
	}
	var receipt r151RiskBaselineReceipt
	if err := json.Unmarshal([]byte(baseline.ReceiptJSON), &receipt); err != nil ||
		receipt.Clock != r151RiskBaselineClock || receipt.TargetPositionFound == nil || !*receipt.TargetPositionFound {
		t.Fatalf("venue watermark receipt=%+v err=%v", receipt, err)
	}
}

func TestR151LegacyPendingRiskClockSkewFallbackIsBoundedAndScoped(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		skew         time.Duration
		wantRelease  bool
	}{
		{"bounded exact scoped fill", "kalshi-get-order+order-scoped-fills", 1250 * time.Millisecond, true},
		{"stale exact scoped fill", "kalshi-get-order+order-scoped-fills", 6 * time.Second, false},
		{"bounded estimated fill", "kalshi-create-order-receipt", time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, st := r148RiskTestServer(t)
			now := time.Now().UTC().Truncate(time.Microsecond)
			in := storage.LivePendingRiskIntent{ReservationID: "risk-legacy-clock", Created: now,
				BaselineObserved: now, Product: "single", DispatchSource: "test", SystemID: "spotlag",
				Route: "taker", PrincipalUSD: 1, FeeUSD: .03, CostUSD: 1.03,
				RequestHash: strings.Repeat("c", 64), ProofJSON: `{}`, BaselineReceiptJSON: `{}`}
			inserted, err := st.InsertLivePendingRisk(context.Background(), in,
				[]storage.LivePendingRiskLeg{{Index: 0, Venue: "kalshi", Ticker: "KXTEST", Side: "YES",
					Action: "BUY", ClientOrderID: "client-1", Quantity: 2, LimitPrice: .5,
					BaselinePositionQty: 0, ExpectedPositionQty: 2}},
				[]storage.LivePendingRiskCluster{{Index: 0, Venue: "kalshi", ClusterKey: "event:test",
					MappingVersion: r148LiveRiskMappingVersion, ReservedUSD: 1.03}})
			if err != nil || !inserted {
				t.Fatalf("insert=%v err=%v", inserted, err)
			}
			leg := 0
			for _, event := range []struct {
				typeName, orderID, source string
				filled, average, fee      float64
			}{
				{storage.LivePendingRiskSubmitStarted, "", "test", 0, .5, 0},
				{storage.LivePendingRiskAck, "order-1", "test", 0, .5, 0},
				{storage.LivePendingRiskFillSeen, "order-1", tc.source, 2, .5, .03},
			} {
				if err := s.r148AppendRiskEvent(context.Background(), in.ReservationID, event.typeName,
					"entry", "BUY", "client-1", event.orderID, event.source, "test", &leg,
					event.filled, event.average, event.fee, map[string]any{}); err != nil {
					t.Fatal(err)
				}
			}
			positionAt := in.BaselineObserved.Add(-tc.skew)
			venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path != "/portfolio/positions" {
					http.NotFound(w, r)
					return
				}
				_, _ = w.Write([]byte(`{"market_positions":[{"ticker":"KXTEST","position_fp":"2","last_updated_ts":"` +
					positionAt.Format(time.RFC3339Nano) + `"}],"cursor":""}`))
			}))
			defer venue.Close()
			s.kal = kalshi.NewClient(venue.URL, queueTestSigner(t), 10000, 2*time.Second)
			if got := s.r148RiskAccountVisible(context.Background(), in.ReservationID, "order-1", "test"); got != tc.wantRelease {
				t.Fatalf("released=%v want=%v", got, tc.wantRelease)
			}
			active, err := st.ActiveLivePendingRisk(context.Background())
			if err != nil || (len(active) == 0) != tc.wantRelease {
				t.Fatalf("active=%d wantRelease=%v err=%v", len(active), tc.wantRelease, err)
			}
		})
	}
}

func TestR151AuthenticatedSettlementReleasesFilledSingleAfterPositionDisappears(t *testing.T) {
	s, st := r148RiskTestServer(t)
	in := r148RiskTestReservation(t, st, "risk-settled-single")
	ctx := context.Background()
	leg := 0
	for _, event := range []struct {
		typeName, orderID, source string
		filled, average, fee      float64
	}{
		{storage.LivePendingRiskSubmitStarted, "", "test", 0, .5, 0},
		{storage.LivePendingRiskAck, "order-settled", "test", 0, .5, 0},
		{storage.LivePendingRiskFillSeen, "order-settled", "kalshi-get-order+order-scoped-fills", 2, .5, .03},
	} {
		if err := s.r148AppendRiskEvent(ctx, in.ReservationID, event.typeName, "entry", "BUY",
			"client-1", event.orderID, event.source, "test", &leg, event.filled, event.average,
			event.fee, map[string]any{}); err != nil {
			t.Fatal(err)
		}
	}
	settledAt := time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
	var settlement kalshi.Settlement
	if err := json.Unmarshal([]byte(`{"ticker":"KXTEST","market_result":"yes",`+
		`"yes_count_fp":"2","no_count_fp":"0","yes_total_cost_dollars":"1.00",`+
		`"no_total_cost_dollars":"0","revenue":"200","fee_cost":"0.03",`+
		`"settled_time":"`+settledAt+`"}`), &settlement); err != nil {
		t.Fatal(err)
	}
	s.r151ReleaseSettledSingleRisks(ctx, []kalshi.Settlement{settlement})
	active, err := st.ActiveLivePendingRisk(ctx)
	if err != nil || len(active) != 0 {
		t.Fatalf("settled exact single stayed reserved: active=%d err=%v", len(active), err)
	}
	row, found, err := st.LivePendingRiskByID(ctx, in.ReservationID)
	if err != nil || !found || !r148RiskHasEvent(row, storage.LivePendingRiskAccountVisible,
		storage.LivePendingRiskReleased) {
		t.Fatalf("terminal settlement proof missing: found=%v err=%v events=%+v", found, err, row.Events)
	}

	// The same settlement cannot release a reservation whose exact fee differs. This guards against
	// treating a same-ticker settlement with other/manual account activity as our order lifecycle.
	s2, st2 := r148RiskTestServer(t)
	in2 := r148RiskTestReservation(t, st2, "risk-settled-mismatch")
	for _, event := range []struct {
		typeName, orderID, source string
		filled, average, fee      float64
	}{
		{storage.LivePendingRiskSubmitStarted, "", "test", 0, .5, 0},
		{storage.LivePendingRiskAck, "order-other", "test", 0, .5, 0},
		{storage.LivePendingRiskFillSeen, "order-other", "kalshi-get-order+order-scoped-fills", 2, .5, .04},
	} {
		if err := s2.r148AppendRiskEvent(ctx, in2.ReservationID, event.typeName, "entry", "BUY",
			"client-1", event.orderID, event.source, "test", &leg, event.filled, event.average,
			event.fee, map[string]any{}); err != nil {
			t.Fatal(err)
		}
	}
	s2.r151ReleaseSettledSingleRisks(ctx, []kalshi.Settlement{settlement})
	active, err = st2.ActiveLivePendingRisk(ctx)
	if err != nil || len(active) != 1 {
		t.Fatalf("mismatched settlement released risk: active=%d err=%v", len(active), err)
	}
}

func TestR148PendingRiskTerminalHelperKeepsExactAttemptIdentity(t *testing.T) {
	s, st := r148RiskTestServer(t)
	in := r148RiskTestReservation(t, st, "risk-server-1")
	ctx := context.Background()
	leg := 0
	if err := s.r148AppendRiskEvent(ctx, in.ReservationID, storage.LivePendingRiskSubmitStarted,
		"entry", "BUY", "client-1", "", "test", "submit", &leg, 0, 0, 0, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if err := s.r148AppendRiskEvent(ctx, in.ReservationID, storage.LivePendingRiskAck,
		"entry", "BUY", "client-1", "order-1", "test", "ack", &leg, 0, 0, 0, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if err := s.r148RiskTerminalUnfilled(ctx, in.ReservationID, "entry", "wrong-order",
		"test", "unfilled", map[string]any{}); err == nil {
		t.Fatal("terminal helper accepted a different order id")
	}
	if err := s.r148RiskTerminalUnfilled(ctx, in.ReservationID, "entry", "order-1",
		"test", "unfilled", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	active, err := st.ActiveLivePendingRisk(ctx)
	if err != nil || len(active) != 0 {
		t.Fatalf("terminal zero-fill did not release: len=%d err=%v", len(active), err)
	}
	row, found, err := st.LivePendingRiskByID(ctx, in.ReservationID)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	terminal := row.Events[len(row.Events)-2]
	if terminal.EventType != storage.LivePendingRiskTerminalUnfilled || terminal.LegIndex == nil ||
		*terminal.LegIndex != 0 || terminal.AttemptKey != "entry" || terminal.Action != "BUY" ||
		terminal.ClientOrderID != "client-1" || terminal.OrderID != "order-1" {
		t.Fatalf("terminal event lost immutable attempt identity: %+v", terminal)
	}
	if release := row.Events[len(row.Events)-1]; release.EventType != storage.LivePendingRiskReleased ||
		release.LegIndex != nil || release.AttemptKey != "" || release.Action != "" {
		t.Fatalf("release was not reservation-scoped: %+v", release)
	}
}

func TestR148PendingRiskOverlayDedupAndMultiLegConservatism(t *testing.T) {
	s, st := r148RiskTestServer(t)
	ctx := context.Background()
	in := r148RiskTestReservation(t, st, "risk-server-1")
	leg0 := 0
	for _, event := range []storage.LivePendingRiskEvent{
		{ReservationID: in.ReservationID, Observed: time.Now().UTC(),
			EventType: storage.LivePendingRiskSubmitStarted, LegIndex: &leg0, AttemptKey: "entry",
			Action: "BUY", ClientOrderID: "client-1", ReceiptSource: "test", Reason: "submit", EvidenceJSON: `{}`},
		{ReservationID: in.ReservationID, Observed: time.Now().UTC(),
			EventType: storage.LivePendingRiskAck, LegIndex: &leg0, AttemptKey: "entry", Action: "BUY",
			ClientOrderID: "client-1", OrderID: "order-1", ReceiptSource: "test", Reason: "ack", EvidenceJSON: `{}`},
	} {
		if ok, err := st.AppendLivePendingRiskEvent(ctx, event); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	total, clusters, err := s.r148PendingRiskOverlay(ctx, "kalshi", map[string]float64{"order-1": .80})
	if err != nil || math.Abs(total-.23) > 1e-9 || math.Abs(clusters["event:test"]-.23) > 1e-9 {
		t.Fatalf("resting de-dup overlay total=%.9f clusters=%v err=%v", total, clusters, err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	staged := storage.LivePendingRiskIntent{ReservationID: "risk-staged-overlay", Created: now,
		BaselineObserved: now.Add(-time.Second), Product: "staged", DispatchSource: "test",
		SystemID: "event-basket-lock", Route: "staged_fok", PrincipalUSD: .70, FeeUSD: .02,
		CostUSD: .72, BundleID: "bundle", RequestHash: strings.Repeat("c", 64),
		ProofJSON: `{}`, BaselineReceiptJSON: `{}`}
	legs := []storage.LivePendingRiskLeg{
		{Index: 0, Venue: "kalshi", Ticker: "KXA", Side: "YES", Action: "BUY",
			ClientOrderID: "stg-a", Quantity: 1, LimitPrice: .40, ExpectedPositionQty: 1},
		{Index: 1, Venue: "kalshi", Ticker: "KXB", Side: "YES", Action: "BUY",
			ClientOrderID: "stg-b", Quantity: 1, LimitPrice: .30, ExpectedPositionQty: 1},
	}
	clusterRows := []storage.LivePendingRiskCluster{
		{Index: 0, Venue: "kalshi", ClusterKey: "event:a", MappingVersion: r148LiveRiskMappingVersion, ReservedUSD: .72},
		{Index: 1, Venue: "kalshi", ClusterKey: "event:b", MappingVersion: r148LiveRiskMappingVersion, ReservedUSD: .72},
	}
	if ok, err := st.InsertLivePendingRisk(ctx, staged, legs, clusterRows); err != nil || !ok {
		t.Fatal(ok, err)
	}
	for _, event := range []storage.LivePendingRiskEvent{
		{ReservationID: staged.ReservationID, Observed: now.Add(time.Second),
			EventType: storage.LivePendingRiskSubmitStarted, LegIndex: &leg0, AttemptKey: "leg-0-buy",
			Action: "BUY", ClientOrderID: "stg-a", ReceiptSource: "test", Reason: "submit", EvidenceJSON: `{}`},
		{ReservationID: staged.ReservationID, Observed: now.Add(2 * time.Second),
			EventType: storage.LivePendingRiskAck, LegIndex: &leg0, AttemptKey: "leg-0-buy", Action: "BUY",
			ClientOrderID: "stg-a", OrderID: "staged-order", ReceiptSource: "test", Reason: "ack", EvidenceJSON: `{}`},
		{ReservationID: staged.ReservationID, Observed: now.Add(3 * time.Second),
			EventType: storage.LivePendingRiskFillSeen, LegIndex: &leg0, AttemptKey: "leg-0-buy", Action: "BUY",
			ClientOrderID: "stg-a", OrderID: "staged-order", FilledQty: 1, AveragePrice: .40, FeeTotal: .01,
			ReceiptSource: "test", Reason: "fill", EvidenceJSON: `{}`},
		{ReservationID: staged.ReservationID, Observed: now.Add(4 * time.Second), AccountObserved: now.Add(4 * time.Second),
			EventType: storage.LivePendingRiskAccountVisible, LegIndex: &leg0, AttemptKey: "leg-0-buy", Action: "BUY",
			ClientOrderID: "stg-a", OrderID: "staged-order", FilledQty: 1, AveragePrice: .40, FeeTotal: .01,
			ReceiptSource: "test", Reason: "visible", EvidenceJSON: `{}`},
	} {
		if ok, err := st.AppendLivePendingRiskEvent(ctx, event); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	total, clusters, err = s.r148PendingRiskOverlay(ctx, "kalshi", nil)
	// Existing single contributes its full $1.03 now that the supplied resting snapshot is empty;
	// the staged package must still contribute all $0.72 despite one account-visible leg.
	if err != nil || math.Abs(total-1.75) > 1e-9 || math.Abs(clusters["event:a"]-.72) > 1e-9 ||
		math.Abs(clusters["event:b"]-.72) > 1e-9 {
		t.Fatalf("multi-leg reservation undercount total=%.9f clusters=%v err=%v", total, clusters, err)
	}
}

func r148AckRiskTestReservation(t *testing.T, s *Server, st *storage.Store, id string) storage.LivePendingRiskIntent {
	t.Helper()
	in := r148RiskTestReservation(t, st, id)
	leg := 0
	for _, event := range []storage.LivePendingRiskEvent{
		{ReservationID: in.ReservationID, Observed: time.Now().UTC(),
			EventType: storage.LivePendingRiskSubmitStarted, LegIndex: &leg, AttemptKey: "entry",
			Action: "BUY", ClientOrderID: "client-1", ReceiptSource: "test", Reason: "submit", EvidenceJSON: `{}`},
		{ReservationID: in.ReservationID, Observed: time.Now().UTC(),
			EventType: storage.LivePendingRiskAck, LegIndex: &leg, AttemptKey: "entry", Action: "BUY",
			ClientOrderID: "client-1", OrderID: "order-1", ReceiptSource: "test", Reason: "ack", EvidenceJSON: `{}`},
	} {
		if ok, err := st.AppendLivePendingRiskEvent(context.Background(), event); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	return in
}

func TestR148SingleRecoveryTracksLateMakerCancelWithoutProcessMemory(t *testing.T) {
	s, st := r148RiskTestServer(t)
	in := r148AckRiskTestReservation(t, s, st, "risk-server-2")
	var canceled atomic.Bool
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/portfolio/orders/order-1":
			status, remaining := "resting", "2"
			if canceled.Load() {
				status, remaining = "canceled", "0"
			}
			_, _ = w.Write([]byte(`{"order":{"order_id":"order-1","client_order_id":"client-1","ticker":"KXTEST","status":"` + status + `","type":"limit","side":"yes","action":"buy","outcome_side":"yes","book_side":"bid","yes_price_dollars":"0.50","no_price_dollars":"0.50","fill_count_fp":"0","remaining_count_fp":"` + remaining + `","initial_count_fp":"2"}}`))
		case "/portfolio/fills":
			_, _ = w.Write([]byte(`{"fills":[],"cursor":""}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer venue.Close()
	s.kal = kalshi.NewClient(venue.URL, queueTestSigner(t), 10000, 2*time.Second)

	if got := s.r148PendingRiskTickerConflict(context.Background(), "kalshi", "KXTEST"); got != "existing-durable-pending-order" {
		t.Fatalf("restart-safe ticker guard=%q", got)
	}
	s.reconcileR148SinglePendingRisks(context.Background())
	row, found, err := st.LivePendingRiskByID(context.Background(), in.ReservationID)
	if err != nil || !found || !r148RiskHasEvent(row, storage.LivePendingRiskRestingVisible) {
		t.Fatalf("late maker was not durably observed resting: found=%v err=%v events=%+v", found, err, row.Events)
	}
	canceled.Store(true)
	s.reconcileR148SinglePendingRisks(context.Background())
	active, err := st.ActiveLivePendingRisk(context.Background())
	if err != nil || len(active) != 0 {
		t.Fatalf("late maker cancel did not release exact reservation: active=%d err=%v", len(active), err)
	}
	if got := s.r148PendingRiskTickerConflict(context.Background(), "kalshi", "KXTEST"); got != "" {
		t.Fatalf("released ticker remained blocked: %q", got)
	}
}

func TestR151KalshiIOCPointLookup404UsesOnlyCompleteScopedFillAndAccountTruth(t *testing.T) {
	s, st := r148RiskTestServer(t)
	in := r148AckRiskTestReservation(t, s, st, "risk-server-2")
	positionUpdated := time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano)
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/portfolio/orders/order-1":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":"not_found","message":"not found","service":"query-exchange"}}`))
		case "/portfolio/fills":
			if r.URL.Query().Get("order_id") != "order-1" {
				t.Fatalf("fill lookup was not scoped to acknowledged order: %s", r.URL.String())
			}
			_, _ = w.Write([]byte(`{"fills":[{"fill_id":"fill-1","order_id":"order-1","ticker":"KXTEST","outcome_side":"yes","book_side":"bid","count_fp":"2","yes_price_dollars":"0.49","no_price_dollars":"0.51","fee_cost":"0.02","is_taker":true}],"cursor":""}`))
		case "/portfolio/positions":
			_, _ = w.Write([]byte(`{"market_positions":[{"ticker":"KXTEST","position_fp":"2","last_updated_ts":"` + positionUpdated + `"}],"cursor":""}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer venue.Close()
	s.kal = kalshi.NewClient(venue.URL, queueTestSigner(t), 10000, 2*time.Second)

	s.reconcileR148SinglePendingRisks(context.Background())
	active, err := st.ActiveLivePendingRisk(context.Background())
	if err != nil || len(active) != 0 || s.ks.Tripped() {
		t.Fatalf("exact IOC 404 recovery active=%d tripped=%v err=%v", len(active), s.ks.Tripped(), err)
	}
	row, found, err := st.LivePendingRiskByID(context.Background(), in.ReservationID)
	if err != nil || !found || !r148RiskHasEvent(row, storage.LivePendingRiskFillSeen) ||
		!r148RiskHasEvent(row, storage.LivePendingRiskAccountVisible) ||
		!r148RiskHasEvent(row, storage.LivePendingRiskReleased) {
		t.Fatalf("exact IOC lifecycle missing: found=%v err=%v events=%+v", found, err, row.Events)
	}
	foundExact := false
	for _, event := range row.Events {
		if event.EventType == storage.LivePendingRiskFillSeen &&
			event.ReceiptSource == "kalshi-create-ack+order-scoped-fills" &&
			math.Abs(event.FilledQty-2) < 1e-9 && math.Abs(event.AveragePrice-.49) < 1e-9 &&
			math.Abs(event.FeeTotal-.02) < 1e-9 {
			foundExact = true
		}
	}
	if !foundExact {
		t.Fatalf("exact ack+fill receipt was not persisted: %+v", row.Events)
	}
}

func TestR151KalshiIOCPointLookup404RejectsWrongOrIncompleteScopedFills(t *testing.T) {
	leg := storage.LivePendingRiskLeg{Index: 0, Venue: "kalshi", Ticker: "KXTEST", Side: "YES",
		Action: "BUY", Quantity: 2, LimitPrice: .50}
	identity := r148RiskAttemptIdentity{AttemptKey: "entry", LegIndex: 0, Action: "BUY",
		ClientOrderID: "client-1", OrderID: "order-1", Acked: true}
	fill := func(raw string) kalshi.Fill {
		t.Helper()
		var out kalshi.Fill
		if err := json.Unmarshal([]byte(raw), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	valid := `{"fill_id":"fill-1","order_id":"order-1","ticker":"KXTEST","outcome_side":"yes","book_side":"bid","count_fp":"2","yes_price_dollars":"0.49","no_price_dollars":"0.51","fee_cost":"0.02","is_taker":true}`
	cases := map[string][]kalshi.Fill{
		"zero fills":       {},
		"partial quantity": {fill(strings.Replace(valid, `"2"`, `"1"`, 1))},
		"wrong order":      {fill(strings.Replace(valid, `"order-1"`, `"other-order"`, 1))},
		"wrong ticker":     {fill(strings.Replace(valid, `"KXTEST"`, `"KXOTHER"`, 1))},
		"wrong side":       {fill(strings.Replace(strings.Replace(valid, `"yes"`, `"no"`, 1), `"bid"`, `"ask"`, 1))},
		"adverse price":    {fill(strings.Replace(valid, `"0.49"`, `"0.51"`, 1))},
		"missing fee":      {fill(strings.Replace(valid, `,"fee_cost":"0.02"`, ``, 1))},
		"not taker":        {fill(strings.Replace(valid, `"is_taker":true`, `"is_taker":false`, 1))},
		"duplicate fill":   {fill(valid), fill(valid)},
	}
	for name, fills := range cases {
		t.Run(name, func(t *testing.T) {
			if receipt, err := r151SingleKalshiAckFillReceipt(fills, leg, identity, "taker"); err == nil ||
				receipt.Authoritative || receipt.State != r148ReceiptAmbiguous {
				t.Fatalf("unsafe IOC 404 receipt accepted: %+v err=%v", receipt, err)
			}
		})
	}
}

func TestR148SingleRecoveryFindsPreAckKalshiOrderByDeterministicClientID(t *testing.T) {
	s, st := r148RiskTestServer(t)
	in := r148RiskTestReservation(t, st, "risk-server-2")
	leg := 0
	if err := s.r148AppendRiskEvent(context.Background(), in.ReservationID,
		storage.LivePendingRiskSubmitStarted, "entry", "BUY", "client-1", "", "test",
		"submit", &leg, 0, 0, 0, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	order := `{"order_id":"order-1","client_order_id":"client-1","ticker":"KXTEST","status":"canceled","type":"limit","side":"yes","action":"buy","outcome_side":"yes","book_side":"bid","yes_price_dollars":"0.50","no_price_dollars":"0.50","fill_count_fp":"0","remaining_count_fp":"0","initial_count_fp":"2"}`
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/portfolio/orders":
			_, _ = w.Write([]byte(`{"orders":[` + order + `],"cursor":""}`))
		case "/portfolio/orders/order-1":
			_, _ = w.Write([]byte(`{"order":` + order + `}`))
		case "/portfolio/fills":
			_, _ = w.Write([]byte(`{"fills":[],"cursor":""}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer venue.Close()
	s.kal = kalshi.NewClient(venue.URL, queueTestSigner(t), 10000, 2*time.Second)
	s.reconcileR148SinglePendingRisks(context.Background())
	active, err := st.ActiveLivePendingRisk(context.Background())
	if err != nil || len(active) != 0 {
		t.Fatalf("pre-ACK exact order was not recovered and released: active=%d err=%v", len(active), err)
	}
	row, found, err := st.LivePendingRiskByID(context.Background(), in.ReservationID)
	if err != nil || !found || !r148RiskAttemptHasEvent(row, "entry", storage.LivePendingRiskAck) ||
		!r148RiskAttemptHasEvent(row, "entry", storage.LivePendingRiskTerminalUnfilled) {
		t.Fatalf("recovered acknowledgement/terminal truth missing: found=%v err=%v events=%+v", found, err, row.Events)
	}
}

func TestR148SingleRecoveryReleasesTerminalPartialOnlyAfterExactPosition(t *testing.T) {
	s, st := r148RiskTestServer(t)
	in := r148AckRiskTestReservation(t, s, st, "risk-server-2")
	stalePositionUpdated := in.BaselineObserved.Add(-time.Second).Format(time.RFC3339Nano)
	freshPositionUpdated := time.Now().UTC().Format(time.RFC3339Nano)
	var freshPosition atomic.Bool
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/portfolio/orders/order-1":
			_, _ = w.Write([]byte(`{"order":{"order_id":"order-1","client_order_id":"client-1","ticker":"KXTEST","status":"canceled","type":"limit","side":"yes","action":"buy","outcome_side":"yes","book_side":"bid","yes_price_dollars":"0.50","no_price_dollars":"0.50","fill_count_fp":"1","remaining_count_fp":"0","initial_count_fp":"2"}}`))
		case "/portfolio/fills":
			_, _ = w.Write([]byte(`{"fills":[{"fill_id":"fill-1","order_id":"order-1","ticker":"KXTEST","outcome_side":"yes","book_side":"bid","count_fp":"1","yes_price_dollars":"0.50","no_price_dollars":"0.50","fee_cost":"0.01"}],"cursor":""}`))
		case "/portfolio/positions":
			positionUpdated := stalePositionUpdated
			if freshPosition.Load() {
				positionUpdated = freshPositionUpdated
			}
			_, _ = w.Write([]byte(`{"market_positions":[{"ticker":"KXTEST","position_fp":"1","last_updated_ts":"` + positionUpdated + `"}],"cursor":""}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer venue.Close()
	s.kal = kalshi.NewClient(venue.URL, queueTestSigner(t), 10000, 2*time.Second)
	s.reconcileR148SinglePendingRisks(context.Background())
	active, err := st.ActiveLivePendingRisk(context.Background())
	if err != nil || len(active) != 1 {
		t.Fatalf("stale aggregate position incorrectly released reservation: active=%d err=%v", len(active), err)
	}
	freshPosition.Store(true)
	s.reconcileR148SinglePendingRisks(context.Background())
	active, err = st.ActiveLivePendingRisk(context.Background())
	if err != nil || len(active) != 0 {
		t.Fatalf("terminal partial fill was not released after exact account visibility: active=%d err=%v", len(active), err)
	}
	row, found, err := st.LivePendingRiskByID(context.Background(), in.ReservationID)
	if err != nil || !found || !r148RiskHasEvent(row, storage.LivePendingRiskFillSeen) ||
		!r148RiskHasEvent(row, storage.LivePendingRiskAccountVisible) ||
		!r148RiskHasEvent(row, storage.LivePendingRiskReleased) {
		t.Fatalf("terminal partial lifecycle incomplete: found=%v err=%v events=%+v", found, err, row.Events)
	}
	fillEvents := 0
	for _, event := range row.Events {
		if event.EventType == storage.LivePendingRiskFillSeen {
			fillEvents++
		}
	}
	if fillEvents != 1 {
		t.Fatalf("unchanged cumulative partial fill replay grew the ledger: fill events=%d", fillEvents)
	}
}

func TestR148SingleRecoveryFinishesReleaseAfterCrashBetweenTerminalCommits(t *testing.T) {
	s, st := r148RiskTestServer(t)
	in := r148AckRiskTestReservation(t, s, st, "risk-server-2")
	leg := 0
	if err := s.r148AppendRiskEvent(context.Background(), in.ReservationID,
		storage.LivePendingRiskTerminalUnfilled, "entry", "BUY", "client-1", "order-1",
		"test", "terminal receipt committed before simulated crash", &leg, 0, 0, 0, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	s.reconcileR148SinglePendingRisks(context.Background())
	active, err := st.ActiveLivePendingRisk(context.Background())
	if err != nil || len(active) != 0 {
		t.Fatalf("restart did not finish terminal release: active=%d err=%v", len(active), err)
	}
}

func TestR148SingleRecoveryShutdownCancellationDoesNotTripKillSwitch(t *testing.T) {
	s, st := r148RiskTestServer(t)
	_ = r148AckRiskTestReservation(t, s, st, "risk-server-2")
	s.kal = kalshi.NewClient("http://127.0.0.1:1", queueTestSigner(t), 10000, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.reconcileR148SinglePendingRisks(ctx)
	if s.ks.Tripped() {
		t.Fatal("normal shutdown cancellation persisted an ambiguous-order kill trip")
	}
	active, err := st.ActiveLivePendingRisk(context.Background())
	if err != nil || len(active) != 1 {
		t.Fatalf("shutdown cancellation did not conservatively retain reservation: active=%d err=%v", len(active), err)
	}
}

func TestR148PreArmPendingRiskReleasesPreNetworkAndBlocksSubmitted(t *testing.T) {
	t.Run("reservation only is safe to release while disarmed", func(t *testing.T) {
		s, st := r148RiskTestServer(t)
		_ = r148RiskTestReservation(t, st, "risk-server-2")
		if why := s.r148PreArmPendingRiskReason(context.Background()); why != "" {
			t.Fatalf("pre-network reservation blocked ARM: %s", why)
		}
		active, err := st.ActiveLivePendingRisk(context.Background())
		if err != nil || len(active) != 0 {
			t.Fatalf("reservation-only row remained active: %d %v", len(active), err)
		}
	})
	t.Run("submitted unresolved row blocks", func(t *testing.T) {
		s, st := r148RiskTestServer(t)
		in := r148RiskTestReservation(t, st, "risk-server-2")
		leg := 0
		if err := s.r148AppendRiskEvent(context.Background(), in.ReservationID,
			storage.LivePendingRiskSubmitStarted, "entry", "BUY", "client-1", "", "test",
			"submit", &leg, 0, 0, 0, map[string]any{}); err != nil {
			t.Fatal(err)
		}
		why := s.r148PreArmPendingRiskReason(context.Background())
		if !strings.Contains(why, "unresolved durable venue mutation blocks ARM") {
			t.Fatalf("submitted unknown did not block ARM: %q", why)
		}
	})
}

func TestR148LiveArmHandlerRefusesUnresolvedDurableSubmit(t *testing.T) {
	s := testServer(t)
	r149EnableLiveDestinations(s, true, false)
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/portfolio/orders":
			_, _ = w.Write([]byte(`{"orders":[],"cursor":""}`))
		default:
			_, _ = w.Write([]byte(`{"market_positions":[],"cursor":"","balance":10000}`))
		}
	}))
	defer venue.Close()
	s.kal = kalshi.NewClient(venue.URL, queueTestSigner(t), 10000, 2*time.Second)
	in := r148RiskTestReservation(t, s.store, "risk-server-2")
	leg := 0
	if err := s.r148AppendRiskEvent(context.Background(), in.ReservationID,
		storage.LivePendingRiskSubmitStarted, "entry", "BUY", "client-1", "", "test",
		"submit", &leg, 0, 0, 0, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/api/live/arm", strings.NewReader(`{"confirm":true}`))
	w := httptest.NewRecorder()
	s.handleLiveArm(w, r)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "unresolved durable venue mutation blocks ARM") {
		t.Fatalf("ARM pending-risk response=%d %s", w.Code, w.Body.String())
	}
	s.liveMu.Lock()
	armed, auto := s.liveArmed, s.liveAuto
	s.liveMu.Unlock()
	if armed || auto || s.kal.Armed() {
		t.Fatalf("unresolved durable submit left a write gate enabled: armed=%v auto=%v client=%v", armed, auto, s.kal.Armed())
	}
}

func TestR148PreArmPermitsAccountVisibleComboAttribution(t *testing.T) {
	s, st := r148RiskTestServer(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	in := storage.LivePendingRiskIntent{ReservationID: "risk-combo-visible", Created: now,
		BaselineObserved: now.Add(-time.Second), Product: "combo", DispatchSource: "test",
		SystemID: "combo", Route: "rfq", PrincipalUSD: .50, FeeUSD: .01, CostUSD: .51,
		RFQID: "rfq-1", QuoteID: "quote-1",
		RequestHash: strings.Repeat("d", 64), ProofJSON: `{}`, BaselineReceiptJSON: `{}`}
	legs := []storage.LivePendingRiskLeg{{Index: 0, Venue: "kalshi", Ticker: "KXCOMBO",
		Side: "YES", Action: "BUY", Quantity: 1, LimitPrice: .50, ExpectedPositionQty: 1}}
	clusters := []storage.LivePendingRiskCluster{{Index: 0, Venue: "kalshi", ClusterKey: "event:combo",
		MappingVersion: r148LiveRiskMappingVersion, ReservedUSD: .51}}
	if ok, err := st.InsertLivePendingRisk(context.Background(), in, legs, clusters); err != nil || !ok {
		t.Fatal(ok, err)
	}
	leg := 0
	for _, event := range []storage.LivePendingRiskEvent{
		{ReservationID: in.ReservationID, Observed: now.Add(time.Second), EventType: storage.LivePendingRiskSubmitStarted,
			LegIndex: &leg, AttemptKey: "rfq", Action: "BUY", ReceiptSource: "test", Reason: "submit", EvidenceJSON: `{}`},
		{ReservationID: in.ReservationID, Observed: now.Add(2 * time.Second), EventType: storage.LivePendingRiskAck,
			LegIndex: &leg, AttemptKey: "rfq", Action: "BUY", OrderID: "combo-order", ReceiptSource: "test", Reason: "ack", EvidenceJSON: `{}`},
		{ReservationID: in.ReservationID, Observed: now.Add(3 * time.Second), EventType: storage.LivePendingRiskFillSeen,
			LegIndex: &leg, AttemptKey: "rfq", Action: "BUY", OrderID: "combo-order", FilledQty: 1,
			AveragePrice: .50, FeeTotal: .01, ReceiptSource: "test", Reason: "fill", EvidenceJSON: `{}`},
		{ReservationID: in.ReservationID, Observed: now.Add(4 * time.Second), AccountObserved: now.Add(4 * time.Second),
			EventType: storage.LivePendingRiskAccountVisible, LegIndex: &leg, AttemptKey: "rfq", Action: "BUY",
			OrderID: "combo-order", FilledQty: 1, AveragePrice: .50, FeeTotal: .01,
			ReceiptSource: "test", Reason: "visible", EvidenceJSON: `{}`},
	} {
		if ok, err := st.AppendLivePendingRiskEvent(context.Background(), event); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	if why := s.r148PreArmPendingRiskReason(context.Background()); why != "" {
		t.Fatalf("account-visible combo attribution blocked ARM: %s", why)
	}
	active, err := st.ActiveLivePendingRisk(context.Background())
	if err != nil || len(active) != 1 {
		t.Fatalf("combo attribution should remain active until settlement: %d %v", len(active), err)
	}
}

func TestR148SinglePUSReceiptRequiresExactIdentityAndSupportsLatePartialCancel(t *testing.T) {
	row := storage.LivePendingRiskReservation{Intent: storage.LivePendingRiskIntent{Route: "maker"}}
	leg := storage.LivePendingRiskLeg{Venue: "polyus", Ticker: "pus-test", Side: "NO", Action: "BUY",
		Quantity: 2, LimitPrice: .40}
	identity := r148RiskAttemptIdentity{AttemptKey: "entry", Action: "BUY", OrderID: "pus-order", Acked: true}
	state := polymarketus.OrderState{ID: "pus-order", MarketSlug: "pus-test", Outcome: "NO", Action: "BUY",
		OrderQty: 2, State: "ORDER_STATE_CANCELED", Cum: 1, Leaves: 0, AvgPx: .39, YesPx: .60,
		TIF: "TIME_IN_FORCE_GOOD_TILL_CANCEL", CommissionTotalUSD: .01, CommissionTotalKnown: true}
	receipt, err := r148SinglePUSReceipt(state, row, leg, identity)
	if err != nil || receipt.State != r148ReceiptFilled || receipt.FilledQty != 1 || !receipt.Authoritative {
		t.Fatalf("terminal PolyUS partial receipt=%+v err=%v", receipt, err)
	}
	state.MarketSlug = "different"
	if _, err := r148SinglePUSReceipt(state, row, leg, identity); err == nil {
		t.Fatal("PolyUS recovery accepted a different immutable market")
	}
}

func TestR148ComboRiskUsesExactCreatorOrderAndRetainsClustersUntilSettlement(t *testing.T) {
	s, st := r148RiskTestServer(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	intent := storage.LivePendingRiskIntent{ReservationID: "risk-combo-exact", Created: now,
		BaselineObserved: now.Add(-time.Second), Product: "combo", DispatchSource: "test",
		SystemID: "event-basket-lock", Route: "rfq", PrincipalUSD: .40, FeeUSD: .01,
		CostUSD: .41, RFQID: "rfq-1", QuoteID: "quote-1", RequestHash: strings.Repeat("d", 64),
		ProofJSON: `{}`, BaselineReceiptJSON: `{}`}
	if ok, err := st.InsertLivePendingRisk(ctx, intent,
		[]storage.LivePendingRiskLeg{{Index: 0, Venue: "kalshi", Ticker: "KXMVE",
			Side: "YES", Action: "BUY", Quantity: 1, LimitPrice: .40, ExpectedPositionQty: 1}},
		[]storage.LivePendingRiskCluster{
			{Index: 0, Venue: "kalshi", ClusterKey: "event:a", MappingVersion: r148LiveRiskMappingVersion, ReservedUSD: .41},
			{Index: 1, Venue: "kalshi", ClusterKey: "event:b", MappingVersion: r148LiveRiskMappingVersion, ReservedUSD: .41},
		}); err != nil || !ok {
		t.Fatal(ok, err)
	}
	leg := 0
	if err := s.r148AppendRiskEvent(ctx, intent.ReservationID, storage.LivePendingRiskSubmitStarted,
		"accept", "BUY", "", "", "test", "accept begins", &leg, 0, .40, .01, nil); err != nil {
		t.Fatal(err)
	}
	row, found, err := st.LivePendingRiskByID(ctx, intent.ReservationID)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	if err := s.r148ComboRecordFillAndVisibility(ctx, row, "creator-order-1", 1, .40, .01,
		1, "test", map[string]any{"fill_id": "fill-1"}); err != nil {
		t.Fatal(err)
	}
	total, clusters, err := s.r148PendingRiskOverlay(ctx, "kalshi", nil)
	if err != nil || math.Abs(total) > 1e-9 || math.Abs(clusters["event:a"]-.41) > 1e-9 ||
		math.Abs(clusters["event:b"]-.41) > 1e-9 {
		t.Fatalf("combo visibility overlay total=%.9f clusters=%v err=%v", total, clusters, err)
	}
	row, found, err = st.LivePendingRiskByID(ctx, intent.ReservationID)
	if err != nil || !found || r148ComboRiskOrderID(row) != "creator-order-1" {
		t.Fatalf("exact creator identity missing found=%v err=%v row=%+v", found, err, row)
	}
	if err := s.r148ReleaseSettledComboRisk(ctx, "rfq-1", "quote-1", "test-settlement",
		map[string]any{"settled": true}); err != nil {
		t.Fatal(err)
	}
	active, err := st.ActiveLivePendingRisk(ctx)
	if err != nil || len(active) != 0 {
		t.Fatalf("settled combo reservation stayed active len=%d err=%v", len(active), err)
	}
}
