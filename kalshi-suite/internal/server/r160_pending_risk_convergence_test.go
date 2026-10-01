package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r160KalshiFill(t *testing.T, raw string) kalshi.Fill {
	t.Helper()
	var fill kalshi.Fill
	if err := json.Unmarshal([]byte(raw), &fill); err != nil {
		t.Fatal(err)
	}
	return fill
}

func r160KalshiOrder(t *testing.T, raw string) kalshi.Order {
	t.Helper()
	var order kalshi.Order
	if err := json.Unmarshal([]byte(raw), &order); err != nil {
		t.Fatal(err)
	}
	return order
}

func r160AckRiskTestReservationAt(t *testing.T, st *storage.Store, created time.Time,
	id string) storage.LivePendingRiskIntent {
	t.Helper()
	in := storage.LivePendingRiskIntent{
		ReservationID: id, Created: created, BaselineObserved: created.Add(-time.Second),
		Product: "single", DispatchSource: "test", SystemID: "spotlag", Route: "taker",
		PrincipalUSD: 1, FeeUSD: .03, CostUSD: 1.03, RequestHash: strings.Repeat("c", 64),
		ProofJSON: `{}`,
		BaselineReceiptJSON: `{"baseline_clock":"venue-account-watermark-v1",` +
			`"baseline_target_position_found":true}`,
	}
	inserted, err := st.InsertLivePendingRisk(context.Background(), in,
		[]storage.LivePendingRiskLeg{{
			Index: 0, Venue: "kalshi", Ticker: "KXTEST", Side: "YES", Action: "BUY",
			ClientOrderID: "client-1", Quantity: 2, LimitPrice: .50, ExpectedPositionQty: 2,
		}},
		[]storage.LivePendingRiskCluster{{
			Index: 0, Venue: "kalshi", ClusterKey: "event:test",
			MappingVersion: r148LiveRiskMappingVersion, ReservedUSD: 1.03,
		}})
	if err != nil || !inserted {
		t.Fatalf("insert=%v err=%v", inserted, err)
	}
	leg := 0
	for _, event := range []storage.LivePendingRiskEvent{
		{ReservationID: id, Observed: created.Add(time.Millisecond),
			EventType: storage.LivePendingRiskSubmitStarted, LegIndex: &leg, AttemptKey: "entry",
			Action: "BUY", ClientOrderID: "client-1", ReceiptSource: "test", Reason: "submit",
			EvidenceJSON: `{}`},
		{ReservationID: id, Observed: created.Add(2 * time.Millisecond),
			EventType: storage.LivePendingRiskAck, LegIndex: &leg, AttemptKey: "entry",
			Action: "BUY", ClientOrderID: "client-1", OrderID: "order-1",
			ReceiptSource: "test", Reason: "ack", EvidenceJSON: `{}`},
	} {
		if ok, appendErr := st.AppendLivePendingRiskEvent(context.Background(), event); appendErr != nil || !ok {
			t.Fatalf("append %s=%v err=%v", event.EventType, ok, appendErr)
		}
	}
	return in
}

func TestR160StableKalshiOrderFillConvergenceIsTransientOnly(t *testing.T) {
	leg := storage.LivePendingRiskLeg{Index: 0, Venue: "kalshi", Ticker: "KXTEST",
		Side: "YES", Action: "BUY", Quantity: 2, LimitPrice: .50}
	identity := r148RiskAttemptIdentity{AttemptKey: "entry", LegIndex: 0, Action: "BUY",
		ClientOrderID: "client-1", OrderID: "order-1", Acked: true}
	partial := r160KalshiFill(t, `{"fill_id":"fill-1","order_id":"order-1","ticker":"KXTEST",`+
		`"outcome_side":"yes","book_side":"bid","count_fp":"1","yes_price_dollars":"0.49",`+
		`"no_price_dollars":"0.51","fee_cost":"0.01","is_taker":true}`)
	wrong := partial
	wrong.Ticker = "KXOTHER"

	for name, fills := range map[string][]kalshi.Fill{
		"no scoped fills yet":  nil,
		"partial scoped fills": {partial},
	} {
		t.Run(name, func(t *testing.T) {
			receipt, err := r151SingleKalshiAckFillReceipt(fills, leg, identity, "taker")
			if !errors.Is(err, errKalshiScopedFillsConverging) ||
				receipt.State != r148ReceiptAmbiguous || receipt.Authoritative {
				t.Fatalf("normal convergence receipt=%+v err=%v", receipt, err)
			}
		})
	}
	if _, err := r151SingleKalshiAckFillReceipt(
		[]kalshi.Fill{wrong}, leg, identity, "taker"); err == nil ||
		errors.Is(err, errKalshiScopedFillsConverging) {
		t.Fatalf("true fill contradiction was treated as endpoint lag: %v", err)
	}

	orderAhead := r160KalshiOrder(t, `{"order_id":"order-1","client_order_id":"client-1",`+
		`"ticker":"KXTEST","status":"executed","type":"limit","side":"yes","action":"buy",`+
		`"outcome_side":"yes","book_side":"bid","yes_price_dollars":"0.50",`+
		`"no_price_dollars":"0.50","fill_count_fp":"2","remaining_count_fp":"0",`+
		`"initial_count_fp":"2"}`)
	if _, err := r148SingleKalshiReceipt(orderAhead, nil, leg, identity); !errors.Is(err, errKalshiScopedFillsConverging) {
		t.Fatalf("order-ahead-of-fills was not retryable convergence: %v", err)
	}
}

func TestR160KalshiEndpointConvergenceDoesNotSetPendingRiskPause(t *testing.T) {
	for _, pointLookupMissing := range []bool{true, false} {
		name := "order-view-ahead"
		if pointLookupMissing {
			name = "point-lookup-missing"
		}
		t.Run(name, func(t *testing.T) {
			s, st := r148RiskTestServer(t)
			in := r160AckRiskTestReservationAt(t, st, time.Now().UTC(), "risk-converging")
			var fillsReady atomic.Bool
			positionUpdated := time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano)
			venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/portfolio/orders/order-1":
					if pointLookupMissing {
						w.WriteHeader(http.StatusNotFound)
						_, _ = w.Write([]byte(`{"error":{"code":"not_found","message":"not found"}}`))
						return
					}
					_, _ = w.Write([]byte(`{"order":{"order_id":"order-1",` +
						`"client_order_id":"client-1","ticker":"KXTEST","status":"executed",` +
						`"type":"limit","side":"yes","action":"buy","outcome_side":"yes",` +
						`"book_side":"bid","yes_price_dollars":"0.50","no_price_dollars":"0.50",` +
						`"fill_count_fp":"2","remaining_count_fp":"0","initial_count_fp":"2"}}`))
				case "/portfolio/fills":
					if !fillsReady.Load() {
						_, _ = w.Write([]byte(`{"fills":[],"cursor":""}`))
						return
					}
					_, _ = w.Write([]byte(`{"fills":[{"fill_id":"fill-1","order_id":"order-1",` +
						`"ticker":"KXTEST","outcome_side":"yes","book_side":"bid","count_fp":"2",` +
						`"yes_price_dollars":"0.49","no_price_dollars":"0.51","fee_cost":"0.02",` +
						`"is_taker":true}],"cursor":""}`))
				case "/portfolio/positions":
					_, _ = w.Write([]byte(`{"market_positions":[{"ticker":"KXTEST",` +
						`"position_fp":"2","last_updated_ts":"` + positionUpdated + `"}],"cursor":""}`))
				default:
					http.NotFound(w, r)
				}
			}))
			defer venue.Close()
			s.kal = kalshi.NewClient(venue.URL, queueTestSigner(t), 10000, 2*time.Second)

			s.reconcileR148SinglePendingRisks(context.Background())
			row, found, err := st.LivePendingRiskByID(context.Background(), in.ReservationID)
			if err != nil || !found || r148RiskHasEvent(row,
				storage.LivePendingRiskAmbiguous, storage.LivePendingRiskFrozen,
				storage.LivePendingRiskReleased) {
				t.Fatalf("normal convergence became ambiguity: found=%v err=%v events=%+v",
					found, err, row.Events)
			}
			if reason := s.liveAutoSafetyPauseReason(); reason != "" {
				t.Fatalf("normal endpoint lag globally paused LIVE: %q", reason)
			}

			fillsReady.Store(true)
			s.reconcileR148SinglePendingRisks(context.Background())
			active, err := st.ActiveLivePendingRisk(context.Background())
			if err != nil || len(active) != 0 {
				t.Fatalf("converged exact fill did not release reservation: active=%d err=%v",
					len(active), err)
			}
		})
	}
}

func TestR160KalshiConvergenceGraceExpiresAndContradictionsStillPause(t *testing.T) {
	for _, tc := range []struct {
		name    string
		created time.Time
		fills   string
	}{
		{
			name:    "overdue incomplete scoped fills",
			created: time.Now().UTC().Add(-kalshiScopedFillConvergenceGrace - time.Second),
			fills:   `{"fills":[],"cursor":""}`,
		},
		{
			name:    "current wrong-ticker fill",
			created: time.Now().UTC(),
			fills: `{"fills":[{"fill_id":"fill-1","order_id":"order-1","ticker":"KXOTHER",` +
				`"outcome_side":"yes","book_side":"bid","count_fp":"2",` +
				`"yes_price_dollars":"0.49","no_price_dollars":"0.51",` +
				`"fee_cost":"0.02","is_taker":true}],"cursor":""}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, st := r148RiskTestServer(t)
			in := r160AckRiskTestReservationAt(t, st, tc.created, "risk-must-pause")
			venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/portfolio/orders/order-1":
					w.WriteHeader(http.StatusNotFound)
					_, _ = w.Write([]byte(`{"error":{"code":"not_found","message":"not found"}}`))
				case "/portfolio/fills":
					_, _ = w.Write([]byte(tc.fills))
				default:
					http.NotFound(w, r)
				}
			}))
			defer venue.Close()
			s.kal = kalshi.NewClient(venue.URL, queueTestSigner(t), 10000, 2*time.Second)

			s.reconcileR148SinglePendingRisks(context.Background())
			row, found, err := st.LivePendingRiskByID(context.Background(), in.ReservationID)
			if err != nil || !found || !r148RiskHasEvent(row, storage.LivePendingRiskAmbiguous) {
				t.Fatalf("unbounded/contradictory receipt did not become ambiguous: found=%v err=%v events=%+v",
					found, err, row.Events)
			}
			if reason := s.liveAutoSafetyPauseReason(); !strings.Contains(reason, "pending-risk") {
				t.Fatalf("unbounded/contradictory receipt did not pause new money: %q", reason)
			}
		})
	}
}

func TestR160KalshiPreReservationGatesPrecedeDurableRiskWrite(t *testing.T) {
	raw, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	start := strings.Index(src, "func (s *Server) handleLivePlace")
	if start < 0 {
		t.Fatal("handleLivePlace source missing")
	}
	block := src[start:]
	if end := strings.Index(block, "\nfunc "); end >= 0 {
		block = block[:end]
	}
	reserveAt := strings.Index(block, "riskID, riskErr = s.r148ReserveSingleRisk")
	if reserveAt < 0 {
		t.Fatal("durable single-risk reservation missing")
	}
	for _, marker := range []string{
		`"pre-reservation-live-boundary"`,
		`"pre-reservation-signal-deadline"`,
		`"pre-reservation-auto-generation"`,
	} {
		if at := strings.Index(block, marker); at < 0 || at > reserveAt {
			t.Fatalf("%s is not checked before durable reservation: marker=%d reserve=%d",
				marker, at, reserveAt)
		}
	}
	if strings.Count(block, "s.liveWriteBoundaryReason(body.Auto)") < 2 ||
		strings.Count(block, "r159LiveSubmitBoundaryReason(") < 2 ||
		strings.Count(block, "s.liveIntentGenerationGateReason(body.Auto,") < 2 {
		t.Fatal("pre-reservation optimization replaced a final race-closing gate")
	}
}
