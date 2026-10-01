package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR167SingleRiskOpportunityRequirementAndHash(t *testing.T) {
	base := r148SingleRiskRequest{
		Product: "single", Venue: "kalshi", Ticker: "KXR167-HASH", Side: "YES",
		Action: "BUY", Route: "taker", DispatchSource: "r167-test", SystemID: "spotlag",
		Quantity: 1, LimitPrice: .40, PrincipalUSD: .40, FeeUSD: .01,
		ClientOrderID: "r167-client", AttemptNonce: "r167-client",
		RequireExecutionShadowAttemptID: true, Proof: map[string]any{"auto": true},
	}
	if _, _, _, err := r148SingleRiskRequestIdentity(base); err == nil {
		t.Fatal("automatic/system cash identity accepted no canonical opportunity id")
	}
	base.ExecutionShadowAttemptID = "exec-r167-a"
	idA, proofA, hashA, err := r148SingleRiskRequestIdentity(base)
	if err != nil || idA != base.ExecutionShadowAttemptID || len(hashA) != 64 {
		t.Fatalf("identity id=%q hash=%q err=%v", idA, hashA, err)
	}
	var proof map[string]any
	if err := json.Unmarshal([]byte(proofA), &proof); err != nil ||
		proof["execution_shadow_attempt_id"] != base.ExecutionShadowAttemptID {
		t.Fatalf("proof=%s err=%v", proofA, err)
	}
	base.ExecutionShadowAttemptID = "exec-r167-b"
	_, _, hashB, err := r148SingleRiskRequestIdentity(base)
	if err != nil || hashA == hashB {
		t.Fatalf("opportunity id omitted from request hash: a=%s b=%s err=%v", hashA, hashB, err)
	}
	base.RequireExecutionShadowAttemptID = false
	base.ExecutionShadowAttemptID = ""
	if _, _, _, err := r148SingleRiskRequestIdentity(base); err != nil {
		t.Fatalf("explicit manual compatibility was removed: %v", err)
	}
}

func TestR167RestartRejoinsPendingRiskToExactAttemptForBothVenues(t *testing.T) {
	for _, venue := range []string{"kalshi", "polyus"} {
		t.Run(venue, func(t *testing.T) {
			s, st := newExecutionShadowTestServer(t)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Microsecond)
			attemptID, reservationID := "exec-r167-restart-"+venue, "risk-r167-restart-"+venue
			ticker := "R167-RESTART-" + strings.ToUpper(venue)
			if inserted, err := st.InsertExecutionShadowAttempt(ctx, storage.ExecutionShadowAttempt{
				AttemptID: attemptID, SignalDecisionID: "decision-" + attemptID,
				ObservedAt: now, TriggerUnixMS: now.UnixMilli(), Venue: venue, Ticker: ticker,
				Side: "YES", Action: "BUY", SystemID: "spotlag", Route: "taker",
				SignalSource: "r167-test", QualificationBasis: "restart-lineage-test",
			}); err != nil || !inserted {
				t.Fatalf("attempt inserted=%v err=%v", inserted, err)
			}
			clientID := ""
			if venue == "kalshi" {
				clientID = "r167-client"
			}
			intent := storage.LivePendingRiskIntent{
				ReservationID: reservationID, Created: now.Add(time.Millisecond),
				BaselineObserved: now, Product: "single", DispatchSource: "r167-restart",
				SystemID: "spotlag", Route: "taker", PrincipalUSD: .40, FeeUSD: .01, CostUSD: .41,
				ExecutionShadowAttemptID: attemptID,
				RequestHash:              strings.Repeat(map[string]string{"kalshi": "e", "polyus": "f"}[venue], 64),
				ProofJSON:                `{}`, BaselineReceiptJSON: `{}`,
			}
			legs := []storage.LivePendingRiskLeg{{Index: 0, Venue: venue, Ticker: ticker,
				Side: "YES", Action: "BUY", ClientOrderID: clientID, Quantity: 1, LimitPrice: .40,
				BaselinePositionQty: 0, ExpectedPositionQty: 1}}
			clusters := []storage.LivePendingRiskCluster{{Index: 0, Venue: venue,
				ClusterKey: "r167:" + venue, MappingVersion: "canonical-v1", ReservedUSD: .41}}
			if inserted, err := st.InsertLivePendingRisk(ctx, intent, legs, clusters); err != nil || !inserted {
				t.Fatalf("risk inserted=%v err=%v", inserted, err)
			}
			leg := 0
			appendRisk := func(event storage.LivePendingRiskEvent) {
				t.Helper()
				if inserted, err := st.AppendLivePendingRiskEvent(ctx, event); err != nil || !inserted {
					t.Fatalf("risk event %s inserted=%v err=%v", event.EventType, inserted, err)
				}
			}
			submit := storage.LivePendingRiskEvent{ReservationID: reservationID,
				Observed: now.Add(2 * time.Millisecond), EventType: storage.LivePendingRiskSubmitStarted,
				LegIndex: &leg, AttemptKey: "entry", Action: "BUY", ClientOrderID: clientID,
				ReceiptSource: venue + "-submit", Reason: "POST begins", EvidenceJSON: `{}`}
			appendRisk(submit)
			ack := submit
			ack.Observed, ack.EventType, ack.OrderID = now.Add(3*time.Millisecond), storage.LivePendingRiskAck, "order-r167-"+venue
			ack.ReceiptSource, ack.Reason = venue+"-ack", "exact order acknowledged"
			appendRisk(ack)
			terminal := ack
			terminal.Observed, terminal.EventType = now.Add(4*time.Millisecond), storage.LivePendingRiskTerminalUnfilled
			terminal.ReceiptSource, terminal.Reason = venue+"-result", "terminal zero fill"
			appendRisk(terminal)

			// A new process has no in-memory candidate/reservation map. Recovery must use only the
			// normalized immutable opportunity ID in the pending-risk row.
			s.reconcileExecutionShadowLiveReceipts(ctx)
			views, err := st.ListExecutionShadowAttempts(ctx, 10)
			if err != nil || len(views) != 1 {
				t.Fatalf("views=%+v err=%v", views, err)
			}
			stages := map[string]storage.ExecutionShadowEvent{}
			for _, event := range views[0].Events {
				stages[event.Stage] = event
			}
			for _, stage := range []string{"live-risk-reservation", "live-risk-submit", "live-exact-reconcile"} {
				event, ok := stages[stage]
				if !ok || event.LiveReservationID != reservationID ||
					event.Evidence["execution_shadow_attempt_id"] != attemptID {
					t.Fatalf("stage %s did not exact-join: %+v", stage, event)
				}
			}
			if event := stages["live-exact-reconcile"]; !event.LiveAuthoritative ||
				event.LiveState != "terminal_unfilled" || event.LiveFee == nil || *event.LiveFee != 0 {
				t.Fatalf("wrong recovered terminal: %+v", event)
			}
		})
	}
}
