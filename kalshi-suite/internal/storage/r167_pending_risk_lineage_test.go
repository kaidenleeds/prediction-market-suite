package storage

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func r167RiskEvidenceAttemptID(t *testing.T, raw string) string {
	t.Helper()
	var evidence map[string]any
	if err := json.Unmarshal([]byte(raw), &evidence); err != nil {
		t.Fatal(err)
	}
	id, _ := evidence["execution_shadow_attempt_id"].(string)
	return id
}

func TestR167PendingRiskPersistsOpportunityAcrossBothVenues(t *testing.T) {
	for _, venue := range []string{"kalshi", "polyus"} {
		t.Run(venue, func(t *testing.T) {
			st := riskTestStore(t)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Microsecond)
			in, legs, clusters := riskTestSingle(now)
			in.ReservationID = "risk-r167-" + venue
			in.ExecutionShadowAttemptID = "exec-r167-" + venue
			in.RequestHash = strings.Repeat(map[string]string{"kalshi": "a", "polyus": "b"}[venue], 64)
			in.ProofJSON = `{"venue":"` + venue + `"}`
			legs[0].Venue, legs[0].Ticker = venue, "R167-"+strings.ToUpper(venue)
			clusters[0].Venue, clusters[0].ClusterKey = venue, "r167:"+venue
			if venue == "polyus" {
				legs[0].ClientOrderID = ""
			}
			if inserted, err := st.InsertLivePendingRisk(ctx, in, legs, clusters); err != nil || !inserted {
				t.Fatalf("inserted=%v err=%v", inserted, err)
			}

			row, found, err := st.LivePendingRiskByID(ctx, in.ReservationID)
			if err != nil || !found {
				t.Fatalf("found=%v err=%v", found, err)
			}
			if row.Intent.ExecutionShadowAttemptID != in.ExecutionShadowAttemptID ||
				r167RiskEvidenceAttemptID(t, row.Intent.ProofJSON) != in.ExecutionShadowAttemptID ||
				r167RiskEvidenceAttemptID(t, row.Events[0].EvidenceJSON) != in.ExecutionShadowAttemptID {
				t.Fatalf("reservation lost canonical opportunity: %+v events=%+v", row.Intent, row.Events)
			}

			leg := 0
			submit := LivePendingRiskEvent{ReservationID: in.ReservationID,
				Observed: now.Add(time.Second), EventType: LivePendingRiskSubmitStarted,
				LegIndex: &leg, AttemptKey: "entry", Action: "BUY",
				ClientOrderID: legs[0].ClientOrderID, ReceiptSource: venue + "-submit",
				Reason: "network mutation begins", EvidenceJSON: `{"wire":true}`}
			if inserted, err := st.AppendLivePendingRiskEvent(ctx, submit); err != nil || !inserted {
				t.Fatalf("submit inserted=%v err=%v", inserted, err)
			}
			conflict := submit
			conflict.EventType, conflict.Observed = LivePendingRiskAmbiguous, now.Add(1500*time.Millisecond)
			conflict.EvidenceJSON = `{"execution_shadow_attempt_id":"wrong-attempt"}`
			if _, err := st.AppendLivePendingRiskEvent(ctx, conflict); err == nil {
				t.Fatal("event changed immutable opportunity lineage")
			}
			clean := submit
			clean.EventType, clean.Observed = LivePendingRiskCleanRejected, now.Add(2*time.Second)
			clean.ReceiptSource, clean.Reason, clean.EvidenceJSON = venue+"-result", "venue rejected", `{}`
			if inserted, err := st.AppendLivePendingRiskEvent(ctx, clean); err != nil || !inserted {
				t.Fatalf("result inserted=%v err=%v", inserted, err)
			}
			release := LivePendingRiskEvent{ReservationID: in.ReservationID,
				Observed: now.Add(3 * time.Second), EventType: LivePendingRiskReleased,
				ReceiptSource: venue + "-reconcile", Reason: "exact zero exposure",
				EvidenceJSON: `{}`}
			if inserted, err := st.AppendLivePendingRiskEvent(ctx, release); err != nil || !inserted {
				t.Fatalf("release inserted=%v err=%v", inserted, err)
			}

			row, found, err = st.LivePendingRiskByID(ctx, in.ReservationID)
			if err != nil || !found {
				t.Fatalf("reload found=%v err=%v", found, err)
			}
			for _, event := range row.Events {
				if got := r167RiskEvidenceAttemptID(t, event.EvidenceJSON); got != in.ExecutionShadowAttemptID {
					t.Fatalf("event %s lost opportunity id: %q", event.EventType, got)
				}
			}
			joined, err := st.LivePendingRisksByExecutionShadowAttempts(ctx,
				[]string{in.ExecutionShadowAttemptID})
			if err != nil || len(joined) != 1 || joined[0].Intent.ReservationID != in.ReservationID {
				t.Fatalf("exact join=%+v err=%v", joined, err)
			}

			changed := in
			changed.ReservationID += "-collision"
			changed.RequestHash = strings.Repeat(map[string]string{"kalshi": "c", "polyus": "d"}[venue], 64)
			legs[0].Ticker += "-OTHER"
			clusters[0].ClusterKey += ":other"
			if _, err := st.InsertLivePendingRisk(ctx, changed, legs, clusters); err == nil {
				t.Fatal("one canonical opportunity created two immutable money reservations")
			}
			if _, err := st.db.ExecContext(ctx, `UPDATE live_pending_risk_intents
SET execution_shadow_attempt_id='changed' WHERE reservation_id=?`, in.ReservationID); err == nil {
				t.Fatal("immutable opportunity column was updated")
			}
		})
	}
}
