package storage

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestR154LiveAcceptedTotalsSurviveReleaseAndUseActualFill(t *testing.T) {
	st := riskTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	in, legs, clusters := riskTestSingle(now)
	if ok, err := st.InsertLivePendingRisk(ctx, in, legs, clusters); err != nil || !ok {
		t.Fatalf("insert=%v err=%v", ok, err)
	}
	leg := 0
	submit := riskEvent(in, now.Add(time.Second), LivePendingRiskSubmitStarted, leg)
	if ok, err := st.AppendLivePendingRiskEvent(ctx, submit); err != nil || !ok {
		t.Fatalf("submit=%v err=%v", ok, err)
	}
	ack := riskEvent(in, now.Add(2*time.Second), LivePendingRiskAck, leg)
	ack.OrderID = "order-1"
	if ok, err := st.AppendLivePendingRiskEvent(ctx, ack); err != nil || !ok {
		t.Fatalf("ack=%v err=%v", ok, err)
	}
	visible := riskEvent(in, now.Add(3*time.Second), LivePendingRiskAccountVisible, leg)
	visible.OrderID, visible.AccountObserved = "order-1", now.Add(3*time.Second)
	visible.FilledQty, visible.AveragePrice, visible.FeeTotal = 1.5, .61, .04
	if ok, err := st.AppendLivePendingRiskEvent(ctx, visible); err != nil || !ok {
		t.Fatalf("visible=%v err=%v", ok, err)
	}
	release := LivePendingRiskEvent{ReservationID: in.ReservationID, Observed: now.Add(4 * time.Second),
		EventType: LivePendingRiskReleased, ReceiptSource: "test", Reason: "settled", EvidenceJSON: `{}`}
	if ok, err := st.AppendLivePendingRiskEvent(ctx, release); err != nil || !ok {
		t.Fatalf("release=%v err=%v", ok, err)
	}
	got, err := st.LiveAcceptedTotalsSince(ctx, now.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got.Orders != 1 || got.Contracts != 1.5 || math.Abs(got.PrincipalUSD-.915) > 1e-9 ||
		math.Abs(got.FeeUSD-.04) > 1e-9 || math.Abs(got.TotalCostUSD-.955) > 1e-9 {
		t.Fatalf("actual accepted totals drifted: %+v", got)
	}
}
