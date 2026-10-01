package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR149TransientStagedRecoveryTimeoutPausesRetriesAndNeverTripsKill(t *testing.T) {
	s := testServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.recoverR148StagedBundleExecutions(ctx)
	if s.ksBlocked() {
		t.Fatalf("clean transient recovery timeout tripped kill: %+v", s.ks.State())
	}
	if reason := s.r148StagedRecoveryPauseReason(); reason == "" {
		t.Fatal("transient recovery timeout did not pause new dispatch")
	}

	// The normal seven-second monitor retry uses a fresh context. A clean read must clear the
	// pause automatically; operator ARM/AUTO state and the kill switch remain untouched.
	s.recoverR148StagedBundleExecutions(context.Background())
	if s.ksBlocked() || s.r148StagedRecoveryPauseReason() != "" {
		t.Fatalf("clean retry did not resume: kill=%v reason=%q", s.ksBlocked(), s.r148StagedRecoveryPauseReason())
	}
}

func TestR149TransientStagedRecoveryTimeoutPausesEvenWhenArmed(t *testing.T) {
	s := testServer(t)
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	broker := newR148ServerStagedBroker(s)
	s.r148StagedRecoveryReadFailure(context.Background(), broker, "active-risk",
		context.DeadlineExceeded, true)
	if s.ksBlocked() {
		t.Fatalf("transient timeout permanently tripped armed session: %+v", s.ks.State())
	}
	if reason := s.r148StagedRecoveryPauseReason(); reason == "" {
		t.Fatal("armed transient timeout did not pause dispatch")
	}
}

func TestR151eAffirmativeStagedCorruptionPausesWithoutOperatorKill(t *testing.T) {
	s := testServer(t)
	broker := newR148ServerStagedBroker(s)
	s.r148StagedRecoveryReadFailure(context.Background(), broker, "active-risk",
		errors.New("stored staged reservation identity is invalid"), true)
	if s.ksBlocked() {
		t.Fatal("automatic staged corruption must not trip the operator-only kill switch")
	}
	if reason := s.liveAutoSafetyPauseReason(); !strings.Contains(reason, "staged-risk") {
		t.Fatalf("affirmative staged corruption did not pause dispatch: %q", reason)
	}
}

func TestR148RecoveryCannotStarveNewestOrphanBehindTwentyHarmlessIntents(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	for i := 0; i < 21; i++ {
		bundle := r148CoordinatorBundle()
		bundle.BundleID, bundle.OpportunityID = fmt.Sprintf("runtime-backlog-bundle-%02d", i), fmt.Sprintf("runtime-backlog-opp-%02d", i)
		if inserted, err := s.store.InsertResearchRouteBundle(ctx, bundle); err != nil || !inserted {
			t.Fatalf("bundle %d inserted=%v err=%v", i, inserted, err)
		}
		order := []int{0, 1}
		executionID := fmt.Sprintf("runtime-backlog-exec-%02d", i)
		intent := storage.StagedBundleExecutionIntent{ExecutionID: executionID, BundleID: bundle.BundleID,
			SystemID: bundle.SystemID, BundleHash: r148BundleIdentityHash(bundle), OrderedLegsHash: r148Hash(order),
			RoutePolicy: "staged_fok_v1", Created: time.Now().UTC().Add(time.Duration(i) * time.Second),
			Proof: map[string]any{"sealed": true}, OrderedLegs: order, RequestedSize: bundle.Size,
			MaxAllInUnit: .82, FirstLegIndex: 0}
		if inserted, err := s.store.InsertStagedBundleExecutionIntent(ctx, intent); err != nil || !inserted {
			t.Fatalf("intent %d inserted=%v err=%v", i, inserted, err)
		}
		if _, err := s.store.AppendStagedBundleExecutionEvent(ctx, storage.StagedBundleExecutionEvent{
			ExecutionID: executionID, EventType: "admitted", Observed: time.Now().UTC(), EvidenceJSON: `{}`}); err != nil {
			t.Fatal(err)
		}
		if i == 20 {
			leg := bundle.Legs[0]
			if _, err := s.store.AppendStagedBundleExecutionEvent(ctx, storage.StagedBundleExecutionEvent{
				ExecutionID: executionID, EventType: "leg_intent", Observed: time.Now().UTC(), LegIndex: &leg.Index,
				Venue: leg.Venue, Ticker: leg.Ticker, Side: leg.Side, Action: "BUY", RequestedQty: leg.Quantity,
				EvidenceJSON: `{}`}); err != nil {
				t.Fatal(err)
			}
		}
	}
	s.recoverR148StagedBundleExecutions(ctx)
	events, err := s.store.StagedBundleExecutionEvents(ctx, "runtime-backlog-exec-20")
	if err != nil || events[len(events)-1].EventType != "frozen" || s.ksBlocked() ||
		!strings.Contains(s.liveAutoSafetyPauseReason(), "staged-risk") {
		t.Fatalf("orphan events=%+v kill=%v pause=%q err=%v", events, s.ksBlocked(), s.liveAutoSafetyPauseReason(), err)
	}
}

func TestR148RecoveryClosesAuthoritativeFlatNoFillWhileBeltOff(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	bundle := r148CoordinatorBundle()
	bundle.BundleID = "runtime-flat-bundle"
	if inserted, err := s.store.InsertResearchRouteBundle(ctx, bundle); err != nil || !inserted {
		t.Fatalf("bundle inserted=%v err=%v", inserted, err)
	}
	order := []int{0, 1}
	executionID := "runtime-flat-execution"
	intent := storage.StagedBundleExecutionIntent{ExecutionID: executionID, BundleID: bundle.BundleID,
		SystemID: bundle.SystemID, BundleHash: r148BundleIdentityHash(bundle), OrderedLegsHash: r148Hash(order),
		RoutePolicy: "staged_fok_v1", Created: time.Now().UTC(), Proof: map[string]any{"sealed": true},
		OrderedLegs: order, RequestedSize: bundle.Size, MaxAllInUnit: .82, FirstLegIndex: 0}
	if inserted, err := s.store.InsertStagedBundleExecutionIntent(ctx, intent); err != nil || !inserted {
		t.Fatalf("intent inserted=%v err=%v", inserted, err)
	}
	if _, err := s.store.AppendStagedBundleExecutionEvent(ctx, storage.StagedBundleExecutionEvent{
		ExecutionID: executionID, EventType: "admitted", Observed: time.Now().UTC(), EvidenceJSON: `{}`}); err != nil {
		t.Fatal(err)
	}
	leg := bundle.Legs[0]
	if _, err := s.store.AppendStagedBundleExecutionEvent(ctx, storage.StagedBundleExecutionEvent{
		ExecutionID: executionID, EventType: "leg_unfilled", Observed: time.Now().UTC(), LegIndex: &leg.Index,
		Venue: leg.Venue, Ticker: leg.Ticker, Side: leg.Side, Action: "BUY", RequestedQty: leg.Quantity,
		OrderID: "flat-fok", ReceiptSource: "kalshi-get-order+order-scoped-fills", Reason: "terminal FOK no-fill",
		EvidenceJSON: `{}`}); err != nil {
		t.Fatal(err)
	}
	s.recoverR148StagedBundleExecutions(ctx)
	events, err := s.store.StagedBundleExecutionEvents(ctx, executionID)
	if err != nil || events[len(events)-1].EventType != "rejected" || s.ksBlocked() {
		t.Fatalf("events=%+v kill=%v err=%v", events, s.ksBlocked(), err)
	}
}
