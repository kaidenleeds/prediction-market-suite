package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestR148PendingStagedRecoveryIsRiskFirstAndUnboundedAtStartup(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	for i := 0; i < 25; i++ {
		b := bundleFixture()
		b.BundleID, b.OpportunityID, b.SystemID = fmt.Sprintf("backlog-bundle-%02d", i), fmt.Sprintf("backlog-opp-%02d", i), "event-basket-lock"
		if inserted, insertErr := st.InsertResearchRouteBundle(ctx, b); insertErr != nil || !inserted {
			t.Fatalf("bundle %d inserted=%v err=%v", i, inserted, insertErr)
		}
		execID := fmt.Sprintf("backlog-exec-%02d", i)
		intent := StagedBundleExecutionIntent{ExecutionID: execID, BundleID: b.BundleID, SystemID: b.SystemID,
			BundleHash: strings.Repeat("a", 64), OrderedLegsHash: strings.Repeat("b", 64), RoutePolicy: "staged_fok_v1",
			Created: time.Now().UTC().Add(time.Duration(i) * time.Second), Proof: map[string]any{"sealed": true},
			OrderedLegs: []int{0, 1}, RequestedSize: 1, MaxAllInUnit: .82, FirstLegIndex: 0}
		if inserted, insertErr := st.InsertStagedBundleExecutionIntent(ctx, intent); insertErr != nil || !inserted {
			t.Fatalf("intent %d inserted=%v err=%v", i, inserted, insertErr)
		}
		if _, appendErr := st.AppendStagedBundleExecutionEvent(ctx, StagedBundleExecutionEvent{
			ExecutionID: execID, EventType: "admitted", Observed: time.Now().UTC(), EvidenceJSON: `{}`}); appendErr != nil {
			t.Fatal(appendErr)
		}
		if i == 24 {
			idx := 0
			if _, appendErr := st.AppendStagedBundleExecutionEvent(ctx, StagedBundleExecutionEvent{ExecutionID: execID,
				EventType: "leg_intent", Observed: time.Now().UTC(), LegIndex: &idx, Venue: "kalshi", Ticker: "KXA",
				Side: "YES", Action: "BUY", RequestedQty: 1, EvidenceJSON: `{}`}); appendErr != nil {
				t.Fatal(appendErr)
			}
		}
	}
	limited, err := st.PendingStagedBundleExecutionIDs(ctx, 20)
	if err != nil || len(limited) != 20 || limited[0] != "backlog-exec-24" {
		t.Fatalf("risk-first limited=%v err=%v", limited, err)
	}
	all, err := st.AllPendingStagedBundleExecutionIDs(ctx)
	if err != nil || len(all) != 25 || all[0] != "backlog-exec-24" {
		t.Fatalf("complete risk-first count=%d first=%v err=%v", len(all), all, err)
	}
	if present, err := st.HasPendingStagedBundleExecutions(ctx); err != nil || !present {
		t.Fatalf("pending presence=%v err=%v", present, err)
	}
}

func TestR148StagedBundleJournalIsSequentialImmutableAndRestartVisible(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	b := bundleFixture()
	b.BundleID, b.OpportunityID, b.SystemID = "stage-bundle", "stage-opp", "event-basket-lock"
	if inserted, insertErr := st.InsertResearchRouteBundle(ctx, b); insertErr != nil || !inserted {
		t.Fatalf("insert bundle=%v err=%v", inserted, insertErr)
	}
	intent := StagedBundleExecutionIntent{ExecutionID: "stage-exec", BundleID: b.BundleID,
		SystemID: b.SystemID, BundleHash: strings.Repeat("a", 64), OrderedLegsHash: strings.Repeat("b", 64),
		RoutePolicy: "staged_fok_v1", Created: time.Now().UTC(), Proof: map[string]any{"sealed": true},
		OrderedLegs: []int{0, 1}, RequestedSize: 1, MaxAllInUnit: .82, FirstLegIndex: 0}
	if inserted, insertErr := st.InsertStagedBundleExecutionIntent(ctx, intent); insertErr != nil || !inserted {
		t.Fatalf("insert intent=%v err=%v", inserted, insertErr)
	}
	byBundle, found, readErr := st.StagedBundleExecutionIntentByBundleID(ctx, b.BundleID)
	if readErr != nil || !found || byBundle.ExecutionID != intent.ExecutionID ||
		byBundle.BundleHash != intent.BundleHash {
		t.Fatalf("intent by durable bundle identity=%+v found=%v err=%v", byBundle, found, readErr)
	}
	idx := 0
	for _, typ := range []string{"admitted", "leg_intent", "leg_submitted"} {
		e := StagedBundleExecutionEvent{ExecutionID: intent.ExecutionID, EventType: typ,
			Observed: time.Now().UTC(), EvidenceJSON: `{}`}
		if typ != "admitted" {
			e.LegIndex, e.Venue, e.Ticker, e.Side, e.Action, e.RequestedQty = &idx, "kalshi", "KXA", "YES", "BUY", 1
		}
		if typ == "leg_submitted" {
			e.OrderID = "order-1"
		}
		got, appendErr := st.AppendStagedBundleExecutionEvent(ctx, e)
		if appendErr != nil || got.Sequence == 0 {
			t.Fatalf("append %s=%+v err=%v", typ, got, appendErr)
		}
	}
	events, err := st.StagedBundleExecutionEvents(ctx, intent.ExecutionID)
	if err != nil || len(events) != 3 || events[0].Sequence != 1 || events[2].Sequence != 3 {
		t.Fatalf("events=%+v err=%v", events, err)
	}
	pending, err := st.PendingStagedBundleExecutionIDs(ctx, 10)
	if err != nil || len(pending) != 1 || pending[0] != intent.ExecutionID {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	if _, err := st.db.Exec(`UPDATE staged_bundle_execution_intents SET requested_size=2`); err == nil {
		t.Fatal("immutable intent updated")
	}
	if _, err := st.db.Exec(`DELETE FROM staged_bundle_execution_events`); err == nil {
		t.Fatal("append-only events deleted")
	}
	if _, err := st.AppendStagedBundleExecutionEvent(ctx, StagedBundleExecutionEvent{ExecutionID: intent.ExecutionID,
		EventType: "frozen", Observed: time.Now().UTC(), Reason: "ambiguous", EvidenceJSON: `{}`}); err != nil {
		t.Fatal(err)
	}
	pending, err = st.PendingStagedBundleExecutionIDs(ctx, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("terminal intent remained pending=%v err=%v", pending, err)
	}
}
