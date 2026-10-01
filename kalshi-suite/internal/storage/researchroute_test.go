package storage

import (
	"context"
	"testing"
	"time"
)

func routeFixture(id, routeID, decision string) ResearchRouteOpportunity {
	low := -0.02
	action, side, route, ticker, qty, price, depth := "reject", "YES", "taker", "KXTEST", 0.0, 0.61, 12.0
	reason := "fees erase edge"
	if decision == "candidate" {
		low, action, qty, reason = -0.62, "buy", 1, "preregistered executable action; research only"
	}
	if decision == "control" {
		action, side, route, ticker, qty, price, depth, reason = "control", "NONE", "control", "", 0, 0, 0, "matched no-order control"
	}
	return ResearchRouteOpportunity{
		OpportunityID: id, RouteID: routeID, Observed: time.Now().UTC(), SystemName: "proper-score-executor",
		IdentityStatus: "verified", Venue: "kalshi", Ticker: ticker, Side: side, Route: route,
		Action: action, QuoteSource: "kalshi_ws_orderbook", QuoteAgeSeconds: .2, DecisionLatencyMS: 12,
		QuoteAgeKnown: true, LatencyKnown: true, TickSize: .01, TickKnown: true,
		ExecutablePrice: price, ExecutableDepth: depth, DepthKnown: true, RequestedQty: qty,
		FeeAmount: .01, FeeAuthority: "kalshi-series-fee-v2", FeeKnown: true, ExpectedPayoutLow: 0,
		ExpectedPayoutHigh: 1, ExpectedNetLow: low, ExpectedNetHigh: .05,
		Decision: decision, DecisionReason: reason, EvidenceJSON: `{"book_complete":true}`,
	}
}

func TestResearchRouteLedgerRetainsNegativeControlAndLifecycle(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	for _, row := range []ResearchRouteOpportunity{
		routeFixture("opp-1", "taker", "candidate"),
		routeFixture("opp-1", "maker", "blocked"),
		routeFixture("opp-1", "no-order", "control"),
	} {
		inserted, err := st.InsertResearchRouteOpportunity(ctx, row)
		if err != nil || !inserted {
			t.Fatalf("insert=%v err=%v row=%+v", inserted, err, row)
		}
	}
	if inserted, err := st.InsertResearchRouteOpportunity(ctx, routeFixture("opp-1", "taker", "candidate")); err != nil || inserted {
		t.Fatalf("immutable duplicate inserted=%v err=%v", inserted, err)
	}
	queue := 4.0
	if _, err := st.AppendResearchRouteEvent(ctx, ResearchRouteEvent{
		OpportunityID: "opp-1", RouteID: "maker", EventType: "queue", QueueAhead: &queue,
		OutcomeStatus: "resting", EvidenceJSON: `{"source":"native_queue"}`,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendResearchRouteEvent(ctx, ResearchRouteEvent{
		OpportunityID: "opp-1", RouteID: "maker", EventType: "cancel", Reason: "price moved one native tick",
	}); err != nil {
		t.Fatal(err)
	}
	report, err := st.ResearchRouteReport(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	counts := report["counts"].(map[string]int)
	if counts["routes"] != 3 || counts["candidates"] != 1 || counts["blocked"] != 1 ||
		counts["controls"] != 1 || counts["lifecycle_events"] != 2 || counts["cancel_events"] != 1 {
		t.Fatalf("report=%+v", report)
	}
}

func TestResearchRouteCandidateAllowsCensorSafeFloorButRequiresExecutableMoneyTruth(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	row := routeFixture("bad", "taker", "candidate")
	if inserted, err := st.InsertResearchRouteOpportunity(context.Background(), row); err != nil || !inserted {
		t.Fatalf("censor-safe candidate rejected inserted=%v err=%v", inserted, err)
	}
	row = routeFixture("bad2", "taker", "candidate")
	row.ExecutableDepth = 0
	if inserted, err := st.InsertResearchRouteOpportunity(context.Background(), row); err == nil || inserted {
		t.Fatalf("depth-free candidate accepted inserted=%v err=%v", inserted, err)
	}
	row = routeFixture("bad3", "taker", "candidate")
	row.LatencyKnown = false
	if inserted, err := st.InsertResearchRouteOpportunity(context.Background(), row); err == nil || inserted {
		t.Fatalf("latency-unknown candidate accepted inserted=%v err=%v", inserted, err)
	}
}

func TestResearchRouteAppendOnlyTriggers(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if _, err := st.InsertResearchRouteOpportunity(ctx, routeFixture("opp", "taker", "candidate")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE research_route_opportunities SET executable_price=.5 WHERE opportunity_id='opp'`); err == nil {
		t.Fatal("immutable opportunity update succeeded")
	}
	if _, err := st.AppendResearchRouteEvent(ctx, ResearchRouteEvent{OpportunityID: "opp", RouteID: "taker", EventType: "intent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `DELETE FROM research_route_events`); err == nil {
		t.Fatal("append-only event delete succeeded")
	}
}
