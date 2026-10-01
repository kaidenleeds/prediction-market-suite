package server

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR172KalshiSubmitJournalIsTheTrueLastPrePOSTBoundary(t *testing.T) {
	raw, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	handler := string(raw)
	start := strings.Index(handler, "func (s *Server) handleLivePlace")
	end := strings.Index(handler[start+1:], "\nfunc ")
	if start < 0 || end < 0 {
		t.Fatal("handleLivePlace source block unavailable")
	}
	handler = handler[start : start+1+end]
	account := strings.Index(handler, "accountConsume := s.consumeR154KalshiAdmissionSnapshot")
	if account < 0 {
		t.Fatal("final account consume unavailable")
	}
	wire := account + strings.Index(handler[account:], "wireQuote, wireWhy := s.liveMirrorExecutable")
	authority := account + strings.Index(handler[account:], "s.r163LiveWireSystemAuthorityReason")
	policy := account + strings.Index(handler[account:], "r163LiveMoneyPolicyGenerationReason")
	deadline := account + strings.Index(handler[account:], "r159LiveSubmitBoundaryReason")
	generation := deadline + strings.Index(handler[deadline:], "s.liveIntentGenerationGateReason")
	capture := generation + strings.Index(handler[generation:], "prePOSTCapturedAt := time.Now()")
	submit := capture + strings.Index(handler[capture:], "storage.LivePendingRiskSubmitStarted")
	detach := submit + strings.Index(handler[submit:], "postCtx, cancelPost := context.WithTimeout")
	create := detach + strings.Index(handler[detach:], "s.kal.CreateOrder(postCtx, req)")
	if !(account < wire && wire < authority && authority < policy && policy < deadline &&
		deadline < generation && generation < capture && capture < submit && submit < detach &&
		detach < create) {
		t.Fatalf("pre-POST order changed: account=%d wire=%d authority=%d policy=%d deadline=%d generation=%d capture=%d submit=%d detach=%d create=%d",
			account, wire, authority, policy, deadline, generation, capture, submit, detach, create)
	}
	beforeSubmit := handler[:submit]
	if strings.LastIndex(beforeSubmit, "storage.LivePendingRiskSubmitStarted") >= 0 {
		t.Fatal("a false submit_started still precedes a known no-send gate")
	}
	journal := handler[capture:create]
	for _, field := range []string{
		`"pre_post_schema"`, `"trigger_unix_ms"`,
		`"pre_post_book"`, `"pre_post_limit_price"`, `"pre_post_requested_qty"`,
		`"pre_post_time_in_force"`, `"pre_post_fee_total"`,
		`"pre_post_fee_per_contract"`, `"pre_post_fee_source"`,
		`"execution_shadow_attempt_id"`, `"submit_journal_duration_ms"`,
		`"signal_to_post_call_ms"`, `"handler_to_post_call_ms"`,
		`"signal_to_submit_journal_ms"`, `"handler_to_submit_journal_ms"`,
		`"post_journal_book"`, `"post_journal_book_reason"`,
	} {
		if !strings.Contains(journal, field) {
			t.Fatalf("submit journal is missing %s", field)
		}
	}
	if !strings.Contains(journal, "liveKalshiPostBoundaryTimeout") ||
		!strings.Contains(journal, "context.WithoutCancel(ctx)") {
		t.Fatal("detached POST lost its explicit overall deadline")
	}
}

func TestR172RestartRecoveryKeepsTheExactPrePOSTBook(t *testing.T) {
	now := time.Now().UTC()
	row := storage.LivePendingRiskReservation{
		Intent: storage.LivePendingRiskIntent{
			ReservationID:            "risk-r172-book",
			ExecutionShadowAttemptID: "exec-r172-book",
			Created:                  now,
			DispatchSource:           "r172-test",
			SystemID:                 "spotlag",
			Route:                    "taker",
		},
		Legs: []storage.LivePendingRiskLeg{{
			Index: 0, Venue: "kalshi", Ticker: "KR172", Side: "YES", Action: "BUY",
			Quantity: 2, LimitPrice: .42,
		}},
		Events: []storage.LivePendingRiskEvent{{
			ID: 7, Sequence: 1, Observed: now.Add(time.Millisecond),
			EventType: storage.LivePendingRiskSubmitStarted, AttemptKey: "entry",
			EvidenceJSON: `{
				"pre_post_schema":"r172-prepost-v1",
				"execution_shadow_attempt_id":"untrusted-overwrite",
				"pre_post_book":{
					"price":0.42,"depth":5,"tick":0.01,"spread_cents":2,"maker":false,
					"book_source":"kalshi_ws_full_orderbook:g17:s23:q42",
					"source_at":"2026-07-25T06:00:00Z",
					"observed_at":"2026-07-25T06:00:00.010Z",
					"capture_at":"2026-07-25T06:00:00.012Z",
					"book_generation":17,"book_subscription_id":23,"book_sequence":42
				},
				"pre_post_limit_price":0.42,
				"pre_post_requested_qty":2,
				"pre_post_fee_total":0.02,
				"pre_post_fee_source":"exact-kalshi-schedule"
			}`,
		}},
	}
	events := executionShadowRiskLineageEvents(row)
	if len(events) != 2 {
		t.Fatalf("events=%+v", events)
	}
	submit := events[1]
	if submit.AttemptID != row.Intent.ExecutionShadowAttemptID ||
		submit.Evidence["execution_shadow_attempt_id"] != row.Intent.ExecutionShadowAttemptID {
		t.Fatalf("durable identity was overwritten: %+v", submit)
	}
	book, ok := submit.Evidence["pre_post_book"].(map[string]any)
	if !ok || book["book_generation"] != float64(17) ||
		book["book_subscription_id"] != float64(23) || book["book_sequence"] != float64(42) {
		t.Fatalf("exact pre-POST book was not recovered: %+v", submit.Evidence)
	}
	if submit.Evidence["pre_post_fee_total"] != .02 || submit.VenueAttempted ||
		submit.Evidence["venue_attempt_state"] != "possible-unconfirmed" {
		t.Fatalf("pre-POST economics or transport state is wrong: %+v", submit)
	}
	if submit.BookSource != "kalshi_ws_full_orderbook:g17:s23:q42" ||
		submit.BookGeneration == nil || *submit.BookGeneration != 17 ||
		submit.BookSubscriptionID == nil || *submit.BookSubscriptionID != 23 ||
		submit.BookSequence == nil || *submit.BookSequence != 42 ||
		submit.SideAsk == nil || *submit.SideAsk != .42 ||
		submit.VisibleDepth == nil || *submit.VisibleDepth != 5 ||
		submit.OriginalLimit == nil || *submit.OriginalLimit != .42 ||
		submit.RequestedQty == nil || *submit.RequestedQty != 2 ||
		submit.FeeQuote == nil || *submit.FeeQuote != .02 ||
		submit.FeeQuoteSource != "exact-kalshi-schedule" {
		t.Fatalf("typed pre-POST evidence is incomplete: %+v", submit)
	}
	_, st := newExecutionShadowTestServer(t)
	ctx := context.Background()
	attempt := storage.ExecutionShadowAttempt{
		AttemptID: row.Intent.ExecutionShadowAttemptID, SignalDecisionID: "decision-r172-book",
		ObservedAt: now, TriggerUnixMS: now.UnixMilli(), Venue: "kalshi", Ticker: "KR172",
		Side: "YES", Action: "BUY", SystemID: "spotlag", Route: "taker",
		SignalSource: "r172-test", QualificationBasis: "durable-prepost-funnel-test",
	}
	if inserted, err := st.InsertExecutionShadowAttempt(ctx, attempt); err != nil || !inserted {
		t.Fatalf("attempt inserted=%v err=%v", inserted, err)
	}
	for _, event := range events {
		event.ElapsedFromTriggerMS = event.At.UnixMilli() - attempt.TriggerUnixMS
		if inserted, err := st.AppendExecutionShadowEvent(ctx, event); err != nil || !inserted {
			t.Fatalf("event %s inserted=%v err=%v", event.Stage, inserted, err)
		}
	}
	report, err := st.ExecutionShadowProfitFunnel(ctx, storage.ProfitFunnelQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Totals.BookValid != 1 || report.Totals.FeeValid != 1 {
		t.Fatalf("crash-recovered executable evidence is invisible to the profit funnel: %+v",
			report.Totals)
	}
}

func TestR172LegacySubmitRecoveryKeepsItsImmutableEventIdentity(t *testing.T) {
	now := time.Now().UTC()
	row := storage.LivePendingRiskReservation{
		Intent: storage.LivePendingRiskIntent{
			ReservationID: "risk-r172-legacy", ExecutionShadowAttemptID: "exec-r172-legacy",
			Created: now,
		},
		Events: []storage.LivePendingRiskEvent{{
			ID: 1, Sequence: 1, Observed: now.Add(time.Millisecond),
			EventType: storage.LivePendingRiskSubmitStarted, AttemptKey: "entry",
			EvidenceJSON: `{"pre_post_book":{"book_generation":99}}`,
		}},
	}
	events := executionShadowRiskLineageEvents(row)
	if len(events) != 2 {
		t.Fatalf("events=%+v", events)
	}
	submit := events[1]
	wantID := executionShadowStableRiskEventID(row.Intent.ExecutionShadowAttemptID,
		row.Intent.ReservationID, "live-risk-submit", "entry")
	if submit.EventID != wantID {
		t.Fatalf("legacy immutable event ID changed: got=%s want=%s", submit.EventID, wantID)
	}
	if _, found := submit.Evidence["pre_post_book"]; found {
		t.Fatalf("unversioned legacy payload was reinterpreted: %+v", submit.Evidence)
	}
	if !submit.VenueAttempted ||
		submit.Reason != "durable venue-mutation boundary rejoins the exact detector opportunity" {
		t.Fatalf("legacy immutable row semantics changed: %+v", submit)
	}
}

func TestR172UnknownCreateErrorsRemainAmbiguousUntilAReceiptExists(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		ambiguous bool
	}{
		{name: "nil", err: nil, ambiguous: false},
		{name: "explicit bad order", err: errors.New(
			"kalshi POST /portfolio/events/orders: status 400: bad request"), ambiguous: false},
		{name: "explicit forbidden", err: errors.New(
			"kalshi POST /portfolio/events/orders: status 403: forbidden"), ambiguous: false},
		{name: "request timeout", err: errors.New(
			"kalshi POST /portfolio/events/orders: status 408: timeout"), ambiguous: true},
		{name: "duplicate identity", err: errors.New(
			"kalshi POST /portfolio/events/orders: status 409: conflict"), ambiguous: true},
		{name: "rate limited", err: errors.New(
			"kalshi POST /portfolio/events/orders: status 429: retry"), ambiguous: true},
		{name: "server failure", err: errors.New(
			"kalshi POST /portfolio/events/orders: status 503: unavailable"), ambiguous: true},
		{name: "dns before wire", err: errors.New("dial tcp: no such host"), ambiguous: true},
		{name: "local signer failure", err: errors.New("private key signer failed"), ambiguous: true},
		{name: "deadline", err: context.DeadlineExceeded, ambiguous: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := venueOutcomeAmbiguous(tc.err); got != tc.ambiguous {
				t.Fatalf("ambiguous=%v want=%v err=%v", got, tc.ambiguous, tc.err)
			}
		})
	}
}
