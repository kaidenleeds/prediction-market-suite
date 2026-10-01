package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type r148BrokerStub struct {
	quotes           map[string]r148StagedQuote
	admission        map[int]r148StagedQuote
	submits          []r148StagedReceipt
	reconciles       map[string]r148StagedReceipt
	reconcileErr     error
	freeze           []string
	freezeErrs       []error
	writeCount       int
	submittedTickers []string
}

type r148IntentRecoveryBrokerStub struct {
	*r148BrokerStub
	receipt r148StagedReceipt
	err     error
	calls   int
}

func (b *r148IntentRecoveryBrokerStub) ReconcileIntent(_ context.Context, _ storage.ResearchRouteBundle,
	_ storage.ResearchRouteBundleLeg, _ string, _ string, _ time.Time) (r148StagedReceipt, error) {
	b.calls++
	return b.receipt, b.err
}

func (b *r148BrokerStub) Admission(_ context.Context, bundle storage.ResearchRouteBundle, _ storage.StagedBundleExecutionProof) (map[int]r148StagedQuote, error) {
	if b.admission != nil {
		return b.admission, nil
	}
	out := make(map[int]r148StagedQuote, len(bundle.Legs))
	for i, leg := range bundle.Legs {
		q, ok := b.quotes["BUY:"+leg.Ticker]
		if !ok {
			return nil, errors.New("admission quote absent")
		}
		out[i] = q
	}
	return out, nil
}
func (b *r148BrokerStub) Quote(_ context.Context, leg storage.ResearchRouteBundleLeg, action string, _ float64) (r148StagedQuote, error) {
	q, ok := b.quotes[action+":"+leg.Ticker]
	if !ok {
		return r148StagedQuote{}, errors.New("quote absent")
	}
	return q, nil
}
func (b *r148BrokerStub) Gate(context.Context, storage.ResearchRouteBundle, storage.ResearchRouteBundleLeg, string, r148StagedQuote) error {
	return nil
}
func (b *r148BrokerStub) SubmitFOK(_ context.Context, _ storage.ResearchRouteBundle, leg storage.ResearchRouteBundleLeg, _ string, _ float64, _ float64, _ string) (r148StagedReceipt, error) {
	b.writeCount++
	b.submittedTickers = append(b.submittedTickers, leg.Ticker)
	if len(b.submits) == 0 {
		return r148StagedReceipt{}, errors.New("unexpected write")
	}
	r := b.submits[0]
	b.submits = b.submits[1:]
	return r, nil
}
func (b *r148BrokerStub) Reconcile(_ context.Context, _ storage.ResearchRouteBundleLeg, _ string, orderID string) (r148StagedReceipt, error) {
	if b.reconcileErr != nil {
		return r148StagedReceipt{}, b.reconcileErr
	}
	r, ok := b.reconciles[orderID]
	if !ok {
		return r148StagedReceipt{}, errors.New("reconciliation absent")
	}
	return r, nil
}
func (b *r148BrokerStub) Freeze(_ context.Context, reason string) error {
	b.freeze = append(b.freeze, reason)
	if len(b.freezeErrs) > 0 {
		err := b.freezeErrs[0]
		b.freezeErrs = b.freezeErrs[1:]
		return err
	}
	return nil
}

func r148CoordinatorBundle() storage.ResearchRouteBundle {
	level := []storage.ResearchRouteBundleLevel{{Price: .40, Quantity: 2,
		FeeQuotes: []storage.ResearchRouteBundleFeeQuote{{Quantity: 1, Total: .01}}}}
	unwind := []storage.ResearchRouteBundleLevel{{Price: .39, Quantity: 2,
		FeeQuotes: []storage.ResearchRouteBundleFeeQuote{{Quantity: 1, Total: .01}}}}
	legs := make([]storage.ResearchRouteBundleLeg, 0, 2)
	for i, ticker := range []string{"KX-STAGE-A", "KX-STAGE-B"} {
		legs = append(legs, storage.ResearchRouteBundleLeg{Index: i, LegID: ticker + "|YES", Venue: "kalshi",
			Ticker: ticker, Side: "YES", PayoffID: "payoff:" + ticker, Quantity: 1, IntegratedCost: .40,
			ExactFee: .01, VisibleDepth: 2, Tick: .01, Age: .1, BookSource: "kalshi_ws_full",
			SourceClockID: "generation:1:sequence:1", FeeSource: "kalshi-fee", Levels: level,
			Payoff: []float64{float64(1 - i), float64(i)}, UnwindKnown: true,
			UnwindBookSource: "kalshi_ws_full", UnwindFeeSource: "kalshi-fee", UnwindLevels: unwind})
	}
	return storage.ResearchRouteBundle{BundleID: "r148-stage-bundle", SystemID: "event-basket-lock",
		Cohort: "r148-test", ExperimentVersion: 1, OpportunityID: "r148-stage-opportunity",
		CanonicalEventID: "event:r148", EventVersion: 1, CertificateHash: "certificate:r148",
		CertificateStatus: "verified", RouteKind: "all_leg_taker", Observed: time.Now().UTC(),
		Size: 1, Cost: .80, Fee: .02, PayoutFloor: 1, NetFloor: .18, PartialFillWorst: -.82,
		UnwindWorst: -.04, UnwindKnown: true, DecisionLatencyMS: 2, LatencyKnown: true,
		StateVectorHash: "state:r148", Blocker: "non-atomic", Legs: legs,
		States:   []storage.ResearchRouteBundleState{{Index: 0, StateID: "A", Payout: 1}, {Index: 1, StateID: "B", Payout: 1}},
		Evidence: map[string]any{"test": true}}
}

func r148SeedCoordinator(t *testing.T, broker *r148BrokerStub) (*r148StagedBundleCoordinator, string, storage.ResearchRouteBundle) {
	t.Helper()
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	b := r148CoordinatorBundle()
	if inserted, insertErr := st.InsertResearchRouteBundle(context.Background(), b); insertErr != nil || !inserted {
		t.Fatalf("bundle inserted=%v err=%v", inserted, insertErr)
	}
	admission := map[int]r148StagedQuote{}
	for i, leg := range b.Legs {
		admission[i] = broker.quotes["BUY:"+leg.Ticker]
	}
	order := r148StagedLegOrder(b, admission)
	executionID := "r148-stage-execution"
	intent := storage.StagedBundleExecutionIntent{ExecutionID: executionID, BundleID: b.BundleID,
		SystemID: b.SystemID, BundleHash: r148BundleIdentityHash(b), OrderedLegsHash: r148Hash(order),
		RoutePolicy: "staged_fok_v1", Created: time.Now().UTC(), Proof: map[string]any{"sealed": true},
		OrderedLegs: order, RequestedSize: 1, MaxAllInUnit: .82, FirstLegIndex: order[0]}
	if inserted, insertErr := st.InsertStagedBundleExecutionIntent(context.Background(), intent); insertErr != nil || !inserted {
		t.Fatalf("intent inserted=%v err=%v", inserted, insertErr)
	}
	if _, appendErr := st.AppendStagedBundleExecutionEvent(context.Background(), storage.StagedBundleExecutionEvent{
		ExecutionID: executionID, EventType: "admitted", Observed: time.Now().UTC(), EvidenceJSON: `{}`}); appendErr != nil {
		t.Fatal(appendErr)
	}
	return &r148StagedBundleCoordinator{store: st, broker: broker}, executionID, b
}

func r148DefaultQuotes(b storage.ResearchRouteBundle) map[string]r148StagedQuote {
	out := map[string]r148StagedQuote{}
	for _, leg := range b.Legs {
		out["BUY:"+leg.Ticker] = r148StagedQuote{Price: .40, Available: 2, Fee: .01, Source: "exact-book"}
		out["SELL:"+leg.Ticker] = r148StagedQuote{Price: .39, Available: 2, Fee: .01, Source: "exact-book"}
	}
	return out
}

func TestR148StagedCoordinatorCompletesOnlyAfterAuthoritativeFOKFills(t *testing.T) {
	template := r148CoordinatorBundle()
	broker := &r148BrokerStub{quotes: r148DefaultQuotes(template), reconciles: map[string]r148StagedReceipt{},
		submits: []r148StagedReceipt{
			{OrderID: "o1", State: r148ReceiptFilled, FilledQty: 1, AveragePrice: .40, FeeTotal: .01, Authoritative: true, Source: "venue-get"},
			{OrderID: "o2", State: r148ReceiptFilled, FilledQty: 1, AveragePrice: .40, FeeTotal: .01, Authoritative: true, Source: "venue-get"},
		}}
	c, id, _ := r148SeedCoordinator(t, broker)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := c.Resume(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	events, err := c.store.StagedBundleExecutionEvents(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if events[len(events)-1].EventType != "completed" || broker.writeCount != 2 || len(broker.freeze) != 0 {
		t.Fatalf("events=%+v writes=%d freeze=%v", events, broker.writeCount, broker.freeze)
	}
}

func TestR165FlatLegacyStagedIntentRetiresWithoutVenueWrite(t *testing.T) {
	template := r148CoordinatorBundle()
	broker := &r148BrokerStub{quotes: r148DefaultQuotes(template)}
	c, id, _ := r148SeedCoordinator(t, broker)
	if err := c.retireFlatBeforeFirstVenueWrite(context.Background(), id,
		"R165 retired staged research/Paper cash authority before first venue write"); err != nil {
		t.Fatal(err)
	}
	events, err := c.store.StagedBundleExecutionEvents(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 || events[len(events)-1].EventType != "rejected" || broker.writeCount != 0 {
		t.Fatalf("events=%+v writes=%d", events, broker.writeCount)
	}
}

func TestR149StagedSourceReadTimeoutBubblesButConfirmedMissingFreezes(t *testing.T) {
	template := r148CoordinatorBundle()
	broker := &r148BrokerStub{quotes: r148DefaultQuotes(template), reconciles: map[string]r148StagedReceipt{}}
	c, id, _ := r148SeedCoordinator(t, broker)
	c.sourceBundleFn = func(context.Context, string) (storage.ResearchRouteBundle, bool, error) {
		return storage.ResearchRouteBundle{}, false, context.DeadlineExceeded
	}
	if err := c.Resume(context.Background(), id); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("transient source read err=%v", err)
	}
	if len(broker.freeze) != 0 {
		t.Fatalf("transient source read froze money boundary: %v", broker.freeze)
	}

	broker2 := &r148BrokerStub{quotes: r148DefaultQuotes(template), reconciles: map[string]r148StagedReceipt{}}
	c2, id2, _ := r148SeedCoordinator(t, broker2)
	c2.sourceBundleFn = func(context.Context, string) (storage.ResearchRouteBundle, bool, error) {
		return storage.ResearchRouteBundle{}, false, nil
	}
	if err := c2.Resume(context.Background(), id2); err != nil {
		t.Fatalf("confirmed missing source freeze journal err=%v", err)
	}
	if len(broker2.freeze) != 1 || !strings.Contains(broker2.freeze[0], "immutable source bundle missing") {
		t.Fatalf("confirmed missing source did not freeze: %v", broker2.freeze)
	}
}

func TestR148StagedCoordinatorRecoversIntentWithoutResubmitting(t *testing.T) {
	for _, tc := range []struct {
		name    string
		receipt r148StagedReceipt
		want    string
	}{
		{name: "filled", receipt: r148StagedReceipt{OrderID: "recovered-fill", State: r148ReceiptFilled,
			FilledQty: 1, AveragePrice: .40, FeeTotal: .01, Authoritative: true,
			Source: "kalshi-client-order-history"}, want: "leg_filled"},
		{name: "unfilled", receipt: r148StagedReceipt{OrderID: "recovered-no-fill", State: r148ReceiptUnfilled,
			Authoritative: true, Source: "kalshi-client-order-history"}, want: "leg_unfilled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := &r148BrokerStub{quotes: r148DefaultQuotes(r148CoordinatorBundle())}
			c, executionID, bundle := r148SeedCoordinator(t, base)
			recovery := &r148IntentRecoveryBrokerStub{r148BrokerStub: base, receipt: tc.receipt}
			c.broker = recovery
			leg := bundle.Legs[0]
			if _, err := c.store.AppendStagedBundleExecutionEvent(context.Background(), storage.StagedBundleExecutionEvent{
				ExecutionID: executionID, EventType: "leg_intent", Observed: time.Now().UTC(), LegIndex: &leg.Index,
				Venue: leg.Venue, Ticker: leg.Ticker, Side: leg.Side, Action: "BUY", RequestedQty: leg.Quantity,
				EvidenceJSON: `{}`}); err != nil {
				t.Fatal(err)
			}
			if err := c.Resume(context.Background(), executionID); err != nil {
				t.Fatal(err)
			}
			events, err := c.store.StagedBundleExecutionEvents(context.Background(), executionID)
			if err != nil || events[len(events)-1].EventType != tc.want || recovery.calls != 1 || base.writeCount != 0 || len(base.freeze) != 0 {
				t.Fatalf("events=%+v calls=%d writes=%d freezes=%v err=%v", events, recovery.calls, base.writeCount, base.freeze, err)
			}
		})
	}
}

func TestR148StagedCoordinatorFreezesFailedIntentRecoveryWithoutRetry(t *testing.T) {
	base := &r148BrokerStub{quotes: r148DefaultQuotes(r148CoordinatorBundle())}
	c, executionID, bundle := r148SeedCoordinator(t, base)
	recovery := &r148IntentRecoveryBrokerStub{r148BrokerStub: base, err: errors.New("history incomplete")}
	c.broker = recovery
	leg := bundle.Legs[0]
	if _, err := c.store.AppendStagedBundleExecutionEvent(context.Background(), storage.StagedBundleExecutionEvent{
		ExecutionID: executionID, EventType: "leg_intent", Observed: time.Now().UTC(), LegIndex: &leg.Index,
		Venue: leg.Venue, Ticker: leg.Ticker, Side: leg.Side, Action: "BUY", RequestedQty: leg.Quantity,
		EvidenceJSON: `{}`}); err != nil {
		t.Fatal(err)
	}
	if err := c.Resume(context.Background(), executionID); err != nil {
		t.Fatal(err)
	}
	events, err := c.store.StagedBundleExecutionEvents(context.Background(), executionID)
	if err != nil || events[len(events)-1].EventType != "frozen" || recovery.calls != 1 ||
		base.writeCount != 0 || len(base.freeze) != 1 || !strings.Contains(base.freeze[0], "history incomplete") {
		t.Fatalf("events=%+v calls=%d writes=%d freezes=%v err=%v", events, recovery.calls, base.writeCount, base.freeze, err)
	}
}

func TestR149StagedTransientVenueReconciliationPausesWithoutFreeze(t *testing.T) {
	t.Run("intent_without_receipt", func(t *testing.T) {
		base := &r148BrokerStub{quotes: r148DefaultQuotes(r148CoordinatorBundle())}
		c, executionID, bundle := r148SeedCoordinator(t, base)
		recovery := &r148IntentRecoveryBrokerStub{r148BrokerStub: base, err: context.DeadlineExceeded}
		c.broker = recovery
		leg := bundle.Legs[0]
		if _, err := c.store.AppendStagedBundleExecutionEvent(context.Background(), storage.StagedBundleExecutionEvent{
			ExecutionID: executionID, EventType: "leg_intent", Observed: time.Now().UTC(), LegIndex: &leg.Index,
			Venue: leg.Venue, Ticker: leg.Ticker, Side: leg.Side, Action: "BUY", RequestedQty: leg.Quantity,
			EvidenceJSON: `{}`}); err != nil {
			t.Fatal(err)
		}
		if err := c.Resume(context.Background(), executionID); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("intent reconciliation err=%v", err)
		}
		if len(base.freeze) != 0 || base.writeCount != 0 {
			t.Fatalf("transient intent reconciliation froze/submitted: freeze=%v writes=%d", base.freeze, base.writeCount)
		}
	})

	t.Run("submitted_order", func(t *testing.T) {
		template := r148CoordinatorBundle()
		base := &r148BrokerStub{quotes: r148DefaultQuotes(template), reconcileErr: context.DeadlineExceeded}
		c, executionID, bundle := r148SeedCoordinator(t, base)
		leg := bundle.Legs[0]
		for _, event := range []storage.StagedBundleExecutionEvent{
			{ExecutionID: executionID, EventType: "leg_intent", Observed: time.Now().UTC(), LegIndex: &leg.Index,
				Venue: leg.Venue, Ticker: leg.Ticker, Side: leg.Side, Action: "BUY", RequestedQty: leg.Quantity, EvidenceJSON: `{}`},
			{ExecutionID: executionID, EventType: "leg_submitted", Observed: time.Now().UTC(), LegIndex: &leg.Index,
				Venue: leg.Venue, Ticker: leg.Ticker, Side: leg.Side, Action: "BUY", RequestedQty: leg.Quantity,
				OrderID: "pending-order", EvidenceJSON: `{}`},
		} {
			if _, err := c.store.AppendStagedBundleExecutionEvent(context.Background(), event); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.Resume(context.Background(), executionID); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("submitted reconciliation err=%v", err)
		}
		if len(base.freeze) != 0 || base.writeCount != 0 {
			t.Fatalf("transient submitted reconciliation froze/submitted: freeze=%v writes=%d", base.freeze, base.writeCount)
		}
	})
}

func TestR148StagedCoordinatorUnwindsKnownFillWhenNextLegFails(t *testing.T) {
	template := r148CoordinatorBundle()
	broker := &r148BrokerStub{quotes: r148DefaultQuotes(template), reconciles: map[string]r148StagedReceipt{},
		submits: []r148StagedReceipt{
			{OrderID: "o1", State: r148ReceiptFilled, FilledQty: 1, AveragePrice: .40, FeeTotal: .01, Authoritative: true, Source: "venue-get"},
			{OrderID: "o2", State: r148ReceiptUnfilled, FilledQty: 0, Authoritative: true, Source: "venue-get", Reason: "FOK expired"},
			{OrderID: "u1", State: r148ReceiptFilled, FilledQty: 1, AveragePrice: .39, FeeTotal: .01, Authoritative: true, Source: "venue-get"},
		}}
	c, id, _ := r148SeedCoordinator(t, broker)
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if err := c.Resume(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	events, err := c.store.StagedBundleExecutionEvents(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if events[len(events)-1].EventType != "rejected" || broker.writeCount != 3 || len(broker.freeze) != 0 {
		t.Fatalf("events=%+v writes=%d freeze=%v", events, broker.writeCount, broker.freeze)
	}
	foundUnwind := false
	for _, e := range events {
		foundUnwind = foundUnwind || e.EventType == "unwind_filled"
	}
	if !foundUnwind {
		t.Fatal("known first-leg fill was not authoritatively unwound")
	}
}

func TestR148StagedCoordinatorRestartAmbiguityFreezesWithoutAnotherWrite(t *testing.T) {
	template := r148CoordinatorBundle()
	broker := &r148BrokerStub{quotes: r148DefaultQuotes(template), reconciles: map[string]r148StagedReceipt{
		"pending-order": {OrderID: "pending-order", State: r148ReceiptAmbiguous, Source: "venue-get", Reason: "lookup timeout"},
	}}
	c, id, b := r148SeedCoordinator(t, broker)
	idx := 0
	for _, typ := range []string{"leg_intent", "leg_submitted"} {
		orderID := ""
		if typ == "leg_submitted" {
			orderID = "pending-order"
		}
		_, err := c.store.AppendStagedBundleExecutionEvent(context.Background(), storage.StagedBundleExecutionEvent{
			ExecutionID: id, EventType: typ, Observed: time.Now().UTC(), LegIndex: &idx, Venue: "kalshi",
			Ticker: b.Legs[0].Ticker, Side: "YES", Action: "BUY", OrderID: orderID, RequestedQty: 1,
			EvidenceJSON: `{}`})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Resume(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	events, err := c.store.StagedBundleExecutionEvents(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if events[len(events)-1].EventType != "frozen" || broker.writeCount != 0 || len(broker.freeze) != 1 ||
		!strings.Contains(broker.freeze[0], "ambiguous") {
		t.Fatalf("events=%+v writes=%d freeze=%v", events, broker.writeCount, broker.freeze)
	}
}

func TestR148StagedCoordinatorIntentOnlyCrashWindowFreezesInsteadOfResubmitting(t *testing.T) {
	template := r148CoordinatorBundle()
	broker := &r148BrokerStub{quotes: r148DefaultQuotes(template), reconciles: map[string]r148StagedReceipt{}}
	c, id, b := r148SeedCoordinator(t, broker)
	idx := 0
	if _, err := c.store.AppendStagedBundleExecutionEvent(context.Background(), storage.StagedBundleExecutionEvent{
		ExecutionID: id, EventType: "leg_intent", Observed: time.Now().UTC(), LegIndex: &idx,
		Venue: "kalshi", Ticker: b.Legs[0].Ticker, Side: "YES", Action: "BUY", RequestedQty: 1,
		EvidenceJSON: `{}`}); err != nil {
		t.Fatal(err)
	}
	if err := c.Resume(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	events, err := c.store.StagedBundleExecutionEvents(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if events[len(events)-1].EventType != "frozen" || broker.writeCount != 0 || len(broker.freeze) != 1 ||
		!strings.Contains(broker.freeze[0], "cannot be disproved") {
		t.Fatalf("events=%+v writes=%d freeze=%v", events, broker.writeCount, broker.freeze)
	}
}

func TestR148StagedCoordinatorPendingWithoutDurableOrderIDFreezes(t *testing.T) {
	template := r148CoordinatorBundle()
	broker := &r148BrokerStub{quotes: r148DefaultQuotes(template), reconciles: map[string]r148StagedReceipt{},
		submits: []r148StagedReceipt{{State: r148ReceiptPending, Source: "ack-without-key"}}}
	c, id, _ := r148SeedCoordinator(t, broker)
	if err := c.Resume(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	events, err := c.store.StagedBundleExecutionEvents(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if events[len(events)-1].EventType != "frozen" || broker.writeCount != 1 || len(broker.freeze) != 1 {
		t.Fatalf("events=%+v writes=%d freeze=%v", events, broker.writeCount, broker.freeze)
	}
}

func TestR148StagedCoordinatorRetriesFailedMoneyFreezeBeforeTerminalJournal(t *testing.T) {
	template := r148CoordinatorBundle()
	broker := &r148BrokerStub{quotes: r148DefaultQuotes(template), reconciles: map[string]r148StagedReceipt{},
		submits:    []r148StagedReceipt{{State: r148ReceiptPending, Source: "ack-without-key"}},
		freezeErrs: []error{errors.New("durable kill switch unavailable")}}
	c, id, _ := r148SeedCoordinator(t, broker)
	if err := c.Resume(context.Background(), id); err == nil || !strings.Contains(err.Error(), "kill switch") {
		t.Fatalf("first failed freeze error=%v", err)
	}
	events, err := c.store.StagedBundleExecutionEvents(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if events[len(events)-1].EventType == "frozen" {
		t.Fatal("failed money-boundary freeze was incorrectly journaled terminal")
	}
	if err := c.Resume(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	events, err = c.store.StagedBundleExecutionEvents(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if events[len(events)-1].EventType != "frozen" || len(broker.freeze) != 2 {
		t.Fatalf("freeze retry events=%+v attempts=%v", events, broker.freeze)
	}
}

func TestR148StagedCoordinatorRejectsReceiptWithoutAuthoritySource(t *testing.T) {
	template := r148CoordinatorBundle()
	broker := &r148BrokerStub{quotes: r148DefaultQuotes(template), reconciles: map[string]r148StagedReceipt{},
		submits: []r148StagedReceipt{{OrderID: "opaque", State: r148ReceiptFilled, FilledQty: 1,
			AveragePrice: .40, FeeTotal: .01, Authoritative: true}}}
	c, id, _ := r148SeedCoordinator(t, broker)
	if err := c.Resume(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	events, err := c.store.StagedBundleExecutionEvents(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if events[len(events)-1].EventType != "frozen" || len(broker.freeze) != 1 ||
		!strings.Contains(broker.freeze[0], "malformed") {
		t.Fatalf("source-less terminal receipt escaped: events=%+v freezes=%v", events, broker.freeze)
	}
}

func TestR148StagedCoordinatorPersistsFreshAdmissionLiquidityOrder(t *testing.T) {
	template := r148CoordinatorBundle()
	// The frozen research frame says A is least liquid. Current admission says B is least liquid.
	template.Legs[0].VisibleDepth, template.Legs[1].VisibleDepth = 1, 100
	current := map[int]r148StagedQuote{
		0: {Price: .40, Available: 10, Fee: .01, Source: "current-auth-book"},
		1: {Price: .40, Available: 1, Fee: .01, Source: "current-auth-book"},
	}
	stale := map[int]r148StagedQuote{
		0: {Price: .40, Available: template.Legs[0].VisibleDepth, Fee: .01, Source: "frozen-research"},
		1: {Price: .40, Available: template.Legs[1].VisibleDepth, Fee: .01, Source: "frozen-research"},
	}
	if r148StagedLegOrder(template, stale)[0] != 0 || r148StagedLegOrder(template, current)[0] != 1 {
		t.Fatal("test fixture did not reverse stale and current liquidity order")
	}
	broker := &r148BrokerStub{quotes: r148DefaultQuotes(template), reconciles: map[string]r148StagedReceipt{},
		submits: []r148StagedReceipt{{OrderID: "fresh-first", State: r148ReceiptFilled, FilledQty: 1,
			AveragePrice: .40, FeeTotal: .01, Authoritative: true, Source: "venue-get"}}}
	c, id, b := r148SeedCoordinator(t, broker)
	order := r148StagedLegOrder(b, current)
	// Replace the seed's isolated temp store with another isolated journal carrying the current order.
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if inserted, err := st.InsertResearchRouteBundle(context.Background(), b); err != nil || !inserted {
		t.Fatalf("insert bundle=%v err=%v", inserted, err)
	}
	intent := storage.StagedBundleExecutionIntent{ExecutionID: id, BundleID: b.BundleID,
		SystemID: b.SystemID, BundleHash: r148BundleIdentityHash(b), OrderedLegs: order,
		OrderedLegsHash: r148Hash(order), RoutePolicy: "staged_fok_v1", Created: time.Now().UTC(),
		Proof: map[string]any{"current_admission": true}, RequestedSize: 1, MaxAllInUnit: .82,
		FirstLegIndex: order[0]}
	if inserted, err := st.InsertStagedBundleExecutionIntent(context.Background(), intent); err != nil || !inserted {
		t.Fatalf("insert intent=%v err=%v", inserted, err)
	}
	if _, err := st.AppendStagedBundleExecutionEvent(context.Background(), storage.StagedBundleExecutionEvent{
		ExecutionID: id, EventType: "admitted", Observed: time.Now().UTC(), EvidenceJSON: `{}`}); err != nil {
		t.Fatal(err)
	}
	c.store = st
	if err := c.Resume(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	got, ok, err := st.StagedBundleExecutionIntentByID(context.Background(), id)
	if err != nil || !ok || len(got.OrderedLegs) != 2 || got.OrderedLegs[0] != 1 {
		t.Fatalf("persisted current order=%+v ok=%v err=%v", got.OrderedLegs, ok, err)
	}
	if len(broker.submittedTickers) != 1 || broker.submittedTickers[0] != b.Legs[1].Ticker {
		t.Fatalf("first write=%v want current least-liquid %s", broker.submittedTickers, b.Legs[1].Ticker)
	}
}

func TestR148StagedCoordinatorDeduplicatesUnchangedPendingRecoveryReceipt(t *testing.T) {
	template := r148CoordinatorBundle()
	pending := r148StagedReceipt{OrderID: "pending-same", State: r148ReceiptPending,
		Source: "venue-get", Reason: "still open"}
	broker := &r148BrokerStub{quotes: r148DefaultQuotes(template),
		reconciles: map[string]r148StagedReceipt{"pending-same": pending}}
	c, id, b := r148SeedCoordinator(t, broker)
	idx := 0
	for _, typ := range []string{"leg_intent", "leg_submitted"} {
		orderID := ""
		if typ == "leg_submitted" {
			orderID = pending.OrderID
		}
		if _, err := c.store.AppendStagedBundleExecutionEvent(context.Background(), storage.StagedBundleExecutionEvent{
			ExecutionID: id, EventType: typ, Observed: time.Now().UTC(), LegIndex: &idx, Venue: "kalshi",
			Ticker: b.Legs[0].Ticker, Side: "YES", Action: "BUY", OrderID: orderID,
			RequestedQty: 1, EvidenceJSON: `{}`}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Resume(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	before, err := c.store.StagedBundleExecutionEvents(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Resume(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	after, err := c.store.StagedBundleExecutionEvents(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("unchanged pending recovery grew journal: before=%d after=%d", len(before), len(after))
	}
}
