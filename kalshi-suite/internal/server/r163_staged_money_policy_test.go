package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type r163StagedMoneyPolicyBroker struct {
	*r148ServerStagedBroker
}

func (b *r163StagedMoneyPolicyBroker) Gate(context.Context, storage.ResearchRouteBundle,
	storage.ResearchRouteBundleLeg, string, r148StagedQuote) error {
	return nil
}

func r163SeedStagedMoneyPolicyCoordinator(t *testing.T, generation uint64,
	create func(context.Context, kalshi.OrderRequest) (*kalshi.CreateOrderResult, error),
) (*Server, *r148ServerStagedBroker, *r148StagedBundleCoordinator, string, storage.ResearchRouteBundle) {
	t.Helper()
	ctx := context.Background()
	s := testServer(t)
	s.liveMoneyPolicyGeneration.Store(generation)
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	bundle := r148CoordinatorBundle()
	if inserted, err := s.store.InsertResearchRouteBundle(ctx, bundle); err != nil || !inserted {
		t.Fatalf("insert bundle=%v err=%v", inserted, err)
	}
	quotes := map[int]r148StagedQuote{}
	for i := range bundle.Legs {
		quotes[i] = r148StagedQuote{Price: .40, Available: 2, Fee: .01, Tick: .01, Source: "r163-current-book+fee"}
	}
	plan := r148DefaultExecutionPlan(bundle)
	plan.LiveMoneyPolicyBound = true
	plan.LiveMoneyPolicyGeneration = generation
	order := r148StagedLegOrder(bundle, quotes)
	executionID := "r163-staged-money-policy-" + r148Hash(map[string]any{
		"bundle": bundle.BundleID, "generation": generation,
	})[:16]
	intent := storage.StagedBundleExecutionIntent{
		ExecutionID: executionID, BundleID: bundle.BundleID, SystemID: bundle.SystemID,
		BundleHash: r148ExecutionBundleHash(bundle, plan), OrderedLegs: order,
		OrderedLegsHash: r148Hash(order), RoutePolicy: "staged_fok_v1",
		Created: time.Now().UTC(), Proof: r148StagedDurableProof{
			Sealed: storage.StagedBundleExecutionProof{SystemID: bundle.SystemID, Mean: .18, LowerBound: .10},
			Plan:   plan,
		},
		RequestedSize: bundle.Size, MaxAllInUnit: .82, FirstLegIndex: order[0],
	}
	if inserted, err := s.store.InsertStagedBundleExecutionIntent(ctx, intent); err != nil || !inserted {
		t.Fatalf("insert intent=%v err=%v", inserted, err)
	}
	if _, err := s.store.AppendStagedBundleExecutionEvent(ctx, storage.StagedBundleExecutionEvent{
		ExecutionID: executionID, EventType: "admitted", Observed: time.Now().UTC(), EvidenceJSON: `{}`,
	}); err != nil {
		t.Fatal(err)
	}
	broker := newR148ServerStagedBroker(s)
	broker.boundaryFn = func(context.Context, storage.ResearchRouteBundle,
		*storage.ResearchRouteBundleLeg, string, bool) string {
		return ""
	}
	broker.quoteFn = func(_ context.Context, leg storage.ResearchRouteBundleLeg,
		_ string, _ float64) (r148StagedQuote, error) {
		return quotes[leg.Index], nil
	}
	broker.wireQuoteFn = func(leg storage.ResearchRouteBundleLeg,
		_ string, _ float64) (r148StagedQuote, error) {
		return quotes[leg.Index], nil
	}
	broker.feeFn = func(string, bool, float64, float64, bool) (float64, bool, string) {
		return .01, true, "r163-exact-fee"
	}
	broker.riskBaselineFn = func(context.Context,
		storage.ResearchRouteBundle) (map[string]r148LiveRiskBaseline, error) {
		now := time.Now().UTC()
		out := map[string]r148LiveRiskBaseline{}
		for _, leg := range bundle.Legs {
			out[leg.Venue+"\x00"+leg.Ticker] = r148LiveRiskBaseline{
				Observed: now, ReceiptJSON: `{"source":"r163-complete-account"}`,
			}
		}
		return out, nil
	}
	broker.createFn = create
	coordinatedBroker := &r163StagedMoneyPolicyBroker{r148ServerStagedBroker: broker}
	return s, broker, &r148StagedBundleCoordinator{store: s.store, broker: coordinatedBroker},
		executionID, bundle
}

func TestR163StagedBUYPolicyChangeIsDurableKnownNoSend(t *testing.T) {
	writes := 0
	s, _, coordinator, executionID, _ := r163SeedStagedMoneyPolicyCoordinator(t, 41,
		func(context.Context, kalshi.OrderRequest) (*kalshi.CreateOrderResult, error) {
			writes++
			return &kalshi.CreateOrderResult{OrderID: "must-not-reach-kalshi"}, nil
		})
	s.liveMoneyPolicyGeneration.Add(1)
	if err := coordinator.Resume(context.Background(), executionID); err != nil {
		t.Fatal(err)
	}
	if writes != 0 {
		t.Fatalf("changed money policy reached venue %d times", writes)
	}
	events, err := s.store.StagedBundleExecutionEvents(context.Background(), executionID)
	if err != nil || len(events) != 3 || events[1].EventType != "leg_intent" ||
		events[2].EventType != "leg_unfilled" ||
		events[2].ReceiptSource != r163StagedMoneyPolicyNoSendSource ||
		events[2].OrderID != "" {
		t.Fatalf("staged events=%+v err=%v", events, err)
	}
	risk, ok, err := s.store.LivePendingRiskByID(context.Background(), r148StagedRiskID(executionID))
	if err != nil || !ok || len(risk.Events) != 3 ||
		risk.Events[1].EventType != storage.LivePendingRiskSubmitStarted ||
		risk.Events[2].EventType != storage.LivePendingRiskCleanRejected ||
		risk.Events[2].ReceiptSource != r163StagedMoneyPolicyNoSendSource ||
		risk.Events[2].OrderID != "" {
		t.Fatalf("risk=%+v ok=%v err=%v", risk, ok, err)
	}
}

func TestR163StagedVenueWriteFenceLinearizesPolicyPublication(t *testing.T) {
	enteredCreate := make(chan struct{})
	releaseCreate := make(chan struct{})
	s, broker, coordinator, executionID, _ := r163SeedStagedMoneyPolicyCoordinator(t, 73,
		func(context.Context, kalshi.OrderRequest) (*kalshi.CreateOrderResult, error) {
			close(enteredCreate)
			<-releaseCreate
			return &kalshi.CreateOrderResult{OrderID: "r163-fenced-order"}, nil
		})
	broker.orderFn = func(context.Context, string) (kalshi.Order, error) {
		return kalshi.Order{}, errors.New("authoritative order receipt not visible yet")
	}
	resumed := make(chan error, 1)
	go func() { resumed <- coordinator.Resume(context.Background(), executionID) }()
	select {
	case <-enteredCreate:
	case <-time.After(2 * time.Second):
		t.Fatal("staged BUY never reached the fenced venue seam")
	}
	published := make(chan struct{})
	go func() {
		s.liveWriteFence.Lock()
		s.liveMoneyPolicyGeneration.Add(1)
		s.liveWriteFence.Unlock()
		close(published)
	}()
	select {
	case <-published:
		t.Fatal("money-policy publication crossed an in-flight staged venue write")
	case <-time.After(75 * time.Millisecond):
	}
	risk, ok, err := s.store.LivePendingRiskByID(context.Background(), r148StagedRiskID(executionID))
	if err != nil || !ok || !r148StagedRiskHas(risk, storage.LivePendingRiskSubmitStarted, "") {
		t.Fatalf("submit-start was not durable before venue seam: risk=%+v ok=%v err=%v", risk, ok, err)
	}
	close(releaseCreate)
	select {
	case <-published:
	case <-time.After(2 * time.Second):
		t.Fatal("policy publication did not resume after venue write")
	}
	select {
	case err := <-resumed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("staged coordinator did not return")
	}
}

func TestR163StagedSELLIgnoresStaleBUYPolicyGeneration(t *testing.T) {
	leg := storage.ResearchRouteBundleLeg{
		Index: 0, Venue: "kalshi", Ticker: "KX-R163-SELL", Side: "YES", Quantity: 1,
	}
	bundle := storage.ResearchRouteBundle{
		BundleID: "r163-sell", SystemID: "event-basket-lock",
		Legs: []storage.ResearchRouteBundleLeg{leg},
	}
	s := testServer(t)
	s.liveMoneyPolicyGeneration.Store(9)
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	writes := 0
	broker := newR148ServerStagedBroker(s)
	broker.boundaryFn = func(context.Context, storage.ResearchRouteBundle,
		*storage.ResearchRouteBundleLeg, string, bool) string {
		return ""
	}
	broker.feeFn = func(string, bool, float64, float64, bool) (float64, bool, string) {
		return .01, true, "r163-exact-fee"
	}
	broker.wireQuoteFn = func(storage.ResearchRouteBundleLeg,
		string, float64) (r148StagedQuote, error) {
		return r148StagedQuote{
			Price: .39, Available: 1, Fee: .01, Tick: .01, Source: "r163-resident-sell",
		}, nil
	}
	broker.createFn = func(context.Context, kalshi.OrderRequest) (*kalshi.CreateOrderResult, error) {
		writes++
		return &kalshi.CreateOrderResult{OrderID: "r163-sell-order"}, nil
	}
	broker.orderFn = func(context.Context, string) (kalshi.Order, error) {
		return kalshi.Order{}, errors.New("receipt pending")
	}
	ctx := r163WithStagedMoneyPolicy(context.Background(), true, 8)
	receipt, err := broker.SubmitFOK(ctx, bundle, leg, "SELL", 1, .39, "r163-sell")
	if err != nil || writes != 1 || receipt.State != r148ReceiptPending ||
		receipt.OrderID != "r163-sell-order" {
		t.Fatalf("receipt=%+v writes=%d err=%v", receipt, writes, err)
	}
}

func TestR163StagedSlowRESTPreflightDoesNotHoldCashWriter(t *testing.T) {
	leg := storage.ResearchRouteBundleLeg{
		Index: 0, Venue: "kalshi", Ticker: "KX-R163-SLOW", Side: "YES", Quantity: 1,
	}
	bundle := storage.ResearchRouteBundle{
		BundleID: "r163-slow-preflight", SystemID: "event-basket-lock",
		CertificateStatus: "verified", CertificateHash: "r163", EventVersion: 1,
		Legs: []storage.ResearchRouteBundleLeg{leg},
	}
	s := r148ArmStagedTestServer(t, bundle.SystemID, bundle.Legs)
	broker := newR148ServerStagedBroker(s)
	enteredIdentity := make(chan struct{})
	releaseIdentity := make(chan struct{})
	broker.identityFn = func(context.Context, storage.ResearchRouteBundle) error {
		close(enteredIdentity)
		<-releaseIdentity
		return errors.New("simulated slow REST identity read ended")
	}
	broker.feeFn = func(string, bool, float64, float64, bool) (float64, bool, string) {
		return .01, true, "r163-exact-fee"
	}

	submitDone := make(chan error, 1)
	go func() {
		_, err := broker.SubmitFOK(context.Background(), bundle, leg, "BUY", 1, .40, "r163-slow")
		submitDone <- err
	}()
	select {
	case <-enteredIdentity:
	case <-time.After(2 * time.Second):
		t.Fatal("staged preflight never reached slow identity seam")
	}
	writerAcquired := make(chan struct{})
	go func() {
		s.liveWriteFence.Lock()
		close(writerAcquired)
		s.liveWriteFence.Unlock()
	}()
	select {
	case <-writerAcquired:
		// Correct: slow REST-like preflight owns no final cash reader fence.
	case <-time.After(250 * time.Millisecond):
		close(releaseIdentity)
		t.Fatal("slow staged REST preflight held the cash-authority writer")
	}
	close(releaseIdentity)
	select {
	case err := <-submitDone:
		if err == nil || !strings.Contains(err.Error(), "simulated slow REST") {
			t.Fatalf("staged preflight err=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow staged preflight did not return")
	}
}

func TestR163StagedFinalInstructionKillOrPolicyChangeIsDurableNoSend(t *testing.T) {
	tests := []struct {
		name       string
		generation uint64
		source     string
		inject     func(*Server)
	}{
		{
			name: "kill immediately before POST", generation: 101,
			source: r163StagedWireAuthorityNoSendSource,
			inject: func(s *Server) { s.ks.Trip("r163 final-instruction kill") },
		},
		{
			name: "money policy immediately before POST", generation: 202,
			source: r163StagedMoneyPolicyNoSendSource,
			inject: func(s *Server) { s.liveMoneyPolicyGeneration.Add(1) },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			writes := 0
			s, broker, coordinator, executionID, _ := r163SeedStagedMoneyPolicyCoordinator(
				t, tc.generation,
				func(context.Context, kalshi.OrderRequest) (*kalshi.CreateOrderResult, error) {
					writes++
					return &kalshi.CreateOrderResult{OrderID: "must-not-send"}, nil
				})
			broker.beforeCreateFn = func() { tc.inject(s) }
			if err := coordinator.Resume(context.Background(), executionID); err != nil {
				t.Fatal(err)
			}
			if writes != 0 {
				t.Fatalf("final-instruction authority change reached Kalshi %d times", writes)
			}
			events, err := s.store.StagedBundleExecutionEvents(context.Background(), executionID)
			if err != nil || len(events) != 3 || events[2].EventType != "leg_unfilled" ||
				events[2].ReceiptSource != tc.source || events[2].OrderID != "" {
				t.Fatalf("staged events=%+v err=%v", events, err)
			}
			risk, ok, err := s.store.LivePendingRiskByID(
				context.Background(), r148StagedRiskID(executionID))
			if err != nil || !ok ||
				!r148StagedRiskHas(risk, storage.LivePendingRiskSubmitStarted, "") ||
				!r148StagedRiskHas(risk, storage.LivePendingRiskCleanRejected, "") {
				t.Fatalf("risk=%+v ok=%v err=%v", risk, ok, err)
			}
			last := risk.Events[len(risk.Events)-1]
			if last.EventType != storage.LivePendingRiskCleanRejected ||
				last.ReceiptSource != tc.source || last.OrderID != "" {
				t.Fatalf("final risk receipt=%+v", last)
			}
		})
	}
}

func TestR163StagedKnownNoSendPersistenceDoesNotHoldCashFence(t *testing.T) {
	s, broker, coordinator, executionID, _ := r163SeedStagedMoneyPolicyCoordinator(
		t, 303,
		func(context.Context, kalshi.OrderRequest) (*kalshi.CreateOrderResult, error) {
			t.Fatal("changed final policy must not reach Kalshi")
			return nil, nil
		})
	broker.beforeCreateFn = func() { s.liveMoneyPolicyGeneration.Add(1) }
	persistEntered := make(chan struct{})
	persistRelease := make(chan struct{})
	broker.riskReceiptFn = func(ctx context.Context, gotExecutionID string,
		leg storage.ResearchRouteBundleLeg, action string, receipt r148StagedReceipt) error {
		close(persistEntered)
		<-persistRelease
		return broker.RecordStagedRiskReceipt(ctx, gotExecutionID, leg, action, receipt)
	}

	resumeDone := make(chan error, 1)
	go func() { resumeDone <- coordinator.Resume(context.Background(), executionID) }()
	select {
	case <-persistEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("known-no-send persistence seam was not reached")
	}
	writerAcquired := make(chan struct{})
	go func() {
		s.liveWriteFence.Lock()
		close(writerAcquired)
		s.liveWriteFence.Unlock()
	}()
	select {
	case <-writerAcquired:
		// Correct: terminal no-send durability runs after the final reader fence is released.
	case <-time.After(250 * time.Millisecond):
		close(persistRelease)
		t.Fatal("known-no-send database persistence held the cash-authority fence")
	}
	close(persistRelease)
	select {
	case err := <-resumeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("staged known-no-send coordinator did not return")
	}
}
