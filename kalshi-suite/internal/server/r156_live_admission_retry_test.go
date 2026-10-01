package server

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r156AdmissionTestServer(t *testing.T) (*Server, *storage.Store,
	*r154AdmissionSnapshotFetcherFake, *r154KalshiAdmissionSnapshotCache) {
	t.Helper()
	s := testServer(t)
	s.kal = &kalshi.Client{}
	fetcher := &r154AdmissionSnapshotFetcherFake{balance: 10_000, portfolio: 0}
	cache := newR154KalshiAdmissionSnapshotCache(fetcher)
	s.kalshiAdmissionSnapshotOnce.Do(func() { s.kalshiAdmissionSnapshot = cache })

	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemPolyUS = false
	cfg.Risk.LiveMaxOrderPct = .05
	cfg.Risk.LiveExposureCapPct = .20
	cfg.Risk.LiveCryptoCapPct = .10
	cfg.Risk.LiveClusterCapPct = .03
	cfg.Risk.LiveMaxOrderUSD = -1
	cfg.Risk.LiveExposureCapUSD = -1
	cfg.Risk.LiveKalshiCapUSD = -1
	cfg.Risk.LivePolyusCapUSD = -1
	cfg.Risk.LiveActivationAt = ""
	cfg.Risk.LiveActivationBankrollUSD = 0
	s.cfgP.Store(&cfg)
	s.liveArmed = true
	s.liveBankArm = map[string]float64{"kalshi": 100}
	return s, s.store, fetcher, cache
}

func r156AdmissionRequest(signalAt time.Time) r156KalshiAdmissionRecheckRequest {
	candidate := liveMirrorCandidate{
		Platform: "kalshi", Ticker: "KXR156-TARGET", Title: "R156 target",
		Side: "YES", Source: "auto-cons-kflow", Price: .40, At: signalAt,
	}
	return r156KalshiAdmissionRecheckRequest{
		Auto: true, TriggerUnixMS: signalAt.UnixMilli(),
		Candidate: candidate, BudgetCandidate: candidate,
		CostUSD: .41, Count: 1, WireUnit: .40, FeeUSD: .01,
		ProspectiveAllocation: true,
		ProofMean:             .20,
		ProofLower:            .10,
		ProofFeePerContract:   .01,
		Quote: liveMirrorQuote{
			Price: .40, Depth: 200, MinQty: 1, MinQtyKnown: true,
		},
	}
}

func r156SetAdmissionPositions(fetcher *r154AdmissionSnapshotFetcherFake,
	positions []kalshi.MarketPosition) {
	fetcher.mu.Lock()
	fetcher.positions = append([]kalshi.MarketPosition(nil), positions...)
	fetcher.mu.Unlock()
}

func TestR156AdmissionRetryIsTypedAndPreReservationOnly(t *testing.T) {
	wrapped := errors.Join(errors.New("boundary"), errR156KalshiAdmissionSnapshotNotCurrent)
	if !r156KalshiAdmissionRetryable(wrapped, false, "") {
		t.Fatal("typed pre-reservation account race was not retryable")
	}
	if r156KalshiAdmissionRetryable(
		errors.New(errR156KalshiAdmissionSnapshotNotCurrent.Error()), false, "") {
		t.Fatal("same error text without the typed sentinel became retryable")
	}
	if r156KalshiAdmissionRetryable(wrapped, true, "") {
		t.Fatal("a second account refresh became retryable")
	}
	if r156KalshiAdmissionRetryable(wrapped, false, "risk-already-durable") {
		t.Fatal("an error after a durable risk id became retryable")
	}
}

func TestR156OneRefreshRecoversUnrelatedPrivateFillAndKeepsDeadline(t *testing.T) {
	s, _, fetcher, cache := r156AdmissionTestServer(t)
	baseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx, first, err := cache.SnapshotContext(baseCtx)
	if err != nil {
		t.Fatal(err)
	}
	deadline, hadDeadline := ctx.Deadline()

	r156SetAdmissionPositions(fetcher, []kalshi.MarketPosition{{
		Ticker: "KXUNRELATED-OTHER", Position: 1, MarketExposure: 100,
		LastUpdatedTS: time.Now().UTC().Format(time.RFC3339Nano),
	}})
	cache.Invalidate() // the private fill/order stream fences the carried account epoch

	result := s.r156RefreshAndRecheckKalshiAdmission(ctx, r156AdmissionRequest(time.Now()))
	if result.Reason != "" || result.HTTPStatus != 200 {
		t.Fatalf("unrelated account update did not recover: %+v", result)
	}
	second, ok := r154KalshiAdmissionSnapshotFromContext(result.Context)
	if !ok || second.epoch == first.epoch || !cache.CurrentForExecution(second, time.Now()) {
		t.Fatalf("refresh did not bind a current new epoch: first=%+v second=%+v ok=%v",
			first, second, ok)
	}
	if gotDeadline, ok := result.Context.Deadline(); ok != hadDeadline || !gotDeadline.Equal(deadline) {
		t.Fatalf("refresh extended/replaced the handler deadline: before=%v/%v after=%v/%v",
			deadline, hadDeadline, gotDeadline, ok)
	}
	if balance, positions, orders := fetcher.calls(); balance != 2 || positions != 2 || orders != 2 {
		t.Fatalf("refresh did not fetch one complete second account view: %d/%d/%d",
			balance, positions, orders)
	}
}

func TestR156RefreshedAccountConflictRefusesBeforeReservation(t *testing.T) {
	s, st, fetcher, cache := r156AdmissionTestServer(t)
	ctx, _, err := cache.SnapshotContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r156SetAdmissionPositions(fetcher, []kalshi.MarketPosition{{
		Ticker: "KXR156-TARGET", Position: 1, MarketExposure: 40,
		LastUpdatedTS: time.Now().UTC().Format(time.RFC3339Nano),
	}})
	cache.Invalidate()

	result := s.r156RefreshAndRecheckKalshiAdmission(ctx, r156AdmissionRequest(time.Now()))
	if !strings.Contains(result.Reason, "existing-kalshi-position") {
		t.Fatalf("new target position escaped refreshed account guard: %+v", result)
	}
	active, activeErr := st.ActiveLivePendingRisk(context.Background())
	if activeErr != nil || len(active) != 0 {
		t.Fatalf("refreshed conflict created durable risk: active=%d err=%v", len(active), activeErr)
	}
}

func TestR156SecondInvalidationFailsClosed(t *testing.T) {
	s, _, _, cache := r156AdmissionTestServer(t)
	ctx, _, err := cache.SnapshotContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cache.Invalidate()
	result := s.r156RefreshAndRecheckKalshiAdmission(ctx, r156AdmissionRequest(time.Now()))
	if result.Reason != "" {
		t.Fatal(result.Reason)
	}
	snapshot, ok := r154KalshiAdmissionSnapshotFromContext(result.Context)
	if !ok {
		t.Fatal("recovered context omitted its snapshot")
	}
	cache.Invalidate()
	if cache.CurrentForExecution(snapshot, time.Now()) {
		t.Fatal("second private update did not invalidate the refreshed epoch")
	}
	if r156KalshiAdmissionRetryable(errR156KalshiAdmissionSnapshotNotCurrent, true, "") {
		t.Fatal("second invalidation was allowed a second refresh")
	}
}

func TestR156StorageAndPostReservationErrorsNeverRetry(t *testing.T) {
	for _, err := range []error{
		errors.New("sqlite busy"),
		errors.New("pending-risk retry could not reload its immutable reservation"),
		context.DeadlineExceeded,
	} {
		if r156KalshiAdmissionRetryable(err, false, "") {
			t.Fatalf("non-account error became retryable: %v", err)
		}
	}
	if r156KalshiAdmissionRetryable(errR156KalshiAdmissionSnapshotNotCurrent, false, "risk-1") {
		t.Fatal("typed error with a durable reservation id became retryable")
	}
}

func TestR156RefreshDoesNotRenewSignalTTLOrHandlerDeadline(t *testing.T) {
	now := time.UnixMilli(time.Now().UnixMilli())
	if why := r156KalshiAdmissionFreshnessReason(context.Background(), true,
		now.Add(-liveMirrorTTL+time.Millisecond).UnixMilli(), now); why != "" {
		t.Fatalf("still-live signal refused: %q", why)
	}
	if why := r156KalshiAdmissionFreshnessReason(context.Background(), true,
		now.Add(-liveMirrorTTL-time.Millisecond).UnixMilli(), now); !strings.Contains(why, "aged past") {
		t.Fatalf("expired signal received renewed authority: %q", why)
	}
	if why := r156KalshiAdmissionFreshnessReason(context.Background(), true,
		now.Add(3*time.Second).UnixMilli(), now); !strings.Contains(why, "future") {
		t.Fatalf("future signal receipt escaped: %q", why)
	}
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if why := r156KalshiAdmissionFreshnessReason(expired, false, 0, now); !strings.Contains(why, "deadline") {
		t.Fatalf("expired manual handler context escaped: %q", why)
	}
}

func TestR156RecoveredReservationUsesSecondVenueWatermark(t *testing.T) {
	s, st, fetcher, cache := r156AdmissionTestServer(t)
	firstAt := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Microsecond)
	secondAt := firstAt.Add(time.Minute)
	r156SetAdmissionPositions(fetcher, []kalshi.MarketPosition{{
		Ticker: "KXR156-TARGET", Position: 1, LastUpdatedTS: firstAt.Format(time.RFC3339Nano),
	}})
	ctx, _, err := cache.SnapshotContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r156SetAdmissionPositions(fetcher, []kalshi.MarketPosition{{
		Ticker: "KXR156-TARGET", Position: 2, LastUpdatedTS: secondAt.Format(time.RFC3339Nano),
	}})
	cache.Invalidate()
	ctx, second, err := cache.SnapshotContext(ctx)
	if err != nil {
		t.Fatal(err)
	}

	riskID, err := s.r148ReserveSingleRisk(ctx, r148SingleRiskRequest{
		Product: "single", Venue: "kalshi", Ticker: "KXR156-TARGET", Side: "YES",
		Action: "BUY", Route: "taker", DispatchSource: "r156-test", SystemID: "kflow",
		Quantity: 1, LimitPrice: .40, PrincipalUSD: .40, FeeUSD: .01,
		ClientOrderID: "r156-client", AttemptNonce: "r156-client",
	})
	if err != nil {
		t.Fatal(err)
	}
	row, found, err := st.LivePendingRiskByID(context.Background(), riskID)
	if err != nil || !found {
		t.Fatalf("reservation missing: found=%v err=%v", found, err)
	}
	var receipt struct {
		Source     string `json:"source"`
		ObservedAt string `json:"observed_at"`
	}
	if err := json.Unmarshal([]byte(row.Intent.BaselineReceiptJSON), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Source != "kalshi-carried-admission-snapshot" ||
		receipt.ObservedAt != secondAt.Format(time.RFC3339Nano) ||
		!row.Intent.BaselineObserved.Equal(secondAt) ||
		second.epoch == 0 {
		t.Fatalf("reservation did not use recovered venue watermark: receipt=%+v baseline=%s snapshot=%+v",
			receipt, row.Intent.BaselineObserved, second)
	}
}

func TestR156QuoteChangeAfterRecoveryRemainsAReleasedPreSubmitNoSend(t *testing.T) {
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
	reserveAt := strings.Index(handler, "riskID, riskErr = s.r148ReserveSingleRisk")
	quoteAt := strings.Index(handler, "refreshed, refreshWhy := s.liveMirrorExecutable")
	releaseAt := -1
	if quoteAt >= 0 {
		releaseAt = strings.Index(handler[quoteAt:], "s.r148ReleaseRisk")
	}
	submitAt := strings.Index(handler, "storage.LivePendingRiskSubmitStarted")
	wireQuoteAt := strings.Index(handler, "wireQuote, wireWhy := s.liveMirrorExecutable")
	cleanRejectAt := -1
	if wireQuoteAt >= 0 {
		if relative := strings.Index(handler[wireQuoteAt:], "s.r154RejectKalshiNoSendRisk"); relative >= 0 {
			cleanRejectAt = wireQuoteAt + relative
		}
	}
	createAt := strings.Index(handler, "s.kal.CreateOrder")
	if reserveAt < 0 || quoteAt <= reserveAt || releaseAt < 0 ||
		wireQuoteAt <= quoteAt || cleanRejectAt <= wireQuoteAt || submitAt <= cleanRejectAt ||
		createAt <= submitAt {
		t.Fatalf("quote no-send ordering changed: reserve=%d quote=%d releaseAfterQuote=%d submit=%d wire=%d cleanAfterWire=%d create=%d",
			reserveAt, quoteAt, releaseAt, submitAt, wireQuoteAt, cleanRejectAt, createAt)
	}
	reserveEnd := strings.Index(handler[reserveAt:], "if riskErr == nil")
	if reserveEnd < 0 || strings.Contains(handler[reserveAt:reserveAt+reserveEnd], "account_resnapshot") {
		t.Fatal("dynamic refresh telemetry entered the immutable reservation hash/idempotency proof")
	}

	s, st := r148RiskTestServer(t)
	intent := r148RiskTestReservation(t, st, "risk-r156-quote-change")
	prior := liveMirrorQuote{Price: .40, Depth: 5, Maker: false}
	changed := liveMirrorQuote{Price: .41, Depth: 5, Maker: false}
	if why := liveMirrorHandlerQuoteChangeReason(prior, changed, 1); why == "" {
		t.Fatal("changed executable quote did not refuse")
	}
	if err := s.r154RejectKalshiNoSendRisk(context.Background(), intent.ReservationID,
		"wire-book-recheck", "final WS executable quote changed; venue was not called", nil); err != nil {
		t.Fatal(err)
	}
	active, err := st.ActiveLivePendingRisk(context.Background())
	if err != nil || len(active) != 0 {
		t.Fatalf("quote-change no-send retained risk: active=%d err=%v", len(active), err)
	}
}
