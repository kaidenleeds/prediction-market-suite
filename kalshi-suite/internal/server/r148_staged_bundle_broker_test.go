package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r148TestKalshiClient(t *testing.T) *kalshi.Client {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	raw := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	signer, err := kalshi.NewSigner("r148-test", raw)
	if err != nil {
		t.Fatal(err)
	}
	c := kalshi.NewClient(kalshi.BaseDemo, signer, 1000, time.Second)
	c.ArmProdWrites()
	return c
}

func r148ArmStagedTestServer(t *testing.T, system string, legs []storage.ResearchRouteBundleLeg) *Server {
	t.Helper()
	s := testServer(t)
	s.kal = r148TestKalshiClient(t)
	allowed := make([]string, 0, len(legs))
	seen := map[string]bool{}
	for _, leg := range legs {
		key := "kalshi|" + system + "|" + leg.Side + "|taker"
		if !seen[key] {
			allowed, seen[key] = append(allowed, key), true
		}
	}
	s.mutateCfg(func(c *config.Config) {
		c.Auto.ParlayEnabled = true
		c.Risk.LiveStagedBundles = true
		c.Risk.LiveStagedBundleAllowlist = strings.Join(allowed, ",")
	})
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	s.kalshiExecutableBookFn = func(liveMirrorCandidate) (r159KalshiExecutableBook, string) {
		return r159KalshiExecutableBook{
			MakerPrice: .39, MakerDepth: 100, MakerTick: .01,
			TakerPrice: .40, TakerDepth: 100, TakerTick: .01,
			Source: "r148-resident-test-book",
		}, ""
	}
	return s
}

func TestR148KalshiFOKRequestMapsEveryOutcomeAndUnwind(t *testing.T) {
	rows := []struct {
		side, action, book, price string
		reduce                    bool
	}{
		{"YES", "BUY", "bid", "0.40", false},
		{"NO", "BUY", "ask", "0.60", false},
		{"YES", "SELL", "ask", "0.40", true},
		{"NO", "SELL", "bid", "0.60", true},
	}
	for _, row := range rows {
		leg := storage.ResearchRouteBundleLeg{Venue: "kalshi", Ticker: "KX-R148", Side: row.side}
		req, err := r148KalshiFOKRequest(leg, row.action, 1, .40, row.side+row.action)
		if err != nil || req.Side != row.book || req.Price != row.price || req.ReduceOnly != row.reduce ||
			req.TimeInForce != "fill_or_kill" || req.PostOnly {
			t.Fatalf("%s/%s request=%+v err=%v", row.side, row.action, req, err)
		}
	}
}

func TestR148StagedBrokerRevalidatesIdentityImmediatelyBeforeWrite(t *testing.T) {
	leg := storage.ResearchRouteBundleLeg{Index: 0, Venue: "kalshi", Ticker: "KX-R148", Side: "YES", Quantity: 1}
	bundle := storage.ResearchRouteBundle{BundleID: "bundle", SystemID: "time-nested-lock",
		CertificateStatus: "verified", CertificateHash: "hash", EventVersion: 1, Legs: []storage.ResearchRouteBundleLeg{leg}}
	s := r148ArmStagedTestServer(t, bundle.SystemID, bundle.Legs)
	writes := 0
	broker := newR148ServerStagedBroker(s)
	broker.now = func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) }
	broker.identityFn = func(context.Context, storage.ResearchRouteBundle) error {
		return errors.New("current rule certificate changed")
	}
	broker.feeFn = func(string, bool, float64, float64, bool) (float64, bool, string) {
		return .01, true, "test-exact-fee"
	}
	broker.createFn = func(context.Context, kalshi.OrderRequest) (*kalshi.CreateOrderResult, error) {
		writes++
		return &kalshi.CreateOrderResult{OrderID: "must-not-write"}, nil
	}
	_, err := broker.SubmitFOK(context.Background(), bundle, leg, "BUY", 1, .40, "identity-change")
	if err == nil || !strings.Contains(err.Error(), "certificate changed") || writes != 0 {
		t.Fatalf("err=%v writes=%d", err, writes)
	}
}

func TestR148StagedBrokerUsesAuthoritativeOrderScopedFill(t *testing.T) {
	leg := storage.ResearchRouteBundleLeg{Index: 0, Venue: "kalshi", Ticker: "KX-R148", Side: "YES", Quantity: 1}
	bundle := storage.ResearchRouteBundle{BundleID: "bundle-fill", SystemID: "time-nested-lock",
		CertificateStatus: "verified", CertificateHash: "hash", EventVersion: 1, Legs: []storage.ResearchRouteBundleLeg{leg}}
	s := r148ArmStagedTestServer(t, bundle.SystemID, bundle.Legs)
	broker := newR148ServerStagedBroker(s)
	broker.now = func() time.Time { return time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC) }
	broker.identityFn = func(context.Context, storage.ResearchRouteBundle) error { return nil }
	broker.feeFn = func(string, bool, float64, float64, bool) (float64, bool, string) {
		return .01, true, "test-exact-fee"
	}
	broker.createFn = func(_ context.Context, req kalshi.OrderRequest) (*kalshi.CreateOrderResult, error) {
		if req.TimeInForce != "fill_or_kill" || req.Side != "bid" {
			t.Fatalf("request=%+v", req)
		}
		return &kalshi.CreateOrderResult{OrderID: "order-r148"}, nil
	}
	broker.orderFn = func(context.Context, string) (kalshi.Order, error) {
		var order kalshi.Order
		if err := json.Unmarshal([]byte(`{"order_id":"order-r148","ticker":"KX-R148","status":"executed","type":"limit","outcome_side":"yes","book_side":"bid","initial_count_fp":"1","fill_count_fp":"1"}`), &order); err != nil {
			t.Fatal(err)
		}
		return order, nil
	}
	broker.fillsFn = func(context.Context, string) ([]kalshi.Fill, error) {
		var fill kalshi.Fill
		if err := json.Unmarshal([]byte(`{"fill_id":"f","order_id":"order-r148","ticker":"KX-R148","count_fp":"1.00","yes_price_dollars":"0.40","no_price_dollars":"0.60","fee_cost":"0.01","outcome_side":"yes","book_side":"bid"}`), &fill); err != nil {
			t.Fatal(err)
		}
		return []kalshi.Fill{fill}, nil
	}
	receipt, err := broker.SubmitFOK(context.Background(), bundle, leg, "BUY", 1, .40, "authoritative-fill")
	if err != nil || receipt.State != r148ReceiptFilled || !receipt.Authoritative ||
		receipt.FilledQty != 1 || receipt.AveragePrice != .40 || receipt.FeeTotal != .01 {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
	s.liveMu.Lock()
	spent := s.liveDaySpendV["kalshi"]
	s.liveMu.Unlock()
	if math.Abs(spent-.41) > 1e-9 {
		t.Fatalf("fee-inclusive risk booking=%v want .41", spent)
	}
	// A bounded unwind is risk-reducing. It must not be booked as new BUY exposure/turnover.
	sell := newR148ServerStagedBroker(s)
	sell.now, sell.identityFn, sell.feeFn = broker.now, broker.identityFn, broker.feeFn
	sell.createFn = func(context.Context, kalshi.OrderRequest) (*kalshi.CreateOrderResult, error) {
		return &kalshi.CreateOrderResult{OrderID: "order-r148-sell"}, nil
	}
	sell.orderFn = func(context.Context, string) (kalshi.Order, error) {
		var order kalshi.Order
		if err := json.Unmarshal([]byte(`{"order_id":"order-r148-sell","ticker":"KX-R148","status":"executed","type":"limit","outcome_side":"no","book_side":"ask","initial_count_fp":"1","fill_count_fp":"1"}`), &order); err != nil {
			t.Fatal(err)
		}
		return order, nil
	}
	sell.fillsFn = func(context.Context, string) ([]kalshi.Fill, error) {
		var fill kalshi.Fill
		if err := json.Unmarshal([]byte(`{"fill_id":"f2","order_id":"order-r148-sell","ticker":"KX-R148","count_fp":"1.00","yes_price_dollars":"0.39","no_price_dollars":"0.61","fee_cost":"0.01","outcome_side":"no","book_side":"ask"}`), &fill); err != nil {
			t.Fatal(err)
		}
		return []kalshi.Fill{fill}, nil
	}
	if receipt, err := sell.SubmitFOK(context.Background(), bundle, leg, "SELL", 1, .39, "authoritative-sell"); err != nil || receipt.State != r148ReceiptFilled {
		t.Fatalf("sell receipt=%+v err=%v", receipt, err)
	}
	s.liveMu.Lock()
	afterSell := s.liveDaySpendV["kalshi"]
	s.liveMu.Unlock()
	if math.Abs(afterSell-spent) > 1e-9 {
		t.Fatalf("risk-reducing SELL changed buy-risk booking: before=%v after=%v", spent, afterSell)
	}
}

func TestR148KalshiIntentRecoveryUsesCurrentOutcomeAndBookSides(t *testing.T) {
	for _, tc := range []struct {
		side, action, outcome, book string
	}{
		{"YES", "BUY", "yes", "bid"},
		{"NO", "BUY", "no", "ask"},
		{"YES", "SELL", "no", "ask"},
		{"NO", "SELL", "yes", "bid"},
	} {
		t.Run(tc.side+"-"+tc.action, func(t *testing.T) {
			leg := storage.ResearchRouteBundleLeg{Index: 0, Venue: "kalshi", Ticker: "KX-R148", Side: tc.side, Quantity: 1}
			bundle := storage.ResearchRouteBundle{BundleID: "bundle-recovery", SystemID: "time-nested-lock", Legs: []storage.ResearchRouteBundleLeg{leg}}
			b := &r148ServerStagedBroker{}
			idem := r148Hash(map[string]any{"execution": "e", "leg": 0, "action": tc.action})
			clientID := idemKey("stg-", idem)
			b.clientOrdersFn = func(_ context.Context, ticker, gotClientID string, minTS time.Time) ([]kalshi.Order, error) {
				if ticker != leg.Ticker || gotClientID != clientID || minTS.IsZero() {
					t.Fatalf("lookup ticker=%q client=%q min=%v", ticker, gotClientID, minTS)
				}
				var order kalshi.Order
				raw := `{"order_id":"recovered-order","client_order_id":"` + clientID +
					`","ticker":"KX-R148","status":"canceled","type":"limit","outcome_side":"` + tc.outcome +
					`","book_side":"` + tc.book + `","initial_count_fp":"1","fill_count_fp":"0"}`
				if err := json.Unmarshal([]byte(raw), &order); err != nil {
					t.Fatal(err)
				}
				return []kalshi.Order{order}, nil
			}
			b.fillsFn = func(context.Context, string) ([]kalshi.Fill, error) { return []kalshi.Fill{}, nil }
			r, err := b.ReconcileIntent(context.Background(), bundle, leg, tc.action, idem, time.Now().UTC())
			if err != nil || r.State != r148ReceiptUnfilled || !r.Authoritative || r.OrderID != "recovered-order" {
				t.Fatalf("receipt=%+v err=%v", r, err)
			}
		})
	}
}

func TestR148KalshiIntentRecoveryJoinsRecoveredOrderToExactFill(t *testing.T) {
	leg := storage.ResearchRouteBundleLeg{Index: 0, Venue: "kalshi", Ticker: "KX-R148", Side: "YES", Quantity: 1}
	bundle := storage.ResearchRouteBundle{BundleID: "bundle-recovery-fill", SystemID: "time-nested-lock", Legs: []storage.ResearchRouteBundleLeg{leg}}
	b := &r148ServerStagedBroker{}
	idem := r148Hash(map[string]any{"execution": "filled", "leg": 0, "action": "BUY"})
	clientID := idemKey("stg-", idem)
	b.clientOrdersFn = func(context.Context, string, string, time.Time) ([]kalshi.Order, error) {
		var order kalshi.Order
		raw := `{"order_id":"recovered-fill","client_order_id":"` + clientID +
			`","ticker":"KX-R148","status":"executed","type":"limit","outcome_side":"yes","book_side":"bid","initial_count_fp":"1","fill_count_fp":"1"}`
		if err := json.Unmarshal([]byte(raw), &order); err != nil {
			t.Fatal(err)
		}
		return []kalshi.Order{order}, nil
	}
	b.fillsFn = func(context.Context, string) ([]kalshi.Fill, error) {
		var fill kalshi.Fill
		if err := json.Unmarshal([]byte(`{"fill_id":"f","order_id":"recovered-fill","ticker":"KX-R148","count_fp":"1","yes_price_dollars":"0.40","no_price_dollars":"0.60","fee_cost":"0.01","outcome_side":"yes","book_side":"bid"}`), &fill); err != nil {
			t.Fatal(err)
		}
		return []kalshi.Fill{fill}, nil
	}
	r, err := b.ReconcileIntent(context.Background(), bundle, leg, "BUY", idem, time.Now().UTC())
	if err != nil || r.State != r148ReceiptFilled || !r.Authoritative || r.FilledQty != 1 || r.AveragePrice != .40 || r.FeeTotal != .01 {
		t.Fatalf("receipt=%+v err=%v", r, err)
	}
}

func TestR148KalshiReceiptRejectsDirectionCountAndExecutedZeroDrift(t *testing.T) {
	leg := storage.ResearchRouteBundleLeg{Venue: "kalshi", Ticker: "KX-R148", Side: "YES", Quantity: 1}
	orderFrom := func(raw string) kalshi.Order {
		var order kalshi.Order
		if err := json.Unmarshal([]byte(raw), &order); err != nil {
			t.Fatal(err)
		}
		return order
	}
	fillFrom := func(raw string) kalshi.Fill {
		var fill kalshi.Fill
		if err := json.Unmarshal([]byte(raw), &fill); err != nil {
			t.Fatal(err)
		}
		return fill
	}
	baseFill := `{"fill_id":"f","order_id":"o","ticker":"KX-R148","count_fp":"1","yes_price_dollars":"0.40","no_price_dollars":"0.60","fee_cost":"0.01","outcome_side":"yes","book_side":"bid"}`
	for _, tc := range []struct {
		name  string
		order kalshi.Order
		fills []kalshi.Fill
	}{
		{"wrong-fill-side", orderFrom(`{"order_id":"o","ticker":"KX-R148","status":"executed","type":"limit","outcome_side":"yes","book_side":"bid","initial_count_fp":"1","fill_count_fp":"1"}`),
			[]kalshi.Fill{fillFrom(strings.Replace(baseFill, `"outcome_side":"yes"`, `"outcome_side":"no"`, 1))}},
		{"fill-count-disagrees", orderFrom(`{"order_id":"o","ticker":"KX-R148","status":"executed","type":"limit","outcome_side":"yes","book_side":"bid","initial_count_fp":"1","fill_count_fp":"0"}`),
			[]kalshi.Fill{fillFrom(baseFill)}},
		{"executed-zero", orderFrom(`{"order_id":"o","ticker":"KX-R148","status":"executed","type":"limit","outcome_side":"yes","book_side":"bid","initial_count_fp":"1","fill_count_fp":"0"}`), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := r148KalshiReceipt(tc.order, tc.fills, leg, "BUY")
			if r.State != r148ReceiptAmbiguous || r.Authoritative {
				t.Fatalf("receipt=%+v", r)
			}
		})
	}
}

func TestR148NormalKalshiReconcileRejectsWrongCurrentDirectionBeforeReadingFills(t *testing.T) {
	leg := storage.ResearchRouteBundleLeg{Venue: "kalshi", Ticker: "KX-R148", Side: "YES", Quantity: 1}
	b := &r148ServerStagedBroker{}
	b.orderFn = func(context.Context, string) (kalshi.Order, error) {
		var order kalshi.Order
		if err := json.Unmarshal([]byte(`{"order_id":"o","ticker":"KX-R148","status":"canceled","type":"limit","outcome_side":"no","book_side":"ask","initial_count_fp":"1","fill_count_fp":"0"}`), &order); err != nil {
			t.Fatal(err)
		}
		return order, nil
	}
	fillReads := 0
	b.fillsFn = func(context.Context, string) ([]kalshi.Fill, error) {
		fillReads++
		return nil, nil
	}
	if _, err := b.Reconcile(context.Background(), leg, "BUY", "o"); err == nil || fillReads != 0 {
		t.Fatalf("wrong-side order err=%v fill_reads=%d", err, fillReads)
	}
}

func TestR148StagedBrokerDurablyBlocksPolyUSBeforeIntent(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) { c.Risk.LiveStagedBundles = true })
	bundle := r148CoordinatorBundle()
	bundle.BundleID = "pus-staged-block"
	bundle.Legs[0].Venue = "polyus"
	broker := newR148ServerStagedBroker(s)
	reason := r148PUSStagedBlockReason
	_, err := broker.Admission(context.Background(), bundle, storage.StagedBundleExecutionProof{})
	if err == nil || !strings.Contains(err.Error(), "intent-only recovery") {
		t.Fatalf("err=%v", err)
	}
	key := "r148_staged_block_" + r148Hash(map[string]string{"bundle": bundle.BundleID, "reason": reason})[:20]
	if raw, ok := s.store.KVGet(context.Background(), key); !ok || strings.TrimSpace(raw) == "" {
		t.Fatalf("durable block missing key=%s raw=%q", key, raw)
	}
}

func TestR148StagedAdmissionRejectsMissingOrWorseCurrentUnwind(t *testing.T) {
	tests := []struct {
		name string
		sell r148StagedQuote
		err  error
		want string
	}{
		{name: "missing sell book", err: errors.New("sell book unavailable"), want: "current unwind leg"},
		{name: "missing exact tick authority", sell: r148StagedQuote{Price: .39, Available: 10,
			Fee: .01, Source: "current-book+exact-fee"}, want: "depth/tick/fee"},
		{name: "worse than certified envelope", sell: r148StagedQuote{Price: .35, Available: 10,
			Fee: .01, Tick: .01, Source: "current-book+exact-fee"}, want: "worse than certified bound"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := testServer(t)
			s.mutateCfg(func(c *config.Config) { c.Risk.LiveStagedBundles = true })
			bundle := r148CoordinatorBundle()
			broker := newR148ServerStagedBroker(s)
			broker.boundaryFn = func(context.Context, storage.ResearchRouteBundle,
				*storage.ResearchRouteBundleLeg, string, bool) string {
				return ""
			}
			broker.quoteFn = func(_ context.Context, _ storage.ResearchRouteBundleLeg,
				action string, _ float64) (r148StagedQuote, error) {
				if action == "SELL" {
					return tc.sell, tc.err
				}
				return r148StagedQuote{Price: .40, Available: 10, Fee: .01, Tick: .01,
					Source: "current-book+exact-fee"}, nil
			}
			if _, err := broker.Admission(context.Background(), bundle,
				storage.StagedBundleExecutionProof{}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("admission err=%v want substring %q", err, tc.want)
			}
		})
	}
}

func TestR148StagedGateRechecksEveryCurrentUnwindBeforeEachBuy(t *testing.T) {
	s := testServer(t)
	bundle := r148CoordinatorBundle()
	broker := newR148ServerStagedBroker(s)
	broker.boundaryFn = func(context.Context, storage.ResearchRouteBundle,
		*storage.ResearchRouteBundleLeg, string, bool) string {
		return ""
	}
	sellReads := 0
	broker.quoteFn = func(_ context.Context, leg storage.ResearchRouteBundleLeg,
		action string, _ float64) (r148StagedQuote, error) {
		if action == "SELL" {
			sellReads++
			if leg.Index == 1 {
				return r148StagedQuote{}, errors.New("second leg exit disappeared")
			}
			return r148StagedQuote{Price: .39, Available: 10, Fee: .01, Tick: .01,
				Source: "current-book+exact-fee"}, nil
		}
		return r148StagedQuote{Price: .40, Available: 10, Fee: .01, Tick: .01,
			Source: "current-book+exact-fee"}, nil
	}
	prior := r148StagedQuote{Price: .40, Available: 10, Fee: .01, Tick: .01,
		Source: "prior-current-book+exact-fee"}
	if err := broker.Gate(context.Background(), bundle, bundle.Legs[0], "BUY", prior); err == nil ||
		!strings.Contains(err.Error(), "second leg exit disappeared") || sellReads != 2 {
		t.Fatalf("gate err=%v sell_reads=%d", err, sellReads)
	}
}

func TestR148StagedSizingCompoundsFreshDepositAndPreservesWholePackageRatios(t *testing.T) {
	bundle := r148CoordinatorBundle()
	s := testServer(t)
	s.liveBankArm = map[string]float64{"kalshi": 100}
	s.maxContracts = 1000
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveKellyMaxFrac = .50
		c.Risk.LiveMaxOrderPct, c.Risk.LiveMaxOrderUSD = .05, -1
		c.Risk.LiveExposureCapPct, c.Risk.LiveExposureCapUSD = .50, -1
		c.Risk.LiveClusterCapPct = .10
		c.Auto.MinEVPerContract = .03
	})
	nav := 100.0
	b := newR148ServerStagedBroker(s)
	b.boundaryFn = func(context.Context, storage.ResearchRouteBundle, *storage.ResearchRouteBundleLeg, string, bool) string {
		return ""
	}
	b.navFn = func(context.Context, string) (float64, string) { return nav, "authenticated-test-nav" }
	b.headroomFn = func(context.Context, string) (float64, error) { return 1000, nil }
	b.clusterFn = func(context.Context, storage.ResearchRouteBundle, float64) (float64, error) { return 1000, nil }
	b.quoteFn = func(_ context.Context, _ storage.ResearchRouteBundleLeg, action string, qty float64) (r148StagedQuote, error) {
		if action == "SELL" {
			return r148StagedQuote{Price: .39, Available: 100, Fee: .01 * qty,
				Tick: .01, Source: "authenticated-current-unwind+exact-fee"}, nil
		}
		if action != "BUY" {
			return r148StagedQuote{}, errors.New("unexpected action")
		}
		return r148StagedQuote{Price: .40, Available: 100, Fee: .01 * qty,
			Tick: .01, Source: "authenticated-current-book+exact-fee"}, nil
	}
	proof := storage.StagedBundleExecutionProof{Mean: .18, LowerBound: .18}
	sized100, plan100, err := b.SizeCurrent(context.Background(), bundle, proof)
	if err != nil || plan100.PackageMultiplier != 6 || sized100.Size != 6 ||
		sized100.Legs[0].Quantity != 6 || sized100.Legs[1].Quantity != 6 ||
		math.Abs(plan100.Cost+plan100.Fee-4.92) > 1e-9 {
		t.Fatalf("$100 sizing bundle=%+v plan=%+v err=%v", sized100, plan100, err)
	}
	// A deposit changes current authenticated NAV but not the immutable $100 ARM drawdown
	// reference.  The 5%-NAV order rail therefore doubles the whole-package allocation.
	nav = 200
	sized200, plan200, err := b.SizeCurrent(context.Background(), bundle, proof)
	if err != nil || plan200.PackageMultiplier != 12 || sized200.Size != 12 ||
		sized200.Legs[0].Quantity != 12 || sized200.Legs[1].Quantity != 12 ||
		plan200.ArmBaseline != 100 || plan200.NAV != 200 || plan200.TargetUSD != 10 {
		t.Fatalf("deposit sizing bundle=%+v plan=%+v err=%v", sized200, plan200, err)
	}
}

func TestR148StagedSizingFailsClosedOnStaleAuthenticatedBalance(t *testing.T) {
	bundle := r148CoordinatorBundle()
	s := testServer(t)
	s.liveBankArm = map[string]float64{"kalshi": 100}
	b := newR148ServerStagedBroker(s)
	b.boundaryFn = func(context.Context, storage.ResearchRouteBundle, *storage.ResearchRouteBundleLeg, string, bool) string {
		return ""
	}
	b.navFn = func(context.Context, string) (float64, string) { return 0, "unknown" }
	b.quoteFn = func(context.Context, storage.ResearchRouteBundleLeg, string, float64) (r148StagedQuote, error) {
		t.Fatal("stale NAV must fail before a book can influence sizing")
		return r148StagedQuote{}, nil
	}
	_, _, err := b.SizeCurrent(context.Background(), bundle,
		storage.StagedBundleExecutionProof{Mean: .18, LowerBound: .18})
	if err == nil || !strings.Contains(err.Error(), "authenticated Kalshi NAV") {
		t.Fatalf("stale balance error=%v", err)
	}
}

func TestR148StagedAllowlistIsSeparateExactAndBounded(t *testing.T) {
	got, why := parseR148StagedBundleAllowlist("kalshi|event-basket-lock|NO|taker;kalshi|nested-ladder-lock|YES|taker")
	if why != "" || len(got) != 2 {
		t.Fatalf("exact staged allowlist=%v why=%q", got, why)
	}
	for _, bad := range []string{
		"kalshi|*|YES|taker", "polyus|xvlock|YES|taker", "kalshi|time-nested-lock|YES|maker",
	} {
		if _, why := parseR148StagedBundleAllowlist(bad); why == "" {
			t.Fatalf("invalid staged allowlist escaped: %q", bad)
		}
	}
}
