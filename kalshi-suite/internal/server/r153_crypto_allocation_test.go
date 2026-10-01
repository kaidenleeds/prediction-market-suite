package server

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR153KalshiRestingRiskUsesOnlySuccessorDirectionAndSidePrice(t *testing.T) {
	tests := []struct {
		name, wire string
		want       float64
		known      bool
	}{
		{"YES ignores contradictory legacy side", `{"order_id":"y","ticker":"KXBTC","side":"ask","outcome_side":"yes","book_side":"bid","yes_price_dollars":"0.40","no_price_dollars":"0.60","remaining_count_fp":"2"}`, .80, true},
		{"NO ignores contradictory legacy side", `{"order_id":"n","ticker":"KXBTC","side":"bid","outcome_side":"no","book_side":"ask","yes_price_dollars":"0.40","no_price_dollars":"0.60","remaining_count_fp":"2"}`, 1.20, true},
		{"missing successor direction", `{"order_id":"missing","ticker":"KXBTC","side":"bid","yes_price_dollars":"0.40","remaining_count_fp":"2"}`, 0, false},
		{"contradictory successor direction", `{"order_id":"conflict","ticker":"KXBTC","outcome_side":"yes","book_side":"ask","yes_price_dollars":"0.40","remaining_count_fp":"2"}`, 0, false},
		{"missing selected side price", `{"order_id":"price","ticker":"KXBTC","outcome_side":"no","book_side":"ask","yes_price_dollars":"0.40","remaining_count_fp":"2"}`, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var order kalshi.Order
			if err := json.Unmarshal([]byte(tc.wire), &order); err != nil {
				t.Fatal(err)
			}
			got, known := kalshiRestingRisk(order)
			if known != tc.known || math.Abs(got-tc.want) > 1e-12 {
				t.Fatalf("risk=%v known=%v want %v/%v", got, known, tc.want, tc.known)
			}
		})
	}
}

func TestR153CryptoClassificationCoversSidesAndRejectsMismatchedRoutes(t *testing.T) {
	for _, side := range []string{"YES", "NO"} {
		c := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXBTC15M-TEST", Side: side, Family: "xmatch"}
		if got := liveCryptoCandidateClass(c); got != liveCryptoYes {
			t.Fatalf("xmatch %s class=%v want crypto", side, got)
		}
	}
	if got := liveCryptoCandidateClass(liveMirrorCandidate{Platform: "kalshi", Ticker: "KXNBAGAME-TEST",
		Side: "YES", Family: "favorite-long"}); got != liveCryptoNo {
		t.Fatalf("sports class=%v want non-crypto", got)
	}
	if got := liveCryptoCandidateClass(liveMirrorCandidate{Platform: "kalshi", Ticker: "KXNBAGAME-TEST",
		Side: "YES", Family: "xmatch"}); got != liveCryptoUnknown {
		t.Fatalf("crypto-only route on sports class=%v want unknown", got)
	}
	if got := liveCryptoCandidateClass(liveMirrorCandidate{Platform: "kalshi", Side: "YES"}); got != liveCryptoUnknown {
		t.Fatalf("blank instrument class=%v want unknown", got)
	}
}

func TestR153HeldAndRestingExposureUseOneCryptoAccumulator(t *testing.T) {
	total, crypto, known := 0.0, 0.0, true
	total, crypto, known = liveCryptoAddRisk(total, crypto, known, 12, liveCryptoYes) // held crypto
	total, crypto, known = liveCryptoAddRisk(total, crypto, known, 5, liveCryptoNo)   // held non-crypto
	total, crypto, known = liveCryptoAddRisk(total, crypto, known, 3, liveCryptoYes)  // resting crypto
	if total != 20 || crypto != 15 || !known {
		t.Fatalf("account breakdown total=%v crypto=%v known=%v", total, crypto, known)
	}
	_, _, known = liveCryptoAddRisk(total, crypto, known, 1, liveCryptoUnknown)
	if known {
		t.Fatal("unknown held/resting classification did not poison crypto certainty")
	}
}

func pendingCryptoRow(side string, cost, qty, price float64) storage.LivePendingRiskReservation {
	return storage.LivePendingRiskReservation{
		Intent: storage.LivePendingRiskIntent{ReservationID: "risk-" + side, CostUSD: cost, SystemID: "spotlag"},
		Legs: []storage.LivePendingRiskLeg{{ReservationID: "risk-" + side, Index: 0, Venue: "kalshi",
			Ticker: "KXBTC15M-PENDING", Side: side, Action: "BUY", Quantity: qty, LimitPrice: price}},
	}
}

func TestR153PendingCryptoCountsYESNOAndSubtractsRestingExactly(t *testing.T) {
	for _, side := range []string{"YES", "NO"} {
		row := pendingCryptoRow(side, 4, 10, .4)
		got, known := livePendingCryptoOverlay([]storage.LivePendingRiskReservation{row}, nil, "kalshi")
		if !known || math.Abs(got-4) > 1e-12 {
			t.Fatalf("%s pending got=%v known=%v want 4,true", side, got, known)
		}
	}

	row := pendingCryptoRow("YES", 4, 10, .4)
	row.Events = []storage.LivePendingRiskEvent{{EventType: storage.LivePendingRiskAck, OrderID: "order-1"}}
	resting := map[string]float64{liveCryptoRestingKey("kalshi", "order-1"): 1.5}
	got, known := livePendingCryptoOverlay([]storage.LivePendingRiskReservation{row}, resting, "kalshi")
	if !known || math.Abs(got-2.5) > 1e-12 {
		t.Fatalf("resting subtraction got=%v known=%v want 2.5,true", got, known)
	}
	row.Events = append(row.Events, storage.LivePendingRiskEvent{EventType: storage.LivePendingRiskAccountVisible})
	got, known = livePendingCryptoOverlay([]storage.LivePendingRiskReservation{row}, resting, "kalshi")
	if !known || got != 0 {
		t.Fatalf("account-visible pending got=%v known=%v want 0,true", got, known)
	}
}

func TestR153PendingCryptoCrossVenueAllocationReleaseAndUnknown(t *testing.T) {
	row := storage.LivePendingRiskReservation{
		Intent: storage.LivePendingRiskIntent{ReservationID: "bundle", CostUSD: 10, SystemID: "event-basket-lock"},
		Legs: []storage.LivePendingRiskLeg{
			{ReservationID: "bundle", Index: 0, Venue: "kalshi", Ticker: "KXBTC15M-A", Side: "YES", Action: "BUY", Quantity: 10, LimitPrice: .4},
			{ReservationID: "bundle", Index: 1, Venue: "polyus", Ticker: "eth-up-or-down", Side: "NO", Action: "BUY", Quantity: 10, LimitPrice: .6},
		},
	}
	k, kk := livePendingCryptoOverlay([]storage.LivePendingRiskReservation{row}, nil, "kalshi")
	p, pk := livePendingCryptoOverlay([]storage.LivePendingRiskReservation{row}, nil, "polyus")
	all, ak := livePendingCryptoOverlay([]storage.LivePendingRiskReservation{row}, nil, "")
	if !kk || !pk || !ak || math.Abs(k-4) > 1e-12 || math.Abs(p-6) > 1e-12 ||
		math.Abs(all-10) > 1e-12 || math.Abs(k+p-all) > 1e-12 {
		t.Fatalf("cross-venue pending K=%v/%v P=%v/%v all=%v/%v", k, kk, p, pk, all, ak)
	}
	// Unequal package fees are allocated by the same principal ratio at admission and pending.
	// $4 crypto principal + $6 sport principal + $5 total fees => $6 crypto sleeve charge.
	row.Intent.CostUSD = 15
	row.Legs[1].Ticker = "basketball-game-winner"
	if admission, known := liveCryptoProportionalCost(15, 10, 4); !known || admission != 6 {
		t.Fatalf("unequal-fee admission allocation=%v/%v want 6,true", admission, known)
	}
	if k, known := livePendingCryptoOverlay([]storage.LivePendingRiskReservation{row}, nil, "kalshi"); !known || k != 6 {
		t.Fatalf("unequal-fee pending allocation=%v/%v want 6,true", k, known)
	}
	if p, known := livePendingCryptoOverlay([]storage.LivePendingRiskReservation{row}, nil, "polyus"); !known || p != 0 {
		t.Fatalf("non-crypto staged leg allocation=%v/%v want 0,true", p, known)
	}
	row.Events = []storage.LivePendingRiskEvent{{EventType: storage.LivePendingRiskReleased}}
	if got, known := livePendingCryptoOverlay([]storage.LivePendingRiskReservation{row}, nil, ""); !known || got != 0 {
		t.Fatalf("released bundle got=%v known=%v", got, known)
	}
	bad := pendingCryptoRow("YES", 1, 1, .5)
	bad.Legs[0].Ticker = ""
	if _, known := livePendingCryptoOverlay([]storage.LivePendingRiskReservation{bad}, nil, "kalshi"); known {
		t.Fatal("unknown pending instrument did not fail closed")
	}
}

func TestR153PendingComboUsesDurableUnderlyingLegsAndFullProductCharge(t *testing.T) {
	row := storage.LivePendingRiskReservation{
		Intent: storage.LivePendingRiskIntent{ReservationID: "combo", Product: "combo", CostUSD: 12,
			SystemID: "combo", ProofJSON: `{"semantic_legs":[{"ticker":"KXBTC15M-COMBO","side":"YES"},{"ticker":"KXNBAGAME-COMBO","side":"NO"}]}`},
		Legs: []storage.LivePendingRiskLeg{{ReservationID: "combo", Venue: "kalshi", Ticker: "KXMVE-COMBO",
			Side: "YES", Action: "BUY", Quantity: 20, LimitPrice: .6}},
	}
	if got, known := livePendingCryptoOverlay([]storage.LivePendingRiskReservation{row}, nil, "kalshi"); !known || got != 12 {
		t.Fatalf("pending combo charge=%v known=%v want 12,true", got, known)
	}
	row.Events = []storage.LivePendingRiskEvent{{EventType: storage.LivePendingRiskAccountVisible}}
	if got, known := livePendingCryptoOverlay([]storage.LivePendingRiskReservation{row}, nil, "kalshi"); !known || got != 0 {
		t.Fatalf("account-visible combo overlay=%v known=%v want held account to own attribution", got, known)
	}
	row.Intent.ProofJSON = `{}`
	if _, known := livePendingCryptoOverlay([]storage.LivePendingRiskReservation{row}, nil, "kalshi"); known {
		t.Fatal("opaque combo without durable semantic legs did not fail closed")
	}
}

func TestR153HeldKXMVEPositionRetainsFullCryptoAttribution(t *testing.T) {
	s, venue := newR153SemanticServer(t)
	const comboTicker = "KXMVE-R153-CRYPTO-MIX"
	s.liveMu.Lock()
	s.comboLegReg = map[string][]kalshi.MVELeg{comboTicker: {
		{MarketTicker: "KXBTC15M-R153", EventTicker: "KXBTC15M-R153-EVENT", Side: "yes"},
		{MarketTicker: r153BKNWinner, EventTicker: r153GameEvent, Side: "yes"},
	}}
	s.liveMu.Unlock()
	venue.account([]map[string]any{{"ticker": comboTicker, "position_fp": "1.00",
		"market_exposure_dollars": "5.00", "last_updated_ts": time.Now().UTC().Format(time.RFC3339Nano)}}, nil)
	exp, crypto, known, err := s.liveVenueExposureBreakdownUSD(context.Background(), "kalshi")
	if err != nil || !known || exp != 5 || crypto != 5 {
		t.Fatalf("held mixed KXMVE exp=%v crypto=%v known=%v err=%v", exp, crypto, known, err)
	}
}

func TestR153ActiveKalshiOrderWithoutCurrentRiskTruthFailsClosed(t *testing.T) {
	s, venue := newR153SemanticServer(t)
	s.kal.ArmProdWrites()
	venue.account(nil, []map[string]any{{
		"order_id": "malformed-active", "ticker": "KXBTC15M-MALFORMED", "status": "resting",
		"type": "limit", "side": "bid", "yes_price_dollars": "0.40", "remaining_count_fp": "2",
	}})
	if _, _, known, err := s.liveVenueExposureBreakdownUSD(context.Background(), "kalshi"); err == nil || known {
		t.Fatalf("malformed active order did not fail closed: known=%v err=%v", known, err)
	}
}

func TestR153CryptoCapUsesEnabledCapitalAndBoundary(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveCryptoCapPct = .50
		c.Risk.LiveExposureCapPct = .50
		c.Risk.LiveExposureCapUSD = -1
		c.Risk.LiveProspectiveAllocation = true
		c.Risk.LiveSystemPolyUS = false
		c.Risk.LiveNewMLPolyUS = false
	})
	s.liveMu.Lock()
	s.liveBankArm = map[string]float64{"kalshi": 100, "polyus": 100}
	s.liveMu.Unlock()
	if got := s.liveCryptoCapUSD(); math.Abs(got-50) > 1e-12 {
		t.Fatalf("disabled PolyUS enlarged crypto cap: got=%v want=50", got)
	}
	if got := s.liveRiskTotalBankroll(); math.Abs(got-100) > 1e-12 {
		t.Fatalf("disabled PolyUS enlarged total LIVE risk equity: got=%v want=100", got)
	}
	if got := s.liveCap(); math.Abs(got-50) > 1e-12 {
		t.Fatalf("disabled PolyUS enlarged the combined exposure wall: got=%v want=50", got)
	}
	if got := s.liveVenueCap("polyus"); got != 0 {
		t.Fatalf("disabled PolyUS published spendable venue cap=%v want=0", got)
	}
	s.mutateCfg(func(c *config.Config) { c.Risk.LiveSystemPolyUS = true })
	if got := s.liveCryptoCapUSD(); math.Abs(got-100) > 1e-12 {
		t.Fatalf("enabled two-venue crypto cap=%v want=100", got)
	}
	if got := s.liveRiskTotalBankroll(); math.Abs(got-200) > 1e-12 {
		t.Fatalf("enabled two-venue total risk equity=%v want=200", got)
	}
	crypto := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXBTC15M-CAP", Side: "NO", Family: "spotlag"}
	if why := s.liveCryptoBudgetReason(&crypto, 49, 1, true); why != "" {
		t.Fatalf("exact cap boundary refused: %q", why)
	}
	if why := s.liveCryptoBudgetReason(&crypto, 99.5, .51, true); !strings.Contains(why, "aggregate crypto cap") {
		t.Fatalf("over-cap order not refused: %q", why)
	}
	nonCrypto := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXNBAGAME-CAP", Side: "YES", Family: "favorite-long"}
	if why := s.liveCryptoBudgetReason(&nonCrypto, 100, 5, false); why != "" {
		t.Fatalf("non-crypto order incorrectly blocked by unavailable crypto classification: %q", why)
	}
}

func TestR153BundleCryptoCostAndCrossMatchRanking(t *testing.T) {
	crypto := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXBTC15M-BUNDLE", Side: "YES", Family: "xmatch"}
	sport := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXNBAGAME-BUNDLE", Side: "NO", Family: "favorite-long"}
	if got, known := liveCryptoBundleCost([]liveMirrorCandidate{crypto, sport}, 7); !known || got != 7 {
		t.Fatalf("mixed bundle crypto charge=%v known=%v want full 7", got, known)
	}
	if got, known := liveCryptoBundleCost([]liveMirrorCandidate{sport}, 7); !known || got != 0 {
		t.Fatalf("non-crypto bundle charge=%v known=%v want 0", got, known)
	}

	at := time.Now()
	xmatch := liveMirrorRankedCandidate{Candidate: crypto, LowerPerDay: 2, MeanPerDay: 3,
		Quote: liveMirrorQuote{Depth: 1}, CapitalHours: 1}
	spot := liveMirrorRankedCandidate{Candidate: liveMirrorCandidate{Platform: "kalshi", Ticker: "KXBTC15M-BUNDLE",
		Side: "YES", Family: "spotlag", At: at}, LowerPerDay: 2, MeanPerDay: 3,
		Quote: liveMirrorQuote{Depth: 100}, CapitalHours: 1}
	if !strongerLiveMirrorRank(xmatch, spot) || strongerLiveMirrorRank(spot, xmatch) {
		t.Fatal("xmatch did not win the exact economic tie")
	}
	xmatch.LowerPerDay = 1.99
	if strongerLiveMirrorRank(xmatch, spot) {
		t.Fatal("xmatch label displaced a materially stronger fee-net candidate")
	}
}

func TestR153XMatchBothSidesHaveOneExactExecutableContract(t *testing.T) {
	for _, side := range []string{"YES", "NO"} {
		rows := r147ExactSignalContracts("xmatch", "kalshi", side, "taker")
		if len(rows) != 1 || rows[0].InputTopology != "K-PINT" || !rows[0].ProducerCapable {
			t.Fatalf("xmatch %s exact contracts=%+v", side, rows)
		}
	}
}

func TestR153EveryProductionMoneyHandlerUsesClassifiedCryptoBudget(t *testing.T) {
	serverRaw, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	server := string(serverRaw)
	block := func(source, start, next string) string {
		a := strings.Index(source, start)
		if a < 0 {
			t.Fatalf("missing source block %q", start)
		}
		b := strings.Index(source[a+len(start):], next)
		if b < 0 {
			return source[a:]
		}
		return source[a : a+len(start)+b]
	}
	kalshiHandler := block(server, "func (s *Server) handleLivePlace", "\nfunc ")
	if strings.Count(kalshiHandler, "liveVenueBudgetCheckFor(") < 2 {
		t.Fatal("Kalshi initial/final-wire money boundary can bypass classified crypto budget")
	}
	polyHandler := block(server, "func (s *Server) handlePolyUSLiveOrder", "\nfunc ")
	if !strings.Contains(polyHandler, "liveVenueBudgetCheckFor(") {
		t.Fatal("PolyUS final money boundary can bypass classified crypto budget")
	}
	comboHandler := block(server, "func (s *Server) handleLiveComboPlace", "\nfunc ")
	if strings.Count(comboHandler, "liveVenueBudgetCheckForBundle(") < 3 {
		t.Fatal("combo placeholder/actual/serialized boundaries can bypass classified crypto budget")
	}
	brokerRaw, err := os.ReadFile("r148_staged_bundle_broker.go")
	if err != nil {
		t.Fatal(err)
	}
	broker := string(brokerRaw)
	if !strings.Contains(broker, "liveVenueBudgetCheckFor(ctx") ||
		!strings.Contains(broker, "liveVenueBudgetCheckForStagedBundle(ctx") {
		t.Fatal("staged single-leg or package boundary can bypass classified crypto budget")
	}
}
