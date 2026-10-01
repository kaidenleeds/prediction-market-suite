package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR138SubcentGolfSweepUsesOneBatchDepthGETAndWritesFunnel(t *testing.T) {
	const ticker = "KXPGATOUR-ISC26-PLAYER"
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/markets/orderbooks":
			if got := r.URL.Query()["tickers"]; len(got) != 1 || got[0] != ticker {
				t.Errorf("batch tickers=%v", got)
			}
			if r.Header.Get("KALSHI-ACCESS-SIGNATURE") == "" {
				t.Error("batch depth GET was not authenticated")
			}
			_, _ = w.Write([]byte(`{"orderbooks":[{"ticker":"` + ticker + `","orderbook_fp":{"yes_dollars":[],"no_dollars":[["0.9990","12.00"]]}}]}`))
		case "/markets":
			_, _ = w.Write([]byte(`{"markets":[],"cursor":""}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := kalshi.NewSigner("subcent-test", pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	if err != nil {
		t.Fatal(err)
	}
	client := kalshi.NewClient(api.URL, signer, 10000, 2*time.Second)
	var m kalshi.Market
	now := time.Now().UTC()
	if err := json.Unmarshal([]byte(`{
"ticker":"`+ticker+`","event_ticker":"KXPGATOUR-ISC26","status":"active",
"title":"Will Test Player win?","yes_sub_title":"Test Player",
"expected_expiration_time":"`+now.Add(-time.Hour).Format(time.RFC3339)+`","close_time":"`+now.Add(14*24*time.Hour).Format(time.RFC3339)+`",
"yes_ask_dollars":"0.0010","yes_ask_size_fp":"12.00","no_ask_dollars":"1.0000",
"fee_waiver_expiration_time":"2030-01-01T00:00:00Z",
"price_level_structure":"tapered_deci_cent",
"price_ranges":[{"start":"0.0000","end":"0.1000","step":"0.0010"}]
}`), &m); err != nil {
		t.Fatal(err)
	}
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{kal: client, store: st, kmkts: map[string]kalshi.Market{ticker: m}}
	s.subcentGolfSweep(context.Background())
	report, err := st.SubcentGolfReport(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if report.Total != 2 || len(report.Recent) != 2 {
		t.Fatalf("prospective maker+taker rows=%+v", report)
	}
	for _, row := range report.Recent {
		if row.BookSource != "kalshi-rest-batch-depth" {
			t.Fatalf("row used non-depth source: %+v", row)
		}
	}
	var status, metrics string
	var eligible, attempted, inserted, funded, paperAuth, liveAuth int
	if err := st.DBForTest().QueryRow(`SELECT status,eligible,attempted,inserted,metrics_json,
funded,paper_authority,live_authority FROM research_collector_receipts
WHERE collector_id='subcent-golf' ORDER BY id DESC LIMIT 1`).
		Scan(&status, &eligible, &attempted, &inserted, &metrics, &funded, &paperAuth, &liveAuth); err != nil {
		t.Fatal(err)
	}
	if status != "healthy" || eligible != 1 || attempted != 1 || inserted != 2 || funded+paperAuth+liveAuth != 0 {
		t.Fatalf("receipt=%s %d/%d/%d authority=%d/%d/%d", status, eligible, attempted, inserted, funded, paperAuth, liveAuth)
	}
	var funnel map[string]any
	if json.Unmarshal([]byte(metrics), &funnel) != nil || funnel["golf"] != float64(1) || funnel["fine_grid"] != float64(1) ||
		funnel["subcent_touch"] != float64(1) || funnel["fresh_complete_book"] != float64(1) || funnel["exact_fee_routes"] != float64(1) {
		t.Fatalf("funnel metrics=%s", metrics)
	}
}

func TestSubcentGolfCohortClassifiersDoNotInventStatus(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		m          kalshi.Market
		wantType   string
		wantStatus string
		wantPhase  string
	}{
		{kalshi.Market{Ticker: "KXPGATOURH2H-26JUL111300-AB", Title: "Player A vs Player B (LIVE)"}, "matchup", "unknown", "live"},
		{kalshi.Market{Ticker: "KXPGATOURR1LEADER-TEST-A", Title: "Will A be the first round leader?", YesSubTitle: "A"}, "round_leader", "unknown", "unknown"},
		{kalshi.Market{Ticker: "KXPGATOUR-TEST-A", EventTicker: "KXPGATOUR-TEST", Title: "Will A win?", YesSubTitle: "A (withdrawn)"}, "winner", "withdrawn", "unknown"},
		{kalshi.Market{Ticker: "KXGOLFPROP-TEST-A", Title: "Will A make the cut?"}, "prop", "unknown", "unknown"},
		{kalshi.Market{Ticker: "KXPGATOUR-26JUL111300-A", Title: "Will A win?"}, "winner", "unknown", "pre_event"},
	}
	for _, tc := range cases {
		if got := subcentGolfMarketType(tc.m); got != tc.wantType {
			t.Errorf("%s type=%s want %s", tc.m.Ticker, got, tc.wantType)
		}
		if got, _ := subcentGolfPlayerStatus(tc.m); got != tc.wantStatus {
			t.Errorf("%s status=%s want %s", tc.m.Ticker, got, tc.wantStatus)
		}
		if got, _ := subcentGolfPhase(tc.m, now); got != tc.wantPhase {
			t.Errorf("%s phase=%s want %s", tc.m.Ticker, got, tc.wantPhase)
		}
	}
}

func TestSubcentGolfOneSidedPointOneCentAskIsCapturedHonestly(t *testing.T) {
	ob := &kalshi.Orderbook{Ticker: "KXPGATOUR-TEST-A",
		YesAsks: []kalshi.OrderbookLevel{{Price: .001, Size: 42}}}
	touches := subcentGolfTouches(ob)
	if len(touches) != 2 {
		t.Fatalf("touches=%d", len(touches))
	}
	if touches[0].Side != "YES" || touches[0].Ask != .001 || touches[0].AskDepth != 42 || touches[0].Bid != 0 {
		t.Fatalf("YES one-sided touch: %+v", touches[0])
	}
	// No YES bid means there is no executable NO ask and no maker price below the 0.1-cent YES
	// ask. The scanner records the YES taker and a maker not_quoteable outcome; it cannot fake one.
	if touches[1].Ask != 0 || touches[0].Bid > 0 {
		t.Fatalf("one-sided book fabricated an opposite quote: %+v", touches)
	}
}

func TestSubcentGolfDepthPriorityOnlyFineGridGolf(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	decode := func(raw string) kalshi.Market {
		var m kalshi.Market
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	markets := map[string]kalshi.Market{
		"golf":  decode(`{"ticker":"KXPGATOUR-TEST-A","event_ticker":"KXPGATOUR-TEST","status":"active","expected_expiration_time":"2026-07-12T12:00:00Z","volume_24h_fp":"10","price_level_structure":"tapered_deci_cent","price_ranges":[{"start":"0.0000","end":"0.1000","step":"0.0010"},{"start":"0.1000","end":"0.9000","step":"0.0100"}]}`),
		"cent":  decode(`{"ticker":"KXPGATOUR-CENT-A","status":"active","expected_expiration_time":"2026-07-12T12:00:00Z","price_level_structure":"linear_cent"}`),
		"other": decode(`{"ticker":"KXOTHER-TEST-A","status":"active","expected_expiration_time":"2026-07-12T12:00:00Z","price_level_structure":"tapered_deci_cent","price_ranges":[{"start":"0.0000","end":"0.1000","step":"0.0010"}]}`),
	}
	got := subcentGolfDepthPriority(markets, now, 64)
	if len(got) != 1 || got[0] != "golf" {
		t.Fatalf("priority=%v", got)
	}
}

func TestSubcentGolfTradabilityUsesCloseNotExpectedExpiration(t *testing.T) {
	now := time.Date(2026, 7, 12, 3, 0, 0, 0, time.UTC)
	var m kalshi.Market
	if err := json.Unmarshal([]byte(`{
"ticker":"KXPGATOUR-ISC26-PLAYER","event_ticker":"KXPGATOUR-ISC26","status":"active",
"expected_expiration_time":"2026-07-12T00:00:00Z","close_time":"2026-07-26T00:00:00Z",
"yes_ask_dollars":"0.0010","yes_ask_size_fp":"12.00","no_ask_dollars":"1.0000",
"price_level_structure":"tapered_deci_cent",
"price_ranges":[{"start":"0.0000","end":"0.1000","step":"0.0010"}]
}`), &m); err != nil {
		t.Fatal(err)
	}
	if !subcentGolfActive(m, now) {
		t.Fatal("active, not-yet-closed market was discarded at its non-binding expected expiration")
	}
	if phase, evidence := subcentGolfPhase(m, now); phase != "live" || !strings.Contains(evidence, "expected event expiration") {
		t.Fatalf("phase=%q evidence=%q", phase, evidence)
	}
	if !subcentGolfCatalogTouch(m) {
		t.Fatal("one-share 0.1-cent catalog ask with published depth was not admitted to fresh-depth verification")
	}
	m.CloseTime = "2026-07-12T02:59:59Z"
	if subcentGolfActive(m, now) {
		t.Fatal("past authoritative close_time remained tradable")
	}
}

func TestResearchMakerDoesNotBlockPortfolioMaker(t *testing.T) {
	s := &Server{pendingMakers: []*pendingMaker{{platform: "kalshi", ticker: "KXGOLF-A", book: "subcent-golf"}}}
	if s.hasPendingMaker("kalshi", "KXGOLF-A") {
		t.Fatal("research-only pending maker blocked the portfolio namespace")
	}
	s.pendingMakers = append(s.pendingMakers, &pendingMaker{platform: "kalshi", ticker: "KXGOLF-A", book: "weather"})
	if !s.hasPendingMaker("kalshi", "KXGOLF-A") {
		t.Fatal("real paper pending maker was not detected")
	}
}

func TestResearchMakerDoesNotBlockMLDuplicateOrClusterPolicy(t *testing.T) {
	rows := []*pendingMaker{{platform: "kalshi", ticker: "KXNBA-26JUL11-A", book: "subcent-golf", px: .4, contracts: 10}}
	dup, twin, stake := mlPendingMakerConflict(rows, "kalshi", "KXNBA-26JUL11-A", "", corrClusterKey("kalshi", "KXNBA-26JUL11-A"))
	if dup || twin || stake != 0 {
		t.Fatalf("research-only maker polluted ML conflict policy: dup=%v twin=%v stake=%v", dup, twin, stake)
	}
	rows = append(rows, &pendingMaker{platform: "kalshi", ticker: "KXNBA-26JUL11-A", book: "ml", px: .4, contracts: 10})
	dup, _, _ = mlPendingMakerConflict(rows, "kalshi", "KXNBA-26JUL11-A", "", "")
	if !dup {
		t.Fatal("real ML pending maker stopped enforcing one-lot-per-market")
	}
}

func TestSubcentGolfMakerPostDoesNotPolluteGlobalMakerCalibration(t *testing.T) {
	ctx := context.Background()
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	trialID, inserted, err := st.InsertSubcentGolfTrial(ctx, storage.SubcentGolfTrial{
		Slot: "2026-07-11T12", Ticker: "KXPGATOUR-ISOLATED-A", EventTicker: "KXPGATOUR-ISOLATED",
		Title: "Will A win?", Player: "A", Side: "YES", Phase: "pre_event", PlayerStatus: "active",
		MarketType: "winner", Route: "maker", BookSource: "kalshi-ws-depth", TickSize: .001,
		BidPx: .001, AskPx: .002, BidDepth: 10, AskDepth: 10, FillState: "resting",
	})
	if err != nil || !inserted {
		t.Fatalf("insert trial: id=%d inserted=%v err=%v", trialID, inserted, err)
	}
	s := &Server{store: st}
	if !s.subcentGolfMakerPost(ctx, trialID, kalshi.Market{Ticker: "KXPGATOUR-ISOLATED-A", Title: "Will A win?"},
		subcentGolfTouch{Side: "YES", Bid: .001, Ask: .002, BidDepth: 10, AskDepth: 10}, "test-fee") {
		t.Fatal("research maker post was not registered")
	}
	var globalN int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM maker_fill_stats`).Scan(&globalN); err != nil || globalN != 0 {
		t.Fatalf("subcent research polluted funded maker calibration: n=%d err=%v", globalN, err)
	}
	var simID int64
	var queueAhead float64
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT maker_attempt_id,queue_ahead FROM subcent_golf_trials WHERE id=?`, trialID).Scan(&simID, &queueAhead); err != nil || simID >= 0 || queueAhead != 10 {
		t.Fatalf("cohort-local simulation id=%d queue=%.2f err=%v", simID, queueAhead, err)
	}
}

func TestSubcentGolfCachedSettlementNeedsNoSignalRow(t *testing.T) {
	ctx := context.Background()
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	trial := storage.SubcentGolfTrial{Slot: "2026-07-11T12", Ticker: "KXPGATOUR-FINAL-A",
		Side: "YES", Phase: "unknown", PlayerStatus: "unknown", MarketType: "winner",
		Route: "taker", BookSource: "kalshi-ws-depth", TickSize: .001, AskPx: .001,
		AskDepth: 10, FillState: "filled", FillPrice: .001}
	if _, inserted, err := st.InsertSubcentGolfTrial(ctx, trial); err != nil || !inserted {
		t.Fatalf("insert: %v %v", inserted, err)
	}
	var final kalshi.Market
	if err := json.Unmarshal([]byte(`{"ticker":"KXPGATOUR-FINAL-A","status":"finalized","result":"yes","settlement_value_dollars":"1.0000"}`), &final); err != nil {
		t.Fatal(err)
	}
	s := &Server{store: st}
	s.settleSubcentGolf(ctx, map[string]kalshi.Market{final.Ticker: final}, time.Now().UTC())
	report, err := st.SubcentGolfReport(ctx, 10)
	if err != nil || report.Settled != 1 || len(report.Recent) != 1 || report.Recent[0].TerminalStatus != "active" {
		t.Fatalf("settlement report=%+v err=%v", report, err)
	}
	var signals int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM signal_log`).Scan(&signals); err != nil || signals != 0 {
		t.Fatalf("research settlement inserted executable signal rows: n=%d err=%v", signals, err)
	}
}
