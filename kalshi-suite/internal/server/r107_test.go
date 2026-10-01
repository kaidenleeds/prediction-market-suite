package server

// r107_test.go — R107: maker sim on EVERY paper book (operator order) + Part-1 venue-rule
// exception + cost-of-patience watch. Pins:
//   1. book-routed posts rest (mfs -1 + pendingMaker book tag) and fill ONLY via the book fill
//      routers at the POSTED price with the PURE maker fee + fill tags (fill-only-on-cross is
//      makerSimDecision, already pinned by TestR106MakerSimLiveParity — here we pin the routing).
//   2. option-c divert books an INSTANT taker lot, tagged divert:<reason> with taker fees.
//   3. historical ML maker/taker settings cannot bypass R166's global new-entry retirement.
//   4. late-fill watch: cancel → watch → expiry stamps late_fill=0; storage setters one-shot.
//   5. ML maker-fill queue rows carry the exact fee + tags; a conflicting open ML lot cancels
//      the fill instead of double-booking.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

// r107kal gives a test server the fast-fail venue client (r86/r87 convention): REST paths error
// instantly and the entry-price helpers fall back to their signal±2¢ models.
func r107kal(s *Server) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		yesBid, noBid := "0.3800", "0.6800" // YES bid 38 cents; YES ask 32 cents
		if strings.Contains(r.URL.Path, "R107D1") {
			yesBid = "0.3000"
		} else if strings.Contains(r.URL.Path, "R114B2") || strings.Contains(r.URL.Path, "R114D1") {
			yesBid, noBid = "0.5800", "0.4000"
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"orderbook_fp":{"yes_dollars":[["` + yesBid + `","50.00"]],"no_dollars":[["` + noBid + `","50.00"]]}}`))
	}))
	s.kal = kalshi.NewClient(ts.URL, nil, 1000, time.Second)
}

func TestR107BookMakerPostAndFillRouting(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	cfg := *s.cfg()
	cfg.Auto.PaperTotalStart = 1000
	cfg.Auto.AllocRawFlow = 0.2
	cfg.Auto.MakerSimBooks = true
	s.cfgP.Store(&cfg)

	if ok := s.bookMakerPost(ctx, "rawflow", "kalshi", "R107T1", "title", "yes", "rawflow", 0.30, 3, 0, routeDecision{}); !ok {
		t.Fatal("bookMakerPost must accept a clean post")
	}
	if ok := s.bookMakerPost(ctx, "rawflow", "kalshi", "R107T1", "title", "yes", "rawflow", 0.30, 3, 0, routeDecision{}); ok {
		t.Fatal("duplicate post on the same market must refuse (one resting quote per market)")
	}
	s.pendMu.Lock()
	if len(s.pendingMakers) != 1 || s.pendingMakers[0].book != "rawflow" {
		s.pendMu.Unlock()
		t.Fatalf("pendingMakers routing tag wrong: %+v", s.pendingMakers)
	}
	p := s.pendingMakers[0]
	s.pendMu.Unlock()
	if p.id <= 0 {
		t.Fatal("mfs attempt row id missing")
	}
	// fill routes into the RawFlow book at the POSTED price with maker fee + tags
	if r := s.rfMakerFill(ctx, p, 0.29); r != "" {
		t.Fatalf("rfMakerFill: %s", r)
	}
	s.rfBookMu.Lock()
	b := s.rfLoadLocked()
	if len(b.Open) != 1 || b.Open[0].Price != 0.30 || b.Open[0].FillKind != "maker" || b.Open[0].FillRule != "trade-through" {
		s.rfBookMu.Unlock()
		t.Fatalf("rawflow lot wrong: %+v", b.Open)
	}
	s.rfBookMu.Unlock()
	// a second fill on the same market must CONFLICT (one lot per market), never double-book
	if r := s.rfMakerFill(ctx, p, 0.29); r != "conflict" {
		t.Fatalf("expected conflict on second fill, got %q", r)
	}
	// weather router: same contract
	cfg2 := *s.cfg()
	cfg2.Auto.AllocWeather = 0.2
	s.cfgP.Store(&cfg2)
	p2 := &pendingMaker{id: p.id, platform: "kalshi", ticker: "R107W1", title: "t", side: "no", source: "weather", px: 0.25, contracts: 2, book: "weather"}
	if r := s.wxMakerFill(ctx, p2, 0.24); r != "" {
		t.Fatalf("wxMakerFill: %s", r)
	}
	s.wxBookMu.Lock()
	wb := s.wxBookLoadLocked()
	if len(wb.Open) != 1 || wb.Open[0].FillKind != "maker" {
		s.wxBookMu.Unlock()
		t.Fatalf("weather lot wrong: %+v", wb.Open)
	}
	s.wxBookMu.Unlock()
}

func TestR107RawFlowTakerDivertRetiredFromLegacyBook(t *testing.T) {
	s := testServer(t)
	r107kal(s)
	ctx := context.Background()
	cfg := *s.cfg()
	cfg.Auto.PaperTotalStart = 1000
	cfg.Auto.AllocRawFlow = 0.2
	cfg.Auto.MakerSimBooks = true
	cfg.Auto.MakerAdverseGuard = true // testServer has NO book feeds ⇒ depth0 ⇒ the gate diverts
	s.cfgP.Store(&cfg)
	s.rawFlowPlace(ctx, "R107D1", "title", "yes", 0.30, 1)
	s.rfBookMu.Lock()
	b := s.rfLoadLocked()
	defer s.rfBookMu.Unlock()
	if len(b.Open) != 0 {
		t.Fatalf("R166 must not create an instant RawFlow lot from a signal quote: %+v", b.Open)
	}
}

func TestR107MakerSimKnobOffDoesNotRestoreLegacyFill(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	cfg := *s.cfg()
	cfg.Auto.PaperTotalStart = 1000
	cfg.Auto.AllocRawFlow = 0.2
	cfg.Auto.MakerSimBooks = false // R166: this knob cannot revive signal-price assumed fills.
	s.cfgP.Store(&cfg)
	s.rawFlowPlace(ctx, "R107L1", "title", "yes", 0.30, 1)
	s.rfBookMu.Lock()
	defer s.rfBookMu.Unlock()
	b := s.rfLoadLocked()
	if len(b.Open) != 0 {
		t.Fatalf("maker-sim off must not restore the retired legacy fill path: %+v", b.Open)
	}
}

func TestR107MLPostSettingsCannotBypassR166Retirement(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Auto.MakerSimBooks = true
	s.cfgP.Store(&cfg)
	post := func(row map[string]any, ref float64) map[string]any {
		body, _ := json.Marshal(map[string]any{"row": row, "ref_price": ref})
		req := httptest.NewRequest("POST", "/api/mlpost", strings.NewReader(string(body)))
		w := httptest.NewRecorder()
		s.handleMLPost(w, req)
		var out map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &out)
		return out
	}
	for _, tc := range []struct {
		platform, ticker string
		makerBlocked     bool
	}{
		{platform: "kalshi", ticker: "R107ML1"},
		{platform: "polyus", ticker: "r107-pus-blocked", makerBlocked: true},
		{platform: "polyus", ticker: "r107-pus-maker"},
	} {
		cfgNow := *s.cfg()
		cfgNow.Auto.PolyusMakerBlocked = tc.makerBlocked
		s.cfgP.Store(&cfgNow)
		out := post(map[string]any{"ticker": tc.ticker, "side": "yes", "platform": tc.platform,
			"contracts": 2.0, "title": "t"}, 0.40)
		if out["status"] != "reject" || out["reason"] != "book-native-ml-paper-log-only" {
			t.Fatalf("%s maker_blocked=%t bypassed R166 retirement: %v", tc.platform, tc.makerBlocked, out)
		}
	}
	s.pendMu.Lock()
	pending := len(s.pendingMakers)
	s.pendMu.Unlock()
	if pending != 0 {
		t.Fatalf("retired ML endpoint created %d pending post(s)", pending)
	}
}

func TestR107MLMakerFillQueueRowAndConflict(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	// the conflict lot exists BEFORE the first mlBookOpenSides call (it snapshot-caches ~30s)
	mlp0 := map[string]any{"open": []any{map[string]any{"ticker": "R107Q2", "side": "no", "platform": "kalshi", "contracts": 1.0, "price": 0.5}}}
	mb0, _ := json.Marshal(mlp0)
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, "ml_paper.json"), mb0, 0o644); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"ticker": "R107Q1", "side": "yes", "platform": "kalshi", "contracts": 3.0, "p_win": 0.6})
	p := &pendingMaker{id: 42, platform: "kalshi", ticker: "R107Q1", side: "yes", source: "ml-book", px: 0.35, contracts: 3, book: "ml", mlRow: raw}
	if r := s.mlMakerFill(ctx, p, 0.34); r != "" {
		t.Fatalf("mlMakerFill: %s", r)
	}
	b, err := os.ReadFile(filepath.Join(s.cfg().DataDir, "ml_maker_fills.jsonl"))
	if err != nil {
		t.Fatalf("queue file: %v", err)
	}
	var rec struct {
		Seq int64          `json:"seq"`
		Row map[string]any `json:"row"`
	}
	if err := json.Unmarshal(b[:len(b)-1], &rec); err != nil {
		t.Fatalf("queue line: %v (%s)", err, b)
	}
	if rec.Seq != 42 || rec.Row["price"] != 0.35 || rec.Row["entry_price"] != 0.35 || rec.Row["fill_kind"] != "maker" || rec.Row["fill_rule"] != "trade-through" {
		t.Fatalf("queue row wrong: %+v", rec)
	}
	if _, ok := rec.Row["fee"].(float64); !ok {
		t.Fatal("queue row must carry the exact entry fee")
	}
	// conflict: the pre-existing open ML lot on R107Q2 blocks the fill (one lot per market at fill time)
	p2 := &pendingMaker{id: 43, platform: "kalshi", ticker: "R107Q2", side: "no", source: "ml-book", px: 0.4, contracts: 2, book: "ml", mlRow: raw}
	if r := s.mlMakerFill(ctx, p2, 0.39); r != "conflict" {
		t.Fatalf("expected conflict (open ML lot), got %q", r)
	}
}

func TestR107LateFillWatchStamps(t *testing.T) {
	s := testServer(t)
	r107kal(s)
	ctx := context.Background()
	id, err := s.store.InsertMakerAttempt(ctx, "kalshi", "R107LF1", "yes", "rawflow", 0.30, 0, 0, 0, nil, 1, "", "none")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.store.MarkMakerExpiredRule(ctx, id, "moved-1c"); err != nil {
		t.Fatal(err)
	}
	// expiry path: a watch already past its window stamps late_fill=0
	s.pendMu.Lock()
	s.lateWatches = append(s.lateWatches, lateWatch{id: id, platform: "kalshi", ticker: "R107LF1", side: "yes",
		postPx: 0.30, canceledAt: time.Now().Add(-31 * time.Minute), until: time.Now().Add(-time.Minute)})
	s.pendMu.Unlock()
	s.sweepLateWatches(ctx)
	s.pendMu.Lock()
	nLeft := len(s.lateWatches)
	s.pendMu.Unlock()
	if nLeft != 0 {
		t.Fatalf("expired watch must be dropped, %d left", nLeft)
	}
	// direct setter semantics: one-shot, filled=0 rows only
	if err := s.store.SetMakerLateFill(ctx, id, 120); err != nil {
		t.Fatal(err)
	} // late_fill already 0 → no overwrite
	var lf, lfs any
	row := s.store.DBForTest().QueryRow(`SELECT late_fill, late_fill_s FROM maker_fill_stats WHERE id=?`, id)
	if err := row.Scan(&lf, &lfs); err != nil {
		t.Fatal(err)
	}
	if lfInt, _ := lf.(int64); lfInt != 0 {
		t.Fatalf("late_fill must stay 0 after expiry (one-shot), got %v", lf)
	}
}

func TestR107TransitionStampOnce(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	cfg := *s.cfg()
	cfg.Auto.MakerSimBooks = true
	s.cfgP.Store(&cfg)
	s.makerSimTransitionStamp(ctx)
	s.makerSimTransitionStamp(ctx) // second call must not add a row
	n := 0
	rows, err := s.store.DBForTest().Query(`SELECT COUNT(*) FROM audit_log WHERE message LIKE 'MAKER-SIM TRANSITION%'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		_ = rows.Scan(&n)
	}
	if n != 1 {
		t.Fatalf("transition stamp must land exactly once, got %d", n)
	}
}
