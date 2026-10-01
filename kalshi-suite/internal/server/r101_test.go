package server

// R101 tests — auditor r28 batch:
//   DO-THIS 1: the adverse-post gate stamps its decision INPUTS (momentum, threshold, trigger,
//              depth source) on every mfs row — schema + insert paths proven here.
//   Parlays:   ARCHIVED (rotation-f verdict) — no suggestions, no briefing section, header line
//              drops once flat; history endpoints stay readable.
//   Briefing:  ML book stats + top LIVE-priced picks folded in; drift/fantasy picks never
//              advertised; "since last" fills/closes line; Shadow line gone once retired+flat.
//   Bug 241:   RFQ would-quote rows carry combo LEGS and rfq_deleted lifecycle rows.
//   Bug 229:   catalog upserts skip-if-fresh (close_ts change bypasses).

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// TestR101MakerGateInputsPersist — DO-THIS 1: both insert paths accept + persist the gate inputs
// (a failed ALTER/column mismatch would error these inserts on the fresh schema).
func TestR101MakerGateInputsPersist(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	mom := -2.5
	if _, err := s.store.InsertMakerAttempt(ctx, "kalshi", "KXR101", "YES", "auto-ml", 0.50, 2, 140, 0, &mom, 1.0, "", "bookws"); err != nil {
		t.Fatalf("InsertMakerAttempt with gate inputs: %v", err)
	}
	if err := s.store.InsertMakerGated(ctx, "polyus", "r101-slug", "NO", "auto-cons-pflow", 0.40, 0, 0, nil, 1.0, "depth0", "none"); err != nil {
		t.Fatalf("InsertMakerGated with gate inputs (nil momentum): %v", err)
	}
	if err := s.store.InsertMakerGated(ctx, "kalshi", "KXR101B", "YES", "auto-cons-kthresh", 0.62, 3, 55, &mom, 1.0, "momentum", "kob"); err != nil {
		t.Fatalf("InsertMakerGated momentum-trigger row: %v", err)
	}
}

// TestR101ParlayArchive — the generic ML combo lane stays retired: no generic suggestions are
// computed and the dedicated legacy message is empty. The separate positive-system Paper route
// owns its own status/API and must not resurrect this path.
func TestR101ParlayArchive(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	if got := s.parlaySuggest(ctx); len(got) != 0 {
		t.Fatalf("archived parlays must produce no suggestions, got %d", len(got))
	}
	drops := s.ParlayDropReasons()
	found := false
	for _, m := range drops {
		for reason := range m {
			if strings.Contains(reason, "retired") || strings.Contains(reason, "archived") {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("the zero-state must explain the retired generic lane, got %v", drops)
	}
	if txt := s.ParlayBriefingText(ctx); txt != "" {
		t.Fatalf("archived parlays must produce no dedicated message, got %q", txt)
	}
}

// TestR101BriefingFoldPicksAndSinceLast — the R101 telegram shape: 🎯 book stats line, ⭐ top
// LIVE-priced picks (above-ceiling drift/fantasy and non-live-priced picks never advertised),
// no 🎰 section, and the 📈 since-last line on the second build.
func TestR101BriefingFoldPicksAndSinceLast(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	writeBookJSON(t, s.cfg().DataDir, "ml_paper.json", map[string]any{
		"bank0": 400.0, "open": []any{}, "closed": []any{},
		"stats":    map[string]any{"net": -28.96, "equity": 374.90, "win_rate": 0.414, "closed": 29, "open_n": 30},
		"lifetime": map[string]any{"net": -28.96, "closed": 29.0, "wins": 12.0, "net_base": 0.0, "bank_ver": 3.0},
	})
	writeBookJSON(t, s.cfg().DataDir, "ml_predictions.json", map[string]any{
		"feature_schema": currentMLCohort, "model_cohort": currentMLCohort,
		"oos_auc": 0.7785, "oos_brier": 0.1881,
		"predictions": []any{
			map[string]any{"title": "Seattle to beat LA tonight", "side": "no", "p_win": 0.71,
				"live_price": 0.62, "ev_net_live": 0.081, "px_src": "live"},
			map[string]any{"title": "FANTASY drift pick", "side": "yes", "p_win": 0.91,
				"live_price": 0.59, "ev_net_live": 0.31, "px_src": "live"}, // above the 20¢ ceiling → never advertised
			map[string]any{"title": "Stale-priced pick", "side": "yes", "p_win": 0.80,
				"live_price": 0.55, "ev_net_live": 0.12, "px_src": "signal"}, // not live-priced → skipped
		},
	})
	txt := s.BriefingText(ctx)
	// Stale stats cannot invent current positions or carry legacy losses into New ML. The actual
	// book-native-v2 arrays are empty, so the briefing must say exactly that.
	if !strings.Contains(txt, "Paper · 0 settled · +$0.00 · real n/a · predicted n/a · open 0") {
		t.Fatalf("ML net-EV line missing/mis-shaped:\n%s", txt)
	}
	if strings.Contains(txt, "Seattle to beat LA tonight") {
		t.Fatalf("individual picks belong on the ML dashboard, not the compact briefing:\n%s", txt)
	}
	if strings.Contains(txt, "FANTASY") || strings.Contains(txt, "Stale-priced") {
		t.Fatalf("above-ceiling / non-live picks must never be advertised:\n%s", txt)
	}
	if strings.Contains(txt, "🎰") {
		t.Fatalf("parlay section must be gone:\n%s", txt)
	}
	// R127 punch-list C: the 📈 since-last line is REMOVED — even after fresh fills land.
	for _, f := range []paper.Fill{
		{Platform: "kalshi", Ticker: "KXR101F", Title: "R101", Side: "YES", Action: "BUY", Price: 0.50, Contracts: 4, Source: "auto-ml"},
		{Platform: "kalshi", Ticker: "KXR101F", Title: "R101", Side: "YES", Action: "SELL", Price: 0.60, Contracts: 4, Source: "auto-ml"},
	} {
		if _, err := s.store.InsertPaperFill(ctx, f); err != nil {
			t.Fatalf("insert fill: %v", err)
		}
	}
	s.fillsBust() // the delta cache must see the new rows this build
	txt = s.BriefingText(ctx)
	if strings.Contains(txt, "📈 since last") {
		t.Fatalf("R127: the since-last line must be gone:\n%s", txt)
	}
}

// TestR101ShadowLineDropsWhenRetiredFlat — the retired shadow ledger is archive/export-only. It
// never appears inside a surface labeled New ML, even if a stale file still claims open lots.
func TestR101ShadowLineDropsWhenRetiredFlat(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	tcfg := *s.cfg()
	tcfg.Auto.ShadowBookRetired = true
	s.cfgP.Store(&tcfg)
	book := func(openN int) {
		writeBookJSON(t, s.cfg().DataDir, "ml_shadow.json", map[string]any{
			"bank0": 800.0, "open": []any{}, "closed": []any{},
			"stats":    map[string]any{"net": 337.85, "open_n": openN, "closed": 500.0, "bank0": 800.0},
			"lifetime": map[string]any{"net": 337.85, "closed": 500.0, "wins": 300.0, "net_base": 0.0, "bank_ver": 3.0},
		})
	}
	book(3)
	if txt := s.BriefingText(ctx); strings.Contains(txt, "👤 SHADOW") {
		t.Fatalf("retired shadow must stay out of New ML briefing:\n%s", txt)
	}
	book(0)
	if txt := s.BriefingText(ctx); strings.Contains(txt, "👤 SHADOW") {
		t.Fatalf("retired shadow with 0 open lots must drop the body line:\n%s", txt)
	}
}

// TestR101RFQLegAndLifecycleRows — bug 241/edge 36: a created RFQ on a leg-cached combo carries
// leg snapshots (side lowercased); the deleted broadcast lands a lifecycle row with the lifetime.
func TestR101RFQLegAndLifecycleRows(t *testing.T) {
	s := testServer(t)
	combo := "KXMVESPORTSMULTIGAMEEXTENDED-S1-A1"
	s.rfqPulseMu.Lock()
	s.rfqLegs = map[string][]kalshi.MVELeg{combo: {
		{MarketTicker: "KXMLBGAME-X-AZ", EventTicker: "KXMLBGAME-X", Side: "YES"},
		{MarketTicker: "KXMLBTOTAL-Y-11", EventTicker: "KXMLBTOTAL-Y", Side: "no"},
	}}
	s.rfqPulseMu.Unlock()
	created := time.Now().Add(-90 * time.Second)
	s.rfqPulseAdd(kalshi.RFQPulseEvent{Type: "created", RFQID: "rfq-1", Ticker: combo,
		Collection: "KXMVESPORTSMULTIGAMEEXTENDED-R", TargetUSD: 40, At: created})
	s.rfqPulseAdd(kalshi.RFQPulseEvent{Type: "deleted", RFQID: "rfq-1", At: time.Now()})
	s.rfqPulseMu.Lock()
	defer s.rfqPulseMu.Unlock()
	if len(s.rfqWould) != 2 {
		t.Fatalf("want 2 buffered rows (created+deleted), got %d", len(s.rfqWould))
	}
	cr := s.rfqWould[0]
	if cr.Kind != "" || len(cr.Legs) != 2 || cr.Legs[0].Mkt != "KXMLBGAME-X-AZ" || cr.Legs[0].Side != "yes" || cr.Legs[1].Side != "no" {
		t.Fatalf("created row must carry both legs with lowercased sides: %+v", cr)
	}
	if cr.Collection == "" {
		t.Fatalf("created row must carry the collection (whale-family split): %+v", cr)
	}
	del := s.rfqWould[1]
	if del.Kind != "deleted" || del.Ticker != combo || del.LifetimeS < 85 || del.LifetimeS > 200 {
		t.Fatalf("deleted row must resolve ticker + carry the ~90s lifetime: %+v", del)
	}
}

// TestR101CatalogSkipIfFresh — bug 229: an unchanged row inside the 45-min window skips; a
// close_ts change bypasses the window immediately.
func TestR101CatalogSkipIfFresh(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	row := storage.CatalogRow{Venue: "kalshi", Ticker: "KXR101CAT", EventKey: "E", Kind: "winner", Title: "T", CloseTS: "2026-07-08T00:00:00Z"}
	if err := s.upsertCatalogFresh(ctx, []storage.CatalogRow{row}); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	if ct, _, ok := s.store.CatalogCloseTS(ctx, "kalshi", "KXR101CAT"); !ok || ct != "2026-07-08T00:00:00Z" {
		t.Fatalf("row must land on first upsert: ok=%v ct=%q", ok, ct)
	}
	e1 := s.catUpSeen["kalshi|KXR101CAT"]
	if err := s.upsertCatalogFresh(ctx, []storage.CatalogRow{row}); err != nil {
		t.Fatalf("second upsert: %v", err)
	}
	if e2 := s.catUpSeen["kalshi|KXR101CAT"]; e2.at != e1.at {
		t.Fatalf("unchanged fresh row must SKIP (seen stamp must not move): %v -> %v", e1.at, e2.at)
	}
	row.CloseTS = "2026-07-09T00:00:00Z" // reschedule — must bypass the freshness window
	if err := s.upsertCatalogFresh(ctx, []storage.CatalogRow{row}); err != nil {
		t.Fatalf("bypass upsert: %v", err)
	}
	if ct, _, _ := s.store.CatalogCloseTS(ctx, "kalshi", "KXR101CAT"); ct != "2026-07-09T00:00:00Z" {
		t.Fatalf("close_ts change must land immediately, got %q", ct)
	}
}
