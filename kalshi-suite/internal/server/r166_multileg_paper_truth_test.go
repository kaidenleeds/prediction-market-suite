package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR166PositiveAndNewMLCombosObserveWithoutBookingPnL(t *testing.T) {
	ctx := context.Background()

	positive := testServer(t)
	positive.polyUS = polymarketus.NewPublicClient(time.Millisecond)
	positive.paperHorizonFn = func(context.Context, string, string, string) float64 { return 1 }
	positive.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }
	r144EnableFundedComboExecutionFixture(positive)
	r166MultiLegPaperTestBypass.Delete(positive)
	pool := r143PositiveComboPool(3, "polyus")
	positive.pusBookMu.Lock()
	positive.pusBookPx = map[string]pusBook{}
	for _, leg := range pool {
		positive.pusBookPx[leg.Ticker] = pusBook{px: leg.Price, at: time.Now()}
	}
	positive.pusBookMu.Unlock()
	positive.autoMu.Lock()
	positive.autoOn = true
	positive.autoMu.Unlock()
	positive.autoPlacePositiveSystemCombos(ctx, pool)
	if rows, err := positive.store.ListParlays(ctx, ""); err != nil || len(rows) != 0 {
		t.Fatalf("positive combo booked unverified Paper P&L: rows=%d err=%v", len(rows), err)
	}
	if st := positive.positiveSystemComboStatus(ctx); st.State != "observing" || st.Eligible == 0 ||
		st.Placed != 0 || st.Reasons["paper_not_observed_unverified_execution"] == 0 {
		t.Fatalf("positive combo did not retain an explicit not-observed receipt: %+v", st)
	}

	ml := testServer(t)
	ml.paperHorizonFn = func(context.Context, string, string, string) float64 { return 1 }
	r146EnableMLComboExecutionFixture(ml)
	r166MultiLegPaperTestBypass.Delete(ml)
	ml.autoMu.Lock()
	ml.autoOn = true
	ml.autoMu.Unlock()
	predictions := []map[string]any{
		{"ticker": "PUS-A", "side": "YES", "platform": "polyus", "title": "event A", "price": .70, "p_win": .85, "ev_net": .15},
		{"ticker": "PUS-B", "side": "YES", "platform": "polyus", "title": "event B", "price": .70, "p_win": .85, "ev_net": .15},
	}
	body, _ := json.Marshal(map[string]any{"execution_enabled": true, "paper_authority": true,
		"feature_schema": currentMLCohort, "model_version": "r166-fixture", "predictions": predictions})
	if err := os.WriteFile(filepath.Join(ml.cfg().DataDir, "ml_predictions.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	ml.autoPlaceMLComboPaper(ctx)
	if rows, err := ml.store.ListParlays(ctx, ""); err != nil || len(rows) != 0 {
		t.Fatalf("New-ML combo booked unverified Paper P&L: rows=%d err=%v", len(rows), err)
	}
	if st := ml.mlComboPaperStatus(ctx); st.State != "observing" || st.Eligible == 0 ||
		st.Placed != 0 || st.Reasons["paper_not_observed_unverified_execution"] == 0 {
		t.Fatalf("New-ML combo did not retain an explicit not-observed receipt: %+v", st)
	}
}

func TestR166StagedBundleIsDurablyRejectedWithoutFunding(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.autoMu.Lock()
	s.autoOn = true
	s.autoMu.Unlock()
	s.portfolioEquityReadForTest = func(context.Context, string) (bookSessionMetrics, bool) {
		return bookSessionMetrics{UnitsComplete: true, OpenComplete: true}, true
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	bundle := r148ServerPaperBundle("nested-ladder-lock", "r166-flat", "r166-flat-opportunity", now)
	if ok, err := s.store.InsertResearchRouteBundle(ctx, bundle); err != nil || !ok {
		t.Fatalf("insert bundle: ok=%v err=%v", ok, err)
	}
	s.sweepR148StagedPaperBundles(ctx)
	stats, err := s.store.StagedPaperBundleSystemStats(ctx)
	if err != nil || len(stats) != 1 || stats[0].Attempts != 1 || stats[0].Rejected != 1 || stats[0].Open != 0 {
		t.Fatalf("staged not-observed decision was not durable and flat: stats=%+v err=%v", stats, err)
	}
	if funds, err := s.store.StagedPaperBundleFundsSince(ctx, time.Time{}); err != nil || funds.OpenCount != 0 || funds.OpenCost != 0 {
		t.Fatalf("staged not-observed decision consumed Paper funds: funds=%+v err=%v", funds, err)
	}
}

func TestR166TypedResearchPaperRequestStopsBeforeVenueWrite(t *testing.T) {
	s := testServer(t)
	body := map[string]any{"collection_ticker": "COLL", "auto": true, "paper_only": true,
		"research_bundle_id": "bundle-r166", "legs": []map[string]any{
			{"ticker": "K-A", "event": "E-A", "side": "yes"},
			{"ticker": "K-B", "event": "E-B", "side": "no"},
		}}
	raw, _ := json.Marshal(body)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/live/combo/place", bytes.NewReader(raw))
	s.handleLiveComboPlace(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("typed unverified Paper request status=%d body=%s", rr.Code, rr.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil || got["paper_execution_state"] != "not_observed" || got["paper_accepted"] != false {
		t.Fatalf("typed request did not report explicit not-observed state: body=%s err=%v", rr.Body.String(), err)
	}
}

func TestR166XvComboSignalNoLongerCreatesProductSimPnL(t *testing.T) {
	s := testServer(t)
	r107kal(s)
	ctx := context.Background()
	s.mutateCfg(func(c *config.Config) { c.Auto.XvgapComboOverlay = true })
	const gapTicker = "KXMLBGAME-26JUL10NYYBOS-NYY"
	const partnerTicker = "KXMLBTOTAL-26JUL10NYYBOS-8"
	seedKalMeta(s, kalshi.Market{Ticker: gapTicker, LastPrice: .50})
	seedKalMeta(s, kalshi.Market{Ticker: partnerTicker, LastPrice: .50})
	eventKey := s.legEventKey("kalshi", gapTicker, "")
	class := plabClass([]plabLeg{{Ticker: gapTicker, Platform: "kalshi", EventKey: eventKey},
		{Ticker: partnerTicker, Platform: "kalshi", EventKey: eventKey}}, eventKey)
	s.plabMu.Lock()
	s.plabCorr = map[string]*plabCorrC{class: {N: 100, A: 50, B: 50, AB: 50}}
	s.plabMu.Unlock()
	s.xvComboTry(ctx, "xvgap", gapTicker, "YES", .50, .20, 1)
	s.xvcMu.Lock()
	open := len(s.xvcLoadLocked().Open)
	s.xvcMu.Unlock()
	if open != 0 {
		t.Fatalf("product-sim observation still created %d Paper P&L row(s)", open)
	}
}

func TestR166XvComboResetStartsFlatAndPreservesOldSettlement(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	old := xvcPos{TS: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano),
		LegA: "R166-XVC-WIN", SideA: "YES", LegB: "R166-XVC-LOSE", SideB: "YES",
		Cost: .30, Contracts: 1, Fee: .02, Expr: xvcExprProduct, Mode: "combo_overlay"}
	s.xvcMu.Lock()
	b := s.xvcLoadLocked()
	b.Open = []xvcPos{old}
	b.Net, b.Wins, b.Losses = -2, 3, 4
	s.xvcDirty = true
	s.xvcMu.Unlock()
	s.xvcFlush()
	archived, err := s.resetXvComboPaperAt(time.Now())
	if err != nil || archived != 1 {
		t.Fatalf("xvcombo reset archived=%d err=%v", archived, err)
	}
	s.xvcMu.Lock()
	b = s.xvcLoadLocked()
	if len(b.Open) != 0 || len(b.ArchivedOpen) != 1 {
		s.xvcMu.Unlock()
		t.Fatalf("xvcombo reset did not start flat while retaining settlement: %+v", b)
	}
	if net, wins, losses := xvcCurrentEpochStats(b); net != 0 || wins != 0 || losses != 0 {
		s.xvcMu.Unlock()
		t.Fatalf("xvcombo current stats survived reset: net=%v wins=%d losses=%d", net, wins, losses)
	}
	s.xvcMu.Unlock()
	for _, row := range []struct {
		ticker string
		yes    float64
	}{{old.LegA, 1}, {old.LegB, 0}} {
		if err := s.store.InsertSignal(ctx, storage.Signal{Platform: "kalshi", Ticker: row.ticker,
			Title: row.ticker, Side: "YES", SignalType: "xvgap", EntryPrice: .5}); err != nil {
			t.Fatal(err)
		}
		if err := s.store.ResolveSignals(ctx, row.ticker, row.yes); err != nil {
			t.Fatal(err)
		}
	}
	s.settleXvComboBook(ctx)
	s.xvcMu.Lock()
	b = s.xvcLoadLocked()
	defer s.xvcMu.Unlock()
	if len(b.ArchivedOpen) != 0 || len(b.Closed) != 1 {
		t.Fatalf("pre-reset xvcombo lot did not finish settlement: %+v", b)
	}
	if net, wins, losses := xvcCurrentEpochStats(b); net != 0 || wins != 0 || losses != 0 {
		t.Fatalf("pre-reset settlement leaked into current P&L: net=%v wins=%d losses=%d", net, wins, losses)
	}
}
