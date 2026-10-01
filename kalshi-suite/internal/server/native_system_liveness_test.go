package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR139NativeProducerStatesDoNotCallHistoricalRowsCollecting(t *testing.T) {
	s := testServer(t)
	if state, _ := s.nativeProducerState("parlay-10leg"); state != "ARCHIVED_BY_6_LEG_LIMIT" {
		t.Fatalf("10-leg history state=%s", state)
	}
	if state, _ := s.nativeProducerState("parlay-6leg"); state != "RESEARCH_ONLY_CURRENT" {
		t.Fatalf("6-leg current state=%s", state)
	}
	if state, _ := s.nativeProducerState("old-mystery-result"); state != "HISTORICAL_UNMAPPED" {
		t.Fatalf("unmapped history state=%s", state)
	}
	s.mutateCfg(func(c *config.Config) {
		c.Auto.SharplineEnabled = true
		c.Auto.OddsAPIKey = ""
	})
	if state, _ := s.nativeProducerState("sharpline"); state != "NEEDS_KEY" {
		t.Fatalf("key-blocked producer state=%s", state)
	}
}

func TestR139SignalToBookFunnelIsBatchedAndVisible(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	sig := storage.Signal{Platform: "polyus", Ticker: "missing-book", Side: "YES", SignalType: "edge"}
	result := s.unitTrialConsider(ctx, sig)
	if !result.QuoteAttempted || result.UnitRow ||
		!strings.Contains(result.Reason, "fresh_side_specific_book_unavailable") {
		t.Fatalf("missing-book result=%+v", result)
	}
	s.noteNativeSystemFunnel(ctx, sig, nativeSignalFunnelEvent{Eligible: 1, InputRows: 1,
		QuoteAttempts: 1, Reason: result.Reason})
	s.flushNativeSystemFunnels(ctx, time.Now().UTC(), true)
	views, err := s.store.NativeSystemFunnelViews(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(views) != 1 || views[0].CycleEligible != 1 || views[0].CycleInputRows != 1 ||
		views[0].CycleQuoteAttempts != 1 || views[0].CycleUnitRows != 0 ||
		views[0].Exclusions[nativeFunnelReason(result.Reason)] != 1 {
		t.Fatalf("batched funnel=%+v", views)
	}
}

func TestR139SparseOneSignalFlushesWithoutASecondSignal(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXSPARSE", Side: "YES", SignalType: "kalshi-flow"}
	s.noteNativeSystemFunnel(ctx, sig, nativeSignalFunnelEvent{Eligible: 1, InputRows: 1,
		Reason: "fresh_side_specific_book_unavailable"})
	st := s.nativeFunnelState()
	st.Lock()
	st.lastFlush = time.Now().UTC().Add(-2 * time.Minute)
	st.Unlock()
	// This is the same non-force path called on every independent research-liveness tick.
	s.flushNativeSystemFunnels(ctx, time.Now().UTC(), false)
	views, err := s.store.NativeSystemFunnelViews(ctx, time.Now().UTC())
	if err != nil || len(views) != 1 || views[0].CycleInputRows != 1 {
		t.Fatalf("sparse periodic flush views=%+v err=%v", views, err)
	}
}

func TestR143SignalHotPathNeverRunsSQLiteFunnelFlush(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	st := s.nativeFunnelState()
	st.Lock()
	st.lastFlush = time.Now().UTC().Add(-2 * time.Minute)
	st.Unlock()
	s.noteNativeSystemFunnel(ctx, storage.Signal{Platform: "kalshi", Ticker: "KXHOT", Side: "YES",
		SignalType: "kalshi-flow"}, nativeSignalFunnelEvent{Eligible: 1, InputRows: 1})
	views, err := s.store.NativeSystemFunnelViews(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(views) != 0 {
		t.Fatalf("signal insertion synchronously flushed research SQLite rows: %+v", views)
	}
	s.flushNativeSystemFunnels(ctx, time.Now().UTC(), false)
	views, err = s.store.NativeSystemFunnelViews(ctx, time.Now().UTC())
	if err != nil || len(views) != 1 || views[0].CycleInputRows != 1 {
		t.Fatalf("independent periodic flush lost hot-path aggregate: views=%+v err=%v", views, err)
	}
}

func TestR139LeaderboardNativeCollectionSeparatesFreshArchivedAndUnmapped(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := s.store.InsertNativeSystemFunnelReceipts(ctx, []storage.NativeSystemFunnelReceipt{{
		Observed: now, CycleID: "fresh", Family: "kalshi-flow", Platform: "kalshi", OriginLayer: "model",
		ProducerState: "CURRENT", ProducerReason: "fixture", Eligible: 1, InputRows: 1,
		QuoteAttempts: 1, UnitRows: 1, MoneyTruthAttempts: 1, FirstInput: now, LastInput: now,
		LastEconomic: now, Exclusions: map[string]int{},
	}}); err != nil {
		t.Fatal(err)
	}
	rows := []leaderboardBacktestRow{
		{UnitTrialLeaderboardStat: storage.UnitTrialLeaderboardStat{Family: "kalshi-flow", Platform: "kalshi", OriginLayer: "model"}},
		{UnitTrialLeaderboardStat: storage.UnitTrialLeaderboardStat{Family: "parlay-10leg", Platform: "kalshi", OriginLayer: "strategy"}},
		{UnitTrialLeaderboardStat: storage.UnitTrialLeaderboardStat{Family: "old-mystery-result", Platform: "kalshi", OriginLayer: "model"}},
	}
	rows = s.annotateNativeSystemFunnels(ctx, rows, now)
	if rows[0].NativeCollection == nil || rows[0].NativeCollection.CollectionState != "COLLECTING" {
		t.Fatalf("fresh producer=%+v", rows[0].NativeCollection)
	}
	if rows[1].NativeCollection == nil || rows[1].NativeCollection.CollectionState != "ARCHIVED_BY_6_LEG_LIMIT" {
		t.Fatalf("archived producer=%+v", rows[1].NativeCollection)
	}
	if rows[2].NativeCollection == nil || rows[2].NativeCollection.CollectionState != "HISTORICAL_UNMAPPED" ||
		!strings.Contains(rows[2].NativeCollection.Reason, "not called collecting") {
		t.Fatalf("unmapped producer=%+v", rows[2].NativeCollection)
	}
}
