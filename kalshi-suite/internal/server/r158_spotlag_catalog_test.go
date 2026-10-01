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

func TestR158SpotlagUsesFreshCompleteBoardReceiptAndSoonestWindow(t *testing.T) {
	now := time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)
	board := []kalshi.Market{
		{
			Ticker:             "KXBTC15M-26JUL180830-30",
			Title:              "BTC later",
			Status:             "active",
			ExpectedExpiration: now.Add(30 * time.Minute).Format(time.RFC3339Nano),
		},
		{
			Ticker:             "KXBTC15M-26JUL180815-15",
			Title:              "BTC sooner",
			Status:             "active",
			ExpectedExpiration: now.Add(15 * time.Minute).Format(time.RFC3339Nano),
		},
		{
			Ticker:             "KXETH15M-26JUL180815-15",
			Title:              "already resolved",
			Status:             "finalized",
			Result:             "yes",
			ExpectedExpiration: now.Add(15 * time.Minute).Format(time.RFC3339Nano),
		},
	}
	priceReads := 0
	rows, sourceOK, reason := spotlagMarketsFromCompleteBoard(
		now, now.Add(-time.Second), board,
		func(string) (float64, bool) {
			priceReads++
			return .42, true
		})
	if !sourceOK || reason != "" || len(rows) != 1 ||
		rows[0].Ticker != "KXBTC15M-26JUL180815-15" || rows[0].Up != .42 {
		t.Fatalf("rows=%+v source_ok=%v reason=%q", rows, sourceOK, reason)
	}
	if priceReads != 2 {
		t.Fatalf("live price reads=%d, want both active BTC windows", priceReads)
	}
}

func TestR159SpotlagWatchesXRPFromCoinbaseThroughKalshiCatalog(t *testing.T) {
	now := time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)
	board := []kalshi.Market{{
		Ticker:             "KXXRP15M-26JUL180815-15",
		Title:              "XRP price in fifteen minutes",
		Status:             "active",
		ExpectedExpiration: now.Add(15 * time.Minute).Format(time.RFC3339Nano),
	}}
	rows, sourceOK, reason := spotlagMarketsFromCompleteBoard(
		now, now.Add(-time.Second), board,
		func(ticker string) (float64, bool) {
			if ticker != board[0].Ticker {
				t.Fatalf("unexpected live-price ticker %q", ticker)
			}
			return .47, true
		})
	if !sourceOK || reason != "" || len(rows) != 1 ||
		rows[0].Coin != "xrp" || rows[0].Ticker != board[0].Ticker || rows[0].Up != .47 {
		t.Fatalf("XRP rows=%+v source_ok=%v reason=%q", rows, sourceOK, reason)
	}
	if got := coinbaseSpotCoin("XRP-USD"); got != "xrp" {
		t.Fatalf("Coinbase XRP product mapped to %q", got)
	}
	if got := spotlagCoinForTicker("KXDOGE15M-26JUL180815-15"); got != "" {
		t.Fatalf("low-volume DOGE unexpectedly entered Spot-lag as %q", got)
	}
}

func TestR158SpotlagCompleteBoardReceiptFailsClosedOnlyWhenCatalogIsInvalid(t *testing.T) {
	now := time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)
	nonSpotlagBoard := []kalshi.Market{{
		Ticker:             "KXWEATHER-R158",
		Status:             "active",
		ExpectedExpiration: now.Add(time.Hour).Format(time.RFC3339Nano),
	}}
	tests := []struct {
		name    string
		board   []kalshi.Market
		boardAt time.Time
		wantOK  bool
		reason  string
	}{
		{"missing receipt", nil, time.Time{}, false, spotlagReasonBoardMissing},
		{"future clock", nonSpotlagBoard, now.Add(spotlagBoardFutureSkew + time.Nanosecond), false,
			spotlagReasonBoardClockInvalid},
		{"stale receipt", nonSpotlagBoard, now.Add(-spotlagBoardCacheMaxAge - time.Nanosecond), false,
			spotlagReasonBoardStale},
		{"fresh board with ordinary no-window gap", nonSpotlagBoard, now.Add(-time.Second), true, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows, ok, reason := spotlagMarketsFromCompleteBoard(now, tc.boardAt, tc.board, nil)
			if ok != tc.wantOK || reason != tc.reason || len(rows) != 0 {
				t.Fatalf("rows=%+v ok=%v reason=%q, want ok=%v reason=%q",
					rows, ok, reason, tc.wantOK, tc.reason)
			}
		})
	}
}

func TestR158SpotlagDetailedPassNamesUnderlyingAndInsertFailures(t *testing.T) {
	market := []spotlagCachedMarket{{
		Coin: "btc", Ticker: "KXBTC15M-R158", Title: "BTC", Up: .42,
	}}
	now := time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)
	sigma := func(string) (float64, bool) { return .02, true }
	momentum := func(string) (float64, bool) { return .005, true }

	attempts, errorsN, reason := runSpotlagDetectorPassDetailed(
		context.Background(), market,
		func(context.Context, string) (float64, float64, time.Time, bool) {
			return 0, 0, time.Time{}, false
		}, sigma, momentum, nil)
	if attempts != 0 || errorsN != 1 || reason != spotlagReasonUnderlyingMissing {
		t.Fatalf("underlying receipt=(%d,%d,%q)", attempts, errorsN, reason)
	}

	attempts, errorsN, reason = runSpotlagDetectorPassDetailed(
		context.Background(), market,
		func(context.Context, string) (float64, float64, time.Time, bool) {
			return 64000, .04, now, true
		}, sigma, momentum, func(context.Context, storage.Signal) error {
			return errors.New("sqlite unavailable")
		})
	if attempts != 1 || errorsN != 1 || reason != spotlagReasonSignalInsertFailed {
		t.Fatalf("insert receipt=(%d,%d,%q)", attempts, errorsN, reason)
	}
}

func TestR158SpotlagProducerReasonIsVisibleAndDurableOnlyOnTransitions(t *testing.T) {
	store, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s := &Server{store: store}
	now := time.Now().UTC()

	s.finishSignalProducerWithReason("spotlag", now, 0, 1, spotlagReasonBoardStale)
	s.finishSignalProducerWithReason("spotlag", now.Add(time.Millisecond), 0, 1,
		spotlagReasonBoardStale)
	receipts, snapshotAt := s.signalProducerSnapshotAt()
	receipt := receipts["spotlag"]
	if receipt.Reason != spotlagReasonBoardStale {
		t.Fatalf("receipt=%+v", receipt)
	}
	if ok, detail := signalProducersClassify(snapshotAt, time.Minute,
		[]string{"spotlag"}, receipts); ok || !strings.Contains(detail, spotlagReasonBoardStale) {
		t.Fatalf("classified ok=%v detail=%q", ok, detail)
	}

	s.finishSignalProducerWithReason("spotlag", now.Add(2*time.Millisecond), 0, 0, "")
	auditRows, err := store.ListAudit(context.Background(), 20)
	if err != nil {
		t.Fatal(err)
	}
	unhealthy, recovered := 0, 0
	for _, row := range auditRows {
		if row.Category != "signal-producer" || !strings.Contains(row.Message, "spotlag") {
			continue
		}
		switch {
		case strings.Contains(row.Message, "unhealthy"):
			unhealthy++
			if row.Detail != spotlagReasonBoardStale {
				t.Fatalf("unhealthy detail=%q", row.Detail)
			}
		case strings.Contains(row.Message, "recovered"):
			recovered++
		}
	}
	if unhealthy != 1 || recovered != 1 {
		t.Fatalf("durable transition rows unhealthy=%d recovered=%d rows=%+v",
			unhealthy, recovered, auditRows)
	}
}
