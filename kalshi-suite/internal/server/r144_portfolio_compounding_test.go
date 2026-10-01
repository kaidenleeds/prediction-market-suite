package server

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

func TestR144EveryPaperPortfolioCompoundsIndependently(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	nets := map[string]float64{vbKalshi: 40, vbPolyus: -50, vbCombos: 30, vbML: 12}
	reads := map[string]int{}
	s.portfolioEquityReadForTest = func(_ context.Context, book string) (bookSessionMetrics, bool) {
		reads[book]++
		return bookSessionMetrics{NetUSD: nets[book]}, true
	}

	want := map[string]float64{vbKalshi: 640, vbPolyus: 550, vbCombos: 630, vbML: 612}
	for book, expected := range want {
		got, ok := s.bookCurrentEquityUSD(ctx, book)
		if !ok || math.Abs(got-expected) > 1e-9 {
			t.Fatalf("%s equity = %.2f ok=%v, want %.2f", book, got, ok, expected)
		}
	}
	// A second sizing burst reuses the exact snapshot; it neither re-adds P&L nor rescans ledgers.
	if got, ok := s.bookCurrentEquityUSD(ctx, vbKalshi); !ok || got != 640 || reads[vbKalshi] != 1 {
		t.Fatalf("cached Kalshi equity = %.2f ok=%v reads=%d; want 640 and one read", got, ok, reads[vbKalshi])
	}
	if got, ok := s.bookCurrentEquityUSD(ctx, vbPolyus); !ok || got != 550 {
		t.Fatalf("Kalshi gain must not subsidize PolyUS: got %.2f ok=%v, want 550", got, ok)
	}
	// Score-weight dollar shares use the same current equity, not the configured $600 anchor.
	s.swMu.Lock()
	s.swTable = &swState{At: time.Now(), Books: map[string][]swEntry{
		vbKalshi: {{Family: "book:r144-k", Weight: .5, Cents: 5}},
		vbPolyus: {{Family: "book:r144-p", Weight: .5, Cents: 5}},
	}}
	s.swMu.Unlock()
	if got, ok := s.subShareUSD("book:r144-k"); !ok || got != 320 {
		t.Fatalf("Kalshi 50%% score share = %.2f ok=%v, want 50%% of compounded 640 = 320", got, ok)
	}
	if got, ok := s.subShareUSD("book:r144-p"); !ok || got != 275 {
		t.Fatalf("PolyUS 50%% score share = %.2f ok=%v, want 50%% of independently compounded 550 = 275", got, ok)
	}

	// Open/pending funded exposure is subtracted after compounding. This $10 Kalshi position must
	// affect Kalshi only; the PolyUS and Combo books retain their own equities.
	if _, err := s.store.InsertPaperFill(ctx, paper.Fill{Platform: vbKalshi, Ticker: "KXR144-COMPOUND",
		Title: "compound exposure", Side: "YES", Action: "BUY", Price: .50, Contracts: 20,
		Source: "auto-cons-r144"}); err != nil {
		t.Fatal(err)
	}
	s.fillsBust()
	if got := s.bookAvailableUSD(ctx, vbKalshi); math.Abs(got-630) > 1e-9 {
		t.Fatalf("Kalshi available = %.2f, want compounded 640 - $10 open = 630", got)
	}
	if got := s.bookAvailableUSD(ctx, vbPolyus); math.Abs(got-550) > 1e-9 {
		t.Fatalf("PolyUS available changed from a Kalshi open: %.2f, want 550", got)
	}
	// A generic resting maker is a real reservation even though it has not filled into paper_fills.
	s.pendMu.Lock()
	s.pendingMakers = append(s.pendingMakers, &pendingMaker{platform: vbPolyus, ticker: "pus-r144-pending",
		px: .25, contracts: 40, postedAt: time.Now()})
	s.pendMu.Unlock()
	pendingFee := s.pureMakerFee(vbPolyus, "pus-r144-pending", 40, .25)
	// The eventual fill keeps its signed rebate in P&L, but an unfilled post cannot spend that
	// credit in advance. Buying power reserves principal plus positive fees only.
	if got, want := s.bookAvailableUSD(ctx, vbPolyus), 550-10-math.Max(0, pendingFee); math.Abs(got-want) > 1e-9 {
		t.Fatalf("PolyUS available with resting maker = %.4f, want %.4f", got, want)
	}
	if got := s.bookAvailableUSD(ctx, vbCombos); math.Abs(got-630) > 1e-9 {
		t.Fatalf("Combo gain did not compound independently: available %.2f, want 630", got)
	}
}

func TestR144PortfolioResetReanchorsAndUnreadableTruthFailsClosed(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	net := 25.0
	readOK := true
	reads := 0
	s.portfolioEquityReadForTest = func(_ context.Context, _ string) (bookSessionMetrics, bool) {
		reads++
		return bookSessionMetrics{NetUSD: net}, readOK
	}
	if got, ok := s.bookCurrentEquityUSD(ctx, vbCombos); !ok || got != 625 {
		t.Fatalf("pre-reset Combo equity = %.2f ok=%v, want 625", got, ok)
	}

	// Reset changes the epoch and invalidates the old snapshot. The new epoch starts at its grant;
	// prior profit is not carried forward a second time.
	s.pnlMu.Lock()
	s.pnlEpoch = time.Now().Add(time.Second)
	s.pnlMu.Unlock()
	net = 0
	s.invalidatePortfolioEquityCache()
	if got, ok := s.bookCurrentEquityUSD(ctx, vbCombos); !ok || got != 600 || reads != 2 {
		t.Fatalf("post-reset Combo equity = %.2f ok=%v reads=%d, want reanchored 600 and two reads", got, ok, reads)
	}

	s.portfolioEquityMu.Lock()
	snap := s.portfolioEquityCache[vbCombos]
	snap.At = time.Now().Add(-portfolioEquityTTL - time.Second)
	s.portfolioEquityCache[vbCombos] = snap
	s.portfolioEquityMu.Unlock()
	readOK = false
	if got, ok := s.bookCurrentEquityUSD(ctx, vbCombos); ok || got != 0 {
		t.Fatalf("expired cache + unreadable fresh truth must fail closed, got %.2f ok=%v", got, ok)
	}

	// There is no artificial drawdown floor: losses beyond the grant produce zero sizing equity.
	readOK, net = true, -700
	s.invalidatePortfolioEquityCache()
	if got, ok := s.bookCurrentEquityUSD(ctx, vbKalshi); !ok || got != 0 {
		t.Fatalf("loss beyond grant = %.2f ok=%v, want a true zero floor", got, ok)
	}
}
