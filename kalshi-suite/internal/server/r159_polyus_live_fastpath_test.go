package server

import (
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

func TestR159PolyUSLiveRulesUseFreshCompleteCrawlWithoutNetwork(t *testing.T) {
	s := testServer(t)
	theta := .06
	active, closed, archived := true, false, false
	m := polymarketus.Market{
		Slug: "fast-market", Title: "Fast market", Active: &active, Closed: &closed,
		Archived: &archived, State: "MARKET_STATE_OPEN", TickSize: .005,
		MinimumQty: 1, FeeCoeff: &theta,
	}
	s.polyUSMu.Lock()
	s.pusSweep = []polymarketus.Market{m}
	s.pusSweepBySlug = map[string]polymarketus.Market{"fast-market": m}
	s.pusSweepAt = time.Now()
	s.polyUSMu.Unlock()

	got, at, ok := s.polyUSLiveMarketRules("FAST-MARKET")
	if !ok || got.Slug != m.Slug || got.TickSize != .005 || got.MinimumQty != 1 ||
		at.IsZero() {
		t.Fatalf("cached live rules=%+v at=%v ok=%v", got, at, ok)
	}
	s.polyUSMu.Lock()
	s.pusSweepAt = time.Now().Add(-polymarketus.MarketsRESTLifecycleMaxAge - time.Second)
	s.polyUSMu.Unlock()
	if _, _, ok := s.polyUSLiveMarketRules("fast-market"); ok {
		t.Fatal("stale complete crawl remained live metadata authority")
	}
}

func TestR159PolyUSExecutableDepthAtLimitBothSides(t *testing.T) {
	bids := []polymarketus.BookLevel{
		{Price: .60, Quantity: 2}, {Price: .58, Quantity: 3}, {Price: .55, Quantity: 7},
	}
	asks := []polymarketus.BookLevel{
		{Price: .62, Quantity: 1}, {Price: .64, Quantity: 4}, {Price: .67, Quantity: 8},
	}
	touch, depth, ok := polyUSExecutableDepthAtLimit(bids, asks, "YES", .64)
	if !ok || math.Abs(touch-.62) > 1e-12 || math.Abs(depth-5) > 1e-12 {
		t.Fatalf("YES executable touch/depth=%v/%v ok=%v", touch, depth, ok)
	}
	touch, depth, ok = polyUSExecutableDepthAtLimit(bids, asks, "NO", .42)
	if !ok || math.Abs(touch-.40) > 1e-12 || math.Abs(depth-5) > 1e-12 {
		t.Fatalf("NO executable touch/depth=%v/%v ok=%v", touch, depth, ok)
	}
	if _, _, ok := polyUSExecutableDepthAtLimit(bids, asks, "YES", .61); ok {
		t.Fatal("non-crossing YES limit reported executable depth")
	}
}

func TestR159PolyUSMoneyPathHasNoInlineMetadataRESTAndRereadsBeforePOST(t *testing.T) {
	raw, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	start := strings.Index(src, "func (s *Server) handlePolyUSLiveOrder")
	if start < 0 {
		t.Fatal("PolyUS LIVE handler not found")
	}
	end := strings.Index(src[start+1:], "\nfunc ")
	if end < 0 {
		t.Fatal("PolyUS LIVE handler boundary not found")
	}
	handler := src[start : start+1+end]
	for _, forbidden := range []string{".MarketBySlug(", ".GetMarketMeta(", ".pusMeta(", ".BookFull("} {
		if strings.Contains(handler, forbidden) {
			t.Fatalf("PolyUS money handler regained inline metadata/book REST: %s", forbidden)
		}
	}
	submitStarted := strings.Index(handler, "storage.LivePendingRiskSubmitStarted")
	finalRead := strings.LastIndex(handler, ".CurrentOpenFullBook(")
	venuePost := strings.Index(handler, ".PlaceLiveOrder(")
	if submitStarted < 0 || finalRead <= submitStarted || venuePost <= finalRead {
		t.Fatalf("final WS boundary order is not journal -> reread -> POST: submit=%d read=%d post=%d",
			submitStarted, finalRead, venuePost)
	}

	raw, err = os.ReadFile("liveautomirror.go")
	if err != nil {
		t.Fatal(err)
	}
	src = string(raw)
	start = strings.Index(src, "func (s *Server) liveMirrorExecutable")
	end = strings.Index(src[start+1:], "\nfunc ")
	if start < 0 || end < 0 {
		t.Fatal("liveMirrorExecutable boundary not found")
	}
	executable := src[start : start+1+end]
	if !strings.Contains(executable, ".CurrentOpenFullBook(") ||
		strings.Contains(executable, ".GetMarketMeta(") {
		t.Fatal("PolyUS dispatcher is not strict-WS plus zero-metadata-REST")
	}
}
