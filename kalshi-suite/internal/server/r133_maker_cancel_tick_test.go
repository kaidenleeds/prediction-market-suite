package server

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

func TestR133MakerCancelMovedUsesAbsoluteVenueTick(t *testing.T) {
	for _, tc := range []struct {
		name            string
		current, rested float64
		tick            float64
		want            bool
	}{
		{"feed repeat", 0.45, 0.45, 0.001, false},
		{"inside sub-cent tick", 0.4509, 0.45, 0.001, false},
		{"upper sub-cent tick", 0.451, 0.45, 0.001, true},
		{"lower sub-cent tick", 0.449, 0.45, 0.001, true},
		{"bad metadata falls back to one cent", 0.455, 0.45, 0, false},
		{"fallback one-cent boundary", 0.46, 0.45, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := makerCancelMoved(tc.current, tc.rested, tc.tick); got != tc.want {
				t.Fatalf("makerCancelMoved(%v,%v,%v)=%v, want %v", tc.current, tc.rested, tc.tick, got, tc.want)
			}
		})
	}
}

func TestR133KalshiCancelUsesOutcomeSideTickBand(t *testing.T) {
	// Deliberately asymmetric fixture: the NO-side order rests at 5c (0.1c tick), while its raw
	// YES wire price is 95c (1c tick). Looking up TickFor on raw YES would miss this required pull.
	var m kalshi.Market
	if err := json.Unmarshal([]byte(`{
		"ticker":"T",
		"price_ranges":[
			{"start":"0.0000","end":"0.1000","step":"0.0010"},
			{"start":"0.1000","end":"1.0000","step":"0.0100"}
		]
	}`), &m); err != nil {
		t.Fatal(err)
	}
	s := &Server{
		liveArmed: true,
		kmkts:     map[string]kalshi.Market{"T": m},
		liveOrders: map[string]liveOrderInfo{
			"no-tail":  {Ticker: "T", Side: "ask", YesPrice: 0.95}, // BUY NO at 5c
			"yes-high": {Ticker: "T", Side: "bid", YesPrice: 0.95}, // BUY YES at 95c
		},
	}
	hits := s.kalshiTickCancelHits("T", 0.949)
	if len(hits) != 1 || hits[0].id != "no-tail" {
		t.Fatalf("hits=%+v, want only NO-tail order at its 0.1c tick", hits)
	}
	if math.Abs(hits[0].rested-0.05) > 1e-9 || math.Abs(hits[0].tick-0.001) > 1e-9 {
		t.Fatalf("NO-side receipt=%+v, want rested=.05 tick=.001", hits[0])
	}
}

func TestR133KalshiCancelUsesDirectionalTicksAtTaperBoundaries(t *testing.T) {
	var m kalshi.Market
	if err := json.Unmarshal([]byte(`{
		"ticker":"BOUNDARY",
		"price_ranges":[
			{"start":"0.0000","end":"0.1000","step":"0.0010"},
			{"start":"0.1000","end":"0.9000","step":"0.0100"},
			{"start":"0.9000","end":"1.0000","step":"0.0010"}
		]
	}`), &m); err != nil {
		t.Fatal(err)
	}
	s := &Server{kmkts: map[string]kalshi.Market{"BOUNDARY": m}}
	for _, tc := range []struct {
		name            string
		rested, current float64
		wantTick        float64
	}{
		{"10c downward enters lower tail", .10, .099, .001},
		{"10c upward enters middle band", .10, .11, .01},
		{"90c downward enters middle band", .90, .89, .01},
		{"90c upward enters upper tail", .90, .901, .001},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tick := s.kalshiMakerCancelTick("BOUNDARY", tc.rested, tc.current)
			if math.Abs(tick-tc.wantTick) > 1e-12 {
				t.Fatalf("directional tick=%v, want %v", tick, tc.wantTick)
			}
			if !makerCancelMoved(tc.current, tc.rested, tick) {
				t.Fatalf("one legal directional tick must cancel: rested=%v current=%v tick=%v", tc.rested, tc.current, tick)
			}
		})
	}
}

func TestR133KalshiEventCancelHitsAtTaperBoundaries(t *testing.T) {
	var m kalshi.Market
	if err := json.Unmarshal([]byte(`{
		"ticker":"BOUNDARY",
		"price_ranges":[
			{"start":"0.0000","end":"0.1000","step":"0.0010"},
			{"start":"0.1000","end":"0.9000","step":"0.0100"},
			{"start":"0.9000","end":"1.0000","step":"0.0010"}
		]
	}`), &m); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name            string
		rested, current float64
	}{
		{"10c down", .10, .099},
		{"10c up", .10, .11},
		{"90c down", .90, .89},
		{"90c up", .90, .901},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{
				liveArmed: true,
				kmkts:     map[string]kalshi.Market{"BOUNDARY": m},
				liveOrders: map[string]liveOrderInfo{
					"order": {Ticker: "BOUNDARY", Side: "bid", YesPrice: tc.rested},
				},
			}
			hits := s.kalshiTickCancelHits("BOUNDARY", tc.current)
			if len(hits) != 1 || hits[0].id != "order" {
				t.Fatalf("one directional boundary tick must hit: %+v", hits)
			}
		})
	}
}

func TestR133PolyUSCancelTickUsesCachedMetadata(t *testing.T) {
	s := &Server{
		pusMetaC: map[string]pusMetaEntry{
			"meta":         {m: polymarketus.MarketMeta{TickUSD: 0.001}},
			"meta-invalid": {m: polymarketus.MarketMeta{TickUSD: 0}},
		},
		polyUSMkts: []polyUSMarket{
			{Slug: "snapshot", TickSize: 0.005},
			{Slug: "meta-invalid", TickSize: 0.002},
			{Slug: "invalid", TickSize: -1},
		},
	}
	if got := s.polyUSMakerCancelTick("meta"); got != 0.001 {
		t.Fatalf("cached metadata tick=%v, want .001", got)
	}
	if got := s.polyUSMakerCancelTick("snapshot"); got != 0.005 {
		t.Fatalf("snapshot tick=%v, want .005", got)
	}
	if got := s.polyUSMakerCancelTick("meta-invalid"); got != 0.002 {
		t.Fatalf("invalid detail-cache tick should fall through to snapshot, got %v", got)
	}
	for _, slug := range []string{"invalid", "unknown"} {
		if got := s.polyUSMakerCancelTick(slug); got != makerCancelFallbackTick {
			t.Fatalf("%s fallback tick=%v, want %v", slug, got, makerCancelFallbackTick)
		}
	}
}

func TestR133MakerFillEvidenceWinsSameSweepCancel(t *testing.T) {
	s := testServer(t)
	r107kal(s) // non-nil fast local venue client; no tape is needed because queue evidence is preloaded
	s.pendingMakers = []*pendingMaker{{
		platform: "kalshi", ticker: "R133-FILL-FIRST", title: "fill first", side: "yes",
		px: 0.45, contracts: 1, source: "r133-test", postedAt: time.Now().Add(-11 * time.Minute),
		queueKnown: true, filledCt: 1, // executable queue tape already earned the contract
	}}
	s.sweepPendingMakers(context.Background())
	fills, err := s.store.ListPaperFills(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(fills) != 1 || fills[0].Ticker != "R133-FILL-FIRST" || fills[0].FillKind != "maker" ||
		!strings.Contains(fills[0].Note, "rule=queue-through") {
		t.Fatalf("fill-before-cancel receipt missing: %+v", fills)
	}
	s.pendMu.Lock()
	remaining := len(s.pendingMakers)
	s.pendMu.Unlock()
	if remaining != 0 {
		t.Fatalf("filled maker remained pending: %d", remaining)
	}
}
