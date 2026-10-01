package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

func TestR133PolyUSPlannerAndSocketCapCannotDrift(t *testing.T) {
	s := &Server{}
	cfg := config.Default()
	cfg.PolyUSBookWSCap = 1000
	s.cfgP.Store(&cfg)

	// The process was created at 600, then mutable config changed to 1,000. Planning must follow
	// the actual socket until restart rather than claiming 1,000 full books it cannot request.
	s.polyUSWS = (&polymarketus.Client{}).NewMarketsWSWithFullBookCap(600)
	if got := s.polyUSBookWSCap(); got != 600 {
		t.Fatalf("effective cap=%d, want immutable socket cap 600", got)
	}
	markets := make([]polyUSMarket, 1100)
	for i := range markets {
		markets[i] = polyUSMarket{Slug: fmt.Sprintf("slug-%04d", i), Kind: "prop", Volume24h: float64(i)}
	}
	all, stats := polyUSSlugPlan(nil, markets, time.Now(), cfg.Auto, s.polyUSBookWSCap())
	if len(all) != 1100 || stats.Cap != 600 || stats.Selected != 600 {
		t.Fatalf("planner drifted from wire: all=%d stats=%+v", len(all), stats)
	}

	// Before socket construction the same shared clamp applies to config.
	s.polyUSWS = nil
	cfg.PolyUSBookWSCap = 5000
	s.cfgP.Store(&cfg)
	if got := s.polyUSBookWSCap(); got != 1000 {
		t.Fatalf("pre-socket configured cap=%d, want clamped 1000", got)
	}
}

func TestR133PolyUSPlannerSelectsOneThousandAndRetainsFullLightweightTail(t *testing.T) {
	markets := make([]polyUSMarket, 1037)
	for i := range markets {
		markets[i] = polyUSMarket{Slug: fmt.Sprintf("tail-%04d", i), Kind: "prop", Volume24h: float64(i)}
	}
	all, stats := polyUSSlugPlan(nil, markets, time.Now(), config.AutoConfig{}, 1000)
	if len(all) != len(markets) {
		t.Fatalf("lightweight tail=%d, want all %d", len(all), len(markets))
	}
	if stats.Cap != 1000 || stats.Selected != 1000 || !stats.LightweightFullBoard {
		t.Fatalf("configured planner receipt=%+v", stats)
	}
}

func TestR133PolyUSReadinessUsesEffectiveCapAndSmallUniverse(t *testing.T) {
	in := healthyPolyUSBooksInput()
	in.FullCap = 1000
	in.Full = 999
	if ok, detail := polyUSBooksReady(in); ok {
		t.Fatalf("999 full books cannot satisfy effective 1000 cap: %s", detail)
	}
	in.Full = 1000
	if ok, detail := polyUSBooksReady(in); !ok {
		t.Fatalf("1000 full books should satisfy effective 1000 cap: %s", detail)
	}

	// A real board smaller than the cap requires the real proof count, not a fabricated 1,000.
	in.Proofs, in.Requested = 37, 42
	in.Full, in.Lite, in.Trade = 37, 42, 42
	if ok, detail := polyUSBooksReady(in); !ok {
		t.Fatalf("small universe should require 37 full books: %s", detail)
	}
}
