package server

import (
	"context"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR142FundedHorizonHardCapsAndTighteningOnly(t *testing.T) {
	a := config.Default().Auto
	a.ConsensusMaxHoursOut = 99
	a.ConsensusCryptoMaxHoursOut = 99
	a.ParlayMaxHoursOut = 99
	a.PaperComboMaxHoursOut = 99
	a.PaperComboCryptoMaxHoursOut = 99
	a.PaperMLMaxHoursOut = 99
	a.PaperMLCryptoMaxHoursOut = 99
	regular, crypto := paperEntryHorizonLimits(a)
	if regular != 4 || crypto != 2 {
		t.Fatalf("single widening escaped hard caps: regular=%v crypto=%v", regular, crypto)
	}
	comboRegular, comboCrypto := paperComboEntryHorizonLimits(a)
	if comboRegular != 24 || comboCrypto != 6 {
		t.Fatalf("combo widening escaped hard caps: regular=%v crypto=%v", comboRegular, comboCrypto)
	}
	liveComboRegular, liveComboCrypto := liveComboEntryHorizonLimits(a)
	if liveComboRegular != 4 || liveComboCrypto != 2 {
		t.Fatalf("LIVE combo widening escaped 4h/2h caps: regular=%v crypto=%v", liveComboRegular, liveComboCrypto)
	}
	mlRegular, mlCrypto := paperMLEntryHorizonLimits(a)
	if mlRegular != 24 || mlCrypto != 6 {
		t.Fatalf("Paper New ML widening escaped 24h/6h caps: regular=%v crypto=%v", mlRegular, mlCrypto)
	}

	a.ConsensusMaxHoursOut = 3
	a.ConsensusCryptoMaxHoursOut = 1
	a.ParlayMaxHoursOut = 4
	a.PaperComboMaxHoursOut = 12
	a.PaperComboCryptoMaxHoursOut = 3
	a.PaperMLMaxHoursOut = 10
	a.PaperMLCryptoMaxHoursOut = 4
	regular, crypto = paperEntryHorizonLimits(a)
	comboRegular, comboCrypto = paperComboEntryHorizonLimits(a)
	if regular != 3 || crypto != 1 || comboRegular != 12 || comboCrypto != 3 {
		t.Fatalf("tightening not route-correct: singles=%v/%v combos=%v/%v", regular, crypto, comboRegular, comboCrypto)
	}
	mlRegular, mlCrypto = paperMLEntryHorizonLimits(a)
	if mlRegular != 10 || mlCrypto != 4 {
		t.Fatalf("Paper New ML tightening not route-correct: regular=%v crypto=%v", mlRegular, mlCrypto)
	}

	a.PaperComboMaxHoursOut = .5
	_, comboCrypto = paperComboEntryHorizonLimits(a)
	if comboCrypto != .5 {
		t.Fatalf("Paper combo regular knob must also tighten crypto, got %v", comboCrypto)
	}
}

func TestR142FundedHorizonRejectsUnknownPastAndNonFinite(t *testing.T) {
	for name, hours := range map[string]float64{
		"unknown": 0,
		"past":    -.001,
		"nan":     math.NaN(),
		"pos-inf": math.Inf(1),
		"neg-inf": math.Inf(-1),
		"too-far": 4.0001,
	} {
		if paperEntryHorizonEligible(hours, 4) {
			t.Errorf("%s horizon %v was eligible", name, hours)
		}
	}
	if !paperEntryHorizonEligible(4, 4) {
		t.Fatal("exact regular boundary must remain eligible")
	}
}

func TestR142PolyUSFundedClockDoesNotUseResearchPastGrace(t *testing.T) {
	s := testServer(t)
	s.paperHorizonFn = nil // production path; do not inherit testServer's one-hour seam
	now := time.Now().UTC()
	s.polyUSMu.Lock()
	s.polyUSMkts = []polyUSMarket{
		{Slug: "ended", Start: now.Add(-4 * time.Hour).Format(time.RFC3339)}, // estimated end was 30m ago
		{Slug: "future", Start: now.Add(15 * time.Minute).Format(time.RFC3339)},
		{Slug: "overtime", Start: now.Add(-4 * time.Hour).Format(time.RFC3339), Live: true},
		{Slug: "unknown"},
	}
	s.polyUSMu.Unlock()

	if got := s.paperEntryResolveHours(context.Background(), "polyus", "ended", ""); got >= 0 {
		t.Fatalf("ended PolyUS funded clock=%v, want negative rather than research +0.1h grace", got)
	}
	if s.paperEntryHorizonNow(context.Background(), "polyus", "ended", "") {
		t.Fatal("ended PolyUS market entered the funded horizon")
	}
	if got := s.paperEntryResolveHours(context.Background(), "polyus", "unknown", ""); got != 0 {
		t.Fatalf("unknown PolyUS funded clock=%v, want 0", got)
	}
	if !s.paperEntryHorizonNow(context.Background(), "polyus", "future", "") {
		t.Fatal("known future PolyUS market inside four hours should be eligible")
	}
	if s.paperEntryHorizonNow(context.Background(), "polyus", "overtime", "") {
		t.Fatal("LIVE label minted a new horizon after the bounded game clock elapsed")
	}
}

func TestR142PlaceParlayRechecksEveryLegAtInsertBoundary(t *testing.T) {
	s := testServer(t)
	s.polyUS = polymarketus.NewPublicClient(time.Millisecond) // cache hit below; no network call
	s.pusBookPx = map[string]pusBook{
		"A": {px: .50, at: time.Now()},
		"B": {px: .50, at: time.Now()},
	}
	calls := 0
	s.paperHorizonFn = func(context.Context, string, string, string) float64 {
		calls++
		if calls <= 2 { // first pricing pass for both legs
			return 1
		}
		return 0 // final check immediately before InsertParlay
	}
	legs := []paper.Leg{
		{Platform: "polyus", Ticker: "A", Side: "YES", Entry: .50},
		{Platform: "polyus", Ticker: "B", Side: "YES", Entry: .50},
	}
	if _, err := s.placeParlayLegs(context.Background(), legs, 10, "test"); err == nil || !strings.Contains(err.Error(), "left the entry horizon") {
		t.Fatalf("combo crossed horizon at insert boundary: calls=%d err=%v", calls, err)
	}
	if rows, err := s.store.ListParlays(context.Background(), ""); err != nil || len(rows) != 0 {
		t.Fatalf("horizon-rejected combo was persisted: rows=%d err=%v", len(rows), err)
	}
}

func TestR142AutoPlaceAndLiveComboFailClosedOnUnknownClock(t *testing.T) {
	s := testServer(t)
	calls := 0
	s.paperHorizonFn = func(context.Context, string, string, string) float64 {
		calls++
		return 0
	}
	if s.autoPlace(context.Background(), "kalshi", "KX-UNKNOWN", "Unknown", "YES", .50, 1, 0, 0, "auto-test") {
		t.Fatal("single Paper auto placed with an unknown clock")
	}
	if calls != 0 {
		t.Fatal("retired immediate Paper path did work after its fail-closed entry gate")
	}

	s.liveMu.Lock()
	s.liveArmed = true
	s.liveMu.Unlock()
	code, out := s.selfPOST(s.handleLiveComboPlace, "/api/live/combo/place", map[string]any{
		"collection_ticker": "COL",
		"legs": []map[string]any{
			{"ticker": "A", "event": "EA", "side": "yes"},
			{"ticker": "B", "event": "EB", "side": "no"},
		},
	})
	if code != http.StatusConflict || !strings.Contains(out["error"].(string), "unknown, elapsed") {
		t.Fatalf("live combo unknown-clock response code=%d out=%v", code, out)
	}
}

func TestR142ComboCryptoClassificationUsesTickerOrTitle(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Auto.PaperComboMaxHoursOut = 99
	cfg.Auto.PaperComboCryptoMaxHoursOut = 99
	s.cfgP.Store(&cfg)
	if s.paperComboEntryHorizonOK(6.01, "GENERIC", "Bitcoin above 100k") {
		t.Fatal("title-classified crypto widened past the six-hour Paper Combo hard cap")
	}
	if !s.paperComboEntryHorizonOK(6, "KXBTC-WINDOW", "") {
		t.Fatal("exact six-hour Paper Combo crypto boundary should be eligible")
	}
	if paperEntryHorizonEligible(2.01, s.liveComboEntryHorizonLimit("GENERIC", "Bitcoin above 100k")) {
		t.Fatal("wider Paper Combo clock leaked into the final LIVE crypto handoff")
	}
}

func TestR142FundedCombosUseCanonicalFixedPortfolio(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Auto.BookCombosUSD = 1234
	cfg.Auto.ParlayBankrollUSD = 7 // legacy knob must not create a competing Combo bankroll
	s.cfgP.Store(&cfg)
	if got := s.parlayBankroll(); got != 1234 {
		t.Fatalf("combo bankroll=%v, want canonical book_combos_usd=1234", got)
	}
	if _, err := s.store.InsertParlay(context.Background(), paper.Parlay{Stake: 10, Fees: 2, Price: .25, Contracts: 40, Status: "open",
		RouteSource: positiveSystemComboRouteSource, Cohort: storage.ComboLabCohortRollingPositive,
		SystemIDs: []string{"exact-system"}, JointP: .3, ExpectedNetPerDollar: .1,
		Legs: []paper.Leg{{Platform: "kalshi", Ticker: "A", Side: "YES", Entry: .5}, {Platform: "kalshi", Ticker: "B", Side: "YES", Entry: .5}}}); err != nil {
		t.Fatal(err)
	}
	if got := s.parlayAvailableUSD(context.Background()); got != 1222 {
		t.Fatalf("canonical available=%v, want fixed portfolio less fee-inclusive open combo cost", got)
	}
}
