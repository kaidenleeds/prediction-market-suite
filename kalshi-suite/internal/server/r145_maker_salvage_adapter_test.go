package server

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR145MakerSalvageRequiresPositiveCurrentMakerAndRejectedCurrentTaker(t *testing.T) {
	candidate := storage.ResearchPromotionCandidate{
		ObservedPrice: .40, ObservedFeePC: .01, ExpectedNetLower: .04,
	}
	makerLow, takerLow, matched := step7MakerSalvageRouteBounds(candidate, .40, .01, .45, .02)
	if !matched || math.Abs(makerLow-.04) > 1e-9 || math.Abs(takerLow-(-.02)) > 1e-9 {
		t.Fatalf("matched=%v maker=%v taker=%v", matched, makerLow, takerLow)
	}
	if _, _, matched = step7MakerSalvageRouteBounds(candidate, .46, .02, .48, .02); matched {
		t.Fatal("a maker route whose current lower bound is non-positive became salvage")
	}
	if _, _, matched = step7MakerSalvageRouteBounds(candidate, .40, .01, .42, .01); matched {
		t.Fatal("a still-positive taker route became maker salvage")
	}
	rebateCandidate := candidate
	rebateCandidate.ObservedFeePC = -.005
	if _, _, matched = step7MakerSalvageRouteBounds(rebateCandidate, .40, -.005, .45, .02); !matched {
		t.Fatal("an authoritative maker rebate was rejected as an invalid fee")
	}
	candidate.ExpectedNetLower = 0
	if _, _, matched = step7MakerSalvageRouteBounds(candidate, .40, .01, .45, .02); matched {
		t.Fatal("missing positive source-model lower bound became maker salvage")
	}
}

func TestR145ResearchPaperRouteFeeUsesSignedAuthoritativeVenueEconomics(t *testing.T) {
	s := testServer(t)
	now := time.Now()
	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{
		"KXFEE": kalFeeInfoFromVenue("quadratic_with_maker_fees", 1),
	}
	s.kalFeesAt = now
	s.kalFeeMu.Unlock()

	kMaker, kSource, ok := s.researchPaperRouteFee("kalshi", "KXFEE-26TEST", "maker", 1, .40)
	wantKMaker, kKnown, _ := s.kalFeeExact("KXFEE-26TEST", true, 1, .40)
	if !ok || !kKnown || math.Abs(kMaker-wantKMaker) > 1e-9 ||
		!strings.Contains(kSource, "quadratic_with_maker_fees") {
		t.Fatalf("Kalshi maker fee=%v source=%q ok=%v, want %v", kMaker, kSource, ok, wantKMaker)
	}
	kTaker, _, ok := s.researchPaperRouteFee("kalshi", "KXFEE-26TEST", "taker", 1, .40)
	wantKTaker, _, _ := s.kalFeeExact("KXFEE-26TEST", false, 1, .40)
	if !ok || math.Abs(kTaker-wantKTaker) > 1e-9 || kTaker <= kMaker {
		t.Fatalf("Kalshi taker fee=%v ok=%v, want %v and above maker %v", kTaker, ok, wantKTaker, kMaker)
	}

	standard, custom, free := polyUSTakerTheta, .03, 0.0
	s.polyUSMu.Lock()
	s.pusSweep = []polymarketus.Market{
		{Slug: "pus-standard", FeeCoeff: &standard, TickSize: .01},
		{Slug: "pus-custom", FeeCoeff: &custom, TickSize: .01},
		{Slug: "pus-free", FeeCoeff: &free, TickSize: .01},
	}
	s.pusSweepAt = now
	s.polyUSMu.Unlock()

	// A one-share rebate rounds to zero at the venue's cent boundary. Ten shares prove that the
	// signed result is preserved rather than rejected or clamped by the research route.
	pMaker, pSource, ok := s.researchPaperRouteFee("polyus", "pus-standard", "maker", 10, .40)
	if want := polyUSFeeWithTheta(true, 10, .40, standard); !ok || pMaker != want || pMaker >= 0 ||
		pSource != polyUSFeeAuthoritySource {
		t.Fatalf("standard PolyUS maker fee=%v source=%q ok=%v, want signed rebate %v", pMaker, pSource, ok, want)
	}
	pTaker, _, ok := s.researchPaperRouteFee("polyus", "pus-standard", "taker", 1, .40)
	if want := polyUSFeeWithTheta(false, 1, .40, standard); !ok || pTaker != want || pTaker <= 0 {
		t.Fatalf("standard PolyUS taker fee=%v ok=%v, want %v", pTaker, ok, want)
	}
	for _, slug := range []string{"pus-custom", "pus-free"} {
		makerFee, _, makerOK := s.researchPaperRouteFee("polyus", slug, "maker", 1, .40)
		if !makerOK || makerFee != 0 {
			t.Fatalf("%s inherited standard maker rebate: fee=%v ok=%v", slug, makerFee, makerOK)
		}
	}
	customTaker, _, customOK := s.researchPaperRouteFee("polyus", "pus-custom", "taker", 1, .40)
	if want := polyUSFeeWithTheta(false, 1, .40, custom); !customOK || customTaker != want {
		t.Fatalf("custom PolyUS taker fee=%v ok=%v, want %v", customTaker, customOK, want)
	}
	if _, _, ok := s.researchPaperRouteFee("polyus", "pus-standard", "hybrid", 1, .40); ok {
		t.Fatal("ambiguous research route received fee authority")
	}
}

func TestR145MakerRebateIsEconomicTruthButNotAdvanceBuyingPower(t *testing.T) {
	s := testServer(t)
	theta := polyUSTakerTheta
	s.polyUSMu.Lock()
	s.pusSweep = []polymarketus.Market{{Slug: "pus-risk", FeeCoeff: &theta, TickSize: .01}}
	s.pusSweepAt = time.Now()
	s.polyUSMu.Unlock()

	rebate, _, known := s.researchPaperRouteFee("polyus", "pus-risk", "maker", 10, .40)
	if !known || rebate >= 0 {
		t.Fatalf("signed rebate unavailable: fee=%v known=%v", rebate, known)
	}
	s.riskMu.Lock()
	s.maxPerPos = 3.99
	s.riskMu.Unlock()
	if why := s.riskReject(context.Background(), "polyus", 10, .40, rebate); why == "" {
		t.Fatal("uncredited maker rebate reduced the risk reservation below principal")
	}

	s.pendMu.Lock()
	s.pendingMakers = []*pendingMaker{{platform: "polyus", ticker: "pus-risk", px: .40, contracts: 10}}
	s.pendMu.Unlock()
	reserved, count := s.pendingPortfolioExposure(vbPolyus)
	if count != 1 || math.Abs(reserved-4.00) > 1e-9 {
		t.Fatalf("pending reservation=%v count=%d, want principal 4.00/1 despite rebate %v", reserved, count, rebate)
	}
}

func TestR145MakerSalvagePolyUSUsesOneFullBookSnapshotAndItsClocks(t *testing.T) {
	bids := []polymarketus.BookLevel{{Price: .40, Quantity: 12}, {Price: .39, Quantity: 5}}
	asks := []polymarketus.BookLevel{{Price: .42, Quantity: 7}, {Price: .44, Quantity: 9}}
	if px, depth, ok := makerSalvagePolyUSSideQuote(bids, asks, "YES"); !ok || px != .42 || depth != 7 {
		t.Fatalf("YES quote=%v depth=%v ok=%v", px, depth, ok)
	}
	if px, depth, ok := makerSalvagePolyUSSideQuote(bids, asks, "NO"); !ok ||
		math.Abs(px-.60) > 1e-9 || depth != 12 {
		t.Fatalf("NO quote=%v depth=%v ok=%v", px, depth, ok)
	}

	decision := time.Date(2026, 7, 14, 12, 0, 2, 0, time.UTC)
	source := decision.Add(-2 * time.Second)
	received := decision.Add(-250 * time.Millisecond)
	age, latency, clockID, ok := makerSalvagePolyUSClocks(decision, source, received)
	if !ok || age != 2 || latency != 250 || !strings.Contains(clockID, source.Format(time.RFC3339Nano)) ||
		!strings.Contains(clockID, received.Format(time.RFC3339Nano)) {
		t.Fatalf("clocks age=%v latency=%v id=%q ok=%v", age, latency, clockID, ok)
	}
	if age, latency, _, ok = makerSalvagePolyUSClocks(decision, time.Time{}, received); !ok || age != .25 || latency != 250 {
		t.Fatalf("missing-source fallback age=%v latency=%v ok=%v", age, latency, ok)
	}
	if _, _, _, ok = makerSalvagePolyUSClocks(decision, source, decision.Add(time.Millisecond)); ok {
		t.Fatal("future local receipt clock was accepted")
	}
}

func TestR146MakerSalvageNativeOverlayRequiresItsOwnExactMakerCell(t *testing.T) {
	s := testServer(t)
	s.verdMu.Lock()
	s.verdCache = []verdictEnt{
		{SourceFamily: "kalshi-flow", Platform: "kalshi", Side: "YES", Route: "taker",
			Group: "taker", N: 500, Mean: .40, MeanAsk: .40, FeePC: .01},
		{SourceFamily: "kflow", Platform: "kalshi", Side: "NO", Route: "maker",
			Group: "maker", N: 100, Mean: .30, MeanAsk: .40, FeePC: .01},
		{SourceFamily: "kalshi-flow", Platform: "kalshi", Side: "YES", Route: "maker",
			Group: "maker", N: 20, Mean: .04, MeanAsk: .40, FeePC: .01},
	}
	s.verdAt = time.Now()
	s.verdMu.Unlock()
	point, ok := s.nativeMakerSystemRouteCurrentPoint("auto-cons-kalshi-flow", "kalshi", "YES", .42, .01, 1)
	if !ok || math.Abs(point-.02) > 1e-9 {
		t.Fatalf("current exact maker point=%v ok=%v", point, ok)
	}
	if _, ok := s.nativeMakerSystemRouteCurrentPoint("auto-cons-kalshi-flow", "kalshi", "NO", .40, .01, 1); ok {
		t.Fatal("opposite-side maker cell was borrowed")
	}
	if _, ok := s.nativeMakerSystemRouteCurrentPoint("auto-cons-kalshi-flow", "polyus", "YES", .40, .01, 1); ok {
		t.Fatal("other-venue maker cell was borrowed")
	}
	// Removing the actual maker row proves a positive taker history cannot manufacture the overlay.
	s.verdMu.Lock()
	s.verdCache = s.verdCache[:1]
	s.verdMu.Unlock()
	if _, ok := s.nativeMakerSystemRouteCurrentPoint("auto-cons-kalshi-flow", "kalshi", "YES", .40, .01, 1); ok {
		t.Fatal("taker evidence masqueraded as maker-overlay authority")
	}
}
