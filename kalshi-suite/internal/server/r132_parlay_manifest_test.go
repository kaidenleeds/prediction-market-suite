package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func TestR132ParlayRawSpaceUsesExactBigInt(t *testing.T) {
	// A stale 10-leg request is hard-capped at six, while the exact representation still handles
	// manifest universes whose six-leg logical space exceeds uint64.
	const want = "1387639652673653329500" // sum(C(10000,k), k=2..6)
	got := plabRawSpace(10000, 10)
	if got.String() != want {
		t.Fatalf("raw combination space = %s, want exact %s", got, want)
	}
	if got.Cmp(big.NewInt(0).SetUint64(^uint64(0))) <= 0 {
		t.Fatalf("test vector must exceed uint64, got %s", got)
	}
}

func TestR132ManifestKeepsMoreThan56LegsAndWritesOnlyBoundedProspectiveRows(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.mutateCfg(func(c *config.Config) {
		c.Auto.ParlayLabMaxLegs = 10 // stale config must normalize to the six-leg hard ceiling
		c.Auto.ParlayLabComboFloor = 0
	})
	s.plabQuoteFn = func(_ context.Context, l plabLeg) (float64, float64, string, bool) {
		return 0.40, 25, "test-executable-ask", true
	}
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }

	const n = 72 // regression: the retired materializer silently truncated its pool to 56
	legs := make([]plabLeg, 0, n)
	for i := 0; i < n; i++ {
		legs = append(legs, plabLeg{
			Ticker: fmt.Sprintf("KXR132-%03d", i), Side: "YES", Platform: "kalshi",
			PWin: 0.70, EventKey: fmt.Sprintf("event:%03d", i),
		})
	}
	s.plabWriteManifest(ctx, legs, 0.005)

	files, err := filepath.Glob(filepath.Join(s.cfg().DataDir, "parlay_manifests", "*.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("manifest files = %v (err=%v), want exactly one", files, err)
	}
	body, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var m plabManifest
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if len(m.Legs) != n {
		t.Fatalf("manifest retained %d legs, want all %d (>56)", len(m.Legs), n)
	}
	if m.MaxLegs != parlayLegLimit || m.RawSpaceDec != "171321438" { // sum(C(72,k), k=2..6)
		t.Fatalf("manifest max/raw = %d/%s, want %d/171321438", m.MaxLegs, m.RawSpaceDec, parlayLegLimit)
	}
	for _, l := range m.Legs {
		if l.Price != 0.40 || l.Depth != 25 || l.QuoteSrc != "test-executable-ask" || l.QuoteTS == "" {
			t.Fatalf("manifest leg lacks executable quote provenance: %+v", l)
		}
	}

	openN, err := s.store.PlabOpenCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	legacyLegs, err := s.store.PlabUnresolvedTickers(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if openN <= 0 || openN > plabSamplePerManifest || len(legacyLegs) > n {
		t.Fatalf("manifest cycle prospective layer is not bounded: combos=%d legs=%d", openN, len(legacyLegs))
	}
}

func TestR132ManifestWriteFailureStillCannotMaterializeLegacyRows(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.plabQuoteFn = func(_ context.Context, _ plabLeg) (float64, float64, string, bool) {
		return 0.40, 10, "test-executable-ask", true
	}
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }
	// Make the manifest directory path non-creatable. The cycle must fail closed instead of
	// activating the retired combo×leg SQLite materializer.
	blocked := filepath.Join(s.cfg().DataDir, "parlay_manifests")
	if err := os.WriteFile(blocked, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.plabWriteManifest(ctx, []plabLeg{
		{Ticker: "KXR132-A", Side: "YES", Platform: "kalshi", PWin: 0.70},
		{Ticker: "KXR132-B", Side: "YES", Platform: "kalshi", PWin: 0.70},
	}, 0.005)
	openN, err := s.store.PlabOpenCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	legacyLegs, err := s.store.PlabUnresolvedTickers(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if openN != 0 || len(legacyLegs) != 0 {
		t.Fatalf("failed manifest write activated legacy DB rows: combos=%d legs=%d", openN, len(legacyLegs))
	}
}

func TestR132ComboMoneyRequiresRealVenueQuote(t *testing.T) {
	s := testServer(t)
	r166AllowUnverifiedMultiLegPaperForTest(s)
	r107kal(s)
	ctx := context.Background()
	s.mutateCfg(func(c *config.Config) { c.Auto.XvgapComboOverlay = true })

	const gapTicker = "KXMLBGAME-26JUL10NYYBOS-NYY"
	const partnerTicker = "KXMLBTOTAL-26JUL10NYYBOS-8"
	seedKalMeta(s, kalshi.Market{Ticker: gapTicker, LastPrice: 0.50})
	seedKalMeta(s, kalshi.Market{Ticker: partnerTicker, LastPrice: 0.50})

	eventKey := s.legEventKey("kalshi", gapTicker, "")
	class := plabClass([]plabLeg{
		{Ticker: gapTicker, Platform: "kalshi", EventKey: eventKey},
		{Ticker: partnerTicker, Platform: "kalshi", EventKey: eventKey},
	}, eventKey)
	s.plabMu.Lock()
	if s.plabCorr == nil {
		s.plabCorr = map[string]*plabCorrC{}
	}
	s.plabCorr[class] = &plabCorrC{N: 100, A: 50, B: 50, AB: 50}
	s.plabMu.Unlock()

	// A large modeled gap guarantees the product-study branch is reached. The venue quote
	// adapter deliberately reports no actual MVE quote, so no money lot may be minted.
	s.xvComboTry(ctx, "xvgap", gapTicker, "YES", 0.50, 0.20, 1)
	s.xvcMu.Lock()
	open := append([]xvcPos(nil), s.xvcLoadLocked().Open...)
	s.xvcMu.Unlock()
	if len(open) == 0 {
		t.Fatal("fixture did not reach combo study branch")
	}
	for _, p := range open {
		if p.Expr == xvcExprSynth || p.Expr == xvcExprRFQ {
			t.Fatalf("combo money lot created without a real venue quote: %+v", p)
		}
		if p.Expr != xvcExprProduct {
			t.Fatalf("unexpected expression without a venue quote: %q", p.Expr)
		}
	}
}
