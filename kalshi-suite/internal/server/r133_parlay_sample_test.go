package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r133SamplePool(n int) []plabLeg {
	out := make([]plabLeg, 0, n)
	for i := 0; i < n; i++ {
		event := fmt.Sprintf("event:%02d", i)
		if i < 4 {
			event = "kgame:alpha"
		} else if i < 8 {
			event = "kgame:beta"
		}
		out = append(out, plabLeg{Ticker: fmt.Sprintf("KXR133-%03d", i), Side: "YES", Platform: "kalshi",
			Price: 0.40, PWin: 0.70, EVNet: 0.30, EventKey: event, TwinKey: fmt.Sprintf("twin:%03d", i)})
	}
	return out
}

func TestR133ProspectiveSampleStableAndStratified(t *testing.T) {
	pool := r133SamplePool(24)
	a, epA := plabStableProspectiveSample(pool, 4, 72)
	for i, j := 0, len(pool)-1; i < j; i, j = i+1, j-1 {
		pool[i], pool[j] = pool[j], pool[i]
	}
	b, epB := plabStableProspectiveSample(pool, 4, 72)
	if epA != epB || len(a) != len(b) || len(a) == 0 {
		t.Fatalf("sample episode/order instability: ep %q/%q n %d/%d", epA, epB, len(a), len(b))
	}
	legsSeen, buckets := map[int]bool{}, map[string]bool{}
	ids := map[string]bool{}
	for i := range a {
		if a[i].ID != b[i].ID {
			t.Fatalf("input reorder changed sample at %d: %s != %s", i, a[i].ID, b[i].ID)
		}
		if ids[a[i].ID] {
			t.Fatalf("duplicate sampled combo id %s", a[i].ID)
		}
		ids[a[i].ID] = true
		legsSeen[len(a[i].Legs)] = true
		buckets[a[i].Bucket] = true
	}
	for _, k := range []int{2, 3, 4} {
		if !legsSeen[k] {
			t.Fatalf("leg-count stratum %d absent: %+v", k, legsSeen)
		}
	}
	if !buckets["overlap"] || !buckets["indep"] {
		t.Fatalf("linkage strata absent: %+v", buckets)
	}
}

func TestR133ManifestGradeSampleBoundedAndEpisodeDeduped(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.mutateCfg(func(c *config.Config) {
		c.Auto.ParlayLabMaxLegs = 4
		c.Auto.ParlayLabComboFloor = 0
	})
	s.plabQuoteFn = func(context.Context, plabLeg) (float64, float64, string, bool) {
		return 0.40, 25, "test-executable-ask", true
	}
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }
	pool := r133SamplePool(24)

	s.plabWriteManifest(ctx, pool, 0.005)
	first, err := s.store.PlabOpenCount(ctx)
	if err != nil || first <= 0 || first > plabSamplePerManifest {
		t.Fatalf("first prospective sample rows=%d err=%v, want 1..%d", first, err, plabSamplePerManifest)
	}
	if s.plabEnum.Enumerated != 0 || s.plabEnum.SampleInserted != int(first) || s.plabEnum.SampleOpenCap != plabSampleOpenCap {
		t.Fatalf("manifest/sample semantics drifted: %+v", s.plabEnum)
	}
	// Same still-open universe chooses the same stable IDs; INSERT OR IGNORE is durable episode
	// dedup even though the manifest itself has a new timestamp and quote snapshot.
	s.plabWriteManifest(ctx, pool, 0.005)
	second, err := s.store.PlabOpenCount(ctx)
	if err != nil || second != first || s.plabEnum.SampleInserted != 0 {
		t.Fatalf("same-episode re-manifest duplicated sample: first=%d second=%d inserted=%d err=%v",
			first, second, s.plabEnum.SampleInserted, err)
	}
}

func TestR133ProspectiveSampleOpenCapAndExactFeeGate(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	if _, err := s.plabCurrentAdmissionEpoch(ctx, time.Now().UTC(), true); err != nil {
		t.Fatal(err)
	}
	prefill := make([]storage.PlabCand, 0, plabSampleOpenCap-4)
	for i := 0; i < plabSampleOpenCap-4; i++ {
		prefill = append(prefill, storage.PlabCand{ID: fmt.Sprintf("prefill-%04d", i), At: time.Now().Unix(),
			Legs: "[]", LegFees: "[]", NLegs: 2})
	}
	if n, err := s.store.PlabInsertBatch(ctx, prefill); err != nil || n != int64(len(prefill)) {
		t.Fatalf("prefill n=%d err=%v", n, err)
	}
	s.plabQuoteFn = func(context.Context, plabLeg) (float64, float64, string, bool) {
		return 0.40, 25, "test-executable-ask", true
	}
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }
	s.plabWriteManifest(ctx, r133SamplePool(18), 0.005)
	if n, _ := s.store.PlabOpenCount(ctx); n > plabSampleOpenCap {
		t.Fatalf("prospective open rows exceeded cap: %d > %d", n, plabSampleOpenCap)
	}

	// No exact fee receipt means the manifest remains complete but the prospective grade layer
	// fails closed. A separate server avoids the prefilled-cap path masking this assertion.
	s2 := testServer(t)
	s2.plabQuoteFn = s.plabQuoteFn
	s2.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, false }
	s2.plabWriteManifest(ctx, r133SamplePool(18), 0.005)
	if n, _ := s2.store.PlabOpenCount(ctx); n != 0 || s2.plabEnum.SampleEligible != 0 {
		t.Fatalf("unknown exact fee admitted prospective rows: open=%d enum=%+v", n, s2.plabEnum)
	}
}

func TestR143ProspectiveCapExcludesLegacyAboveSixLegRows(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	if _, err := s.plabCurrentAdmissionEpoch(ctx, time.Now().UTC(), true); err != nil {
		t.Fatal(err)
	}
	prefill := make([]storage.PlabCand, 0, 540)
	for i := 0; i < 160; i++ {
		prefill = append(prefill, storage.PlabCand{ID: fmt.Sprintf("legacy-8leg-%03d", i),
			At: time.Now().Unix(), Legs: "[]", LegFees: "[]", NLegs: 8})
	}
	for i := 0; i < plabSampleOpenCap-4; i++ {
		prefill = append(prefill, storage.PlabCand{ID: fmt.Sprintf("current-2leg-%03d", i),
			At: time.Now().Unix(), Legs: "[]", LegFees: "[]", NLegs: 2})
	}
	if n, err := s.store.PlabInsertBatch(ctx, prefill); err != nil || n != int64(len(prefill)) {
		t.Fatalf("prefill n=%d err=%v", n, err)
	}
	s.plabQuoteFn = func(context.Context, plabLeg) (float64, float64, string, bool) {
		return .40, 25, "test-executable-ask", true
	}
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }
	s.plabWriteManifest(ctx, r133SamplePool(18), .005)
	current, err := s.store.PlabOpenCurrentCount(ctx, 6)
	if err != nil || current != plabSampleOpenCap {
		t.Fatalf("current sample count=%d err=%v, want cap=%d", current, err, plabSampleOpenCap)
	}
	total, _ := s.store.PlabOpenCount(ctx)
	if total != int64(plabSampleOpenCap+160) || s.plabEnum.SampleInserted != 4 ||
		s.plabEnum.SampleLegacyOpen != 160 || s.plabEnum.SampleCurrentOpen != plabSampleOpenCap {
		t.Fatalf("legacy rows consumed current cap: total=%d enum=%+v", total, s.plabEnum)
	}
}

func TestR133ParlayPolyIntRequiresExactOutcomeBook(t *testing.T) {
	q := xvlPintQuote{BestAsk: 0.41, BestAskDepth: 12, NoBestAsk: 0.63, NoBestAskDepth: 0}
	if ask, depth, src, ok := plabPintOutcomeAsk(q, "NO", true); ok || ask != 0 || depth != 0 || src != "polyint-ws" {
		t.Fatalf("zero-depth NO token quote counted as executable: ask=%v depth=%v src=%q ok=%v", ask, depth, src, ok)
	}
	q.NoBestAskDepth = 9
	if ask, depth, src, ok := plabPintOutcomeAsk(q, "NO", true); !ok || ask != 0.63 || depth != 9 || src != "polyint-ws" {
		t.Fatalf("exact NO token quote/depth was not preserved: ask=%v depth=%v src=%q ok=%v", ask, depth, src, ok)
	}
	if ask, _, _, ok := plabPintOutcomeAsk(q, "NO", false); ok || ask != 0 {
		t.Fatalf("stale/missing CLOB quote fell back to a synthetic complement: ask=%v ok=%v", ask, ok)
	}
	if ask, depth, _, ok := plabPintOutcomeAsk(q, "YES", true); !ok || ask != 0.41 || depth != 12 {
		t.Fatalf("exact YES token quote/depth drifted: ask=%v depth=%v ok=%v", ask, depth, ok)
	}
}

func TestR133ManifestQuotesConcurrentlyWithoutDroppingLegs(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.mutateCfg(func(c *config.Config) { c.Auto.ParlayLabMaxLegs = 3 })
	var mu sync.Mutex
	active, peak := 0, 0
	s.plabQuoteFn = func(context.Context, plabLeg) (float64, float64, string, bool) {
		mu.Lock()
		active++
		if active > peak {
			peak = active
		}
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()
		return 0.40, 10, "test-executable-ask", true
	}
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }
	pool := r133SamplePool(32)
	s.plabWriteManifest(ctx, pool, 0.005)
	if peak < 2 {
		t.Fatalf("manifest quote refresh remained serial: peak concurrency=%d", peak)
	}
	files, err := filepath.Glob(filepath.Join(s.cfg().DataDir, "parlay_manifests", "*.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("manifest files=%v err=%v", files, err)
	}
	body, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var m plabManifest
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatal(err)
	}
	if len(m.Legs) != len(pool) {
		t.Fatalf("concurrent quote refresh dropped logical pool legs: got=%d want=%d", len(m.Legs), len(pool))
	}
}

func TestR140ComboSampleSeparatesPromotedSystemsAcrossTwoThroughSixLegs(t *testing.T) {
	pool := r133SamplePool(24)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for i := 0; i < 12; i++ {
		pool[i].Promoted = true
		pool[i].SystemID = fmt.Sprintf("sealed-system-%02d", i)
		pool[i].ProofRunID = int64(100 + i)
		pool[i].ProofResultHash = fmt.Sprintf("%064x", i+1)
		pool[i].ProofMeanAllIn = .40
		pool[i].ProofLowerPC = .02
		pool[i].ProofCapacity = 10
		pool[i].ProofRoute = "taker"
		pool[i].ProofObserved = now
	}
	rows, _ := plabStableProspectiveSample(pool, 6, 240)
	seen := map[int]bool{}
	for _, row := range rows {
		if row.Cohort != storage.ComboLabCohortPromotedSystem {
			continue
		}
		seen[len(row.Legs)] = true
		for _, leg := range row.Legs {
			if !leg.Promoted || leg.SystemID == "" || leg.ProofRunID <= 0 ||
				leg.ProofResultHash == "" || leg.ProofCapacity < 1 || leg.ProofRoute != "taker" ||
				leg.Platform != "kalshi" {
				t.Fatalf("promoted cohort admitted an unsealed leg: %+v", leg)
			}
		}
	}
	for legs := 2; legs <= 6; legs++ {
		if !seen[legs] {
			t.Fatalf("promoted-system cohort did not cover %d-leg rows: %+v", legs, seen)
		}
	}
}

func TestR143ComboSampleAddsPaperOnlyRollingPositiveSystemsTwoThroughSix(t *testing.T) {
	pool := r133SamplePool(18)
	for i := range pool {
		if i >= 9 {
			pool[i].Platform = "polyus"
			pool[i].Ticker = fmt.Sprintf("pus-r143-%02d", i)
		}
		pool[i].RollingPositive = true
		pool[i].PositiveSystems = []string{fmt.Sprintf("positive-system-%02d", i%5)}
		pool[i].SystemRoute = "taker"
	}
	rows, _ := plabStableProspectiveSample(pool, 6, 240)
	seen := map[int]bool{}
	venues := map[string]bool{}
	for _, row := range rows {
		if row.Cohort != storage.ComboLabCohortRollingPositive {
			continue
		}
		seen[len(row.Legs)] = true
		venue := row.Legs[0].Platform
		venues[venue] = true
		for _, leg := range row.Legs {
			if !leg.RollingPositive || len(leg.PositiveSystems) == 0 || leg.SystemRoute != "taker" ||
				leg.Promoted || leg.Platform != venue {
				t.Fatalf("rolling PAPER cohort admitted invalid/sealed leg: %+v", leg)
			}
		}
	}
	for legs := 2; legs <= 6; legs++ {
		if !seen[legs] {
			t.Fatalf("rolling-positive cohort did not cover %d legs: %+v", legs, seen)
		}
	}
	if !venues["kalshi"] || !venues["polyus"] {
		t.Fatalf("rolling-positive cohort did not independently cover both executable venues: %+v", venues)
	}
}

func TestR143RollingPositiveRowsPersistPaperOnlyAndNotSealed(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.mutateCfg(func(c *config.Config) { c.Auto.ParlayLabMaxLegs = 6 })
	s.plabQuoteFn = func(context.Context, plabLeg) (float64, float64, string, bool) {
		return .40, 25, "test-fresh-book", true
	}
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }
	pool := r133SamplePool(12)
	for i := range pool {
		pool[i].RollingPositive = true
		pool[i].PositiveSystems = []string{fmt.Sprintf("positive-system-%02d", i)}
		pool[i].SystemRoute = "taker"
	}
	s.plabWriteManifest(ctx, pool, .005)
	counts, err := s.store.PlabOpenCohortCounts(ctx)
	if err != nil || counts[storage.ComboLabCohortRollingPositive] == 0 ||
		counts[storage.ComboLabCohortPromotedSystem] != 0 {
		t.Fatalf("rolling/sealed cohort counts=%+v err=%v", counts, err)
	}
	for _, leg := range pool {
		if _, err := s.store.PlabResolveTicker(ctx, leg.Platform, leg.Ticker, 1); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := s.store.PlabGradable(ctx, time.Now().Add(time.Minute).Unix(), 500)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range rows {
		if row.Cohort == storage.ComboLabCohortRollingPositive {
			found = true
			if row.RouteState != "paper-only-rolling-positive" {
				t.Fatalf("rolling row route state=%q", row.RouteState)
			}
		}
	}
	if !found {
		t.Fatal("no gradable rolling-positive PAPER row")
	}
}

func TestR140PromotedComboRowsPersistAwaitingRFQAndNeverClaimLiveTransfer(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.mutateCfg(func(c *config.Config) { c.Auto.ParlayLabMaxLegs = 6 })
	s.plabQuoteFn = func(context.Context, plabLeg) (float64, float64, string, bool) {
		return .40, 25, "test-fresh-book", true
	}
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }
	pool := r133SamplePool(12)
	now := time.Now().UTC()
	for i := range pool {
		pool[i].Promoted = true
		pool[i].SystemID = fmt.Sprintf("sealed-system-%02d", i)
		pool[i].ProofRunID = int64(200 + i)
		pool[i].ProofResultHash = fmt.Sprintf("%064x", i+100)
		pool[i].ProofMeanAllIn = .40
		pool[i].ProofLowerPC = .30
		pool[i].ProofCapacity = 10
		pool[i].ProofRoute = "taker"
		pool[i].ProofObserved = now.Format(time.RFC3339Nano)
	}
	s.plabWriteManifest(ctx, pool, .005)
	counts, err := s.store.PlabOpenCohortCounts(ctx)
	if err != nil || counts[storage.ComboLabCohortPromotedSystem] == 0 {
		t.Fatalf("promoted cohort not persisted: counts=%+v err=%v enum=%+v", counts, err, s.plabEnum)
	}
	for _, leg := range pool {
		if _, err := s.store.PlabResolveTicker(ctx, leg.Platform, leg.Ticker, 1); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := s.store.PlabGradable(ctx, now.Add(time.Minute).Unix(), 500)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range rows {
		if row.Cohort == storage.ComboLabCohortPromotedSystem {
			found = true
			if row.RouteState != "awaiting-rfq-identical-proof" {
				t.Fatalf("promoted row falsely implied route proof: %+v", row)
			}
		}
	}
	if !found {
		t.Fatal("no gradable promoted-system row found")
	}

	rr := httptest.NewRecorder()
	s.handleParlayLab(rr, httptest.NewRequest("GET", "/api/parlaylab", nil))
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["name"] != "Combo Lab" || body["live_transferable"] != false ||
		!strings.Contains(fmt.Sprint(body["note"]), "not placed profit") {
		t.Fatalf("Combo Lab API truth missing: %#v", body)
	}
	if body["current_max_legs"] != float64(parlayLegLimit) || body["open_by_legs"] == nil ||
		body["open_within_leg_limit"] == nil || body["open_above_leg_limit"] == nil {
		t.Fatalf("Combo Lab open-vs-settled denominator fields missing: %#v", body)
	}
}
