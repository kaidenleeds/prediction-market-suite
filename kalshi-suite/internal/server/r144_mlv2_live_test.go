package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeR144NewMLFiles(t *testing.T, s *Server, open, closed []map[string]any) {
	writeR144NewMLFilesWithAuthority(t, s, open, closed, true)
}

func writeR144NewMLFilesWithAuthority(t *testing.T, s *Server, open, closed []map[string]any, sealed bool) {
	t.Helper()
	cfg := *s.cfg()
	cfg.Risk.LiveNewMLKalshi = true
	cfg.Risk.LiveNewMLPolyUS = true
	s.cfgP.Store(&cfg)
	now := time.Now().Unix()
	pred := map[string]any{
		"generated_at": now, "feature_schema": currentMLCohort, "model_cohort": currentMLCohort,
		"model_version": "r144-current", "model_lineage": newMLV2ModelLineage,
		"execution_enabled": true, "paper_authority": true,
		"live_authority": sealed,
		"live_validation": map[string]any{"state": map[bool]string{true: "SEALED_UNTOUCHED_PASS", false: "READY_FOR_SEPARATE_REPLICATION"}[sealed],
			"authority": sealed, "model_version": "r144-current", "replication_id": map[bool]string{true: "r144-sealed", false: ""}[sealed],
			"frozen_before_test": sealed, "untouched": sealed},
		"predictions": []map[string]any{{"platform": "kalshi", "ticker": "KXMLV2", "side": "YES",
			"p_win": .70, "live_price": .40, "ev_net_live": .25},
			{"platform": "polyus", "ticker": "pus-mlv2", "side": "NO",
				"p_win": .68, "live_price": .41, "ev_net_live": .23}},
	}
	book := map[string]any{"epoch_id": "r144-ml-v2", "reset_at": time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano),
		"current_model_cohort": currentMLCohort, "open": open, "closed": closed}
	for name, v := range map[string]any{"ml_predictions.json": pred, "ml_paper.json": book} {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(s.cfg().DataDir, name), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func r144NewMLOpen(now int64) map[string]any {
	return map[string]any{"platform": "kalshi", "ticker": "KXMLV2", "title": "current ML",
		"side": "YES", "model_cohort": currentMLCohort, "model_version": "r144-current", "model_lineage": newMLV2ModelLineage, "fill_kind": "taker", "price": .40,
		"p_win": .70, "ev_net": .25, "contracts": 12.0, "opened": now, "decision_ts": now,
		"fee_known": true, "epoch_id": "r144-ml-v2"}
}

func r144NewMLClosed(platform, ticker, side, route string, contracts, pnl float64, feeKnown bool) map[string]any {
	return map[string]any{"platform": platform, "ticker": ticker, "side": side,
		"model_cohort": currentMLCohort, "model_version": "r144-current", "model_lineage": newMLV2ModelLineage,
		"fill_kind": route, "price": .40, "contracts": contracts, "opened": time.Now().Unix(), "decision_ts": time.Now().Unix(),
		"pnl": pnl, "won": 1, "fee_known": feeKnown, "epoch_id": "r144-ml-v2",
		"entry_resolve_hours": 1.0, "entry_is_crypto": false}
}

func r144NewMLProofRows(platform, side, route string, pnlPC float64) []map[string]any {
	rows := make([]map[string]any, 0, newMLV2MinProofMarkets)
	for i := 0; i < newMLV2MinProofMarkets; i++ {
		rows = append(rows, r144NewMLClosed(platform, fmt.Sprintf("%s-%s-%s-%02d", platform, side, route, i),
			side, route, 10, 10*pnlPC, true))
	}
	return rows
}

func TestR144NewMLV2OnlyActualFreshPaperDecisionEntersMirror(t *testing.T) {
	s := testServer(t)
	s.liveArmed, s.liveAuto = true, true
	now := time.Now().Unix()
	writeR144NewMLFiles(t, s, []map[string]any{r144NewMLOpen(now)}, nil)
	if got := s.enqueueNewMLV2LiveCandidates(t.Context()); got != 1 {
		t.Fatalf("current actual New-ML Paper decision queued=%d want 1", got)
	}
	if len(s.liveMirrorQ) != 1 || !isNewMLV2LiveSource(s.liveMirrorQ[0].Source) ||
		s.liveMirrorQ[0].Family != "new-ml-v2" {
		t.Fatalf("opaque current-v2 candidate missing: %+v", s.liveMirrorQ)
	}
	if got := s.enqueueNewMLV2LiveCandidates(t.Context()); got != 0 {
		t.Fatalf("duplicate current Paper decision queued again: %d", got)
	}
	if bank, _, _ := s.newMLV2SizingBankroll("kalshi"); math.Abs(bank-(300-12*.40-12)) > .011 {
		t.Fatalf("dedup refusal leaked a second capability reservation: available=%v", bank)
	}

	legacy := s.liveMirrorQ[0]
	legacy.Source, legacy.Family = "ml-book", ""
	legacy.Ticker = "LEGACY"
	if s.enqueueLiveMirrorCandidate(legacy) {
		t.Fatal("raw/legacy ML source bypassed the opaque current-v2 capability")
	}

	s.liveMirrorQ, s.liveMirrorSeen = nil, nil
	old := r144NewMLOpen(time.Now().Add(-liveMirrorTTL - time.Second).Unix())
	writeR144NewMLFiles(t, s, []map[string]any{old}, nil)
	if got := s.enqueueNewMLV2LiveCandidates(t.Context()); got != 0 || len(s.liveMirrorQ) != 0 {
		t.Fatalf("old Paper decision replayed after arming: queued=%d q=%+v", got, s.liveMirrorQ)
	}
}

func TestR144NewMLV2ReceiptAuthorityCannotBeBypassed(t *testing.T) {
	s := testServer(t)
	s.liveArmed, s.liveAuto = true, true
	writeR144NewMLFilesWithAuthority(t, s, []map[string]any{r144NewMLOpen(time.Now().Unix())},
		r144NewMLProofRows("kalshi", "YES", "taker", .20), false)
	if got := s.enqueueNewMLV2LiveCandidates(t.Context()); got != 0 || len(s.liveMirrorQ) != 0 {
		t.Fatalf("Paper/model point results bypassed sealed untouched live authority: queued=%d q=%+v", got, s.liveMirrorQ)
	}
}

func TestR144NewMLV2PaperEvidenceIsVenueRouteFeeAndCohortExact(t *testing.T) {
	s := testServer(t)
	closed := []map[string]any{
		r144NewMLClosed("kalshi", "K-NOFEE", "YES", "taker", 10, 100, false),
		{"platform": "kalshi", "ticker": "LEGACY", "side": "YES", "fill_kind": "taker",
			"model_cohort": "legacy-v1", "contracts": 10.0, "pnl": 100.0, "fee_known": true,
			"epoch_id": "r144-ml-v2"},
	}
	for i := 0; i < newMLV2MinProofMarkets; i++ {
		closed = append(closed,
			r144NewMLClosed("kalshi", fmt.Sprintf("K-T-%02d", i), "YES", "taker", 10, 2, true),
			r144NewMLClosed("polyus", fmt.Sprintf("P-T-%02d", i), "NO", "taker", 10, 3, true),
			r144NewMLClosed("kalshi", fmt.Sprintf("K-M-%02d", i), "YES", "maker", 10, -1, true))
	}
	// Same ticker repeated is still one independent market, and opposite-side/old-model wins
	// cannot leak into this exact cohort.
	closed = append(closed, r144NewMLClosed("kalshi", "K-T-00", "YES", "taker", 10, 2, true))
	old := r144NewMLClosed("kalshi", "OLD-WIN", "YES", "taker", 10, 100, true)
	old["model_version"] = "older-model"
	closed = append(closed, old, r144NewMLClosed("kalshi", "NO-WIN", "NO", "taker", 10, 100, true))
	writeR144NewMLFiles(t, s, nil, closed)
	if mean, lo, n, why := s.newMLV2PositivePaperRoute("kalshi", "YES", "taker", "r144-current"); why != "" || n != newMLV2MinProofMarkets || math.Abs(mean-.20) > 1e-8 || math.Abs(lo-.20) > 1e-8 {
		t.Fatalf("Kalshi/YES/taker evidence mean=%v lo=%v m=%d why=%q", mean, lo, n, why)
	}
	if mean, lo, n, why := s.newMLV2PositivePaperRoute("polyus", "NO", "taker", "r144-current"); why != "" || n != newMLV2MinProofMarkets || math.Abs(mean-.30) > 1e-8 || math.Abs(lo-.30) > 1e-8 {
		t.Fatalf("PolyUS/NO/taker evidence mean=%v lo=%v m=%d why=%q", mean, lo, n, why)
	}
	if mean, lo, n, why := s.newMLV2PositivePaperRoute("kalshi", "YES", "maker", "r144-current"); why != "new-ml-exact-paper-cohort-lower-bound-not-positive-after-fees" || n != newMLV2MinProofMarkets || mean >= 0 || lo >= 0 {
		t.Fatalf("negative maker route qualified: mean=%v lo=%v m=%d why=%q", mean, lo, n, why)
	}
	if _, _, n, why := s.newMLV2PositivePaperRoute("kalshi", "NO", "taker", "r144-current"); n != 1 || why != "insufficient-independent-markets-for-new-ml-live-proof" {
		t.Fatalf("one opposite-side win borrowed authority: m=%d why=%q", n, why)
	}
	if _, _, n, why := s.newMLV2PositivePaperRoute("kalshi", "YES", "taker", "older-model"); n != 1 || why != "insufficient-independent-markets-for-new-ml-live-proof" {
		t.Fatalf("one old-model win borrowed authority: m=%d why=%q", n, why)
	}
}

func TestR165NewMLV2PaperCapabilityCannotAuthorizeCash(t *testing.T) {
	s := testServer(t)
	s.liveArmed, s.liveAuto = true, true
	now := time.Now().Unix()
	writeR144NewMLFiles(t, s, []map[string]any{r144NewMLOpen(now)},
		r144NewMLProofRows("kalshi", "YES", "taker", .20))
	if s.enqueueNewMLV2LiveCandidates(t.Context()) != 1 {
		t.Fatal("current capability was not queued")
	}
	source := s.liveMirrorQ[0].Source
	if ok, _ := s.authorizeNewMLV2LiveHandler(source, "kalshi", "KXMLV2", "NO", "taker"); ok {
		t.Fatal("capability crossed to the opposite side")
	}
	if ok, why := s.authorizeNewMLV2LiveHandler(source, "kalshi", "KXMLV2", "YES", "taker"); ok || why != newMLV2LiveCashRetiredReason {
		t.Fatalf("Paper-derived New-ML capability reached cash: ok=%v why=%q", ok, why)
	}
	if ok, _ := s.authorizeNewMLV2LiveHandler(source, "kalshi", "KXMLV2", "YES", "taker"); ok {
		t.Fatal("one-use New-ML handler capability replayed")
	}
	if ok, _ := s.authorizeNewMLV2LiveHandler(newMLV2LiveSourcePrefix+"forged", "kalshi", "KXMLV2", "YES", "taker"); ok {
		t.Fatal("forged New-ML source reached an AUTO handler")
	}
}

func TestR144NewMLV2StaleSetUsesFreshExactOutcomeWithoutLegacyAuthority(t *testing.T) {
	s := testServer(t)
	writeR144NewMLFiles(t, s, nil, nil)
	legacy, current, legacyLoaded, currentLoaded := s.staleEVSet()
	if !legacyLoaded || !currentLoaded {
		t.Fatalf("fresh current receipt was not loaded: legacy=%v current=%v", legacyLoaded, currentLoaded)
	}
	if legacy["KXMLV2"] {
		t.Fatal("retired live_authority=false receipt authorized the legacy ML stale set")
	}
	if !current["kalshi|KXMLV2|YES"] {
		t.Fatalf("fresh book-native-v2 YES signal absent from exact stale set: %+v", current)
	}
	if current["kalshi|KXMLV2|NO"] {
		t.Fatal("YES model signal incorrectly kept an opposite-side NO order alive")
	}
}

func TestR144NewMLV2SizesFromCurrentDestinationSleeveEquity(t *testing.T) {
	s := testServer(t)
	closed := []map[string]any{
		r144NewMLClosed("kalshi", "K-WIN", "YES", "taker", 10, 60, true),
		r144NewMLClosed("polyus", "P-LOSS", "NO", "taker", 10, -100, true),
	}
	writeR144NewMLFiles(t, s, nil, closed)

	if bank, ref, src := s.newMLV2SizingBankroll("kalshi"); math.Abs(bank-360) > 0.011 || ref != 300 || src != "current-new-ml-kalshi-sleeve-available" {
		t.Fatalf("Kalshi New-ML bank=%v ref=%v src=%q, want its independently compounded $360/$300 sleeve", bank, ref, src)
	}
	if bank, ref, src := s.newMLV2SizingBankroll("polyus"); math.Abs(bank-200) > 0.011 || ref != 300 || src != "current-new-ml-polyus-sleeve-available" {
		t.Fatalf("PolyUS New-ML bank=%v ref=%v src=%q, want its independently compounded $200/$300 sleeve", bank, ref, src)
	}
	if bank, _, _ := s.newMLV2SizingBankroll("unknown"); bank != 0 {
		t.Fatalf("missing destination sleeve fell back to total/fixed grant: %v", bank)
	}
}

func TestR144NewMLV2SleeveUsesAvailableAndCumulativeCapabilityReservations(t *testing.T) {
	s := testServer(t)
	now := time.Now().Unix()
	open := r144NewMLOpen(now)
	open["contracts"], open["price"], open["fee"] = 100.0, .50, 5.0
	writeR144NewMLFiles(t, s, []map[string]any{open}, nil)
	if bank, _, _ := s.newMLV2SizingBankroll("kalshi"); math.Abs(bank-245) > .011 {
		t.Fatalf("sleeve sized from equity or ignored open principal+fee: available=%v want 245", bank)
	}
	c1 := liveMirrorCandidate{Platform: "kalshi", Ticker: "K-CAP-1", Side: "YES", Price: .40, At: time.Now()}
	c2 := liveMirrorCandidate{Platform: "kalshi", Ticker: "K-CAP-2", Side: "YES", Price: .40, At: time.Now()}
	var ok bool
	if c1, ok = registerNewMLV2LiveCapability(s, c1, "taker", "r144-current", 100); !ok {
		t.Fatal("first capability failed despite available sleeve")
	}
	if c2, ok = registerNewMLV2LiveCapability(s, c2, "taker", "r144-current", 100); !ok {
		t.Fatal("second capability failed despite remaining sleeve")
	}
	if bank, _, _ := s.newMLV2SizingBankrollFor("kalshi", c1.Source); math.Abs(bank-145) > .011 {
		t.Fatalf("own capability did not exclude only the other reservation: available=%v want 145", bank)
	}
	if bank, _, _ := s.newMLV2SizingBankroll("kalshi"); math.Abs(bank-45) > .011 {
		t.Fatalf("cumulative capabilities reused sleeve dollars: available=%v want 45", bank)
	}
	if _, ok := registerNewMLV2LiveCapability(s,
		liveMirrorCandidate{Platform: "kalshi", Ticker: "K-CAP-3", Side: "YES", Price: .40, At: time.Now()},
		"taker", "r144-current", 50); ok {
		t.Fatal("third capability overcommitted destination sleeve")
	}
}

func TestR148NewMLV2LiveSizingUsesFreshVenueNAVButRetainsPaperCapacityGate(t *testing.T) {
	s := testServer(t)
	closed := []map[string]any{
		r144NewMLClosed("kalshi", "K-WIN", "YES", "taker", 10, 60, true),
	}
	writeR144NewMLFiles(t, s, nil, closed)
	s.liveBankArm = map[string]float64{"kalshi": 100}
	cash, cashOK := 100.0, true
	s.liveBankCashRead = func(context.Context, string) (float64, bool) { return cash, cashOK }

	if bank, src := s.newMLV2LiveSizingBankrollFor(context.Background(), "kalshi", ""); bank != 100 ||
		!strings.Contains(src, "venue;paper-capacity=current-new-ml-kalshi-sleeve-available") {
		t.Fatalf("New-ML LIVE bank/source = %.2f/%q, want fresh $100 venue NAV plus Paper capacity proof", bank, src)
	}
	cash = 200
	if bank, _ := s.newMLV2LiveSizingBankrollFor(context.Background(), "kalshi", ""); bank != 200 {
		t.Fatalf("fresh deposit did not enlarge New-ML LIVE sizing NAV: %.2f", bank)
	}
	cashOK = false
	if bank, _ := s.newMLV2LiveSizingBankrollFor(context.Background(), "kalshi", ""); bank != 0 {
		t.Fatalf("unreadable destination NAV retained New-ML buying power: %.2f", bank)
	}

	// The live-dollar source changed, but the Paper capability remains the independent contract
	// capacity ceiling at dispatch; a larger deposit cannot manufacture proof units.
	if count, why := liveMirrorSizedCount(100, .50, 1000, 10, 1, true); why != "" || count != 10 {
		t.Fatalf("Paper capacity failed to cap deposited LIVE dollars: count=%v why=%q", count, why)
	}
}

func TestR144LiveMirrorSizingUsesBalanceDepthAndProofCapacity(t *testing.T) {
	if count, why := liveMirrorSizedCount(20, .50, 100, 12, 1, true); why != "" || count != 12 {
		t.Fatalf("large destination bankroll should reach the 12-unit Paper proof cap: count=%v why=%q", count, why)
	}
	if count, why := liveMirrorSizedCount(1.20, .50, 100, 12, 1, true); why != "" || count != 2 {
		t.Fatalf("smaller destination bankroll should size two: count=%v why=%q", count, why)
	}
	if count, why := liveMirrorSizedCount(20, .50, 3, 12, 1, true); why != "" || count != 3 {
		t.Fatalf("fresh touch depth should cap size at three: count=%v why=%q", count, why)
	}
	if count, why := liveMirrorSizedCount(20, .50, 2, 12, 3, true); count != 0 || why == "" {
		t.Fatalf("venue minimum must not round above fresh depth: count=%v why=%q", count, why)
	}
}

func TestR144NewMLV2CurrentPointEdgeIsNotItsOwnConfidenceBound(t *testing.T) {
	mean, lower := newMLV2ConservativeLiveEdges(.25, .08, .03)
	if mean != .08 || lower != .03 {
		t.Fatalf("point forecast was reused as confidence proof: mean=%v lower=%v", mean, lower)
	}
	mean, lower = newMLV2ConservativeLiveEdges(.02, .08, .03)
	if mean != .02 || lower != .02 {
		t.Fatalf("worse current economics did not tighten both proof edges: mean=%v lower=%v", mean, lower)
	}
}
