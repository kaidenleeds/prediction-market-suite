package server

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func behavioralTestObservation(id int64, opened time.Time, ticker, signal string) storage.BehavioralBiasObservation {
	return storage.BehavioralBiasObservation{
		ID: id, ObservedAt: opened, ResolvedAt: opened.Add(time.Hour), Venue: "kalshi",
		Ticker: ticker, CanonicalEvent: "fixture:" + ticker, Side: "YES", SignalType: signal, Category: "Sports", Kind: "winner",
		BookSource: "kalshi_book_ws", Bid: .20, BidKnown: true, Ask: .40, AskDepth: 4,
		TakerFeePC: .01, Payout: 1, ResolveHours: 4, IsLive: 0,
	}
}

func TestBehavioralBiasWalkForwardIsDeterministicCompleteDayAndDedupSafe(t *testing.T) {
	asOf := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	var rows []storage.BehavioralBiasObservation
	id := int64(1)
	for _, day := range []int{6, 7, 8, 10, 11, 13, 14, 16, 17} {
		rows = append(rows, behavioralTestObservation(id, time.Date(2026, 7, day, 12, 0, 0, 0, time.UTC),
			"T"+time.Date(2026, 7, day, 0, 0, 0, 0, time.UTC).Format("02"), "kalshi-flow"))
		id++
	}
	// Same ticker/cohort/day is one prospective opportunity even if the ten-minute logger sampled
	// it twice. Keeping the whole day together also makes train/test ticker-day leakage impossible.
	dup := behavioralTestObservation(id, time.Date(2026, 7, 7, 14, 0, 0, 0, time.UTC), "T07", "kalshi-flow")
	dup.Payout = 0 // would visibly corrupt the result if the later duplicate survived
	rows = append(rows, dup)
	rows = append(rows,
		behavioralTestObservation(id+1, time.Date(2026, 7, 21, 1, 0, 0, 0, time.UTC), "CURRENT", "kalshi-flow"),
		behavioralTestObservation(id+2, time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC), "FUTURE", "kalshi-flow"))

	r1 := behavioralBiasReport(rows, storage.BehavioralBiasCoverage{Eligible: int64(len(rows))}, asOf)
	r2 := behavioralBiasReport(rows, storage.BehavioralBiasCoverage{Eligible: int64(len(rows))}, asOf)
	if r1.Coverage.CurrentDayExcluded != 1 || r1.Coverage.FutureExcluded != 1 || r1.Coverage.CompleteDayEligible != 10 {
		t.Fatalf("maturity coverage wrong: %+v", r1.Coverage)
	}
	if len(r1.Cohorts) != 1 || len(r2.Cohorts) != 1 {
		t.Fatalf("unexpected cohort count: %d/%d", len(r1.Cohorts), len(r2.Cohorts))
	}
	a, b := r1.Cohorts[0], r2.Cohorts[0]
	if a.ObservedLane != "direct_observed" || a.Train.Observations != 5 || a.Validation.Observations != 1 || a.Test.Observations != 2 {
		t.Fatalf("event-disjoint 60/20/20 split, embargo, or ticker-day dedup failed: %+v", a)
	}
	if a.Train.ExactElapsedSeconds != 6*86400 || a.Validation.ExactElapsedSeconds != 86400 || a.Test.ExactElapsedSeconds != 2*86400 ||
		a.Train.UniqueCompletedUTCDays != 5 || a.Validation.UniqueCompletedUTCDays != 1 || a.Test.UniqueCompletedUTCDays != 2 {
		t.Fatalf("complete UTC clocks wrong: train=%+v test=%+v", a.Train, a.Test)
	}
	if a.Test.Observations != b.Test.Observations || a.Test.ExactOneShareNetPerDay != b.Test.ExactOneShareNetPerDay ||
		(a.Test.OOSNetPerDayLower == nil) != (b.Test.OOSNetPerDayLower == nil) ||
		(a.Test.OOSNetPerDayLower != nil && *a.Test.OOSNetPerDayLower != *b.Test.OOSNetPerDayLower) {
		t.Fatalf("walk-forward/bootstrap is not deterministic: %+v vs %+v", a.Test, b.Test)
	}
	if math.Abs(a.Test.MeanCalibrationImplied-.30) > 1e-12 || math.Abs(a.Test.MeanRealized-1) > 1e-12 ||
		math.Abs(a.Test.CalibrationResidual-.70) > 1e-12 || math.Abs(a.Test.Brier-.49) > 1e-12 ||
		math.Abs(a.Test.MeanExecutableNetCentsPerShare-59) > 1e-12 ||
		math.Abs(a.Test.ExactOneShareNetPerDay-.59) > 1e-12 || math.Abs(a.Test.OpportunitiesPerDay-1) > 1e-12 {
		t.Fatalf("calibration/economics response values wrong: %+v", a.Test)
	}
}

func TestBehavioralBiasNeverFabricatesOrMergesInverseLane(t *testing.T) {
	asOf := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	rows := []storage.BehavioralBiasObservation{
		behavioralTestObservation(1, time.Date(2026, 7, 8, 10, 0, 0, 0, time.UTC), "D1", "freshlist"),
		behavioralTestObservation(2, time.Date(2026, 7, 9, 10, 0, 0, 0, time.UTC), "D2", "freshlist"),
	}
	directOnly := behavioralBiasReport(rows, storage.BehavioralBiasCoverage{}, asOf)
	for _, c := range directOnly.Cohorts {
		if c.ObservedLane != "direct_observed" {
			t.Fatalf("inverse was algebraically fabricated from direct observations: %+v", c)
		}
	}
	rows = append(rows,
		behavioralTestObservation(3, time.Date(2026, 7, 8, 11, 0, 0, 0, time.UTC), "I1", "freshfade"),
		behavioralTestObservation(4, time.Date(2026, 7, 9, 11, 0, 0, 0, time.UTC), "I2", "freshfade"))
	both := behavioralBiasReport(rows, storage.BehavioralBiasCoverage{}, asOf)
	lanes := map[string]map[string]bool{}
	for _, c := range both.Cohorts {
		if lanes[c.SourceSignal] == nil {
			lanes[c.SourceSignal] = map[string]bool{}
		}
		lanes[c.SourceSignal][c.ObservedLane] = true
	}
	if !lanes["freshlist"]["direct_observed"] || !lanes["freshfade"]["inverse_observed"] ||
		lanes["freshlist"]["inverse_observed"] || lanes["freshfade"]["direct_observed"] {
		t.Fatalf("observed direct/inverse lanes merged or synthesized: %+v", lanes)
	}
}

func TestBehavioralBiasEndpointValuesAndAuthority(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	today := behavioralUTCDay(time.Now().UTC())
	insert := func(dayOffset int, ticker string) {
		t.Helper()
		opened := today.AddDate(0, 0, dayOffset).Add(12 * time.Hour)
		closed := opened.Add(time.Hour)
		_, err := s.store.DBForTest().ExecContext(ctx, `INSERT INTO signal_log(
ts,day,slot,platform,ticker,title,side,signal_type,entry_price,book_feature_ver,
label_version,pricing_version,book_bid,book_ask,book_ask_depth,book_taker_fee_pc,book_source,resolved,won,resolved_at,
category,kind,is_live,resolve_hours)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, opened.Format(time.RFC3339),
			opened.Format("2006-01-02"), ticker, "kalshi", ticker, "bias", "YES", "bookskew", .99,
			1, "kalshi_start_clock_v2", "book-native-v2", .20, .40, 4.0, .01, "kalshi_book_ws", 1, 1, closed.Format(time.RFC3339),
			"Sports", "winner", 0, 4.0)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i, offset := range []int{-14, -13, -12, -10, -9, -7, -6, -4, -3} {
		insert(offset, "E"+string([]byte{byte('A' + i)}))
	}

	rr := httptest.NewRecorder()
	s.handleBehavioralBiasRegime(rr, httptest.NewRequest(http.MethodGet, "/api/behavioral-bias-regime", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var got behavioralBiasResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON: %v body=%s", err, rr.Body.String())
	}
	if got.System != "behavioral-bias-regime" || got.State != behavioralBiasState || !got.ResearchOnly ||
		got.Promotable || got.LiveAuthority || got.PaperAuthority || len(got.Cohorts) != 1 {
		t.Fatalf("authority or response missing: %+v", got)
	}
	if got.Cohorts[0].Test.Observations != 2 || math.Abs(got.Cohorts[0].Test.MeanExecutableNetCentsPerShare-59) > 1e-12 {
		t.Fatalf("API economics wrong: %+v", got.Cohorts[0])
	}
	if !strings.Contains(got.ProofRule, "untouched OOS executable lower-bound Net/day") ||
		!strings.Contains(got.MultipleTesting, "Holm") ||
		!strings.Contains(got.HierarchicalShrinkage, "partial pooling") {
		t.Fatalf("required proof limitations missing: %+v", got)
	}
	if strings.Contains(rr.Body.String(), `"balance":`) || strings.Contains(rr.Body.String(), `"sizing":`) {
		t.Fatalf("balance/sizing authority leaked into response: %s", rr.Body.String())
	}
}

func TestBehavioralBiasEndpointCachesSnapshotUntilTTL(t *testing.T) {
	s := testServer(t)
	behavioralBiasCacheByServer.Delete(s)
	t.Cleanup(func() { behavioralBiasCacheByServer.Delete(s) })
	ctx := context.Background()
	today := behavioralUTCDay(time.Now().UTC())
	insert := func(dayOffset int, ticker string) {
		t.Helper()
		opened := today.AddDate(0, 0, dayOffset).Add(12 * time.Hour)
		closed := opened.Add(time.Hour)
		_, err := s.store.DBForTest().ExecContext(ctx, `INSERT INTO signal_log(
ts,day,slot,platform,ticker,title,side,signal_type,entry_price,book_feature_ver,
label_version,pricing_version,book_bid,book_ask,book_ask_depth,book_taker_fee_pc,book_source,resolved,won,resolved_at,
category,kind,is_live,resolve_hours)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, opened.Format(time.RFC3339),
			opened.Format("2006-01-02"), ticker, "kalshi", ticker, "bias", "YES", "bookskew", .99,
			1, "kalshi_start_clock_v2", "book-native-v2", .20, .40, 4.0, .01, "kalshi_book_ws", 1, 1, closed.Format(time.RFC3339),
			"Sports", "winner", 0, 4.0)
		if err != nil {
			t.Fatal(err)
		}
	}
	insert(-4, "CACHE-A")
	insert(-3, "CACHE-B")

	request := func() *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		s.handleBehavioralBiasRegime(rr, httptest.NewRequest(http.MethodGet, "/api/behavioral-bias-regime", nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
		}
		return rr
	}
	first := request()
	if first.Header().Get("X-Behavioral-Bias-Cache") != "miss" {
		t.Fatalf("first request cache header=%q", first.Header().Get("X-Behavioral-Bias-Cache"))
	}
	insert(-2, "CACHE-C") // a semantic change that must not trigger another full scan inside the TTL
	second := request()
	if second.Header().Get("X-Behavioral-Bias-Cache") != "hit" || second.Body.String() != first.Body.String() {
		t.Fatalf("cache hit changed the immutable snapshot: header=%q equal=%v",
			second.Header().Get("X-Behavioral-Bias-Cache"), second.Body.String() == first.Body.String())
	}

	cache := behavioralBiasServerCache(s)
	cache.mu.Lock()
	cache.builtAt = time.Now().Add(-behavioralBiasCacheTTL - time.Second)
	cache.mu.Unlock()
	third := request()
	if third.Header().Get("X-Behavioral-Bias-Cache") != "miss" || third.Body.String() == first.Body.String() {
		t.Fatalf("expired cache did not rebuild: header=%q equal=%v",
			third.Header().Get("X-Behavioral-Bias-Cache"), third.Body.String() == first.Body.String())
	}
	var before, after behavioralBiasResponse
	if json.Unmarshal(first.Body.Bytes(), &before) != nil || json.Unmarshal(third.Body.Bytes(), &after) != nil {
		t.Fatal("cached/rebuilt payload was not valid JSON")
	}
	if after.Coverage.TotalRows != before.Coverage.TotalRows+1 || after.Coverage.CompleteDayEligible != before.Coverage.CompleteDayEligible+1 {
		t.Fatalf("rebuild changed research semantics: before=%+v after=%+v", before.Coverage, after.Coverage)
	}
}

func TestBehavioralBiasPredeclaredFeatureCohorts(t *testing.T) {
	o := behavioralTestObservation(1, time.Now().Add(-48*time.Hour), "T", "xvgap")
	mom, flow, hhi, imb := 6.0, .8, .4, .8
	o.Mom1h, o.FlowRatio15m, o.HoldersHHI, o.BookImb3 = &mom, &flow, &hhi, &imb
	o.TraderCount, o.Strength, o.PricePath = 6, .06, `[0.20,0.21,0.30]`
	seen := map[string]bool{}
	for _, m := range behavioralMemberships(o) {
		seen[m.Bias] = true
	}
	for _, want := range []string{"favorite-longshot", "hot-streak-momentum", "whale-herding",
		"cross-venue-divergence", "volatility-shock", "liquidity-imbalance"} {
		if !seen[want] {
			t.Fatalf("predeclared %s cohort absent: %+v", want, behavioralMemberships(o))
		}
	}
}

func TestBehavioralBiasPhasePrefersKnownStartClock(t *testing.T) {
	future, started := int64(3600), int64(-60)
	o := behavioralTestObservation(1, time.Now().Add(-48*time.Hour), "T", "kalshi-flow")
	o.IsLive, o.SecsToStart = 1, &future
	if got := behavioralPhase(o); got != "pre_event" {
		t.Fatalf("known future start was mislabeled %q", got)
	}
	o.IsLive, o.SecsToStart = 0, &started
	if got := behavioralPhase(o); got != "unknown" {
		t.Fatalf("started but activity-unknown row was mislabeled %q", got)
	}
	o.IsLive = 1
	if got := behavioralPhase(o); got != "live" {
		t.Fatalf("started active row was mislabeled %q", got)
	}
}
