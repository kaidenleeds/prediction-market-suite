package server

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestR135cSystemsRegimeUsesExactUTCBlocksAndQueueMean(t *testing.T) {
	asOf := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	var obs []systemRegimeObservation
	for day := 0; day < 7; day++ {
		for j := 0; j < 3; j++ {
			opened := time.Date(2026, 7, 5+day, j, 0, 0, 0, time.UTC)
			obs = append(obs, systemRegimeObservation{
				System: "bookskew", Signal: "bookskew", Venue: "kalshi", Route: "taker",
				Source: "live_fills", Evidence: "live_fill", Opened: opened,
				Closed: opened.Add(time.Hour), ClockKnown: true, CostPC: .5, FeePC: .01,
				FeeKnown: true, PnLPC: .01, PnLKnown: true, Terminal: true, Filled: true,
				CompleteHistory: true, OriginLayer: "strategy",
				Depth: 4, DepthKnown: true, QueueAhead: float64(10 + 10*(j%2)), QueueKnown: true,
			})
		}
	}
	r, ok := summarizeSystemWindow(obs, systemWindow{name: "7d", days: 7}, asOf)
	if !ok || r.UTCBlockDays != 7 || r.MatureUTCBlockDays != 6 {
		t.Fatalf("7d must mean seven named UTC blocks, got ok=%v row=%+v", ok, r)
	}
	if math.Abs(r.ElapsedSeconds-6.5*86400) > 1e-9 {
		t.Fatalf("exact partial-day elapsed clock lost: got %.0f", r.ElapsedSeconds)
	}
	if r.MeanQueueAhead == nil || math.Abs(*r.MeanQueueAhead-(40.0/3.0)) > 1e-9 || r.QueueKnownShare != 1 {
		t.Fatalf("queue sum/fraction crossed: mean=%v share=%v", r.MeanQueueAhead, r.QueueKnownShare)
	}
	if r.State != "COLLECTING" {
		t.Fatalf("six completed days plus today's partial block self-proved: %+v", r)
	}
	if r.QualifiesForReview {
		t.Fatal("7d row must never be the selected promotion-review regime")
	}
	// Add the seventh completed day. It is outside the seven named point blocks but correctly
	// supplies the prior seven completed UTC blocks used only for proof.
	for j := 0; j < 3; j++ {
		opened := time.Date(2026, 7, 4, j, 0, 0, 0, time.UTC)
		obs = append(obs, systemRegimeObservation{System: "bookskew", Signal: "bookskew", Venue: "kalshi", Route: "taker",
			Source: "live_fills", Evidence: "live_fill", OriginLayer: "strategy", Opened: opened,
			Closed: opened.Add(time.Hour), ClockKnown: true, CostPC: .5, FeePC: .01, FeeKnown: true,
			PnLPC: .01, PnLKnown: true, Terminal: true, Filled: true, CompleteHistory: true,
			Depth: 4, DepthKnown: true})
	}
	r, ok = summarizeSystemWindow(obs, systemWindow{name: "7d", days: 7}, asOf)
	if !ok || r.MatureUTCBlockDays != 7 || r.NetPerDayLower == nil || *r.NetPerDayLower <= 0 || r.State != "LOWER-BOUND+" {
		t.Fatalf("seven completed stable blocks did not clear research bound: %+v", r)
	}
	r30, ok := summarizeSystemWindow(obs, systemWindow{name: "30d", days: 30}, asOf)
	if !ok || r30.QualifiesForReview || r30.State != "COLLECTING" {
		t.Fatalf("partial 30d history must stay collecting rather than borrow its 7d result: %+v", r30)
	}
	full := append([]systemRegimeObservation{}, obs...)
	for day := 0; day < 24; day++ {
		for j := 0; j < 3; j++ {
			opened := time.Date(2026, 6, 11+day, j, 0, 0, 0, time.UTC)
			full = append(full, systemRegimeObservation{System: "bookskew", Signal: "bookskew",
				Venue: "kalshi", Route: "taker", Source: "live_fills", Evidence: "live_fill",
				Opened: opened, Closed: opened.Add(time.Hour), ClockKnown: true, CostPC: .5,
				FeePC: .01, FeeKnown: true, PnLPC: .01, PnLKnown: true, Terminal: true, Filled: true,
				Depth: 4, DepthKnown: true, CompleteHistory: true, OriginLayer: "strategy"})
		}
	}
	r30, ok = summarizeSystemWindow(full, systemWindow{name: "30d", days: 30}, asOf)
	if !ok || !r30.QualifiesForReview || r30.MatureUTCBlockDays != 30 {
		t.Fatalf("full stable 30-block route did not qualify for review: %+v", r30)
	}
}

func TestR135cSystemsRegimeResearchAndMissingFeesNeverQualify(t *testing.T) {
	asOf := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	makeRows := func(research, feeKnown bool) []systemRegimeObservation {
		var out []systemRegimeObservation
		for i := 0; i < 21; i++ {
			op := asOf.Add(-time.Duration(7-i%7) * 24 * time.Hour)
			out = append(out, systemRegimeObservation{System: "subcent-golf/pre_event/active/winner",
				Signal: "fresh sub-cent golf depth-book quote", Venue: "kalshi", Route: "taker",
				Source: "subcent_golf_trials", Evidence: "research_quote", Opened: op,
				Closed: op.Add(time.Hour), ClockKnown: true, CostPC: .001, PnLPC: .02,
				PnLKnown: true, FeeKnown: feeKnown, Terminal: true, ResearchOnly: research,
				Depth: 1, DepthKnown: true, CompleteHistory: true, OriginLayer: "strategy"})
		}
		return out
	}
	for _, tc := range []struct {
		name               string
		research, feeKnown bool
	}{
		{"unfunded cohort", true, true},
		{"missing fee", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, ok := summarizeSystemWindow(makeRows(tc.research, tc.feeKnown), systemWindow{name: "30d", days: 30}, asOf)
			if !ok || r.QualifiesForReview {
				t.Fatalf("unsafe review qualification: %+v", r)
			}
		})
	}
}

func TestR135cDayBlockBootstrapIsDeterministicAndKeepsZeroDays(t *testing.T) {
	x := []float64{.1, 0, .1, 0, .1, 0, .1}
	lo1, hi1, ok1 := deterministicDayBounds(x, "xvgap|polyus|taker|30d")
	lo2, hi2, ok2 := deterministicDayBounds(x, "xvgap|polyus|taker|30d")
	if !ok1 || !ok2 || lo1 != lo2 || hi1 != hi2 || lo1 < 0 || hi1 <= lo1 {
		t.Fatalf("day bootstrap not deterministic/valid: %.6f %.6f / %.6f %.6f", lo1, hi1, lo2, hi2)
	}
}

func TestR135cSystemsRegimeEndpointIsFiniteAndResearchOnly(t *testing.T) {
	s := testServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/systems-regimes", nil)
	rr := httptest.NewRecorder()
	s.handleSystemsRegime(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("non-finite/invalid endpoint JSON: %v body=%s", err, rr.Body.String())
	}
	if got["research_only"] != true || got["authority"] == "" {
		t.Fatalf("endpoint lost read-only authority: %+v", got)
	}
	if _, ok := got["review_candidates_30d"]; !ok {
		t.Fatal("30d review surface missing")
	}
}

func TestR135cSystemsRegimeKeepsModelAndStrategyOriginsSeparate(t *testing.T) {
	asOf := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	base := systemRegimeObservation{System: "xvgap", Signal: "xvgap", Venue: "polyus",
		Route: "taker", Source: "unit_trials", Evidence: "executable_quote",
		Opened: asOf.Add(-time.Hour), CostPC: .4, FeeKnown: true, CompleteHistory: true}
	model, strategy := base, base
	model.OriginLayer, strategy.OriginLayer = "model", "strategy"
	rows := systemsRegimeRows([]systemRegimeObservation{model, strategy}, asOf)
	allN := 0
	origins := map[string]bool{}
	for _, r := range rows {
		if r.Window == "all" {
			allN++
			origins[r.OriginLayer] = true
		}
	}
	if allN != 2 || !origins["model"] || !origins["strategy"] {
		t.Fatalf("model/strategy origins merged: %+v", rows)
	}
}

func TestR135cMakerRegimeUsesPersistedFeeNotCurrentSchedule(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	insert := func(ticker string, receipt bool) {
		t.Helper()
		id, err := s.store.InsertMakerAttempt(ctx, "kalshi", ticker, "YES", "auto-cons-edge", .40, 2, 8, 0, nil, 1, "", "bookws")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.store.SetMakerStrategy(ctx, id, "edge", false); err != nil {
			t.Fatal(err)
		}
		if receipt {
			err = s.store.MarkMakerFilledWithFee(ctx, id, .01, "kalshi:historical-receipt")
		} else {
			err = s.store.MarkMakerFilled(ctx, id)
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.store.DBForTest().ExecContext(ctx, `UPDATE maker_fill_stats SET settle_val=1 WHERE id=?`, id); err != nil {
			t.Fatal(err)
		}
	}
	insert("KXHIST-RECEIPT", true)
	insert("KXHIST-LEGACY", false)
	// Today's schedule is intentionally incompatible with the historical 1c receipt. The regime
	// must still report +59c and must exclude the receipt-less legacy fill entirely.
	s.kalFees = map[string]kalFeeInfo{"KXHIST": {maker: .90, taker: .90, typ: "quadratic_with_maker_fees", multiplier: 1}}
	s.kalFeesAt = time.Now()
	obs, _ := s.collectStorageSystemObservations(ctx)
	found := 0
	for _, o := range obs {
		if o.System == "edge" && o.Evidence == "maker_attempt" {
			found++
			if !o.PnLKnown || math.Abs(o.PnLPC-.59) > 1e-12 || math.Abs(o.FeePC-.01) > 1e-12 {
				t.Fatalf("current fee schedule rewrote historical maker row: %+v", o)
			}
		}
	}
	if found != 1 {
		t.Fatalf("expected exactly the receipted maker row, got %d in %+v", found, obs)
	}
}

func TestR135cSystemsRegimeOpenPartialAndModelCohortsCannotReview(t *testing.T) {
	asOf := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	var base []systemRegimeObservation
	for day := 0; day < 30; day++ {
		opened := time.Date(2026, 6, 11+day, 0, 0, 0, 0, time.UTC)
		base = append(base, systemRegimeObservation{System: "edge", Signal: "edge", Venue: "kalshi",
			Route: "taker", Source: "live_fills", Evidence: "live_fill", OriginLayer: "strategy",
			Opened: opened, Closed: opened.Add(time.Hour), ClockKnown: true, CostPC: .2,
			FeePC: .01, FeeKnown: true, PnLPC: .10, PnLKnown: true, Terminal: true, Filled: true,
			Depth: 2, DepthKnown: true, CompleteHistory: true})
	}
	good, ok := summarizeSystemWindow(base, systemWindow{name: "30d", days: 30}, asOf)
	if !ok || !good.QualifiesForReview || good.MatureUTCBlockDays != 30 {
		t.Fatalf("control cohort should qualify: %+v", good)
	}

	withOpen := append([]systemRegimeObservation{}, base...)
	withOpen = append(withOpen, systemRegimeObservation{System: "edge", Signal: "edge", Venue: "kalshi",
		Route: "taker", Source: "live_fills", Evidence: "live_fill", OriginLayer: "strategy",
		Opened: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), CostPC: .2, FeePC: .01,
		FeeKnown: true, Depth: 2, DepthKnown: true, CompleteHistory: true})
	openRow, ok := summarizeSystemWindow(withOpen, systemWindow{name: "30d", days: 30}, asOf)
	if !ok || openRow.ProofOpen != 1 || openRow.State != "COLLECTING" || openRow.QualifiesForReview {
		t.Fatalf("slow unresolved row was censored out of proof: %+v", openRow)
	}

	partial := append([]systemRegimeObservation{}, base...)
	for i := range partial {
		partial[i].CompleteHistory = false
	}
	partialRow, _ := summarizeSystemWindow(partial, systemWindow{name: "30d", days: 30}, asOf)
	if partialRow.State != "PARTIAL-HISTORY" || partialRow.QualifiesForReview {
		t.Fatalf("capped/incomplete history became reviewable: %+v", partialRow)
	}

	model := append([]systemRegimeObservation{}, base...)
	for i := range model {
		model[i].OriginLayer = "model"
	}
	modelRow, _ := summarizeSystemWindow(model, systemWindow{name: "30d", days: 30}, asOf)
	if modelRow.State != "LOWER-BOUND+" || modelRow.QualifiesForReview {
		t.Fatalf("model discovery evidence became strategy review authority: %+v", modelRow)
	}
}
