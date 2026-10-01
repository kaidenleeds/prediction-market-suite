package server

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR138WeatherForecastDiagnosticsAreSafeAndClassified(t *testing.T) {
	raw := errors.New(`kalshi GET /series/SECRET/events/E/forecast_percentile_history?percentiles=5000: status 400: {"error":{"message":"bad request"}}`)
	class, safe := wxSafeForecastError(raw)
	if class != "http_400" || safe != "forecast-history venue response HTTP 400" {
		t.Fatalf("class=%q safe=%q", class, safe)
	}
	if strings.Contains(safe, "SECRET") || strings.Contains(safe, "percentiles") || strings.Contains(safe, "{") {
		t.Fatalf("safe diagnostic leaked raw request/response: %q", safe)
	}
	if class, _ := wxSafeForecastError(context.DeadlineExceeded); class != "timeout" {
		t.Fatalf("deadline class=%q", class)
	}
}

func TestR138WeatherCollectorReceiptCarriesWireAndAuthorityTruth(t *testing.T) {
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{store: st}
	started := time.Date(2026, 7, 12, 3, 7, 0, 0, time.UTC)
	s.wxResearchCollectorBegin(started)
	s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) {
		c.Stations, c.Series, c.Events = 2, 2, 1
		c.Requests, c.Successes, c.Frames = 1, 1, 12
	})
	if !s.wxResearchAudit(context.Background(), wxCurveResidualFamily, "fixture", wxResearchRecord{
		Kind: "curve_residual_observation", InputStatus: "ready", SignalLogged: false,
	}) {
		t.Fatal("fixture research row did not persist")
	}
	s.wxResearchCollectorFinish(context.Background())
	var status, metrics string
	var funded, paper, live int
	if err := st.DBForTest().QueryRow(`SELECT status,metrics_json,funded,paper_authority,live_authority
FROM research_collector_receipts WHERE collector_id='weather-research' ORDER BY id DESC LIMIT 1`).
		Scan(&status, &metrics, &funded, &paper, &live); err != nil {
		t.Fatal(err)
	}
	if status != "healthy" || funded != 0 || paper != 0 || live != 0 {
		t.Fatalf("receipt status=%q authority=%d/%d/%d", status, funded, paper, live)
	}
	var m map[string]any
	if json.Unmarshal([]byte(metrics), &m) != nil || m["time_boundaries"] != "UTC minute-aligned" || m["percentiles_encoding"] != "repeated query keys" {
		t.Fatalf("metrics=%s", metrics)
	}
}

func TestR139WeatherNBMAdapterReflectsCurrentValidatedReceipt(t *testing.T) {
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	spec, ok := storage.R138SourceClockSpec("noaa-nbm")
	if !ok {
		t.Fatal("NOAA NBM source-clock spec missing")
	}
	now := time.Now().UTC()
	if _, err := st.RecordSourceClock(context.Background(), storage.SourceClockReceipt{
		SourceID: spec.SourceID, SpecVersion: spec.Version, SchemaVersion: spec.SchemaVersion,
		SchemaHash: spec.SchemaHash, Received: now, Watermark: now.Add(-30 * time.Minute),
		RowsSeen: 1, EvidenceJSON: `{"fixture":"validated NBM station bulletin"}`,
	}); err != nil {
		t.Fatal(err)
	}
	s := &Server{store: st}
	view := s.weatherNBMAdapterStatus(context.Background())
	if view["implementation"] != "PROSPECTIVE_PROBABILITY_ENVELOPE" || view["status"] != "COLLECTING_INPUTS" ||
		view["mapping_status"] != "IMPLEMENTED_RESEARCH_ONLY" {
		t.Fatalf("adapter view=%+v", view)
	}
}

func testWXQuantiles() []wxCurveQuantile {
	return []wxCurveQuantile{
		{Percentile: 0, ValueF: 40}, {Percentile: 100, ValueF: 41},
		{Percentile: 1000, ValueF: 45}, {Percentile: 2500, ValueF: 48},
		{Percentile: 5000, ValueF: 50}, {Percentile: 7500, ValueF: 52},
		{Percentile: 9000, ValueF: 55}, {Percentile: 9900, ValueF: 59},
		{Percentile: 9999, ValueF: 60},
	}
}

func TestR135TradeDerivedWeatherCurveIsWholeSimplexAndUsesExecutableSides(t *testing.T) {
	books := []wxResearchBook{
		{Ticker: "LOW", LoF: -999, HiF: 49},
		{Ticker: "MID", LoF: 50, HiF: 50, YesExecutable: true, YesAsk: .04, YesTakerFeePC: .01, YesAskDepth: 7},
		{Ticker: "HIGH", LoF: 51, HiF: 999, NoExecutable: true, NoAsk: .85, NoTakerFeePC: .01, NoAskDepth: 9},
	}
	got, err := wxCurveMassSimplex(books, testWXQuantiles())
	if err != nil {
		t.Fatal(err)
	}
	sum := 0.0
	for _, b := range got {
		sum += b.CurveMass
	}
	if math.Abs(sum-1) > 1e-12 {
		t.Fatalf("simplex sum=%v want 1", sum)
	}
	best, err := wxBestCurveResidual(got)
	if err != nil {
		t.Fatal(err)
	}
	if best.Ticker != "MID" || best.Side != "YES" || best.Ask != .04 || best.FeePC != .01 || best.Depth != 7 || best.ResidualPC <= 0 {
		t.Fatalf("best must use the exact executable YES side: %+v", best)
	}
}

func TestR135WeatherCurveFailsClosedOnBadDistribution(t *testing.T) {
	q := testWXQuantiles()
	q[5].ValueF = 49 // p75 below p50
	frame := kalshi.ForecastPercentileFrame{EndPeriodTS: 1}
	for _, x := range q {
		frame.Points = append(frame.Points, kalshi.ForecastPercentilePoint{Percentile: x.Percentile, NumericalForecast: x.ValueF})
	}
	if _, err := wxFrameQuantiles(frame); err == nil {
		t.Fatal("non-monotone percentile distribution must fail closed")
	}
	if _, ok := wxQuantileCDF(q[:len(q)-1], 50); ok {
		t.Fatal("CDF without the 99.99th tail anchor must be unavailable")
	}
}

func TestR135WeatherCDFHandlesRepeatedDiscreteQuantiles(t *testing.T) {
	q := []wxCurveQuantile{{Percentile: 0, ValueF: 40}, {Percentile: 1000, ValueF: 45},
		{Percentile: 5000, ValueF: 45}, {Percentile: 9999, ValueF: 60}}
	got, ok := wxQuantileCDF(q, 45)
	if !ok || math.Abs(got-.5) > 1e-12 {
		t.Fatalf("CDF at repeated trade-derived curve atom=%v ok=%v, want highest duplicate percentile .5", got, ok)
	}
}

func TestR135WeatherSignalCarriesCompleteBookNativeProvenance(t *testing.T) {
	b := wxResearchBook{Ticker: "WX", YesExecutable: true, YesBid: .20, YesAsk: .22,
		YesBidDepth: 11, YesAskDepth: 8, QuoteAgeS: .4, YesMakerTick: .01, YesTakerTick: .01,
		YesMakerFeePC: .002, YesTakerFeePC: .009, BookSource: "kalshi-ws-depth"}
	sig := storage.Signal{Platform: "kalshi", Ticker: "WX", Side: "YES"}
	if !wxCopyBookSignalFields(&sig, b, "YES") {
		t.Fatal("complete side book was rejected")
	}
	if sig.BookFeatureVer != mlBookFeatureVersion || sig.BookBid == nil || *sig.BookBid != .20 || sig.BookAsk == nil || *sig.BookAsk != .22 ||
		sig.BookBidDepth == nil || *sig.BookBidDepth != 11 || sig.BookAskDepth == nil || *sig.BookAskDepth != 8 ||
		sig.BookQuoteAgeS == nil || *sig.BookQuoteAgeS != .4 || sig.BookMakerTick == nil || sig.BookTakerTick == nil ||
		sig.BookMakerFeePC == nil || sig.BookTakerFeePC == nil || sig.FeePC == nil || *sig.FeePC != .009 ||
		sig.EntryPrice != .22 || sig.BookSource != "kalshi-ws-depth" {
		t.Fatalf("incomplete book-v1 signal: %+v", sig)
	}
	if wxCopyBookSignalFields(&storage.Signal{}, wxResearchBook{}, "YES") {
		t.Fatal("missing book must fail closed")
	}
	fractional := b
	fractional.YesAskDepth = .75
	if wxCopyBookSignalFields(&storage.Signal{}, fractional, "YES") {
		t.Fatal("fractional ask depth cannot support one executable share")
	}
}

func TestR135WeatherResearchFamiliesArePermanentlyResearchOnly(t *testing.T) {
	if wxResearchSource != "kalshi-trade-derived-forecast-graph" || !strings.HasPrefix(wxForecastIndependence, "MARKET_DERIVED:") {
		t.Fatalf("dishonest curve provenance source=%q independence=%q", wxResearchSource, wxForecastIndependence)
	}
	want := map[string]bool{"weather-curve-residual": false, "weather-curve-revision": false,
		"independent-probabilistic-weather": false}
	for _, p := range policyFamilies {
		if _, ok := want[p.fam]; ok {
			want[p.fam] = p.research
		}
	}
	for fam, research := range want {
		if !research {
			t.Fatalf("%s missing permanent research policy", fam)
		}
	}
}

func TestR135CurveRevisionDoesNotBackfillLateBooksIntoFixedHorizons(t *testing.T) {
	if got := wxHorizonDisposition(2*60*60, 0); got != "missed" {
		t.Fatalf("two-hour-late first observation became h0: %q", got)
	}
	if got := wxHorizonDisposition(2*60*60, 30*60); got != "missed" {
		t.Fatalf("two-hour-late first observation became h30: %q", got)
	}
	if got := wxHorizonDisposition(2*60*60, 2*60*60); got != "capture" {
		t.Fatalf("on-time h120 disposition=%q, want capture", got)
	}
	if got := wxHorizonDisposition(29*60, 30*60); got != "wait" {
		t.Fatalf("pre-horizon snapshot disposition=%q, want wait", got)
	}
}

func TestR135LateRevisionPersistsMissInsteadOfFalseH0Response(t *testing.T) {
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{store: st}
	p := &wxRevisionPending{ID: "rev", EventKey: "S|D", Series: "S", Event: "E", Date: "D",
		Station: "KXYZ", SourceAt: time.Now().Add(-2 * time.Hour),
		Baseline: []wxResearchBook{{Ticker: "T"}}, Seen: map[int64]bool{}}
	ev := &wxResearchEventState{Pending: []*wxRevisionPending{p}}
	s.wxCollectRevisionResponses(context.Background(), ev, []wxResearchBook{{Ticker: "T"}})
	rows, err := st.WeatherResearchAudit(context.Background(), 20)
	if err != nil {
		t.Fatal(err)
	}
	foundMiss, falseH0 := false, false
	for _, row := range rows {
		var rec wxResearchRecord
		if json.Unmarshal([]byte(row.Detail), &rec) != nil || rec.HorizonS == nil || *rec.HorizonS != 0 {
			continue
		}
		foundMiss = foundMiss || rec.Kind == "curve_horizon_missed"
		falseH0 = falseH0 || rec.Kind == "curve_response"
		if !strings.HasPrefix(rec.SourceIndependence, "MARKET_DERIVED:") || !rec.PromotionBlocked {
			t.Fatalf("curve record must disclose market derivation and block promotion: %+v", rec)
		}
	}
	if !foundMiss || falseH0 {
		t.Fatalf("h0 durable outcome miss=%v false_response=%v rows=%+v", foundMiss, falseH0, rows)
	}
}
