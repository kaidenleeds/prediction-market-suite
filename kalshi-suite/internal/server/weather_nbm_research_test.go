package server

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/nbm"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func testNBMQuantiles(values ...float64) []wxNBMQuantile {
	out := make([]wxNBMQuantile, 0, len(values))
	for i, value := range values {
		out = append(out, wxNBMQuantile{Field: wxNBMPercentiles[i].Field,
			Percentile: wxNBMPercentiles[i].P, ValueF: value})
	}
	return out
}

func TestR139NBMDailyMaxUsesOnlyAligned00UTCMaximum(t *testing.T) {
	run := time.Date(2026, 7, 12, 13, 0, 0, 0, time.UTC)
	forecast := nbm.StationForecast{Station: "KNYC", Run: run, ArtifactURL: "fixture",
		Values: map[string][]int{
			"UTC": {12, 0, 12, 0}, "FHR": {23, 35, 47, 59},
			"TXNP1": {60, 70, 61, 71}, "TXNP2": {62, 72, 63, 73},
			"TXNP5": {64, 74, 65, 75}, "TXNP7": {66, 76, 67, 77},
			"TXNP9": {68, 78, 69, 79},
		}}
	points, err := wxNBMDailyMaxima(forecast)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 2 || points[0].FHR != 35 || points[0].ValidUTC.Hour() != 0 ||
		points[0].Values[0].ValueF != 70 || points[0].Values[4].ValueF != 78 {
		t.Fatalf("daily maxima=%+v", points)
	}
	bad := forecast
	bad.Values = make(map[string][]int, len(forecast.Values))
	for k, v := range forecast.Values {
		bad.Values[k] = append([]int(nil), v...)
	}
	bad.Values["UTC"][1] = 1
	if _, err := wxNBMDailyMaxima(bad); err == nil || !strings.Contains(err.Error(), "alignment") {
		t.Fatalf("misaligned UTC/FHR accepted: %v", err)
	}
}

func TestR139NBMExactValidInstantSuppliesSettlementLocalDate(t *testing.T) {
	valid := time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC)
	edt := time.FixedZone("EDT", -4*60*60)
	periods := []wxPeriod{{start: valid.In(edt), tempF: 87}}
	if got, ok := wxNBMLocalDate(periods, valid); !ok || got != "2026-07-12" {
		t.Fatalf("local date=%q ok=%v", got, ok)
	}
	if got, ok := wxNBMLocalDate(nil, valid); ok || got != "" {
		t.Fatalf("missing exact offset must fail closed: %q %v", got, ok)
	}
	// Today's offset is not enough across a future DST boundary: only a period at the valid instant
	// may map the date.
	if _, ok := wxNBMLocalDate([]wxPeriod{{start: valid.Add(-24 * time.Hour).In(edt)}}, valid); ok {
		t.Fatal("unrelated period supplied a guessed local date")
	}
}

func TestR139NBMQuantileOrderBoundsDoNotInventDistribution(t *testing.T) {
	q := testNBMQuantiles(70, 75, 80, 85, 90)
	lo, hi, ok := wxNBMBinProbabilityBounds(q, -999, 75)
	if !ok || math.Abs(lo-.25) > 1e-12 || math.Abs(hi-.50) > 1e-12 {
		t.Fatalf("lower tail=[%v,%v] ok=%v, want [.25,.50]", lo, hi, ok)
	}
	lo, hi, ok = wxNBMBinProbabilityBounds(q, 86, 999)
	if !ok || math.Abs(lo-.10) > 1e-12 || math.Abs(hi-.25) > 1e-12 {
		t.Fatalf("upper tail=[%v,%v] ok=%v, want [.10,.25]", lo, hi, ok)
	}
	// The five quantiles cannot prove a positive mass at the single 80F atom. A normal fit or
	// linear interpolation would fabricate a point estimate; the honest lower bound is zero.
	lo, hi, ok = wxNBMBinProbabilityBounds(q, 80, 80)
	if !ok || lo != 0 || math.Abs(hi-.50) > 1e-12 {
		t.Fatalf("80F atom=[%v,%v] ok=%v, want [0,.50]", lo, hi, ok)
	}
}

func TestR139NBMCandidateUsesOnlyPositiveExecutableLowerBound(t *testing.T) {
	q := testNBMQuantiles(70, 75, 80, 85, 90)
	books := []wxResearchBook{
		{Ticker: "LOW", LoF: -999, HiF: 75, YesExecutable: true, YesAsk: .20,
			YesAskDepth: 4, YesTakerFeePC: .01, YesTakerTick: .01, QuoteAgeS: .2, BookSource: "kalshi_book_ws"},
		{Ticker: "MID", LoF: 76, HiF: 85, YesExecutable: true, YesAsk: .30,
			YesAskDepth: 3, YesTakerFeePC: .01, YesTakerTick: .01, QuoteAgeS: .2, BookSource: "kalshi_book_ws"},
		{Ticker: "HIGH", LoF: 86, HiF: 999, NoExecutable: true, NoAsk: .85,
			NoAskDepth: 2, NoTakerFeePC: .01, NoTakerTick: .01, QuoteAgeS: .2, BookSource: "kalshi_book_ws"},
	}
	bins, best, err := wxNBMEnvelopeBooks(books, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(bins) != 3 || best == nil || best.Ticker != "LOW" || best.Side != "YES" || math.Abs(best.NetLower-.04) > 1e-12 {
		t.Fatalf("bins=%+v best=%+v", bins, best)
	}
	books[0].YesAsk = .25
	_, best, err = wxNBMEnvelopeBooks(books, q)
	if err != nil || best == nil || best.NetLower > 0 {
		t.Fatalf("no conservative positive route expected, best=%+v err=%v", best, err)
	}
}

func TestR139IndependentWeatherFamilyHasNoMoneyAuthority(t *testing.T) {
	if !strings.HasPrefix(wxNBMSourceIndependence, "INDEPENDENT:") {
		t.Fatalf("NBM provenance=%q", wxNBMSourceIndependence)
	}
	found := false
	for _, family := range policyFamilies {
		if family.fam == wxNBMSystem {
			found = family.research
		}
	}
	if !found {
		t.Fatal("independent NBM system must remain research-only")
	}
}

func TestR139NBMCandidateUsesCanonicalResearchSettlementLedger(t *testing.T) {
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{store: st}
	book := wxResearchBook{Ticker: "KXHIGHNY-26JUL13-T75", Title: "NYC high (75 or below)",
		LoF: -999, HiF: 75, YesExecutable: true, YesBid: .17, YesAsk: .20,
		YesBidDepth: 5, YesAskDepth: 4, QuoteAgeS: .2, YesMakerTick: .01, YesTakerTick: .01,
		YesMakerFeePC: 0, YesTakerFeePC: .01, BookSource: "kalshi_book_ws"}
	market := kalshi.Market{Ticker: book.Ticker, Title: "NYC high", YesSubTitle: "75 or below",
		EventTicker: "KXHIGHNY-26JUL13", ExpectedExpiration: time.Now().Add(12 * time.Hour).Format(time.RFC3339)}
	inserted, err := s.wxInsertResearchSignal(context.Background(), wxNBMSystem, "YES", book, market, .25, .04, .25)
	if err != nil || !inserted {
		t.Fatalf("research settlement insert=%v err=%v", inserted, err)
	}
	var family, side, source, execExpr string
	var ask, fee, depth float64
	if err := st.DBForTest().QueryRow(`SELECT signal_type,side,book_ask,book_taker_fee_pc,
book_ask_depth,book_source,exec_expr FROM signal_log WHERE ticker=?`, book.Ticker).
		Scan(&family, &side, &ask, &fee, &depth, &source, &execExpr); err != nil {
		t.Fatal(err)
	}
	if family != wxNBMSystem || side != "YES" || ask != .20 || fee != .01 || depth != 4 ||
		source != "kalshi_book_ws" || execExpr != "research-only:taker" {
		t.Fatalf("dishonest research row family=%q side=%q ask=%v fee=%v depth=%v source=%q exec=%q",
			family, side, ask, fee, depth, source, execExpr)
	}
	if _, err := st.DBForTest().Exec(`UPDATE signal_log SET resolved=1,won=1,settle_val=1 WHERE ticker=?`, book.Ticker); err != nil {
		t.Fatal(err)
	}
	stats, err := st.WeatherResearchSignalStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range stats {
		if row.SignalType == wxNBMSystem {
			found = row.Logged == 1 && row.Resolved == 1 && math.Abs(row.NetPC-.79) < 1e-12
		}
	}
	if !found {
		t.Fatalf("independent NBM row was not testable through canonical settlement stats: %+v", stats)
	}
}
