package server

import (
	"math"
	"strings"
	"testing"
	"time"
)

func adaptiveWhaleTestPtr[T any](v T) *T { return &v }

func strongAdaptiveWhaleInput() adaptiveWhaleInput {
	now := time.Unix(1770000000, 0).UTC()
	return adaptiveWhaleInput{
		Venue: "kalshi", Market: "KXTEST", Side: "YES", Action: "BUY", Notional: 500, Price: .4,
		ObservedAt: now, Now: now, WindowNotionals: []float64{8, 9, 10, 10, 10, 11, 12, 13, 500},
		ResolveHours: adaptiveWhaleTestPtr(24.0), Frequency: adaptiveWhaleTestPtr(9),
		WalletRank: adaptiveWhaleTestPtr(1), WalletPnL: adaptiveWhaleTestPtr(200000.0),
		WalletSkill: adaptiveWhaleTestPtr(1.0), WalletAllocation: adaptiveWhaleTestPtr(1.0),
		AgreementCount: adaptiveWhaleTestPtr(4), SLTPDiscipline: adaptiveWhaleTestPtr(1.0),
		HotColdStreak: adaptiveWhaleTestPtr(1.0), InsiderNovelty: adaptiveWhaleTestPtr(1.0),
		SpreadCents: adaptiveWhaleTestPtr(0.0), DepthUSD: adaptiveWhaleTestPtr(1000.0),
		BookVolatility: adaptiveWhaleTestPtr(0.0),
	}
}

func adaptiveWhaleFactorByName(t *testing.T, r adaptiveWhaleReceipt, name string) adaptiveWhaleFactor {
	t.Helper()
	for _, f := range r.Factors {
		if f.Name == name {
			return f
		}
	}
	t.Fatalf("factor %q missing", name)
	return adaptiveWhaleFactor{}
}

func TestAdaptiveWhaleRequiresNonzeroVenueFloor(t *testing.T) {
	in := strongAdaptiveWhaleInput()
	in.Notional = 9
	r := scoreAdaptiveWhale(in)
	if r.VenueBaseFloor <= 0 || r.PassBaseFloor || r.Qualifies {
		t.Fatalf("venue floor failed closed incorrectly: %+v", r)
	}
}

func TestAdaptiveWhaleRequiresRelativeSignificance(t *testing.T) {
	in := strongAdaptiveWhaleInput()
	in.Venue, in.Notional = "polyus", 50
	in.WindowNotionals = []float64{90, 95, 100, 100, 100, 105, 110, 115}
	r := scoreAdaptiveWhale(in)
	if !r.PassBaseFloor || r.PassRelativeSignificance || r.Qualifies {
		t.Fatalf("ordinary-size print bypassed adaptive bar: floor=%v relative=%v threshold=%.2f", r.PassBaseFloor, r.PassRelativeSignificance, r.Baseline.RelativeThreshold)
	}
}

func TestAdaptiveWhaleRecencyUsesResolutionHorizon(t *testing.T) {
	near := strongAdaptiveWhaleInput()
	near.ObservedAt = near.Now.Add(-10 * time.Minute)
	near.ResolveHours = adaptiveWhaleTestPtr(0.1)
	far := near
	far.ResolveHours = adaptiveWhaleTestPtr(24.0)
	rNear, rFar := scoreAdaptiveWhale(near), scoreAdaptiveWhale(far)
	if rNear.RecencyHalfLifeSeconds >= rFar.RecencyHalfLifeSeconds {
		t.Fatalf("near-resolution flow did not get shorter half-life: near=%.0f far=%.0f", rNear.RecencyHalfLifeSeconds, rFar.RecencyHalfLifeSeconds)
	}
	if adaptiveWhaleFactorByName(t, rNear, "recency").Value >= adaptiveWhaleFactorByName(t, rFar, "recency").Value {
		t.Fatal("near-resolution stale print was not decayed more aggressively")
	}
}

func TestAdaptiveWhaleSellIsEvidenceAgainstSide(t *testing.T) {
	in := strongAdaptiveWhaleInput()
	in.Action, in.Side = "sell", "YES"
	r := scoreAdaptiveWhale(in)
	if r.Evidence != "against YES" || r.Action != "SELL" {
		t.Fatalf("SELL semantics lost: action=%q evidence=%q", r.Action, r.Evidence)
	}
	if !r.Qualifies {
		t.Fatal("a significant SELL should remain a qualifying research observation against the side")
	}
}

func TestAdaptiveWhaleMissingFactorsAreExplicitNeutral(t *testing.T) {
	in := strongAdaptiveWhaleInput()
	in.WalletRank, in.WalletPnL, in.WalletSkill, in.WalletAllocation = nil, nil, nil, nil
	in.AgreementCount, in.SLTPDiscipline, in.HotColdStreak, in.InsiderNovelty = nil, nil, nil, nil
	in.SpreadCents, in.DepthUSD, in.BookVolatility = nil, nil, nil
	r := scoreAdaptiveWhale(in)
	for _, name := range []string{"wallet_rank", "wallet_pnl", "wallet_skill", "wallet_allocation", "agreement_count", "sl_tp_discipline", "hot_cold_streak", "insider_novelty", "book_liquidity_context", "mature_oos_calibration"} {
		f := adaptiveWhaleFactorByName(t, r, name)
		if f.Available || math.Abs(f.Value-50) > 1e-12 {
			t.Fatalf("missing factor %s was not explicit neutral: %+v", name, f)
		}
	}
	if len(r.MissingNeutralFactors) < 10 {
		t.Fatalf("missing-factor receipt incomplete: %v", r.MissingNeutralFactors)
	}
}

func TestAdaptiveWhaleScoreRoundsNearestFive(t *testing.T) {
	r := scoreAdaptiveWhale(strongAdaptiveWhaleInput())
	if r.ScoreRounded5 < 0 || r.ScoreRounded5 > 100 || r.ScoreRounded5%5 != 0 {
		t.Fatalf("score not rounded to a 0..100 multiple of five: continuous=%f rounded=%d", r.ScoreContinuous, r.ScoreRounded5)
	}
	if math.Abs(float64(r.ScoreRounded5)-r.ScoreContinuous) > 2.5000001 {
		t.Fatalf("nearest-five rounding too far: continuous=%f rounded=%d", r.ScoreContinuous, r.ScoreRounded5)
	}
}

func TestAdaptiveWhaleCalibrationAppliesOnlyWhenMatureExecutable(t *testing.T) {
	immature := strongAdaptiveWhaleInput()
	immature.Calibration = &adaptiveWhaleCalibration{Mature: false, Executable: true, Outcomes: 100, UTCBlocks: 90, LowerNetPerUnit: .10}
	mature := immature
	mature.Calibration = &adaptiveWhaleCalibration{Mature: true, Executable: true, Outcomes: 100, UTCBlocks: 90, LowerNetPerUnit: .10}
	r0, r1 := scoreAdaptiveWhale(immature), scoreAdaptiveWhale(mature)
	if r0.CalibrationApplied || adaptiveWhaleFactorByName(t, r0, "mature_oos_calibration").Available {
		t.Fatal("immature history altered adaptive scorer")
	}
	if !r1.CalibrationApplied || !adaptiveWhaleFactorByName(t, r1, "mature_oos_calibration").Available {
		t.Fatal("mature executable OOS history was not applied")
	}
	if r1.AdaptiveMinScore >= r0.AdaptiveMinScore || r1.ScoreContinuous <= r0.ScoreContinuous {
		t.Fatalf("positive mature calibration did not adapt conservatively: before=%d/%.2f after=%d/%.2f", r0.AdaptiveMinScore, r0.ScoreContinuous, r1.AdaptiveMinScore, r1.ScoreContinuous)
	}
}

func TestAdaptiveWhaleHasNoExecutionAuthority(t *testing.T) {
	r := scoreAdaptiveWhale(strongAdaptiveWhaleInput())
	if !r.ResearchOnly || r.PaperAuthority || r.LiveAuthority || !strings.HasPrefix(r.State, "RESEARCH_ONLY") {
		t.Fatalf("adaptive scorer gained execution authority: %+v", r)
	}
}

func TestAdaptiveWhaleSystemsLeaderboardCoverage(t *testing.T) {
	rows := adaptiveWhaleCoverageRows()
	if len(rows) != 6 {
		t.Fatalf("coverage rows=%d want 6", len(rows))
	}
	seen := map[string]bool{}
	for _, r := range rows {
		seen[r.Family] = true
		if !r.ResearchOnly || r.LiveAuthorizes ||
			(r.State != "COLLECTING" && r.State != "RESEARCH_ONLY_COLLECTING") || r.Timed {
			t.Fatalf("coverage row implies proof/authority: %+v", r)
		}
	}
	for _, venue := range []string{"kalshi", "polyus", "polymarket"} {
		for _, sys := range []string{adaptiveWhaleRawSystem, adaptiveWhaleSystem} {
			if !seen[sys+"@"+venue] {
				t.Fatalf("missing %s coverage for %s", sys, venue)
			}
		}
	}
}

func TestAdaptiveWhaleLaterHugePrintCannotChangeEarlierReceipt(t *testing.T) {
	base := time.Unix(1770000000, 0).UTC()
	raw := make([]adaptiveWhaleRaw, 0, 11)
	for i := 0; i < 8; i++ {
		raw = append(raw, adaptiveWhaleRaw{Venue: "kalshi", Market: "base", Side: "YES", Action: "BUY",
			Notional: 10 + float64(i%2), Price: .5, At: base.Add(time.Duration(i) * time.Second)})
	}
	raw = append(raw, adaptiveWhaleRaw{Venue: "kalshi", Market: "target", Side: "YES", Action: "BUY",
		Notional: 40, Price: .5, At: base.Add(9 * time.Second)})
	now := base.Add(20 * time.Second)
	before, _ := (*Server)(nil).adaptiveWhaleChronological(raw, now)
	raw = append(raw, adaptiveWhaleRaw{Venue: "kalshi", Market: "future-huge", Side: "YES", Action: "BUY",
		Notional: 1000000, Price: .5, At: base.Add(10 * time.Second)})
	after, _ := (*Server)(nil).adaptiveWhaleChronological(raw, now)
	find := func(rows []adaptiveWhaleReceipt) adaptiveWhaleReceipt {
		for _, r := range rows {
			if r.Market == "target" {
				return r
			}
		}
		t.Fatal("target receipt missing")
		return adaptiveWhaleReceipt{}
	}
	a, b := find(before), find(after)
	if a.Baseline != b.Baseline || math.Abs(a.ScoreContinuous-b.ScoreContinuous) > 1e-12 || a.ScoreRounded5 != b.ScoreRounded5 || a.Qualifies != b.Qualifies {
		t.Fatalf("later print leaked backward:\nbefore=%+v\nafter=%+v", a, b)
	}
}

func TestAdaptiveWhaleOldReceiptCannotUseCurrentBook(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()
	raw := []adaptiveWhaleRaw{{Venue: "kalshi", Market: "old", Side: "YES", Action: "BUY", Notional: 100, Price: .5, At: now.Add(-time.Minute)}}
	receipts, _ := (&Server{}).adaptiveWhaleChronological(raw, now)
	if len(receipts) != 1 {
		t.Fatalf("receipts=%d", len(receipts))
	}
	f := adaptiveWhaleFactorByName(t, receipts[0], "book_liquidity_context")
	if f.Available || math.Abs(f.Value-50) > 1e-12 {
		t.Fatalf("historical receipt used current book context: %+v", f)
	}
}

func TestAdaptiveWhaleBookContextRequiresFreshSnapshot(t *testing.T) {
	now := time.Unix(1770000000, 0).UTC()
	if adaptiveWhaleBookFresh(now.Add(-adaptiveWhaleBookMaxAge-time.Nanosecond), now) {
		t.Fatal("stale cached book was accepted")
	}
	if !adaptiveWhaleBookFresh(now.Add(-adaptiveWhaleBookMaxAge), now) {
		t.Fatal("book exactly at freshness boundary was rejected")
	}
	if adaptiveWhaleBookFresh(time.Time{}, now) || adaptiveWhaleBookFresh(now.Add(2*time.Second), now) {
		t.Fatal("missing or materially future book timestamp was accepted")
	}
}
