package storage

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestR145UnitTrialCostsEqualWeightDistinctContracts(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	opened := time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)

	// Ticker A is observed five times at the expensive cost. B and C are each observed once.
	// The contract cohort is therefore (.90 + .90 + .10)/3, not the raw-row value
	// (5*.90 + .90 + .10)/7.
	for episode := 1; episode <= 5; episode++ {
		trial := UnitTrial{OpenedTS: opened, Family: "contract-cost", Platform: "kalshi",
			Ticker: "A", Side: "YES", Episode: episode, Ask: .90, FeePC: .09,
			FeeKnown: true, FeeSource: "kalshi:test", Depth: 1, QuoteSource: "kalshi-ws"}
		if inserted, insertErr := st.InsertUnitTrial(ctx, trial); insertErr != nil || !inserted {
			t.Fatalf("insert A/%d=%v err=%v", episode, inserted, insertErr)
		}
	}
	for _, trial := range []UnitTrial{
		{OpenedTS: opened, Family: "contract-cost", Platform: "kalshi", Ticker: "B",
			Side: "YES", Episode: 1, Ask: .90, FeePC: .09, FeeKnown: true,
			FeeSource: "kalshi:test", Depth: 1, QuoteSource: "kalshi-ws"},
		{OpenedTS: opened, Family: "contract-cost", Platform: "kalshi", Ticker: "C",
			Side: "YES", Episode: 1, Ask: .10, FeePC: .01, FeeKnown: true,
			FeeSource: "kalshi:test", Depth: 1, QuoteSource: "kalshi-ws"},
	} {
		if inserted, insertErr := st.InsertUnitTrial(ctx, trial); insertErr != nil || !inserted {
			t.Fatalf("insert %s=%v err=%v", trial.Ticker, inserted, insertErr)
		}
	}
	closed := opened.Add(time.Hour).Format(time.RFC3339Nano)
	if _, err := st.db.ExecContext(ctx, `UPDATE unit_trials SET settled=1,closed_ts=?,pnl_pc=.08,
return_per_dollar=.08/(ask+fee_pc),capital_day=.08/(ask+fee_pc),event_version=1,
canonical_event_id=CASE WHEN ticker IN ('A','B') THEN 'event-ab' ELSE 'event-c' END
WHERE family='contract-cost'`, closed); err != nil {
		t.Fatal(err)
	}

	wantAsk := (0.90 + 0.90 + 0.10) / 3
	wantFee := (0.09 + 0.09 + 0.01) / 3
	assertCosts := func(label string, n, contracts int, ask, fee float64) {
		t.Helper()
		if n != 7 || contracts != 3 || math.Abs(ask-wantAsk) > 1e-12 || math.Abs(fee-wantFee) > 1e-12 {
			t.Fatalf("%s raw repeats dominated contract costs: rows=%d contracts=%d ask=%v fee=%v want=%v/%v",
				label, n, contracts, ask, fee, wantAsk, wantFee)
		}
	}

	stats, err := st.UnitTrialStatsAt(ctx, now)
	if err != nil || len(stats) != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	assertCosts("stats", stats[0].N, stats[0].SettledMarkets, stats[0].MeanAsk, stats[0].MeanFeePC)
	// Event-cluster research remains separately event-weighted: AB is one .90 event and C is
	// one .10 event. It cannot silently replace the contract cohort above.
	if math.Abs(stats[0].EventClusterMeanAsk-.50) > 1e-12 {
		t.Fatalf("event diagnostic lost its separate weighting: %+v", stats[0])
	}

	leaders, err := st.UnitTrialLeaderboard(ctx, now)
	if err != nil || len(leaders) != 1 {
		t.Fatalf("leaders=%+v err=%v", leaders, err)
	}
	assertCosts("leaderboard", leaders[0].N, leaders[0].SettledMarkets, leaders[0].MeanAsk, leaders[0].MeanFeePC)
	// ProofMean* stays the separately labelled canonical-event/30d diagnostic. Event AB and
	// event C receive one vote each, so it intentionally remains .50 rather than authorizing PAPER.
	if math.Abs(leaders[0].ProofMeanAsk-.50) > 1e-12 ||
		math.Abs(leaders[0].ProofMeanFeePC-.05) > 1e-12 {
		t.Fatalf("canonical-event cost diagnostic changed cohort: %+v", leaders[0])
	}

	digest, err := st.UnitTrialDigest(ctx, now)
	if err != nil || len(digest) != 1 {
		t.Fatalf("digest=%+v err=%v", digest, err)
	}
	assertCosts("digest", digest[0].N, digest[0].SettledMarkets, digest[0].MeanAsk, digest[0].MeanFeePC)
}
