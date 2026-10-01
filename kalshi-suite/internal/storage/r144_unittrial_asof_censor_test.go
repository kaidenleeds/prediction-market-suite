package storage

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestR144UnitTrialCompactReadersMatchLeaderboardAsOfCensor(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Date(2026, 7, 14, 18, 0, 0, 0, time.UTC)
	opened := now.Add(-2 * time.Hour)
	tickers := []string{"VALID", "MISSING-CLOSE", "PRE-OPEN-CLOSE", "FUTURE-CLOSE", "INVALID-CLOSE", "OPEN", "FUTURE-OPEN"}
	for _, ticker := range tickers {
		trialOpened := opened
		if ticker == "FUTURE-OPEN" {
			trialOpened = now.Add(time.Hour)
		}
		inserted, insertErr := st.InsertUnitTrial(ctx, UnitTrial{
			OpenedTS: trialOpened, Family: "asof-clock", Platform: "kalshi", Ticker: ticker,
			Side: "YES", Ask: .40, FeePC: .01, FeeKnown: true, FeeSource: "kalshi:test",
			Depth: 1, QuoteSource: "kalshi-ws",
		})
		if insertErr != nil || !inserted {
			t.Fatalf("insert %s=%v err=%v", ticker, inserted, insertErr)
		}
		if _, updateErr := st.db.ExecContext(ctx, `UPDATE unit_trials
SET canonical_event_id=?,event_version=1 WHERE ticker=?`, "event:"+ticker, ticker); updateErr != nil {
			t.Fatal(updateErr)
		}
	}
	updates := []struct {
		ticker string
		close  string
	}{
		{"VALID", now.Add(-time.Hour).Format(time.RFC3339Nano)},
		{"MISSING-CLOSE", ""},
		{"PRE-OPEN-CLOSE", now.Add(-3 * time.Hour).Format(time.RFC3339Nano)},
		{"FUTURE-CLOSE", now.Add(time.Hour).Format(time.RFC3339Nano)},
		{"INVALID-CLOSE", "not-a-clock"},
		{"FUTURE-OPEN", now.Add(2 * time.Hour).Format(time.RFC3339Nano)},
	}
	for _, update := range updates {
		if _, updateErr := st.db.ExecContext(ctx, `UPDATE unit_trials
SET settled=1,closed_ts=?,pnl_pc=.10,return_per_dollar=.25,capital_day=.25 WHERE ticker=?`,
			update.close, update.ticker); updateErr != nil {
			t.Fatal(updateErr)
		}
	}

	leaders, err := st.UnitTrialLeaderboard(ctx, now)
	if err != nil || len(leaders) != 1 {
		t.Fatalf("leaderboard=%+v err=%v", leaders, err)
	}
	digest, err := st.UnitTrialDigest(ctx, now)
	if err != nil || len(digest) != 1 {
		t.Fatalf("digest=%+v err=%v", digest, err)
	}
	stats, err := st.UnitTrialStatsAt(ctx, now)
	if err != nil || len(stats) != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}

	lead, compact, stat := leaders[0], digest[0], stats[0]
	if lead.Total != 6 || lead.N != 1 || lead.Open != 5 || lead.UniqueMarkets != 6 ||
		lead.SettledMarkets != 1 || lead.OpenMarkets != 5 || math.Abs(lead.TotalPnL-.10) > 1e-12 {
		t.Fatalf("full reader censor baseline wrong: %+v", lead)
	}
	if compact.Total != lead.Total || compact.N != lead.N || compact.Open != lead.Open ||
		compact.UniqueMarkets != lead.UniqueMarkets || compact.SettledMarkets != lead.SettledMarkets ||
		compact.OpenMarkets != lead.OpenMarkets || math.Abs(compact.TotalPnL-lead.TotalPnL) > 1e-12 {
		t.Fatalf("digest clock disagrees with full reader: digest=%+v leaderboard=%+v", compact, lead)
	}
	if stat.N != lead.N || stat.Open != lead.Open || stat.UniqueMarkets != lead.UniqueMarkets ||
		stat.SettledMarkets != lead.SettledMarkets || stat.OpenMarkets != lead.OpenMarkets ||
		math.Abs(stat.MeanPC-.10) > 1e-12 {
		t.Fatalf("stats clock disagrees with full reader: stats=%+v leaderboard=%+v", stat, lead)
	}
	for name, got := range map[string]struct {
		settled, open, clustered, unclustered int
	}{
		"leaderboard": {lead.SettledEventClusters, lead.OpenCanonicalEventClusters, lead.ClusteredSettledMarkets, lead.UnclusteredSettledMarkets},
		"digest":      {compact.SettledEventClusters, compact.OpenCanonicalEventClusters, compact.ClusteredSettledMarkets, compact.UnclusteredSettledMarkets},
		"stats":       {stat.SettledEventClusters, stat.OpenCanonicalEventClusters, stat.ClusteredSettledMarkets, stat.UnclusteredSettledMarkets},
	} {
		if got.settled != 1 || got.open != 5 || got.clustered != 1 || got.unclustered != 0 {
			t.Fatalf("%s event-cluster clock wrong: %+v", name, got)
		}
	}
}
