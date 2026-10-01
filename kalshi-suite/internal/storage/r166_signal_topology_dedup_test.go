package storage

import (
	"context"
	"testing"
)

func TestR166SignalDedupKeepsDistinctPlatformAndInputTopology(t *testing.T) {
	st, _ := openTemp(t)
	ctx := context.Background()
	base := Signal{Platform: "kalshi", Ticker: "R166-SAME", Title: "exact topology",
		Side: "YES", SignalType: "arb", EntryPrice: .4}

	first := base
	first.ExecExpr = "input=K-PINT/input_at=2026-07-21T20:00:00Z"
	if inserted, err := st.InsertSignalResult(ctx, first); err != nil || !inserted {
		t.Fatalf("first topology inserted=%t err=%v", inserted, err)
	}
	second := base
	second.ExecExpr = "input=K-PUS/input_at=2026-07-21T20:00:00Z"
	if inserted, err := st.InsertSignalResult(ctx, second); err != nil || !inserted {
		t.Fatalf("second topology collapsed: inserted=%t err=%v", inserted, err)
	}
	if inserted, err := st.InsertSignalResult(ctx, second); err != nil || inserted {
		t.Fatalf("same exact topology was not deduped: inserted=%t err=%v", inserted, err)
	}
	otherVenue := second
	otherVenue.Platform = "polyus"
	if inserted, err := st.InsertSignalResult(ctx, otherVenue); err != nil || !inserted {
		t.Fatalf("other venue collapsed: inserted=%t err=%v", inserted, err)
	}

	rows, err := st.db.Query(`SELECT platform,input_topology FROM signal_log
WHERE ticker=? ORDER BY platform,input_topology`, base.Ticker)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := [][2]string{}
	for rows.Next() {
		var platform, topology string
		if err := rows.Scan(&platform, &topology); err != nil {
			t.Fatal(err)
		}
		got = append(got, [2]string{platform, topology})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := [][2]string{{"kalshi", "K-PINT"}, {"kalshi", "K-PUS"}, {"polyus", "K-PUS"}}
	if len(got) != len(want) {
		t.Fatalf("exact rows=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("exact rows=%v want=%v", got, want)
		}
	}
}
