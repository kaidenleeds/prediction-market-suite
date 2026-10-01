package server

import (
	"context"
	"testing"
	"time"
)

func TestR166PaperResetCancelsOldFundedMakerPostsButKeepsNewAndResearchOnly(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	epoch := time.Now().UTC()
	newAttempt := func(ticker string) int64 {
		t.Helper()
		id, err := s.store.InsertMakerAttempt(ctx, "kalshi", ticker, "YES", "test", .40,
			1, 5, 0, nil, 0, "", "test-book")
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	oldML := newAttempt("R166-OLD-ML")
	currentRaw := newAttempt("R166-CURRENT-RAW")
	oldResearch := newAttempt("R166-OLD-RESEARCH")
	s.pendingMakers = []*pendingMaker{
		{id: oldML, platform: "kalshi", ticker: "R166-OLD-ML", book: "ml", postedAt: epoch.Add(-time.Second)},
		{id: currentRaw, platform: "kalshi", ticker: "R166-CURRENT-RAW", book: "rawflow", postedAt: epoch.Add(time.Second)},
		{id: oldResearch, platform: "kalshi", ticker: "R166-OLD-RESEARCH", book: "subcent-golf", postedAt: epoch.Add(-time.Second)},
	}
	n, byBook, err := s.cancelPreResetPaperMakerPosts(ctx, epoch)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || byBook["ml"] != 1 {
		t.Fatalf("canceled=%d by_book=%+v", n, byBook)
	}
	if len(s.pendingMakers) != 2 || s.pendingMakers[0].id != currentRaw || s.pendingMakers[1].id != oldResearch {
		t.Fatalf("kept pending=%+v", s.pendingMakers)
	}
	for _, tc := range []struct {
		id         int64
		wantFilled int
		wantRule   string
	}{{oldML, 0, "paper-reset-epoch"}, {currentRaw, -1, ""}, {oldResearch, -1, ""}} {
		var filled int
		var rule string
		if err := s.store.DBForTest().QueryRowContext(ctx,
			`SELECT filled,COALESCE(cancel_rule,'') FROM maker_fill_stats WHERE id=?`, tc.id).Scan(&filled, &rule); err != nil {
			t.Fatal(err)
		}
		if filled != tc.wantFilled || rule != tc.wantRule {
			t.Fatalf("maker id=%d filled=%d rule=%q want %d/%q", tc.id, filled, rule, tc.wantFilled, tc.wantRule)
		}
	}
}
