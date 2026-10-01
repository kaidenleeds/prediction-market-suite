package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestR139ConcreteSeriesAndCreditInputsAreImmutableZeroAuthority(t *testing.T) {
	st := nativeLockTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	inserted, err := st.InsertSeriesEventMap(ctx, "KXSERIES", "KXEVENT-1", now, "official event snapshot")
	if err != nil || !inserted {
		t.Fatalf("series inserted=%v err=%v", inserted, err)
	}
	inserted, err = st.InsertSeriesEventMap(ctx, "KXSERIES", "KXEVENT-1", now.Add(time.Minute), "official event snapshot")
	if err != nil || inserted {
		t.Fatalf("series duplicate inserted=%v err=%v", inserted, err)
	}
	credit := ResearchCreditEvent{Venue: "kalshi", InstrumentID: "KXSETTLED", CreditAt: now,
		Quantity: 2, CreditedAmount: 1.7, AmountKnown: true, SettlePx: -1,
		SourceArtifact: "Kalshi GET /portfolio/settlements", SourceHash: "sha256:fixture"}
	inserted, err = st.InsertResearchCreditEvent(ctx, credit)
	if err != nil || !inserted {
		t.Fatalf("credit inserted=%v err=%v", inserted, err)
	}
	rows, err := st.RecentResearchCreditEvents(ctx, now.Add(-time.Minute), 10)
	if err != nil || len(rows) != 1 || !rows[0].AmountKnown || rows[0].CreditedAmount != 1.7 {
		t.Fatalf("credits=%+v err=%v", rows, err)
	}
	var sf, sp, sl, cf, cp, cl int
	if err := st.db.QueryRow(`SELECT funded,paper_authority,live_authority FROM research_series_event_map`).Scan(&sf, &sp, &sl); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT funded,paper_authority,live_authority FROM research_credit_events`).Scan(&cf, &cp, &cl); err != nil {
		t.Fatal(err)
	}
	if sf+sp+sl+cf+cp+cl != 0 {
		t.Fatalf("research inputs gained authority series=%d/%d/%d credit=%d/%d/%d", sf, sp, sl, cf, cp, cl)
	}
	if _, err := st.db.Exec(`UPDATE research_credit_events SET credited_amount=99`); err == nil {
		t.Fatal("credit event was mutable")
	}
}

func TestR139ConcreteSignalReaderRequiresCompleteBookNativeMoneyTruth(t *testing.T) {
	st := nativeLockTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	insert := func(ticker string, complete bool) {
		t.Helper()
		version, source := "", ""
		var bid, ask, bidDepth, askDepth, age, mtick, ttick, mf, tf, latency any
		if complete {
			version, source = "book-native-v2", "kalshi_book_ws"
			bid, ask, bidDepth, askDepth, age = .39, .41, 5.0, 6.0, .2
			mtick, ttick, mf, tf, latency = .01, .01, 0.0, .01, 2.0
		}
		_, err := st.db.Exec(`INSERT INTO signal_log(ts,day,slot,platform,ticker,title,side,signal_type,
entry_price,book_feature_ver,book_bid,book_ask,book_bid_depth,book_ask_depth,book_quote_age_s,
book_maker_tick,book_taker_tick,book_maker_fee_pc,book_taker_fee_pc,book_latency_ms,book_source,
pricing_version,label_version,resolved) VALUES(?,?,?,?,?,?,?,?,?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,0)`,
			now.Format(time.RFC3339Nano), now.Format("2006-01-02"), ticker, "kalshi", ticker,
			"fixture", "YES", "fbridge", .4, bid, ask, bidDepth, askDepth, age, mtick, ttick,
			mf, tf, latency, source, version, "kalshi_start_clock_v2")
		if err != nil {
			t.Fatal(err)
		}
	}
	insert("GOOD", true)
	insert("BAD", false)
	rows, err := st.RecentConcreteSignalTriggers(ctx, now.Add(-time.Minute), 10)
	if err != nil || len(rows) != 1 || rows[0].Ticker != "GOOD" || rows[0].BookAsk != .41 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
}

func TestR144ConcreteSignalKeysetCursorsCoverTailFamiliesAndReplaySafely(t *testing.T) {
	st := nativeLockTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	stmt, err := st.db.Prepare(`INSERT INTO signal_log(ts,day,slot,platform,ticker,title,side,signal_type,
entry_price,book_feature_ver,book_bid,book_ask,book_bid_depth,book_ask_depth,book_quote_age_s,
book_maker_tick,book_taker_tick,book_maker_fee_pc,book_taker_fee_pc,book_latency_ms,book_source,
pricing_version,label_version,resolved) VALUES(?,?,?,?,?,?,?,?,?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,0)`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < 151; i++ {
		family := "dominant-flow"
		if i >= 130 {
			family = fmt.Sprintf("quiet-family-%02d", i-130)
		}
		ticker := fmt.Sprintf("CURSOR-%03d", i)
		if _, err := stmt.Exec(now.Format(time.RFC3339Nano), now.Format("2006-01-02"), "slot",
			"kalshi", ticker, "fixture", "YES", family, .4, .39, .41, 5.0, 6.0, .2,
			.01, .01, 0.0, .01, 2.0, "kalshi_book_ws", "book-native-v2",
			"kalshi_start_clock_v2"); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	const pairsConsumer = "paired-and-attention-test"
	first, err := st.ConcreteSignalTriggersAfter(ctx, now.Add(-time.Minute), 0, 100)
	if err != nil || len(first) != 100 {
		t.Fatalf("first page n=%d err=%v", len(first), err)
	}
	for i := range first {
		if first[i].Family != "dominant-flow" || (i > 0 && first[i].ID <= first[i-1].ID) {
			t.Fatalf("first page lost oldest-first order/diversity contract at %d: %+v", i, first[i])
		}
	}
	// A crash before cursor persistence must replay the exact immutable IDs. Downstream experiment
	// opportunity keys make this replay idempotent rather than counting a second sample.
	replay, err := st.ConcreteSignalTriggersAfter(ctx, now.Add(-time.Minute), 0, 100)
	if err != nil || len(replay) != len(first) {
		t.Fatalf("replay n=%d err=%v", len(replay), err)
	}
	for i := range first {
		if replay[i].ID != first[i].ID {
			t.Fatalf("replay changed id at %d: %d != %d", i, replay[i].ID, first[i].ID)
		}
	}
	if err := st.AdvanceConcreteSignalCursor(ctx, pairsConsumer, first[len(first)-1].ID); err != nil {
		t.Fatal(err)
	}
	cursor, err := st.ConcreteSignalCursor(ctx, pairsConsumer)
	if err != nil || cursor != first[len(first)-1].ID {
		t.Fatalf("pairs cursor=%d err=%v", cursor, err)
	}
	second, err := st.ConcreteSignalTriggersAfter(ctx, now.Add(-time.Minute), cursor, 100)
	if err != nil || len(second) != 51 {
		t.Fatalf("second page n=%d err=%v", len(second), err)
	}
	quiet := map[string]bool{}
	for _, row := range second {
		if row.Family != "dominant-flow" {
			quiet[row.Family] = true
		}
	}
	if len(quiet) != 21 {
		t.Fatalf("quiet families starved: covered=%d rows=%+v", len(quiet), second)
	}
	if err := st.AdvanceConcreteSignalCursor(ctx, pairsConsumer, second[len(second)-1].ID); err != nil {
		t.Fatal(err)
	}
	// Cursor updates are monotone, and the carry consumer is independent from the paired consumer.
	if err := st.AdvanceConcreteSignalCursor(ctx, pairsConsumer, first[0].ID); err != nil {
		t.Fatal(err)
	}
	if cursor, err = st.ConcreteSignalCursor(ctx, pairsConsumer); err != nil || cursor != second[len(second)-1].ID {
		t.Fatalf("cursor regressed=%d err=%v", cursor, err)
	}
	carryCursor, err := st.ConcreteSignalCursor(ctx, "settlement-latency-carry-test")
	if err != nil || carryCursor != 0 {
		t.Fatalf("carry cursor contaminated=%d err=%v", carryCursor, err)
	}
	carryFirst, err := st.ConcreteSignalTriggersAfter(ctx, now.Add(-time.Minute), carryCursor, 100)
	if err != nil || len(carryFirst) != 100 || carryFirst[0].ID != first[0].ID {
		t.Fatalf("independent carry page n=%d first=%v err=%v", len(carryFirst), func() int64 {
			if len(carryFirst) == 0 {
				return 0
			}
			return carryFirst[0].ID
		}(), err)
	}
}
