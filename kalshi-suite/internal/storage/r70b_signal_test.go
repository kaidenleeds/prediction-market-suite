package storage

// R70-B: round-trip the new signal_log feature columns (holders_hhi/holders_skill/in_play/
// score_margin/oi_delta_1h) — locks the INSERT column↔placeholder alignment so a future column
// append can't silently shift the bind order, and proves nil pointers land as NULL.

import (
	"context"
	"database/sql"
	"testing"
)

func TestInsertSignalR70BColumnsRoundTrip(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	ctx := context.Background()

	hhi, skl, mg, oi := 0.42, -0.07, 3.0, 1250.5
	ip := int64(1)
	full := Signal{Platform: "polymarket", Ticker: "0xabc", Title: "T", Side: "YES", SignalType: "poly-consensus",
		EntryPrice: 0.55, HoldersHHI: &hhi, HoldersSkill: &skl, InPlay: &ip, ScoreMargin: &mg, OIDelta1h: &oi}
	if err := st.InsertSignal(ctx, full); err != nil {
		t.Fatalf("InsertSignal(full): %v", err)
	}
	empty := Signal{Platform: "polyus", Ticker: "slug-x", Title: "T2", Side: "NO", SignalType: "polyus-flow", EntryPrice: 0.40}
	if err := st.InsertSignal(ctx, empty); err != nil {
		t.Fatalf("InsertSignal(empty): %v", err)
	}

	var gh, gs, gm, go1 sql.NullFloat64
	var gip sql.NullInt64
	row := st.db.QueryRow(`SELECT holders_hhi, holders_skill, in_play, score_margin, oi_delta_1h FROM signal_log WHERE ticker='0xabc'`)
	if err := row.Scan(&gh, &gs, &gip, &gm, &go1); err != nil {
		t.Fatalf("scan full row: %v", err)
	}
	if !gh.Valid || gh.Float64 != hhi || !gs.Valid || gs.Float64 != skl || !gip.Valid || gip.Int64 != 1 ||
		!gm.Valid || gm.Float64 != mg || !go1.Valid || go1.Float64 != oi {
		t.Fatalf("full row round-trip mismatch: hhi=%+v skill=%+v in_play=%+v margin=%+v oi=%+v", gh, gs, gip, gm, go1)
	}
	// entry_price must be untouched by the new trailing binds (alignment canary).
	var ep float64
	if err := st.db.QueryRow(`SELECT entry_price FROM signal_log WHERE ticker='0xabc'`).Scan(&ep); err != nil || ep != 0.55 {
		t.Fatalf("entry_price = %v (err %v), want 0.55 — bind order shifted?", ep, err)
	}

	row = st.db.QueryRow(`SELECT holders_hhi, holders_skill, in_play, score_margin, oi_delta_1h FROM signal_log WHERE ticker='slug-x'`)
	if err := row.Scan(&gh, &gs, &gip, &gm, &go1); err != nil {
		t.Fatalf("scan empty row: %v", err)
	}
	if gh.Valid || gs.Valid || gip.Valid || gm.Valid || go1.Valid {
		t.Fatalf("nil pointers must land as NULL, got hhi=%+v skill=%+v in_play=%+v margin=%+v oi=%+v", gh, gs, gip, gm, go1)
	}
}
