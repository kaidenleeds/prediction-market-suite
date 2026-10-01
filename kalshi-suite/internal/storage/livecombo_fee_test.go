package storage

import (
	"context"
	"database/sql"
	"math"
	"path/filepath"
	"testing"
)

func TestLiveComboSettlementPersistsAndSubtractsExactAcceptedFee(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	ctx := context.Background()
	in := LiveComboInsert{
		Market: "KXMVE-TEST", Collection: "KXMVE", LegsJSON: `[{"ticker":"A","side":"yes"}]`,
		ModelCohort: "book-native-v2",
		Fair:        .9, ProdPrice: .2, Quote: .2, Contracts: 2, AcceptedFee: .04,
		FeeSource: "quadratic-estimate", FeeKnown: true, AcceptState: LiveComboDispatched, QuotesSeen: "[.2]",
		RFQID: "rfq-1", QuoteID: "quote-1",
	}
	if err := st.InsertLiveCombo(ctx, in); err != nil {
		t.Fatalf("InsertLiveCombo: %v", err)
	}
	open, err := st.ListOpenLiveCombos(ctx, 10)
	if err != nil || len(open) != 1 {
		t.Fatalf("ListOpenLiveCombos=(%d,%v), want one", len(open), err)
	}
	if !open[0].FeeKnown || open[0].AcceptedFee != .04 || open[0].AcceptState != LiveComboDispatched ||
		open[0].ModelCohort != "book-native-v2" ||
		open[0].RFQID != "rfq-1" || open[0].QuoteID != "quote-1" {
		t.Fatalf("exact dispatch fee estimate not round-tripped: %+v", open[0])
	}
	if err := st.ConfirmLiveComboExecution(ctx, open[0].ID, in.Market, LiveComboFilled, 2, .03,
		"kalshi-portfolio-fills:fee_cost", `{"fill_ids":["f1"]}`); err != nil {
		t.Fatalf("ConfirmLiveComboExecution: %v", err)
	}
	open, _ = st.ListOpenLiveCombos(ctx, 10)
	got, err := st.SettleLiveCombo(ctx, open[0].ID, 1)
	if err != nil {
		t.Fatalf("SettleLiveCombo: %v", err)
	}
	want := 2*(1-.2) - .03
	if !got.Reportable || !got.FeeKnown || math.Abs(got.Realized-want) > 1e-12 {
		t.Fatalf("settlement=%+v, want exact fee-net %.6f", got, want)
	}
	total, err := st.LiveCombosRealized(ctx)
	if err != nil || math.Abs(total-want) > 1e-12 {
		t.Fatalf("LiveCombosRealized=(%.6f,%v), want %.6f", total, err, want)
	}
	rows, err := st.ListLiveCombosAll(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("ListLiveCombosAll=(%d,%v)", len(rows), err)
	}
	if known, _ := rows[0]["fee_known"].(bool); !known || rows[0]["accepted_fee"] != .03 || rows[0]["fee_source"] != "kalshi-portfolio-fills:fee_cost" {
		t.Fatalf("export omitted exact fee provenance: %#v", rows[0])
	}
	if rows[0]["model_cohort"] != "book-native-v2" {
		t.Fatalf("export omitted model cohort provenance: %#v", rows[0])
	}
	if reportable, _ := rows[0]["fee_net_reportable"].(bool); !reportable {
		t.Fatalf("confirmed exact-fee settlement must be reportable: %#v", rows[0])
	}
}

func TestLiveComboLegacyMigrationIsIdempotentAndFeeUnknown(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "kalshi.db"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	_, err = db.Exec(`CREATE TABLE live_combos (
		id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, market TEXT NOT NULL,
		collection TEXT NOT NULL DEFAULT '', legs TEXT NOT NULL, fair REAL NOT NULL DEFAULT 0,
		prod_price REAL NOT NULL DEFAULT 0, quote REAL NOT NULL DEFAULT 0,
		contracts REAL NOT NULL DEFAULT 1, quotes_seen TEXT NOT NULL DEFAULT '',
		settled INTEGER NOT NULL DEFAULT 0, payout REAL NOT NULL DEFAULT 0,
		realized REAL NOT NULL DEFAULT 0, settled_ts TEXT NOT NULL DEFAULT '')`)
	if err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO live_combos
		(ts,market,legs,quote,contracts,settled,payout,realized,settled_ts) VALUES
		('2026-01-01T00:00:00Z','OLD-SETTLED','[]',.2,1,1,1,.8,'2026-01-02T00:00:00Z'),
		('2026-01-01T00:00:00Z','OLD-OPEN','[]',.3,2,0,0,0,'')`); err != nil {
		t.Fatalf("insert legacy rows: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy db: %v", err)
	}

	st, err := Open(dir)
	if err != nil {
		t.Fatalf("first migrated Open: %v", err)
	}
	ctx := context.Background()
	rows, err := st.ListLiveCombosAll(ctx, 10)
	if err != nil || len(rows) != 2 {
		t.Fatalf("migrated ListLiveCombosAll=(%d,%v)", len(rows), err)
	}
	for _, row := range rows {
		if known, _ := row["fee_known"].(bool); known || row["accept_state"] != LiveComboLegacyUnknown {
			t.Fatalf("legacy row must migrate fee/accept unknown: %#v", row)
		}
		if reportable, _ := row["fee_net_reportable"].(bool); reportable {
			t.Fatalf("legacy row must not export as fee-net reportable: %#v", row)
		}
		if row["model_cohort"] != "" {
			t.Fatalf("legacy row invented a model cohort: %#v", row)
		}
	}
	if total, err := st.LiveCombosRealized(ctx); err != nil || total != 0 {
		t.Fatalf("legacy gross P&L leaked into fee-net total: (%.6f,%v)", total, err)
	}
	open, err := st.ListOpenLiveCombos(ctx, 10)
	if err != nil || len(open) != 1 {
		t.Fatalf("migrated open combos=(%d,%v)", len(open), err)
	}
	settled, err := st.SettleLiveCombo(ctx, open[0].ID, 1)
	if err != nil {
		t.Fatalf("settle legacy unknown: %v", err)
	}
	if settled.FeeKnown || settled.Reportable || settled.Realized != 0 {
		t.Fatalf("legacy settlement became fee-net evidence: %+v", settled)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close migrated store: %v", err)
	}

	// A second boot must see the same schema and truth; duplicate ALTERs are harmless.
	st, err = Open(dir)
	if err != nil {
		t.Fatalf("second migrated Open: %v", err)
	}
	defer st.Close()
	if total, err := st.LiveCombosRealized(ctx); err != nil || total != 0 {
		t.Fatalf("idempotent reopen leaked legacy P&L: (%.6f,%v)", total, err)
	}
}

func TestLiveComboAmbiguousAcceptCanReconcileToAuthoritativeFullFill(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if err := st.InsertLiveCombo(ctx, LiveComboInsert{Market: "MVE-AMB", Collection: "COL",
		LegsJSON: `[]`, Quote: .3, Contracts: 1, AcceptedFee: .01, FeeSource: "estimate",
		FeeKnown: true, AcceptState: LiveComboAcceptAmbiguous, RFQID: "rfq-amb", QuoteID: "quote-amb"}); err != nil {
		t.Fatal(err)
	}
	rows, _ := st.ListOpenLiveCombos(ctx, 10)
	if len(rows) != 1 {
		t.Fatalf("ambiguous row missing: %+v", rows)
	}
	if err := st.ConfirmLiveComboExecution(ctx, rows[0].ID, rows[0].Market, LiveComboFilled, 1, .012,
		"kalshi-portfolio-fills:fee_cost", `{"order_id":"rfq-order"}`); err != nil {
		t.Fatalf("authoritative fill did not resolve ambiguous accept: %v", err)
	}
	rows, _ = st.ListOpenLiveCombos(ctx, 10)
	if len(rows) != 1 || rows[0].AcceptState != LiveComboFilled || rows[0].AcceptedFee != .012 {
		t.Fatalf("resolved ambiguous row=%+v", rows)
	}
}
