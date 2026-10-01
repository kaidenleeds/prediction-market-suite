package storage

import (
	"context"
	"reflect"
	"testing"
)

func TestR144PlabInsertRejectsRepeatedInstrumentLegs(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	c := PlabCand{ID: "duplicate-instrument", At: 1, Bucket: "indep", Class: "ind:2leg",
		Legality: "synthetic_legs_only", Prod: .25, JointP: .30, LegFees: "[0,0]",
		Legs:  `[{"platform":"kalshi","ticker":"SAME","side":"YES"},{"platform":"kalshi","ticker":"SAME","side":"NO"}]`,
		NLegs: 2, LegKeys: []PlabLegKey{{Platform: "kalshi", Ticker: "SAME"},
			{Platform: "KALSHI", Ticker: "same"}}}
	if n, err := st.PlabInsertBatch(ctx, []PlabCand{c}); err == nil || n != 0 {
		t.Fatalf("repeated instrument inserted: n=%d err=%v", n, err)
	}
	if n, err := st.PlabOpenCount(ctx); err != nil || n != 0 {
		t.Fatalf("rejected row left partial parent/legs: n=%d err=%v", n, err)
	}
}

func TestR144PlabReceiptMigrationDerivesVenueTickerMarketKeys(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if err := st.ensureComboLabCohortSchema(ctx); err != nil {
		t.Fatal(err)
	}
	// Simulate an all-in-v2 receipt written just before market_keys_json existed. The DEFAULT []
	// must remain readable and derive executable market identity from immutable legs_json.
	_, err = st.db.ExecContext(ctx, `INSERT INTO plab_grade_receipts(
combo_id,candidate_at,graded_ts,cell,bucket,class,legality,cohort,route_state,nlegs,prod,joint_p,
fees,entry_capital,joint_payout,realized_return,predicted_return,mve_valid,realized_mve_return,won,
independence_keys_json,legs_json,payouts_json,economics_version)
VALUES('legacy-split',1,'2026-07-14T00:00:00Z','indep|2leg|x','indep','x','synthetic_legs_only',
'all-eligible','synthetic-settlement-only',2,.25,.36,0,1,1,3,.44,0,0,1,
'["shared-resolution"]','[{"platform":"kalshi","ticker":"SAME"},{"platform":"polyus","ticker":"SAME"}]','[1,1]','all-in-v2')`)
	if err != nil {
		t.Fatal(err)
	}
	receipts, err := st.PlabGradeReceipts(ctx)
	if err != nil || len(receipts) != 1 {
		t.Fatalf("legacy receipt read rows=%d err=%v", len(receipts), err)
	}
	if !reflect.DeepEqual(receipts[0].MarketKeys, []string{"kalshi|SAME", "polyus|SAME"}) ||
		!reflect.DeepEqual(receipts[0].IndependenceKeys, []string{"shared-resolution"}) {
		t.Fatalf("market/dependence keys were not kept separate: %+v", receipts[0])
	}
}
