package storage

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func r132Condition(hexPair string) string { return "0x" + strings.Repeat(hexPair, 32) }

func TestR132InsertPolyTraderTradeQuarantinesAndCanRepairMalformedCondition(t *testing.T) {
	s, _ := openTemp(t)
	ctx := context.Background()
	bad := "0x" + strings.Repeat("a", 62)
	base := TraderTrade{
		Wallet: "0xwallet", TS: 100, ConditionID: bad, Asset: "123456", Side: "BUY",
		Size: 2, Price: .4, UsdcSize: .8, TxHash: "0xtx",
	}
	if err := s.InsertPolyTraderTrade(ctx, base); err != nil {
		t.Fatal(err)
	}
	var conditionID, resolvedAt string
	var resolved int
	if err := s.db.QueryRow(`SELECT condition_id,resolved,COALESCE(resolved_at,'') FROM poly_trader_trades WHERE tx_hash=?`, base.TxHash).
		Scan(&conditionID, &resolved, &resolvedAt); err != nil {
		t.Fatal(err)
	}
	if conditionID != bad || resolved != -3 || resolvedAt == "" {
		t.Fatalf("malformed row condition=%q resolved=%d resolved_at=%q", conditionID, resolved, resolvedAt)
	}
	openRows, _, graded, quarantined, err := s.PTTBacklogStats(ctx)
	if err != nil || openRows != 0 || graded != 0 || quarantined != 1 {
		t.Fatalf("backlog open=%d graded=%d quarantined=%d err=%v", openRows, graded, quarantined, err)
	}

	// A later overlapping activity pull that successfully recovers the token must repair the
	// quarantined print rather than losing to the existing tx+asset+side dedup row.
	good := r132Condition("aB")
	base.ConditionID = good
	if err := s.InsertPolyTraderTrade(ctx, base); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT condition_id,resolved,COALESCE(resolved_at,'') FROM poly_trader_trades WHERE tx_hash=?`, base.TxHash).
		Scan(&conditionID, &resolved, &resolvedAt); err != nil {
		t.Fatal(err)
	}
	if conditionID != strings.ToLower(good) || resolved != 0 || resolvedAt != "" {
		t.Fatalf("repaired row condition=%q resolved=%d resolved_at=%q", conditionID, resolved, resolvedAt)
	}
	openRows, _, graded, quarantined, err = s.PTTBacklogStats(ctx)
	if err != nil || openRows != 1 || graded != 0 || quarantined != 0 {
		t.Fatalf("repaired backlog open=%d graded=%d quarantined=%d err=%v", openRows, graded, quarantined, err)
	}
}

func TestR132OpenMigratesMalformedConditionBacklogOnce(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	bad := "0x" + strings.Repeat("b", 62)
	good := r132Condition("cd")
	for _, q := range []struct {
		condition string
		tx        string
	}{
		{bad, "bad-tx"},
		{good, "good-tx"},
	} {
		if _, err := s.db.Exec(`INSERT INTO poly_trader_trades(wallet,ts,condition_id,asset,side,size,price,usdc_size,tx_hash,resolved)
VALUES('w',1,?,'asset-'||?,'BUY',1,.5,.5,?,0)`, q.condition, q.tx, q.tx); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`INSERT INTO poly_condition_status(condition_id,attempts,next_retry) VALUES(?,1,0)`, q.condition); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.Exec(`DELETE FROM kv WHERE k='r132_ptt_condition_quarantine'`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(filepath.Clean(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var badResolved, goodResolved int
	var badResolvedAt string
	if err := s.db.QueryRow(`SELECT resolved,COALESCE(resolved_at,'') FROM poly_trader_trades WHERE condition_id=?`, bad).
		Scan(&badResolved, &badResolvedAt); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT resolved FROM poly_trader_trades WHERE condition_id=?`, good).Scan(&goodResolved); err != nil {
		t.Fatal(err)
	}
	if badResolved != -3 || badResolvedAt == "" || goodResolved != 0 {
		t.Fatalf("bad=(%d,%q) good=%d", badResolved, badResolvedAt, goodResolved)
	}
	var badStatus, goodStatus, auditRows, latchRows int
	_ = s.db.QueryRow(`SELECT COUNT(*) FROM poly_condition_status WHERE condition_id=?`, bad).Scan(&badStatus)
	_ = s.db.QueryRow(`SELECT COUNT(*) FROM poly_condition_status WHERE condition_id=?`, good).Scan(&goodStatus)
	_ = s.db.QueryRow(`SELECT COUNT(*) FROM audit_log WHERE message='R132 malformed Polymarket condition ids quarantined'`).Scan(&auditRows)
	_ = s.db.QueryRow(`SELECT COUNT(*) FROM kv WHERE k='r132_ptt_condition_quarantine' AND v='1'`).Scan(&latchRows)
	if badStatus != 0 || goodStatus != 1 || auditRows != 1 || latchRows != 1 {
		t.Fatalf("condition status bad=%d good=%d audit=%d latch=%d", badStatus, goodStatus, auditRows, latchRows)
	}
}
