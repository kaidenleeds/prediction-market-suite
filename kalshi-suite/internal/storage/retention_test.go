package storage

// R70 retention tests (audit §e): the bounded prune must age out audit/arb/maker-stats rows and
// the unresolvable whale-tape quarantine, blank ONLY stale resolved-signal path blobs — and must
// NEVER delete a signal_log row or a resolved (training) poly_trader_trades row.
//
// R124 POLICY AMENDMENT (operator approved — archive-then-prune): resolved ptt rows may only be
// MOVED to the archive DB (kalshi_archive.db, archive.go), never deleted. PruneRetention itself
// still never touches resolved=1 (pinned below); TestArchiveResolvedPTT pins the move policy:
// archive-row-count == moved-count, main+archive conserve every resolved row, crash-replay
// converges without loss or double-count.

import (
	"context"
	"testing"
	"time"
)

func TestPruneRetentionPolicy(t *testing.T) {
	s, _ := openTemp(t)
	ctx := context.Background()
	now := time.Now().UTC()
	oldTS := now.Add(-120 * 24 * time.Hour).Format(time.RFC3339Nano)   // > 90d
	ancient := now.Add(-200 * 24 * time.Hour).Format(time.RFC3339Nano) // > 180d
	fresh := now.Format(time.RFC3339Nano)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := s.db.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	count := func(q string, args ...any) int {
		t.Helper()
		var n int
		if err := s.db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
			t.Fatalf("count %q: %v", q, err)
		}
		return n
	}

	// audit_log / arb_log / maker_fill_stats: one stale + one fresh row each.
	exec(`INSERT INTO audit_log(ts,level,category,message,detail) VALUES(?,?,?,?,''),(?,?,?,?,'')`,
		oldTS, "info", "t", "old", fresh, "info", "t", "new")
	exec(`INSERT INTO arb_log(ts,slot,kind,market,category,buy_venue,sell_venue) VALUES(?,?,?,?,?,?,?),(?,?,?,?,?,?,?)`,
		oldTS, "s1", "xv", "m1", "", "kalshi", "polyus", fresh, "s2", "xv", "m2", "", "kalshi", "polyus")
	exec(`INSERT INTO maker_fill_stats(ts,platform,ticker,side,post_px) VALUES(?,?,?,?,0.5),(?,?,?,?,0.5)`,
		oldTS, "kalshi", "T1", "YES", fresh, "kalshi", "T2", "YES")

	// poly_trader_trades: an OLD unresolvable quarantine row (deletable), an OLD RESOLVED training
	// row (sacred) and an OLD still-open row (stays for the sweep). ts is unix seconds here.
	oldUnix := now.Add(-40 * 24 * time.Hour).Unix()
	exec(`INSERT INTO poly_trader_trades(wallet,ts,condition_id,asset,side,size,price,usdc_size,tx_hash,resolved,won)
	      VALUES('0xw',?, 'cond-dead','a1','BUY',1,0.5,1,'tx1',-1,NULL),
	            ('0xw',?, 'cond-train','a2','BUY',1,0.5,1,'tx2',1,1),
	            ('0xw',?, 'cond-open','a3','BUY',1,0.5,1,'tx3',0,NULL)`, oldUnix, oldUnix, oldUnix)
	// condition_status: one orphan (its condition has no unresolved trades) + one live.
	exec(`INSERT INTO poly_condition_status(condition_id,attempts,next_retry) VALUES('cond-dead',3,0),('cond-open',1,0)`)

	// signal_log: an ANCIENT resolved row with fat path blobs (blobs must blank, ROW must stay),
	// an ancient UNRESOLVED row (untouched — still in the sweep queue) and a fresh resolved row.
	exec(`INSERT INTO signal_log(ts,day,slot,platform,ticker,title,side,signal_type,entry_price,resolved,won,price_path,post_path)
	      VALUES(?,?,?,?,?,?,?,?,?,1,1,'[1,2,3]','0.5,0.6'),
	            (?,?,?,?,?,?,?,?,?,0,NULL,'[4,5]',''),
	            (?,?,?,?,?,?,?,?,?,1,0,'[7,8]','0.7')`,
		ancient, ancient[:10], "s1", "kalshi", "OLD-RES", "t", "YES", "kalshi-flow", 0.5,
		ancient, ancient[:10], "s2", "kalshi", "OLD-OPEN", "t", "YES", "kalshi-flow", 0.5,
		fresh, fresh[:10], "s3", "kalshi", "NEW-RES", "t", "YES", "kalshi-flow", 0.5)

	counts, err := s.PruneRetention(ctx)
	if err != nil {
		t.Fatalf("PruneRetention: %v", err)
	}
	for rule, want := range map[string]int64{
		"audit_log_90d": 1, "arb_log_90d": 1, "maker_fill_stats_90d": 1,
		"trader_trades_unresolvable_30d": 1, "condition_status_orphans": 1, "signal_path_blobs_180d": 1,
	} {
		if counts[rule] != want {
			t.Fatalf("rule %s touched %d rows, want %d (all: %v)", rule, counts[rule], want, counts)
		}
	}
	if n := count(`SELECT COUNT(*) FROM audit_log`); n != 1 {
		t.Fatalf("audit_log rows = %d, want 1 fresh survivor", n)
	}
	if n := count(`SELECT COUNT(*) FROM poly_trader_trades WHERE resolved=1`); n != 1 {
		t.Fatal("a RESOLVED (training) trade row was deleted — the prune must never touch training data (R124: only ArchiveResolvedPTT may MOVE it, never delete)")
	}
	if n := count(`SELECT COUNT(*) FROM poly_trader_trades WHERE resolved=0`); n != 1 {
		t.Fatal("an OPEN trade row was deleted — it belongs to the resolution sweep")
	}
	if n := count(`SELECT COUNT(*) FROM poly_condition_status WHERE condition_id='cond-open'`); n != 1 {
		t.Fatal("a LIVE condition-status row was deleted")
	}
	// signal_log: EVERY row survives; only the ancient RESOLVED row's blobs are blanked.
	if n := count(`SELECT COUNT(*) FROM signal_log`); n != 3 {
		t.Fatalf("signal_log rows = %d, want 3 — retention must NEVER delete signal rows", n)
	}
	if n := count(`SELECT COUNT(*) FROM signal_log WHERE ticker='OLD-RES' AND price_path='' AND post_path=''`); n != 1 {
		t.Fatal("ancient resolved row's path blobs must be blanked")
	}
	if n := count(`SELECT COUNT(*) FROM signal_log WHERE ticker='OLD-OPEN' AND price_path='[4,5]'`); n != 1 {
		t.Fatal("an UNRESOLVED row's path must be untouched (still awaiting settlement)")
	}
	if n := count(`SELECT COUNT(*) FROM signal_log WHERE ticker='NEW-RES' AND price_path='[7,8]'`); n != 1 {
		t.Fatal("a fresh resolved row's path must be untouched (recent training window)")
	}
	// drained: a second pass touches nothing (the monitor's stop condition).
	counts2, err := s.PruneRetention(ctx)
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	total := int64(0)
	for _, v := range counts2 {
		total += v
	}
	if total != 0 {
		t.Fatalf("second pass must be a no-op, touched %d (%v)", total, counts2)
	}
}

// TestArchiveResolvedPTT pins the R124 archive-then-prune policy: resolved ptt rows older than
// the cutoff may only be MOVED to kalshi_archive.db — never deleted — and the move is idempotent
// (a crash between the archive commit and the main delete re-converges without loss or
// double-count). Enforced invariants: archive-row-count == moved-count; main+archive together
// conserve every resolved row; open/quarantine/fresh-resolved rows never move.
func TestArchiveResolvedPTT(t *testing.T) {
	s, _ := openTemp(t)
	ctx := context.Background()
	now := time.Now().UTC()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := s.db.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	count := func(q string, args ...any) int {
		t.Helper()
		var n int
		if err := s.db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
			t.Fatalf("count %q: %v", q, err)
		}
		return n
	}
	oldUnix := now.Add(-40 * 24 * time.Hour).Unix()
	freshUnix := now.Unix()
	// Seed: two OLD resolved rows (must move), one FRESH resolved (stays hot), one OLD open
	// (resolution sweep's), one OLD unresolvable (PruneRetention's, never the archive's).
	exec(`INSERT INTO poly_trader_trades(wallet,ts,condition_id,asset,side,size,price,usdc_size,tx_hash,resolved,won)
	      VALUES('0xw',?, 'c1','a1','BUY',1,0.5,1,'t1',1,1),
	            ('0xw',?, 'c2','a2','BUY',1,0.4,1,'t2',1,0),
	            ('0xw',?, 'c3','a3','BUY',1,0.5,1,'t3',1,1),
	            ('0xw',?, 'c4','a4','BUY',1,0.5,1,'t4',0,NULL),
	            ('0xw',?, 'c5','a5','BUY',1,0.5,1,'t5',-1,NULL)`,
		oldUnix, oldUnix, freshUnix, oldUnix, oldUnix)
	resolvedBefore := count(`SELECT COUNT(*) FROM poly_trader_trades WHERE resolved=1`)

	moved := int64(0)
	for i := 0; i < 10; i++ { // batch=1 exercises the drain loop
		n, dup, err := s.ArchiveResolvedPTT(ctx, 30, 1)
		if err != nil {
			t.Fatalf("ArchiveResolvedPTT: %v", err)
		}
		if dup != 0 {
			t.Fatalf("no dedup expected on the first drain, got %d", dup)
		}
		if n == 0 {
			break
		}
		moved += n
	}
	if moved != 2 {
		t.Fatalf("moved %d rows, want 2 (the two old resolved rows)", moved)
	}
	archN, err := s.ArchivePTTCount(ctx)
	if err != nil {
		t.Fatalf("ArchivePTTCount: %v", err)
	}
	if archN != moved {
		t.Fatalf("MOVE invariant broken: archive rows (%d) != moved (%d)", archN, moved)
	}
	mainRes := count(`SELECT COUNT(*) FROM poly_trader_trades WHERE resolved=1`)
	if mainRes != 1 {
		t.Fatalf("fresh resolved row must stay in main, have %d resolved rows", mainRes)
	}
	if int64(mainRes)+archN != int64(resolvedBefore) {
		t.Fatalf("CONSERVATION broken: main %d + archive %d != seeded resolved %d — a resolved row was DELETED", mainRes, archN, resolvedBefore)
	}
	if n := count(`SELECT COUNT(*) FROM poly_trader_trades WHERE resolved=0`); n != 1 {
		t.Fatal("an OPEN row moved/vanished — the archive must only take resolved=1")
	}
	if n := count(`SELECT COUNT(*) FROM poly_trader_trades WHERE resolved=-1`); n != 1 {
		t.Fatal("a QUARANTINE row moved/vanished — resolved=-1 belongs to PruneRetention, not the archive")
	}
	// Drained: next pass is a no-op.
	if n, dup, err := s.ArchiveResolvedPTT(ctx, 30, 100); err != nil || n != 0 || dup != 0 {
		t.Fatalf("drained pass must be a (0,0,nil) no-op, got (%d,%d,%v)", n, dup, err)
	}
	// CRASH-REPLAY: simulate the non-atomic cross-file window (archive committed, main delete
	// lost) by restoring an archived row into main WITH ITS ORIGINAL id — the next pass must
	// re-move it via INSERT OR IGNORE + delete, without double-counting in the archive.
	exec(`INSERT INTO poly_trader_trades(id,wallet,ts,condition_id,asset,side,size,price,usdc_size,tx_hash,resolved,won)
	      VALUES(1,'0xw',?, 'c1','a1','BUY',1,0.5,1,'t1',1,1)`, oldUnix)
	n, dup, err := s.ArchiveResolvedPTT(ctx, 30, 100)
	if err != nil {
		t.Fatalf("crash-replay pass: %v", err)
	}
	if n != 1 || dup != 0 {
		t.Fatalf("crash-replay pass must re-move the duplicate as a MOVE (1,0), got (%d,%d)", n, dup)
	}
	if archN2, _ := s.ArchivePTTCount(ctx); archN2 != 2 {
		t.Fatalf("archive must dedup the replayed row (want 2 rows, have %d)", archN2)
	}
	// DEDUP RECONCILIATION (measured live R124): the whale puller re-inserts an already-archived
	// print under a NEW id (its dedup key was freed by the move) and it re-resolves — the pass
	// must reconcile it as a content-verified duplicate (deleted from main; the print stays
	// preserved in the archive; archive count unchanged).
	exec(`INSERT INTO poly_trader_trades(wallet,ts,condition_id,asset,side,size,price,usdc_size,tx_hash,resolved,won)
	      VALUES('0xw',?, 'c1','a1','BUY',1,0.5,1,'t1',1,1)`, oldUnix)
	n, dup, err = s.ArchiveResolvedPTT(ctx, 30, 100)
	if err != nil {
		t.Fatalf("dedup pass: %v", err)
	}
	if n != 0 || dup != 1 {
		t.Fatalf("re-inserted duplicate must reconcile as dedup (0,1), got (%d,%d)", n, dup)
	}
	if archN3, _ := s.ArchivePTTCount(ctx); archN3 != 2 {
		t.Fatalf("dedup must not grow the archive (want 2 rows, have %d)", archN3)
	}
	if nMain := count(`SELECT COUNT(*) FROM poly_trader_trades WHERE tx_hash='t1'`); nMain != 0 {
		t.Fatalf("reconciled duplicate must leave main, %d rows remain", nMain)
	}
	// CONTENT-MISMATCH collision (same dedup key, DIFFERENT price): must NEVER auto-delete —
	// the pass errors loudly and the row stays for inspection.
	exec(`INSERT INTO poly_trader_trades(wallet,ts,condition_id,asset,side,size,price,usdc_size,tx_hash,resolved,won)
	      VALUES('0xw',?, 'c1','a1','BUY',1,0.9,1,'t1',1,1)`, oldUnix)
	if _, _, err := s.ArchiveResolvedPTT(ctx, 30, 100); err == nil {
		t.Fatal("content-mismatch collision must surface as a wedge error, got nil")
	}
	if nMain := count(`SELECT COUNT(*) FROM poly_trader_trades WHERE tx_hash='t1' AND price=0.9`); nMain != 1 {
		t.Fatal("mismatched-content row must stay in main for inspection")
	}
	// READERS: the wallet-skill training set must span hot + archive (BasketWallets union path):
	// 2 archived BUY-resolved rows + 1 fresh = 3 observations for 0xw.
	bw, err := s.BasketWallets(ctx, 3, -1)
	if err != nil {
		t.Fatalf("BasketWallets(union): %v", err)
	}
	if !bw["0xw"] {
		t.Fatalf("BasketWallets must see hot+archive history (want 0xw at minN=3, got %v)", bw)
	}
	sk, err := s.WalletSkillScores(ctx)
	if err != nil {
		t.Fatalf("WalletSkillScores(union): %v", err)
	}
	if sk["0xw"] == nil || sk["0xw"].Markets != 3 {
		got := -1
		if sk["0xw"] != nil {
			got = sk["0xw"].Markets
		}
		t.Fatalf("WalletSkillScores must see hot+archive markets (want 3, got %d)", got)
	}
}

func TestArchiveResolvedPTTDeleteFailureCannotLoseRow(t *testing.T) {
	s, _ := openTemp(t)
	ctx := context.Background()
	oldUnix := time.Now().UTC().Add(-40 * 24 * time.Hour).Unix()
	if _, err := s.db.ExecContext(ctx, `INSERT INTO poly_trader_trades(
		wallet,ts,condition_id,asset,side,size,price,usdc_size,tx_hash,resolved,won)
		VALUES('0xcrash',?,'crash-c','crash-a','BUY',1,0.42,1,'crash-tx',1,1)`, oldUnix); err != nil {
		t.Fatal(err)
	}
	// Abort only the main-file delete. The archive phase must already be durable when this fires.
	if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER fail_archive_delete BEFORE DELETE ON poly_trader_trades
		WHEN OLD.tx_hash='crash-tx' BEGIN SELECT RAISE(ABORT,'injected delete crash'); END`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ArchiveResolvedPTT(ctx, 30, 100); err == nil {
		t.Fatal("injected main delete failure must be returned")
	}
	var mainN, archiveN int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM poly_trader_trades WHERE tx_hash='crash-tx'`).Scan(&mainN); err != nil {
		t.Fatal(err)
	}
	if err := s.archConn.QueryRowContext(ctx, `SELECT COUNT(*) FROM archive.poly_trader_trades WHERE tx_hash='crash-tx'`).Scan(&archiveN); err != nil {
		t.Fatal(err)
	}
	if mainN != 1 || archiveN != 1 {
		t.Fatalf("delete failure lost durability: main=%d archive=%d", mainN, archiveN)
	}
	if _, err := s.db.ExecContext(ctx, `DROP TRIGGER fail_archive_delete`); err != nil {
		t.Fatal(err)
	}
	if moved, dup, err := s.ArchiveResolvedPTT(ctx, 30, 100); err != nil || moved != 1 || dup != 0 {
		t.Fatalf("replay did not converge: moved=%d dup=%d err=%v", moved, dup, err)
	}
}

// A same primary key is only safe to prune after the committed archive row's full content matches.
func TestArchiveResolvedPTTRefusesMismatchedSameID(t *testing.T) {
	s, _ := openTemp(t)
	ctx := context.Background()
	oldUnix := time.Now().UTC().Add(-40 * 24 * time.Hour).Unix()
	if _, err := s.db.ExecContext(ctx, `INSERT INTO poly_trader_trades(
		id,wallet,ts,condition_id,asset,title,outcome,outcome_index,side,size,price,usdc_size,
		tx_hash,resolved,won,resolved_at,close_px)
		VALUES(77,'0xmain',?,'main-c','main-a','main title','YES',0,'BUY',2,0.42,0.84,
		'main-tx',1,1,'2026-06-01T00:00:00Z',1)`, oldUnix); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureArchive(ctx); err != nil {
		t.Fatal(err)
	}
	// A pre-existing archive PK with unrelated contents makes INSERT OR IGNORE report success but
	// is not a verified copy. The main row must remain and the mover must alarm instead of deleting.
	if _, err := s.archConn.ExecContext(ctx, `INSERT INTO archive.poly_trader_trades(
		id,wallet,ts,condition_id,asset,title,outcome,outcome_index,side,size,price,usdc_size,
		tx_hash,resolved,won,resolved_at,close_px)
		VALUES(77,'0xbad',?,'bad-c','bad-a','bad title','NO',1,'SELL',9,0.91,8.19,
		'bad-tx',1,0,'2026-06-02T00:00:00Z',0)`, oldUnix); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ArchiveResolvedPTT(ctx, 30, 100); err == nil {
		t.Fatal("mismatched same-id archive row authorized deletion")
	}
	var mainN int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM poly_trader_trades WHERE id=77 AND wallet='0xmain'`).Scan(&mainN); err != nil {
		t.Fatal(err)
	}
	if mainN != 1 {
		t.Fatalf("mismatched archive PK deleted main truth: main=%d", mainN)
	}
}

// TestR128ArchiveMoverClasses — the R125 one-row inspection outcome, pinned (see archive.go class
// notes): (1) float-precision price drift reconciles as a duplicate under the 1e-6 tolerance;
// (2) a won-disagreement syncs the FRESH main-side grade onto the archived print (venue re-query
// wins) and then reconciles; (3) two REAL prints sharing one tx_hash (different wallets — a batch
// transaction) BOTH archive under the widened arc_ptt_dedup2 key; (4) a same-wallet same-second
// MATERIAL price difference still wedges loudly (true anomaly, never auto-deleted).
func TestR128ArchiveMoverClasses(t *testing.T) {
	s, _ := openTemp(t)
	ctx := context.Background()
	now := time.Now().UTC()
	oldUnix := now.Add(-40 * 24 * time.Hour).Unix()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := s.db.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	// Seed + move one print so the archive holds it.
	exec(`INSERT INTO poly_trader_trades(wallet,ts,condition_id,asset,side,size,price,usdc_size,tx_hash,resolved,won)
	      VALUES('0xa',?, 'c1','a1','BUY',10,0.6189311779,1,'tx1',1,1)`, oldUnix)
	if n, _, err := s.ArchiveResolvedPTT(ctx, 30, 100); err != nil || n != 1 {
		t.Fatalf("seed move: n=%d err=%v", n, err)
	}
	// (1) FLOAT-PRECISION DRIFT: same print re-enters with the venue's full-precision price.
	exec(`INSERT INTO poly_trader_trades(wallet,ts,condition_id,asset,side,size,price,usdc_size,tx_hash,resolved,won)
	      VALUES('0xa',?, 'c1','a1','BUY',10,0.618931177942261,1,'tx1',1,1)`, oldUnix)
	if n, dup, err := s.ArchiveResolvedPTT(ctx, 30, 100); err != nil || n != 0 || dup != 1 {
		t.Fatalf("float-drift row must reconcile as dedup (0,1), got (%d,%d,%v)", n, dup, err)
	}
	// (2) won-DISAGREEMENT: the re-entered print carries a FRESH re-grade (won 1→0) — sync then
	// reconcile; the archived copy must carry the fresh grade afterwards.
	exec(`INSERT INTO poly_trader_trades(wallet,ts,condition_id,asset,side,size,price,usdc_size,tx_hash,resolved,won)
	      VALUES('0xa',?, 'c1','a1','BUY',10,0.6189311779,1,'tx1',1,0)`, oldUnix)
	if _, _, err := s.ArchiveResolvedPTT(ctx, 30, 100); err != nil {
		t.Fatalf("won-sync pass: %v", err)
	}
	if n, _, err := s.ArchiveResolvedPTT(ctx, 30, 100); err != nil || n != 0 {
		t.Fatalf("post-sync drain: n=%d err=%v", n, err)
	}
	var mainLeft int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM poly_trader_trades WHERE tx_hash='tx1'`).Scan(&mainLeft); err != nil || mainLeft != 0 {
		t.Fatalf("won-mismatch row must reconcile out of main after sync, %d remain (err=%v)", mainLeft, err)
	}
	var archWon int
	if err := s.archConn.QueryRowContext(ctx, `SELECT won FROM archive.poly_trader_trades WHERE tx_hash='tx1'`).Scan(&archWon); err != nil || archWon != 0 {
		t.Fatalf("archived print must carry the FRESH grade (won=0), got %d err=%v", archWon, err)
	}
	// (3) TWO REAL PRINTS IN ONE TX (different wallets — a batch transaction): main's own coarse
	// dedup key means they never coexist in main (the live history: print A archived, key freed,
	// print B ingested later). Both must end up preserved in the archive under the widened
	// arc_ptt_dedup2 key — pre-R128 print B was unarchivable forever (the wedge we found live).
	exec(`INSERT INTO poly_trader_trades(wallet,ts,condition_id,asset,side,size,price,usdc_size,tx_hash,resolved,won)
	      VALUES('0xb',?, 'c2','a2','BUY',488390.9,0.999,1,'tx2',1,1)`, oldUnix)
	if n, _, err := s.ArchiveResolvedPTT(ctx, 30, 100); err != nil || n != 1 {
		t.Fatalf("print A move: n=%d err=%v", n, err)
	}
	exec(`INSERT INTO poly_trader_trades(wallet,ts,condition_id,asset,side,size,price,usdc_size,tx_hash,resolved,won)
	      VALUES('0xc',?, 'c2','a2','BUY',18609.0,0.999,1,'tx2',1,1)`, oldUnix)
	if n, _, err := s.ArchiveResolvedPTT(ctx, 30, 100); err != nil || n != 1 {
		t.Fatalf("print B must archive under the widened key (pre-R128 this wedged): n=%d err=%v", n, err)
	}
	var archTx2 int
	if err := s.archConn.QueryRowContext(ctx, `SELECT COUNT(*) FROM archive.poly_trader_trades WHERE tx_hash='tx2'`).Scan(&archTx2); err != nil || archTx2 != 2 {
		t.Fatalf("both batch-tx prints must be preserved (want 2, got %d err=%v)", archTx2, err)
	}
	// (4) TRUE ANOMALY: same wallet+ts, MATERIALLY different price — stays and wedges.
	exec(`INSERT INTO poly_trader_trades(wallet,ts,condition_id,asset,side,size,price,usdc_size,tx_hash,resolved,won)
	      VALUES('0xa',?, 'c1','a1','BUY',10,0.9,1,'tx1',1,1)`, oldUnix)
	if _, _, err := s.ArchiveResolvedPTT(ctx, 30, 100); err == nil {
		t.Fatal("material content mismatch must still wedge loudly")
	}
}
