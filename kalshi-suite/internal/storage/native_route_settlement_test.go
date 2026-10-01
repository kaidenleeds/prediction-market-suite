package storage

import (
	"context"
	"testing"
	"time"
)

func TestR139VenueSettlementReceiptIsExactAndPlatformScoped(t *testing.T) {
	st := nativeLockTestStore(t)
	ctx := context.Background()
	opened := time.Now().UTC().Add(-2 * time.Hour)
	resolved := opened.Add(time.Hour)
	insert := func(platform, signal string, settle any, won int) {
		t.Helper()
		if _, err := st.db.Exec(`INSERT INTO signal_log(
ts,day,slot,platform,ticker,title,side,signal_type,entry_price,resolved,won,settle_val,resolved_at)
VALUES(?,?,?,?,?,?,?,?,?,1,?,?,?)`, opened.Format(time.RFC3339Nano), opened.Format("2006-01-02"),
			opened.Format("200601021504"), platform, "SAME", "fixture", "YES", signal, .5,
			won, settle, resolved.Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
	}
	insert("kalshi", "kalshi-exact", 1.0, 1)
	insert("polyus", "polyus-exact", 0.0, 0)
	// PolyUS exact numeric signal rows are no longer settlement authority because they do not
	// retain whether the venue source was preliminary. The versioned durable receipt does.
	if inserted, err := st.RecordVenueSettlement(ctx, "polyus", "SAME", 0, resolved,
		PolyUSFinalBookSettlementV2); err != nil || !inserted {
		t.Fatalf("PolyUS final receipt inserted=%v err=%v", inserted, err)
	}
	k, ok, err := st.VenueSettlementForTicker(ctx, "kalshi", "SAME")
	if err != nil || !ok || k.YesValue != 1 || k.Platform != "kalshi" || k.Hash == "" {
		t.Fatalf("kalshi=%+v ok=%v err=%v", k, ok, err)
	}
	p, ok, err := st.VenueSettlementForTicker(ctx, "polyus", "SAME")
	if err != nil || !ok || p.YesValue != 0 || p.Platform != "polyus" || p.Hash == k.Hash {
		t.Fatalf("polyus=%+v ok=%v err=%v", p, ok, err)
	}
	// A side-relative win with the legacy -1 sentinel instead of exact settle_val is intentionally insufficient.
	if _, err := st.db.Exec(`INSERT INTO signal_log(
ts,day,slot,platform,ticker,title,side,signal_type,entry_price,resolved,won,settle_val,resolved_at)
VALUES(?,?,?,?,?,?,?,?,?,1,1,-1,?)`, opened.Format(time.RFC3339Nano), opened.Format("2006-01-02"),
		opened.Format("200601021504"), "kalshi", "LEGACY", "fixture", "YES", "legacy-won-only", .5,
		resolved.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := st.VenueSettlementForTicker(ctx, "kalshi", "LEGACY"); err != nil || ok {
		t.Fatalf("won-only row became settlement got=%+v ok=%v err=%v", got, ok, err)
	}
}

func TestR139TerminalCollectorContractsAreImmutableAndZeroAuthority(t *testing.T) {
	st := nativeLockTestStore(t)
	ctx := context.Background()
	for _, id := range []string{"native-route-terminal-grades", "research-system-terminal-grades"} {
		spec, err := st.currentCollectorSpec(ctx, id)
		if err != nil || spec.ExpectedCadence != 5*time.Minute || spec.SchemaVersion == "" || len(spec.Systems) == 0 {
			t.Fatalf("%s spec=%+v err=%v", id, spec, err)
		}
		drift := spec
		drift.ZeroPolicy += " drift"
		if _, err := st.RegisterCollectorSpec(ctx, drift); err == nil {
			t.Fatalf("%s immutable drift accepted", id)
		}
	}
}
