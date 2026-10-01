package server

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR144ServerUnitTrialFreezesCatalogEventBeforeInsert(t *testing.T) {
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if err := st.UpsertMarketCatalog(ctx, []storage.CatalogRow{
		{Venue: "kalshi", Ticker: "KXGAME-SPREAD", EventKey: "KXGAME", Kind: "spread", Title: "Away at Home spread"},
		{Venue: "kalshi", Ticker: "KXGAME-TOTAL", EventKey: "KXGAME", Kind: "total", Title: "Away at Home total"},
	}); err != nil {
		t.Fatal(err)
	}
	s := &Server{store: st}
	for _, ticker := range []string{"KXGAME-SPREAD", "KXGAME-TOTAL"} {
		inserted, insertErr := s.insertCanonicalUnitTrial(ctx, storage.UnitTrial{
			OpenedTS: time.Now().UTC(), Family: "catalog-event-proof", Platform: "kalshi",
			Ticker: ticker, Side: "YES", Ask: .40, FeeKnown: true, FeeSource: "kalshi:test",
			Depth: 1, QuoteSource: "kalshi-ws",
		})
		if insertErr != nil || !inserted {
			t.Fatalf("insert %s=%v err=%v", ticker, inserted, insertErr)
		}
	}
	rows, err := st.DBForTest().Query(`SELECT canonical_event_id,event_version FROM unit_trials
WHERE family='catalog-event-proof' ORDER BY ticker`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var eventID string
	count := 0
	for rows.Next() {
		var got string
		var version int
		if err := rows.Scan(&got, &version); err != nil {
			t.Fatal(err)
		}
		if got == "" || version <= 0 {
			t.Fatalf("missing frozen identity %q/v%d", got, version)
		}
		if eventID != "" && got != eventID {
			t.Fatalf("related sub-bets split across events %q and %q", eventID, got)
		}
		eventID = got
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("rows=%d want 2", count)
	}
}

func TestR144UnitTrialMoneyTruthUpgradeAlsoFreezesNewCatalogIdentity(t *testing.T) {
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	trial := storage.UnitTrial{OpenedTS: time.Now().UTC().Add(-time.Minute), Family: "upgrade-identity",
		Platform: "kalshi", Ticker: "KXUPGRADE", Side: "YES", Episode: 1,
		Ask: .40, Depth: 0, FeeKnown: false, QuoteSource: "kalshi-ws"}
	if inserted, insertErr := st.InsertUnitTrial(ctx, trial); insertErr != nil || !inserted {
		t.Fatalf("legacy incomplete insert=%v err=%v", inserted, insertErr)
	}
	if err := st.UpsertMarketCatalog(ctx, []storage.CatalogRow{{Venue: "kalshi", Ticker: trial.Ticker,
		EventKey: "KXUPGRADE-EVENT", Kind: "winner", Title: "upgrade event"}}); err != nil {
		t.Fatal(err)
	}
	s := &Server{store: st}
	trial.OpenedTS, trial.Depth, trial.FeeKnown, trial.FeeSource = time.Now().UTC(), 1, true, "kalshi:test"
	inserted, insertErr := s.insertCanonicalUnitTrial(ctx, trial)
	if insertErr != nil || !inserted {
		t.Fatalf("complete upgrade=%v err=%v", inserted, insertErr)
	}
	var eventID string
	var version int
	if err := st.DBForTest().QueryRow(`SELECT canonical_event_id,event_version FROM unit_trials
WHERE family=? AND ticker=?`, trial.Family, trial.Ticker).Scan(&eventID, &version); err != nil {
		t.Fatal(err)
	}
	if eventID == "" || version <= 0 {
		t.Fatalf("money-truth upgrade stayed unclustered: %q/v%d", eventID, version)
	}
}
