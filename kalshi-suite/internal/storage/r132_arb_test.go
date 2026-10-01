package storage

import (
	"context"
	"testing"
)

func TestR132ListArbQuarantinesLegacyDutchRows(t *testing.T) {
	s, _ := openTemp(t)
	ctx := context.Background()
	for _, row := range []ArbRow{
		{Kind: "dutch-sell", Market: "poison-sell", BuyVenue: "polyus", SellVenue: "polyus"},
		{Kind: "dutch-buy", Market: "poison-buy", BuyVenue: "kalshi", SellVenue: "kalshi"},
		{Kind: "dutch-v2-sell", Market: "valid-v2", BuyVenue: "polyus", SellVenue: "polyus"},
		{Kind: "xv", Market: "valid-xv", BuyVenue: "kalshi", SellVenue: "polyus"},
	} {
		if err := s.InsertArb(ctx, row); err != nil {
			t.Fatalf("InsertArb(%s): %v", row.Kind, err)
		}
	}
	got, err := s.ListArb(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("ListArb returned %d rows, want only v2+xv: %+v", len(got), got)
	}
	for _, row := range got {
		if row.Kind == "dutch-sell" || row.Kind == "dutch-buy" {
			t.Fatalf("legacy Dutch poison escaped quarantine: %+v", row)
		}
	}
	var rawLegacy int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM arb_log WHERE kind IN ('dutch-sell','dutch-buy')`).Scan(&rawLegacy); err != nil {
		t.Fatal(err)
	}
	if rawLegacy != 2 {
		t.Fatalf("quarantine must preserve raw forensic rows, got %d", rawLegacy)
	}
}
