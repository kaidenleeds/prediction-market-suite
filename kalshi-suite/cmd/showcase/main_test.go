package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestDemoSeedsAndAdvances(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "showcase.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	srv := &demoServer{db: db, dbPath: dbPath}
	if err := srv.prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	before, err := srv.snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Markets) != 6 || len(before.Signals) < 3 || len(before.Trades) != 1 || len(before.Research) != 5 {
		t.Fatalf("unexpected seed counts: markets=%d signals=%d trades=%d research=%d", len(before.Markets), len(before.Signals), len(before.Trades), len(before.Research))
	}
	for i := 0; i < 6; i++ {
		if err := srv.step(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	after, err := srv.snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if after.Tick != 6 || len(after.Signals) <= len(before.Signals) || len(after.Trades) <= len(before.Trades) {
		t.Fatalf("demo did not advance: tick=%d signals=%d trades=%d", after.Tick, len(after.Signals), len(after.Trades))
	}
}
