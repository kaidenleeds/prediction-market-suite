package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestMLPolyUSLotHasPriorityAndSettlesWithoutSignalRow(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	book := map[string]any{
		"open": []any{map[string]any{
			"platform": "polyus", "ticker": "PUS-ML-RESTART", "side": "YES",
			"price": .4, "contracts": 2.0, "fee": .02,
		}},
		"closed": []any{}, "lifetime": map[string]any{"net": 0.0, "wins": 0.0, "closed": 0.0},
		"stats": map[string]any{}, "bank0": 600.0,
	}
	raw, _ := json.Marshal(book)
	path := filepath.Join(s.cfg().DataDir, "ml_paper.json")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	targets := s.priorityPaperSettlementTargets()
	found := false
	for _, target := range targets {
		if target.Platform == "polyus" && target.Ticker == "PUS-ML-RESTART" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("PolyUS ML lot was absent from restart settlement priority targets")
	}
	if _, err := s.store.RecordVenueSettlement(ctx, "polyus", "PUS-ML-RESTART", 1,
		time.Now().UTC(), storage.PolyUSFinalEndpointSettlementV2); err != nil {
		t.Fatal(err)
	}
	s.settleMLBook(ctx)
	var got struct {
		Open   []map[string]any `json:"open"`
		Closed []map[string]any `json:"closed"`
	}
	if b, err := os.ReadFile(path); err != nil || json.Unmarshal(b, &got) != nil {
		t.Fatalf("read settled ML book: %v", err)
	}
	if len(got.Open) != 0 || len(got.Closed) != 1 {
		t.Fatalf("ML position did not reconcile from durable receipt: open=%d closed=%d",
			len(got.Open), len(got.Closed))
	}
}

func TestMalformedEmptyComboCannotBecomePhantomWin(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	id, err := s.store.InsertParlay(ctx, paper.Parlay{Stake: 10, Price: .5, Contracts: 20})
	if err != nil {
		t.Fatal(err)
	}
	s.settleParlays(ctx)
	open, err := s.store.ListParlays(ctx, "open")
	if err != nil || len(open) != 1 || open[0].ID != id {
		t.Fatalf("empty combo was paid as a win instead of held for repair: rows=%+v err=%v", open, err)
	}
}
