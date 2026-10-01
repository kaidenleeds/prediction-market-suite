package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR146ColdBootVerdictFailureKeepsDurableExactRoutes(t *testing.T) {
	s := testServer(t)
	want := verdictEnt{Family: "taker:route@kalshi", SourceFamily: "route",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES",
		Group: "taker", N: 17, Markets: 11, ContractMarkets: 11, Mean: .03}
	blob, err := json.Marshal(map[string]any{"experiments": []verdictEnt{want}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, "verdicts.json"), blob, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // every fresh DB query fails, exactly like a cold-boot lane deadline
	rows := s.computeExperimentVerdicts(ctx)
	for _, row := range rows {
		if row.Group == "taker" && row.SourceFamily == "route" && row.Platform == "kalshi" &&
			row.Side == "YES" && row.ContractMarkets == 11 {
			return
		}
	}
	t.Fatalf("cold-boot query failure erased durable exact route: %+v", rows)
}

func TestR146DirectSystemRejectionIsDurablyVisible(t *testing.T) {
	s := testServer(t)
	s.genfollowConsider(context.Background(), storage.Signal{Platform: "polymarket",
		Ticker: "research-only", Side: "YES", SignalType: "direct-system"})
	rows, err := s.store.ListAudit(context.Background(), 20)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Category == "system-route" && row.Message == "REJECTED direct-system polymarket research-only: invalid-venue" {
			return
		}
	}
	t.Fatalf("direct-system rejection disappeared from the funnel: %+v", rows)
}
