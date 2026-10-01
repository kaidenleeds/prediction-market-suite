package server

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

func TestR135ManualPaperPlacementSurfaceRemovedButHistoryPreserved(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate server source")
	}
	serverSource, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "server.go"))
	if err != nil {
		t.Fatalf("read server.go: %v", err)
	}
	for _, forbidden := range []string{`POST /api/paper/order`, `handlePaperOrder`} {
		if strings.Contains(string(serverSource), forbidden) {
			t.Fatalf("manual paper placement server surface still contains %q", forbidden)
		}
	}
	for _, forbidden := range []string{`/api/paper/order`, `id="buycard"`, `confirmBuy(`, `openBuyTicker(`} {
		if strings.Contains(dashboardHTML, forbidden) {
			t.Fatalf("manual paper placement dashboard surface still contains %q", forbidden)
		}
	}

	// Removing placement is not a data deletion: old manual rows remain readable for audit/history.
	s := testServer(t)
	if _, err := s.store.InsertPaperFill(context.Background(), paper.Fill{
		Platform: "kalshi", Ticker: "HISTORICAL-MANUAL", Title: "historical", Side: "YES",
		Action: "BUY", Price: .40, Contracts: 1, Fee: .01, Source: "manual",
	}); err != nil {
		t.Fatalf("insert historical manual fill: %v", err)
	}
	fills, err := s.store.ListPaperFills(context.Background())
	if err != nil {
		t.Fatalf("list historical manual fills: %v", err)
	}
	if len(fills) != 1 || fills[0].Source != "manual" || fills[0].Ticker != "HISTORICAL-MANUAL" {
		t.Fatalf("historical manual fill was not preserved: %+v", fills)
	}
}
