package server

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

func TestR132GenfollowSeesSharedAutoPositions(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	if _, err := s.store.InsertPaperFill(ctx, paper.Fill{
		TS: time.Now().UTC().Format(time.RFC3339), Platform: "kalshi", Ticker: "GF-SHARED",
		Title: "shared auto", Side: "YES", Action: "BUY", Price: 0.40, Contracts: 5,
		Source: "auto-cons-kalshi",
	}); err != nil {
		t.Fatal(err)
	}
	s.fillsBust()
	for _, side := range []string{"YES", "NO"} {
		if got := s.gfConflictReason(ctx, "kalshi", "GF-SHARED", side); got != "dup-ticker" {
			t.Fatalf("shared auto position with candidate %s returned %q, want dup-ticker", side, got)
		}
	}
	if got := s.gfConflictReason(ctx, "kalshi", "GF-OTHER", "YES"); got != "" {
		t.Fatalf("unrelated candidate blocked: %q", got)
	}
}
