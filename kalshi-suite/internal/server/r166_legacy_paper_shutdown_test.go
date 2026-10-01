package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestR166LegacySyntheticPaperRoutesAreRetired(t *testing.T) {
	if !r166LegacySyntheticPaperRoute("kalshi", false, false) ||
		!r166LegacySyntheticPaperRoute("polyus", false, false) ||
		r166LegacySyntheticPaperRoute("kalshi", true, false) ||
		r166LegacySyntheticPaperRoute("kalshi", false, true) {
		t.Fatal("legacy Paper gate must block simulations but preserve real demo/prod boundaries")
	}

	s := testServer(t)
	ctx := context.Background()
	if s.autoPlace(ctx, "kalshi", "KX-R166-LEGACY", "legacy Paper", "YES", .50,
		.50, 0, 0, "auto-cons-test") {
		t.Fatal("legacy autoPlace reported a Paper placement")
	}
	fills, err := s.store.ListPaperFills(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(fills) != 0 {
		t.Fatalf("legacy autoPlace created %d immediate Paper fill(s)", len(fills))
	}
}

func TestR166KflowTwinAndMLBookCannotOpenNewModeledLots(t *testing.T) {
	s := testServer(t)
	s.kfBooks = &kflowBooks{}
	s.kflowBookPlace(0, "KX-R166-KFLOW", "old twin", "YES", .45, 1)
	if len(s.kfBooks.Pre.Open) != 0 || len(s.kfBooks.Live.Open) != 0 {
		t.Fatalf("retired Kalshi-flow twin opened a new lot: %+v", s.kfBooks)
	}

	source, err := os.ReadFile(filepath.Join("..", "..", "ml", "live_ml.py"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	if !strings.Contains(text, "ML_PAPER_NEW_ENTRIES_ENABLED = False") ||
		!strings.Contains(text, "if not ML_PAPER_NEW_ENTRIES_ENABLED:") ||
		!strings.Contains(text, "preds = []") {
		t.Fatal("book-native ML does not visibly settle history while refusing new modeled entries")
	}
}
