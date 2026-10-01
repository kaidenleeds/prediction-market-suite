package server

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR140PaperPositionsAggregateOncePerFillGeneration(t *testing.T) {
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	reads := 0
	s := &Server{store: st, log: slog.New(slog.NewTextHandler(io.Discard, nil)), fillsCacheTTL: time.Hour,
		fillsReadFn: func(context.Context) ([]paper.Fill, error) {
			reads++
			return []paper.Fill{{ID: 1, TS: "2026-01-01T00:00:00Z", Platform: "kalshi", Ticker: "KX1",
				Side: "YES", Action: "BUY", Price: .4, Contracts: 2, Source: "auto-test"}}, nil
		}}
	p1, _, generation, err := s.paperPositionsCached(context.Background())
	if err != nil || len(p1) != 1 || generation.IsZero() || reads != 1 {
		t.Fatalf("first positions=%+v generation=%v reads=%d err=%v", p1, generation, reads, err)
	}
	p1[0].CurPrice = .99
	p2, _, generation2, err := s.paperPositionsCached(context.Background())
	if err != nil || len(p2) != 1 || !generation.Equal(generation2) || reads != 1 || p2[0].CurPrice != 0 {
		t.Fatalf("cached positions=%+v generation=%v reads=%d err=%v", p2, generation2, reads, err)
	}
	s.fillsBust()
	if _, _, _, err := s.paperPositionsCached(context.Background()); err != nil || reads != 2 {
		t.Fatalf("busted generation reads=%d err=%v", reads, err)
	}
}
