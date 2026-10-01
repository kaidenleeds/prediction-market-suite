package server

import (
	"context"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
)

func TestR146MLExecutorUsesCurrentVenueClockInsteadOfFrozenSignalHorizon(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) {
		c.Auto.ConsensusMaxHoursOut = 4
		c.Auto.ConsensusCryptoMaxHoursOut = 2
		c.Auto.PaperMLMaxHoursOut = 24
		c.Auto.PaperMLCryptoMaxHoursOut = 6
	})

	// This candidate may have been scored 20 hours out. The executor intentionally has no frozen
	// resolve-hours input: only this current venue clock can decide whether it has aged into range.
	current := 3.0
	s.paperHorizonFn = func(context.Context, string, string, string) float64 { return current }
	if hours, ok := s.mlExecutorCurrentHorizon(t.Context(), "polyus", "soccer-match", "Soccer match"); !ok || hours != 3 {
		t.Fatalf("current 3h market did not age into the Paper New ML funded window: hours=%v ok=%v", hours, ok)
	}

	current = 23
	if hours, ok := s.mlExecutorCurrentHorizon(t.Context(), "polyus", "soccer-match", "Soccer match"); !ok || hours != 23 {
		t.Fatalf("current 23h market did not enter the 24h Paper New ML window: hours=%v ok=%v", hours, ok)
	}
	if s.paperEntryHorizonNow(t.Context(), "polyus", "soccer-match", "Soccer match") {
		t.Fatal("23h Paper New ML candidate escaped the shorter final LIVE horizon")
	}
	current = 25
	if hours, ok := s.mlExecutorCurrentHorizon(t.Context(), "polyus", "soccer-match", "Soccer match"); ok || hours != 25 {
		t.Fatalf("current 25h market escaped the 24h Paper New ML window: hours=%v ok=%v", hours, ok)
	}

	current = 1.5
	if _, ok := s.mlExecutorCurrentHorizon(t.Context(), "kalshi", "KXBTCD-TEST", "BTC"); !ok {
		t.Fatal("current 1.5h crypto market should pass the 2h funded window")
	}
	current = 5
	if _, ok := s.mlExecutorCurrentHorizon(t.Context(), "kalshi", "KXBTCD-TEST", "BTC"); !ok {
		t.Fatal("current 5h crypto market should pass the 6h Paper New ML window")
	}
	if s.paperEntryHorizonNow(t.Context(), "kalshi", "KXBTCD-TEST", "BTC") {
		t.Fatal("5h Paper New ML crypto candidate escaped the shorter final LIVE horizon")
	}
	current = 7
	if _, ok := s.mlExecutorCurrentHorizon(t.Context(), "kalshi", "KXBTCD-TEST", "BTC"); ok {
		t.Fatal("current 7h crypto market escaped the 6h Paper New ML window")
	}
}
