package server

// R90 tests: the operator's ML BORDERS (plausibility band + EV window, boundary behavior is the
// spec) and the edge-29 realization haircut's gate application. Config defaults are asserted so
// an absent key can never silently disable the band.

import (
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
)

func borderServer(t *testing.T, pwMin, pwMax, evMinC, evMaxC float64) *Server {
	t.Helper()
	s := &Server{}
	c := config.Config{}
	c.Auto.MLPWinMin, c.Auto.MLPWinMax = pwMin, pwMax
	c.Auto.MLEVMinCents, c.Auto.MLEVMaxCents = evMinC, evMaxC
	c.Auto.RealizationHaircut = true
	s.cfgP.Store(&c)
	return s
}

func TestMLBordersBoundaries(t *testing.T) {
	s := borderServer(t, 0.05, 0.95, 2, 20)
	cases := []struct {
		name    string
		pwin    float64
		evNet   float64 // dollars/contract
		verdict string
	}{
		{"mid-band passes", 0.60, 0.05, ""},
		{"AT pwin floor passes (strict inequality trips)", 0.05, 0.05, ""},
		{"below pwin floor rejects", 0.049, 0.05, "reject"},
		{"AT pwin ceiling passes", 0.95, 0.05, ""},
		{"above pwin ceiling rejects", 0.951, 0.05, "reject"},
		{"AT ev floor passes (2¢)", 0.60, 0.02, ""},
		{"below ev floor rejects", 0.60, 0.0199, "reject"},
		{"AT ev ceiling passes (20¢)", 0.60, 0.20, ""},
		{"above ev ceiling QUARANTINES", 0.60, 0.201, "quarantine"},
	}
	for _, tc := range cases {
		if v, why := s.mlBorders(tc.pwin, tc.evNet); v != tc.verdict {
			t.Fatalf("%s: pwin=%v evNet=%v → verdict %q (%s), want %q", tc.name, tc.pwin, tc.evNet, v, why, tc.verdict)
		}
	}
}

func TestMLBordersExplicitZeroDisables(t *testing.T) {
	s := borderServer(t, 0, 0, 0, 0) // every border explicitly disabled
	for _, tc := range []struct{ pwin, ev float64 }{{0.001, -0.5}, {0.999, 5.0}} {
		if v, why := s.mlBorders(tc.pwin, tc.ev); v != "" {
			t.Fatalf("disabled borders must pass everything, got %q (%s) for pwin=%v ev=%v", v, why, tc.pwin, tc.ev)
		}
	}
}

func TestHaircutEVGateApplication(t *testing.T) {
	s := borderServer(t, 0.05, 0.95, 2, 20)
	s.realiz.ratios = map[string]float64{"kalshi-flow": 0.29, "pbridge": 0.0}
	s.realiz.ns = map[string]int{"kalshi-flow": 93, "pbridge": 8}

	if ev, hc := s.haircutEV("kalshi-flow", 0.10); ev < 0.0289 || ev > 0.0291 || hc != 0.29 {
		t.Fatalf("kalshi-flow haircut: got ev=%v hc=%v, want ≈0.029/0.29", ev, hc)
	}
	if ev, _ := s.haircutEV("pbridge", 0.17); ev != 0 {
		t.Fatalf("floor-0 family must zero its EV, got %v", ev)
	}
	if ev, hc := s.haircutEV("kcrypto", 0.05); ev != 0.05 || hc != 1 {
		t.Fatalf("unknown family must pass untouched at ratio 1, got ev=%v hc=%v", ev, hc)
	}
	if ev, _ := s.haircutEV("", 0.05); ev != 0.05 {
		t.Fatalf("empty family must pass untouched, got %v", ev)
	}
	if ev, _ := s.haircutEV("pbridge", -0.02); ev != -0.02 {
		t.Fatalf("negative EV is never scaled (already rejected downstream), got %v", ev)
	}
	// Log-only validation mode: ratio still reported, EV untouched.
	c := config.Config{}
	c.Auto.RealizationHaircut = false
	s.cfgP.Store(&c)
	if ev, hc := s.haircutEV("kalshi-flow", 0.10); ev != 0.10 || hc != 0.29 {
		t.Fatalf("log-only mode must not scale (ev=%v) but must report the ratio (hc=%v)", ev, hc)
	}
}

// The Go Default() must carry the operator's border defaults — an absent config key inherits
// these (Load unmarshals over Default), so the band can never be silently un-set.
func TestR90ConfigDefaults(t *testing.T) {
	d := config.Default()
	if d.Auto.MLPWinMin != 0.05 || d.Auto.MLPWinMax != 0.95 {
		t.Fatalf("pwin band defaults: got %v/%v want 0.05/0.95", d.Auto.MLPWinMin, d.Auto.MLPWinMax)
	}
	if d.Auto.MLEVMinCents != 2 || d.Auto.MLEVMaxCents != 20 {
		t.Fatalf("EV window defaults: got %v/%v want 2/20", d.Auto.MLEVMinCents, d.Auto.MLEVMaxCents)
	}
	if !d.Auto.RealizationHaircut || !d.Auto.MakerAdverseGuard {
		t.Fatalf("R90 realization/adverse defaults must be ON, got %v/%v", d.Auto.RealizationHaircut, d.Auto.MakerAdverseGuard)
	}
	if d.Kalshi.OrdersPollMS != 1000 {
		t.Fatalf("orders_poll_ms default: got %v want 1000 (bug 134)", d.Kalshi.OrdersPollMS)
	}
}
