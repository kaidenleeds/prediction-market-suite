package server

import (
	"context"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR153SpotlagSignalPreservesOriginalGates(t *testing.T) {
	at := time.Date(2026, 7, 16, 18, 0, 0, 0, time.UTC)
	yes, ok := spotlagSignalFromInputs("KXBTC15M", "BTC up", .42, 64000, .031, .01, .02, true, at)
	if !ok || yes.Side != "YES" || yes.EntryPrice != .42 || yes.Strength <= 1.5 ||
		yes.SignalType != "spotlag" || yes.ExecExpr == "" {
		t.Fatalf("valid YES spotlag was changed or suppressed: ok=%v signal=%+v", ok, yes)
	}
	no, ok := spotlagSignalFromInputs("KXBTC15M", "BTC up", .42, 64000, -.031, .01, .02, true, at)
	if !ok || no.Side != "NO" || math.Abs(no.EntryPrice-.58) > 1e-12 || no.Strength <= 1.5 {
		t.Fatalf("valid NO spotlag was changed or suppressed: ok=%v signal=%+v", ok, no)
	}

	rejections := []struct {
		name                       string
		kup, move, momentum, sigma float64
		known                      bool
	}{
		{name: "unknown input", kup: .42, move: .04, momentum: .01, sigma: .02, known: false},
		{name: "below 1.5 sigma", kup: .42, move: .029, momentum: .01, sigma: .02, known: true},
		{name: "contract moved 2c", kup: .42, move: .04, momentum: .02, sigma: .02, known: true},
		{name: "price boundary", kup: .02, move: .04, momentum: .01, sigma: .02, known: true},
	}
	for _, tc := range rejections {
		t.Run(tc.name, func(t *testing.T) {
			if got, emit := spotlagSignalFromInputs("KXBTC15M", "BTC up", tc.kup, 64000,
				tc.move, tc.momentum, tc.sigma, tc.known, at); emit {
				t.Fatalf("invalid input emitted spotlag: %+v", got)
			}
		})
	}
}

func TestR153OnlyProvedKalshiPreSubmitRefusalReleasesClaim(t *testing.T) {
	if !liveMirrorHandlerProvesNoVenueCall("kalshi", http.StatusForbidden,
		map[string]any{"error": "current proof moved"}) {
		t.Fatal("proved Kalshi pre-submit refusal was not releasable")
	}
	if liveMirrorHandlerProvesNoVenueCall("kalshi", http.StatusBadGateway,
		map[string]any{"error": "timeout", "execution_state": "ambiguous"}) {
		t.Fatal("ambiguous post-submit outcome was marked releasable")
	}
	if liveMirrorHandlerProvesNoVenueCall("polyus", http.StatusForbidden,
		map[string]any{"error": "pre-submit"}) {
		t.Fatal("PolyUS response contract incorrectly inherited the Kalshi shortcut")
	}
	if liveMirrorHandlerProvesNoVenueCall("kalshi", http.StatusOK,
		map[string]any{"ok": false, "execution_state": "rejected"}) {
		t.Fatal("venue-called clean rejection was marked releasable")
	}

	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	s := &Server{store: st}
	candidate := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXSPOT", Side: "NO",
		Family: "spotlag", Source: "auto-cons-spotlag", SignalContractID: "spotlag-contract"}
	if !s.claimLiveMirrorIntent(context.Background(), candidate) ||
		s.claimLiveMirrorIntent(context.Background(), candidate) {
		t.Fatal("claim did not latch before a release")
	}
	if !s.releaseLiveMirrorPreSubmitClaim(context.Background(), candidate) {
		t.Fatal("proved pre-submit claim release failed")
	}
	if !s.claimLiveMirrorIntent(context.Background(), candidate) {
		t.Fatal("fresh signal remained suppressed after proved no-submit release")
	}
}
