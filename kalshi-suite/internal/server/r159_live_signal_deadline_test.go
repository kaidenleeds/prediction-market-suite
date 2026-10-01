package server

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestR159SelfPOSTContextPreservesCallerCancellation(t *testing.T) {
	type contextKey string
	const markerKey contextKey = "r159-self-post-marker"

	parent, cancel := context.WithCancel(
		context.WithValue(context.Background(), markerKey, "kept"),
	)
	cancel()

	s := &Server{}
	code, out := s.selfPOSTContext(parent, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusConflict, map[string]any{
			"canceled": r.Context().Err() == context.Canceled,
			"marker":   r.Context().Value(markerKey),
		})
	}, "/internal/r159/context", map[string]any{"ok": true})

	if code != http.StatusConflict {
		t.Fatalf("status=%d, want %d", code, http.StatusConflict)
	}
	if canceled, _ := out["canceled"].(bool); !canceled {
		t.Fatalf("handler context did not preserve caller cancellation: %#v", out)
	}
	if marker, _ := out["marker"].(string); marker != "kept" {
		t.Fatalf("handler context lost caller values: %#v", out)
	}
}

func TestR159LiveSubmitBoundaryKeepsOriginalTwentyFiveSecondLease(t *testing.T) {
	signalAt := time.UnixMilli(1_800_000_000_000)
	triggerUnixMS := signalAt.UnixMilli()

	if _, why := r159LiveAutoSignalAt(
		true, triggerUnixMS, signalAt.Add(liveMirrorTTL-time.Millisecond)); why != "" {
		t.Fatalf("initially valid signal refused before TTL: %q", why)
	}
	expiredAt := signalAt.Add(liveMirrorTTL + time.Millisecond)
	if why := r159LiveSubmitBoundaryReason(
		context.Background(), true, triggerUnixMS, expiredAt); !strings.Contains(why, "aged past") {
		t.Fatalf("handler work renewed the original signal lease: %q", why)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if why := r159LiveSubmitBoundaryReason(
		canceled, true, triggerUnixMS, signalAt); !strings.Contains(why, "canceled") {
		t.Fatalf("canceled urgent dispatch could still reach a venue boundary: %q", why)
	}
}

func TestR159SignalDeadlineNoSendReleasesBothVenueClaims(t *testing.T) {
	noSend := map[string]any{
		"venue_attempted":     false,
		"execution_source":    "wire-signal-deadline",
		"risk_reservation_id": "risk-r159",
	}
	for _, venue := range []string{"kalshi", "polyus"} {
		if !liveMirrorHandlerProvesNoVenueCall(venue, http.StatusConflict, noSend) {
			t.Fatalf("%s explicit deadline no-send would leave its dispatch claim latched", venue)
		}
	}
}
