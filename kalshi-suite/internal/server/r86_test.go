package server

// R86 tests — the "kalshi_auth" readiness component (operator: a PC crash killed the shell that
// exported KALSHI_SUITE_PASSPHRASE and the suite rebooted public-only with only a quiet warn).
// Contract: green when the signer loaded; RED + the exact fix text (and overall ready=false) when
// credentials are stored but locked; grey/neutral ("none", ok stays true) when nothing is stored.

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

// readyPayload runs handleReady against the hermetic server and decodes the component map.
func readyPayload(t *testing.T, s *Server) (map[string]map[string]any, bool) {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleReady(rec, httptest.NewRequest("GET", "/api/ready", nil))
	var out struct {
		Ready      bool                      `json:"ready"`
		Components map[string]map[string]any `json:"components"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("ready JSON: %v (body %q)", err, rec.Body.String())
	}
	return out.Components, out.Ready
}

func TestReadyKalshiAuthComponent(t *testing.T) {
	s := testServer(t)
	s.kal = kalshi.NewClient(kalshi.BaseDemo, nil, 1, 500*time.Millisecond) // handleReady reads TickerCount/BookStats

	// Nothing set (defensive default) — grey "none", ok=true: an unauthenticated suite is a valid
	// state and must NOT hold overall readiness red by itself.
	comp, _ := readyPayload(t, s)
	c := comp["kalshi_auth"]
	if c == nil {
		t.Fatal("kalshi_auth component missing from /api/ready")
	}
	if c["state"] != "none" || c["ok"] != true || c["detail"] != "no credentials stored" {
		t.Fatalf("default kalshi_auth = %v, want state=none ok=true detail=%q", c, "no credentials stored")
	}

	// Locked (stored creds + missing passphrase): RED with the exact fix text, ready goes false.
	fix := "credentials stored but locked - set KALSHI_SUITE_PASSPHRASE in the launching shell (or User env) and restart"
	s.SetKalshiAuth("locked", fix)
	comp, ready := readyPayload(t, s)
	c = comp["kalshi_auth"]
	if c["state"] != "locked" || c["ok"] != false || c["detail"] != fix {
		t.Fatalf("locked kalshi_auth = %v, want state=locked ok=false + fix text", c)
	}
	if ready {
		t.Fatal("locked kalshi_auth must hold overall ready=false")
	}

	// Signer loaded: green.
	s.SetKalshiAuth("ok", "signer loaded (key k-1, env demo)")
	comp, _ = readyPayload(t, s)
	c = comp["kalshi_auth"]
	if c["state"] != "ok" || c["ok"] != true {
		t.Fatalf("loaded kalshi_auth = %v, want state=ok ok=true", c)
	}
}
