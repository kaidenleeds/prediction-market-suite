package server

// R87 tests — the "polyus_auth" readiness component (file-based credentials round). Contract
// mirrors kalshi_auth (r86_test.go): green when the Ed25519 client built (detail names the
// source), RED + ready=false when credentials are configured but unusable, grey/neutral "none"
// (ok stays true) when nothing is configured.

import (
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func TestReadyPolyUSAuthComponent(t *testing.T) {
	s := testServer(t)
	s.kal = kalshi.NewClient(kalshi.BaseDemo, nil, 1, 500*time.Millisecond) // handleReady reads TickerCount/BookStats

	// Nothing set — grey "none", ok=true: a public-data-only PolyUS is a valid state and must not
	// hold overall readiness red by itself.
	comp, _ := readyPayload(t, s)
	c := comp["polyus_auth"]
	if c == nil {
		t.Fatal("polyus_auth component missing from /api/ready")
	}
	if c["state"] != "none" || c["ok"] != true {
		t.Fatalf("default polyus_auth = %v, want state=none ok=true", c)
	}

	// Configured but broken (e.g. secret file present without POLY_US_KEY_ID): RED with the exact
	// fix text, ready goes false.
	fix := "polyus secret file found but POLY_US_KEY_ID not set (env)"
	s.SetPolyUSAuthState("error", fix)
	comp, ready := readyPayload(t, s)
	c = comp["polyus_auth"]
	if c["state"] != "error" || c["ok"] != false || c["detail"] != fix {
		t.Fatalf("error polyus_auth = %v, want state=error ok=false + fix text", c)
	}
	if ready {
		t.Fatal("polyus_auth error must hold overall ready=false")
	}

	// Client built: green, detail names the source.
	s.SetPolyUSAuthState("ok", "Ed25519 client ready (key k-1, source file)")
	comp, _ = readyPayload(t, s)
	c = comp["polyus_auth"]
	if c["state"] != "ok" || c["ok"] != true || !strings.Contains(c["detail"].(string), "source file") {
		t.Fatalf("loaded polyus_auth = %v, want state=ok ok=true + source in detail", c)
	}
}
