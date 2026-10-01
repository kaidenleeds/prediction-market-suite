package main

// R88 tests — polyus_key_id config field: polyUSLoad must take the key id from CONFIG first
// (Settings-persistable, immune to stale parent process envs — the 2026-07-05 polyus-RED boot)
// and only fall back to the POLY_US_KEY_ID env var. Secrets are throwaways in temp dirs, same
// harness rules as r87_keyfile_test.go (the operator's real key files are never touched).

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
)

func TestPolyUSLoadConfigKeyIDWinsOverEnv(t *testing.T) {
	t.Setenv("POLY_US_KEY_ID", "env-pus-id") // both set — config must win
	t.Setenv("POLY_US_SECRET", "")
	t.Setenv("POLY_US_SECRET_FILE", "")
	cfg := baseCfg()
	cfg.PolyUSKeyFile = writeKeyFile(t, []byte(throwawaySecret(t)))
	cfg.PolyUSKeyID = " cfg-pus-id \n" // whitespace-tolerant, like kalshi_key_id

	c, src, err := polyUSLoad(cfg)
	if err != nil || c == nil {
		t.Fatalf("polyUSLoad(config key id) = %v, %v", c, err)
	}
	if src != "file" || c.KeyID() != "cfg-pus-id" {
		t.Fatalf("got source=%q key_id=%q, want file/cfg-pus-id (config wins over env)", src, c.KeyID())
	}
}

func TestPolyUSLoadConfigKeyIDWithoutEnv(t *testing.T) {
	// THE 2026-07-05 failure shape: User-scope env existed but the process env didn't carry it.
	// With polyus_key_id in config the boot must succeed with an empty process env.
	t.Setenv("POLY_US_KEY_ID", "")
	t.Setenv("POLY_US_SECRET", "")
	t.Setenv("POLY_US_SECRET_FILE", "")
	cfg := baseCfg()
	cfg.PolyUSKeyFile = writeKeyFile(t, []byte(throwawaySecret(t)))
	cfg.PolyUSKeyID = "cfg-pus-id"

	c, src, err := polyUSLoad(cfg)
	if err != nil || c == nil {
		t.Fatalf("polyUSLoad(config key id, env empty) = %v, %v", c, err)
	}
	if src != "file" || c.KeyID() != "cfg-pus-id" {
		t.Fatalf("got source=%q key_id=%q, want file/cfg-pus-id", src, c.KeyID())
	}
}

func TestPolyUSLoadConfigKeyIDNamesEnvSecret(t *testing.T) {
	// No secret FILE — the env-secret path must also honor the config key id (source stays "env").
	t.Setenv("POLY_US_KEY_ID", "")
	t.Setenv("POLY_US_SECRET", throwawaySecret(t))
	t.Setenv("POLY_US_SECRET_FILE", "")
	cfg := baseCfg()
	cfg.PolyUSKeyFile = filepath.Join(t.TempDir(), "does-not-exist.txt")
	cfg.PolyUSKeyID = "cfg-pus-id"

	c, src, err := polyUSLoad(cfg)
	if err != nil || c == nil || src != "env" {
		t.Fatalf("got client=%v src=%q err=%v, want client/env/nil", c, src, err)
	}
	if c.KeyID() != "cfg-pus-id" {
		t.Fatalf("key_id = %q, want cfg-pus-id", c.KeyID())
	}
}

func TestPolyUSLoadStillRedWithNoKeyIDAnywhere(t *testing.T) {
	t.Setenv("POLY_US_KEY_ID", "")
	t.Setenv("POLY_US_SECRET", "")
	t.Setenv("POLY_US_SECRET_FILE", "")
	cfg := baseCfg()
	cfg.PolyUSKeyFile = writeKeyFile(t, []byte(throwawaySecret(t)))
	cfg.PolyUSKeyID = "   " // whitespace-only must count as unset

	c, src, err := polyUSLoad(cfg)
	if !errors.Is(err, errPolyUSNoKeyID) {
		t.Fatalf("want errPolyUSNoKeyID, got client=%v src=%q err=%v", c, src, err)
	}
	if src != "file" {
		t.Fatalf("source = %q, want file", src)
	}
}

func TestConfigPolyUSKeyIDRoundTrip(t *testing.T) {
	// polyus_key_id must decode from config.json as a KNOWN key (no unknown-key warn) and land
	// in cfg.PolyUSKeyID. Mirrors how the live config.json carries it after R88.
	p := filepath.Join(t.TempDir(), "config.json")
	body := `{"environment":"demo","polyus_key_file":"C:\\nowhere\\poly.txt","polyus_key_id":"round-trip-id","kalshi_key_id":"k-id"}`
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, _, unknown, err := config.LoadWithReport(p)
	if err != nil {
		t.Fatalf("LoadWithReport: %v", err)
	}
	if len(unknown) != 0 {
		t.Fatalf("unknown keys reported for R88 fields: %v", unknown)
	}
	if cfg.PolyUSKeyID != "round-trip-id" || cfg.KalshiKeyID != "k-id" {
		t.Fatalf("round trip: polyus_key_id=%q kalshi_key_id=%q", cfg.PolyUSKeyID, cfg.KalshiKeyID)
	}
}
