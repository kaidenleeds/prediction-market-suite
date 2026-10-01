package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// marshalForTest mirrors writeConfigAtomic's marshal (json.MarshalIndent, no BOM).
func marshalForTest(cfg Config) ([]byte, error) { return json.MarshalIndent(cfg, "", "  ") }

// clearEnv keeps the suite's env overrides out of these tests (they'd silently rewrite the
// loaded values on a machine that has them exported).
func clearEnv(t *testing.T) {
	t.Helper()
	t.Setenv("KALSHI_SUITE_ENV", "")
	t.Setenv("KALSHI_SUITE_ADDR", "")
	t.Setenv("KALSHI_SUITE_DATA_DIR", "")
}

func writeTmp(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// R79 boot robustness (a): a UTF-8 BOM in config.json (Notepad "UTF-8 with BOM", PowerShell
// redirects) fail-stopped the suite invisibly. The loader must strip it and parse normally.
func TestLoadStripsUTF8BOM(t *testing.T) {
	clearEnv(t)
	p := writeTmp(t, "bom.json", "\xef\xbb\xbf{\"environment\":\"prod\",\"server_addr\":\"1.2.3.4:9\"}")
	cfg, _, unknown, err := LoadWithReport(p)
	if err != nil {
		t.Fatalf("BOM-prefixed config must parse: %v", err)
	}
	if cfg.Environment != EnvProd || cfg.ServerAddr != "1.2.3.4:9" {
		t.Fatalf("BOM config not applied: env=%q addr=%q", cfg.Environment, cfg.ServerAddr)
	}
	if len(unknown) != 0 {
		t.Fatalf("no unknown keys expected, got %v", unknown)
	}
}

// R79 boot robustness (b): unknown keys are a WARN + continue (the boot defaults-diff already
// surfaces the real key), no longer the R76 fail-stop. Known keys in the same file still apply.
func TestLoadUnknownKeysWarnNotFail(t *testing.T) {
	clearEnv(t)
	p := writeTmp(t, "unk.json",
		`{"environment":"prod","kellyy_frac":1,"auto":{"stake_usd":42,"not_a_knob":true},"bogus":{"nested":1}}`)
	cfg, defaulted, unknown, err := LoadWithReport(p)
	if err != nil {
		t.Fatalf("unknown keys must not fail the boot: %v", err)
	}
	if cfg.Auto.StakeUSD != 42 {
		t.Fatalf("known keys must still apply on the lenient pass: stake_usd=%v", cfg.Auto.StakeUSD)
	}
	got := strings.Join(unknown, " ")
	for _, want := range []string{"kellyy_frac", "auto.not_a_knob", "bogus"} {
		if !strings.Contains(got, want) {
			t.Fatalf("unknown keys must name %q, got %v", want, unknown)
		}
	}
	if len(defaulted) == 0 { // kelly_frac (the real key) must show as running on its default
		t.Fatal("defaulted-keys report must still be produced alongside unknown keys")
	}
	if !strings.Contains(strings.Join(defaulted, " "), "auto.kelly_frac") {
		t.Fatalf("the typo'd knob's REAL key must appear in the defaults list: %v", defaulted)
	}
}

// R79 boot robustness (b, contra): truly invalid JSON must still fail the boot.
func TestLoadInvalidJSONStillFails(t *testing.T) {
	clearEnv(t)
	if _, _, _, err := LoadWithReport(writeTmp(t, "bad.json", `{"environment":"prod",`)); err == nil {
		t.Fatal("truncated JSON must still fail the boot")
	}
	// Wrong TYPE on a known key is invalid config, not an unknown key — must also still fail.
	if _, _, _, err := LoadWithReport(writeTmp(t, "typ.json", `{"auto":{"stake_usd":"ten"}}`)); err == nil {
		t.Fatal("type-mismatched JSON must still fail the boot")
	}
}

// A BOM plus unknown keys together (the belt-and-suspenders case) must also boot.
func TestLoadBOMPlusUnknownKeys(t *testing.T) {
	clearEnv(t)
	p := writeTmp(t, "both.json", "\xef\xbb\xbf{\"environment\":\"demo\",\"zzz_unknown\":true}")
	cfg, _, unknown, err := LoadWithReport(p)
	if err != nil {
		t.Fatalf("BOM+unknown must still boot: %v", err)
	}
	if cfg.Environment != EnvDemo || len(unknown) != 1 || unknown[0] != "zzz_unknown" {
		t.Fatalf("got env=%q unknown=%v", cfg.Environment, unknown)
	}
}

// R79 (d): writeConfigAtomic (server) writes json.MarshalIndent bytes — this pins the loader
// side of that contract: a round-trip of Default() marshals BOM-less and reloads cleanly, and
// every leaf the schema knows is accepted (no unknown keys against our own marshal).
func TestOwnMarshalHasNoBOMAndNoUnknownKeys(t *testing.T) {
	clearEnv(t)
	cfg := Default()
	cfg.Environment = EnvProd
	b, err := marshalForTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) >= 3 && b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF {
		t.Fatal("our own config marshal must be BOM-less")
	}
	p := writeTmp(t, "own.json", string(b))
	_, _, unknown, err := LoadWithReport(p)
	if err != nil {
		t.Fatalf("our own marshal must reload: %v", err)
	}
	if len(unknown) != 0 {
		t.Fatalf("our own marshal must have zero unknown keys, got %v", unknown)
	}
}
