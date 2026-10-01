package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestR133PolyUSBookWSCapDefaultAndConfigDecode(t *testing.T) {
	if got := Default().PolyUSBookWSCap; got != 600 {
		t.Fatalf("PolyUSBookWSCap default=%d, want conservative 600", got)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"polyus_book_ws_cap":1000}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, defaulted, unknown, err := LoadWithReport(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PolyUSBookWSCap != 1000 {
		t.Fatalf("decoded PolyUSBookWSCap=%d, want 1000", cfg.PolyUSBookWSCap)
	}
	if len(unknown) != 0 {
		t.Fatalf("polyus_book_ws_cap was treated as unknown: %v", unknown)
	}
	for _, key := range defaulted {
		if key == "polyus_book_ws_cap" {
			t.Fatalf("explicit polyus_book_ws_cap was reported defaulted: %v", defaulted)
		}
	}
}
