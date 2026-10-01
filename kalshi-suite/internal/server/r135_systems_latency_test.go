package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
)

func TestLeaderboardVerdictSnapshotServesExpiredMemoryWithoutRecompute(t *testing.T) {
	old := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	s := &Server{
		verdCache: []verdictEnt{{Family: "cached-system", N: 21, State: "PROVEN+"}},
		verdAt:    old,
	}
	rows, source, asOf := s.leaderboardVerdictSnapshot()
	if source != "memory" || asOf != old.Format(time.RFC3339) {
		t.Fatalf("snapshot receipt = source %q as_of %q", source, asOf)
	}
	if len(rows) != 1 || rows[0].Family != "cached-system" {
		t.Fatalf("snapshot rows = %+v", rows)
	}
	// Callers receive an immutable copy, not the shared verdict cache.
	rows[0].Family = "mutated"
	if s.verdCache[0].Family != "cached-system" {
		t.Fatal("leaderboard snapshot exposed mutable verdict cache")
	}
}

func TestLeaderboardVerdictSnapshotFallsBackToPersistedSweep(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Config{DataDir: dir}
	s := &Server{}
	s.cfgP.Store(&cfg)
	wantTS := "2026-07-11T19:07:35Z"
	b, err := json.Marshal(map[string]any{
		"ts":          wantTS,
		"experiments": []verdictEnt{{Family: "disk-system", N: 30, State: "COLLECTING"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "verdicts.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}

	rows, source, asOf := s.leaderboardVerdictSnapshot()
	if source != "persisted" || asOf != wantTS {
		t.Fatalf("snapshot receipt = source %q as_of %q", source, asOf)
	}
	if len(rows) != 1 || rows[0].Family != "disk-system" || rows[0].N != 30 {
		t.Fatalf("persisted snapshot rows = %+v", rows)
	}
}
