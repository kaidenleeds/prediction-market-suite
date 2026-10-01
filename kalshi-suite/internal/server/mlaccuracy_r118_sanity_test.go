package server

// mlaccuracy_r118_sanity_test.go — opt-in (env ML_ACC_SANITY=1) sanity harness: runs the FULL
// R118 RefreshMLAccuracy against the real data dir (books + sqlite universe) and logs the
// resulting payload so the computed headline numbers can be eyeballed against recon. Skipped in
// normal `go test ./...` runs; NEVER run it while the suite process holds the DB.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR118MLAccuracySanityRealData(t *testing.T) {
	if os.Getenv("ML_ACC_SANITY") == "" {
		t.Skip("set ML_ACC_SANITY=1 (suite STOPPED) to recompute ml_accuracy.json from the real data dir")
	}
	dir := os.Getenv("ML_ACC_DATA")
	if dir == "" {
		dir = filepath.Join("..", "..", "data")
	}
	st, err := storage.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	cfg := config.Default()
	cfg.DataDir = dir
	s := &Server{store: st, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s.cfgP.Store(&cfg)
	if err := s.RefreshMLAccuracy(context.Background(), true); err != nil {
		t.Fatalf("RefreshMLAccuracy: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "ml_accuracy.json"))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	t.Logf("ml_accuracy.json (%d bytes):\n%s", len(b), b)
}
