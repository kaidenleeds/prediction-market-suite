package main

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
)

func TestR135HeadlessPreservesPersistedPaperAuto(t *testing.T) {
	for _, tc := range []struct {
		enabled bool
		mode    string
	}{
		{enabled: false, mode: "observe"},
		{enabled: true, mode: "autobet"},
	} {
		cfg := config.Default()
		cfg.OpenBrowser = true
		cfg.Auto.Enabled = tc.enabled
		cfg.Auto.Mode = tc.mode
		applyLaunchFlags(&cfg, true)
		if cfg.OpenBrowser {
			t.Fatal("headless launch did not suppress browser")
		}
		if cfg.Auto.Enabled != tc.enabled || cfg.Auto.Mode != tc.mode {
			t.Fatalf("headless changed persisted PAPER AUTO from %v/%q to %v/%q", tc.enabled, tc.mode, cfg.Auto.Enabled, cfg.Auto.Mode)
		}
	}
}

func TestMLChildOutputUsesFilesOnWindows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows detached-child contract")
	}
	cmd := exec.Command("python", "-V")
	files := configureMLChildOutput(cmd, t.TempDir())
	defer closeChildLogs(files)
	if len(files) != 2 {
		t.Fatalf("opened %d child log files, want stdout+stderr", len(files))
	}
	if cmd.Stdout != files[0] || cmd.Stderr != files[1] {
		t.Fatal("ML child streams are not wired directly to durable files")
	}
}

func TestR135MLSidecarReceivesExactDataAndConfigPaths(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.DataDir = filepath.Join(root, "alternate-data")
	cfgPath := filepath.Join(root, "alternate-config.json")
	args := mlSidecarArgs(cfg, cfgPath)

	valueAfter := func(flag string) string {
		t.Helper()
		for i := 0; i+1 < len(args); i++ {
			if args[i] == flag {
				return args[i+1]
			}
		}
		t.Fatalf("missing sidecar argument %s in %v", flag, args)
		return ""
	}
	if got, want := valueAfter("--db"), filepath.Join(cfg.DataDir, "kalshi.db"); got != want || !filepath.IsAbs(got) {
		t.Fatalf("--db = %q, want absolute %q", got, want)
	}
	if got := valueAfter("--config"); got != cfgPath || !filepath.IsAbs(got) {
		t.Fatalf("--config = %q, want absolute %q", got, cfgPath)
	}
}
