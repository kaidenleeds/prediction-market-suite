package main

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeBootReceiptRecordsSpecificOrderlyReason(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	path, rec := beginRuntimeBoot(dir, true, log)
	open, ok := readRuntimeBoot(path)
	if !ok || open.BootID == "" || open.EndedAt != "" || open.Exit != "" {
		t.Fatalf("open receipt = %+v ok=%v", open, ok)
	}

	endRuntimeBoot(path, rec, "shutdown_signal", log)
	closed, ok := readRuntimeBoot(path)
	if !ok {
		t.Fatal("closed receipt was not readable")
	}
	if closed.EndedAt == "" || closed.Exit != "shutdown_signal" {
		t.Fatalf("closed receipt = %+v, want shutdown_signal with ended_at", closed)
	}
}

func TestRuntimeBootReceiptReportsUnknownIncompleteExitWithoutAttributingActor(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runtime_boot.json")
	prev := runtimeBootRecord{BootID: "old-boot", StartedAt: "2026-07-11T10:00:00Z", PID: 123, Build: "test"}
	if err := writeRuntimeBoot(path, prev); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	_, _ = beginRuntimeBoot(dir, false, log)
	msg := strings.ToLower(buf.String())
	if !strings.Contains(msg, "cause is unknown") {
		t.Fatalf("warning did not preserve uncertainty: %s", msg)
	}
	for _, falseAttribution := range []string{"operator closed", "user closed", "model closed"} {
		if strings.Contains(msg, falseAttribution) {
			t.Fatalf("warning falsely attributed actor %q: %s", falseAttribution, msg)
		}
	}
	history, err := os.ReadFile(filepath.Join(dir, "runtime_boot_history.jsonl"))
	if err != nil {
		t.Fatalf("read persistent history: %v", err)
	}
	if !strings.Contains(string(history), `"state":"incomplete_exit_detected"`) || !strings.Contains(string(history), `"boot_id":"old-boot"`) {
		t.Fatalf("incomplete receipt was not preserved before current receipt replaced it: %s", history)
	}
}
