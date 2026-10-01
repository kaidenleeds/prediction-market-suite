package logging

import (
	"bytes"
	"errors"
	"testing"
)

type failedConsole struct{}

func (failedConsole) Write([]byte) (int, error) { return 0, errors.New("no console") }

func TestFileFirstTeePersistsBeforeDetachedConsoleError(t *testing.T) {
	var file bytes.Buffer
	w := fileFirstTee(&file, failedConsole{})
	if _, err := w.Write([]byte("durable\n")); err == nil {
		t.Fatal("expected the simulated detached console error")
	}
	if got := file.String(); got != "durable\n" {
		t.Fatalf("disk writer got %q, want durable log line", got)
	}
}
