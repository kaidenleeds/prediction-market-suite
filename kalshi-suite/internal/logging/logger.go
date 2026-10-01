// Package logging provides a small structured-logging helper over log/slog.
package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

func levelOf(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// New returns a text structured logger at the given level (debug|info|warn|error) writing to stdout.
func New(level string) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: levelOf(level)}))
}

// NewWithFile is like New but ALSO appends every log line to `path`, so a long-running server's
// activity is captured on disk even when the console window is closed, minimized, or unfocused.
// Falls back to stdout-only if the file can't be opened. The file handle stays open for the life of
// the process (the OS reclaims it on exit).
func NewWithFile(level, path string) *slog.Logger {
	var w io.Writer = os.Stdout
	if path != "" {
		if f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
			// The disk writer must run first. A direct detached Windows launch has no valid
			// stdout handle; console-first made MultiWriter stop before persisting anything.
			w = fileFirstTee(f, os.Stdout)
		}
	}
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: levelOf(level)}))
}

func fileFirstTee(file, console io.Writer) io.Writer {
	return io.MultiWriter(file, console)
}
