//go:build !windows

package main

import "log/slog"

// killOnCloseJob is a no-op on non-Windows. On those platforms the children are taken down by the
// explicit Process.Kill calls in main on graceful shutdown.
type killOnCloseJob struct{}

func newKillOnCloseJob(*slog.Logger) *killOnCloseJob { return nil }

func (j *killOnCloseJob) assign(int, string, *slog.Logger) {}
