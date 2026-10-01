//go:build windows

package server

import (
	"context"
	"os/exec"
	"syscall"

	"golang.org/x/sys/windows"
)

// CREATE_NO_WINDOW is explicit: this diagnostic must never flash a console on the operator's PC.
func defaultWLANCommand(ctx context.Context) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "netsh", "wlan", "show", "interfaces")
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: windows.CREATE_NO_WINDOW}
	return cmd.CombinedOutput()
}
