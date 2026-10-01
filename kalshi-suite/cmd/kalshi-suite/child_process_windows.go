//go:build windows

package main

import (
	"os/exec"
	"syscall"

	"golang.org/x/sys/windows"
)

const createNoWindow = windows.CREATE_NO_WINDOW

// configureHeadlessChild prevents a console-subsystem helper from asking Windows Terminal to
// allocate a new window when the suite itself was started without a console. Redirected stdout
// and stderr still work, so ML failures remain available in the suite logs.
func configureHeadlessChild(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		HideWindow:    true,
		CreationFlags: createNoWindow,
	}
}
