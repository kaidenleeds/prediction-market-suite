//go:build windows

package main

import (
	"os/exec"
	"testing"
)

func TestConfigureHeadlessChildPreventsConsoleAllocation(t *testing.T) {
	cmd := exec.Command("cmd.exe", "/c", "exit", "0")
	configureHeadlessChild(cmd)
	if cmd.SysProcAttr == nil {
		t.Fatal("SysProcAttr is nil; a detached helper may allocate a console window")
	}
	if !cmd.SysProcAttr.HideWindow {
		t.Fatal("HideWindow is false")
	}
	if got := cmd.SysProcAttr.CreationFlags & createNoWindow; got == 0 {
		t.Fatalf("CreationFlags=%#x does not include CREATE_NO_WINDOW", cmd.SysProcAttr.CreationFlags)
	}
}
