//go:build !windows

package main

import "os/exec"

func configureHeadlessChild(_ *exec.Cmd) {}
