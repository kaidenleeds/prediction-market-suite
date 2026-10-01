//go:build !windows

package main

import "errors"

// appWindowFullscreen — R67c: native window toggle exists only on Windows (user32); the Linux
// service builds have no app window to control, so /api/fullscreen reports unavailable.
func appWindowFullscreen() (bool, error) {
	return false, errors.New("native fullscreen toggle is Windows-only")
}
