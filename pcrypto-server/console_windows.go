//go:build windows

package main

import (
	"os"
	"syscall"
	"unsafe"
)

// disableQuickEdit turns off the Windows console "QuickEdit Mode" for this process's input handle.
// With QuickEdit ON (the Windows default), the moment you CLICK or select text inside the cmd window
// the program is PAUSED until you press Enter/Esc — which looks exactly like the app "freezing" the
// instant the window is touched/unfocused. Clearing the flag (and setting EXTENDED_FLAGS, which is
// required for the change to take effect) lets the program keep running no matter what you do with
// the window. No-op when stdin isn't a real console (piped / launched without a window).
func disableQuickEdit() {
	const (
		enableExtendedFlags = 0x0080
		enableQuickEdit     = 0x0040
	)
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	getConsoleMode := kernel32.NewProc("GetConsoleMode")
	setConsoleMode := kernel32.NewProc("SetConsoleMode")
	h := syscall.Handle(os.Stdin.Fd())
	var mode uint32
	if r, _, _ := getConsoleMode.Call(uintptr(h), uintptr(unsafe.Pointer(&mode))); r == 0 {
		return // stdin is not a console → nothing to disable
	}
	mode = (mode &^ enableQuickEdit) | enableExtendedFlags
	_, _, _ = setConsoleMode.Call(uintptr(h), uintptr(mode))
}
