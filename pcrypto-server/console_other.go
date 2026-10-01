//go:build !windows

package main

// disableQuickEdit is a no-op off Windows (QuickEdit is a Windows-console-only feature).
func disableQuickEdit() {}
