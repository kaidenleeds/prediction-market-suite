//go:build windows

package main

import (
	"fmt"
	"path/filepath"

	"golang.org/x/sys/windows"
)

func driveSnapshotAvailableBytes(path string) (uint64, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return 0, fmt.Errorf("resolve snapshot destination: %w", err)
	}
	directory, err := windows.UTF16PtrFromString(absolute)
	if err != nil {
		return 0, fmt.Errorf("encode snapshot destination: %w", err)
	}
	var available uint64
	if err := windows.GetDiskFreeSpaceEx(directory, &available, nil, nil); err != nil {
		return 0, fmt.Errorf("query snapshot destination free space: %w", err)
	}
	return available, nil
}
