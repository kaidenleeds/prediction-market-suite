//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package main

import (
	"fmt"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func driveSnapshotAvailableBytes(path string) (uint64, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return 0, fmt.Errorf("resolve snapshot destination: %w", err)
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(absolute, &stat); err != nil {
		return 0, fmt.Errorf("query snapshot destination free space: %w", err)
	}
	return uint64(stat.Bavail) * uint64(stat.Bsize), nil
}
