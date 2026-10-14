// Copyright 2026 The raftseg authors.

//go:build linux

package vfs

import (
	"os"
	"syscall"
)

// fallocKeepSize is FALLOC_FL_KEEP_SIZE from linux/falloc.h, which the
// syscall package does not export.
const fallocKeepSize = 0x1

func preallocate(f *os.File, offset, length int64) error {
	// Reserve blocks without changing the size, like Pebble does.
	err := syscall.Fallocate(int(f.Fd()), fallocKeepSize, offset, length)
	if err == syscall.EOPNOTSUPP || err == syscall.ENOSYS {
		return nil
	}
	return err
}

func syncData(f *os.File) error {
	for {
		err := syscall.Fdatasync(int(f.Fd()))
		if err != syscall.EINTR {
			return err
		}
	}
}
