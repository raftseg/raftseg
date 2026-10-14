// Copyright 2026 The raftseg authors.

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package vfs

import (
	"fmt"
	"io"
	"os"
	"syscall"
)

type fileLock struct{ f *os.File }

func (l fileLock) Close() error { return l.f.Close() } // closing releases the flock

func lockFile(name string) (io.Closer, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("vfs: lock %s: %w (is another process using this log?)", name, err)
	}
	return fileLock{f}, nil
}
