// Copyright 2026 The raftseg authors.

//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package vfs

import (
	"io"
	"os"
)

// lockFile without flock: the file is created but not locked, so two
// processes are not kept from opening the same log.
func lockFile(name string) (io.Closer, error) {
	return os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o644)
}
