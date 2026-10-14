// Copyright 2026 The raftseg authors.

//go:build !linux

package vfs

import "os"

func preallocate(f *os.File, offset, length int64) error { return nil }

// syncData falls back to a full sync where fdatasync is not available.
func syncData(f *os.File) error { return f.Sync() }
