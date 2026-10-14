// Copyright 2026 The raftseg authors.

// Package vfs defines the filesystem interface of raftseg's storage
// packages, and implements it with the operating system's filesystem.
package vfs

import (
	"io"
	"os"
	"path/filepath"
	"sort"
)

// FS is a filesystem, as the storage packages use one. OS is the operating
// system's; tests substitute filesystems that simulate crashes and errors
// (internal/crashfs).
type FS interface {
	// Create creates or truncates a file for reading and writing.
	Create(name string) (File, error)
	// Open opens an existing file for reading.
	Open(name string) (File, error)
	// OpenReadWrite opens an existing file for reading and writing.
	OpenReadWrite(name string) (File, error)
	// OpenDir opens a directory so that it can be synced.
	OpenDir(name string) (File, error)
	Remove(name string) error
	Rename(oldname, newname string) error
	MkdirAll(dir string, perm os.FileMode) error
	// List returns the names of the entries in dir.
	List(dir string) ([]string, error)
	Stat(name string) (os.FileInfo, error)
	// Lock takes an exclusive, advisory lock on name (creating it), held
	// until the returned Closer is closed. It fails if another process holds
	// the lock.
	Lock(name string) (io.Closer, error)
	PathJoin(elem ...string) string
	PathDir(path string) string
}

// File is an open file.
type File interface {
	io.Closer
	io.ReaderAt
	io.WriterAt
	// Preallocate reserves space without changing the file's size (best
	// effort).
	Preallocate(offset, length int64) error
	Stat() (os.FileInfo, error)
	// Sync makes the file's data and metadata durable.
	Sync() error
	// SyncData makes the file's data durable, and its metadata only as far
	// as needed to read the data back (fdatasync).
	SyncData() error
}

// OS is the operating system's filesystem.
var OS FS = osFS{}

type osFS struct{}

func (osFS) Create(name string) (File, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (osFS) Open(name string) (File, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (osFS) OpenReadWrite(name string) (File, error) {
	f, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (osFS) OpenDir(name string) (File, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (osFS) Remove(name string) error                    { return os.Remove(name) }
func (osFS) Rename(oldname, newname string) error        { return os.Rename(oldname, newname) }
func (osFS) MkdirAll(dir string, perm os.FileMode) error { return os.MkdirAll(dir, perm) }
func (osFS) Stat(name string) (os.FileInfo, error)       { return os.Stat(name) }
func (osFS) PathJoin(elem ...string) string              { return filepath.Join(elem...) }
func (osFS) PathDir(path string) string                  { return filepath.Dir(path) }

func (osFS) List(dir string) ([]string, error) {
	d, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	sort.Strings(names)
	return names, err
}

func (osFS) Lock(name string) (io.Closer, error) { return lockFile(name) }

type osFile struct{ *os.File }

func (f osFile) Stat() (os.FileInfo, error)             { return f.File.Stat() }
func (f osFile) Preallocate(offset, length int64) error { return preallocate(f.File, offset, length) }
func (f osFile) SyncData() error                        { return syncData(f.File) }
