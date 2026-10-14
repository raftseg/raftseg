// Copyright 2026 The raftseg authors.

package crashfs

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/raftseg/raftseg/vfs"
)

// ErrInjected is the error tests inject.
var ErrInjected = errors.New("crashfs: injected error")

// ErrSyncLost, returned by a hook for a file Sync or SyncData of a crashfs
// file, fails the sync and loses the file's unsynced writes for good, as a
// writeback error does on Linux: they stay readable, but a later sync does
// not make them durable. For anything else it is a plain injected error.
var ErrSyncLost = fmt.Errorf("%w: sync failed, unsynced writes lost", ErrInjected)

// OpKind is a kind of filesystem operation.
type OpKind int

const (
	OpCreate OpKind = iota
	OpOpen
	OpOpenReadWrite
	OpOpenDir
	OpRemove
	OpRename
	OpMkdirAll
	OpList
	OpStat
	OpLock
	OpClose
	OpReadAt
	OpWriteAt
	OpPreallocate
	OpFileStat
	OpSync
	OpSyncData
)

var opNames = [...]string{"create", "open", "open-rw", "open-dir", "remove", "rename", "mkdir-all", "list",
	"stat", "lock", "close", "read-at", "write-at", "preallocate", "file-stat", "sync", "sync-data"}

func (k OpKind) String() string { return opNames[k] }

// IsWrite reports whether the operation changes the filesystem.
func (k OpKind) IsWrite() bool {
	switch k {
	case OpCreate, OpRemove, OpRename, OpMkdirAll, OpWriteAt, OpPreallocate, OpSync, OpSyncData:
		return true
	}
	return false
}

// Op is an operation about to be performed.
type Op struct {
	Kind OpKind
	Path string // for file operations, the path the file was opened with
	Off  int64  // ReadAt, WriteAt, Preallocate
	Len  int    // ReadAt, WriteAt
	Data []byte // WriteAt: the data to be written (read only)
}

// WithHook returns fs with hook called before every operation; an error
// from hook fails the operation, which then has no effect.
func WithHook(fs vfs.FS, hook func(Op) error) vfs.FS { return hooked{fs, hook} }

type hooked struct {
	fs   vfs.FS
	hook func(Op) error
}

func (h hooked) file(f vfs.File, err error, path string) (vfs.File, error) {
	if err != nil {
		return nil, err
	}
	return &hookedFile{f, h.hook, path}, nil
}

func (h hooked) Create(name string) (vfs.File, error) {
	if err := h.hook(Op{Kind: OpCreate, Path: name}); err != nil {
		return nil, err
	}
	f, err := h.fs.Create(name)
	return h.file(f, err, name)
}

func (h hooked) Open(name string) (vfs.File, error) {
	if err := h.hook(Op{Kind: OpOpen, Path: name}); err != nil {
		return nil, err
	}
	f, err := h.fs.Open(name)
	return h.file(f, err, name)
}

func (h hooked) OpenReadWrite(name string) (vfs.File, error) {
	if err := h.hook(Op{Kind: OpOpenReadWrite, Path: name}); err != nil {
		return nil, err
	}
	f, err := h.fs.OpenReadWrite(name)
	return h.file(f, err, name)
}

func (h hooked) OpenDir(name string) (vfs.File, error) {
	if err := h.hook(Op{Kind: OpOpenDir, Path: name}); err != nil {
		return nil, err
	}
	f, err := h.fs.OpenDir(name)
	return h.file(f, err, name)
}

func (h hooked) Remove(name string) error {
	if err := h.hook(Op{Kind: OpRemove, Path: name}); err != nil {
		return err
	}
	return h.fs.Remove(name)
}

func (h hooked) Rename(oldname, newname string) error {
	if err := h.hook(Op{Kind: OpRename, Path: oldname}); err != nil {
		return err
	}
	return h.fs.Rename(oldname, newname)
}

func (h hooked) MkdirAll(dir string, perm os.FileMode) error {
	if err := h.hook(Op{Kind: OpMkdirAll, Path: dir}); err != nil {
		return err
	}
	return h.fs.MkdirAll(dir, perm)
}

func (h hooked) List(dir string) ([]string, error) {
	if err := h.hook(Op{Kind: OpList, Path: dir}); err != nil {
		return nil, err
	}
	return h.fs.List(dir)
}

func (h hooked) Stat(name string) (os.FileInfo, error) {
	if err := h.hook(Op{Kind: OpStat, Path: name}); err != nil {
		return nil, err
	}
	return h.fs.Stat(name)
}

func (h hooked) Lock(name string) (io.Closer, error) {
	if err := h.hook(Op{Kind: OpLock, Path: name}); err != nil {
		return nil, err
	}
	return h.fs.Lock(name)
}

func (h hooked) PathJoin(elem ...string) string { return h.fs.PathJoin(elem...) }
func (h hooked) PathDir(path string) string     { return h.fs.PathDir(path) }

type hookedFile struct {
	f    vfs.File
	hook func(Op) error
	path string
}

func (f *hookedFile) Close() error {
	if err := f.hook(Op{Kind: OpClose, Path: f.path}); err != nil {
		return err
	}
	return f.f.Close()
}

func (f *hookedFile) ReadAt(p []byte, off int64) (int, error) {
	if err := f.hook(Op{Kind: OpReadAt, Path: f.path, Off: off, Len: len(p)}); err != nil {
		return 0, err
	}
	return f.f.ReadAt(p, off)
}

func (f *hookedFile) WriteAt(p []byte, off int64) (int, error) {
	if err := f.hook(Op{Kind: OpWriteAt, Path: f.path, Off: off, Len: len(p), Data: p}); err != nil {
		return 0, err
	}
	return f.f.WriteAt(p, off)
}

func (f *hookedFile) Preallocate(off, length int64) error {
	if err := f.hook(Op{Kind: OpPreallocate, Path: f.path, Off: off}); err != nil {
		return err
	}
	return f.f.Preallocate(off, length)
}

func (f *hookedFile) Stat() (os.FileInfo, error) {
	if err := f.hook(Op{Kind: OpFileStat, Path: f.path}); err != nil {
		return nil, err
	}
	return f.f.Stat()
}

func (f *hookedFile) Sync() error {
	if err := f.hook(Op{Kind: OpSync, Path: f.path}); err != nil {
		f.failSync(err)
		return err
	}
	return f.f.Sync()
}

func (f *hookedFile) SyncData() error {
	if err := f.hook(Op{Kind: OpSyncData, Path: f.path}); err != nil {
		f.failSync(err)
		return err
	}
	return f.f.SyncData()
}

func (f *hookedFile) failSync(err error) {
	if !errors.Is(err, ErrSyncLost) {
		return
	}
	inner := f.f
	for {
		if h, ok := inner.(*hookedFile); ok {
			inner = h.f
			continue
		}
		break
	}
	if c, ok := inner.(*file); ok {
		c.loseUnsynced()
	}
}
