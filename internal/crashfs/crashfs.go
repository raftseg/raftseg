// Copyright 2026 The raftseg authors.

// Package crashfs is an in-memory filesystem (vfs.FS) for tests, which can
// show what a power cut would leave behind, and fail operations on demand.
//
// It models what a POSIX filesystem promises and no more:
//
//   - Written data is durable once the file is synced (Sync or SyncData).
//     Until then, a crash keeps any subset of the unsynced data at 512-byte
//     sector granularity, in no particular order: writes can be torn and
//     reordered. A file's size after a crash is its synced size or its
//     latest size; bytes past the synced size that the crash did not keep
//     read as zeros.
//   - Creating, removing and renaming a name is durable once its directory
//     is synced. Until then, a crash keeps a prefix of the directory's
//     unsynced operations, as a journaling filesystem would.
//   - A file sync that a hook fails with ErrSyncLost behaves as Linux does
//     after a writeback error: the unsynced writes stay readable, but they
//     are never written, not even by a later sync that succeeds, unless they
//     are written again.
//
// Files and directories live in one tree under "/"; paths are cleaned and
// may be relative to "/".
package crashfs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/raftseg/raftseg/vfs"
)

const sectorSize = 512

// FS is a crashable in-memory filesystem.
type FS struct {
	mu    sync.Mutex
	dirs  map[string]*dir // cleaned path -> directory
	locks map[*inode]bool
}

type dir struct {
	durable map[string]*inode // as of the last sync of this directory
	cur     map[string]*inode
	ops     []dirOp // unsynced, in order
}

type dirOp struct {
	name   string
	node   *inode // nil: removed
	remove string // also removed, atomically (a rename)
}

type inode struct {
	durable []byte  // as of the last sync
	cur     []byte  // as written
	writes  []write // unsynced, in order
	lost    []span  // written, but never to be durable (see ErrSyncLost)
}

// span is the byte range [off, end).
type span struct{ off, end int64 }

// subtract removes [off, end) from spans.
func subtract(spans []span, off, end int64) []span {
	var out []span
	for _, s := range spans {
		if s.end <= off || s.off >= end {
			out = append(out, s)
			continue
		}
		if s.off < off {
			out = append(out, span{s.off, off})
		}
		if s.end > end {
			out = append(out, span{end, s.end})
		}
	}
	return out
}

type write struct {
	off  int64
	data []byte
}

// New returns an empty filesystem with a root directory.
func New() *FS {
	return &FS{dirs: map[string]*dir{"/": newDir()}, locks: map[*inode]bool{}}
}

func newDir() *dir { return &dir{durable: map[string]*inode{}, cur: map[string]*inode{}} }

func clean(name string) string { return path.Clean("/" + name) }

func split(name string) (string, string) {
	name = clean(name)
	return path.Dir(name), path.Base(name)
}

// missing is the error for a name that is not there. Like the operating
// system, it is ENOTDIR (not ErrNotExist) when a directory on the way is a
// file. Callers hold f.mu.
func (f *FS) missing(op, name string) error {
	c := clean(name)
	for p := path.Dir(c); p != "/"; p = path.Dir(p) {
		if dd, ok := f.dirs[path.Dir(p)]; ok {
			if _, isFile := dd.cur[path.Base(p)]; isFile {
				return &fs.PathError{Op: op, Path: name, Err: syscall.ENOTDIR}
			}
		}
	}
	return notExist(op, name)
}

func notExist(op, name string) error {
	return &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist}
}

// lookup finds the file at name in the current state.
func (f *FS) lookup(op, name string) (*inode, error) {
	d, base := split(name)
	dd, ok := f.dirs[d]
	if !ok {
		return nil, f.missing(op, name)
	}
	n, ok := dd.cur[base]
	if !ok {
		return nil, f.missing(op, name)
	}
	return n, nil
}

func (f *FS) setEntry(dirName, base string, n *inode) {
	dd := f.dirs[dirName]
	if n == nil {
		delete(dd.cur, base)
	} else {
		dd.cur[base] = n
	}
	dd.ops = append(dd.ops, dirOp{name: base, node: n})
}

// Create creates or truncates a file.
func (f *FS) Create(name string) (vfs.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, base := split(name)
	dd, ok := f.dirs[d]
	if !ok {
		return nil, f.missing("create", name)
	}
	if _, isDir := f.dirs[clean(name)]; isDir {
		return nil, &fs.PathError{Op: "create", Path: name, Err: errors.New("is a directory")}
	}
	if n, ok := dd.cur[base]; ok {
		// Truncation is data: durable once the file is synced.
		n.cur = nil
		n.lost = nil
		n.writes = append(n.writes, write{off: -1})
		return &file{fs: f, n: n, name: clean(name), writable: true}, nil
	}
	n := &inode{}
	f.setEntry(d, base, n)
	return &file{fs: f, n: n, name: clean(name), writable: true}, nil
}

func (f *FS) open(op, name string, writable bool) (vfs.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, isDir := f.dirs[clean(name)]; isDir {
		// As with open(2): a directory opens for reading only, and reading
		// it fails.
		if writable {
			return nil, &fs.PathError{Op: op, Path: name, Err: syscall.EISDIR}
		}
		return &file{fs: f, dir: clean(name), name: clean(name)}, nil
	}
	n, err := f.lookup(op, name)
	if err != nil {
		return nil, err
	}
	return &file{fs: f, n: n, name: clean(name), writable: writable}, nil
}

// Open opens a file for reading.
func (f *FS) Open(name string) (vfs.File, error) { return f.open("open", name, false) }

// OpenReadWrite opens a file for reading and writing.
func (f *FS) OpenReadWrite(name string) (vfs.File, error) { return f.open("open", name, true) }

// OpenDir opens a directory, so that it can be synced.
func (f *FS) OpenDir(name string) (vfs.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.dirs[clean(name)]; !ok {
		return nil, f.missing("open", name)
	}
	return &file{fs: f, dir: clean(name), name: clean(name)}, nil
}

// Remove removes a file or an empty directory.
func (f *FS) Remove(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := clean(name)
	if dd, ok := f.dirs[c]; ok {
		empty := len(dd.cur) == 0
		for p := range f.dirs {
			if p != "/" && path.Dir(p) == c {
				empty = false // a subdirectory
			}
		}
		if !empty || c == "/" {
			return &fs.PathError{Op: "remove", Path: name, Err: syscall.ENOTEMPTY}
		}
		delete(f.dirs, c)
		return nil
	}
	if _, err := f.lookup("remove", name); err != nil {
		return err
	}
	d, base := split(name)
	f.setEntry(d, base, nil)
	return nil
}

// Rename renames a file, replacing any file at newname. Within a
// directory the rename is atomic, after a crash too; across directories it
// is two operations.
func (f *FS) Rename(oldname, newname string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// As the operating system does: both parent directories are resolved
	// before either name is looked up.
	od, obase := split(oldname)
	nd, nbase := split(newname)
	if _, ok := f.dirs[od]; !ok {
		return f.missing("rename", oldname)
	}
	if _, ok := f.dirs[nd]; !ok {
		return f.missing("rename", newname)
	}
	n, err := f.lookup("rename", oldname)
	if err != nil {
		return err
	}
	if od == nd && obase == nbase {
		return nil
	}
	if od == nd {
		// Atomic within a directory.
		dd := f.dirs[nd]
		dd.cur[nbase] = n
		delete(dd.cur, obase)
		dd.ops = append(dd.ops, dirOp{name: nbase, node: n, remove: obase})
		return nil
	}
	f.setEntry(nd, nbase, n)
	f.setEntry(od, obase, nil)
	return nil
}

// MkdirAll creates a directory and its parents. Directories are durable
// at once: the storage packages create theirs before writing to them.
func (f *FS) MkdirAll(name string, _ os.FileMode) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := clean(name)
	for p := c; ; p = path.Dir(p) {
		if _, ok := f.dirs[p]; !ok {
			if parent, base := split(p); f.dirs[parent] != nil && f.dirs[parent].cur[base] != nil {
				return &fs.PathError{Op: "mkdir", Path: name, Err: errors.New("not a directory")}
			}
		}
		if p == "/" {
			break
		}
	}
	var missing []string
	for p := c; p != "/"; p = path.Dir(p) {
		if _, ok := f.dirs[p]; ok {
			break
		}
		missing = append(missing, p)
	}
	for i := len(missing) - 1; i >= 0; i-- {
		f.dirs[missing[i]] = newDir()
	}
	return nil
}

// List returns the sorted names in a directory: files and subdirectories.
func (f *FS) List(name string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := clean(name)
	dd, ok := f.dirs[c]
	if !ok {
		if _, err := f.lookup("open", name); err == nil {
			return nil, &fs.PathError{Op: "open", Path: name, Err: syscall.ENOTDIR}
		}
		return nil, f.missing("open", name)
	}
	var names []string
	for n := range dd.cur {
		names = append(names, n)
	}
	for p := range f.dirs {
		if p != "/" && path.Dir(p) == c {
			names = append(names, path.Base(p))
		}
	}
	sort.Strings(names)
	return names, nil
}

// Stat describes a file or directory.
func (f *FS) Stat(name string) (os.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := clean(name)
	if _, ok := f.dirs[c]; ok {
		return info{name: path.Base(c), dir: true}, nil
	}
	n, err := f.lookup("stat", name)
	if err != nil {
		return nil, err
	}
	return info{name: path.Base(c), size: int64(len(n.cur))}, nil
}

// Lock takes an exclusive lock on name, creating the file.
func (f *FS) Lock(name string) (io.Closer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.lookup("lock", name)
	if err != nil {
		d, base := split(name)
		if _, ok := f.dirs[d]; !ok {
			return nil, err
		}
		n = &inode{}
		f.setEntry(d, base, n)
	}
	if f.locks[n] {
		return nil, fmt.Errorf("crashfs: lock %s: held", name)
	}
	f.locks[n] = true
	return &lock{fs: f, n: n}, nil
}

type lock struct {
	fs   *FS
	n    *inode
	once sync.Once
}

func (l *lock) Close() error {
	l.once.Do(func() {
		l.fs.mu.Lock()
		delete(l.fs.locks, l.n)
		l.fs.mu.Unlock()
	})
	return nil
}

// PathJoin joins path elements.
func (f *FS) PathJoin(elem ...string) string { return path.Join(elem...) }

// PathDir returns the directory of a path.
func (f *FS) PathDir(p string) string { return path.Dir(p) }

// CrashClone returns the filesystem a power cut at this instant could
// leave: synced state, plus a random part of what was not synced, of which
// keepPercent percent is kept on average (0: nothing, 100: everything).
// Locks are not carried over: they die with the process.
func (f *FS) CrashClone(keepPercent int, rng *rand.Rand) *FS {
	f.mu.Lock()
	defer f.mu.Unlock()
	keep := func() bool { return rng.IntN(100) < keepPercent }
	clone := &FS{dirs: map[string]*dir{}, locks: map[*inode]bool{}}
	nodes := map[*inode]*inode{}
	cloneNode := func(n *inode) *inode {
		if c, ok := nodes[n]; ok {
			return c
		}
		data := crashData(n, keep, rng)
		c := &inode{durable: data, cur: append([]byte(nil), data...)}
		nodes[n] = c
		return c
	}
	paths := make([]string, 0, len(f.dirs))
	for p := range f.dirs {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		dd := f.dirs[p]
		entries := map[string]*inode{}
		for name, n := range dd.durable {
			entries[name] = n
		}
		// A prefix of the unsynced operations survives.
		kept := 0
		if keepPercent >= 100 {
			kept = len(dd.ops)
		} else if keepPercent > 0 {
			for kept < len(dd.ops) && keep() {
				kept++
			}
		}
		for _, op := range dd.ops[:kept] {
			if op.node == nil {
				delete(entries, op.name)
			} else {
				entries[op.name] = op.node
			}
			if op.remove != "" {
				delete(entries, op.remove)
			}
		}
		nd := newDir()
		names := make([]string, 0, len(entries))
		for name := range entries {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			c := cloneNode(entries[name])
			nd.durable[name] = c
			nd.cur[name] = c
		}
		clone.dirs[p] = nd
	}
	return clone
}

// crashData is what a crash leaves of a file's contents.
func crashData(n *inode, keep func() bool, rng *rand.Rand) []byte {
	data := append([]byte(nil), n.durable...)
	if len(n.writes) == 0 {
		return data
	}
	size := int64(len(data))
	if keep() {
		size = int64(len(n.cur)) // the size reached the journal
	}
	img := make([]byte, max(size, int64(len(data))))
	copy(img, data)
	for _, w := range n.writes {
		if w.off < 0 {
			// A truncation that survives empties what came before.
			if keep() {
				clear(img)
			}
			continue
		}
		for s := w.off / sectorSize * sectorSize; s < w.off+int64(len(w.data)); s += sectorSize {
			if !keep() {
				continue
			}
			lo, hi := max(s, w.off), min(s+sectorSize, w.off+int64(len(w.data)))
			if hi > int64(len(img)) {
				if hi > size {
					hi = size // past the surviving size
				}
				if hi <= lo {
					continue
				}
			}
			copy(img[lo:hi], w.data[lo-w.off:hi-w.off])
		}
	}
	return img[:size]
}

type file struct {
	fs       *FS
	n        *inode // nil for a directory
	dir      string
	name     string
	writable bool
	mu       sync.Mutex
	closed   bool
}

func (h *file) check(op string) error {
	if h.closed {
		return &fs.PathError{Op: op, Path: h.name, Err: fs.ErrClosed}
	}
	return nil
}

func (h *file) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.check("close"); err != nil {
		return err
	}
	h.closed = true
	return nil
}

func (h *file) ReadAt(p []byte, off int64) (int, error) {
	h.mu.Lock()
	closed := h.closed
	h.mu.Unlock()
	if closed {
		return 0, &fs.PathError{Op: "read", Path: h.name, Err: fs.ErrClosed}
	}
	if h.n == nil {
		return 0, &fs.PathError{Op: "read", Path: h.name, Err: syscall.EISDIR}
	}
	if off < 0 {
		return 0, &fs.PathError{Op: "read", Path: h.name, Err: errors.New("negative offset")}
	}
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if off >= int64(len(h.n.cur)) {
		return 0, io.EOF
	}
	n := copy(p, h.n.cur[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (h *file) WriteAt(p []byte, off int64) (int, error) {
	h.mu.Lock()
	err := h.check("write")
	h.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if h.n == nil || !h.writable {
		return 0, &fs.PathError{Op: "write", Path: h.name, Err: fs.ErrPermission}
	}
	if off < 0 {
		return 0, &fs.PathError{Op: "write", Path: h.name, Err: errors.New("negative offset")}
	}
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if end := off + int64(len(p)); end > int64(len(h.n.cur)) {
		h.n.cur = append(h.n.cur, make([]byte, end-int64(len(h.n.cur)))...)
	}
	copy(h.n.cur[off:], p)
	h.n.writes = append(h.n.writes, write{off: off, data: append([]byte(nil), p...)})
	if len(h.n.lost) > 0 {
		h.n.lost = subtract(h.n.lost, off, off+int64(len(p)))
	}
	return len(p), nil
}

func (h *file) Preallocate(offset, length int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.check("preallocate")
}

func (h *file) Stat() (os.FileInfo, error) {
	h.mu.Lock()
	err := h.check("stat")
	h.mu.Unlock()
	if err != nil {
		return nil, err
	}
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if h.n == nil {
		return info{name: path.Base(h.dir), dir: true}, nil
	}
	return info{name: path.Base(h.name), size: int64(len(h.n.cur))}, nil
}

// Sync makes a file's data, or a directory's entries, durable.
func (h *file) Sync() error {
	h.mu.Lock()
	err := h.check("sync")
	h.mu.Unlock()
	if err != nil {
		return err
	}
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	if h.n == nil {
		dd, ok := h.fs.dirs[h.dir]
		if !ok {
			return nil // removed since it was opened: nothing left to sync
		}
		dd.durable = map[string]*inode{}
		for name, n := range dd.cur {
			dd.durable[name] = n
		}
		dd.ops = nil
		return nil
	}
	old := h.n.durable
	h.n.durable = append([]byte(nil), h.n.cur...)
	for _, s := range h.n.lost {
		// The page cache holds these bytes, the disk never got them.
		for i := s.off; i < s.end && i < int64(len(h.n.durable)); i++ {
			if i < int64(len(old)) {
				h.n.durable[i] = old[i]
			} else {
				h.n.durable[i] = 0
			}
		}
	}
	h.n.writes = nil
	return nil
}

// loseUnsynced drops the file's unsynced writes from what can become
// durable: a failed writeback.
func (h *file) loseUnsynced() {
	if h.n == nil {
		return
	}
	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()
	for _, w := range h.n.writes {
		if w.off >= 0 {
			h.n.lost = append(h.n.lost, span{w.off, w.off + int64(len(w.data))})
		}
	}
	// A truncation is metadata; it stays pending.
	var pending []write
	for _, w := range h.n.writes {
		if w.off < 0 {
			pending = append(pending, w)
		}
	}
	h.n.writes = pending
}

// SyncData is Sync: a file's size is part of the data needed to read it.
func (h *file) SyncData() error { return h.Sync() }

type info struct {
	name string
	size int64
	dir  bool
}

func (i info) Name() string { return i.name }
func (i info) Size() int64  { return i.size }
func (i info) Mode() os.FileMode {
	if i.dir {
		return os.ModeDir | 0o755
	}
	return 0o644
}
func (i info) ModTime() time.Time { return time.Time{} }
func (i info) IsDir() bool        { return i.dir }
func (i info) Sys() any           { return nil }

var _ vfs.FS = (*FS)(nil)

// String describes the filesystem, for debugging.
func (f *FS) String() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var b strings.Builder
	paths := make([]string, 0, len(f.dirs))
	for p := range f.dirs {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		dd := f.dirs[p]
		fmt.Fprintf(&b, "%s (%d unsynced ops)\n", p, len(dd.ops))
		names := make([]string, 0, len(dd.cur))
		for n := range dd.cur {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			in := dd.cur[n]
			fmt.Fprintf(&b, "  %s: %d bytes, %d synced, %d unsynced writes\n", n, len(in.cur), len(in.durable), len(in.writes))
		}
	}
	return b.String()
}

// NewFile returns a writable file holding data, outside any filesystem.
func NewFile(data []byte) vfs.File {
	n := &inode{durable: append([]byte(nil), data...), cur: append([]byte(nil), data...)}
	return &file{fs: New(), n: n, name: "/(file)", writable: true}
}
