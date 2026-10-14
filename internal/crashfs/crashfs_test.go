// Copyright 2026 The raftseg authors.

package crashfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"testing"

	"github.com/raftseg/raftseg/vfs"
)

func mustWrite(t *testing.T, f vfs.File, p []byte, off int64) {
	t.Helper()
	if n, err := f.WriteAt(p, off); err != nil || n != len(p) {
		t.Fatalf("write: %d, %v", n, err)
	}
}

func readAll(t *testing.T, fsys vfs.FS, name string) []byte {
	t.Helper()
	f, err := fsys.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, st.Size())
	if n, err := f.ReadAt(b, 0); n != len(b) || (err != nil && err != io.EOF) {
		t.Fatalf("read %s: %d, %v", name, n, err)
	}
	return b
}

func syncPath(t *testing.T, fsys vfs.FS, name string, dir bool) {
	t.Helper()
	var f vfs.File
	var err error
	if dir {
		f, err = fsys.OpenDir(name)
	} else {
		f, err = fsys.OpenReadWrite(name)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func TestBasics(t *testing.T) {
	fsys := New()
	if err := fsys.MkdirAll("/a/b", 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := fsys.Create("/a/b/x")
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, f, []byte("hello"), 0)
	mustWrite(t, f, []byte("!"), 7)
	if got := readAll(t, fsys, "a/b/x"); !bytes.Equal(got, []byte("hello\x00\x00!")) {
		t.Fatalf("read %q", got)
	}
	buf := make([]byte, 4)
	if n, err := f.ReadAt(buf, 6); n != 2 || err != io.EOF {
		t.Fatalf("short read: %d, %v", n, err)
	}
	if n, err := f.ReadAt(buf, 100); n != 0 || err != io.EOF {
		t.Fatalf("read past end: %d, %v", n, err)
	}
	f.Close()
	if _, err := f.WriteAt([]byte("x"), 0); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("write after close: %v", err)
	}
	ro, _ := fsys.Open("/a/b/x")
	if _, err := ro.WriteAt([]byte("x"), 0); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("write to read-only file: %v", err)
	}
	names, err := fsys.List("/a")
	if err != nil || fmt.Sprint(names) != "[b]" {
		t.Fatalf("list /a: %v %v", names, err)
	}
	if err := fsys.Rename("/a/b/x", "/a/b/y"); err != nil {
		t.Fatal(err)
	}
	if _, err := fsys.Stat("/a/b/x"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("old name: %v", err)
	}
	if st, err := fsys.Stat("/a/b/y"); err != nil || st.Size() != 8 {
		t.Fatalf("new name: %v %v", st, err)
	}
	if err := fsys.Remove("/a/b"); err == nil {
		t.Fatal("removed a non-empty directory")
	}
	l, err := fsys.Lock("/a/LOCK")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fsys.Lock("/a/LOCK"); err == nil {
		t.Fatal("lock taken twice")
	}
	l.Close()
	l2, err := fsys.Lock("/a/LOCK")
	if err != nil {
		t.Fatal(err)
	}
	l2.Close()
}

func TestCrashKeepsSyncedOnly(t *testing.T) {
	fsys := New()
	fsys.MkdirAll("/d", 0o755)
	f, _ := fsys.Create("/d/synced")
	mustWrite(t, f, []byte("durable"), 0)
	f.Sync()
	mustWrite(t, f, []byte("DURABLE and more"), 0)
	g, _ := fsys.Create("/d/unsynced-dir-entry")
	mustWrite(t, g, []byte("data"), 0)
	g.Sync() // data synced, but the name is not
	syncPath(t, fsys, "/d", true)
	h, _ := fsys.Create("/d/new")
	mustWrite(t, h, []byte("x"), 0)
	h.Sync()

	none := fsys.CrashClone(0, rand.New(rand.NewPCG(1, 2)))
	if got := readAll(t, none, "/d/synced"); string(got) != "durable" {
		t.Fatalf("synced file after crash: %q", got)
	}
	if got := readAll(t, none, "/d/unsynced-dir-entry"); string(got) != "data" {
		t.Fatalf("file named before the directory sync: %q", got)
	}
	if _, err := none.Stat("/d/new"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("unsynced name survived: %v", err)
	}
	all := fsys.CrashClone(100, rand.New(rand.NewPCG(1, 2)))
	if got := readAll(t, all, "/d/synced"); string(got) != "DURABLE and more" {
		t.Fatalf("keep everything: %q", got)
	}
	if got := readAll(t, all, "/d/new"); string(got) != "x" {
		t.Fatalf("keep everything: %q", got)
	}
	// The original is untouched, and the clone is independent of it.
	if got := readAll(t, fsys, "/d/synced"); string(got) != "DURABLE and more" {
		t.Fatalf("original: %q", got)
	}
	cf, _ := all.OpenReadWrite("/d/new")
	mustWrite(t, cf, []byte("y"), 0)
	if got := readAll(t, fsys, "/d/new"); string(got) != "x" {
		t.Fatalf("clone shares data with the original: %q", got)
	}
}

func TestRenameIsAtomic(t *testing.T) {
	for seed := uint64(0); seed < 200; seed++ {
		fsys := New()
		fsys.MkdirAll("/d", 0o755)
		f, _ := fsys.Create("/d/a.tmp")
		mustWrite(t, f, []byte("new"), 0)
		f.Sync()
		fsys.Rename("/d/a.tmp", "/d/a")
		c := fsys.CrashClone(50, rand.New(rand.NewPCG(seed, 1)))
		names, _ := c.List("/d")
		switch fmt.Sprint(names) {
		case "[]", "[a.tmp]", "[a]":
		default:
			t.Fatalf("after a crash: %v", names)
		}
	}
}

// After a crash, every byte of a file is what it was when last synced, or
// what some unsynced write put there, or zero; and its size is the synced
// size or the latest.
func TestCrashDataIsPlausible(t *testing.T) {
	for seed := uint64(0); seed < 300; seed++ {
		rng := rand.New(rand.NewPCG(seed, 7))
		fsys := New()
		fsys.MkdirAll("/d", 0o755)
		f, _ := fsys.Create("/d/f")
		syncPath(t, fsys, "/d", true)
		var synced []byte
		cur := []byte{}
		var writes [][2]int64 // offset, length of unsynced writes
		var data [][]byte
		for i := 0; i < 20; i++ {
			if rng.IntN(5) == 0 {
				f.Sync()
				synced = append([]byte(nil), cur...)
				writes, data = nil, nil
				continue
			}
			off := int64(rng.IntN(3000))
			p := make([]byte, 1+rng.IntN(1500))
			for j := range p {
				p[j] = byte(1 + rng.IntN(255))
			}
			mustWrite(t, f, p, off)
			if end := off + int64(len(p)); end > int64(len(cur)) {
				cur = append(cur, make([]byte, end-int64(len(cur)))...)
			}
			copy(cur[off:], p)
			writes = append(writes, [2]int64{off, int64(len(p))})
			data = append(data, p)
		}
		c := fsys.CrashClone(rng.IntN(101), rng)
		got := readAll(t, c, "/d/f")
		if len(got) != len(synced) && len(got) != len(cur) {
			t.Fatalf("seed %d: size %d, want %d or %d", seed, len(got), len(synced), len(cur))
		}
		for i, b := range got {
			ok := b == 0 || (i < len(synced) && b == synced[i])
			for k, w := range writes {
				if int64(i) >= w[0] && int64(i) < w[0]+w[1] && data[k][int64(i)-w[0]] == b {
					ok = true
				}
			}
			if !ok {
				t.Fatalf("seed %d: byte %d is %d, which nothing wrote", seed, i, b)
			}
		}
	}
}

func TestHook(t *testing.T) {
	fsys := New()
	var ops []string
	hfs := WithHook(fsys, func(op Op) error {
		ops = append(ops, op.Kind.String())
		if op.Kind == OpSyncData {
			return ErrInjected
		}
		return nil
	})
	f, err := hfs.Create("/x")
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, f, []byte("a"), 0)
	if err := f.SyncData(); !errors.Is(err, ErrInjected) {
		t.Fatalf("got %v", err)
	}
	f.Close()
	if fmt.Sprint(ops) != "[create write-at sync-data close]" {
		t.Fatalf("ops %v", ops)
	}
	// The failed sync had no effect.
	if c := fsys.CrashClone(0, rand.New(rand.NewPCG(1, 1))); true {
		if _, err := c.Stat("/x"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("got %v", err)
		}
	}
}

// A sync failed with ErrSyncLost loses the unsynced writes for good: they
// stay readable, a later successful sync does not persist them, and a crash
// shows the old contents. Writing them again makes them durable again.
func TestSyncLost(t *testing.T) {
	fsys := New()
	fsys.MkdirAll("/d", 0o755)
	f, _ := fsys.Create("/d/f")
	mustWrite(t, f, []byte("old data"), 0)
	f.Sync()
	syncPath(t, fsys, "/d", true)
	fail := true
	hfs := WithHook(fsys, func(op Op) error {
		if fail && op.Kind == OpSyncData {
			return ErrSyncLost
		}
		return nil
	})
	h, _ := hfs.OpenReadWrite("/d/f")
	mustWrite(t, h, []byte("NEW"), 0)
	mustWrite(t, h, []byte(" tail"), 8)
	if err := h.SyncData(); !errors.Is(err, ErrSyncLost) || !errors.Is(err, ErrInjected) {
		t.Fatalf("sync: %v", err)
	}
	if got := readAll(t, fsys, "/d/f"); string(got) != "NEW data tail" {
		t.Fatalf("reads after a failed sync: %q", got)
	}
	fail = false
	if err := h.SyncData(); err != nil {
		t.Fatal(err)
	}
	c := fsys.CrashClone(100, rand.New(rand.NewPCG(1, 2)))
	if got := readAll(t, c, "/d/f"); string(got) != "old data\x00\x00\x00\x00\x00" {
		t.Fatalf("after a crash: %q", got)
	}
	mustWrite(t, h, []byte("NE"), 0) // rewrites part of the lost range
	h.SyncData()
	c = fsys.CrashClone(0, rand.New(rand.NewPCG(1, 2)))
	if got := readAll(t, c, "/d/f"); string(got) != "NEd data\x00\x00\x00\x00\x00" {
		t.Fatalf("after rewriting part of it: %q", got)
	}
	// A plain injected error loses nothing.
	g, _ := WithHook(fsys, func(op Op) error {
		if op.Kind == OpSync {
			return ErrInjected
		}
		return nil
	}).OpenReadWrite("/d/f")
	mustWrite(t, g, []byte("X"), 3)
	g.Sync()
	g2, _ := fsys.OpenReadWrite("/d/f")
	g2.Sync()
	if got := readAll(t, fsys.CrashClone(0, rand.New(rand.NewPCG(1, 2))), "/d/f"); string(got[:4]) != "NEdX" {
		t.Fatalf("plain error: %q", got)
	}
}

// Storage code compares files by identity (for example, to sync each file
// once): every File must be comparable, hooked or not.
func TestFilesComparable(t *testing.T) {
	fsys := New()
	f, _ := fsys.Create("/a")
	h, _ := WithHook(fsys, func(Op) error { return nil }).Open("/a")
	files := []vfs.File{f, h, NewFile(nil)}
	for _, x := range files {
		for _, y := range files {
			_ = x == y // panics if either is not comparable
		}
	}
}
