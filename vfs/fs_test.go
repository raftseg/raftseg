// Copyright 2026 The raftseg authors.

package vfs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// The lock keeps a second process (or a second Open in this one) away from
// a log directory, and is released by Close or by the process ending.
func TestLock(t *testing.T) {
	if runtime.GOOS == "windows" || runtime.GOOS == "plan9" || runtime.GOOS == "js" || runtime.GOOS == "wasip1" {
		t.Skip("no flock")
	}
	path := filepath.Join(t.TempDir(), "LOCK")
	l, err := OS.Lock(path)
	if err != nil {
		t.Fatal(err)
	}
	if l2, err := OS.Lock(path); err == nil {
		l2.Close()
		t.Fatal("a second lock in the same process succeeded")
	}
	if err := lockInChild(path); err == nil {
		t.Fatal("another process took a held lock")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lockInChild(path); err != nil {
		t.Fatalf("another process could not take a released lock: %v", err)
	}
	l3, err := OS.Lock(path)
	if err != nil {
		t.Fatalf("relock: %v", err)
	}
	l3.Close()
}

// lockInChild runs TestLockHelper in a new process, which tries to take
// the lock at path.
func lockInChild(path string) error {
	cmd := exec.Command(os.Args[0], "-test.run=^TestLockHelper$")
	cmd.Env = append(os.Environ(), "VFS_LOCK_HELPER="+path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return errors.New(string(out))
	}
	return nil
}

func TestLockHelper(t *testing.T) {
	path := os.Getenv("VFS_LOCK_HELPER")
	if path == "" {
		t.Skip("helper for TestLock")
	}
	l, err := OS.Lock(path)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
}

// Preallocate reserves space without changing the size, so that a file's
// size still says how much was written.
func TestPreallocateKeepsSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	f, err := OS.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt([]byte("hello"), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Preallocate(0, 1<<20); err != nil {
		t.Fatal(err)
	}
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != 5 {
		t.Fatalf("size %d after Preallocate, want 5", st.Size())
	}
	// Reads past the size see the end of the file, not the reservation.
	buf := make([]byte, 10)
	if n, err := f.ReadAt(buf, 0); n != 5 || err != io.EOF {
		t.Fatalf("read %d, %v", n, err)
	}
}

// The operations the storage packages rely on, against the real
// filesystem. (internal/crashfs checks that its model agrees with these.)
func TestOperations(t *testing.T) {
	dir := t.TempDir()
	p := OS.PathJoin(dir, "a", "b")
	if err := OS.MkdirAll(p, 0o755); err != nil {
		t.Fatal(err)
	}
	if OS.PathDir(p) != filepath.Join(dir, "a") {
		t.Fatalf("PathDir %s", OS.PathDir(p))
	}
	name := OS.PathJoin(p, "x")
	f, err := OS.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("world"), 6); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("hello"), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.SyncData(); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	r, err := OS.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 11)
	if n, err := r.ReadAt(buf, 0); n != 11 || (err != nil && err != io.EOF) || !bytes.Equal(buf, []byte("hello\x00world")) {
		t.Fatalf("read %q (%d, %v)", buf, n, err)
	}
	if _, err := r.WriteAt([]byte("x"), 0); err == nil {
		t.Fatal("wrote through a read-only handle")
	}
	r.Close()
	if err := OS.Rename(name, OS.PathJoin(p, "y")); err != nil {
		t.Fatal(err)
	}
	names, err := OS.List(p)
	if err != nil || len(names) != 1 || names[0] != "y" {
		t.Fatalf("List %v, %v", names, err)
	}
	d, err := OS.OpenDir(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Sync(); err != nil {
		t.Fatalf("directory sync: %v", err)
	}
	d.Close()
	if _, err := OS.Stat(name); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("old name: %v", err)
	}
	if _, err := OS.Open(name); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("open old name: %v", err)
	}
	// Create truncates.
	f, err = OS.Create(OS.PathJoin(p, "y"))
	if err != nil {
		t.Fatal(err)
	}
	if st, _ := f.Stat(); st.Size() != 0 {
		t.Fatalf("Create kept %d bytes", st.Size())
	}
	f.Close()
	if err := OS.Remove(OS.PathJoin(p, "y")); err != nil {
		t.Fatal(err)
	}
}
