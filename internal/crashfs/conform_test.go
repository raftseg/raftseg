// Copyright 2026 The raftseg authors.

package crashfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/raftseg/raftseg/vfs"
)

// crashfs is only as good a model as it is faithful: every crash test in
// the module trusts that, without a crash, it behaves as the operating
// system's filesystem does. TestConformsToOS runs the same random
// operations against both and compares every result: errors (by class),
// data read, sizes and listings.

// errClass reduces an error to what callers may rely on.
func errClass(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, fs.ErrNotExist):
		return "not-exist"
	case errors.Is(err, io.EOF):
		return "eof"
	default:
		return "error"
	}
}

type fsUnderTest struct {
	name  string
	fs    vfs.FS
	root  string
	files map[string]vfs.File // open handles by slot
	locks map[string]io.Closer
}

func (u *fsUnderTest) path(p string) string {
	if p == "" {
		return u.root
	}
	return u.fs.PathJoin(u.root, p)
}

func TestConformsToOS(t *testing.T) {
	seeds := 300
	if testing.Short() {
		seeds = 60
	}
	for seed := uint64(0); seed < uint64(seeds); seed++ {
		osRoot := t.TempDir()
		a := &fsUnderTest{name: "os", fs: vfs.OS, root: osRoot, files: map[string]vfs.File{}, locks: map[string]io.Closer{}}
		b := &fsUnderTest{name: "crashfs", fs: New(), root: "/r", files: map[string]vfs.File{}, locks: map[string]io.Closer{}}
		b.fs.MkdirAll("/r", 0o755)
		runConformance(t, seed, a, b)
	}
}

func runConformance(t *testing.T, seed uint64, a, b *fsUnderTest) {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, 99))
	names := []string{"a", "b", "d/x", "d/y", "d/e/z", "d", "d/e", "LOCK"}
	slots := []string{"1", "2", "3"}
	pick := func(xs []string) string { return xs[rng.IntN(len(xs))] }
	var log []string
	for step := 0; step < 120; step++ {
		var desc string
		var results [2]string
		do := func(f func(u *fsUnderTest) string) {
			results[0], results[1] = f(a), f(b)
		}
		switch op := rng.IntN(16); op {
		case 0:
			p := pick(names)
			slot := pick(slots)
			desc = fmt.Sprintf("create %s as %s", p, slot)
			do(func(u *fsUnderTest) string {
				if f := u.files[slot]; f != nil {
					f.Close()
				}
				f, err := u.fs.Create(u.path(p))
				u.files[slot] = f
				return errClass(err)
			})
		case 1, 2:
			p, slot, rw := pick(names), pick(slots), rng.IntN(2) == 0
			desc = fmt.Sprintf("open %s rw=%v as %s", p, rw, slot)
			do(func(u *fsUnderTest) string {
				if f := u.files[slot]; f != nil {
					f.Close()
				}
				var f vfs.File
				var err error
				if rw {
					f, err = u.fs.OpenReadWrite(u.path(p))
				} else {
					f, err = u.fs.Open(u.path(p))
				}
				u.files[slot] = f
				return errClass(err)
			})
		case 3, 4:
			slot := pick(slots)
			off := int64(rng.IntN(3000))
			data := bytes.Repeat([]byte{byte(1 + rng.IntN(250))}, 1+rng.IntN(1500))
			desc = fmt.Sprintf("write %d bytes at %d to %s", len(data), off, slot)
			do(func(u *fsUnderTest) string {
				f := u.files[slot]
				if f == nil {
					return "no-file"
				}
				n, err := f.WriteAt(data, off)
				if err != nil {
					return errClass(err)
				}
				return fmt.Sprint(n)
			})
		case 5, 6:
			slot := pick(slots)
			off := int64(rng.IntN(4000))
			n := 1 + rng.IntN(2000) // (os.File reads nothing, so fails nothing, for an empty buffer)
			desc = fmt.Sprintf("read %d bytes at %d from %s", n, off, slot)
			do(func(u *fsUnderTest) string {
				f := u.files[slot]
				if f == nil {
					return "no-file"
				}
				buf := make([]byte, n)
				got, err := f.ReadAt(buf, off)
				// The OS may report a short read at the end with or without
				// io.EOF only when the read ends exactly there; both promise
				// io.EOF for a short read.
				if got < n && err == nil {
					return fmt.Sprintf("short read %d without an error", got)
				}
				if got == n && errors.Is(err, io.EOF) {
					err = nil // allowed either way when the read fills buf
				}
				return fmt.Sprintf("%d %s %x", got, errClass(err), fnv(buf[:got]))
			})
		case 7:
			slot := pick(slots)
			desc = "stat " + slot
			do(func(u *fsUnderTest) string {
				f := u.files[slot]
				if f == nil {
					return "no-file"
				}
				st, err := f.Stat()
				if err != nil {
					return errClass(err)
				}
				if st.IsDir() {
					return "dir"
				}
				return fmt.Sprint(st.Size())
			})
		case 8:
			slot := pick(slots)
			desc = "close " + slot
			do(func(u *fsUnderTest) string {
				f := u.files[slot]
				if f == nil {
					return "no-file"
				}
				err := f.Close()
				// A closed handle stays in its slot: using it must fail.
				return errClass(err)
			})
		case 9:
			p, q := pick(names), pick(names)
			desc = fmt.Sprintf("rename %s %s", p, q)
			do(func(u *fsUnderTest) string {
				st, err := u.fs.Stat(u.path(p))
				if err == nil && st.IsDir() {
					return "skip" // directory renames are not used
				}
				if st2, err := u.fs.Stat(u.path(q)); err == nil && st2.IsDir() {
					return "skip"
				}
				return errClass(u.fs.Rename(u.path(p), u.path(q)))
			})
		case 10:
			p := pick(names)
			desc = "remove " + p
			do(func(u *fsUnderTest) string { return errClass(u.fs.Remove(u.path(p))) })
		case 11:
			p := pick([]string{"d", "d/e", "d/e/f", "a/b"})
			desc = "mkdirall " + p
			do(func(u *fsUnderTest) string { return errClass(u.fs.MkdirAll(u.path(p), 0o755)) })
		case 12:
			p := pick([]string{"", "d", "d/e", "a", "nope"})
			desc = "list " + p
			do(func(u *fsUnderTest) string {
				names, err := u.fs.List(u.path(p))
				if err != nil {
					return errClass(err)
				}
				sort.Strings(names)
				return strings.Join(names, ",")
			})
		case 13:
			p := pick(names)
			desc = "stat " + p
			do(func(u *fsUnderTest) string {
				st, err := u.fs.Stat(u.path(p))
				if err != nil {
					return errClass(err)
				}
				if st.IsDir() {
					return "dir"
				}
				return fmt.Sprint(st.Size())
			})
		case 14:
			slot := pick(slots)
			desc = "sync " + slot
			do(func(u *fsUnderTest) string {
				f := u.files[slot]
				if f == nil {
					return "no-file"
				}
				if rng.IntN(2) == 0 {
					return errClass(f.Sync())
				}
				return errClass(f.SyncData())
			})
		case 15:
			p := pick([]string{"LOCK", "d/LOCK"})
			release := rng.IntN(2) == 0
			desc = fmt.Sprintf("lock %s release=%v", p, release)
			do(func(u *fsUnderTest) string {
				if release {
					if l := u.locks[p]; l != nil {
						delete(u.locks, p)
						return errClass(l.Close())
					}
					return "none"
				}
				l, err := u.fs.Lock(u.path(p))
				if err == nil {
					// A second lock from the same process must fail too.
					if l2, err2 := u.fs.Lock(u.path(p)); err2 == nil {
						l2.Close()
						l.Close()
						return "locked twice"
					}
					if old := u.locks[p]; old != nil {
						old.Close()
					}
					u.locks[p] = l
				}
				return errClass(err)
			})
		}
		log = append(log, fmt.Sprintf("%s: os %s, crashfs %s", desc, results[0], results[1]))
		if results[0] != results[1] {
			t.Fatalf("seed %d step %d: %s: os %q, crashfs %q\nhistory:\n%s", seed, step, desc, results[0], results[1], strings.Join(log[max(0, len(log)-15):], "\n"))
		}
	}
	for _, u := range []*fsUnderTest{a, b} {
		for _, f := range u.files {
			if f != nil {
				f.Close()
			}
		}
		for _, l := range u.locks {
			l.Close()
		}
	}
	_ = filepath.Join
}

func fnv(b []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}
