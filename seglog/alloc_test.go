// Copyright 2026 The raftseg authors.

package seglog

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
	"github.com/raftseg/raftseg/vfs"
)

// allocated returns how many bytes fn allocated.
func allocated(fn func()) uint64 {
	var a, b runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&a)
	fn()
	runtime.ReadMemStats(&b)
	return b.TotalAlloc - a.TotalAlloc
}

// A snapshot reference claiming a huge size made Snapshot allocate that
// much before it looked at the file (up to 1 TiB, which kills the process).
// The size is now checked against the file first.
func TestSnapshotRefSizeNotTrusted(t *testing.T) {
	mem := crashfs.New()
	l := open(t, crashDir, memOpts(mem, Options{SnapshotInlineBytes: 16}))
	if err := l.Save(Update{Group: 1, Entries: ents(1, 1, 5, 10), HardState: &raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(5))}}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Group(1).CreateSnapshot(5, nil, make([]byte, 500)); err != nil {
		t.Fatal(err)
	}
	l.Close()
	c, err := decodeCheckpoint(mustRead(t, mem, crashDir+"/CHECKPOINT-A"))
	if err != nil {
		t.Fatal(err)
	}
	c.groups[1].wsnap.ref.size = 4 << 30
	b := encodeCheckpointFile(c, formatVersion)
	rewrite(t, mem, crashDir+"/CHECKPOINT-A", b)
	rewrite(t, mem, crashDir+"/CHECKPOINT-B", b)
	l = open(t, crashDir, memOpts(mem, Options{}))
	defer l.Close()
	var serr error
	if n := allocated(func() { _, serr = l.Group(1).Snapshot() }); n > 1<<20 {
		t.Fatalf("Snapshot allocated %d bytes", n)
	}
	if !errors.Is(serr, ErrCorrupt) {
		t.Fatalf("Snapshot: %v", serr)
	}
}

// Sparse files report a size they do not hold. Open must not allocate a
// checkpoint's or a sidecar's reported size beyond what such a file can
// legitimately be, nor scan a segment larger than a segment can be.
func TestSparseFilesNotRead(t *testing.T) {
	build := func(t *testing.T) string {
		dir := t.TempDir()
		l := open(t, dir, Options{SegmentSize: 64 << 10})
		for i := uint64(0); i < 40; i++ {
			if err := l.Save(Update{Group: 1, Entries: ents(1, i*10+1, i*10+10, 1000), HardState: &raftpb.HardState{Term: new(uint64(1)), Commit: new(i*10 + 10)}}); err != nil {
				t.Fatal(err)
			}
		}
		l.Close()
		return dir
	}
	grow := func(t *testing.T, path string, size int64) {
		if err := os.Truncate(path, size); err != nil {
			t.Skipf("cannot make a sparse file: %v", err)
		}
	}
	segs := func(t *testing.T, dir string) []uint64 {
		ns, err := listSegments(vfs.OS, filepath.Join(dir, "segments"))
		if err != nil {
			t.Fatal(err)
		}
		return ns
	}
	t.Run("checkpoint", func(t *testing.T) {
		dir := build(t)
		grow(t, filepath.Join(dir, "CHECKPOINT-A"), 3<<30)
		var l *Log
		var err error
		if n := allocated(func() { l, err = Open(dir, Options{}) }); n > 64<<20 {
			t.Fatalf("Open allocated %d bytes", n)
		}
		if err != nil {
			t.Fatal(err) // the other copy is intact
		}
		l.Close()
	})
	t.Run("sidecar", func(t *testing.T) {
		dir := build(t)
		grow(t, filepath.Join(dir, "segments", idxName(segs(t, dir)[0])), 3<<30)
		l := open(t, dir, Options{CacheEntries: -1})
		defer l.Close()
		var err error
		if n := allocated(func() { _, err = l.Group(1).Entries(1, 401, 1<<30) }); n > 64<<20 {
			t.Fatalf("reading allocated %d bytes", n)
		}
		if err != nil {
			t.Fatal(err) // rebuilt from the segment
		}
	})
	t.Run("segment", func(t *testing.T) {
		dir := build(t)
		ns := segs(t, dir)
		grow(t, filepath.Join(dir, "segments", segName(ns[len(ns)-1])), 8<<30)
		start := time.Now()
		if _, err := Open(dir, Options{}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("open: %v", err)
		}
		if d := time.Since(start); d > 10*time.Second {
			t.Fatalf("Open took %v", d)
		}
	})
}
