// Copyright 2026 The raftseg authors.

package seglog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
)

func snapFiles(t *testing.T, dir string) []string {
	t.Helper()
	names, err := os.ReadDir(filepath.Join(dir, "snapshots"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, n := range names {
		out = append(out, n.Name())
	}
	return out
}

func TestExternalSnapshots(t *testing.T) {
	dir := t.TempDir()
	o := Options{SnapshotInlineBytes: 1 << 10}
	l := open(t, dir, o)
	s := l.Group(5)
	if err := s.Save(&raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(10))}, ents(1, 1, 10, 50), &raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	big := bytes.Repeat([]byte("state"), 10000)
	if _, err := s.CreateSnapshot(5, nil, big); err != nil {
		t.Fatal(err)
	}
	if got := snapFiles(t, dir); len(got) != 1 || got[0] != snapName(5, 5) {
		t.Fatalf("snapshot files %v", got)
	}
	// Replacing it removes the old file; a small one goes back inline.
	if _, err := s.CreateSnapshot(8, nil, big[:100]); err != nil {
		t.Fatal(err)
	}
	if got := snapFiles(t, dir); len(got) != 0 {
		t.Fatalf("old snapshot file not removed: %v", got)
	}
	// A large snapshot received from a leader.
	recv := &raftpb.Snapshot{Data: big, Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(50)), Term: new(uint64(3)), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2}}}}
	if err := s.Save(&raftpb.HardState{Term: new(uint64(3)), Commit: new(uint64(50))}, nil, recv); err != nil {
		t.Fatal(err)
	}
	// An orphan (e.g. from a crash before the index referred to it).
	os.WriteFile(filepath.Join(dir, "snapshots", snapName(5, 7)), []byte("junk"), 0o644)
	os.WriteFile(filepath.Join(dir, "snapshots", snapName(5, 60)+".tmp"), []byte("junk"), 0o644)
	l.Close()

	l = open(t, dir, o)
	st, _ := l.Stats()
	if st.Recovery.OrphanFiles != 2 {
		t.Fatalf("orphans removed: %d", st.Recovery.OrphanFiles)
	}
	got, err := l.Group(5).Snapshot()
	if err != nil || got.Metadata.GetIndex() != 50 || !bytes.Equal(got.Data, big) {
		t.Fatalf("snapshot after reopen: %v %d", err, got.Metadata.GetIndex())
	}
	l.Close()

	// A damaged snapshot file is reported, never returned.
	p := filepath.Join(dir, "snapshots", snapName(5, 50))
	data, _ := os.ReadFile(p)
	data[100] ^= 1
	os.WriteFile(p, data, 0o644)
	l = open(t, dir, o)
	defer l.Close()
	if _, err := l.Group(5).Snapshot(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt snapshot file: %v", err)
	}
	if rep, _ := l.Scrub(); len(rep.Problems) == 0 {
		t.Fatal("scrub missed the corrupt snapshot file")
	}
}

// The tail of the newest segment: a cut-off write is discarded and
// reported as torn; non-zero bytes failing their checksum in the final
// commit may be bit rot in an acknowledged commit, so Open refuses them
// unless DiscardDamagedTail is set.
func TestTailForensics(t *testing.T) {
	build := func() (*crashfs.FS, int64) {
		mem := crashfs.New()
		l, err := Open(crashDir, memOpts(mem, Options{testDropIndexAfter: 1}))
		if err != nil {
			t.Fatal(err)
		}
		for i := uint64(1); i <= 4; i++ {
			l.Save(Update{Group: 1, Entries: ents(1, i*10-9, i*10, 300), HardState: &raftpb.HardState{Term: new(uint64(1)), Commit: new(i * 10)}})
		}
		l.crash()
		st, _ := mem.Stat(crashDir + "/segments/" + segName(1))
		return mem, st.Size()
	}
	flip := func(mem *crashfs.FS, off int64) {
		f, _ := mem.OpenReadWrite(crashDir + "/segments/" + segName(1))
		b := []byte{0}
		f.ReadAt(b, off)
		b[0] ^= 0x5a
		f.WriteAt(b, off)
		f.Close()
	}
	// Bit rot in the last commit (whose marker says nothing after the
	// previous commit was durable).
	mem, size := build()
	flip(mem, size-500)
	l, err := Open(crashDir, memOpts(mem, Options{DiscardDamagedTail: true}))
	if err != nil {
		t.Fatal(err)
	}
	st, _ := l.Stats()
	li, _ := l.Group(1).LastIndex()
	l.Close()
	if st.Recovery.Tail == nil || st.Recovery.Tail.TornLike || li != 30 {
		t.Fatalf("tail %+v, last %d", st.Recovery.Tail, li)
	}
	mem, size = build()
	flip(mem, size-500)
	if _, err := Open(crashDir, memOpts(mem, Options{})); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("default: %v", err)
	}
	// Bit rot in an earlier, unindexed commit is corruption either way: a
	// later marker vouches that it had been synced. (Damage in indexed
	// commits is caught when the entry is read, or by Scrub.)
	mem, _ = build()
	flip(mem, 4000)
	if _, err := Open(crashDir, memOpts(mem, Options{})); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("earlier commit damage: %v", err)
	}
	// A cut-off final write is torn-like and discarded by default.
	mem, size = build()
	f, _ := mem.OpenReadWrite(crashDir + "/segments/" + segName(1))
	f.WriteAt(make([]byte, 400), size-400)
	f.Close()
	l, err = Open(crashDir, memOpts(mem, Options{}))
	if err != nil {
		t.Fatal(err)
	}
	st, _ = l.Stats()
	l.Close()
	if st.Recovery.Tail == nil || !st.Recovery.Tail.TornLike {
		t.Fatalf("zeroed tail: %+v", st.Recovery.Tail)
	}
	// A single lost sector anywhere in the final commit is torn-like, even
	// when only a few bytes of a chunk, or part of its header, fell in it.
	_, size = build()
	for b := (size - 3000) &^ (tornSector - 1); b < size; b += tornSector {
		mem, _ := build()
		f, _ := mem.OpenReadWrite(crashDir + "/segments/" + segName(1))
		f.WriteAt(make([]byte, min(tornSector, size-b)), b)
		f.Close()
		l, err := Open(crashDir, memOpts(mem, Options{}))
		if err != nil {
			t.Fatalf("sector at %d lost: %v", b, err)
		}
		st, _ := l.Stats()
		li, _ := l.Group(1).LastIndex()
		l.Close()
		if li != 30 || st.Recovery.Tail == nil || !st.Recovery.Tail.TornLike {
			t.Fatalf("sector at %d lost: last %d, tail %+v", b, li, st.Recovery.Tail)
		}
	}
}
