// Copyright 2026 The raftseg authors.

package seglog

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
)

func open(t testing.TB, dir string, o Options) *Log {
	t.Helper()
	l, err := Open(dir, o)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return l
}

func ents(term, lo, hi uint64, size int) []raftpb.Entry {
	var out []raftpb.Entry
	for i := lo; i <= hi; i++ {
		data := bytes.Repeat([]byte{byte(i)}, size)
		copy(data, fmt.Sprintf("t%d-i%d:", term, i))
		out = append(out, raftpb.Entry{Term: term, Index: i, Data: data})
	}
	return out
}

func mustEntries(t testing.TB, s raft.Storage, lo, hi uint64) []raftpb.Entry {
	t.Helper()
	got, err := s.Entries(lo, hi, math.MaxUint64)
	if err != nil {
		t.Fatalf("Entries(%d,%d): %v", lo, hi, err)
	}
	return got
}

func eqEntries(t testing.TB, what string, got, want []raftpb.Entry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d entries, want %d", what, len(got), len(want))
	}
	for i := range got {
		if got[i].Index != want[i].Index || got[i].Term != want[i].Term || !bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("%s: entry %d: got (i%d t%d %dB) want (i%d t%d %dB)", what, i,
				got[i].Index, got[i].Term, len(got[i].Data), want[i].Index, want[i].Term, len(want[i].Data))
		}
	}
}

// Entries larger than a 32 KiB block, interleaved groups, segment rolls,
// reads from disk (cache disabled) and from a reopened log.
func TestAppendReadReopen(t *testing.T) {
	dir := t.TempDir()
	o := Options{SegmentSize: 256 << 10, CacheEntries: -1}
	l := open(t, dir, o)
	want := map[uint64][]raftpb.Entry{}
	for round := uint64(0); round < 20; round++ {
		var ups []Update
		for g := uint64(1); g <= 4; g++ {
			size := 100
			if g == 4 {
				size = 70 << 10 // spans several blocks
			}
			e := ents(1, round*3+1, round*3+3, size)
			want[g] = append(want[g], e...)
			ups = append(ups, Update{Group: g, Entries: e, HardState: raftpb.HardState{Term: 1, Commit: round*3 + 3}})
		}
		if err := l.Save(ups...); err != nil {
			t.Fatal(err)
		}
	}
	if l.SegmentCount() < 3 {
		t.Fatalf("expected segment rolls, have %d segments", l.SegmentCount())
	}
	check := func(l *Log) {
		for g, w := range want {
			s := l.Group(g)
			eqEntries(t, fmt.Sprintf("group %d", g), mustEntries(t, s, 1, 61), w)
			eqEntries(t, fmt.Sprintf("group %d middle", g), mustEntries(t, s, 17, 23), w[16:22])
			if term, err := s.Term(60); err != nil || term != 1 {
				t.Fatalf("Term(60) = %d, %v", term, err)
			}
			if hs, _, _ := s.InitialState(); hs.Commit != 60 {
				t.Fatalf("group %d commit %d", g, hs.Commit)
			}
		}
	}
	check(l)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l = open(t, dir, o)
	defer l.Close()
	check(l)
}

func TestSizeLimit(t *testing.T) {
	l := open(t, t.TempDir(), Options{CacheEntries: -1})
	defer l.Close()
	e := ents(1, 1, 10, 100)
	if err := l.Save(Update{Group: 1, Entries: e}); err != nil {
		t.Fatal(err)
	}
	s := l.Group(1)
	one := uint64(e[0].Size())
	for _, tc := range []struct{ max, n uint64 }{{0, 1}, {one, 1}, {one*3 + 1, 3}, {math.MaxUint64, 10}} {
		got, err := s.Entries(1, 11, tc.max)
		if err != nil || uint64(len(got)) != tc.n {
			t.Fatalf("max %d: got %d entries (%v), want %d", tc.max, len(got), err, tc.n)
		}
	}
}

func TestTruncateSuffix(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})
	s := l.Group(7)
	if err := s.Save(raftpb.HardState{}, ents(1, 1, 10, 10), raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(raftpb.HardState{}, ents(2, 5, 7, 10), raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	want := append(ents(1, 1, 4, 10), ents(2, 5, 7, 10)...)
	if li, _ := s.LastIndex(); li != 7 {
		t.Fatalf("last %d", li)
	}
	eqEntries(t, "after truncate", mustEntries(t, s, 1, 8), want)
	// A gap must be rejected without writing anything.
	if err := s.Save(raftpb.HardState{}, ents(2, 9, 9, 10), raftpb.Snapshot{}); err == nil {
		t.Fatal("expected gap error")
	}
	l.Close()
	l = open(t, dir, Options{CacheEntries: -1})
	defer l.Close()
	eqEntries(t, "after reopen", mustEntries(t, l.Group(7), 1, 8), want)
	if _, err := l.Group(7).Term(8); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("Term(8) err %v", err)
	}
}

// The index (checkpoints and index sidecars) stops being written while the
// segments stay durable:
// Open must rebuild entries, truncations and HardStates from the segments.
func TestReplayAfterIndexLoss(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentSize: 64 << 10, testDropIndexAfter: 2})
	model := map[uint64]*raft.MemoryStorage{1: raft.NewMemoryStorage(), 2: raft.NewMemoryStorage()}
	hard := map[uint64]raftpb.HardState{}
	step := func(g uint64, e []raftpb.Entry, commit uint64) {
		hs := raftpb.HardState{Term: e[0].Term, Vote: 1, Commit: commit}
		if err := l.Save(Update{Group: g, Entries: e, HardState: hs}); err != nil {
			t.Fatal(err)
		}
		model[g].Append(e)
		hard[g] = hs
	}
	step(1, ents(1, 1, 5, 2000), 2)
	step(2, ents(1, 1, 3, 2000), 1)
	// Everything below is only in the segments.
	step(1, ents(1, 6, 40, 2000), 10) // rolls segments
	step(1, ents(2, 20, 25, 2000), 18)
	step(2, ents(3, 2, 30, 2000), 4)
	l.crash()

	l = open(t, dir, Options{SegmentSize: 64 << 10, CacheEntries: -1})
	defer l.Close()
	for g, m := range model {
		compareStorage(t, fmt.Sprintf("group %d", g), l.Group(g), m)
		if hs, _, _ := l.Group(g).InitialState(); hs != hard[g] {
			t.Fatalf("group %d hardstate %+v want %+v", g, hs, hard[g])
		}
	}
	// The recovered log keeps working.
	if err := l.Save(Update{Group: 1, Entries: ents(2, 26, 26, 10)}); err != nil {
		t.Fatal(err)
	}
}

// A torn write at the tail of the newest segment is discarded; the segment is
// then sealed, and a later reopen reads it strictly.
func TestTornTail(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{testDropIndexAfter: 1})
	if err := l.Save(Update{Group: 1, Entries: ents(1, 1, 3, 500)}); err != nil {
		t.Fatal(err)
	}
	if err := l.Save(Update{Group: 1, Entries: ents(1, 4, 6, 500)}); err != nil {
		t.Fatal(err)
	}
	if err := l.Save(Update{Group: 1, Entries: ents(1, 7, 7, 50000)}); err != nil {
		t.Fatal(err)
	}
	active := l.active
	l.crash()
	// Cut the last record in half, as if the write was interrupted.
	p := filepath.Join(dir, "segments", segName(active))
	fi, _ := os.Stat(p)
	if err := os.Truncate(p, fi.Size()-30000); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		l = open(t, dir, Options{CacheEntries: -1})
		s := l.Group(1)
		if li, _ := s.LastIndex(); li != 6 {
			t.Fatalf("reopen %d: last %d, want 6", i, li)
		}
		eqEntries(t, "survivors", mustEntries(t, s, 1, 7), ents(1, 1, 6, 500))
		l.Close()
	}
	l = open(t, dir, Options{})
	defer l.Close()
	if err := l.Save(Update{Group: 1, Entries: ents(1, 7, 8, 10)}); err != nil {
		t.Fatal(err)
	}
}

// Corruption inside a segment that is not the newest is an error, never a
// silent truncation.
func TestCorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentSize: 64 << 10, testDropIndexAfter: 1})
	for i := uint64(0); i < 20; i++ {
		if err := l.Save(Update{Group: 1, Entries: ents(1, i*4+1, i*4+4, 3000)}); err != nil {
			t.Fatal(err)
		}
	}
	l.crash()
	segs, _ := listSegments(OSFS, filepath.Join(dir, "segments"))
	if len(segs) < 3 {
		t.Fatalf("want >= 3 segments, got %d", len(segs))
	}
	p := filepath.Join(dir, "segments", segName(segs[1]))
	data, _ := os.ReadFile(p)
	data[len(data)/2] ^= 0xff
	os.WriteFile(p, data, 0o644)
	if _, err := Open(dir, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected corruption error, got %v", err)
	}
}

func TestReadDetectsBitFlip(t *testing.T) {
	dir := t.TempDir()
	o := Options{CacheEntries: -1, SegmentSize: 16 << 10}
	l := open(t, dir, o)
	for i := uint64(0); i < 4; i++ {
		if err := l.Save(Update{Group: 1, Entries: ents(1, i*5+1, i*5+5, 1000)}); err != nil {
			t.Fatal(err)
		}
	}
	l.Close()
	// Sealed segments are not rescanned at open; the CRC catches the flip
	// when the entry is read.
	segs, _ := listSegments(OSFS, filepath.Join(dir, "segments"))
	p := filepath.Join(dir, "segments", segName(segs[0]))
	data, _ := os.ReadFile(p)
	data[3*1000] ^= 0x01
	os.WriteFile(p, data, 0o644)
	l = open(t, dir, o)
	defer l.Close()
	if _, err := l.Group(1).Entries(1, 21, math.MaxUint64); err == nil {
		t.Fatal("expected checksum error reading corrupted entry")
	}
	// Entries in intact segments are still readable.
	eqEntries(t, "intact", mustEntries(t, l.Group(1), 16, 21), ents(1, 16, 20, 1000))
}

func TestCompactSnapshotGC(t *testing.T) {
	dir := t.TempDir()
	// Relocation and automatic GC off: this test is about pinning.
	o := Options{SegmentSize: 32 << 10, CacheEntries: 8, CleanMaxLiveRatio: -1, NoAutoGC: true}
	l := open(t, dir, o)
	for i := uint64(0); i < 30; i++ {
		var ups []Update
		for g := uint64(1); g <= 3; g++ {
			ups = append(ups, Update{Group: g, Entries: ents(1, i*5+1, i*5+5, 600)})
		}
		if err := l.Save(ups...); err != nil {
			t.Fatal(err)
		}
	}
	before := l.SegmentCount()
	// Group 3 lags: nothing can be deleted until it compacts too.
	for g := uint64(1); g <= 2; g++ {
		s := l.Group(g)
		if _, err := s.CreateSnapshot(140, &raftpb.ConfState{Voters: []uint64{1}}, []byte("state")); err != nil {
			t.Fatal(err)
		}
		if err := s.Compact(140); err != nil {
			t.Fatal(err)
		}
	}
	if res, _ := l.GC(); res.RemovedSegments != 0 {
		t.Fatalf("GC removed %d segments while group 3 still needs them", res.RemovedSegments)
	}
	if err := l.Group(3).Compact(140); err != nil {
		t.Fatal(err)
	}
	res, err := l.GC()
	n := res.RemovedSegments
	if err != nil || n == 0 {
		t.Fatalf("GC removed %d (%v)", n, err)
	}
	if l.SegmentCount() != before-n {
		t.Fatalf("segment count %d, want %d", l.SegmentCount(), before-n)
	}
	l.Close()
	l = open(t, dir, Options{SegmentSize: 32 << 10, CacheEntries: -1, CleanMaxLiveRatio: -1, NoAutoGC: true})
	defer l.Close()
	for g := uint64(1); g <= 3; g++ {
		s := l.Group(g)
		if fi, _ := s.FirstIndex(); fi != 141 {
			t.Fatalf("group %d first %d", g, fi)
		}
		if term, err := s.Term(140); err != nil || term != 1 {
			t.Fatalf("Term(140) = %d, %v", term, err)
		}
		if _, err := s.Term(139); !errors.Is(err, raft.ErrCompacted) {
			t.Fatalf("Term(139) err %v", err)
		}
		if _, err := s.Entries(140, 150, math.MaxUint64); !errors.Is(err, raft.ErrCompacted) {
			t.Fatalf("Entries(140) err %v", err)
		}
		eqEntries(t, "retained", mustEntries(t, s, 141, 151), ents(1, 141, 150, 600))
	}
	snap, _ := l.Group(1).Snapshot()
	if snap.Metadata.Index != 140 || string(snap.Data) != "state" {
		t.Fatalf("snapshot %+v", snap.Metadata)
	}
}

func TestApplySnapshot(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})
	s := l.Group(1)
	if err := s.Save(raftpb.HardState{}, ents(1, 1, 5, 10), raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	snap := raftpb.Snapshot{Data: []byte("sm"), Metadata: raftpb.SnapshotMetadata{Index: 100, Term: 4, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}}
	// Snapshot and the entries that follow it arrive in the same Ready.
	if err := s.Save(raftpb.HardState{Term: 4, Commit: 100}, ents(4, 101, 102, 10), snap); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(raftpb.HardState{}, nil, snap); !errors.Is(err, raft.ErrSnapOutOfDate) {
		t.Fatalf("stale snapshot err %v", err)
	}
	l.Close()
	l = open(t, dir, Options{CacheEntries: -1})
	defer l.Close()
	s = l.Group(1)
	fi, _ := s.FirstIndex()
	li, _ := s.LastIndex()
	_, cs, _ := s.InitialState()
	if fi != 101 || li != 102 || len(cs.Voters) != 3 {
		t.Fatalf("first %d last %d cs %+v", fi, li, cs)
	}
	if term, _ := s.Term(100); term != 4 {
		t.Fatalf("Term(100) = %d", term)
	}
	eqEntries(t, "after snapshot", mustEntries(t, s, 101, 103), ents(4, 101, 102, 10))
}

// Concurrent Saves from many goroutines (one per group, like a multi-raft
// scheduler) are coalesced into far fewer segment fsyncs.
func TestGroupCommitConcurrent(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentSize: 1 << 20})
	const groups, saves = 32, 40
	errs := make(chan error, groups)
	for g := uint64(1); g <= groups; g++ {
		go func(g uint64) {
			s := l.Group(g)
			for i := uint64(0); i < saves; i++ {
				e := ents(1, i*2+1, i*2+2, 200)
				if err := s.Save(raftpb.HardState{Term: 1, Commit: i * 2}, e, raftpb.Snapshot{}); err != nil {
					errs <- err
					return
				}
				// Read back our own group while others write.
				if got, err := s.Entries(i*2+1, i*2+3, math.MaxUint64); err != nil || len(got) != 2 {
					errs <- fmt.Errorf("group %d read: %d entries, %v", g, len(got), err)
					return
				}
			}
			errs <- nil
		}(g)
	}
	for i := 0; i < groups; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	commits := l.commits.Load()
	if commits >= groups*saves {
		t.Fatalf("no coalescing: %d commits for %d saves", commits, groups*saves)
	}
	t.Logf("%d saves -> %d group commits (fsyncs)", groups*saves, commits)
	l.Close()
	l = open(t, dir, Options{CacheEntries: -1})
	defer l.Close()
	for g := uint64(1); g <= groups; g++ {
		if li, _ := l.Group(g).LastIndex(); li != saves*2 {
			t.Fatalf("group %d last %d", g, li)
		}
	}
}

// Per-segment entry counts (which drive cleaning) are rebuilt exactly when
// the index lost them and the segments are replayed.
func TestSegmentEntryCountsReplayed(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentSize: 64 << 10, testDropIndexAfter: 1, NoAutoGC: true})
	for i := uint64(0); i < 200; i++ {
		u := []Update{{Group: 1 + i%3, Entries: ents(1, i/3+1, i/3+1, 700), HardState: raftpb.HardState{Term: 1, Commit: i/3 + 1}}}
		if i%7 == 0 {
			u = append(u, Update{Group: 9, HardState: raftpb.HardState{Term: 2, Vote: 1}})
		}
		if err := l.Save(u...); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := l.Stats()
	l.crash()
	l = open(t, dir, Options{SegmentSize: 64 << 10, NoAutoGC: true})
	defer l.Close()
	after, _ := l.Stats()
	if after.Recovery.ReplayedCommits == 0 || len(before.Segments) < 3 {
		t.Fatalf("test did not exercise replay: %+v, %d segments", after.Recovery, len(before.Segments))
	}
	var total uint64
	for i, s := range before.Segments {
		a := after.Segments[i]
		if a.Segment != s.Segment || a.Entries != s.Entries || a.Records != s.Records {
			t.Fatalf("segment %d: before crash %+v, after replay %+v", s.Segment, s, a)
		}
		total += a.Entries
	}
	if total != 200 {
		t.Fatalf("segments hold %d entries, want 200", total)
	}
}
