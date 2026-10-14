// Copyright 2026 The raftseg authors.

package seglog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
)

// A quiet group pins the oldest segment; GC copies its live entries forward
// and deletes the segment.
func TestCleaningFreesPinnedSegments(t *testing.T) {
	dir := t.TempDir()
	o := Options{SegmentSize: 64 << 10, CleanAfterSegments: 2, NoAutoGC: true}
	l := open(t, dir, o)
	quiet := ents(1, 1, 3, 500)
	if err := l.Save(Update{Group: 1, Entries: quiet, HardState: raftpb.HardState{Term: 1, Commit: 3}}); err != nil {
		t.Fatal(err)
	}
	s2 := l.Group(2)
	for i := uint64(0); i < 60; i++ {
		if err := s2.Save(raftpb.HardState{Term: 1, Commit: i*5 + 5}, ents(1, i*5+1, i*5+5, 1500), raftpb.Snapshot{}); err != nil {
			t.Fatal(err)
		}
		if i > 2 {
			if err := s2.Compact(i*5 - 5); err != nil {
				t.Fatal(err)
			}
		}
	}
	segsBefore := l.SegmentCount()
	res, err := l.GC()
	if err != nil {
		t.Fatal(err)
	}
	if res.CleanedSegments == 0 || res.RemovedSegments == 0 || res.RelocatedEntries != 3 {
		t.Fatalf("GC result %+v with %d segments", res, segsBefore)
	}
	st, _ := l.Stats()
	for _, sg := range st.Segments {
		if sg.Sealed && sg.Segment+2 <= st.ActiveSegment && float64(sg.LiveEntries) <= 0.5*float64(sg.Entries) {
			t.Fatalf("segment %d still mostly dead after GC: %+v", sg.Segment, sg)
		}
	}
	l.Close()
	l = open(t, dir, Options{CacheEntries: -1})
	defer l.Close()
	eqEntries(t, "relocated group", mustEntries(t, l.Group(1), 1, 4), quiet)
	if rep, err := l.Scrub(); err != nil || len(rep.Problems) > 0 {
		t.Fatalf("scrub: %v %v", err, rep.Problems)
	}
}

// Group size no longer matters: a large group whose live entries are a small
// part of old segments is cleaned too, while fully-live segments are left
// alone (moving them would reclaim nothing).
func TestCleaningLargeGroupsAndLiveSegments(t *testing.T) {
	l := open(t, t.TempDir(), Options{SegmentSize: 256 << 10, CleanAfterSegments: 1, NoAutoGC: true})
	defer l.Close()
	big, churn := l.Group(1), l.Group(2)
	// Group 1 keeps 6 MiB live, written interleaved with churn that is
	// compacted away, so its old segments end up ~1/3 live.
	for i := uint64(0); i < 300; i++ {
		big.Save(raftpb.HardState{}, ents(1, i*2+1, i*2+2, 10<<10), raftpb.Snapshot{})
		churn.Save(raftpb.HardState{}, ents(1, i*4+1, i*4+4, 10<<10), raftpb.Snapshot{})
		if i > 0 {
			churn.Compact(i * 4)
		}
	}
	res, err := l.GC()
	if err != nil {
		t.Fatal(err)
	}
	if res.CleanedSegments == 0 || res.RemovedSegments == 0 {
		t.Fatalf("large group's mostly-dead segments not cleaned: %+v", res)
	}
	eqEntries(t, "big group", mustEntries(t, big, 1, 601), ents(1, 1, 600, 10<<10))

	// A log that is entirely live is never moved.
	l2 := open(t, t.TempDir(), Options{SegmentSize: 64 << 10, CleanAfterSegments: 1, NoAutoGC: true})
	defer l2.Close()
	for i := uint64(0); i < 40; i++ {
		l2.Save(Update{Group: 1, Entries: ents(1, i*4+1, i*4+4, 1500)})
	}
	if res, _ := l2.GC(); res.CleanedSegments != 0 || res.RemovedSegments != 0 {
		t.Fatalf("fully live log was cleaned: %+v", res)
	}
}

// Reopening resumes the newest segment instead of starting a new one.
func TestResumeNewestSegment(t *testing.T) {
	dir := t.TempDir()
	for i := uint64(0); i < 5; i++ {
		l := open(t, dir, Options{})
		if err := l.Save(Update{Group: 1, Entries: ents(1, i+1, i+1, 100)}); err != nil {
			t.Fatal(err)
		}
		if n := l.SegmentCount(); n != 1 {
			t.Fatalf("open %d: %d segments", i, n)
		}
		if st, _ := l.Stats(); i > 0 && !st.Recovery.ResumedSegment {
			t.Fatalf("open %d did not resume", i)
		}
		l.Close()
	}
	l := open(t, dir, Options{CacheEntries: -1})
	defer l.Close()
	eqEntries(t, "after resumes", mustEntries(t, l.Group(1), 1, 6), func() []raftpb.Entry {
		var all []raftpb.Entry
		for i := uint64(1); i <= 5; i++ {
			all = append(all, ents(1, i, i, 100)...)
		}
		return all
	}())
}

// Readers of a group racing with Saves that truncate and rewrite its tail
// must always see a consistent log: contiguous indexes, and every entry the
// one its index and term say.
func TestConcurrentReadsDuringSave(t *testing.T) {
	l := open(t, t.TempDir(), Options{SegmentSize: 128 << 10, CacheEntries: 8})
	defer l.Close()
	s := l.Group(1)
	if err := s.Save(raftpb.HardState{}, ents(1, 1, 20, 300), raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	var reads atomic.Int64
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				fi, _ := s.FirstIndex()
				li, _ := s.LastIndex()
				if fi > li {
					continue
				}
				got, err := s.Entries(fi, li+1, math.MaxUint64)
				if err != nil {
					if errors.Is(err, raft.ErrCompacted) || strings.Contains(err.Error(), "out of bound") {
						continue // bounds moved between calls
					}
					t.Errorf("read: %v", err)
					return
				}
				for k, e := range got {
					want := fmt.Sprintf("t%d-i%d:", e.Term, e.Index)
					if e.Index != fi+uint64(k) || !strings.HasPrefix(string(e.Data), want) {
						t.Errorf("inconsistent read: entry %d is (i%d t%d) %q", fi+uint64(k), e.Index, e.Term, e.Data[:len(want)])
						return
					}
					if k > 0 && e.Term < got[k-1].Term {
						t.Errorf("terms go backwards at %d", e.Index)
						return
					}
				}
				for i := fi; i <= li; i++ {
					if _, err := s.Term(i); err != nil && !errors.Is(err, raft.ErrCompacted) && !errors.Is(err, raft.ErrUnavailable) {
						t.Errorf("Term(%d): %v", i, err)
						return
					}
				}
				reads.Add(1)
			}
		}()
	}
	last, term := uint64(20), uint64(1)
	for i := 0; i < 300; i++ {
		start := last + 1
		if i%3 == 0 {
			start = last - 4 // overwrite a suffix with a higher term
			term++
		}
		if err := s.Save(raftpb.HardState{}, ents(term, start, start+5, 300), raftpb.Snapshot{}); err != nil {
			t.Fatal(err)
		}
		last = start + 5
		if i%50 == 49 {
			s.Compact(last - 30)
		}
	}
	stop.Store(true)
	wg.Wait()
	t.Logf("%d consistent reads during %d saves", reads.Load(), 300)
}

func TestRemoveGroup(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentSize: 64 << 10})
	old := l.Group(9)
	if err := old.Save(raftpb.HardState{Term: 1, Commit: 5}, ents(1, 1, 5, 100), raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	if err := l.RemoveGroup(9); err != nil {
		t.Fatal(err)
	}
	if _, err := old.LastIndex(); !errors.Is(err, ErrGroupRemoved) {
		t.Fatalf("old handle: %v", err)
	}
	if err := old.Save(raftpb.HardState{}, ents(1, 6, 6, 1), raftpb.Snapshot{}); !errors.Is(err, ErrGroupRemoved) {
		t.Fatalf("old handle save: %v", err)
	}
	// The id can be reused from scratch.
	fresh := l.Group(9)
	if li, _ := fresh.LastIndex(); li != 0 {
		t.Fatalf("reused id has last %d", li)
	}
	if err := fresh.Save(raftpb.HardState{}, ents(3, 1, 2, 10), raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	l.Close()
	l = open(t, dir, Options{CacheEntries: -1})
	defer l.Close()
	s := l.Group(9)
	eqEntries(t, "reused group", mustEntries(t, s, 1, 3), ents(3, 1, 2, 10))
	if hs, _, _ := s.InitialState(); !raft.IsEmptyHardState(hs) {
		t.Fatalf("hard state survived removal: %+v", hs)
	}
}

func TestReadsAfterClose(t *testing.T) {
	l := open(t, t.TempDir(), Options{CacheEntries: -1})
	s := l.Group(1)
	s.Save(raftpb.HardState{}, ents(1, 1, 3, 10), raftpb.Snapshot{})
	l.Close()
	if _, err := s.Entries(1, 4, math.MaxUint64); !errors.Is(err, ErrClosed) {
		t.Fatalf("Entries after Close: %v", err)
	}
	if _, err := s.Term(2); !errors.Is(err, ErrClosed) {
		t.Fatalf("Term after Close: %v", err)
	}
	if err := l.Save(Update{Group: 1}); err != nil && !errors.Is(err, ErrClosed) {
		t.Fatalf("Save after Close: %v", err)
	}
	if err := s.Save(raftpb.HardState{Term: 1}, nil, raftpb.Snapshot{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Save after Close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestSecondOpenRejected(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})
	defer l.Close()
	if l2, err := Open(dir, Options{}); err == nil {
		l2.Close()
		t.Fatal("second Open of the same directory succeeded")
	}
}

func TestScrub(t *testing.T) {
	dir := t.TempDir()
	o := Options{SegmentSize: 64 << 10, NoAutoGC: true}
	l := open(t, dir, o)
	for i := uint64(0); i < 30; i++ {
		l.Save(Update{Group: i%3 + 1, Entries: ents(1, i/3*4+1, i/3*4+4, 1200), HardState: raftpb.HardState{Term: 1}})
	}
	rep, err := l.Scrub()
	if err != nil || len(rep.Problems) > 0 || rep.Commits != 30 || rep.IndexedEntries != 120 {
		t.Fatalf("clean scrub: %+v %v", rep, err)
	}
	l.Close()
	segs, _ := listSegments(OSFS, filepath.Join(dir, "segments"))
	p := filepath.Join(dir, "segments", segName(segs[0]))
	data, _ := os.ReadFile(p)
	data[5000] ^= 0x10
	os.WriteFile(p, data, 0o644)
	l = open(t, dir, o)
	rep, _ = l.Scrub()
	if len(rep.Problems) == 0 {
		t.Fatal("scrub missed a flipped bit in a sealed segment")
	}
	l.Close()
	if _, err := Open(dir, Options{ScrubOnOpen: true}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ScrubOnOpen: %v", err)
	}
}

// I/O errors make the log fail closed; reopening recovers every
// acknowledged write.
func TestFaultInjection(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind crashfs.OpKind
	}{
		{"write", crashfs.OpWriteAt},
		{"syncdata", crashfs.OpSyncData},
		{"create", crashfs.OpCreate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := crashfs.New()
			var armed atomic.Bool
			fs := crashfs.WithHook(mem, func(op crashfs.Op) error {
				if armed.Load() && op.Kind == tc.kind && strings.Contains(op.Path, "/segments/") {
					return crashfs.ErrInjected
				}
				return nil
			})
			o := memOpts(fs, Options{SegmentSize: 64 << 10})
			l, err := Open(crashDir, o)
			if err != nil {
				t.Fatal(err)
			}
			s := l.Group(1)
			var acked uint64
			var failedAt int
			for i := 0; i < 200; i++ {
				if i == 50 {
					armed.Store(true)
				}
				lo := uint64(i*3 + 1)
				err := s.Save(raftpb.HardState{Term: 1, Commit: lo + 2}, ents(1, lo, lo+2, 2000), raftpb.Snapshot{})
				if err != nil {
					failedAt = i
					break
				}
				acked = lo + 2
			}
			if failedAt == 0 {
				t.Fatal("injected fault never surfaced")
			}
			// The failure is sticky.
			if err := s.Save(raftpb.HardState{}, ents(1, acked+1, acked+1, 1), raftpb.Snapshot{}); err == nil {
				t.Fatal("Save succeeded after a fatal I/O error")
			}
			if _, err := l.GC(); err == nil {
				t.Fatal("GC succeeded after a fatal I/O error")
			}
			l.Close()
			armed.Store(false)
			l, err = Open(crashDir, o)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer l.Close()
			s = l.Group(1)
			li, _ := s.LastIndex()
			if li < acked {
				t.Fatalf("acknowledged entries lost: last %d, acked %d", li, acked)
			}
			got := mustEntries(t, s, 1, acked+1)
			eqEntries(t, "acked", got, ents(1, 1, acked, 2000))
			if err := s.Save(raftpb.HardState{}, ents(1, li+1, li+1, 10), raftpb.Snapshot{}); err != nil {
				t.Fatalf("save after recovery: %v", err)
			}
		})
	}
}

// Many groups with bounded cache memory.
func TestCacheBudget(t *testing.T) {
	l := open(t, t.TempDir(), Options{CacheEntries: 1000, CacheBytes: 256 << 10})
	defer l.Close()
	for g := uint64(1); g <= 50; g++ {
		if err := l.Save(Update{Group: g, Entries: ents(1, 1, 40, 1000)}); err != nil {
			t.Fatal(err)
		}
	}
	st, _ := l.Stats()
	if st.CacheBytes > 256<<10 {
		t.Fatalf("cache %d bytes over budget", st.CacheBytes)
	}
	for g := uint64(1); g <= 50; g++ {
		eqEntries(t, "group", mustEntries(t, l.Group(g), 1, 41), ents(1, 1, 40, 1000))
	}
}

// The index is checkpointed, so replay after losing it is bounded.
func TestCheckpointInterval(t *testing.T) {
	l := open(t, t.TempDir(), Options{CheckpointInterval: time.Millisecond})
	defer l.Close()
	for i := uint64(0); i < 20; i++ {
		time.Sleep(2 * time.Millisecond)
		l.Save(Update{Group: 1, Entries: ents(1, i+1, i+1, 10)})
	}
	if st, _ := l.Stats(); st.Checkpoints < 10 {
		t.Fatalf("only %d checkpoints", st.Checkpoints)
	}
}

func TestFormatVersionChecked(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})
	l.Save(Update{Group: 1, Entries: ents(1, 1, 1, 10)})
	l.Close()
	// A checkpoint of another format version (with a valid checksum).
	for _, name := range ckptNames {
		p := filepath.Join(dir, name)
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		body := data[:len(data)-4]
		body[4] = 3 // the format version varint
		data = binary.LittleEndian.AppendUint32(body, crc32.Checksum(body, crcTable))
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Open(dir, Options{}); err == nil || !strings.Contains(err.Error(), "format") {
		t.Fatalf("expected format error, got %v", err)
	}
	// A directory written by v3 (a key-value index, no checkpoint).
	v3 := t.TempDir()
	os.MkdirAll(filepath.Join(v3, "index"), 0o755)
	if _, err := Open(v3, Options{}); err == nil || !strings.Contains(err.Error(), "format 3") {
		t.Fatalf("expected a v3 format error, got %v", err)
	}
}

// Queues whose consumers keep up empty their segments in order. GC must let
// those segments die instead of copying entries that are about to be
// compacted, while still freeing a segment pinned by a quiet group.
func TestCleaningSkipsDecayingSegments(t *testing.T) {
	l := open(t, t.TempDir(), Options{SegmentSize: 64 << 10, CleanAfterSegments: 2})
	defer l.Close()
	quiet := ents(1, 1, 3, 300)
	if err := l.Save(Update{Group: 99, Entries: quiet, HardState: raftpb.HardState{Term: 1, Commit: 3}}); err != nil {
		t.Fatal(err)
	}
	// 8 queues keep their newest 32 entries of 512 B: 128 KiB live, the same
	// as CleanAfterSegments × SegmentSize, the case that invites cleaning.
	const groups, keep = 8, 32
	var next [groups + 1]uint64
	written := 0
	for i := 0; i < 2000; i++ {
		g := uint64(1 + i%groups)
		lo := next[g] + 1
		if err := l.Group(g).Save(raftpb.HardState{Term: 1, Commit: lo + 1}, ents(1, lo, lo+1, 512), raftpb.Snapshot{}); err != nil {
			t.Fatal(err)
		}
		next[g] += 2
		written += 2
		if next[g] > keep {
			if err := l.Group(g).Compact(next[g] - keep); err != nil {
				t.Fatal(err)
			}
		}
	}
	st, _ := l.Stats()
	if float64(st.RelocatedEntries) > 0.05*float64(written) {
		t.Fatalf("GC relocated %d entries of %d written: it is copying entries that were about to die", st.RelocatedEntries, written)
	}
	// The quiet group's segment stops changing and is cleaned.
	for i := 0; i < 2; i++ {
		if _, err := l.GC(); err != nil {
			t.Fatal(err)
		}
	}
	st, _ = l.Stats()
	if st.Segments[0].Segment == 1 {
		t.Fatalf("segment 1, pinned by a quiet group, was never cleaned: %+v", st.Segments[0])
	}
	eqEntries(t, "quiet group", mustEntries(t, l.Group(99), 1, 4), quiet)
	t.Logf("written %d entries, relocated %d, %d segments left", written, st.RelocatedEntries, len(st.Segments))
}
