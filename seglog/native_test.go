// Copyright 2026 The raftseg authors.

package seglog

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	randv2 "math/rand/v2"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
	"github.com/raftseg/raftseg/vfs"
)

// Tests of the native index: checkpoints, segment index sidecars, the index
// cache, and replay of every record type.

// fillLog writes groups*rounds Saves of 4 entries across many small segments.
func fillLog(t *testing.T, l *Log, groups, rounds uint64, size int) {
	t.Helper()
	for i := uint64(0); i < rounds; i++ {
		var ups []Update
		for g := uint64(1); g <= groups; g++ {
			ups = append(ups, Update{Group: g, Entries: ents(1, i*4+1, i*4+4, size), HardState: raftpb.HardState{Term: 1, Commit: i*4 + 4}})
		}
		if err := l.Save(ups...); err != nil {
			t.Fatal(err)
		}
	}
}

func checkFilled(t *testing.T, l *Log, groups, rounds uint64, size int) {
	t.Helper()
	for g := uint64(1); g <= groups; g++ {
		eqEntries(t, fmt.Sprintf("group %d", g), mustEntries(t, l.Group(g), 1, rounds*4+1), ents(1, 1, rounds*4, size))
	}
}

// Either checkpoint copy alone is enough; with both damaged Open refuses.
func TestCheckpointCopies(t *testing.T) {
	o := Options{SegmentSize: 64 << 10}
	for _, damage := range [][]string{{"CHECKPOINT-A"}, {"CHECKPOINT-B"}, {"CHECKPOINT-A", "CHECKPOINT-B"}} {
		dir := t.TempDir()
		l := open(t, dir, o)
		fillLog(t, l, 3, 30, 700)
		l.Close()
		for _, name := range damage {
			p := filepath.Join(dir, name)
			data, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			data[len(data)/2] ^= 0x40
			os.WriteFile(p, data, 0o644)
		}
		l, err := Open(dir, o)
		if len(damage) == 2 {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("both copies damaged: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%v damaged: %v", damage, err)
		}
		st, _ := l.Stats()
		if len(st.Recovery.Repairs) == 0 {
			t.Fatalf("%v damaged: not reported", damage)
		}
		checkFilled(t, l, 3, 30, 700)
		l.Close()
	}
}

// An older checkpoint is a valid starting point: Open replays everything
// after it, across the segments sealed since.
func TestOlderCheckpointReplays(t *testing.T) {
	dir := t.TempDir()
	o := Options{SegmentSize: 64 << 10}
	l := open(t, dir, o)
	fillLog(t, l, 2, 10, 700)
	l.Close()
	saved := map[string][]byte{}
	for _, name := range ckptNames {
		saved[name], _ = os.ReadFile(filepath.Join(dir, name))
	}
	l = open(t, dir, o)
	for i := uint64(10); i < 40; i++ {
		for g := uint64(1); g <= 2; g++ {
			if err := l.Group(g).Save(raftpb.HardState{Term: 1, Commit: i*4 + 4}, ents(1, i*4+1, i*4+4, 700), raftpb.Snapshot{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	l.crash()
	for name, data := range saved {
		os.WriteFile(filepath.Join(dir, name), data, 0o644)
	}
	l = open(t, dir, o)
	defer l.Close()
	st, _ := l.Stats()
	if st.Recovery.ReplayedCommits < 30 {
		t.Fatalf("replayed only %d commits", st.Recovery.ReplayedCommits)
	}
	checkFilled(t, l, 2, 40, 700)
}

// Sidecars are caches: missing ones are rebuilt at Open, damaged ones when
// loaded, and Scrub reports damage.
func TestSidecarRebuild(t *testing.T) {
	dir := t.TempDir()
	o := Options{SegmentSize: 64 << 10, CacheEntries: -1, IndexCacheBytes: 1}
	l := open(t, dir, o)
	fillLog(t, l, 3, 40, 900)
	l.Close()
	idx, _ := listNumbered(OSFS, filepath.Join(dir, "segments"), idxSuffix)
	if len(idx) < 4 {
		t.Fatalf("only %d sidecars", len(idx))
	}
	os.Remove(filepath.Join(dir, "segments", idxName(idx[0])))
	p := filepath.Join(dir, "segments", idxName(idx[1]))
	data, _ := os.ReadFile(p)
	data[len(data)/2] ^= 1
	os.WriteFile(p, data, 0o644)

	l = open(t, dir, o)
	st, _ := l.Stats()
	if st.Recovery.RebuiltIndexes != 1 {
		t.Fatalf("rebuilt %d sidecars at open, want 1", st.Recovery.RebuiltIndexes)
	}
	checkFilled(t, l, 3, 40, 900)
	if st, _ = l.Stats(); st.IndexRebuilds == 0 {
		t.Fatal("the damaged sidecar was not rebuilt when loaded")
	}
	rep, err := l.Scrub()
	if err != nil || len(rep.Problems) != 1 {
		t.Fatalf("scrub: %v %v", err, rep.Problems)
	}
	l.Close()
}

// With a tiny index cache, every read loads a sidecar; reads stay correct
// and the cache stays within its budget.
func TestIndexCacheBudget(t *testing.T) {
	dir := t.TempDir()
	o := Options{SegmentSize: 64 << 10, CacheEntries: -1, IndexCacheBytes: 4 << 10}
	l := open(t, dir, o)
	defer l.Close()
	fillLog(t, l, 4, 50, 800)
	rng := rand.New(rand.NewSource(1))
	for k := 0; k < 300; k++ {
		g := uint64(1 + rng.Intn(4))
		i := uint64(1 + rng.Intn(200))
		es := mustEntries(t, l.Group(g), i, i+1)
		eqEntries(t, "random read", es, ents(1, i, i, 800))
	}
	st, _ := l.Stats()
	if st.IndexCacheBytes > 4<<10 {
		t.Fatalf("index cache holds %d bytes", st.IndexCacheBytes)
	}
}

// Every record type is replayed when no checkpoint covers it: snapshots
// (inline and in files, received and created), conf states, compaction
// below a created snapshot, truncations and group removal.
func TestReplayAllRecordTypes(t *testing.T) {
	dir := t.TempDir()
	o := Options{SegmentSize: 64 << 10, SnapshotInlineBytes: 1 << 10, testDropIndexAfter: 1}
	l := open(t, dir, o)
	s1, s2, s3 := l.Group(1), l.Group(2), l.Group(3)
	for _, s := range []*Storage{s1, s2, s3} {
		if err := s.Save(raftpb.HardState{Term: 1, Commit: 20}, ents(1, 1, 20, 300), raftpb.Snapshot{}); err != nil {
			t.Fatal(err)
		}
	}
	big := bytes.Repeat([]byte("snapshot"), 1000)
	if _, err := s1.CreateSnapshot(10, &raftpb.ConfState{Voters: []uint64{1, 2}}, big); err != nil {
		t.Fatal(err)
	}
	if _, err := s1.CreateSnapshot(12, nil, []byte("small")); err != nil {
		t.Fatal(err)
	}
	cs := raftpb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}
	if err := s2.SetConfState(cs); err != nil {
		t.Fatal(err)
	}
	// Truncate group 2 at 15 with a new term.
	if err := s2.Save(raftpb.HardState{Term: 2, Vote: 3, Commit: 14}, ents(2, 15, 17, 300), raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	recv := raftpb.Snapshot{Data: big, Metadata: raftpb.SnapshotMetadata{Index: 50, Term: 3, ConfState: raftpb.ConfState{Voters: []uint64{7}}}}
	if err := s3.Save(raftpb.HardState{Term: 3, Commit: 50}, ents(3, 51, 52, 300), recv); err != nil {
		t.Fatal(err)
	}
	if err := l.RemoveGroup(1); err != nil {
		t.Fatal(err)
	}
	s1 = l.Group(1) // the id is reused
	if err := s1.Save(raftpb.HardState{Term: 9, Commit: 1}, ents(9, 1, 1, 10), raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	want := map[uint64]func(*Storage){
		1: func(s *Storage) {
			if hs := s.HardState(); hs.Term != 9 {
				t.Fatalf("group 1 hard state %+v", hs)
			}
			if snap, _ := s.Snapshot(); !raft.IsEmptySnap(snap) {
				t.Fatalf("removed group's snapshot came back: %+v", snap.Metadata)
			}
			eqEntries(t, "reused group", mustEntries(t, s, 1, 2), ents(9, 1, 1, 10))
		},
		2: func(s *Storage) {
			_, gotCS, _ := s.InitialState()
			if !confStateEq(gotCS, cs) {
				t.Fatalf("group 2 conf state %+v", gotCS)
			}
			if li, _ := s.LastIndex(); li != 17 {
				t.Fatalf("group 2 last %d", li)
			}
			eqEntries(t, "group 2", mustEntries(t, s, 1, 18), append(ents(1, 1, 14, 300), ents(2, 15, 17, 300)...))
			if term, _ := s.Term(16); term != 2 {
				t.Fatalf("group 2 term(16) = %d", term)
			}
		},
		3: func(s *Storage) {
			snap, err := s.Snapshot()
			if err != nil || snap.Metadata.Index != 50 || !bytes.Equal(snap.Data, big) {
				t.Fatalf("group 3 snapshot: %v %+v", err, snap.Metadata)
			}
			if fi, _ := s.FirstIndex(); fi != 51 {
				t.Fatalf("group 3 first %d", fi)
			}
			if term, _ := s.Term(50); term != 3 {
				t.Fatalf("group 3 term(50) = %d", term)
			}
			eqEntries(t, "group 3", mustEntries(t, s, 51, 53), ents(3, 51, 52, 300))
		},
	}
	for g, check := range want {
		check(l.Group(g))
	}
	l.crash()
	l = open(t, dir, Options{SnapshotInlineBytes: 1 << 10, ScrubOnOpen: true})
	defer l.Close()
	if st, _ := l.Stats(); st.Recovery.ReplayedCommits < 8 {
		t.Fatalf("replayed %d commits", st.Recovery.ReplayedCommits)
	}
	for g, check := range want {
		check(l.Group(g))
	}
}

func confStateEq(a, b raftpb.ConfState) bool {
	x, _ := a.Marshal()
	y, _ := b.Marshal()
	return bytes.Equal(x, y)
}

// Damage in data a checkpoint covers does not stop Open: everything else is
// indexed, the damaged entries fail to read, and the damage is reported.
func TestDamageInCheckpointedData(t *testing.T) {
	dir := t.TempDir()
	o := Options{CacheEntries: -1}
	l := open(t, dir, o)
	fillLog(t, l, 1, 20, 1000)
	l.Close()
	// Remove the active segment's sidecar so Open rescans what the
	// checkpoint covers.
	segs, _ := listSegments(OSFS, filepath.Join(dir, "segments"))
	os.Remove(filepath.Join(dir, "segments", idxName(segs[len(segs)-1])))
	p := filepath.Join(dir, "segments", segName(segs[len(segs)-1]))
	data, _ := os.ReadFile(p)
	data[30000] ^= 0x20
	os.WriteFile(p, data, 0o644)
	l = open(t, dir, o)
	defer l.Close()
	st, _ := l.Stats()
	if len(st.Recovery.Damage) == 0 {
		t.Fatal("damage not reported")
	}
	if _, err := l.Group(1).Entries(1, 81, math.MaxUint64); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("reading across the damage: %v", err)
	}
	eqEntries(t, "after the damage", mustEntries(t, l.Group(1), 60, 81), ents(1, 60, 80, 1000))
	if rep, _ := l.Scrub(); len(rep.Problems) == 0 {
		t.Fatal("scrub found nothing")
	}
}

// entryHeader reads exactly what Unmarshal reads.
func TestEntryHeader(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for k := 0; k < 2000; k++ {
		e := raftpb.Entry{Term: rng.Uint64() >> uint(rng.Intn(64)), Index: rng.Uint64() >> uint(rng.Intn(64)), Type: raftpb.EntryType(rng.Intn(3))}
		if rng.Intn(4) > 0 {
			e.Data = make([]byte, rng.Intn(300))
			rng.Read(e.Data)
		}
		b, _ := e.Marshal()
		idx, term, err := entryHeader(b)
		if err != nil || idx != e.Index || term != e.Term {
			t.Fatalf("%+v: got %d %d %v", e, idx, term, err)
		}
	}
}

// Term ranges against a plain per-entry model.
func TestTermRuns(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	var ts []termRun
	model := map[uint64]uint64{}
	first, last, term := uint64(1), uint64(0), uint64(1)
	for step := 0; step < 5000; step++ {
		switch rng.Intn(4) {
		case 0, 1: // append, possibly truncating
			from := last + 1
			if last >= first && rng.Intn(3) == 0 {
				from = first + uint64(rng.Int63n(int64(last-first+1)))
				ts = termsTruncate(ts, from)
				for i := from; i <= last; i++ {
					delete(model, i)
				}
				term++
			}
			n := uint64(1 + rng.Intn(5))
			var es []raftpb.Entry
			for i := from; i < from+n; i++ {
				es = append(es, raftpb.Entry{Index: i, Term: term})
				model[i] = term
			}
			ts = termsAppend(ts, es)
			last = from + n - 1
		case 2: // compact
			if last >= first {
				first += uint64(rng.Int63n(int64(last-first+1))) + 1
				ts = termsCompact(ts, first)
			}
		case 3:
			if rng.Intn(10) == 0 {
				term++
			}
		}
		for i := first; i <= last; i++ {
			if got, ok := termAt(ts, i); !ok || got != model[i] {
				t.Fatalf("step %d: term(%d) = %d, %v; want %d", step, i, got, ok, model[i])
			}
		}
	}
}

// durableRemoves makes every file removal durable at once. crashfs keeps an
// unsynced removal only sometimes, while a real filesystem may always keep
// it: this is the pessimistic case for code that deletes a file too early.
type durableRemoves struct{ vfs.FS }

func (d durableRemoves) Remove(name string) error {
	if err := d.FS.Remove(name); err != nil {
		return err
	}
	dir, err := d.FS.OpenDir(d.FS.PathDir(name))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// crashEveryOp runs ops once per mutating filesystem operation, simulating
// a power cut just before that operation (keeping none or all unsynced
// data), and calls check on each crash image with how many ops had
// returned.
func crashEveryOp(t *testing.T, o Options, ops []func(*Log) error, check func(what string, l *Log, done int)) {
	t.Helper()
	run := func(hook func(crashfs.Op) error) int {
		mem := crashfs.New()
		l, err := Open(crashDir, memOpts(durableRemoves{countingFS(mem, hook)}, o))
		if err != nil {
			t.Fatal(err)
		}
		for _, op := range ops {
			if err := op(l); err != nil {
				t.Fatal(err)
			}
		}
		l.Close()
		return 0
	}
	var total atomic.Int64
	run(func(crashfs.Op) error { total.Add(1); return nil })
	for k := int64(1); k <= total.Load(); k++ {
		for _, keep := range []int{0, 100} {
			var clone *crashfs.FS
			var done atomic.Int64
			var doneAt int64
			var count atomic.Int64
			mem := crashfs.New()
			l, err := Open(crashDir, memOpts(durableRemoves{countingFS(mem, func(crashfs.Op) error {
				if count.Add(1) == k {
					clone = mem.CrashClone(keep, randv2.New(randv2.NewPCG(uint64(k), uint64(keep))))
					doneAt = done.Load()
				}
				return nil
			})}, o))
			if err != nil {
				t.Fatal(err)
			}
			for _, op := range ops {
				if err := op(l); err != nil {
					t.Fatal(err)
				}
				done.Add(1)
			}
			l.Close()
			if clone == nil {
				continue
			}
			what := fmt.Sprintf("crash before op %d (keep %d%%, %d done)", k, keep, doneAt)
			rl, err := Open(crashDir, memOpts(clone, Options{ScrubOnOpen: true, SnapshotInlineBytes: o.SnapshotInlineBytes}))
			if err != nil {
				t.Fatalf("%s: open: %v", what, err)
			}
			check(what, rl, int(doneAt))
			rl.Close()
		}
	}
}

// Replacing a snapshot whose data is in a file, by a created or a received
// snapshot, never leaves a recovered log pointing at a deleted file.
func TestSnapshotReplacementCrash(t *testing.T) {
	o := Options{SnapshotInlineBytes: 1 << 10}
	payload := func(i uint64) []byte {
		if i%2 == 0 {
			return bytes.Repeat([]byte{byte(i)}, 5000) // in a file
		}
		return []byte{byte(i)} // inline
	}
	create := func(i uint64) func(*Log) error {
		return func(l *Log) error { _, err := l.Group(1).CreateSnapshot(i, nil, payload(i)); return err }
	}
	receive := func(i uint64) func(*Log) error {
		return func(l *Log) error {
			snap := raftpb.Snapshot{Data: payload(i), Metadata: raftpb.SnapshotMetadata{Index: i, Term: 2, ConfState: raftpb.ConfState{Voters: []uint64{1}}}}
			return l.Group(1).Save(raftpb.HardState{Term: 2, Commit: i}, nil, snap)
		}
	}
	ops := []func(*Log) error{
		func(l *Log) error {
			return l.Group(1).Save(raftpb.HardState{Term: 1, Commit: 20}, ents(1, 1, 20, 100), raftpb.Snapshot{})
		},
		create(4), create(6), receive(30), receive(32), receive(34), receive(35), receive(36),
	}
	// Snapshot index after each op; received snapshots are durable when Save
	// returns, created ones with the next commit.
	snapAfter := []uint64{0, 0, 4, 6, 30, 32, 34, 35, 36}
	received := []bool{false, false, false, false, true, true, true, true, true}
	crashEveryOp(t, o, ops, func(what string, l *Log, done int) {
		snap, err := l.Group(1).Snapshot()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		i := snap.Metadata.Index
		var lo uint64
		for k := 0; k <= done; k++ {
			if received[k] {
				lo = snapAfter[k]
			}
		}
		hi := snapAfter[min(done+1, len(snapAfter)-1)]
		if i < lo || i > hi {
			t.Fatalf("%s: snapshot %d, want between %d and %d", what, i, lo, hi)
		}
		if i > 0 && !bytes.Equal(snap.Data, payload(i)) {
			t.Fatalf("%s: snapshot %d has the wrong data", what, i)
		}
	})
}

// GC interrupted at any point leaves no orphaned segment files once the log
// is reopened, and loses nothing.
func TestGCCrashLeavesNoOrphans(t *testing.T) {
	o := Options{SegmentSize: 64 << 10, NoAutoGC: true, CleanMaxLiveRatio: -1}
	ops := []func(*Log) error{
		func(l *Log) error {
			for i := uint64(0); i < 40; i++ {
				if err := l.Group(1).Save(raftpb.HardState{Term: 1, Commit: i*4 + 4}, ents(1, i*4+1, i*4+4, 1000), raftpb.Snapshot{}); err != nil {
					return err
				}
			}
			return l.Group(1).Compact(120)
		},
		func(l *Log) error { _, err := l.GC(); return err },
	}
	crashEveryOp(t, o, ops, func(what string, l *Log, done int) {
		fi, _ := l.Group(1).FirstIndex()
		li, _ := l.Group(1).LastIndex()
		if done >= 1 && li != 160 {
			t.Fatalf("%s: last %d", what, li)
		}
		if li >= fi {
			eqEntries(t, what, mustEntries(t, l.Group(1), fi, li+1), ents(1, fi, li, 1000))
		}
		files, _ := listSegments(l.fs, l.segDir)
		if len(files) != l.SegmentCount() {
			t.Fatalf("%s: %d segment files, %d segments", what, len(files), l.SegmentCount())
		}
		st, _ := l.Stats()
		for _, sg := range st.Segments {
			if sg.Segment != st.ActiveSegment && !sg.Sealed {
				t.Fatalf("%s: segment %d is neither active nor sealed: %+v", what, sg.Segment, sg)
			}
		}
	})
}
