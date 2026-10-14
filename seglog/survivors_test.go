// Copyright 2026 The raftseg authors.

package seglog

// Tests written for mutants that survived the first mutation run: each
// exercises one safeguard no other test reached on its own.

import (
	"errors"
	"strings"
	"testing"

	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
	randv2 "math/rand/v2"
)

// activeSegment returns the active segment once the writer is idle (a roll
// runs on the writer after the Save that triggered it has returned).
func activeSegment(t *testing.T, l *Log) uint64 {
	t.Helper()
	st, err := l.Stats()
	if err != nil {
		t.Fatal(err)
	}
	return st.ActiveSegment
}

// fillSegments saves fixed-size entries to group 1, one per commit, until
// the log has at least n segments.
func fillSegments(t *testing.T, l *Log, mem *crashfs.FS, n int) uint64 {
	t.Helper()
	i := uint64(0)
	for len(segmentNumbers(t, mem)) < n {
		i++
		if err := l.Save(Update{Group: 1, Entries: ents(1, i, i, 1000), HardState: raftpb.HardState{Term: 1, Commit: i}}); err != nil {
			t.Fatal(err)
		}
	}
	return i
}

// Commits are replayed in sequence. Segments written after the last
// checkpoint are replayed in order of their numbers; if one holds a copy of
// another, its commits repeat, and Open must refuse rather than replay them
// again and lose the real ones.
func TestReplayRefusesCommitsOutOfSequence(t *testing.T) {
	mem := crashfs.New()
	l := open(t, crashDir, memOpts(mem, Options{SegmentSize: 64 << 10, testDropIndexAfter: 1}))
	fillSegments(t, l, mem, 4)
	l.crash()
	segs := segmentNumbers(t, mem)
	// Every segment follows the checkpoint (none was written after commit
	// 1). Segment 2 is replaced by a copy of segment 1.
	rewrite(t, mem, segPathN(segs[1]), mustRead(t, mem, segPathN(segs[0])))
	if _, err := Open(crashDir, memOpts(mem, Options{})); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open: %v", err)
	}
}

// A roll creates the next segment and syncs it; if that sync fails, the
// log fails before syncing the segment's name. After a restart without a
// power cut, the new segment is resumed, and commits are acknowledged into
// it. Open must sync the name before that: a later power cut must not lose
// the file.
func TestResumedSegmentNameIsDurable(t *testing.T) {
	mem := crashfs.New()
	var created string
	fs := crashfs.WithHook(mem, func(op crashfs.Op) error {
		if op.Kind == crashfs.OpCreate && strings.HasSuffix(op.Path, segSuffix) && !strings.HasSuffix(op.Path, segName(1)) {
			created = op.Path
		}
		if op.Kind == crashfs.OpSync && op.Path == created && created != "" {
			created = ""
			return crashfs.ErrSyncLost
		}
		return nil
	})
	l := open(t, crashDir, memOpts(fs, Options{SegmentSize: 64 << 10}))
	var last uint64
	for i := uint64(1); i < 200 && l.failure() == nil; i++ {
		if err := l.Save(Update{Group: 1, Entries: ents(1, i, i, 1000), HardState: raftpb.HardState{Term: 1, Commit: i}}); err == nil {
			last = i
		}
	}
	l.Close()
	if l.failure() == nil {
		t.Fatal("the roll's sync did not fail")
	}
	// Restart (the page cache still has everything), and write a little.
	l = open(t, crashDir, memOpts(mem, Options{SegmentSize: 64 << 10}))
	if err := l.Save(Update{Group: 1, Entries: ents(1, last+1, last+1, 10), HardState: raftpb.HardState{Term: 1, Commit: last + 1}}); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	img := mem.CrashClone(0, randv2.New(randv2.NewPCG(1, 1)))
	l, err := Open(crashDir, memOpts(img, Options{}))
	if err != nil {
		t.Fatalf("after a power cut: %v", err)
	}
	defer l.Close()
	if got, _ := l.Group(1).LastIndex(); got != last+1 {
		t.Fatalf("last %d, want %d", got, last+1)
	}
}

// A checkpoint whose HardState commits beyond the group's last entry
// (crafted: Save refuses to produce one) must not open: raft would panic on
// it.
func TestOpenRefusesCommitBeyondLast(t *testing.T) {
	mem := crashfs.New()
	l := open(t, crashDir, memOpts(mem, Options{}))
	if err := l.Save(Update{Group: 1, Entries: ents(1, 1, 5, 10), HardState: raftpb.HardState{Term: 1, Commit: 5}}); err != nil {
		t.Fatal(err)
	}
	l.Close()
	c, err := decodeCheckpoint(mustRead(t, mem, crashDir+"/CHECKPOINT-A"))
	if err != nil {
		t.Fatal(err)
	}
	c.groups[1].hs.Commit = 6
	b := encodeCheckpointFile(c, formatVersion)
	rewrite(t, mem, crashDir+"/CHECKPOINT-A", b)
	rewrite(t, mem, crashDir+"/CHECKPOINT-B", b)
	if _, err := Open(crashDir, memOpts(mem, Options{})); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open: %v", err)
	}
}

// A sidecar names the segment it indexes. Two segments with the same size
// and entry count otherwise have interchangeable sidecars: one copied over
// the other must be rejected and the index rebuilt, losing nothing.
func TestSidecarOfLookalikeSegment(t *testing.T) {
	mem := crashfs.New()
	opts := Options{SegmentSize: 64 << 10, CacheEntries: -1, IndexCacheBytes: 1, NoAutoGC: true}
	l := open(t, crashDir, memOpts(mem, opts))
	last := fillSegments(t, l, mem, 6)
	st, _ := l.Stats()
	l.Close()
	// Find two sealed segments alike in size and entry count.
	var a, b uint64
	for i, x := range st.Segments {
		for _, y := range st.Segments[i+1:] {
			if x.Sealed && y.Sealed && x.Bytes == y.Bytes && x.Entries == y.Entries && a == 0 {
				a, b = x.Segment, y.Segment
			}
		}
	}
	if a == 0 {
		t.Fatalf("no two alike segments among %+v", st.Segments)
	}
	rewrite(t, mem, crashDir+"/segments/"+idxName(b), mustRead(t, mem, crashDir+"/segments/"+idxName(a)))
	l = open(t, crashDir, memOpts(mem, opts))
	defer l.Close()
	es, err := l.Group(1).Entries(1, last+1, 1<<30)
	if err != nil || uint64(len(es)) != last {
		t.Fatalf("read %d entries: %v", len(es), err)
	}
	for _, e := range es {
		if !entryDataOK(e) {
			t.Fatalf("entry %d holds %q", e.Index, e.Data[:16])
		}
	}
}

// A sidecar that points an entry at an older record of the same index (one
// a raft overwrite superseded) must not make reads return the old entry:
// the term every entry is checked against says which one is current.
func TestStaleRecordNotReturned(t *testing.T) {
	mem := crashfs.New()
	opts := Options{SegmentSize: 64 << 10, CacheEntries: -1, IndexCacheBytes: 1, NoAutoGC: true}
	l := open(t, crashDir, memOpts(mem, opts))
	if err := l.Save(Update{Group: 1, Entries: ents(1, 1, 5, 100), HardState: raftpb.HardState{Term: 1, Commit: 3}}); err != nil {
		t.Fatal(err)
	}
	// Overwrite entries 4 and 5 in term 2 (a new leader's log).
	if err := l.Save(Update{Group: 1, Entries: ents(2, 4, 5, 100), HardState: raftpb.HardState{Term: 2, Commit: 5}}); err != nil {
		t.Fatal(err)
	}
	// Seal the segment.
	for i := uint64(1); len(segmentNumbers(t, mem)) < 2; i++ {
		if err := l.Save(Update{Group: 2, Entries: ents(1, i, i, 1000), HardState: raftpb.HardState{Term: 1, Commit: i}}); err != nil {
			t.Fatal(err)
		}
	}
	l.Close()
	seg := segmentNumbers(t, mem)[0]
	st, _ := Open(crashDir, memOpts(mem, opts))
	stats, _ := st.Stats()
	st.Close()
	var meta SegmentStats
	for _, s := range stats.Segments {
		if s.Segment == seg {
			meta = s
		}
	}
	path := crashDir + "/segments/" + idxName(seg)
	x, err := decodeSegIndex(mustRead(t, mem, path), seg, meta.Bytes, meta.Entries)
	if err != nil {
		t.Fatal(err)
	}
	es := x.groups[1]
	if len(es) != 2 || es[0].lo != 1 || es[1].lo != 4 {
		t.Fatalf("extents %+v", es)
	}
	// Point the overwrite's entry 4 at the original record of entry 4.
	es[1].locs[0], es[1].locs[1] = es[0].locs[6], es[0].locs[7]
	rewrite(t, mem, path, x.encode(seg, meta.Bytes))
	l = open(t, crashDir, memOpts(mem, opts))
	defer l.Close()
	got, err := l.Group(1).Entries(4, 5, 1<<20)
	if err == nil && (len(got) != 1 || got[0].Term != 2) {
		t.Fatalf("entry 4 read as term %d: the superseded record", got[0].Term)
	}
	if err != nil && !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

// A sealed segment shorter than its commits is reported by Open.
func TestTruncatedSealedSegmentReported(t *testing.T) {
	mem := crashfs.New()
	l := open(t, crashDir, memOpts(mem, Options{SegmentSize: 64 << 10}))
	fillSegments(t, l, mem, 3)
	l.Close()
	seg := segmentNumbers(t, mem)[0]
	b := mustRead(t, mem, segPathN(seg))
	rewrite(t, mem, segPathN(seg), b[:len(b)/2])
	l, err := Open(crashDir, memOpts(mem, Options{}))
	if err != nil {
		return // refusing is fine too
	}
	defer l.Close()
	st, _ := l.Stats()
	found := false
	for _, d := range st.Recovery.Damage {
		found = found || strings.Contains(d, "commits end at")
	}
	if !found {
		t.Fatalf("damage not reported: %v", st.Recovery.Damage)
	}
}

// A missing segment stops Open, even one that holds no live entry: the log
// cannot tell which it was, and a file nothing deleted has gone.
func TestMissingDeadSegmentRefused(t *testing.T) {
	mem := crashfs.New()
	l := open(t, crashDir, memOpts(mem, Options{SegmentSize: 64 << 10, NoAutoGC: true}))
	last := fillSegments(t, l, mem, 3)
	if err := l.Group(1).Compact(last); err != nil {
		t.Fatal(err)
	}
	l.Close()
	if err := mem.Remove(segPathN(segmentNumbers(t, mem)[0])); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(crashDir, memOpts(mem, Options{})); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open: %v", err)
	}
}

// After an I/O error the log has failed, and Close says so: a caller that
// only checks Close must not take the shutdown for a clean one.
func TestCloseReportsFailure(t *testing.T) {
	mem := crashfs.New()
	fail := false
	fs := crashfs.WithHook(mem, func(op crashfs.Op) error {
		if fail && op.Kind == crashfs.OpSyncData {
			return crashfs.ErrInjected
		}
		return nil
	})
	l := open(t, crashDir, memOpts(fs, Options{}))
	if err := l.Save(Update{Group: 1, Entries: ents(1, 1, 1, 10), HardState: raftpb.HardState{Term: 1, Commit: 1}}); err != nil {
		t.Fatal(err)
	}
	fail = true
	if err := l.Save(Update{Group: 1, Entries: ents(1, 2, 2, 10), HardState: raftpb.HardState{Term: 1, Commit: 2}}); err == nil {
		t.Fatal("Save succeeded without its fsync")
	}
	if err := l.Close(); !errors.Is(err, crashfs.ErrInjected) {
		t.Fatalf("Close: %v", err)
	}
}

// A commit marker must match its records: their number and the offset of
// the first. A marker that does not (written by a bug, or crafted) makes
// Open refuse rather than apply records it does not vouch for.
func TestLyingMarkerRefused(t *testing.T) {
	for _, lie := range []string{"count", "start"} {
		mem := crashfs.New()
		l := open(t, crashDir, memOpts(mem, Options{testDropIndexAfter: 1}))
		if err := l.Save(Update{Group: 1, Entries: ents(1, 1, 3, 10), HardState: raftpb.HardState{Term: 1, Commit: 3}}); err != nil {
			t.Fatal(err)
		}
		err := l.exec(func() error {
			st := newStage()
			e := raftpb.Entry{Index: 4, Term: 1, Data: []byte("x")}
			eb, _ := e.Marshal()
			l.writeEncoded(st, encodeRecord(nil, recEntry, 1, eb))
			m := commitMarker{seq: l.seq + 1, nrecs: st.records, start: st.firstStart}
			if lie == "count" {
				m.nrecs++
			} else {
				m.start++
			}
			l.writeEncoded(st, encodeRecord(nil, recCommit, 0, m.encode(nil)))
			return l.w.flush()
		})
		if err != nil {
			t.Fatal(err)
		}
		l.crash()
		if _, err := Open(crashDir, memOpts(mem, Options{})); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("marker lying about its %s: open: %v", lie, err)
		}
	}
}

// A discarded torn tail is zeroed on disk, so that its bytes can never be
// read as records once appends resume before them.
func TestTornTailZeroed(t *testing.T) {
	mem := crashfs.New()
	l := open(t, crashDir, memOpts(mem, Options{testDropIndexAfter: 1}))
	for i := uint64(1); i <= 4; i++ {
		if err := l.Save(Update{Group: 1, Entries: ents(1, i, i, 3000), HardState: raftpb.HardState{Term: 1, Commit: i}}); err != nil {
			t.Fatal(err)
		}
	}
	l.crash()
	size := fileSize(t, mem, segPathN(1))
	f, _ := mem.OpenReadWrite(segPathN(1))
	f.WriteAt(make([]byte, 600), size-1500) // the last commit lost a sector
	f.Close()
	l = open(t, crashDir, memOpts(mem, Options{}))
	st, _ := l.Stats()
	l.Close()
	tail := st.Recovery.Tail
	if tail == nil || !tail.TornLike {
		t.Fatalf("tail %+v", tail)
	}
	b := mustRead(t, mem, segPathN(1))
	end := int64(len(b)) - tail.DiscardedBytes
	for i, c := range b[end:] {
		if c != 0 {
			t.Fatalf("byte %d of the discarded tail is %#x, not zero", end+int64(i), c)
		}
	}
}

// Commits that a restarted process replays may exist only in the page
// cache: the process before died before their fsync. Open syncs them
// before anything refers to them (here, the checkpoint Open writes after a
// replay), so that a power cut right after the restart cannot take them
// from under that checkpoint.
func TestReplayedCommitsMadeDurable(t *testing.T) {
	mem := crashfs.New()
	// The first process writes without fsyncs, then dies.
	l := open(t, crashDir, memOpts(mem, Options{NoSync: true}))
	for i := uint64(1); i <= 5; i++ {
		if err := l.Save(Update{Group: 1, Entries: ents(1, i, i, 1000), HardState: raftpb.HardState{Term: 1, Commit: i}}); err != nil {
			t.Fatal(err)
		}
	}
	l.crash()
	// The restart replays them from the page cache.
	l = open(t, crashDir, memOpts(mem, Options{}))
	if last, _ := l.Group(1).LastIndex(); last != 5 {
		t.Fatalf("restart: last %d", last)
	}
	l.crash()
	img := mem.CrashClone(0, randv2.New(randv2.NewPCG(3, 3)))
	l, err := Open(crashDir, memOpts(img, Options{}))
	if err != nil {
		t.Fatalf("after a power cut: %v", err)
	}
	defer l.Close()
	if last, _ := l.Group(1).LastIndex(); last != 5 {
		t.Fatalf("after a power cut: last %d, want 5", last)
	}
}

// Commits are replayed in sequence, even when replaying them out of order
// would leave no gap in any group's entries. Here two segments that follow
// the checkpoint are swapped. Each holds one filler group's entries, and a
// HardState of group 2: term 2 in the first, term 3 in the second. Applied
// out of order, the older HardState (an older vote) would silently win.
func TestSwappedSegmentsRefused(t *testing.T) {
	mem := crashfs.New()
	l := open(t, crashDir, memOpts(mem, Options{SegmentSize: 64 << 10, testDropIndexAfter: 1}))
	// Segment 1: group 1's entries, until it rolls.
	for i := uint64(1); activeSegment(t, l) == 1; i++ {
		if err := l.Save(Update{Group: 1, Entries: ents(1, i, i, 1000), HardState: raftpb.HardState{Term: 1, Commit: i}}); err != nil {
			t.Fatal(err)
		}
	}
	// Segments 2 and 3: a HardState of group 2, then one filler group each.
	for seg := uint64(2); seg <= 3; seg++ {
		if err := l.Save(Update{Group: 2, HardState: raftpb.HardState{Term: seg, Vote: seg}}); err != nil {
			t.Fatal(err)
		}
		g := 8 + seg
		for i := uint64(1); activeSegment(t, l) == seg; i++ {
			if err := l.Save(Update{Group: g, Entries: ents(1, i, i, 1000), HardState: raftpb.HardState{Term: 1, Commit: i}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	l.crash()
	a, b := mustRead(t, mem, segPathN(2)), mustRead(t, mem, segPathN(3))
	rewrite(t, mem, segPathN(2), b)
	rewrite(t, mem, segPathN(3), a)
	l, err := Open(crashDir, memOpts(mem, Options{}))
	if err == nil {
		hs, _, _ := l.Group(2).InitialState()
		l.Close()
		t.Fatalf("swapped segments opened; group 2's HardState is %+v", hs)
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

// Damage at the end of a sealed segment, after its last commit, is
// corruption: the segment was synced when it was sealed. Here the next
// segment is still empty, so no later commit's sequence number would show
// that commits are missing.
func TestDamageAfterSealedSegmentsLastCommit(t *testing.T) {
	mem := crashfs.New()
	l := open(t, crashDir, memOpts(mem, Options{SegmentSize: 64 << 10, testDropIndexAfter: 1}))
	for i := uint64(1); activeSegment(t, l) == 1; i++ {
		if err := l.Save(Update{Group: 1, Entries: ents(1, i, i, 1000), HardState: raftpb.HardState{Term: 1, Commit: i}}); err != nil {
			t.Fatal(err)
		}
	}
	key := l.key
	l.crash() // segment 2 exists and is empty
	// Damage the first record of segment 1's last commit, so that nothing of
	// that commit parses and only the bytes after the last good commit show
	// that something is wrong.
	f, _ := mem.OpenReadWrite(segPathN(1))
	sc, _ := newSegScanner(f, key)
	var lastStart int64 = -1
	for pos := int64(0); ; {
		rec, err := sc.next(pos)
		if err != nil {
			break
		}
		if typ, _, payload, _ := decodeRecord(rec.data); typ == recCommit {
			m, _ := decodeCommitMarker(payload)
			lastStart = m.start
		}
		pos = rec.end
	}
	b := []byte{0}
	f.ReadAt(b, lastStart)
	b[0] ^= 0x40
	f.WriteAt(b, lastStart)
	f.Close()
	if _, err := Open(crashDir, memOpts(mem, Options{DiscardDamagedTail: true})); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open: %v", err)
	}
}

// Every entry read is checked to belong to the group asked for: an index
// that points a group's entry at another group's record with the same index
// and term must not make the read return the other group's data.
func TestEntryOfAnotherGroupNotReturned(t *testing.T) {
	mem := crashfs.New()
	opts := Options{SegmentSize: 64 << 10, CacheEntries: -1, IndexCacheBytes: 1, NoAutoGC: true}
	l := open(t, crashDir, memOpts(mem, opts))
	for g := uint64(1); g <= 2; g++ {
		if err := l.Save(Update{Group: g, Entries: ents(1, 1, 5, 100), HardState: raftpb.HardState{Term: 1, Commit: 5}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := uint64(1); len(segmentNumbers(t, mem)) < 2; i++ {
		if err := l.Save(Update{Group: 3, Entries: ents(1, i, i, 1000), HardState: raftpb.HardState{Term: 1, Commit: i}}); err != nil {
			t.Fatal(err)
		}
	}
	st, _ := l.Stats()
	l.Close()
	seg := segmentNumbers(t, mem)[0]
	var meta SegmentStats
	for _, s := range st.Segments {
		if s.Segment == seg {
			meta = s
		}
	}
	path := crashDir + "/segments/" + idxName(seg)
	x, err := decodeSegIndex(mustRead(t, mem, path), seg, meta.Bytes, meta.Entries)
	if err != nil {
		t.Fatal(err)
	}
	copy(x.groups[1][0].locs, x.groups[2][0].locs) // group 1's entries at group 2's records
	rewrite(t, mem, path, x.encode(seg, meta.Bytes))
	l = open(t, crashDir, memOpts(mem, opts))
	defer l.Close()
	es, err := l.Group(1).Entries(1, 6, 1<<20)
	if err == nil {
		t.Fatalf("group 1 read %d entries of group 2's records", len(es))
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}
