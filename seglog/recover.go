// Copyright 2026 The raftseg authors.

package seglog

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/raftseg/raft/raftpb"
)

// RecoveryStats describes what the last Open had to do.
type RecoveryStats struct {
	ReplayedCommits uint64 // complete commits applied from segments
	ReplayedRecords uint64
	ResumedSegment  bool // the newest segment was reopened for appends
	// RebuiltIndexes is how many segment index sidecars were missing or
	// damaged and were rebuilt from their segments.
	RebuiltIndexes int
	// Damage lists damaged data found in segments where the checkpoint says
	// there were complete commits. Open indexes everything else; reading an
	// entry in a damaged part fails with ErrCorrupt.
	Damage []string
	// Tail describes damage found at the end of the newest segment and
	// discarded as an interrupted write, or nil.
	Tail *TailDamage
	// Repairs lists inconsistencies Open fixed (none are expected).
	Repairs []string
	// OrphanFiles is how many unreferenced snapshot, index or temporary
	// files were removed.
	OrphanFiles int
}

// TailDamage describes a discarded tail.
type TailDamage struct {
	Segment        uint64
	Offset         int64
	Reason         string
	DiscardedBytes int64
	// TornLike is true when the damage looks like an interrupted write
	// (zeroes or end of file). False means non-zero bytes failed their
	// checksum, which could also be bit rot in an acknowledged commit; Open
	// then refuses unless Options.DiscardDamagedTail is set.
	TornLike bool
}

func (l *Log) groupAt(id uint64) *group {
	g, ok := l.groups[id]
	if !ok {
		g = newGroup()
		l.groups[id] = g
	}
	return g
}

// recover loads the newest checkpoint and replays every commit that followed
// it, then checks the result.
func (l *Log) recover() error {
	files, err := listSegments(l.fs, l.segDir)
	if err != nil {
		return err
	}
	ck, err := l.readCheckpoint()
	if err != nil {
		return err
	}
	if ck == nil {
		if ck, err = l.initFormat(files); err != nil {
			return err
		}
		if files, err = listSegments(l.fs, l.segDir); err != nil {
			return err
		}
	}
	l.ckptGen, l.seq, l.key = ck.gen, ck.seq, ck.key
	l.groups = ck.groups
	for n, m := range ck.segs {
		l.segs[n] = &segState{meta: m}
	}

	onDisk := map[uint64]bool{}
	for _, n := range files {
		onDisk[n] = true
	}
	// Segments GC dropped from the state before the checkpoint was written,
	// whose files remained.
	orphans := 0
	for _, n := range files {
		if _, ok := ck.segs[n]; !ok && n < ck.seg {
			if err := l.removeSegmentFiles(n); err != nil {
				return err
			}
			delete(onDisk, n)
			orphans++
		}
	}
	for n, m := range ck.segs {
		if onDisk[n] {
			if n < ck.seg && !m.sealed {
				return fmt.Errorf("%w: segment %d precedes the replay start but is not sealed", ErrCorrupt, n)
			}
			continue
		}
		if n >= ck.seg && m.end == 0 {
			delete(l.segs, n) // created, and lost before anything was written to it
			continue
		}
		return fmt.Errorf("%w: segment %d is missing", ErrCorrupt, n)
	}
	var live []uint64
	for _, n := range files {
		if onDisk[n] {
			live = append(live, n)
		}
	}
	// Each segment from the replay start on was created by the roll of the
	// one before it, so their numbers are consecutive, starting with the
	// checkpoint's. A gap means a segment is missing; a number out of
	// sequence, a file that does not belong here (and would one day be
	// overwritten by a roll).
	next := ck.seg
	for _, n := range live {
		switch {
		case n < ck.seg:
		case n > maxSegment:
			return fmt.Errorf("%w: segment number %d is out of range", ErrCorrupt, n)
		case n != next:
			return fmt.Errorf("%w: segment %d is missing, but segment %d exists", ErrCorrupt, next, n)
		default:
			next++
		}
	}

	resume := false
	var resumeEnd int64
	for i, n := range live {
		ss, ok := l.segs[n]
		if !ok {
			ss = &segState{}
			l.segs[n] = ss
		}
		newest := i == len(live)-1
		if n < ck.seg {
			// A sealed segment shorter than its commits lost data after it was
			// synced. Open indexes it as usual; reads of what is gone fail with
			// ErrCorrupt.
			if st, err := l.fs.Stat(l.segPath(n)); err != nil {
				return err
			} else if st.Size() < ss.meta.end {
				l.recovery.Damage = append(l.recovery.Damage, fmt.Sprintf("segment %d: %d bytes, but its commits end at %d", n, st.Size(), ss.meta.end))
			}
			if err := l.ensureSidecar(n, ss.meta); err != nil {
				return err
			}
			continue
		}
		from := int64(0)
		var base *replayResult
		if n == ck.seg {
			from = ck.off
			// Close writes the active segment's index too: skip rescanning
			// what it covers.
			if m, ok := ck.segs[n]; ok && from > 0 {
				if data, err := l.readFile(l.idxPath(n)); err == nil {
					if x, err := decodeSegIndex(data, n, from, m.entries); err == nil {
						base = &replayResult{count: m.count, entries: m.entries, idx: x}
					}
				}
			}
		}
		r, err := l.replaySegment(n, from, newest, base)
		if err != nil {
			return err
		}
		ss.meta = segMeta{count: r.count, entries: r.entries, end: r.end}
		ss.idx = r.idx
		if !newest || r.end >= l.opts.SegmentSize {
			ss.meta.sealed = true
			if err := l.writeSidecar(n, r.idx, r.end); err != nil {
				return err
			}
			l.idxBytes += r.idx.memBytes()
		} else {
			resume, resumeEnd = true, r.end
			ss.pinned = true
		}
	}
	l.repairInvariants()
	if err := l.checkGroups(); err != nil {
		return err
	}

	if resume {
		n := live[len(live)-1]
		f, err := l.fs.OpenReadWrite(l.segPath(n))
		if err != nil {
			return err
		}
		if !l.opts.NoPreallocate {
			_ = f.Preallocate(0, l.opts.SegmentSize)
		}
		m := l.segs[n].meta
		l.active, l.wf, l.w, l.wsegCount, l.wsegEnts = n, f, newSegWriter(f, resumeEnd, l.key), m.count, m.entries
		l.durable.Store(&durablePos{seg: n, end: resumeEnd}) // replaySegment synced it
		l.recovery.ResumedSegment = true
	} else {
		next := ck.seg
		for _, n := range live {
			next = max(next, n+1)
		}
		if err := l.openSegment(next); err != nil {
			return err
		}
	}
	l.evictIndexes()

	l.mu.Lock()
	for _, g := range l.groups {
		g.meta, g.snap, g.whs = g.wmeta, g.wsnap, g.hs
		g.pruns = append([]segRun(nil), g.runs...)
		g.pterms = append([]termRun(nil), g.terms...)
	}
	l.evictCaches()
	l.mu.Unlock()
	o, err := l.cleanOrphans()
	if err != nil {
		return err
	}
	l.recovery.OrphanFiles += orphans + o
	// The segments kept, including one resumed for appends, may have been
	// created by a process that died (or whose sync failed) before it synced
	// their names. Make the names durable before anything new refers to them.
	if err := syncDir(l.fs, l.segDir); err != nil {
		return err
	}
	if l.recovery.ReplayedCommits > 0 || len(l.recovery.Repairs) > 0 || l.recovery.Tail != nil {
		// Later opens need not replay this again.
		return l.checkpoint(nil)
	}
	return nil
}

// initFormat creates the first checkpoint of a new log, before anything else
// is written, so a crash during the very first Open cannot leave files that
// look like data without a checkpoint.
func (l *Log) initFormat(files []uint64) (*checkpoint, error) {
	for _, old := range []string{"index", "index.bolt"} {
		if _, err := l.fs.Stat(l.fs.PathJoin(l.dir, old)); err == nil {
			return nil, fmt.Errorf("seglog: %s uses on-disk format 3, which this version cannot open", l.dir)
		}
	}
	for _, n := range files {
		// Only empty segments from an interrupted first Open are acceptable.
		f, err := l.fs.Open(l.segPath(n))
		if err != nil {
			return nil, err
		}
		sc, err := newSegScanner(f, l.key)
		var zero bool
		if err == nil {
			zero, err = sc.allZero(0)
		}
		f.Close()
		if err != nil {
			return nil, err
		}
		if !zero {
			return nil, fmt.Errorf("%w: segments exist but no checkpoint", ErrCorrupt)
		}
		if err := l.removeSegmentFiles(n); err != nil {
			return nil, err
		}
	}
	key, err := newKey()
	if err != nil {
		return nil, err
	}
	if l.opts.testZeroKey {
		key = 0
	}
	l.key = key
	l.active, l.w = 1, newSegWriter(nil, 0, key)
	if err := l.checkpoint(nil); err != nil {
		return nil, err
	}
	return &checkpoint{gen: l.ckptGen, seg: 1, key: key, segs: map[uint64]segMeta{}, groups: map[uint64]*group{}}, nil
}

// ensureSidecar makes sure a sealed segment from before the replay start has
// an index sidecar, rebuilding a missing one. (Damaged sidecars are found
// when loaded, and rebuilt in memory.)
func (l *Log) ensureSidecar(n uint64, m segMeta) error {
	if _, err := l.fs.Stat(l.idxPath(n)); err == nil {
		return nil
	}
	x, err := l.loadIndex(n, m)
	if err != nil {
		return err
	}
	l.recovery.RebuiltIndexes++
	return l.writeSidecar(n, x, m.end)
}

func (l *Log) removeSegmentFiles(n uint64) error {
	if err := l.fs.Remove(l.segPath(n)); err != nil {
		return err
	}
	return l.removeIfExists(l.idxPath(n))
}

func (l *Log) removeIfExists(path string) error {
	if err := l.fs.Remove(path); err != nil {
		if _, serr := l.fs.Stat(path); serr == nil {
			return err
		}
	}
	return nil
}

func parseNumber(s string) (uint64, error) { return strconv.ParseUint(s, 10, 64) }

type replayResult struct {
	end            int64
	count, entries uint64
	idx            *segIndex
}

// replaySegment indexes segment n and applies its complete commits that
// follow offset from (those before it are reflected in the checkpoint). A
// commit is applied only once its marker is read and checked, so a
// partially written commit is never half-applied. Invalid data is a torn
// tail only if it is confined to the last commits of the newest segment that
// no later commit vouches for; that tail is zeroed and the segment can be
// resumed. Anything else is corruption.
func (l *Log) replaySegment(n uint64, from int64, newest bool, base *replayResult) (replayResult, error) {
	var res replayResult
	path := l.segPath(n)
	f, err := l.fs.Open(path)
	if err != nil {
		return res, err
	}
	sc, err := newSegScanner(f, l.key)
	if err != nil {
		f.Close()
		return res, err
	}
	defer f.Close()
	if sc.size > maxSegmentFileBytes {
		return res, fmt.Errorf("%w: segment %d is %d bytes, more than a segment can hold", ErrCorrupt, n, sc.size)
	}
	if from > sc.size {
		return res, fmt.Errorf("%w: segment %d is %d bytes but the checkpoint covers %d", ErrCorrupt, n, sc.size, from)
	}
	x := newSegIndex()
	var pending []rawRecord
	pos, end := int64(0), int64(0)
	if base != nil {
		// The index of [0, from) came from the segment's sidecar.
		x, pos, end = base.idx, from, from
		res.count, res.entries = base.count, base.entries
	}
	corrupt := func(format string, a ...interface{}) error {
		return fmt.Errorf("%w: segment %d: %s", ErrCorrupt, n, fmt.Sprintf(format, a...))
	}
	var tail *TailDamage
	for {
		rec, err := sc.next(pos)
		if err == io.EOF {
			break
		}
		var bd *badData
		if errors.As(err, &bd) {
			if bd.at < from {
				// Damage in data the checkpoint covers (so it had been durable):
				// index whatever else can be read. Reads of entries in the
				// damaged part fail, and Scrub reports it.
				l.recovery.Damage = append(l.recovery.Damage, fmt.Sprintf("segment %d: %v", n, bd))
				p, rerr := sc.resync(bd.at)
				if rerr != nil {
					return res, rerr
				}
				if p < 0 || p > from {
					p = from
				}
				pos, end = p, p
				continue
			}
			fatal, why, cerr := sc.classify(bd.at)
			if cerr != nil {
				return res, cerr
			}
			if fatal {
				return res, corrupt("%v; %s", bd, why)
			}
			if !newest {
				// Sealed by a roll, so it ended with a complete, synced commit;
				// only zeroes may follow.
				zero, zerr := sc.allZero(end)
				if zerr != nil {
					return res, zerr
				}
				if !zero {
					return res, corrupt("%v in a segment that is not the newest", bd)
				}
				break
			}
			torn, terr := sc.tornLike(bd)
			if terr != nil {
				return res, terr
			}
			tail = &TailDamage{Segment: n, Offset: bd.at, Reason: bd.why, TornLike: torn}
			break
		}
		if err != nil {
			return res, err
		}
		typ, _, payload, derr := decodeRecord(rec.data)
		if derr != nil {
			return res, corrupt("record at %d: %v", rec.start, derr)
		}
		pos = rec.end
		if rec.end <= from {
			// Covered by the checkpoint: only index it.
			if err := x.addRecord(rec); err != nil {
				return res, corrupt("record at %d: %v", rec.start, err)
			}
			if typ == recEntry || typ == recRelocate {
				res.entries++
			}
			res.count++
			if typ == recCommit {
				end = rec.end
			}
			continue
		}
		if typ != recCommit {
			pending = append(pending, rec)
			continue
		}
		m, derr := decodeCommitMarker(payload)
		if derr != nil {
			return res, corrupt("commit marker at %d: %v", rec.start, derr)
		}
		if m.nrecs != uint64(len(pending)) || len(pending) == 0 || m.start != pending[0].start {
			return res, corrupt("commit %d at %d does not match its %d records", m.seq, rec.start, len(pending))
		}
		if m.seq != l.seq+1 {
			return res, corrupt("commit at %d has sequence %d, expected %d", rec.start, m.seq, l.seq+1)
		}
		for _, p := range pending {
			if err := x.addRecord(p); err != nil {
				return res, corrupt("record at %d: %v", p.start, err)
			}
			if p.data[0] == recEntry || p.data[0] == recRelocate {
				res.entries++
			}
			if err := l.replayRecord(n, p.start, p.data); err != nil {
				return res, err
			}
		}
		l.seq = m.seq
		l.recovery.ReplayedCommits++
		l.recovery.ReplayedRecords += uint64(len(pending))
		res.count += uint64(len(pending)) + 1
		pending = pending[:0]
		end = rec.end
	}
	if end < from {
		return res, corrupt("valid commits end at %d, before the checkpointed position %d", end, from)
	}
	if len(pending) > 0 && !newest {
		return res, corrupt("ends with an incomplete commit")
	}
	if res.count > 0 || (newest && end > 0) {
		// What was replayed may only have been in the page cache (a process
		// crash). Make it durable before anything refers to it.
		if err := syncFile(l.fs, path); err != nil {
			return res, err
		}
	}
	if newest && sc.size > end {
		zero, err := sc.allZero(end)
		if err != nil {
			return res, err
		}
		if !zero {
			if tail == nil {
				tail = &TailDamage{Segment: n, Offset: end, Reason: "incomplete final commit", TornLike: true}
			}
			tail.DiscardedBytes = sc.size - end
			if !tail.TornLike && !l.opts.DiscardDamagedTail {
				return res, corrupt("damaged final commit at %d does not look like an interrupted write (%s), and it may have been acknowledged; set DiscardDamagedTail to discard it", tail.Offset, tail.Reason)
			}
			// Zero the tail so stale bytes can never be mistaken for records
			// once appends resume at end.
			if err := zeroRange(l.fs, path, end, sc.size); err != nil {
				return res, err
			}
			l.recovery.Tail = tail
		}
	}
	for _, es := range x.groups {
		for _, e := range es {
			e.vis = e.n()
		}
	}
	res.end, res.idx = end, x
	return res, nil
}

func syncFile(fs FS, path string) error {
	f, err := fs.OpenReadWrite(path)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func zeroRange(fs FS, path string, from, to int64) error {
	f, err := fs.OpenReadWrite(path)
	if err != nil {
		return err
	}
	zeros := make([]byte, blockSize)
	for p := from; p < to; {
		n := min(int64(blockSize), to-p)
		if _, err := f.WriteAt(zeros[:n], p); err != nil {
			f.Close()
			return err
		}
		p += n
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// replayRecord applies one record of a commit that followed the checkpoint.
func (l *Log) replayRecord(seg uint64, start int64, data []byte) error {
	typ, id, payload, err := decodeRecord(data)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if typ == recRemove {
		if g := l.groups[id]; g != nil {
			l.runsClear(g)
			l.cacheClear(g)
			delete(l.groups, id)
		}
		return nil
	}
	g := l.groupAt(id)
	switch typ {
	case recEntry, recRelocate:
		var e = &raftpb.Entry{}
		if err := proto.Unmarshal(payload, e); err != nil {
			return fmt.Errorf("%w: entry: %v", ErrCorrupt, err)
		}
		m := &g.wmeta
		if typ == recRelocate {
			// A copy of a live entry: never changes the log's shape.
			if e.GetIndex() >= m.first && e.GetIndex() <= m.last {
				l.runsMove(g, e.GetIndex(), e.GetIndex(), seg)
			}
			return nil
		}
		if g.born.seg == 0 {
			g.born = position{seg, start} // as writeEntries does
		}
		if e.GetIndex() < m.first {
			return nil // compacted or superseded by a snapshot
		}
		if e.GetIndex() > m.last+1 {
			return fmt.Errorf("%w: group %d: replay gap, last %d, record %d", ErrCorrupt, id, m.last, e.GetIndex())
		}
		if e.GetIndex() <= m.last {
			l.runsRemoveFrom(g, e.GetIndex())
			g.terms = termsTruncate(g.terms, e.GetIndex())
		}
		l.runsAppend(g, seg, e.GetIndex(), e.GetIndex())
		g.terms = termsAppend(g.terms, []*raftpb.Entry{e})
		m.last = e.GetIndex()
		l.cacheAppend(g, []*raftpb.Entry{e})
	case recHardState:
		var hs = &raftpb.HardState{}
		if err := proto.Unmarshal(payload, hs); err != nil {
			return fmt.Errorf("%w: hard state: %v", ErrCorrupt, err)
		}
		g.hs = copyHardState(hs)
	case recConfState:
		var cs = &raftpb.ConfState{}
		if err := proto.Unmarshal(payload, cs); err != nil {
			return fmt.Errorf("%w: conf state: %v", ErrCorrupt, err)
		}
		g.cs = copyConfState(cs)
	case recSnapshot:
		flags, s, err := decodeSnapRecord(payload)
		if err != nil {
			return err
		}
		if flags&snapApplied != 0 {
			g.wmeta = groupMeta{first: s.meta.GetIndex() + 1, last: s.meta.GetIndex(), compactTerm: s.meta.GetTerm()}
			l.runsClear(g)
			g.terms = nil
			l.cacheClear(g)
			g.cs = s.meta.GetConfState()
		} else if flags&snapSetCS != 0 {
			g.cs = s.meta.GetConfState()
		}
		g.wsnap = s
	default:
		return fmt.Errorf("%w: unknown record type %d", ErrCorrupt, typ)
	}
	return nil
}

// repairInvariants enforces what raft checks on restart. A commit index
// below FirstIndex-1 is raised (everything up to there is in a snapshot or
// compacted, hence applied, hence committed). A commit index beyond the last
// entry cannot be repaired and is reported as corruption by checkGroups.
func (l *Log) repairInvariants() {
	for id, g := range l.groups {
		m := g.wmeta
		if m.first > 1 && g.hs.GetCommit() < m.first-1 && !isEmptyHS(g.hs) {
			l.recovery.Repairs = append(l.recovery.Repairs, fmt.Sprintf("group %d: commit %d raised to %d", id, g.hs.GetCommit(), m.first-1))
			g.hs = withCommit(g.hs, m.first-1)
		}
	}
}

func isEmptyHS(hs *raftpb.HardState) bool {
	return hs.GetTerm() == 0 && hs.GetVote() == 0 && hs.GetCommit() == 0
}

// checkGroups verifies that every group's runs cover exactly [first, last]
// in existing segments and that its terms are known, and rebuilds the
// per-segment live counts.
func (l *Log) checkGroups() error {
	l.live = map[uint64]uint64{}
	for id, g := range l.groups {
		m := g.wmeta
		if m.first == 0 || m.last > maxIndex || m.first > m.last+1 {
			return fmt.Errorf("%w: group %d: bad log bounds [%d, %d]", ErrCorrupt, id, m.first, m.last)
		}
		// Every live entry was written at or after the group's first entry
		// record.
		if b := g.born; b.off < 0 || b.off > maxSegmentFileBytes || b.seg > maxSegment ||
			(b.seg == 0 && (b.off != 0 || len(g.runs) > 0)) {
			return fmt.Errorf("%w: group %d: bad first record position %d:%d", ErrCorrupt, id, b.seg, b.off)
		}
		next := m.first
		for _, r := range g.runs {
			if r.seg < g.born.seg {
				return fmt.Errorf("%w: group %d: entries [%d, %d] in segment %d, before the group's first record (segment %d)", ErrCorrupt, id, r.lo, r.hi, r.seg, g.born.seg)
			}
			if r.lo != next || r.hi < r.lo || r.hi > m.last {
				return fmt.Errorf("%w: group %d: entries [%d, %d] in segment %d do not continue the log at %d (log [%d, %d])", ErrCorrupt, id, r.lo, r.hi, r.seg, next, m.first, m.last)
			}
			if _, ok := l.segs[r.seg]; !ok {
				return fmt.Errorf("%w: group %d: entries [%d, %d] are in missing segment %d", ErrCorrupt, id, r.lo, r.hi, r.seg)
			}
			l.live[r.seg] += r.hi - r.lo + 1
			next = r.hi + 1
		}
		if m.first <= m.last {
			if next != m.last+1 {
				return fmt.Errorf("%w: group %d: entries known up to %d, last is %d", ErrCorrupt, id, next-1, m.last)
			}
			if _, ok := termAt(g.terms, m.first); !ok {
				return fmt.Errorf("%w: group %d: term of entry %d unknown", ErrCorrupt, id, m.first)
			}
		}
		if g.hs.GetCommit() > m.last {
			return fmt.Errorf("%w: group %d: commit index %d beyond last entry %d", ErrCorrupt, id, g.hs.GetCommit(), m.last)
		}
	}
	return nil
}

// cleanOrphans removes files nothing refers to: snapshot files of replaced
// snapshots, index sidecars of deleted segments, and temporary files of
// interrupted writes. It also checks that every referenced snapshot file
// exists.
func (l *Log) cleanOrphans() (int, error) {
	removed := 0
	names, err := l.fs.List(l.snapDir)
	if err != nil {
		return 0, err
	}
	for _, name := range names {
		keep := false
		if gid, idx, ok := parseSnapName(name); ok && !strings.HasSuffix(name, ".tmp") {
			if g := l.groups[gid]; g != nil && g.wsnap != nil && g.wsnap.ref != nil && g.wsnap.meta.GetIndex() == idx {
				keep = true
			}
		}
		if !keep {
			if err := l.fs.Remove(l.fs.PathJoin(l.snapDir, name)); err != nil {
				return 0, err
			}
			removed++
		}
	}
	if removed > 0 {
		if err := syncDir(l.fs, l.snapDir); err != nil {
			return 0, err
		}
	}
	for id, g := range l.groups {
		if g.wsnap != nil && g.wsnap.ref != nil {
			if _, err := l.fs.Stat(l.snapPath(id, g.wsnap.meta.GetIndex())); err != nil {
				return 0, fmt.Errorf("%w: group %d: snapshot file for index %d is missing", ErrCorrupt, id, g.wsnap.meta.GetIndex())
			}
		}
	}
	segNames, err := l.fs.List(l.segDir)
	if err != nil {
		return 0, err
	}
	for _, name := range segNames {
		orphan := strings.HasSuffix(name, ".tmp")
		if base, ok := strings.CutSuffix(name, idxSuffix); ok {
			n, perr := parseNumber(base)
			_, known := l.segs[n]
			orphan = perr != nil || !known
		}
		if orphan {
			if err := l.fs.Remove(l.fs.PathJoin(l.segDir, name)); err != nil {
				return 0, err
			}
			removed++
		}
	}
	for _, name := range ckptNames {
		p := l.fs.PathJoin(l.dir, name+".tmp")
		if _, err := l.fs.Stat(p); err == nil {
			if err := l.fs.Remove(p); err != nil {
				return 0, err
			}
			removed++
		}
	}
	return removed, nil
}
