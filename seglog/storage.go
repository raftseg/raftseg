// Copyright 2026 The raftseg authors.

package seglog

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
)

// Storage is one raft group's view of a Log. It implements raft.Storage with
// the same semantics as raft.MemoryStorage. All methods are safe to call
// concurrently with Save, including Saves of the same group.
type Storage struct {
	l  *Log
	id uint64
	g  *group
}

var _ raft.Storage = (*Storage)(nil)

// Group returns the storage for a raft group, creating it on first use. A
// handle stays bound to its group: after RemoveGroup it returns
// ErrGroupRemoved, even if the id is reused.
func (l *Log) Group(id uint64) *Storage {
	return &Storage{l: l, id: id, g: l.groupOrCreate(id)}
}

// Groups returns the ids of all groups that have state in the log.
func (l *Log) Groups() []uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	ids := make([]uint64, 0, len(l.groups))
	for id := range l.groups {
		ids = append(ids, id)
	}
	return ids
}

// ID returns the group id.
func (s *Storage) ID() uint64 { return s.id }

// read runs fn under the read lock with a stable view of the group. If fn
// needs disk it returns a continuation; the continuation runs without locks
// and its result is discarded and retried if the group changed meanwhile.
func (s *Storage) read(fn func(g *group) (done bool, disk func() error, err error)) error {
	if err := s.l.enter(); err != nil {
		return err
	}
	defer s.l.exit()
	for {
		s.l.mu.RLock()
		for s.g.version&1 == 1 && !s.g.removed {
			s.l.cond.Wait()
		}
		if s.g.removed {
			s.l.mu.RUnlock()
			return ErrGroupRemoved
		}
		v := s.g.version
		done, disk, err := fn(s.g)
		s.l.mu.RUnlock()
		if done || err != nil {
			return err
		}
		derr := disk()
		s.l.mu.RLock()
		stable := s.g.version == v && !s.g.removed
		s.l.mu.RUnlock()
		if stable {
			return derr
		}
		if f := s.l.failure(); f != nil {
			return f
		}
	}
}

// InitialState implements raft.Storage.
func (s *Storage) InitialState() (hs *raftpb.HardState, cs *raftpb.ConfState, err error) {
	err = s.read(func(g *group) (bool, func() error, error) {
		hs, cs = copyHardState(g.hs), copyConfState(g.cs) // the caller's own
		return true, nil, nil
	})
	return hs, cs, err
}

// FirstIndex implements raft.Storage.
func (s *Storage) FirstIndex() (i uint64, err error) {
	err = s.read(func(g *group) (bool, func() error, error) {
		i = g.meta.first
		return true, nil, nil
	})
	return i, err
}

// LastIndex implements raft.Storage.
func (s *Storage) LastIndex() (i uint64, err error) {
	err = s.read(func(g *group) (bool, func() error, error) {
		i = g.meta.last
		return true, nil, nil
	})
	return i, err
}

// HardState returns the group's last persisted HardState.
func (s *Storage) HardState() *raftpb.HardState {
	hs, _, _ := s.InitialState()
	return hs
}

// Term implements raft.Storage.
func (s *Storage) Term(i uint64) (term uint64, err error) {
	err = s.read(func(g *group) (bool, func() error, error) {
		m := g.meta
		switch {
		case i < m.first-1:
			return true, nil, raft.ErrCompacted
		case i == m.first-1:
			term = m.compactTerm
			return true, nil, nil
		case i > m.last:
			return true, nil, raft.ErrUnavailable
		}
		t, ok := termAt(g.pterms, i)
		if !ok {
			return true, nil, fmt.Errorf("%w: group %d: term of entry %d unknown", ErrCorrupt, s.id, i)
		}
		term = t
		return true, nil, nil
	})
	return term, err
}

// Entries implements raft.Storage.
func (s *Storage) Entries(lo, hi, maxSize uint64) (out []*raftpb.Entry, err error) {
	err = s.read(func(g *group) (bool, func() error, error) {
		out = nil
		m := g.meta
		switch {
		case lo < m.first:
			return true, nil, raft.ErrCompacted
		case hi > m.last+1:
			return true, nil, fmt.Errorf("seglog: group %d: entries' hi(%d) is out of bound lastindex(%d)", s.id, hi, m.last)
		case m.first > m.last:
			return true, nil, raft.ErrUnavailable
		case lo >= hi:
			return true, nil, nil
		}
		// Copy the cached suffix under the lock; read the rest from disk.
		diskHi := hi
		var cached []*raftpb.Entry
		if n := len(g.cache); n > 0 && hi > g.cache[0].GetIndex() {
			c0 := g.cache[0].GetIndex()
			from := lo
			if from < c0 {
				from = c0
			}
			cached = append([]*raftpb.Entry(nil), g.cache[from-c0:hi-c0]...)
			diskHi = from
		}
		appendCached := func() {
			var size uint64
			for i := range out {
				size += uint64(entrySize(out[i]))
			}
			for _, e := range cached {
				if !addLimited(&out, e, &size, maxSize) {
					return
				}
			}
		}
		if lo >= diskHi {
			appendCached()
			return true, nil, nil
		}
		runs, terms, born := g.pruns, g.pterms, g.born
		return false, func() error {
			var full bool
			var err error
			out, full, err = s.l.readEntries(s.id, lo, diskHi, maxSize, runs, terms, born)
			if err == nil && !full {
				appendCached()
			}
			return err
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Snapshot implements raft.Storage.
func (s *Storage) Snapshot() (snap *raftpb.Snapshot, err error) {
	err = s.read(func(g *group) (bool, func() error, error) {
		si := g.snap
		snap = &raftpb.Snapshot{}
		if si == nil {
			snap = raftpb.EnsureSnapshot(snap) // as raft.MemoryStorage
			return true, nil, nil
		}
		snap.Metadata = copySnapshotMetadata(si.meta)
		if si.ref == nil {
			snap.Data = append([]byte(nil), si.data...)
			return true, nil, nil
		}
		return false, func() error {
			var err error
			snap.Data, err = s.l.readSnapFile(s.id, *si.ref)
			return err
		}, nil
	})
	return snap, err
}

// Save persists one Ready's worth of state for this group. As with
// raft.MemoryStorage, the entries are kept as given (in the entry cache):
// they must not be changed afterwards. The hard state and snapshot are
// copied.
func (s *Storage) Save(hs *raftpb.HardState, ents []*raftpb.Entry, snap *raftpb.Snapshot) error {
	if s.removed() {
		return ErrGroupRemoved
	}
	return s.l.save(s.g, []Update{{Group: s.id, HardState: hs, Entries: ents, Snapshot: snap}})
}

func (s *Storage) removed() bool {
	s.l.mu.RLock()
	defer s.l.mu.RUnlock()
	return s.g.removed
}

// writeOp runs a metadata change for this group on the writer goroutine,
// making concurrent disk readers of the group wait and retry.
func (s *Storage) writeOp(fn func(g *group) error) error {
	return s.l.exec(func() error {
		s.l.mu.Lock()
		if s.g.removed {
			s.l.mu.Unlock()
			return ErrGroupRemoved
		}
		s.g.version++
		s.l.mu.Unlock()
		err := fn(s.g)
		s.l.mu.Lock()
		s.g.version++
		s.l.mu.Unlock()
		s.l.cond.Broadcast()
		return err
	})
}

// Compact discards entries up to and including compactIndex. Only compact
// entries that have been applied and captured in a snapshot. Like an
// unsynced change, a compaction may be undone by a crash (more entries are
// then kept); it becomes durable with the next checkpoint, which is always
// written before GC deletes the compacted entries.
func (s *Storage) Compact(compactIndex uint64) error {
	return s.writeOp(func(g *group) error {
		m := g.wmeta
		if compactIndex < m.first {
			return raft.ErrCompacted
		}
		if compactIndex > m.last {
			return fmt.Errorf("seglog: group %d: compact %d is out of bound lastindex(%d)", s.id, compactIndex, m.last)
		}
		term, err := s.l.termLocked(s.id, g, compactIndex)
		if err != nil {
			return err
		}
		nm := groupMeta{first: compactIndex + 1, last: m.last, compactTerm: term}
		g.wmeta = nm
		s.l.runsRemoveBelow(g, nm.first)
		g.terms = termsCompact(g.terms, nm.first)
		s.l.mu.Lock()
		g.meta = nm
		s.l.cacheTrimBelow(g, nm.first)
		g.pruns = append([]segRun(nil), g.runs...)
		g.pterms = append([]termRun(nil), g.terms...)
		s.l.mu.Unlock()
		return nil
	})
}

// SetConfState persists the group's current configuration, returned by
// InitialState after a restart. Call it after applying a conf change. It is
// durable when it returns.
func (s *Storage) SetConfState(cs *raftpb.ConfState) error {
	cs = copyConfState(cs) // the caller keeps its own
	buf, err := proto.Marshal(cs)
	if err != nil {
		return err
	}
	return s.l.exec(func() error {
		if s.l.isRemoved(s.g) {
			return ErrGroupRemoved
		}
		j := &job{st: newStage()}
		d := j.st.delta(s.l, s.id)
		s.l.writeRecord(j.st, recConfState, s.id, buf)
		d.cs = cs
		s.l.dispatch(j)
		s.l.drain()
		return s.l.failure()
	})
}

// termLocked looks up a term from the writer goroutine.
func (l *Log) termLocked(id uint64, g *group, i uint64) (uint64, error) {
	m := g.wmeta
	switch {
	case i < m.first-1:
		return 0, raft.ErrCompacted
	case i == m.first-1:
		return m.compactTerm, nil
	case i > m.last:
		return 0, raft.ErrUnavailable
	}
	t, ok := termAt(g.terms, i)
	if !ok {
		return 0, fmt.Errorf("%w: group %d: term of entry %d unknown", ErrCorrupt, id, i)
	}
	return t, nil
}

// RemoveGroup deletes all state of a group (for example after a partition
// moved to other nodes). It is durable when it returns. Existing Storage
// handles for the group return ErrGroupRemoved; the id can be reused.
func (l *Log) RemoveGroup(id uint64) error {
	return l.exec(func() error {
		l.mu.RLock()
		g, ok := l.groups[id]
		l.mu.RUnlock()
		if !ok {
			return nil
		}
		j := &job{st: newStage()}
		d := j.st.delta(l, id)
		d.remove = true
		l.writeRecord(j.st, recRemove, id, nil)
		l.runsClear(g)
		g.terms = nil
		if old := g.wsnap; old != nil && old.ref != nil {
			j.after = append(j.after, func() { l.dropSnapFile(id, old.meta.GetIndex()) })
		}
		g.wsnap = nil
		l.dispatch(j)
		l.drain()
		return l.failure()
	})
}

// ---- disk reads ----

// Adjacent records within this gap are fetched with one read.
const (
	coalesceGap  = 64 << 10
	coalesceSpan = 1 << 20
)

// readEntries reads entries [lo, hi) of a group from the segments, using
// the group's runs and terms as of the read, stopping once the size budget
// is exhausted (full reports that). born is where the group's first entry
// record is (see group.born).
func (l *Log) readEntries(id, lo, hi, maxSize uint64, runs []segRun, terms []termRun, born position) (out []*raftpb.Entry, full bool, err error) {
	var size uint64
	want := lo
	const chunk = 1024
	locs := make([]location, chunk)
	found := make([]bool, chunk)
	for _, r := range runs {
		if want >= hi {
			break
		}
		if r.hi < want {
			continue
		}
		if r.lo > want {
			return out, false, fmt.Errorf("%w: group %d: entry %d is not in any segment", ErrCorrupt, id, want)
		}
		x, err := l.segmentIndex(r.seg)
		if err != nil {
			return out, false, err
		}
		for want <= r.hi && want < hi {
			n := min(r.hi+1, hi, want+chunk) - want
			ls, fs := locs[:n], found[:n]
			clear(fs)
			if x.fill(r.seg, id, want, ls, fs) != int(n) {
				return out, false, fmt.Errorf("%w: group %d: entries [%d, %d) not all indexed in segment %d", ErrCorrupt, id, want, want+n, r.seg)
			}
			for k := range ls {
				t, ok := termAt(terms, want+uint64(k))
				if !ok {
					return out, false, fmt.Errorf("%w: group %d: term of entry %d unknown", ErrCorrupt, id, want+uint64(k))
				}
				ls[k].term = t
				// The segment's index leads to an older record than the live
				// one only if the live one is damaged and the index was rebuilt
				// from what remained (see replaySegment). An older record with
				// the same term holds the same entry, by raft's log matching,
				// unless a removed group with the same id wrote it.
				if born.after(ls[k]) {
					return out, false, fmt.Errorf("%w: group %d: entry %d: the only readable record in segment %d belongs to a removed group with the same id", ErrCorrupt, id, want+uint64(k), r.seg)
				}
			}
			for i := 0; i < len(ls); {
				// Coalesce a run of nearby records in one segment.
				j := i + 1
				for j < len(ls) && ls[j].start >= ls[j-1].end &&
					ls[j].start-ls[j-1].end <= coalesceGap && ls[j].end-ls[i].start <= coalesceSpan {
					j++
				}
				ents, err := l.readRun(id, want, ls[i:j])
				if err != nil {
					return out, false, err
				}
				for _, e := range ents {
					if !addLimited(&out, e, &size, maxSize) {
						return out, true, nil
					}
					want++
				}
				i = j
			}
		}
	}
	if want != hi {
		return out, false, fmt.Errorf("%w: group %d: entries [%d, %d) are not in any segment", ErrCorrupt, id, want, hi)
	}
	return out, false, nil
}

func (l *Log) readRun(id, first uint64, run []location) ([]*raftpb.Entry, error) {
	f, err := l.acquire(run[0].seg)
	if err != nil {
		return nil, err
	}
	defer l.release(run[0].seg)
	base := run[0].start
	buf := make([]byte, run[len(run)-1].end-base)
	if _, err := f.ReadAt(buf, base); err != nil {
		return nil, readErr(run[0].seg, base, err)
	}
	out := make([]*raftpb.Entry, 0, len(run))
	for k, loc := range run {
		payload, err := decodeChunks(buf[loc.start-base:loc.end-base], loc.start, l.key)
		if err != nil {
			return nil, fmt.Errorf("%w: segment %d record at %d: %v", ErrCorrupt, loc.seg, loc.start, err)
		}
		e, err := decodeEntry(payload, id, first+uint64(k))
		if err != nil {
			return nil, err
		}
		if e.GetTerm() != loc.term {
			return nil, fmt.Errorf("%w: group %d entry %d: term %d, index says %d", ErrCorrupt, id, e.GetIndex(), e.GetTerm(), loc.term)
		}
		out = append(out, e)
	}
	return out, nil
}

func decodeEntry(record []byte, id, index uint64) (*raftpb.Entry, error) {
	var e = &raftpb.Entry{}
	typ, gid, payload, err := decodeRecord(record)
	if err != nil || (typ != recEntry && typ != recRelocate) || gid != id {
		return e, fmt.Errorf("%w: group %d entry %d: record mismatch", ErrCorrupt, id, index)
	}
	if err := proto.Unmarshal(payload, e); err != nil {
		return e, fmt.Errorf("%w: group %d entry %d: %v", ErrCorrupt, id, index, err)
	}
	if e.GetIndex() != index {
		return e, fmt.Errorf("%w: group %d: record holds entry %d, want %d", ErrCorrupt, id, e.GetIndex(), index)
	}
	return e, nil
}

// addLimited appends e unless that would exceed maxSize (the first entry is
// always included). It reports whether the caller should continue.
func addLimited(out *[]*raftpb.Entry, e *raftpb.Entry, size *uint64, maxSize uint64) bool {
	s := uint64(entrySize(e))
	if len(*out) > 0 && *size+s > maxSize {
		return false
	}
	*out = append(*out, e)
	*size += s
	return true
}
