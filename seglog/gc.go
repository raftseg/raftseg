// Copyright 2026 The raftseg authors.

package seglog

import (
	"math"
	"sort"
)

// GCResult reports what one GC run did.
type GCResult struct {
	RemovedSegments  int
	CleanedSegments  int
	RelocatedEntries int
	RelocatedBytes   int64
}

// GC reclaims disk. It first cleans sealed segments that are at least
// CleanAfterSegments behind the active one and mostly dead: their few live
// entries (of any group, however large) are copied into the active segment.
// It then deletes every sealed segment that holds no live entry. GC also
// runs automatically whenever a segment is sealed.
func (l *Log) GC() (GCResult, error) {
	var res GCResult
	err := l.exec(func() error {
		var err error
		res, err = l.gcLocked()
		return err
	})
	return res, err
}

func (l *Log) gcLocked() (GCResult, error) {
	var res GCResult
	if l.dropIndex() || l.failure() != nil {
		return res, l.failure()
	}
	l.inGC = true
	defer func() { l.inGC = false }()

	budget := l.opts.CleanBudgetBytes
	for _, seg := range l.cleanCandidates() {
		if budget <= 0 {
			break
		}
		n, bytes, err := l.cleanSegment(seg)
		if err != nil {
			return res, err
		}
		res.CleanedSegments++
		res.RelocatedEntries += n
		res.RelocatedBytes += bytes
		budget -= bytes
		l.stats.cleanedSegments.Add(1)
		l.stats.relocatedEntries.Add(uint64(n))
		l.stats.relocatedBytes.Add(uint64(bytes))
	}

	var dead []uint64
	l.filesMu.Lock()
	for n, ss := range l.segs {
		if n != l.active && ss.meta.sealed && !ss.deleted && l.live[n] == 0 {
			dead = append(dead, n)
		}
	}
	l.filesMu.Unlock()
	if len(dead) == 0 {
		return res, nil
	}
	sort.Slice(dead, func(i, j int) bool { return dead[i] < dead[j] })
	// Checkpoint the state without them first. The checkpoint also makes
	// every compaction and relocation that freed them durable, so neither
	// that checkpoint nor any later one, nor a replay of what follows it,
	// can need a deleted segment.
	skip := map[uint64]bool{}
	for _, n := range dead {
		skip[n] = true
	}
	l.drain()
	if err := l.checkpoint(skip); err != nil {
		return res, l.fail(err)
	}
	for _, n := range dead {
		// Mark the segment deleted before its files go, so that no reader
		// (or Scrub) starts on it and finds them missing.
		l.filesMu.Lock()
		if ss := l.segs[n]; ss != nil {
			ss.deleted = true
			l.dropSegIndex(ss)
			if ss.refs == 0 {
				if ss.rf != nil {
					ss.rf.Close()
				}
				delete(l.segs, n)
			}
		}
		l.filesMu.Unlock()
		if err := l.removeSegmentFiles(n); err != nil {
			return res, l.fail(err)
		}
		delete(l.live, n)
		delete(l.liveSeen, n)
		res.RemovedSegments++
	}
	l.stats.gcSegments.Add(uint64(res.RemovedSegments))
	if err := syncDir(l.fs, l.segDir); err != nil {
		return res, l.fail(err)
	}
	return res, nil
}

// cleanCandidates returns old, mostly-dead sealed segments, emptiest first.
//
// A segment that is still dying on its own is left alone: if at least 1/16
// of its entries died since the previous GC run, the groups in it are
// compacting, and copying their entries forward would mostly copy entries
// about to be compacted anyway. (A queue whose consumers keep up empties
// its segments in order; cleaning them early multiplies writes.) A segment
// pinned by quiet groups stops changing and is cleaned at the next run.
func (l *Log) cleanCandidates() []uint64 {
	type cand struct {
		seg   uint64
		ratio float64
	}
	var cs []cand
	after := uint64(l.opts.CleanAfterSegments)
	l.filesMu.Lock()
	for n, ss := range l.segs {
		live := l.live[n]
		if ss.deleted || !ss.meta.sealed || live == 0 || ss.meta.entries == 0 {
			continue
		}
		prev, seen := l.liveSeen[n]
		l.liveSeen[n] = live
		if n+after > l.active {
			continue
		}
		if seen && prev >= live && prev-live >= max(ss.meta.entries/16, 1) {
			continue // still decaying
		}
		r := float64(live) / float64(ss.meta.entries)
		if r <= l.opts.CleanMaxLiveRatio {
			cs = append(cs, cand{n, r})
		}
	}
	l.filesMu.Unlock()
	sort.Slice(cs, func(i, j int) bool {
		if cs[i].ratio != cs[j].ratio {
			return cs[i].ratio < cs[j].ratio
		}
		return cs[i].seg < cs[j].seg
	})
	out := make([]uint64, len(cs))
	for i, c := range cs {
		out[i] = c.seg
	}
	return out
}

// cleanSegment copies every live entry of seg into the active segment as
// relocation records (which replay never treats as truncations).
func (l *Log) cleanSegment(seg uint64) (int, int64, error) {
	type part struct {
		id     uint64
		g      *group
		lo, hi uint64
	}
	var parts []part
	l.mu.RLock()
	for id, g := range l.groups {
		for _, r := range g.runs {
			if r.seg == seg {
				parts = append(parts, part{id, g, r.lo, r.hi})
			}
		}
	}
	l.mu.RUnlock()
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].id != parts[j].id {
			return parts[i].id < parts[j].id
		}
		return parts[i].lo < parts[j].lo
	})
	var moved int
	var bytes int64
	for _, p := range parts {
		ents, _, err := l.readEntries(p.id, p.lo, p.hi+1, math.MaxUint64, p.g.runs, p.g.terms, p.g.born)
		if err != nil {
			return moved, bytes, err
		}
		st := newStage()
		st.delta(l, p.id) // bump the group's version while its entries move
		mark := l.w.pos()
		var size int64
		for i := range ents {
			data, err := ents[i].Marshal()
			if err != nil {
				l.w.truncate(mark)
				return moved, bytes, err
			}
			loc := l.writeRecord(st, recRelocate, p.id, data)
			st.idx = append(st.idx, pendingIdx{p.id, ents[i].Index, loc.start, loc.end})
			size += loc.end - loc.start
		}
		l.runsMove(p.g, p.lo, p.hi, l.active)
		l.dispatch(&job{st: st})
		l.drain()
		if f := l.failure(); f != nil {
			return moved, bytes, f
		}
		moved += len(ents)
		bytes += size
	}
	return moved, bytes, nil
}

// Stats describes the log's current state.
type Stats struct {
	Groups           int
	Segments         []SegmentStats
	SegmentBytes     int64
	ActiveSegment    uint64
	CommitSeq        uint64
	Commits          uint64
	Fsyncs           uint64
	Checkpoints      uint64 // checkpoints written
	CheckpointBytes  uint64 // bytes written to checkpoint files
	IndexBytes       uint64 // bytes written to index sidecars
	IndexRebuilds    uint64 // segment indexes rebuilt by scanning their segment
	IndexCacheBytes  int64  // memory of cached sealed-segment indexes
	Records          uint64
	BytesWritten     uint64
	CacheBytes       int64
	CleanedSegments  uint64
	RelocatedEntries uint64
	RelocatedBytes   uint64
	RemovedSegments  uint64
	Recovery         RecoveryStats
}

// SegmentStats describes one live segment file.
type SegmentStats struct {
	Segment     uint64
	Sealed      bool
	Bytes       int64
	Records     uint64 // records written, including HardStates and markers
	Entries     uint64 // entries written (including relocated ones)
	LiveEntries uint64 // entries some group still needs
}

// Stats returns operational statistics.
func (l *Log) Stats() (Stats, error) {
	var s Stats
	err := l.exec(func() error {
		l.filesMu.Lock()
		for n, ss := range l.segs {
			if ss.deleted {
				continue
			}
			st := SegmentStats{Segment: n, Sealed: ss.meta.sealed, Bytes: ss.meta.end, Records: ss.meta.count, Entries: ss.meta.entries, LiveEntries: l.live[n]}
			if n == l.active {
				st.Bytes, st.Records, st.Entries = l.w.pos(), l.wsegCount, l.wsegEnts
			}
			s.Segments = append(s.Segments, st)
			s.SegmentBytes += st.Bytes
		}
		s.IndexCacheBytes = l.idxBytes
		l.filesMu.Unlock()
		sort.Slice(s.Segments, func(i, j int) bool { return s.Segments[i].Segment < s.Segments[j].Segment })
		l.mu.RLock()
		s.Groups, s.CacheBytes = len(l.groups), l.cacheBytes
		l.mu.RUnlock()
		s.ActiveSegment, s.CommitSeq = l.active, l.seq
		return nil
	})
	s.Commits = l.stats.commits.Load()
	s.Fsyncs = l.stats.fsyncs.Load()
	s.Checkpoints = l.stats.checkpoints.Load()
	s.CheckpointBytes = l.stats.checkpointBytes.Load()
	s.IndexBytes = l.stats.sidecarBytes.Load()
	s.IndexRebuilds = l.stats.indexRebuilds.Load()
	s.Records = l.stats.records.Load()
	s.BytesWritten = l.stats.bytes.Load()
	s.CleanedSegments = l.stats.cleanedSegments.Load()
	s.RelocatedEntries = l.stats.relocatedEntries.Load()
	s.RelocatedBytes = l.stats.relocatedBytes.Load()
	s.RemovedSegments = l.stats.gcSegments.Load()
	s.Recovery = l.recovery
	return s, err
}

// SegmentCount returns the number of live segment files.
func (l *Log) SegmentCount() int { return len(l.sortedSegs()) }
