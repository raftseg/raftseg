// Copyright 2026 The raftseg authors.

package seglog

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
)

// ScrubReport is the result of Scrub. An empty Problems list means every
// check passed.
type ScrubReport struct {
	Segments       int
	Commits        int
	Records        int
	IndexedEntries int
	Problems       []string
}

func (r *ScrubReport) problem(format string, a ...interface{}) {
	if len(r.Problems) < 1000 {
		r.Problems = append(r.Problems, fmt.Sprintf(format, a...))
	}
}

// Scrub verifies the whole log without blocking writers:
//   - every segment parses into complete, CRC-valid commits whose markers
//     match their records and whose sequence numbers increase by one;
//   - sealed segments contain nothing but zeroes after their last commit,
//     and their index sidecar matches what the segment holds;
//   - every entry of every group, from FirstIndex to LastIndex, is found
//     where the index says, holding that group, index and term;
//   - snapshot files match their size and checksum.
//
// Problems are reported, not repaired. Scrub returns an error only if it
// could not run.
func (l *Log) Scrub() (ScrubReport, error) {
	var rep ScrubReport
	if err := l.enter(); err != nil {
		return rep, err
	}
	defer l.exit()

	// Fix the scan bounds first: sealed segments to their end, the active
	// one to what has been published.
	type bound struct {
		seg    uint64
		end    int64
		sealed bool
	}
	var bounds []bound
	for _, n := range l.sortedSegs() {
		l.filesMu.Lock()
		ss := l.segs[n]
		var b bound
		if ss != nil {
			b = bound{n, ss.meta.end, ss.meta.sealed}
		}
		l.filesMu.Unlock()
		if ss != nil {
			bounds = append(bounds, b)
		}
	}

	var lastSeq uint64
	for _, b := range bounds {
		f, err := l.acquire(b.seg)
		if err != nil {
			continue // removed by GC since the listing
		}
		rep.Segments++
		sc, err := newSegScanner(f, l.key)
		if err != nil {
			l.release(b.seg)
			return rep, err
		}
		if b.end > sc.size {
			rep.problem("segment %d: %d bytes on disk, %d expected", b.seg, sc.size, b.end)
			l.release(b.seg)
			continue
		}
		if !b.sealed {
			sc.size = b.end
		}
		x := newSegIndex()
		var pending []rawRecord
		var segSeq uint64
		pos := int64(0)
		for {
			rec, err := sc.next(pos)
			if err == io.EOF {
				break
			}
			var bd *badData
			if errors.As(err, &bd) {
				if zero, _ := sc.allZero(pos); !zero || len(pending) > 0 || pos < b.end {
					rep.problem("segment %d: %v", b.seg, bd)
				}
				break
			}
			if err != nil {
				l.release(b.seg)
				return rep, err
			}
			rep.Records++
			pos = rec.end
			typ, _, payload, derr := decodeRecord(rec.data)
			if derr != nil {
				rep.problem("segment %d at %d: %v", b.seg, rec.start, derr)
				continue
			}
			if typ != recCommit {
				pending = append(pending, rec)
				continue
			}
			m, derr := decodeCommitMarker(payload)
			switch {
			case derr != nil:
				rep.problem("segment %d at %d: %v", b.seg, rec.start, derr)
			case len(pending) == 0 || m.nrecs != uint64(len(pending)) || m.start != pending[0].start:
				rep.problem("segment %d: commit %d at %d does not match its records", b.seg, m.seq, rec.start)
			case segSeq != 0 && m.seq != segSeq+1:
				rep.problem("segment %d: commit sequence jumps from %d to %d", b.seg, segSeq, m.seq)
			case segSeq == 0 && m.seq <= lastSeq:
				rep.problem("segment %d: commit sequence %d does not follow %d", b.seg, m.seq, lastSeq)
			}
			for _, p := range pending {
				if err := x.addRecord(p); err != nil {
					rep.problem("segment %d at %d: %v", b.seg, p.start, err)
				}
			}
			lastSeq, segSeq = m.seq, m.seq
			rep.Commits++
			pending = pending[:0]
		}
		if len(pending) > 0 {
			rep.problem("segment %d: %d records after the last commit marker", b.seg, len(pending))
		}
		if pos < b.end {
			rep.problem("segment %d: valid data ends at %d, expected %d", b.seg, pos, b.end)
		}
		l.release(b.seg)
		if b.sealed {
			// The sidecar must say exactly what the segment holds, unless GC
			// deleted both since they were listed.
			data, err := l.readFile(l.idxPath(b.seg))
			switch {
			case err != nil && l.segGone(b.seg):
			case err != nil:
				rep.problem("segment %d: index sidecar: %v", b.seg, err)
			case !bytes.Equal(data, x.encode(b.seg, b.end)):
				rep.problem("segment %d: index sidecar does not match the segment", b.seg)
			}
		}
	}

	// Every entry must be where the index says.
	l.mu.RLock()
	ids := make([]uint64, 0, len(l.groups))
	for id := range l.groups {
		ids = append(ids, id)
	}
	l.mu.RUnlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		var probs []string
		var checked int
		for attempt := 0; attempt < 5; attempt++ {
			probs, checked = l.scrubGroup(id)
			if probs != nil || checked >= 0 {
				break
			}
		}
		if checked < 0 {
			continue // kept changing or was removed
		}
		rep.IndexedEntries += checked
		for _, p := range probs {
			rep.problem("%s", p)
		}
	}
	return rep, nil
}

// scrubGroup checks every entry and the snapshot of one group against a
// stable view of it. It returns checked < 0 if the group changed meanwhile.
func (l *Log) scrubGroup(id uint64) (probs []string, checked int) {
	l.mu.RLock()
	g := l.groups[id]
	if g == nil {
		l.mu.RUnlock()
		return nil, -1
	}
	for g.version&1 == 1 && !g.removed {
		l.cond.Wait()
	}
	v, m, runs, terms, snap, born := g.version, g.meta, g.pruns, g.pterms, g.snap, g.born
	l.mu.RUnlock()
	stable := func() bool {
		l.mu.RLock()
		defer l.mu.RUnlock()
		return g.version == v && !g.removed
	}
	add := func(format string, a ...interface{}) { probs = append(probs, fmt.Sprintf(format, a...)) }
	next := m.first
	for _, r := range runs {
		if r.lo != next {
			add("group %d: entries jump from %d to %d", id, next, r.lo)
		}
		next = r.hi + 1
		for lo := r.lo; lo <= r.hi; {
			n := min(r.hi-lo+1, 4096)
			ents, _, err := l.readEntries(id, lo, lo+n, ^uint64(0), []segRun{{r.seg, lo, lo + n - 1}}, terms, born)
			if err != nil {
				if !stable() {
					return nil, -1
				}
				add("group %d entries [%d, %d] in segment %d: %v", id, lo, lo+n-1, r.seg, err)
			}
			checked += len(ents)
			lo += n
		}
	}
	if m.first <= m.last && next != m.last+1 {
		add("group %d: entries known up to %d, last is %d", id, next-1, m.last)
	}
	if snap != nil && snap.ref != nil {
		if _, err := l.readSnapFile(id, *snap.ref); err != nil {
			add("group %d: %v", id, err)
		}
	}
	if !stable() {
		return nil, -1
	}
	return probs, checked
}

// segGone reports whether segment n has been deleted by GC.
func (l *Log) segGone(n uint64) bool {
	l.filesMu.Lock()
	defer l.filesMu.Unlock()
	ss, ok := l.segs[n]
	return !ok || ss.deleted
}

func (l *Log) readFile(path string) ([]byte, error) {
	f, err := l.fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	data := make([]byte, st.Size())
	if _, err := f.ReadAt(data, 0); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return data, nil
}
