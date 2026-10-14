// Copyright 2026 The raftseg authors.

package seglog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"sync"
)

// Entry index. Every segment has an index of the entry records it holds:
// for each group, a list of extents, each a run of consecutive entry
// indexes written one after another, with the offsets of each record (8
// bytes per entry). The same entry index can appear several times (a raft
// leader change overwrites a suffix; cleaning copies live entries forward);
// the latest record of an entry is the live one, and each group's runs (in
// memory) say which segment holds it.
//
// The active segment's index is built in memory as records are written.
// When a segment is sealed its index is written to a checksummed sidecar
// file, <segment>.idx. Sealed indexes are loaded on demand into a cache
// bounded by Options.IndexCacheBytes. A sidecar is only a cache of what the
// segment says: a missing or damaged one is rebuilt by scanning the segment.

type extent struct {
	lo   uint64
	locs []uint32 // start, end offset pairs
	vis  int      // entries readers may see: those of published commits
}

func (e *extent) n() int { return len(e.locs) / 2 }

type segIndex struct {
	mu      sync.RWMutex
	groups  map[uint64][]*extent
	entries int64
	extents int64
}

func newSegIndex() *segIndex { return &segIndex{groups: map[uint64][]*extent{}} }

// memBytes estimates the index's memory use.
func (x *segIndex) memBytes() int64 { return 8*x.entries + 64*x.extents + 64 }

// add records entry (g, idx) at [start, end). Callers hold x.mu for writing.
// A record that continues the group's latest extent extends it; anything
// else (an overwritten suffix, a relocated entry) starts a new extent.
func (x *segIndex) add(g, idx uint64, start, end int64) *extent {
	es := x.groups[g]
	if n := len(es); n > 0 {
		if e := es[n-1]; e.lo+uint64(e.n()) == idx {
			e.locs = append(e.locs, uint32(start), uint32(end))
			x.entries++
			return e
		}
	}
	e := &extent{lo: idx, locs: make([]uint32, 0, 8)}
	e.locs = append(e.locs, uint32(start), uint32(end))
	x.groups[g] = append(es, e)
	x.entries++
	x.extents++
	return e
}

// fill sets locs[k] (and found[k]) for each entry lo+k of group g held by
// this segment, from the latest visible record of each. It returns how many
// it set.
func (x *segIndex) fill(seg, g, lo uint64, locs []location, found []bool) int {
	x.mu.RLock()
	defer x.mu.RUnlock()
	hi := lo + uint64(len(locs)) - 1
	set := 0
	es := x.groups[g]
	for k := len(es) - 1; k >= 0 && set < len(locs); k-- {
		e := es[k]
		if e.vis == 0 {
			continue
		}
		a, b := max(lo, e.lo), min(hi, e.lo+uint64(e.vis)-1)
		for i := a; i <= b; i++ {
			j := i - lo
			if found[j] {
				continue
			}
			o := 2 * (i - e.lo)
			locs[j] = location{seg: seg, start: int64(e.locs[o]), end: int64(e.locs[o+1])}
			found[j] = true
			set++
		}
	}
	return set
}

// ---- sidecar files ----

const idxMagic = "RSI1"

// encode serialises the index of segment seg, which covers its first end
// bytes. Every extent must be fully visible.
func (x *segIndex) encode(seg uint64, end int64) []byte {
	x.mu.RLock()
	defer x.mu.RUnlock()
	ids := make([]uint64, 0, len(x.groups))
	for id := range x.groups {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	b := make([]byte, 0, 32+8*x.entries+16*x.extents)
	b = append(b, idxMagic...)
	b = binary.LittleEndian.AppendUint64(b, seg)
	b = binary.LittleEndian.AppendUint64(b, uint64(end))
	b = binary.LittleEndian.AppendUint64(b, uint64(x.entries))
	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {
		es := x.groups[id]
		b = binary.AppendUvarint(b, id)
		b = binary.AppendUvarint(b, uint64(len(es)))
		for _, e := range es {
			b = binary.AppendUvarint(b, e.lo)
			b = binary.AppendUvarint(b, uint64(e.n()))
			for _, o := range e.locs {
				b = binary.LittleEndian.AppendUint32(b, o)
			}
		}
	}
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, crcTable))
}

var errBadSidecar = errors.New("bad index sidecar")

// decodeSegIndex parses a sidecar, checking that it belongs to segment seg
// covering end bytes with the given number of entries.
func decodeSegIndex(b []byte, seg uint64, end int64, entries uint64) (*segIndex, error) {
	if len(b) < 4+24+1+4 || string(b[:4]) != idxMagic {
		return nil, errBadSidecar
	}
	body := b[:len(b)-4]
	if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return nil, errBadSidecar
	}
	if binary.LittleEndian.Uint64(body[4:]) != seg || int64(binary.LittleEndian.Uint64(body[12:])) != end ||
		binary.LittleEndian.Uint64(body[20:]) != entries {
		return nil, errBadSidecar
	}
	p := body[28:]
	uv := func() (uint64, bool) {
		v, n := binary.Uvarint(p)
		if n <= 0 {
			return 0, false
		}
		p = p[n:]
		return v, true
	}
	x := newSegIndex()
	ng, ok := uv()
	if !ok {
		return nil, errBadSidecar
	}
	for ; ng > 0; ng-- {
		id, ok1 := uv()
		ne, ok2 := uv()
		if !ok1 || !ok2 || ne > uint64(len(p)) {
			return nil, errBadSidecar
		}
		es := make([]*extent, 0, ne)
		for ; ne > 0; ne-- {
			lo, ok1 := uv()
			n, ok2 := uv()
			if !ok1 || !ok2 || n == 0 || n > uint64(len(p))/8 {
				return nil, errBadSidecar
			}
			e := &extent{lo: lo, locs: make([]uint32, 2*n), vis: int(n)}
			for k := range e.locs {
				e.locs[k] = binary.LittleEndian.Uint32(p[4*k:])
			}
			for k := 0; k < len(e.locs); k += 2 {
				if e.locs[k+1] <= e.locs[k] || int64(e.locs[k+1]) > end {
					return nil, errBadSidecar
				}
			}
			p = p[8*n:]
			es = append(es, e)
			x.entries += int64(n)
			x.extents++
		}
		x.groups[id] = es
	}
	if len(p) != 0 || uint64(x.entries) != entries {
		return nil, errBadSidecar
	}
	return x, nil
}

// scanIndex rebuilds a segment's index from its first end bytes, which must
// hold complete, valid commits.
func scanIndex(f File, seg uint64, end int64, key uint32) (*segIndex, error) {
	sc, err := newSegScanner(f, key)
	if err != nil {
		return nil, err
	}
	if sc.size < end {
		return nil, fmt.Errorf("%w: segment %d is %d bytes, expected at least %d", ErrCorrupt, seg, sc.size, end)
	}
	sc.size = end
	x := newSegIndex()
	var pending []rawRecord
	for pos := int64(0); ; {
		rec, err := sc.next(pos)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%w: segment %d: rebuilding its index: %v", ErrCorrupt, seg, err)
		}
		pos = rec.end
		if len(rec.data) == 0 {
			return nil, fmt.Errorf("%w: segment %d: empty record at %d", ErrCorrupt, seg, rec.start)
		}
		if rec.data[0] != recCommit {
			pending = append(pending, rec)
			continue
		}
		for _, p := range pending {
			if err := x.addRecord(p); err != nil {
				return nil, fmt.Errorf("%w: segment %d at %d: %v", ErrCorrupt, seg, p.start, err)
			}
		}
		pending = pending[:0]
	}
	if len(pending) > 0 {
		return nil, fmt.Errorf("%w: segment %d: rebuilding its index: incomplete commit", ErrCorrupt, seg)
	}
	for _, es := range x.groups {
		for _, e := range es {
			e.vis = e.n()
		}
	}
	return x, nil
}

// addRecord indexes rec if it holds an entry.
func (x *segIndex) addRecord(rec rawRecord) error {
	typ, g, payload, err := decodeRecord(rec.data)
	if err != nil {
		return err
	}
	if typ != recEntry && typ != recRelocate {
		return nil
	}
	idx, _, err := entryHeader(payload)
	if err != nil {
		return err
	}
	x.add(g, idx, rec.start, rec.end)
	return nil
}

func (l *Log) idxPath(n uint64) string { return l.fs.PathJoin(l.segDir, idxName(n)) }

// writeSidecar writes segment n's index durably (temporary file, sync,
// rename). The caller syncs the directory.
func (l *Log) writeSidecar(n uint64, x *segIndex, end int64) error {
	data := x.encode(n, end)
	final := l.idxPath(n)
	tmp := final + ".tmp"
	f, err := l.fs.Create(tmp)
	if err != nil {
		return err
	}
	_, err = f.WriteAt(data, 0)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = l.fs.Rename(tmp, final)
	}
	if err != nil {
		l.fs.Remove(tmp)
		return err
	}
	l.stats.sidecarBytes.Add(uint64(len(data)))
	return nil
}

// loadIndex reads segment n's sidecar, or rebuilds the index by scanning
// the segment if the sidecar is missing or does not match it.
func (l *Log) loadIndex(n uint64, m segMeta) (*segIndex, error) {
	if f, err := l.fs.Open(l.idxPath(n)); err == nil {
		st, err := f.Stat()
		var x *segIndex
		if err == nil && st.Size() > maxMetaFileBytes {
			err = errBadSidecar
		}
		if err == nil {
			buf := make([]byte, st.Size())
			if _, err = f.ReadAt(buf, 0); err == nil || errors.Is(err, io.EOF) {
				x, err = decodeSegIndex(buf, n, m.end, m.entries)
			}
		}
		f.Close()
		if err == nil && x != nil {
			return x, nil
		}
	}
	l.stats.indexRebuilds.Add(1)
	f, err := l.fs.Open(l.segPath(n))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	x, err := scanIndex(f, n, m.end, l.key)
	if err != nil {
		return nil, err
	}
	if uint64(x.entries) != m.entries {
		return nil, fmt.Errorf("%w: segment %d holds %d entries, expected %d", ErrCorrupt, n, x.entries, m.entries)
	}
	return x, nil
}

// segmentIndex returns segment n's index, loading it into the cache if
// needed.
func (l *Log) segmentIndex(n uint64) (*segIndex, error) {
	l.filesMu.Lock()
	ss, ok := l.segs[n]
	if !ok || ss.deleted {
		l.filesMu.Unlock()
		return nil, fmt.Errorf("seglog: segment %d not available", n)
	}
	l.idxTick++
	ss.idxUsed = l.idxTick
	if x := ss.idx; x != nil {
		l.filesMu.Unlock()
		return x, nil
	}
	m := ss.meta
	l.filesMu.Unlock()
	x, err := l.loadIndex(n, m)
	if err != nil {
		return nil, err
	}
	l.filesMu.Lock()
	defer l.filesMu.Unlock()
	if ss.idx != nil {
		return ss.idx, nil
	}
	if ss.deleted {
		return x, nil
	}
	ss.idx = x
	l.idxBytes += x.memBytes()
	l.evictIndexes()
	return x, nil
}

// evictIndexes drops the least recently used sealed-segment indexes until
// the cache fits its budget. Callers hold filesMu.
func (l *Log) evictIndexes() {
	for l.idxBytes > l.opts.IndexCacheBytes {
		var vs *segState
		for _, ss := range l.segs {
			if ss.idx != nil && !ss.pinned && (vs == nil || ss.idxUsed < vs.idxUsed) {
				vs = ss
			}
		}
		if vs == nil {
			return
		}
		l.idxBytes -= vs.idx.memBytes()
		vs.idx = nil
	}
}

// dropSegIndex forgets a segment's cached index. Callers hold filesMu.
func (l *Log) dropSegIndex(ss *segState) {
	if ss.idx != nil {
		l.idxBytes -= ss.idx.memBytes()
		ss.idx = nil
	}
}
