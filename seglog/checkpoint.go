// Copyright 2026 The raftseg authors.

package seglog

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/raftseg/raft/raftpb"
)

// Checkpoints. Every change to the log's state is a record in the segments,
// so the segments alone could rebuild it; a checkpoint is the state as of a
// commit, so that Open only replays what followed. It holds every group's
// metadata, HardState, ConfState and snapshot (inline data or a reference to
// its file), which segment holds which of its entries, and its terms, plus
// the list of segments.
//
// A checkpoint is written at every segment seal, before GC deletes any
// segment, at a clean Close and every CheckpointInterval. It goes to two
// files, CHECKPOINT-A and CHECKPOINT-B, each written to a temporary file,
// synced and renamed, then the directory is synced. Open uses the newest
// copy that passes its checksum. GC deletes segments only after both copies
// of a checkpoint that no longer needs them are durable, so falling back to
// an older checkpoint never needs a deleted segment.

const ckptMagic = "RSC1"

// errUnsupportedFormat: a checkpoint written by a version with another
// on-disk format. That is not corruption, and Open says so.
var errUnsupportedFormat = errors.New("seglog: unsupported on-disk format")

// newKey returns a random, non-zero checksum key for a new log.
func newKey() (uint32, error) {
	var b [4]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			return 0, err
		}
		if k := binary.LittleEndian.Uint32(b[:]); k != 0 {
			return k, nil
		}
	}
}

// maxMetaFileBytes bounds the checkpoint and index sidecar files Open reads
// into memory. Both are far smaller in practice: a checkpoint holds a few
// dozen bytes per group, a sidecar 8 bytes per entry of a segment of at most
// 1 GiB.
const maxMetaFileBytes = 1 << 30

var ckptNames = [2]string{"CHECKPOINT-A", "CHECKPOINT-B"}

// checkpoint is a decoded checkpoint file.
type checkpoint struct {
	key    uint32 // the log's checksum key
	gen    uint64
	seq    uint64 // last commit reflected
	seg    uint64 // replay starts in this segment...
	off    int64  // ...at this offset
	segs   map[uint64]segMeta
	groups map[uint64]*group
}

type ckptWriter struct{ b []byte }

func (w *ckptWriter) uv(v uint64) { w.b = binary.AppendUvarint(w.b, v) }
func (w *ckptWriter) bytes(p []byte) {
	w.uv(uint64(len(p)))
	w.b = append(w.b, p...)
}

type ckptReader struct {
	b   []byte
	err error
}

func (r *ckptReader) uv() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = errors.New("bad varint")
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *ckptReader) bytes() []byte {
	n := r.uv()
	if r.err != nil {
		return nil
	}
	if n > uint64(len(r.b)) {
		r.err = errors.New("bad length")
		return nil
	}
	p := r.b[:n]
	r.b = r.b[n:]
	return p
}

// encodeCheckpoint serialises the writer's state. It must run on the writer
// goroutine (or with the writer stopped) after every dispatched commit has
// been published. Segments in skip are left out.
func (l *Log) encodeCheckpoint(gen uint64, skip map[uint64]bool) ([]byte, error) {
	w := &ckptWriter{b: make([]byte, 0, 4096)}
	w.b = append(w.b, ckptMagic...)
	w.uv(formatVersion)
	w.uv(uint64(l.key))
	w.uv(gen)
	w.uv(l.seq)
	w.uv(l.active)
	w.uv(uint64(l.w.pos()))

	l.filesMu.Lock()
	segs := make([]uint64, 0, len(l.segs))
	metas := map[uint64]segMeta{}
	for n, ss := range l.segs {
		if ss.deleted || skip[n] {
			continue
		}
		m := ss.meta
		if n == l.active {
			m = segMeta{count: l.wsegCount, entries: l.wsegEnts, end: l.w.pos()}
		}
		segs = append(segs, n)
		metas[n] = m
	}
	l.filesMu.Unlock()
	sort.Slice(segs, func(i, j int) bool { return segs[i] < segs[j] })
	w.uv(uint64(len(segs)))
	for _, n := range segs {
		m := metas[n]
		w.uv(n)
		w.uv(m.count)
		w.uv(m.entries)
		w.uv(uint64(m.end))
		if m.sealed {
			w.uv(1)
		} else {
			w.uv(0)
		}
	}

	l.mu.RLock()
	ids := make([]uint64, 0, len(l.groups))
	for id := range l.groups {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	w.uv(uint64(len(ids)))
	var err error
	for _, id := range ids {
		g := l.groups[id]
		w.uv(id)
		w.uv(g.wmeta.first)
		w.uv(g.wmeta.last)
		w.uv(g.wmeta.compactTerm)
		w.uv(g.born.seg)
		w.uv(uint64(g.born.off))
		hs, e1 := proto.Marshal(g.hs)
		cs, e2 := proto.Marshal(g.cs)
		if err = errors.Join(e1, e2); err != nil {
			break
		}
		w.bytes(hs)
		w.bytes(cs)
		if g.wsnap == nil {
			w.bytes(nil)
		} else {
			rec, err2 := encodeSnapRecord(0, g.wsnap)
			if err = err2; err != nil {
				break
			}
			w.bytes(rec)
		}
		w.uv(uint64(len(g.runs)))
		for _, r := range g.runs {
			w.uv(r.seg)
			w.uv(r.lo)
			w.uv(r.hi)
		}
		w.uv(uint64(len(g.terms)))
		for _, t := range g.terms {
			w.uv(t.index)
			w.uv(t.term)
		}
	}
	l.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	return binary.LittleEndian.AppendUint32(w.b, crc32.Checksum(w.b, crcTable)), nil
}

func decodeCheckpoint(b []byte) (*checkpoint, error) {
	if len(b) < 8 || string(b[:4]) != ckptMagic {
		return nil, errors.New("not a checkpoint")
	}
	body := b[:len(b)-4]
	if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return nil, errors.New("checksum mismatch")
	}
	r := &ckptReader{b: body[4:]}
	if v := r.uv(); r.err == nil && v != formatVersion {
		return nil, fmt.Errorf("%w: on-disk format %d, this version reads format %d", errUnsupportedFormat, v, formatVersion)
	}
	c := &checkpoint{segs: map[uint64]segMeta{}, groups: map[uint64]*group{}}
	if key := r.uv(); key > math.MaxUint32 {
		r.err = errors.New("bad checksum key")
	} else {
		c.key = uint32(key)
	}
	c.gen, c.seq, c.seg, c.off = r.uv(), r.uv(), r.uv(), int64(r.uv())
	for ns := r.uv(); ns > 0 && r.err == nil; ns-- {
		n := r.uv()
		m := segMeta{count: r.uv(), entries: r.uv(), end: int64(r.uv()), sealed: r.uv() == 1}
		c.segs[n] = m
	}
	for ng := r.uv(); ng > 0 && r.err == nil; ng-- {
		id := r.uv()
		g := newGroup()
		g.wmeta = groupMeta{first: r.uv(), last: r.uv(), compactTerm: r.uv()}
		g.born = position{seg: r.uv(), off: int64(r.uv())}
		hs, cs, snap := r.bytes(), r.bytes(), r.bytes()
		if r.err != nil {
			break
		}
		var dhs raftpb.HardState
		if err := proto.Unmarshal(hs, &dhs); err != nil {
			return nil, fmt.Errorf("group %d: hard state: %v", id, err)
		}
		var dcs raftpb.ConfState
		if err := proto.Unmarshal(cs, &dcs); err != nil {
			return nil, fmt.Errorf("group %d: conf state: %v", id, err)
		}
		g.hs, g.cs = copyHardState(&dhs), copyConfState(&dcs)
		if len(snap) > 0 {
			_, s, err := decodeSnapRecord(snap)
			if err != nil {
				return nil, fmt.Errorf("group %d: %v", id, err)
			}
			g.wsnap = s
		}
		for nr := r.uv(); nr > 0 && r.err == nil; nr-- {
			g.runs = append(g.runs, segRun{seg: r.uv(), lo: r.uv(), hi: r.uv()})
		}
		for nt := r.uv(); nt > 0 && r.err == nil; nt-- {
			g.terms = append(g.terms, termRun{index: r.uv(), term: r.uv()})
		}
		c.groups[id] = g
	}
	if r.err == nil && len(r.b) != 0 {
		r.err = errors.New("trailing bytes")
	}
	if r.err != nil {
		return nil, r.err
	}
	return c, nil
}

// readCheckpoint returns the newest valid checkpoint, or nil if there is
// none. It fails if copies exist but none is valid.
func (l *Log) readCheckpoint() (*checkpoint, error) {
	var best *checkpoint
	var errs []error
	found := 0
	for _, name := range ckptNames {
		f, err := l.fs.Open(l.fs.PathJoin(l.dir, name))
		if err != nil {
			continue
		}
		found++
		var data []byte
		if st, err2 := f.Stat(); err2 != nil {
			err = err2
		} else if st.Size() > maxMetaFileBytes {
			err = fmt.Errorf("implausible size %d", st.Size())
		} else {
			data = make([]byte, st.Size())
			if _, err = f.ReadAt(data, 0); errors.Is(err, io.EOF) {
				err = nil
			}
		}
		f.Close()
		var c *checkpoint
		if err == nil {
			c, err = decodeCheckpoint(data)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			continue
		}
		if best == nil || c.gen > best.gen {
			best = c
		}
	}
	if best == nil && found > 0 {
		for _, e := range errs {
			if errors.Is(e, errUnsupportedFormat) {
				return nil, fmt.Errorf("%s: %w", l.dir, e)
			}
		}
		return nil, fmt.Errorf("%w: no valid checkpoint: %v", ErrCorrupt, errors.Join(errs...))
	}
	if len(errs) > 0 && best != nil {
		l.recovery.Repairs = append(l.recovery.Repairs, fmt.Sprintf("ignored a damaged checkpoint copy: %v", errors.Join(errs...)))
	}
	return best, nil
}

// checkpoint writes the current state to both checkpoint files. See the
// comment at the top of this file for when it may run.
func (l *Log) checkpoint(skip map[uint64]bool) error {
	data, err := l.encodeCheckpoint(l.ckptGen+1, skip)
	if err != nil {
		return err
	}
	for _, name := range ckptNames {
		final := l.fs.PathJoin(l.dir, name)
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
	}
	if err := syncDir(l.fs, l.dir); err != nil {
		return err
	}
	l.ckptGen++
	l.lastCheckpoint.Store(time.Now().UnixNano())
	l.stats.checkpoints.Add(1)
	l.stats.checkpointBytes.Add(uint64(2 * len(data)))
	return nil
}

// ---- terms ----

// termRun says that entries from index on have term, up to the next run.
type termRun struct{ index, term uint64 }

// termAt returns the term of entry i, which must be within the log.
func termAt(ts []termRun, i uint64) (uint64, bool) {
	k := sort.Search(len(ts), func(k int) bool { return ts[k].index > i })
	if k == 0 {
		return 0, false
	}
	return ts[k-1].term, true
}

// termsTruncate forgets entries from index on.
func termsTruncate(ts []termRun, from uint64) []termRun {
	k := sort.Search(len(ts), func(k int) bool { return ts[k].index >= from })
	return ts[:k]
}

// termsAppend records consecutive entries following the last one known.
func termsAppend(ts []termRun, ents []*raftpb.Entry) []termRun {
	for i := range ents {
		if n := len(ts); n == 0 || ts[n-1].term != ents[i].GetTerm() {
			ts = append(ts, termRun{ents[i].GetIndex(), ents[i].GetTerm()})
		}
	}
	return ts
}

// termsCompact forgets entries below first.
func termsCompact(ts []termRun, first uint64) []termRun {
	k := sort.Search(len(ts), func(k int) bool { return ts[k].index > first })
	if k == 0 {
		return ts
	}
	out := append([]termRun(nil), ts[k-1:]...)
	if out[0].index < first {
		out[0].index = first
	}
	return out
}
