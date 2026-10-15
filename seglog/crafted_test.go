// Copyright 2026 The raftseg authors.

package seglog

// Crafted files. Checksums catch random damage, but not a file that is
// well-formed and wrong: written by a bug, by an older or newer version, by
// an operator's tool, or by someone with write access to the disk. These
// fuzzers write segment records and checkpoints whose framing and checksums
// are valid and whose contents are arbitrary. Open may refuse them; it must
// not panic, hang or allocate without bound, and if it opens, every API call
// must return without panicking.

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
	randv2 "math/rand/v2"
)

// craftIndex picks an index: mostly near the end of the logs the tests
// build (indexes 1 to 10), sometimes an edge or a wrapping value.
func craftIndex(r *opReader) uint64 {
	if c := r.byte(); c%2 == 0 {
		return uint64(6 + c%8)
	}
	switch c := r.byte(); c % 10 {
	case 0:
		return 0
	case 1:
		return math.MaxUint64
	case 2:
		return math.MaxUint64 - uint64(r.byte()%3)
	case 3:
		return 1 << 63
	case 4:
		return maxIndex
	case 5:
		return uint64(r.u16())
	default:
		return uint64(c % 40)
	}
}

// craftRecord builds one record from fuzz input: structured most of the
// time, raw bytes sometimes.
func craftRecord(r *opReader) []byte {
	// Mostly entries and HardStates; any type, valid or not, sometimes.
	typ := []byte{recEntry, recEntry, recEntry, recHardState, recHardState, recSnapshot, recConfState, recRemove, recRelocate, recEntry}[r.byte()%10]
	if r.byte()%12 == 0 {
		typ = r.byte() % 9 // 0 and 8 are not record types
	}
	group := uint64(1 + r.byte()%3)
	if r.byte()%16 == 0 {
		group = []uint64{0, 4, math.MaxUint64}[r.byte()%3]
	}
	var payload []byte
	switch {
	case r.byte()%20 == 0:
		payload = make([]byte, r.byte())
		for i := range payload {
			payload[i] = r.byte()
		}
	case typ == recEntry || typ == recRelocate:
		e := &raftpb.Entry{Index: new(craftIndex(r)), Term: new(craftIndex(r)), Data: make([]byte, r.byte())}
		payload, _ = proto.Marshal(e)
	case typ == recHardState:
		hs := &raftpb.HardState{Term: new(craftIndex(r)), Vote: new(uint64(r.byte())), Commit: new(craftIndex(r))}
		payload, _ = proto.Marshal(hs)
	case typ == recConfState:
		cs := &raftpb.ConfState{Voters: []uint64{uint64(r.byte())}, Learners: []uint64{uint64(r.byte())}}
		payload, _ = proto.Marshal(cs)
	case typ == recSnapshot:
		si := &snapInfo{meta: &raftpb.SnapshotMetadata{Index: new(craftIndex(r)), Term: new(craftIndex(r))}}
		if r.byte()%2 == 0 {
			si.data = make([]byte, r.byte())
		}
		if r.byte()%3 == 0 {
			si.ref = &snapRef{index: si.meta.GetIndex(), size: craftIndex(r) << (r.byte() % 40), crc: uint32(r.u16())}
			si.data = nil
			if r.byte()%4 == 0 {
				si.ref.index = craftIndex(r)
			}
		}
		payload, _ = encodeSnapRecord(r.byte()&7, si)
	case typ == recCommit:
		m := commitMarker{seq: craftIndex(r), nrecs: craftIndex(r), start: int64(r.u16()), synced: int64(r.u16())}
		payload = m.encode(nil)
	}
	return encodeRecord(nil, typ, group, payload)
}

// writeCrafted appends crafted commits to the active segment through the
// log's own writer, so that framing, offsets and commit markers are valid,
// then crashes the log, so that Open must replay them. With raw, the commit
// markers' fields are crafted too.
func writeCrafted(t *testing.T, l *Log, r *opReader) {
	t.Helper()
	err := l.exec(func() error {
		for c := 0; c < 1+int(r.byte()%4); c++ {
			j := &job{st: newStage()}
			n := 1 + int(r.byte()%5)
			for i := 0; i < n; i++ {
				l.writeEncoded(j.st, craftRecord(r))
			}
			if r.byte()%8 == 0 {
				// A commit whose marker lies: written by hand.
				l.writeEncoded(j.st, encodeRecord(nil, recCommit, 0, commitMarker{seq: l.seq + uint64(r.byte()%3), nrecs: uint64(r.byte() % 8), start: j.st.firstStart + int64(r.byte()%3), synced: int64(r.u16())}.encode(nil)))
				l.w.flush()
				continue
			}
			l.dispatch(j)
			l.drain()
		}
		return nil
	})
	if err != nil {
		t.Logf("crafting: %v", err)
	}
	l.crash()
}

// exercise calls every read and write API on the groups that may exist,
// and checks only that they return. It fails the test if a call hangs.
func exercise(t *testing.T, l *Log, r *opReader) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ids := append(l.Groups(), 0, 1, 2, 3, 4, math.MaxUint64)
		for _, id := range ids {
			s := l.Group(id)
			first, _ := s.FirstIndex()
			last, _ := s.LastIndex()
			s.InitialState()
			s.Snapshot()
			for _, i := range []uint64{0, first - 1, first, last, last + 1, math.MaxUint64} {
				s.Term(i)
			}
			if last >= first && last-first < 1<<20 {
				s.Entries(first, last+1, math.MaxUint64)
				s.Entries(first, last+1, 0)
			}
			s.Entries(0, math.MaxUint64, 100)
		}
		l.Scrub()
		l.Stats()
		for _, id := range ids[:min(3, len(ids))] {
			s := l.Group(id)
			last, _ := s.LastIndex()
			hs, _, _ := s.InitialState()
			s.Save(&raftpb.HardState{Term: new(hs.GetTerm() + 1), Commit: hs.Commit}, []*raftpb.Entry{{Index: new(last + 1), Term: new(hs.GetTerm() + 1)}}, &raftpb.Snapshot{})
			s.CreateSnapshot(last, nil, []byte("x"))
			s.Compact(last)
		}
		l.GC()
		l.RemoveGroup(uint64(r.byte() % 5))
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("API calls on a crafted log hung")
	}
}

// openWithin opens the log, failing the test if Open hangs.
func openWithin(t *testing.T, fs *crashfs.FS, opts Options) (*Log, error) {
	t.Helper()
	type res struct {
		l   *Log
		err error
	}
	ch := make(chan res, 1)
	go func() {
		l, err := Open(crashDir, memOpts(fs, opts))
		ch <- res{l, err}
	}()
	select {
	case r := <-ch:
		return r.l, r.err
	case <-time.After(20 * time.Second):
		t.Fatal("Open of a crafted log hung")
		return nil, nil
	}
}

// craftedOutcomes counts how crafted logs fared: refused, opened.
var craftedOutcomes [2]atomic.Int64

func runCraftedRecords(t *testing.T, input []byte) {
	r := &opReader{b: input}
	mem := crashfs.New()
	opts := Options{SegmentSize: 64 << 10, SnapshotInlineBytes: 64, CacheEntries: int(r.byte()%3) - 1}
	l, err := Open(crashDir, memOpts(mem, opts))
	if err != nil {
		t.Fatal(err)
	}
	// Some ordinary state first.
	for g := uint64(1); g <= 3; g++ {
		if err := l.Save(Update{Group: g, Entries: ents(1, 1, 10, 20), HardState: &raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(5))}}); err != nil {
			t.Fatal(err)
		}
	}
	if r.byte()%2 == 0 {
		if _, err := l.Group(2).CreateSnapshot(4, &raftpb.ConfState{Voters: []uint64{1}}, make([]byte, 100)); err != nil {
			t.Fatal(err)
		}
	}
	writeCrafted(t, l, r)
	for round := 0; round < 2; round++ {
		img := mem.CrashClone(100, randv2.New(randv2.NewPCG(1, 1)))
		l, err := openWithin(t, img, Options{DiscardDamagedTail: r.byte()%2 == 0})
		if err != nil {
			craftedOutcomes[0].Add(1)
			return
		}
		craftedOutcomes[1].Add(1)
		exercise(t, l, r)
		l.Close()
		mem = img
	}
}

func FuzzCraftedRecords(f *testing.F) {
	for seed := 0; seed < 8; seed++ {
		f.Add(pseudoRandomBytes(seed, 200))
	}
	f.Fuzz(runCraftedRecords)
}

// TestCraftedRecords runs FuzzCraftedRecords on pseudo-random inputs.
func TestCraftedRecords(t *testing.T) {
	n := 600
	if testing.Short() {
		n = 100
	}
	craftedOutcomes[0].Store(0)
	craftedOutcomes[1].Store(0)
	for seed := 0; seed < n; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runCraftedRecords(t, pseudoRandomBytes(seed, 300)) })
	}
	t.Logf("opens refused %d, succeeded %d", craftedOutcomes[0].Load(), craftedOutcomes[1].Load())
}

// pseudoRandomBytes returns deterministic bytes, mostly small values.
func pseudoRandomBytes(seed, n int) []byte {
	b := make([]byte, n)
	x := uint64(seed)*0x9e3779b97f4a7c15 + 1
	for i := range b {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		b[i] = byte(x)
		if b[i]&0xc0 != 0 && i%3 != 0 {
			b[i] &= 0x0f
		}
	}
	return b
}

// ---- crafted checkpoints ----

// encodeCheckpointFile writes c in the checkpoint format, with a valid
// checksum.
func encodeCheckpointFile(c *checkpoint, version uint64) []byte {
	w := &ckptWriter{b: []byte(ckptMagic)}
	w.uv(version)
	w.uv(uint64(c.key))
	w.uv(c.gen)
	w.uv(c.seq)
	w.uv(c.seg)
	w.uv(uint64(c.off))
	segs := sortedKeys(c.segs)
	w.uv(uint64(len(segs)))
	for _, n := range segs {
		m := c.segs[n]
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
	ids := sortedKeys(c.groups)
	w.uv(uint64(len(ids)))
	for _, id := range ids {
		g := c.groups[id]
		w.uv(id)
		w.uv(g.wmeta.first)
		w.uv(g.wmeta.last)
		w.uv(g.wmeta.compactTerm)
		w.uv(g.born.seg)
		w.uv(uint64(g.born.off))
		hs, _ := proto.Marshal(g.hs)
		cs, _ := proto.Marshal(g.cs)
		w.bytes(hs)
		w.bytes(cs)
		if g.wsnap == nil {
			w.bytes(nil)
		} else {
			rec, _ := encodeSnapRecord(0, g.wsnap)
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
	return binary.LittleEndian.AppendUint32(w.b, crc32.Checksum(w.b, crcTable))
}

func sortedKeys[V any](m map[uint64]V) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// mutateCheckpoint changes fields of c as r directs.
func mutateCheckpoint(c *checkpoint, r *opReader) {
	pick := func(v uint64) uint64 {
		switch r.byte() % 16 {
		case 0:
			return v + 1
		case 1:
			return v - 1
		case 2:
			return 0
		case 3:
			return craftIndex(r)
		case 4:
			return v ^ uint64(1)<<(r.byte()%64)
		default:
			return v
		}
	}
	for n := int(r.byte() % 3); n >= 0 && len(r.b) > 0; n-- {
		switch r.byte() % 13 {
		case 12:
			c.key = uint32(pick(uint64(c.key))) // a wrong key: every chunk fails its checksum
		case 0:
			c.seq = pick(c.seq)
		case 1:
			c.seg = pick(c.seg)
		case 2:
			c.off = int64(pick(uint64(c.off)))
		case 3, 4:
			for _, k := range sortedKeys(c.segs) {
				m := c.segs[k]
				switch r.byte() % 12 {
				case 0:
					m.count = pick(m.count)
				case 1:
					m.entries = pick(m.entries)
				case 2:
					m.end = int64(pick(uint64(m.end)))
				case 3:
					m.sealed = !m.sealed
				}
				c.segs[k] = m
			}
			if r.byte()%4 == 0 {
				c.segs[craftIndex(r)] = segMeta{count: craftIndex(r), end: int64(r.u16())}
			}
		default:
			for _, id := range sortedKeys(c.groups) {
				g := c.groups[id]
				switch r.byte() % 24 {
				case 0:
					g.wmeta.first = pick(g.wmeta.first)
				case 1:
					g.wmeta.last = pick(g.wmeta.last)
				case 2:
					g.wmeta.compactTerm = pick(g.wmeta.compactTerm)
				case 3:
					g.hs = withCommit(g.hs, pick(g.hs.GetCommit()))
				case 4:
					if len(g.runs) > 0 {
						k := int(r.byte()) % len(g.runs)
						g.runs[k] = segRun{seg: pick(g.runs[k].seg), lo: pick(g.runs[k].lo), hi: pick(g.runs[k].hi)}
					}
				case 5:
					g.runs = append(g.runs, segRun{seg: craftIndex(r), lo: craftIndex(r), hi: craftIndex(r)})
				case 6:
					if len(g.terms) > 0 {
						k := int(r.byte()) % len(g.terms)
						g.terms[k] = termRun{index: pick(g.terms[k].index), term: pick(g.terms[k].term)}
					}
				case 7:
					g.terms = append(g.terms, termRun{index: craftIndex(r), term: craftIndex(r)})
				case 8:
					if g.wsnap != nil {
						g.wsnap.meta.Index = new(pick(g.wsnap.meta.GetIndex()))
						if g.wsnap.ref != nil {
							g.wsnap.ref.size = pick(g.wsnap.ref.size)
							g.wsnap.ref.index = g.wsnap.meta.GetIndex()
						}
					} else {
						g.wsnap = &snapInfo{meta: &raftpb.SnapshotMetadata{Index: new(craftIndex(r)), Term: new(craftIndex(r))}, data: []byte("x")}
					}
				case 9:
					g.runs, g.terms = nil, nil
				case 10:
					g.born = position{seg: pick(g.born.seg), off: int64(pick(uint64(g.born.off)))}
				}
			}
			if r.byte()%6 == 0 {
				g := newGroup()
				g.wmeta = groupMeta{first: craftIndex(r), last: craftIndex(r), compactTerm: craftIndex(r)}
				g.runs = []segRun{{seg: craftIndex(r), lo: craftIndex(r), hi: craftIndex(r)}}
				g.terms = []termRun{{index: craftIndex(r), term: 1}}
				g.born = position{seg: 1}
				c.groups[craftIndex(r)] = g
			}
		}
	}
}

func runCraftedCheckpoint(t *testing.T, input []byte) {
	r := &opReader{b: input}
	mem := crashfs.New()
	opts := Options{SegmentSize: 64 << 10, SnapshotInlineBytes: 64}
	l, err := Open(crashDir, memOpts(mem, opts))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		g := uint64(1 + i%3)
		if err := l.Save(Update{Group: g, Entries: ents(1, uint64(i/3)*20+1, uint64(i/3)*20+20, 300), HardState: &raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(i/3)*20 + 10)}}); err != nil {
			t.Fatal(err)
		}
	}
	l.Group(1).CreateSnapshot(30, nil, make([]byte, 500)) // in a file
	l.Group(2).CreateSnapshot(20, nil, []byte("inline"))
	l.Group(3).Compact(15)
	if r.byte()%2 == 0 {
		l.Close()
	} else {
		l.crash()
	}
	data := fileBytes(t, mem, crashDir+"/CHECKPOINT-A")
	c, err := decodeCheckpoint(data)
	if err != nil {
		t.Fatal(err)
	}
	version := uint64(formatVersion)
	if r.byte()%32 == 0 {
		version = uint64(r.byte())
	}
	mutateCheckpoint(c, r)
	crafted := encodeCheckpointFile(c, version)
	if r.byte()%5 == 0 && len(crafted) > 8 {
		// Splice raw bytes into the body, and fix the checksum.
		body := crafted[:len(crafted)-4]
		at := 4 + int(r.byte())%(len(body)-4)
		for i := 0; i < int(r.byte()%8) && at+i < len(body); i++ {
			body[at+i] = r.byte()
		}
		crafted = binary.LittleEndian.AppendUint32(body, crc32.Checksum(body, crcTable))
	}
	rewrite(t, mem, crashDir+"/CHECKPOINT-A", crafted)
	rewrite(t, mem, crashDir+"/CHECKPOINT-B", crafted)
	l, err = openWithin(t, mem, Options{})
	if err != nil {
		craftedOutcomes[0].Add(1)
		return
	}
	craftedOutcomes[1].Add(1)
	exercise(t, l, r)
	l.Close()
	if l, err = openWithin(t, mem, Options{}); err == nil {
		l.Close()
	}
}

func FuzzCraftedCheckpoint(f *testing.F) {
	for seed := 0; seed < 8; seed++ {
		f.Add(pseudoRandomBytes(seed+100, 120))
	}
	f.Fuzz(runCraftedCheckpoint)
}

// TestCraftedCheckpoint runs FuzzCraftedCheckpoint on pseudo-random inputs.
func TestCraftedCheckpoint(t *testing.T) {
	n := 600
	if testing.Short() {
		n = 100
	}
	craftedOutcomes[0].Store(0)
	craftedOutcomes[1].Store(0)
	for seed := 0; seed < n; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runCraftedCheckpoint(t, pseudoRandomBytes(seed+1000, 150)) })
	}
	t.Logf("opens refused %d, succeeded %d", craftedOutcomes[0].Load(), craftedOutcomes[1].Load())
}
