// Copyright 2026 The raftseg authors.

package seglog

// The raft.Storage contract. Storage promises raft.MemoryStorage's
// semantics, so upstream's MemoryStorage tests are run against it here, and
// FuzzStorageContract drives both with arbitrary operations, valid or not.
//
// Where MemoryStorage accepts an operation, Storage must end up in the same
// observable state, also after a reopen. Storage is stricter in a few
// places, each a state raft never produces and could not restart from:
//   - indexes must be below 2^63 (so that index arithmetic never wraps);
//   - a Save must not leave the HardState's commit index beyond the last
//     entry (raft panics on restart with such a state);
//   - an empty HardState means "unchanged", as in raft.Ready.
// Storage must reject those, and everything MemoryStorage panics on, with an
// error, and change nothing.
//
// Two deliberate differences in state: applying a snapshot raises the
// stored commit index to the snapshot's index (raft commits it in the same
// Ready), and Open raises a commit index below FirstIndex-1 to FirstIndex-1.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
)

// termsFrom returns entries first, first+1, ... with the given terms and no
// data, like upstream's index(first).terms(...).
func termsFrom(first uint64, terms ...uint64) []*raftpb.Entry {
	out := make([]*raftpb.Entry, len(terms))
	for i, t := range terms {
		out[i] = &raftpb.Entry{Index: new(first + uint64(i)), Term: new(t)}
	}
	return out
}

func sameEntry(a, b *raftpb.Entry) bool {
	return a.GetIndex() == b.GetIndex() && a.GetTerm() == b.GetTerm() && a.GetType() == b.GetType() && bytes.Equal(a.Data, b.Data)
}

func sameEntries(a, b []*raftpb.Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !sameEntry(a[i], b[i]) {
			return false
		}
	}
	return true
}

func fmtEntries(es []*raftpb.Entry) string {
	var parts []string
	for _, e := range es {
		parts = append(parts, fmt.Sprintf("%d/%d(%dB)", e.GetIndex(), e.GetTerm(), len(e.Data)))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// upstreamStorage builds the state upstream's storage tests start from,
// MemoryStorage{ents: index(3).terms(3, 4, 5, ...)}: entries 4 and 5 with
// terms 4 and 5, preceded by compacted entry 3 of term 3.
func upstreamStorage(t *testing.T, terms ...uint64) (*Log, *Storage) {
	t.Helper()
	l := open(t, crashDir, memOpts(crashfs.New(), Options{CacheEntries: -1}))
	t.Cleanup(func() { l.Close() })
	s := l.Group(1)
	all := append([]uint64{1, 2}, terms...)
	if err := s.Save(&raftpb.HardState{Term: new(all[len(all)-1]), Commit: new(uint64(3))}, termsFrom(1, all...), &raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(3); err != nil {
		t.Fatal(err)
	}
	return l, s
}

func TestUpstreamStorageTerm(t *testing.T) {
	for _, tt := range []struct {
		i     uint64
		werr  error
		wterm uint64
	}{
		{2, raft.ErrCompacted, 0},
		{3, nil, 3},
		{4, nil, 4},
		{5, nil, 5},
		{6, raft.ErrUnavailable, 0},
	} {
		_, s := upstreamStorage(t, 3, 4, 5)
		term, err := s.Term(tt.i)
		if !errors.Is(err, tt.werr) || (err == nil) != (tt.werr == nil) || term != tt.wterm {
			t.Errorf("Term(%d) = %d, %v; want %d, %v", tt.i, term, err, tt.wterm, tt.werr)
		}
	}
}

func TestUpstreamStorageEntries(t *testing.T) {
	es := termsFrom(3, 3, 4, 5, 6)
	sz := func(k int) uint64 { return uint64(proto.Size(es[k])) }
	for _, tt := range []struct {
		lo, hi, maxsize uint64
		werr            error
		wentries        []*raftpb.Entry
	}{
		{2, 6, math.MaxUint64, raft.ErrCompacted, nil},
		{3, 4, math.MaxUint64, raft.ErrCompacted, nil},
		{4, 5, math.MaxUint64, nil, termsFrom(4, 4)},
		{4, 6, math.MaxUint64, nil, termsFrom(4, 4, 5)},
		{4, 7, math.MaxUint64, nil, termsFrom(4, 4, 5, 6)},
		// even if maxsize is zero, the first entry should be returned
		{4, 7, 0, nil, termsFrom(4, 4)},
		// limit to 2
		{4, 7, sz(1) + sz(2), nil, termsFrom(4, 4, 5)},
		// limit to 2
		{4, 7, sz(1) + sz(2) + sz(3)/2, nil, termsFrom(4, 4, 5)},
		{4, 7, sz(1) + sz(2) + sz(3) - 1, nil, termsFrom(4, 4, 5)},
		// all
		{4, 7, sz(1) + sz(2) + sz(3), nil, termsFrom(4, 4, 5, 6)},
	} {
		_, s := upstreamStorage(t, 3, 4, 5, 6)
		got, err := s.Entries(tt.lo, tt.hi, tt.maxsize)
		if !errors.Is(err, tt.werr) || (err == nil) != (tt.werr == nil) || !sameEntries(got, tt.wentries) {
			t.Errorf("Entries(%d, %d, %d) = %s, %v; want %s, %v", tt.lo, tt.hi, tt.maxsize, fmtEntries(got), err, fmtEntries(tt.wentries), tt.werr)
		}
	}
}

func TestUpstreamStorageLastIndex(t *testing.T) {
	_, s := upstreamStorage(t, 3, 4, 5)
	if last, err := s.LastIndex(); err != nil || last != 5 {
		t.Fatalf("LastIndex = %d, %v", last, err)
	}
	if err := s.Save(&raftpb.HardState{}, termsFrom(6, 5), &raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	if last, err := s.LastIndex(); err != nil || last != 6 {
		t.Fatalf("LastIndex = %d, %v", last, err)
	}
}

func TestUpstreamStorageFirstIndex(t *testing.T) {
	_, s := upstreamStorage(t, 3, 4, 5)
	if first, err := s.FirstIndex(); err != nil || first != 4 {
		t.Fatalf("FirstIndex = %d, %v", first, err)
	}
	if err := s.Compact(4); err != nil {
		t.Fatal(err)
	}
	if first, err := s.FirstIndex(); err != nil || first != 5 {
		t.Fatalf("FirstIndex = %d, %v", first, err)
	}
}

func TestUpstreamStorageCompact(t *testing.T) {
	for _, tt := range []struct {
		i             uint64
		werr          error
		windex, wterm uint64
		wlen          int
	}{
		{2, raft.ErrCompacted, 3, 3, 3},
		{3, raft.ErrCompacted, 3, 3, 3},
		{4, nil, 4, 4, 2},
		{5, nil, 5, 5, 1},
	} {
		_, s := upstreamStorage(t, 3, 4, 5)
		if err := s.Compact(tt.i); !errors.Is(err, tt.werr) || (err == nil) != (tt.werr == nil) {
			t.Errorf("Compact(%d) = %v, want %v", tt.i, err, tt.werr)
		}
		// MemoryStorage's ents[0] is the compacted entry: FirstIndex-1 and
		// its term; len(ents) counts it.
		first, _ := s.FirstIndex()
		last, _ := s.LastIndex()
		term, err := s.Term(first - 1)
		if first-1 != tt.windex || term != tt.wterm || err != nil || int(last-first+2) != tt.wlen {
			t.Errorf("Compact(%d): first %d (term %d, %v), last %d; want ents[0] = %d/%d, len %d", tt.i, first, term, err, last, tt.windex, tt.wterm, tt.wlen)
		}
	}
}

func TestUpstreamStorageCreateSnapshot(t *testing.T) {
	cs := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	data := []byte("data")
	for _, tt := range []struct {
		i     uint64
		wsnap *raftpb.Snapshot
	}{
		{4, &raftpb.Snapshot{Data: data, Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(4)), Term: new(uint64(4)), ConfState: cs}}},
		{5, &raftpb.Snapshot{Data: data, Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(5)), Term: new(uint64(5)), ConfState: cs}}},
	} {
		l, s := upstreamStorage(t, 3, 4, 5)
		snap, err := s.CreateSnapshot(tt.i, cs, data)
		if err != nil || !sameSnapshot(snap, tt.wsnap) {
			t.Errorf("CreateSnapshot(%d) = %+v, %v; want %+v", tt.i, snap.Metadata, err, tt.wsnap.Metadata)
		}
		if got, err := s.Snapshot(); err != nil || !sameSnapshot(got, tt.wsnap) {
			t.Errorf("Snapshot() after CreateSnapshot(%d) = %+v, %v", tt.i, got.Metadata, err)
		}
		_ = l
	}
}

func TestUpstreamStorageAppend(t *testing.T) {
	for _, tt := range []struct {
		entries  []*raftpb.Entry
		wentries []*raftpb.Entry // including the compacted entry, as ms.ents
	}{
		{termsFrom(1, 1, 2), termsFrom(3, 3, 4, 5)},
		{termsFrom(3, 3, 4, 5), termsFrom(3, 3, 4, 5)},
		{termsFrom(3, 3, 6, 6), termsFrom(3, 3, 6, 6)},
		{termsFrom(3, 3, 4, 5, 5), termsFrom(3, 3, 4, 5, 5)},
		// Truncate incoming entries, truncate the existing entries and append.
		{termsFrom(2, 3, 3, 5), termsFrom(3, 3, 5)},
		// Truncate the existing entries and append.
		{termsFrom(4, 5), termsFrom(3, 3, 5)},
		// Direct append.
		{termsFrom(6, 5), termsFrom(3, 3, 4, 5, 5)},
	} {
		_, s := upstreamStorage(t, 3, 4, 5)
		if err := s.Save(&raftpb.HardState{}, tt.entries, &raftpb.Snapshot{}); err != nil {
			t.Fatalf("Append(%s): %v", fmtEntries(tt.entries), err)
		}
		first, _ := s.FirstIndex()
		last, _ := s.LastIndex()
		term, _ := s.Term(first - 1)
		got := []*raftpb.Entry{{Index: new(first - 1), Term: new(term)}}
		if first <= last {
			es, err := s.Entries(first, last+1, math.MaxUint64)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, es...)
		}
		if !sameEntries(got, tt.wentries) {
			t.Errorf("Append(%s): ents %s, want %s", fmtEntries(tt.entries), fmtEntries(got), fmtEntries(tt.wentries))
		}
	}
}

func TestUpstreamStorageApplySnapshot(t *testing.T) {
	cs := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	data := []byte("data")
	l := open(t, crashDir, memOpts(crashfs.New(), Options{}))
	defer l.Close()
	s := l.Group(1)
	if err := s.Save(&raftpb.HardState{}, nil, &raftpb.Snapshot{Data: data, Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(4)), Term: new(uint64(4)), ConfState: cs}}); err != nil {
		t.Fatal(err)
	}
	// ApplySnapshot fails due to ErrSnapOutOfDate.
	if err := s.Save(&raftpb.HardState{}, nil, &raftpb.Snapshot{Data: data, Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(3)), Term: new(uint64(3)), ConfState: cs}}); !errors.Is(err, raft.ErrSnapOutOfDate) {
		t.Fatalf("got %v", err)
	}
}

// sameSnapshot compares snapshots by value: an unset field reads as its
// zero (raft.MemoryStorage sets some unset fields when it hands a snapshot
// out, seglog hands out what it was given).
func sameSnapshot(a, b *raftpb.Snapshot) bool {
	am, bm := a.GetMetadata(), b.GetMetadata()
	return am.GetIndex() == bm.GetIndex() && am.GetTerm() == bm.GetTerm() &&
		sameCS(am.GetConfState(), bm.GetConfState()) && bytes.Equal(a.GetData(), b.GetData())
}

// ---- the differential fuzzer ----

// contractModel is raft.MemoryStorage plus what Storage keeps beside it.
type contractModel struct {
	ms *raft.MemoryStorage
	hs *raftpb.HardState
}

func (m *contractModel) bounds() (first, last uint64) {
	first, _ = m.ms.FirstIndex()
	last, _ = m.ms.LastIndex()
	return first, last
}

// noPanic runs fn and reports whether it returned normally.
func noPanic(fn func()) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	fn()
	return true
}

type opReader struct{ b []byte }

func (r *opReader) byte() byte {
	if len(r.b) == 0 {
		return 0
	}
	c := r.b[0]
	r.b = r.b[1:]
	return c
}

func (r *opReader) u16() int { return int(r.byte())<<8 | int(r.byte()) }

// index picks an index of interest: the edges of the log, wrapping values,
// or one near the log.
func (r *opReader) index(first, last uint64) uint64 {
	switch c := r.byte(); c % 14 {
	case 0:
		return 0
	case 1:
		return 1
	case 2:
		return first - 1
	case 3:
		return first
	case 4:
		return last
	case 5:
		return last + 1
	case 6:
		return last + 2
	case 7:
		return math.MaxUint64
	case 8:
		return math.MaxUint64 - 1
	case 9:
		return 1 << 63
	case 10:
		return 1<<63 - 1
	case 11:
		return last + uint64(r.byte())
	default:
		if span := last - first + 3; span > 0 {
			return first - 1 + uint64(r.byte())%span
		}
		return first
	}
}

func (r *opReader) data() []byte {
	n := int(r.byte())
	switch r.byte() % 8 {
	case 0:
		n = 0
	case 1:
		n *= 97 // up to 24 KiB: exceeds SnapshotInlineBytes and the cache
	}
	if n == 0 && r.byte()%2 == 0 {
		return nil
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + n)
	}
	return b
}

func (r *opReader) confState() *raftpb.ConfState {
	switch r.byte() % 3 {
	case 0:
		return nil
	case 1:
		return &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	}
	return &raftpb.ConfState{Voters: []uint64{uint64(r.byte())}, Learners: []uint64{uint64(r.byte()) + 300}, AutoLeave: new(true)}
}

const maxSafeIndex = 1<<63 - 1

// checkContract compares every observable of s with the model.
func checkContract(t *testing.T, what string, s *Storage, m *contractModel) {
	t.Helper()
	first, last := m.bounds()
	sf, err1 := s.FirstIndex()
	sl, err2 := s.LastIndex()
	if err1 != nil || err2 != nil || sf != first || sl != last {
		t.Fatalf("%s: first/last %d/%d (%v, %v), want %d/%d", what, sf, sl, err1, err2, first, last)
	}
	hs, cs, err := s.InitialState()
	_, mcs, _ := m.ms.InitialState()
	if err != nil || !sameHS(hs, m.hs) || !sameCS(cs, mcs) {
		t.Fatalf("%s: InitialState %+v %+v (%v), want %+v %+v", what, hs, cs, err, m.hs, mcs)
	}
	var probes []uint64
	for _, i := range []uint64{0, 1, first - 2, math.MaxUint64, 1 << 63} {
		probes = append(probes, i)
	}
	for i := first - 1; i <= last+2 && i-first+1 < 40; i++ {
		probes = append(probes, i)
	}
	for i := max(first, last-20); i <= last+2; i++ {
		probes = append(probes, i)
	}
	for _, i := range probes {
		var mt uint64
		var merr error
		if !noPanic(func() { mt, merr = m.ms.Term(i) }) {
			if _, err := s.Term(i); err == nil {
				// MemoryStorage panics (a wrapped index): any error will do.
				t.Fatalf("%s: Term(%d) succeeded where MemoryStorage panics", what, i)
			}
			continue
		}
		st, serr := s.Term(i)
		if st != mt || (merr == nil) != (serr == nil) || (merr != nil && !errors.Is(serr, merr)) {
			t.Fatalf("%s: Term(%d) = %d, %v; want %d, %v", what, i, st, serr, mt, merr)
		}
	}
	if first <= last {
		want, _ := m.ms.Entries(first, last+1, math.MaxUint64)
		got, err := s.Entries(first, last+1, math.MaxUint64)
		if err != nil || !sameEntries(got, want) {
			t.Fatalf("%s: Entries(%d, %d) = %s, %v; want %s", what, first, last+1, fmtEntries(got), err, fmtEntries(want))
		}
	}
	ms, _ := m.ms.Snapshot()
	ss, err := s.Snapshot()
	if err != nil || !sameSnapshot(ss, ms) {
		t.Fatalf("%s: Snapshot %+v (%d bytes, %v), want %+v (%d bytes)", what, ss.Metadata, len(ss.Data), err, ms.Metadata, len(ms.Data))
	}
}

func FuzzStorageContract(f *testing.F) {
	f.Add([]byte{0, 0, 5, 1, 1, 0, 3, 3, 4, 0, 7, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	f.Add([]byte{0, 0, 9, 3, 2, 1, 1, 0, 2, 4, 4, 1, 0, 6, 6, 0, 1, 1, 5, 8, 3, 3, 10, 1, 9})
	f.Add([]byte{4, 7, 1, 1, 0, 0, 0, 0, 3, 0, 9, 0, 0, 11, 2, 0, 3, 0, 0, 7, 0, 1, 0})
	f.Add([]byte{0, 0, 30, 2, 1, 1, 2, 4, 5, 6, 1, 1, 3, 3, 0, 5, 0, 2, 4, 1, 3, 0, 9, 9, 9, 10})
	f.Fuzz(func(t *testing.T, input []byte) {
		runStorageContract(t, input)
	})
}

// TestStorageContract runs the fuzzer's operations from pseudo-random
// inputs, so the contract is checked by every test run, not just by fuzzing.
func TestStorageContract(t *testing.T) {
	n := 400
	if testing.Short() {
		n = 80
	}
	for seed := 0; seed < n; seed++ {
		b := make([]byte, 300+seed%200)
		x := uint64(seed)*0x9e3779b97f4a7c15 + 1
		for i := range b {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			b[i] = byte(x)
			// Mostly small numbers: they hit the interesting cases.
			if b[i]&0xc0 != 0 && i%3 != 0 {
				b[i] &= 0x0f
			}
		}
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runStorageContract(t, b) })
	}
}

func runStorageContract(t *testing.T, input []byte) {
	// MemoryStorage logs before it panics.
	raft.SetLogger(&raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)})
	defer raft.ResetDefaultLogger()
	r := &opReader{b: input}
	mem := crashfs.New()
	opts := memOpts(mem, Options{SegmentSize: 64 << 10, CacheEntries: int(r.byte()%5) - 1, SnapshotInlineBytes: 512})
	l, err := Open(crashDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { l.Close() }()
	const groups = 2
	models := map[uint64]*contractModel{}
	handles := map[uint64]*Storage{}
	for g := uint64(1); g <= groups; g++ {
		models[g] = &contractModel{ms: raft.NewMemoryStorage()}
		handles[g] = l.Group(g)
	}
	checkAll := func(what string) {
		for g := uint64(1); g <= groups; g++ {
			checkContract(t, fmt.Sprintf("%s, group %d", what, g), handles[g], models[g])
		}
	}
	for step := 0; len(r.b) > 0 && step < 200; step++ {
		g := uint64(1 + r.byte()%groups)
		m, s := models[g], handles[g]
		first, last := m.bounds()
		snapIdx := func() uint64 { sn, _ := m.ms.Snapshot(); return sn.Metadata.GetIndex() }()
		var desc string
		switch op := r.byte() % 10; op {
		case 0, 1, 2: // append, with or without a HardState
			start := r.index(first, last)
			n := int(r.byte() % 6)
			term := uint64(r.byte() % 8)
			var es []*raftpb.Entry
			for k := 0; k < n; k++ {
				if r.byte()%4 == 0 {
					term = uint64(r.byte() % 8) // terms need not grow
				}
				es = append(es, &raftpb.Entry{Index: new(start + uint64(k)), Term: new(term), Type: raftpb.EntryType(r.byte() % 3).Enum(), Data: r.data()})
			}
			if r.byte()%8 == 0 && n > 1 {
				es[n-1].Index = new(es[n-1].GetIndex() + uint64(r.byte()%3) + 1) // a gap
			}
			var hs = &raftpb.HardState{}
			if op != 0 {
				hs = &raftpb.HardState{Term: new(uint64(r.byte() % 8)), Vote: new(uint64(r.byte() % 3)), Commit: new(r.index(first, last+uint64(n)))}
			}
			desc = fmt.Sprintf("Save(%+v, %s)", hs, fmtEntries(es))
			valid := true
			for k, e := range es {
				if e.GetIndex() == 0 || e.GetIndex() > maxSafeIndex || (k > 0 && e.GetIndex() != es[k-1].GetIndex()+1) {
					valid = false
				}
			}
			newLast := last
			if valid && len(es) > 0 {
				lastNew := es[len(es)-1].GetIndex()
				switch {
				case lastNew < first:
				case max(es[0].GetIndex(), first) > last+1:
					valid = false // MemoryStorage panics: missing entries
				default:
					newLast = lastNew
				}
			}
			newHS := m.hs
			if !raft.IsEmptyHardState(hs) {
				newHS = hs
			}
			if valid && newHS.GetCommit() > newLast && !raft.IsEmptyHardState(newHS) {
				valid = false // commit beyond the last entry
			}
			err := s.Save(hs, es, &raftpb.Snapshot{})
			if valid {
				if err != nil {
					t.Fatalf("step %d: %s: %v", step, desc, err)
				}
				if !noPanic(func() { m.ms.Append(es) }) {
					t.Fatalf("step %d: %s: model panicked on an input judged valid", step, desc)
				}
				m.hs = newHS
			} else if err == nil {
				t.Fatalf("step %d: %s accepted (first %d, last %d, hs %+v)", step, desc, first, last, m.hs)
			}
		case 3: // Compact
			i := r.index(first, last)
			desc = fmt.Sprintf("Compact(%d)", i)
			var merr error
			ok := noPanic(func() { merr = m.ms.Compact(i) })
			err := s.Compact(i)
			switch {
			case !ok && err == nil:
				t.Fatalf("step %d: %s accepted where MemoryStorage panics (first %d, last %d)", step, desc, first, last)
			case ok && ((merr == nil) != (err == nil) || (merr != nil && !errors.Is(err, merr))):
				t.Fatalf("step %d: %s = %v, want %v", step, desc, err, merr)
			}
		case 4: // CreateSnapshot
			i := r.index(first, last)
			cs := r.confState()
			data := r.data()
			desc = fmt.Sprintf("CreateSnapshot(%d, %v, %d bytes)", i, cs, len(data))
			valid := i <= snapIdx || (i+1 >= first && i <= last)
			sn, err := s.CreateSnapshot(i, cs, data)
			if !valid {
				if err == nil {
					t.Fatalf("step %d: %s accepted (first %d, last %d)", step, desc, first, last)
				}
				break
			}
			msn, merr := m.ms.CreateSnapshot(i, cs, data)
			if (merr == nil) != (err == nil) || (merr != nil && !errors.Is(err, merr)) {
				t.Fatalf("step %d: %s = %v, want %v", step, desc, err, merr)
			}
			if err == nil && !sameSnapshot(sn, msn) {
				t.Fatalf("step %d: %s = %+v, want %+v", step, desc, sn.Metadata, msn.Metadata)
			}
		case 5: // a received snapshot, with or without a HardState and entries
			idx := r.index(first, last)
			sn := &raftpb.Snapshot{Data: r.data(), Metadata: &raftpb.SnapshotMetadata{Index: new(idx), Term: new(uint64(r.byte() % 9))}}
			if cs := r.confState(); cs != nil {
				sn.Metadata.ConfState = cs
			}
			var hs = &raftpb.HardState{}
			if r.byte()%2 == 0 {
				hs = &raftpb.HardState{Term: sn.Metadata.Term, Vote: new(uint64(r.byte() % 3)), Commit: new(idx + uint64(r.byte()%3))}
			}
			var es []*raftpb.Entry
			if r.byte()%2 == 0 {
				es = termsFrom(idx+1, sn.Metadata.GetTerm(), sn.Metadata.GetTerm())
			}
			desc = fmt.Sprintf("Save(%+v, %s, snapshot %d/%d)", hs, fmtEntries(es), idx, sn.Metadata.GetTerm())
			err := s.Save(hs, es, sn)
			newHS := copyHardState(m.hs)
			if !raft.IsEmptyHardState(hs) {
				newHS = copyHardState(hs)
			}
			newHS.Commit = new(max(newHS.GetCommit(), idx))
			newLast := idx + uint64(len(es))
			switch {
			case idx == 0:
				// An empty snapshot: only the entries and HardState count.
				newLast = last
				if len(es) > 0 && max(es[0].GetIndex(), first) > last+1 {
					if err == nil {
						t.Fatalf("step %d: %s accepted", step, desc)
					}
					break
				}
				if len(es) > 0 && es[len(es)-1].GetIndex() >= first {
					newLast = es[len(es)-1].GetIndex()
				}
				newHS = m.hs
				if !raft.IsEmptyHardState(hs) {
					newHS = hs
				}
				if newHS.GetCommit() > newLast || newLast > maxSafeIndex {
					if err == nil {
						t.Fatalf("step %d: %s accepted", step, desc)
					}
					break
				}
				if err != nil {
					t.Fatalf("step %d: %s: %v", step, desc, err)
				}
				m.ms.Append(es)
				m.hs = newHS
			case idx <= snapIdx:
				if !errors.Is(err, raft.ErrSnapOutOfDate) {
					t.Fatalf("step %d: %s = %v, want ErrSnapOutOfDate", step, desc, err)
				}
			case newLast > maxSafeIndex || newLast < idx || newHS.GetCommit() > newLast:
				if err == nil {
					t.Fatalf("step %d: %s accepted", step, desc)
				}
			default:
				if err != nil {
					t.Fatalf("step %d: %s: %v", step, desc, err)
				}
				if err := m.ms.ApplySnapshot(sn); err != nil {
					t.Fatalf("step %d: %s: model: %v", step, desc, err)
				}
				m.ms.Append(es)
				m.hs = newHS
			}
		case 6: // reads with arbitrary bounds
			lo, hi := r.index(first, last), r.index(first, last)
			maxSize := uint64(r.u16())
			if r.byte()%4 == 0 {
				maxSize = math.MaxUint64
			}
			desc = fmt.Sprintf("Entries(%d, %d, %d)", lo, hi, maxSize)
			var want []*raftpb.Entry
			var merr error
			ok := noPanic(func() { want, merr = m.ms.Entries(lo, hi, maxSize) })
			got, err := s.Entries(lo, hi, maxSize)
			if !ok {
				break // out of bounds: MemoryStorage panics; any answer but a panic
			}
			if (merr == nil) != (err == nil) || (merr != nil && !errors.Is(err, merr)) || !sameEntries(got, want) {
				t.Fatalf("step %d: %s = %s, %v; want %s, %v", step, desc, fmtEntries(got), err, fmtEntries(want), merr)
			}
		case 7: // reopen
			desc = "reopen"
			if err := l.Close(); err != nil {
				t.Fatalf("step %d: close: %v", step, err)
			}
			if l, err = Open(crashDir, opts); err != nil {
				t.Fatalf("step %d: reopen: %v", step, err)
			}
			for gg := uint64(1); gg <= groups; gg++ {
				handles[gg] = l.Group(gg)
				mm := models[gg]
				f, _ := mm.bounds()
				if !raft.IsEmptyHardState(mm.hs) && mm.hs.GetCommit() < f-1 {
					mm.hs = withCommit(mm.hs, f-1)
				}
			}
		case 8: // RemoveGroup: the old handle dies, a new one starts empty
			desc = "RemoveGroup"
			if err := l.RemoveGroup(g); err != nil {
				t.Fatalf("step %d: RemoveGroup: %v", step, err)
			}
			if _, err := s.FirstIndex(); !errors.Is(err, ErrGroupRemoved) {
				t.Fatalf("step %d: removed handle: %v", step, err)
			}
			if err := s.Save(&raftpb.HardState{Term: new(uint64(1))}, nil, &raftpb.Snapshot{}); !errors.Is(err, ErrGroupRemoved) {
				t.Fatalf("step %d: Save on a removed handle: %v", step, err)
			}
			models[g] = &contractModel{ms: raft.NewMemoryStorage()}
			handles[g] = l.Group(g)
		case 9:
			desc = "GC"
			if _, err := l.GC(); err != nil {
				t.Fatalf("step %d: GC: %v", step, err)
			}
		}
		checkAll(fmt.Sprintf("step %d after %s", step, desc))
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if l, err = Open(crashDir, opts); err != nil {
		t.Fatalf("final reopen: %v", err)
	}
	for g := uint64(1); g <= groups; g++ {
		handles[g] = l.Group(g)
		mm := models[g]
		f, _ := mm.bounds()
		if !raft.IsEmptyHardState(mm.hs) && mm.hs.GetCommit() < f-1 {
			mm.hs = withCommit(mm.hs, f-1)
		}
	}
	checkAll("final reopen")
}
