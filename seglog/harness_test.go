// Copyright 2026 The raftseg authors.

package seglog

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	randv2 "math/rand/v2"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
	"github.com/raftseg/raftseg/vfs"
)

// ---- a raft-like random workload with an exact model ----

type world struct {
	g     map[uint64]*groupModel
	terms map[uint64]uint64
}

func newWorld(groups int) *world {
	w := &world{g: map[uint64]*groupModel{}, terms: map[uint64]uint64{}}
	for g := uint64(1); g <= uint64(groups); g++ {
		w.g[g], w.terms[g] = newGroupModel(), 1
	}
	return w
}

func (m *groupModel) clone() *groupModel {
	c := *m
	c.log = make(map[uint64]*raftpb.Entry, len(m.log))
	for k, v := range m.log {
		c.log[k] = v
	}
	c.termAt = make(map[uint64]uint64, len(m.termAt))
	for k, v := range m.termAt {
		c.termAt[k] = v
	}
	return &c
}

func (w *world) clone() *world {
	c := &world{g: map[uint64]*groupModel{}, terms: map[uint64]uint64{}}
	for k, v := range w.g {
		c.g[k] = v.clone()
	}
	for k, v := range w.terms {
		c.terms[k] = v
	}
	return c
}

// snapPayload is the deterministic state-machine image of group g at index
// i; every third one is large enough to be stored in its own file.
func snapPayload(g, i uint64) []byte {
	n := 200
	if i%3 == 0 {
		n = 80 << 10
	}
	h := fnv.New64a()
	fmt.Fprintf(h, "%d/%d", g, i)
	r := rand.New(rand.NewSource(int64(h.Sum64())))
	b := make([]byte, n)
	r.Read(b)
	return b
}

type opKind int

const (
	opSave opKind = iota
	opCreateSnapshot
	opCompact
	opApplySnapshot
	opSetConf
	opRemove
	opGC
)

type wop struct {
	kind  opKind
	group uint64
	ups   []Update
	idx   uint64
	cs    *raftpb.ConfState
}

func (o wop) String() string {
	switch o.kind {
	case opSave:
		var parts []string
		for _, u := range o.ups {
			parts = append(parts, fmt.Sprintf("g%d[%d..%d]", u.Group, u.Entries[0].GetIndex(), u.Entries[len(u.Entries)-1].GetIndex()))
		}
		return "save " + strings.Join(parts, " ")
	case opCreateSnapshot:
		return fmt.Sprintf("snapshot g%d@%d", o.group, o.idx)
	case opCompact:
		return fmt.Sprintf("compact g%d@%d", o.group, o.idx)
	case opApplySnapshot:
		return fmt.Sprintf("apply-snapshot g%d@%d", o.group, o.idx)
	case opSetConf:
		return fmt.Sprintf("setconf g%d", o.group)
	case opRemove:
		return fmt.Sprintf("remove g%d", o.group)
	}
	return "gc"
}

// genOp picks an operation that raft could issue in the model's state.
func genOp(rng *rand.Rand, w *world) wop {
	ids := make([]uint64, 0, len(w.g))
	for id := range w.g {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	g := ids[rng.Intn(len(ids))]
	m := w.g[g]
	switch r := rng.Intn(100); {
	case r < 60:
		var ups []Update
		seen := map[uint64]bool{}
		for k := 0; k < 1+rng.Intn(3); k++ {
			gg := ids[rng.Intn(len(ids))]
			if seen[gg] {
				continue
			}
			seen[gg] = true
			mm := w.g[gg]
			term := w.terms[gg]
			start := mm.last + 1
			lo := maxU(mm.first, mm.hs.GetCommit()+1)
			if mm.last >= lo && rng.Intn(4) == 0 {
				start = lo + uint64(rng.Int63n(int64(mm.last-lo+1)))
				term++
			}
			n := uint64(1 + rng.Intn(6))
			size := 30 + rng.Intn(900)
			if rng.Intn(20) == 0 {
				size = 40 << 10
			}
			commit := mm.hs.GetCommit()
			if c := start + n - 1 - uint64(rng.Intn(int(n))); c > commit {
				commit = c
			}
			ups = append(ups, Update{Group: gg, Entries: ents(term, start, start+n-1, size), HardState: &raftpb.HardState{Term: new(term), Vote: new(uint64(1)), Commit: new(commit)}})
		}
		return wop{kind: opSave, ups: ups}
	case r < 70 && m.hs.GetCommit() >= m.first && m.hs.GetCommit() > m.snapMax:
		lo := maxU(m.snapMax+1, m.first)
		i := lo + uint64(rng.Int63n(int64(m.hs.GetCommit()-lo+1)))
		o := wop{kind: opCreateSnapshot, group: g, idx: i}
		if rng.Intn(2) == 0 {
			o.cs = &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
		}
		return o
	case r < 80 && m.hs.GetCommit() >= m.first:
		return wop{kind: opCompact, group: g, idx: m.first + uint64(rng.Int63n(int64(m.hs.GetCommit()-m.first+1)))}
	case r < 85:
		return wop{kind: opApplySnapshot, group: g, idx: maxU(m.last, m.snapMax) + 1 + uint64(rng.Intn(10))}
	case r < 88:
		return wop{kind: opSetConf, group: g, cs: &raftpb.ConfState{Voters: []uint64{1, uint64(2 + rng.Intn(5))}}}
	case r < 90:
		return wop{kind: opRemove, group: g}
	default:
		return wop{kind: opGC}
	}
}

// run executes o; on success the caller applies it to the model.
func (o wop) run(l *Log, w *world) error {
	switch o.kind {
	case opSave:
		return l.Save(o.ups...)
	case opCreateSnapshot:
		_, err := l.Group(o.group).CreateSnapshot(o.idx, o.cs, snapPayload(o.group, o.idx))
		return err
	case opCompact:
		return l.Group(o.group).Compact(o.idx)
	case opApplySnapshot:
		term := w.terms[o.group] + 1
		u := Update{Group: o.group, HardState: &raftpb.HardState{Term: new(term), Commit: new(o.idx)},
			Snapshot: &raftpb.Snapshot{Data: snapPayload(o.group, o.idx), Metadata: &raftpb.SnapshotMetadata{Index: new(o.idx), Term: new(term), ConfState: &raftpb.ConfState{Voters: []uint64{1}}}}}
		if o.idx%2 == 0 {
			u.Entries = ents(term, o.idx+1, o.idx+2, 100)
		}
		return l.Save(u)
	case opSetConf:
		return l.Group(o.group).SetConfState(o.cs)
	case opRemove:
		return l.RemoveGroup(o.group)
	default:
		_, err := l.GC()
		return err
	}
}

func (o wop) apply(w *world) {
	m := w.g[o.group]
	switch o.kind {
	case opSave:
		for _, u := range o.ups {
			mm := w.g[u.Group]
			mm.append(u.Entries)
			mm.hs = u.HardState
			w.terms[u.Group] = u.HardState.GetTerm()
		}
	case opCreateSnapshot:
		m.snapMax = o.idx
	case opCompact:
		if o.idx+1 > m.first {
			m.termAt[o.idx] = m.term(o.idx)
			m.first = o.idx + 1
		}
	case opApplySnapshot:
		term := w.terms[o.group] + 1
		w.terms[o.group] = term
		m.log = map[uint64]*raftpb.Entry{}
		m.termAt[o.idx] = term
		m.first, m.minFirst, m.last = o.idx+1, o.idx+1, o.idx
		m.snapMin, m.snapMax = o.idx, o.idx
		m.hs = &raftpb.HardState{Term: new(term), Commit: new(o.idx)}
		if o.idx%2 == 0 {
			m.append(ents(term, o.idx+1, o.idx+2, 100))
		}
	case opRemove:
		w.g[o.group] = newGroupModel()
		w.terms[o.group] = 1
	}
}

// partial returns the state a crash may leave while o is in flight, other
// than before or after it: a snapshot received with entries in the same
// Ready is applied (synced) before those entries are written, so the
// snapshot can survive without them. raft treats that like having received
// only the snapshot.
func (o wop) partial(before *world) *world {
	if o.kind != opApplySnapshot || o.idx%2 != 0 {
		return nil
	}
	w := before.clone()
	o.apply(w)
	m := w.g[o.group]
	for i := o.idx + 1; i <= m.last; i++ {
		delete(m.log, i)
	}
	m.last = o.idx
	return w
}

// matchGroup checks a recovered group against one model state, allowing
// unsynced compactions and snapshot creations to have been lost.
func matchGroup(s *Storage, m *groupModel) error {
	first, err := s.FirstIndex()
	if err != nil {
		return err
	}
	last, err := s.LastIndex()
	if err != nil {
		return err
	}
	if last != m.last {
		return fmt.Errorf("last %d, want %d", last, m.last)
	}
	if first > m.first || first < m.minFirst {
		return fmt.Errorf("first %d, want in [%d, %d]", first, m.minFirst, m.first)
	}
	hs, _, err := s.InitialState()
	if err != nil {
		return err
	}
	if !sameHS(hs, m.hs) {
		return fmt.Errorf("hard state %+v, want %+v", hs, m.hs)
	}
	if term, err := s.Term(first - 1); err != nil || term != m.term(first-1) {
		return fmt.Errorf("Term(%d) = %d, %v; want %d", first-1, term, err, m.term(first-1))
	}
	if first <= last {
		got, err := s.Entries(first, last+1, math.MaxUint64)
		if err != nil {
			return fmt.Errorf("Entries(%d, %d): %v", first, last+1, err)
		}
		for k, e := range got {
			want := m.log[first+uint64(k)]
			if e.GetIndex() != want.GetIndex() || e.GetTerm() != want.GetTerm() || !bytes.Equal(e.Data, want.Data) {
				return fmt.Errorf("entry %d: got (i%d t%d) want (i%d t%d)", first+uint64(k), e.GetIndex(), e.GetTerm(), want.GetIndex(), want.GetTerm())
			}
		}
	}
	snap, err := s.Snapshot()
	if err != nil {
		return fmt.Errorf("Snapshot: %v", err)
	}
	si := snap.Metadata.GetIndex()
	if si < m.snapMin || si > m.snapMax {
		return fmt.Errorf("snapshot %d, want in [%d, %d]", si, m.snapMin, m.snapMax)
	}
	if si > 0 && !bytes.Equal(snap.Data, snapPayload(s.id, si)) {
		return fmt.Errorf("snapshot %d has wrong data (%d bytes)", si, len(snap.Data))
	}
	return nil
}

// matchWorld checks every group against any of the candidate states.
func matchWorld(l *Log, cands ...*world) error {
	for id := range cands[0].g {
		var errs []string
		ok := false
		for _, w := range cands {
			if err := matchGroup(l.Group(id), w.g[id]); err != nil {
				errs = append(errs, err.Error())
			} else {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("group %d matches no acceptable state: %s", id, strings.Join(errs, " | "))
		}
	}
	return nil
}

// workload runs nops generated operations, returning the model state after
// each one (states[0] is the initial state). acked is updated after each
// operation returns.
func workload(t testing.TB, l *Log, seed int64, nops int, acked *atomic.Int64) []*world {
	states, _ := workloadP(t, l, seed, nops, acked)
	return states
}

// workloadP also returns, for each operation i (1-based), the extra partial
// state a crash during it may leave, if any.
func workloadP(t testing.TB, l *Log, seed int64, nops int, acked *atomic.Int64) ([]*world, map[int]*world) {
	rng := rand.New(rand.NewSource(seed))
	w := newWorld(4)
	states := []*world{w.clone()}
	partials := map[int]*world{}
	for i := 0; i < nops; i++ {
		o := genOp(rng, w)
		if p := o.partial(w); p != nil {
			partials[i+1] = p
		}
		err := o.run(l, w)
		if err == nil {
			o.apply(w)
		} else if l.failure() != nil {
			// A crash point or injected fault: the caller decides.
			states = append(states, w.clone())
			acked.Store(int64(i + 1))
			for j := i + 1; j < nops; j++ {
				states = append(states, w.clone())
			}
			return states, partials
		} else if !errors.Is(err, raft.ErrSnapOutOfDate) {
			t.Fatalf("op %d (%v): %v", i, o, err)
		}
		states = append(states, w.clone())
		acked.Store(int64(i + 1))
	}
	return states, partials
}

// candidates are the acceptable states after a crash with a acked
// operations: before or after the one in flight, or its partial state.
func candidates(states []*world, partials map[int]*world, a int64) []*world {
	c := []*world{states[a]}
	if int(a)+1 < len(states) {
		c = append(c, states[a+1])
		if p := partials[int(a)+1]; p != nil {
			c = append(c, p)
		}
	}
	return c
}

var harnessOpts = Options{
	SegmentSize:         64 << 10,
	CleanAfterSegments:  1,
	SnapshotInlineBytes: 4 << 10,
	CacheEntries:        16,
}

// countingFS calls hook before every mutating filesystem operation.
func countingFS(mem vfs.FS, hook func(op crashfs.Op) error) vfs.FS {
	return crashfs.WithHook(mem, func(op crashfs.Op) error {
		if op.Kind.IsWrite() {
			return hook(op)
		}
		return nil
	})
}

// ---- crash at every filesystem operation ----

// TestCrashAtEveryOp runs a fixed workload once per mutating filesystem
// operation (segment writes and fsyncs, checkpoint and index sidecar
// writes, renames, directory syncs) and simulates a power cut immediately
// before that operation: keeping no unsynced data, all of it, and a random
// half of it (torn and reordered writes, a random prefix of directory
// operations). Every recovered log must hold exactly the state after the
// last acknowledged operation (or after the one in flight), and must
// recover again from a second power cut during its own recovery.
func TestCrashAtEveryOp(t *testing.T) {
	const nops = 120
	seed := int64(42)
	// Dry run: count operations.
	var total atomic.Int64
	mem := crashfs.New()
	l, err := Open(crashDir, memOpts(countingFS(mem, func(crashfs.Op) error { total.Add(1); return nil }), harnessOpts))
	if err != nil {
		t.Fatal(err)
	}
	var acked atomic.Int64
	states, partials := workloadP(t, l, seed, nops, &acked)
	l.Close()
	n := int(total.Load())
	stride := 1
	if testing.Short() {
		stride = n/60 + 1
	}
	t.Logf("%d filesystem operations, crashing before every %d", n, stride)
	checked := 0
	for k := 1; k <= n; k += stride {
		for _, keep := range []int{0, 100, 50} {
			mem := crashfs.New()
			var count atomic.Int64
			var clone *crashfs.FS
			var ackedAt int64
			var acked atomic.Int64
			fs := countingFS(mem, func(crashfs.Op) error {
				if count.Add(1) == int64(k) {
					clone = mem.CrashClone(keep, randv2.New(randv2.NewPCG(uint64(k), uint64(keep))))
					ackedAt = acked.Load()
				}
				return nil
			})
			l, err := Open(crashDir, memOpts(fs, harnessOpts))
			if err != nil {
				t.Fatal(err)
			}
			workload(t, l, seed, nops, &acked)
			l.Close()
			if clone == nil {
				continue // a later operation count than this run reached
			}
			cands := candidates(states, partials, ackedAt)
			verifyClone(t, fmt.Sprintf("crash before op %d (keep %d%%, %d acked)", k, keep, ackedAt), clone, cands)
			if checked%7 == 0 {
				crashDuringRecovery(t, fmt.Sprintf("op %d keep %d", k, keep), clone, cands, int64(k))
			}
			checked++
		}
	}
	t.Logf("verified %d crash images", checked)
}

func verifyClone(t *testing.T, what string, fs *crashfs.FS, cands []*world) {
	t.Helper()
	img := fs.CrashClone(100, randv2.New(randv2.NewPCG(1, 2)))
	l, err := Open(crashDir, memOpts(img, Options{ScrubOnOpen: true}))
	if err != nil {
		t.Fatalf("%s: open: %v", what, err)
	}
	defer l.Close()
	if err := matchWorld(l, cands...); err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// crashDuringRecovery power-cuts the recovery of fs at several points and
// checks that the result still recovers to an acceptable state; sometimes
// it power-cuts the recovery of that result too.
func crashDuringRecovery(t *testing.T, what string, fs *crashfs.FS, cands []*world, seed int64) {
	t.Helper()
	crashDuringRecoveryN(t, what, fs, cands, seed, 0)
}

func crashDuringRecoveryN(t *testing.T, what string, fs *crashfs.FS, cands []*world, seed int64, depth int) {
	t.Helper()
	var total atomic.Int64
	probe := fs.CrashClone(100, randv2.New(randv2.NewPCG(1, 2)))
	l, err := Open(crashDir, memOpts(countingFS(probe, func(crashfs.Op) error { total.Add(1); return nil }), Options{}))
	if err != nil {
		t.Fatalf("%s: probe open: %v", what, err)
	}
	l.Close()
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < 3 && total.Load() > 0; i++ {
		k := 1 + rng.Int63n(total.Load())
		img := fs.CrashClone(100, randv2.New(randv2.NewPCG(1, 2)))
		var count atomic.Int64
		var second *crashfs.FS
		cfs := countingFS(img, func(crashfs.Op) error {
			if count.Add(1) == k {
				second = img.CrashClone([]int{0, 100, 50, 10, 90}[rng.Intn(5)], randv2.New(randv2.NewPCG(uint64(seed), uint64(k))))
			}
			return nil
		})
		if l, err := Open(crashDir, memOpts(cfs, Options{})); err == nil {
			l.Close()
		}
		if second != nil {
			w := fmt.Sprintf("%s, then crash during recovery op %d", what, k)
			verifyClone(t, w, second, cands)
			if depth < 2 && rng.Intn(3) == 0 {
				crashDuringRecoveryN(t, w, second, cands, seed*31+k, depth+1)
			}
		}
	}
}

// ---- an I/O error at every operation ----

// TestErrorAtEveryOp fails each mutating operation on segment and snapshot
// files in turn. Nothing may panic or hang; after the error the log either
// keeps working (snapshot file errors) or fails closed, and reopening
// recovers every acknowledged operation.
func TestErrorAtEveryOp(t *testing.T) {
	const nops = 80
	seed := int64(7)
	ours := func(op crashfs.Op) bool {
		return strings.Contains(op.Path, "/segments") || strings.Contains(op.Path, "/snapshots")
	}
	var total atomic.Int64
	l, err := Open(crashDir, memOpts(countingFS(crashfs.New(), func(op crashfs.Op) error {
		if ours(op) {
			total.Add(1)
		}
		return nil
	}), harnessOpts))
	if err != nil {
		t.Fatal(err)
	}
	var a atomic.Int64
	workload(t, l, seed, nops, &a)
	l.Close()
	n := int(total.Load())
	stride := 1
	if testing.Short() {
		stride = n/40 + 1
	}
	for k := 1; k <= n; k += stride {
		mem := crashfs.New()
		var count atomic.Int64
		var injected atomic.Bool
		fs := countingFS(mem, func(op crashfs.Op) error {
			if ours(op) && count.Add(1) == int64(k) {
				injected.Store(true)
				return crashfs.ErrInjected
			}
			return nil
		})
		l, err := Open(crashDir, memOpts(fs, harnessOpts))
		if err != nil {
			continue // the fault hit Open itself, which reported it
		}
		var acked atomic.Int64
		type res struct {
			s []*world
			p map[int]*world
		}
		done := make(chan res, 1)
		go func() {
			s, p := workloadUntilError(t, l, seed, nops, &acked)
			done <- res{s, p}
		}()
		var r res
		select {
		case r = <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("op %d: workload hung after injected error", k)
		}
		l.Close()
		l2, err := Open(crashDir, memOpts(mem, Options{ScrubOnOpen: true}))
		if err != nil {
			t.Fatalf("error at op %d: reopen: %v", k, err)
		}
		a := acked.Load()
		if err := matchWorld(l2, candidates(r.s, r.p, a)...); err != nil {
			t.Fatalf("error at op %d (%d acked): %v", k, a, err)
		}
		l2.Close()
	}
}

// workloadUntilError stops at the first failed operation; states[acked+1]
// is the state had that operation succeeded.
func workloadUntilError(t testing.TB, l *Log, seed int64, nops int, acked *atomic.Int64) ([]*world, map[int]*world) {
	rng := rand.New(rand.NewSource(seed))
	w := newWorld(4)
	states := []*world{w.clone()}
	partials := map[int]*world{}
	for i := 0; i < nops; i++ {
		o := genOp(rng, w)
		if p := o.partial(w); p != nil {
			partials[i+1] = p
		}
		err := o.run(l, w)
		if err != nil && errors.Is(err, raft.ErrSnapOutOfDate) {
			states = append(states, w.clone())
			acked.Store(int64(i + 1))
			continue
		}
		next := w.clone()
		o.apply(next)
		if err != nil {
			return append(states, next), partials
		}
		w = next
		states = append(states, w.clone())
		acked.Store(int64(i + 1))
	}
	return states, partials
}

// ---- metamorphic: options must not change what is stored ----

func logicalState(t testing.TB, l *Log, ids []uint64) string {
	var b strings.Builder
	for _, id := range ids {
		s := l.Group(id)
		fi, _ := s.FirstIndex()
		li, _ := s.LastIndex()
		hs, cs, _ := s.InitialState()
		snap, err := s.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&b, "g%d [%d,%d] hs%v cs%v snap%d/%d/%x\n", id, fi, li, hs, cs.Voters, snap.Metadata.GetIndex(), snap.Metadata.GetTerm(), fnvSum(snap.Data))
		if fi <= li {
			es, err := s.Entries(fi, li+1, math.MaxUint64)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range es {
				fmt.Fprintf(&b, "  %d/%d/%x\n", e.GetIndex(), e.GetTerm(), fnvSum(e.Data))
			}
		}
	}
	return b.String()
}

func fnvSum(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

func TestMetamorphicOptions(t *testing.T) {
	variants := []Options{
		harnessOpts,
		{PipelineDepth: 1, SegmentSize: 1 << 20, CacheEntries: -1},
		{PipelineDepth: 16, SegmentSize: 64 << 10, NoPreallocate: true, SnapshotInlineBytes: 1 << 30, CleanMaxLiveRatio: -1},
		{SegmentSize: 128 << 10, NoAutoGC: true, CacheBytes: 32 << 10, CleanAfterSegments: 1, CleanMaxLiveRatio: 1},
		{NoSync: true, MaxBatchUpdates: 1, CheckpointInterval: time.Nanosecond},
	}
	ids := []uint64{1, 2, 3, 4}
	var want string
	for vi, o := range variants {
		mem := crashfs.New()
		l, err := Open(crashDir, memOpts(mem, o))
		if err != nil {
			t.Fatal(err)
		}
		var a atomic.Int64
		workload(t, l, 99, 400, &a)
		live := logicalState(t, l, ids)
		l.Close()
		l, err = Open(crashDir, memOpts(mem, o))
		if err != nil {
			t.Fatal(err)
		}
		reopened := logicalState(t, l, ids)
		if rep, err := l.Scrub(); err != nil || len(rep.Problems) > 0 {
			t.Fatalf("variant %d scrub: %v %v", vi, err, rep.Problems)
		}
		l.Close()
		if live != reopened {
			t.Fatalf("variant %d: state changed across reopen", vi)
		}
		if vi == 0 {
			want = live
		} else if live != want {
			t.Fatalf("variant %d stores a different logical state:\n%s\nvs\n%s", vi, firstDiff(live, want), "")
		}
	}
}

func firstDiff(a, b string) string {
	al, bl := strings.Split(a, "\n"), strings.Split(b, "\n")
	for i := range al {
		if i >= len(bl) || al[i] != bl[i] {
			other := "<end>"
			if i < len(bl) {
				other = bl[i]
			}
			return fmt.Sprintf("line %d: %q vs %q", i, al[i], other)
		}
	}
	return "length differs"
}

// ---- fuzzing ----

// FuzzScanner feeds arbitrary bytes to the segment scanner, resync and
// classify logic and the chunk decoder. They must never panic or loop.
func FuzzScanner(f *testing.F) {
	data, _ := buildCommits(4, 3, 3000)
	f.Add(data)
	f.Add(data[:len(data)/2])
	f.Add(make([]byte, 100))
	f.Fuzz(func(t *testing.T, b []byte) {
		sc := &segScanner{f: crashfs.NewFile(b), size: int64(len(b)), blk: make([]byte, blockSize), blkN: -1}
		pos := int64(0)
		for i := 0; i < 10000; i++ {
			rec, err := sc.next(pos)
			if err != nil {
				var bd *badData
				if errors.As(err, &bd) {
					sc.classify(bd.at)
					sc.tornLike(bd)
				}
				break
			}
			if rec.end <= pos {
				t.Fatalf("scanner did not advance at %d", pos)
			}
			decodeRecord(rec.data)
			pos = rec.end
		}
		decodeChunks(b, 0, 0)
		if len(b) > 8 {
			decodeCommitMarker(b[1:])
			decodeSnapRecord(b)
			entryHeader(b)
			decodeSegIndex(b, 1, 1<<20, uint64(b[0]))
			decodeCheckpoint(b)
		}
	})
}

// FuzzCorruptLog flips bytes in the segment and snapshot files of a closed
// log. Open may refuse (with an error, never a panic); if it opens, every
// read must return either the right data or an error, and if Scrub finds
// nothing wrong, every read must have succeeded.
func FuzzCorruptLog(f *testing.F) {
	f.Add(int64(1), []byte{0, 0, 10, 0, 0xff})
	f.Add(int64(2), []byte{1, 0, 0x40, 0, 0x01, 0, 0, 0x80, 1, 0x10})
	f.Add(int64(3), []byte{2, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	f.Fuzz(corruptLogCase)
}

// corruptLogCase is one FuzzCorruptLog input: a workload from seed, then
// byte flips, five bytes each (file, offset, xor).
func corruptLogCase(t *testing.T, seed int64, flips []byte) {
	mem := crashfs.New()
	l, err := Open(crashDir, memOpts(mem, harnessOpts))
	if err != nil {
		t.Fatal(err)
	}
	var a atomic.Int64
	states := workload(t, l, seed, 40, &a)
	l.Close()
	final := states[len(states)-1]
	var files []string
	for _, d := range []string{crashDir + "/segments", crashDir + "/snapshots", crashDir} {
		names, _ := mem.List(d)
		sort.Strings(names)
		for _, n := range names {
			files = append(files, d+"/"+n)
		}
	}
	if len(files) == 0 {
		return
	}
	for i := 0; i+4 < len(flips); i += 5 {
		p := files[int(flips[i])%len(files)]
		fh, err := mem.OpenReadWrite(p)
		if err != nil {
			continue
		}
		st, _ := fh.Stat()
		if st.Size() > 0 {
			off := int64(uint32(flips[i+1])<<16|uint32(flips[i+2])<<8|uint32(flips[i+3])) % st.Size()
			b := []byte{0}
			fh.ReadAt(b, off)
			b[0] ^= flips[i+4] | 1
			fh.WriteAt(b, off)
		}
		fh.Close()
	}
	l, err = Open(crashDir, memOpts(mem, Options{}))
	if err != nil {
		return // refused: fine
	}
	defer l.Close()
	readErr := false
	for id, m := range final.g {
		if err := matchGroup(l.Group(id), m); err != nil {
			if !errors.Is(err, ErrCorrupt) && !strings.Contains(err.Error(), "corruption") {
				t.Fatalf("group %d returned wrong data instead of an error: %v", id, err)
			}
			readErr = true
		}
	}
	rep, err := l.Scrub()
	if err != nil {
		t.Fatal(err)
	}
	if readErr && len(rep.Problems) == 0 {
		t.Fatal("reads failed but Scrub found nothing")
	}
}
