// Copyright 2026 The raftseg authors.

package seglog

import (
	"fmt"
	"math"
	"math/rand"
	randv2 "math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
	"github.com/raftseg/raftseg/vfs"
)

// These tests run the log on crashfs, a crashable in-memory filesystem.
// CrashClone produces the filesystem a machine could come back with after a
// power cut at that instant: everything that was synced, plus a random
// subset of unsynced file blocks and directory entries. That exercises the
// real durability contract, including checkpoints, index sidecars,
// directory syncs, and torn segment writes.

const crashDir = "/data"

func memOpts(fs vfs.FS, o Options) Options {
	o.FS = fs
	return o
}

// groupModel is what a group must look like after recovery.
type groupModel struct {
	log      map[uint64]raftpb.Entry // current content of every index <= last, compacted or not
	termAt   map[uint64]uint64       // terms known only from snapshots
	first    uint64                  // FirstIndex as of the last acknowledged op
	minFirst uint64                  // FirstIndex can be no lower than this (synced snapshot apply)
	last     uint64
	hs       raftpb.HardState
	snapMin  uint64 // snapshot index guaranteed durable
	snapMax  uint64 // newest snapshot index created
}

func newGroupModel() *groupModel {
	return &groupModel{log: map[uint64]raftpb.Entry{}, termAt: map[uint64]uint64{0: 0}, first: 1, minFirst: 1}
}

func (m *groupModel) term(i uint64) uint64 {
	if e, ok := m.log[i]; ok {
		return e.Term
	}
	return m.termAt[i]
}

func (m *groupModel) append(es []raftpb.Entry) {
	if len(es) == 0 {
		return
	}
	for i := es[0].Index; i <= m.last; i++ {
		delete(m.log, i)
	}
	for _, e := range es {
		m.log[e.Index] = e
	}
	m.last = es[len(es)-1].Index
}

// checkGroup verifies a recovered group against the model. Compactions and
// snapshot creations are not synced, so they may have been lost; everything
// acknowledged by Save must be there exactly.
func checkGroup(t *testing.T, what string, s *Storage, m *groupModel) {
	t.Helper()
	first, _ := s.FirstIndex()
	last, _ := s.LastIndex()
	if last != m.last {
		t.Fatalf("%s: last %d, want %d", what, last, m.last)
	}
	if first > m.first || first < m.minFirst {
		t.Fatalf("%s: first %d, want in [%d, %d]", what, first, m.minFirst, m.first)
	}
	if hs, _, _ := s.InitialState(); hs != m.hs {
		t.Fatalf("%s: hard state %+v, want %+v", what, hs, m.hs)
	}
	if term, err := s.Term(first - 1); err != nil || term != m.term(first-1) {
		t.Fatalf("%s: Term(%d) = %d, %v; want %d", what, first-1, term, err, m.term(first-1))
	}
	if first <= last {
		got, err := s.Entries(first, last+1, math.MaxUint64)
		if err != nil {
			t.Fatalf("%s: Entries(%d, %d): %v", what, first, last+1, err)
		}
		for k, e := range got {
			want := m.log[first+uint64(k)]
			if e.Index != want.Index || e.Term != want.Term || string(e.Data) != string(want.Data) {
				t.Fatalf("%s: entry %d: got (i%d t%d) want (i%d t%d)", what, first+uint64(k), e.Index, e.Term, want.Index, want.Term)
			}
		}
	}
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if si := snap.Metadata.Index; si < m.snapMin || si > m.snapMax {
		t.Fatalf("%s: snapshot %d, want in [%d, %d]", what, si, m.snapMin, m.snapMax)
	}
}

func TestCrashCloneSequential(t *testing.T) {
	seeds := 12
	if testing.Short() {
		seeds = 3
	}
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runCrashSequential(t, seed) })
	}
}

func runCrashSequential(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	fs := crashfs.New()
	opts := Options{
		SegmentSize:        64 << 10,
		CacheEntries:       rng.Intn(40) - 10,
		CleanAfterSegments: 1,
		// Power cuts only ever lose unsynced data, so the tail damage they
		// leave must always be recognised as an interrupted write.
	}
	l, err := Open(crashDir, memOpts(fs, opts))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { l.Close() }()
	const groups = 5
	models := map[uint64]*groupModel{}
	terms := map[uint64]uint64{}
	for g := uint64(1); g <= groups; g++ {
		models[g], terms[g] = newGroupModel(), 1
	}
	crashes := 0
	for step := 0; step < 700; step++ {
		g := uint64(1 + rng.Intn(groups))
		m := models[g]
		switch r := rng.Intn(20); {
		case r < 12: // appends to several groups in one Save
			var ups []Update
			seen := map[uint64]bool{}
			for k := 0; k < 1+rng.Intn(3); k++ {
				gg := uint64(1 + rng.Intn(groups))
				if seen[gg] {
					continue
				}
				seen[gg] = true
				mm := models[gg]
				start := mm.last + 1
				if mm.last >= mm.first && mm.last > mm.hs.Commit && rng.Intn(3) == 0 {
					// overwrite an uncommitted suffix, as a new leader would
					lo := max(mm.first, mm.hs.Commit+1)
					start = lo + uint64(rng.Int63n(int64(mm.last-lo+1)))
					terms[gg]++
				}
				n := uint64(1 + rng.Intn(8))
				size := 50 + rng.Intn(1500)
				if rng.Intn(15) == 0 {
					size = 40 << 10
				}
				es := ents(terms[gg], start, start+n-1, size)
				commit := mm.hs.Commit
				if c := start + n - 1 - uint64(rng.Intn(int(n))); c > commit {
					commit = c
				}
				ups = append(ups, Update{Group: gg, Entries: es, HardState: raftpb.HardState{Term: terms[gg], Commit: commit}})
			}
			if err := l.Save(ups...); err != nil {
				t.Fatalf("step %d: %v", step, err)
			}
			for _, u := range ups {
				models[u.Group].append(u.Entries)
				models[u.Group].hs = u.HardState
			}
		case r < 15 && m.hs.Commit >= m.first: // snapshot + compaction of applied entries
			i := m.first + uint64(rng.Int63n(int64(m.hs.Commit-m.first+1)))
			s := l.Group(g)
			if i > m.snapMax {
				if _, err := s.CreateSnapshot(i, &raftpb.ConfState{Voters: []uint64{1}}, []byte(fmt.Sprint(i))); err != nil {
					t.Fatal(err)
				}
				m.snapMax = i
			}
			if i >= m.first && rng.Intn(4) != 0 {
				if err := s.Compact(i); err != nil {
					t.Fatal(err)
				}
				m.first = i + 1
				m.termAt[i] = m.term(i)
			}
		case r < 16: // a snapshot received from a leader (synced)
			idx := max(m.last, m.snapMax) + 1 + uint64(rng.Intn(20))
			terms[g]++
			snap := raftpb.Snapshot{Data: []byte("remote"), Metadata: raftpb.SnapshotMetadata{Index: idx, Term: terms[g]}}
			u := Update{Group: g, Snapshot: snap, HardState: raftpb.HardState{Term: terms[g], Commit: idx}}
			if rng.Intn(2) == 0 {
				u.Entries = ents(terms[g], idx+1, idx+2, 100)
			}
			if err := l.Save(u); err != nil {
				t.Fatal(err)
			}
			m.log = map[uint64]raftpb.Entry{}
			m.termAt[idx] = terms[g]
			m.first, m.minFirst, m.last = idx+1, idx+1, idx
			m.snapMin, m.snapMax = idx, idx
			m.append(u.Entries)
			m.hs = u.HardState
		case r < 18:
			if _, err := l.GC(); err != nil {
				t.Fatal(err)
			}
		default: // power cut
			crashes++
			clone := fs.CrashClone(rng.Intn(101), randv2.New(randv2.NewPCG(rng.Uint64(), rng.Uint64())))
			l.Close()
			fs = clone
			if l, err = Open(crashDir, memOpts(fs, opts)); err != nil {
				t.Fatalf("step %d: reopen after crash %d: %v", step, crashes, err)
			}
			for gg, mm := range models {
				checkGroup(t, fmt.Sprintf("step %d crash %d group %d", step, crashes, gg), l.Group(gg), mm)
				mm.first, _ = l.Group(gg).FirstIndex()
				snap, _ := l.Group(gg).Snapshot()
				mm.snapMin, mm.snapMax = snap.Metadata.Index, snap.Metadata.Index
			}
			if rep, err := l.Scrub(); err != nil || len(rep.Problems) > 0 {
				t.Fatalf("scrub after crash %d: %v %v", crashes, err, rep.Problems)
			}
		}
	}
	st, _ := l.Stats()
	t.Logf("%d crashes, %d commits, %d segments live, %d removed, %d segments cleaned",
		crashes, st.Commits, len(st.Segments), st.RemovedSegments, st.CleanedSegments)
}

// Writers on many groups and concurrent readers run while the filesystem is
// crash-cloned at random moments. After recovery each group must be in a
// state its writer reached, no older than the last Save that returned.
func TestCrashCloneConcurrent(t *testing.T) {
	rounds := 8
	if testing.Short() {
		rounds = 2
	}
	for r := 0; r < rounds; r++ {
		t.Run(fmt.Sprint(r), func(t *testing.T) { runCrashConcurrent(t, int64(r)) })
	}
}

func runCrashConcurrent(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	fs := crashfs.New()
	opts := Options{SegmentSize: 64 << 10}
	l, err := Open(crashDir, memOpts(fs, opts))
	if err != nil {
		t.Fatal(err)
	}
	const groups, ops = 8, 60
	// history[g][k] is the update of op k+1; op k sets HardState.Vote = k,
	// which identifies the state a group recovered to.
	history := make([][]Update, groups+1)
	var acked [groups + 1]atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := uint64(1); g <= groups; g++ {
		r := rand.New(rand.NewSource(seed*100 + int64(g)))
		var last uint64
		term := uint64(1)
		for k := 1; k <= ops; k++ {
			start := last + 1
			if last > 3 && r.Intn(4) == 0 {
				start = last - uint64(r.Intn(3))
				term++
			}
			n := uint64(1 + r.Intn(4))
			history[g] = append(history[g], Update{Group: g, Entries: ents(term, start, start+n-1, 200+r.Intn(3000)),
				HardState: raftpb.HardState{Term: term, Vote: uint64(k)}})
			last = start + n - 1
		}
		wg.Add(1)
		go func(g uint64) {
			defer wg.Done()
			s := l.Group(g)
			for k, u := range history[g] {
				if err := l.Save(u); err != nil {
					t.Errorf("group %d op %d: %v", g, k+1, err)
					return
				}
				acked[g].Store(int64(k + 1))
				// Read our own log back while other groups write.
				li, _ := s.LastIndex()
				if _, err := s.Entries(li, li+1, math.MaxUint64); err != nil {
					t.Errorf("group %d read: %v", g, err)
					return
				}
			}
		}(g)
	}
	go func() { // background readers on all groups
		rng := rand.New(rand.NewSource(seed + 7))
		for {
			select {
			case <-stop:
				return
			default:
			}
			s := l.Group(uint64(1 + rng.Intn(groups)))
			fi, _ := s.FirstIndex()
			li, _ := s.LastIndex()
			if fi <= li {
				s.Entries(fi, li+1, 1<<16)
			}
		}
	}()
	var clones []*crashfs.FS
	var floors [][groups + 1]int64
	for i := 0; i < 5; i++ {
		var floor [groups + 1]int64
		for g := 1; g <= groups; g++ {
			floor[g] = acked[g].Load()
		}
		clones = append(clones, fs.CrashClone(rng.Intn(101), randv2.New(randv2.NewPCG(rng.Uint64(), rng.Uint64()))))
		floors = append(floors, floor)
	}
	wg.Wait()
	close(stop)
	l.Close()

	for ci, clone := range clones {
		rl, err := Open(crashDir, memOpts(clone, opts))
		if err != nil {
			t.Fatalf("clone %d: %v", ci, err)
		}
		for g := uint64(1); g <= groups; g++ {
			s := rl.Group(g)
			hs, _, _ := s.InitialState()
			k := int(hs.Vote)
			if int64(k) < floors[ci][g] {
				t.Fatalf("clone %d group %d: recovered op %d, but op %d was acknowledged", ci, g, k, floors[ci][g])
			}
			// Rebuild the expected log after op k.
			m := newGroupModel()
			for _, u := range history[g][:k] {
				m.append(u.Entries)
				m.hs = u.HardState
			}
			if k == 0 {
				m.hs = raftpb.HardState{}
			}
			m.snapMax = 0
			checkGroup(t, fmt.Sprintf("clone %d group %d op %d", ci, g, k), s, m)
		}
		if rep, err := rl.Scrub(); err != nil || len(rep.Problems) > 0 {
			t.Fatalf("clone %d scrub: %v %v", ci, err, rep.Problems)
		}
		rl.Close()
	}
}
