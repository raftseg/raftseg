// Copyright 2026 The raftseg authors.

package seglog

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
)

// compareStorage checks every observable of s against the reference model.
func compareStorage(t testing.TB, what string, s raft.Storage, m *raft.MemoryStorage) {
	t.Helper()
	mf, _ := m.FirstIndex()
	ml, _ := m.LastIndex()
	sf, _ := s.FirstIndex()
	sl, _ := s.LastIndex()
	if mf != sf || ml != sl {
		t.Fatalf("%s: first/last = %d/%d, want %d/%d", what, sf, sl, mf, ml)
	}
	lo := uint64(0)
	if mf > 3 {
		lo = mf - 3
	}
	for i := lo; i <= ml+2; i++ {
		mt, merr := m.Term(i)
		st, serr := s.Term(i)
		if mt != st || !errors.Is(serr, merr) && !(merr == nil && serr == nil) {
			t.Fatalf("%s: Term(%d) = %d,%v want %d,%v", what, i, st, serr, mt, merr)
		}
	}
	if mf <= ml {
		want, _ := m.Entries(mf, ml+1, math.MaxUint64)
		eqEntries(t, what+" all entries", mustEntries(t, s, mf, ml+1), want)
		if mf > 1 {
			if _, err := s.Entries(mf-1, ml+1, math.MaxUint64); !errors.Is(err, raft.ErrCompacted) {
				t.Fatalf("%s: Entries below first: %v", what, err)
			}
		}
	}
	ms, _ := m.Snapshot()
	ss, _ := s.Snapshot()
	if ms.Metadata.Index != ss.Metadata.Index || ms.Metadata.Term != ss.Metadata.Term || string(ms.Data) != string(ss.Data) {
		t.Fatalf("%s: snapshot %+v want %+v", what, ss.Metadata, ms.Metadata)
	}
}

// TestDifferential runs random operation sequences against Log and
// raft.MemoryStorage, interleaved with clean reopens and simulated crashes
// in which the index loses its unsynced tail.
func TestDifferential(t *testing.T) {
	seeds := 25
	if testing.Short() {
		seeds = 5
	}
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) { runDifferential(t, seed) })
	}
}

func runDifferential(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	dir := t.TempDir()
	const groups = 3
	base := Options{SegmentSize: int64(8<<10 + rng.Intn(64<<10)), CacheEntries: rng.Intn(20) - 5}
	l := open(t, dir, base)
	defer func() { l.Close() }()
	models := map[uint64]*raft.MemoryStorage{}
	hard := map[uint64]raftpb.HardState{}
	terms := map[uint64]uint64{}
	for g := uint64(1); g <= groups; g++ {
		models[g] = raft.NewMemoryStorage()
		terms[g] = 1
	}
	verify := func(when string) {
		for g, m := range models {
			compareStorage(t, fmt.Sprintf("%s group %d", when, g), l.Group(g), m)
			if hs, _, _ := l.Group(g).InitialState(); hs != hard[g] {
				t.Fatalf("%s group %d: hardstate %+v want %+v", when, g, hs, hard[g])
			}
		}
	}
	// appendOp builds a random valid append for group g.
	appendOp := func(g uint64) Update {
		m := models[g]
		first, _ := m.FirstIndex()
		last, _ := m.LastIndex()
		start := last + 1
		if last >= first && rng.Intn(3) == 0 {
			start = first + uint64(rng.Int63n(int64(last-first+1))) // overwrite a suffix
			terms[g]++
		}
		n := uint64(1 + rng.Intn(12))
		size := 1 + rng.Intn(300)
		if rng.Intn(10) == 0 {
			size = 40 << 10
		}
		e := ents(terms[g], start, start+n-1, size)
		hs := raftpb.HardState{Term: terms[g], Vote: uint64(rng.Intn(3)), Commit: start + n - 1}
		return Update{Group: g, Entries: e, HardState: hs}
	}
	applyModel := func(u Update) {
		models[u.Group].Append(u.Entries)
		if !raft.IsEmptyHardState(u.HardState) {
			hard[u.Group] = u.HardState
		}
	}

	for round := 0; round < 12; round++ {
		// Phase 1: every kind of operation.
		for op := 0; op < 40; op++ {
			g := uint64(1 + rng.Intn(groups))
			m := models[g]
			first, _ := m.FirstIndex()
			last, _ := m.LastIndex()
			switch r := rng.Intn(10); {
			case r < 6:
				var ups []Update
				for k := 0; k < 1+rng.Intn(3); k++ {
					gg := uint64(1 + rng.Intn(groups))
					if hasGroup(ups, gg) {
						continue
					}
					ups = append(ups, appendOp(gg))
				}
				if err := l.Save(ups...); err != nil {
					t.Fatal(err)
				}
				for _, u := range ups {
					applyModel(u)
				}
			case r == 6 && last >= first:
				ci := first + uint64(rng.Int63n(int64(last-first+1)))
				data := []byte(fmt.Sprintf("snap-%d-%d", g, ci))
				ms, _ := m.Snapshot()
				_, merr := m.CreateSnapshot(ci, nil, data)
				_, serr := l.Group(g).CreateSnapshot(ci, nil, data)
				if (merr == nil) != (serr == nil) || ci > ms.Metadata.Index && serr != nil {
					t.Fatalf("CreateSnapshot(%d): %v vs model %v", ci, serr, merr)
				}
			case r == 7 && hard[g].Commit >= first && last >= first:
				// raft compacts only applied, hence committed, entries
				hi := min(last, hard[g].Commit)
				ci := first + uint64(rng.Int63n(int64(hi-first+1)))
				if err := l.Group(g).Compact(ci); err != nil {
					t.Fatal(err)
				}
				m.Compact(ci)
			case r == 8:
				ms, _ := m.Snapshot()
				idx := ms.Metadata.Index + 1 + uint64(rng.Intn(int(last)+20))
				terms[g]++
				snap := raftpb.Snapshot{Data: []byte(fmt.Sprintf("remote-%d", idx)), Metadata: raftpb.SnapshotMetadata{Index: idx, Term: terms[g]}}
				// As raft does, the Ready carrying a snapshot also commits it.
				u := Update{Group: g, Snapshot: snap, HardState: raftpb.HardState{Term: terms[g], Commit: idx}}
				if rng.Intn(2) == 0 {
					u.Entries = ents(terms[g], idx+1, idx+3, 50)
				}
				hard[g] = u.HardState
				if err := l.Save(u); err != nil {
					t.Fatal(err)
				}
				m.ApplySnapshot(snap)
				m.Append(u.Entries)
			case r == 9:
				if _, err := l.GC(); err != nil {
					t.Fatal(err)
				}
			}
		}
		verify(fmt.Sprintf("round %d live", round))
		l.Close()
		l = open(t, dir, base)
		verify(fmt.Sprintf("round %d reopen", round))

		// Phase 2: the index stops persisting after k commits, then crash.
		l.Close()
		crashOpts := base
		crashOpts.testDropIndexAfter = int64(1 + rng.Intn(4))
		l = open(t, dir, crashOpts)
		for op := 0; op < 3+rng.Intn(12); op++ {
			u := appendOp(uint64(1 + rng.Intn(groups)))
			if err := l.Save(u); err != nil {
				t.Fatal(err)
			}
			applyModel(u)
		}
		l.crash()
		l = open(t, dir, base)
		verify(fmt.Sprintf("round %d after crash", round))
	}
}

func hasGroup(ups []Update, g uint64) bool {
	for _, u := range ups {
		if u.Group == g {
			return true
		}
	}
	return false
}
