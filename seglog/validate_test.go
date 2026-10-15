// Copyright 2026 The raftseg authors.

package seglog

import (
	"errors"
	"math"
	"testing"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
)

// Each case is a Save raft never issues, after which the log could not have
// been reopened: Open refused a commit index beyond the last entry, or an
// index that made FirstIndex wrap to 0. That took down every group on the
// node at its next restart. Save now refuses them, and the log reopens with
// the state from before.
func TestSaveRejectsUnrestartableState(t *testing.T) {
	snap := func(i, term uint64) *raftpb.Snapshot {
		return &raftpb.Snapshot{Data: []byte("s"), Metadata: &raftpb.SnapshotMetadata{Index: new(i), Term: new(term)}}
	}
	for _, tt := range []struct {
		name string
		u    Update
	}{
		{"commit beyond the last entry", Update{HardState: &raftpb.HardState{Term: new(uint64(2)), Commit: new(uint64(9))}}},
		{"commit beyond the appended entries", Update{HardState: &raftpb.HardState{Term: new(uint64(2)), Commit: new(uint64(9))}, Entries: ents(2, 6, 7, 10)}},
		{"overwrite behind the commit index", Update{Entries: ents(3, 2, 2, 10)}},
		{"overwrite behind a new commit index", Update{HardState: &raftpb.HardState{Term: new(uint64(3)), Commit: new(uint64(4))}, Entries: ents(3, 2, 3, 10)}},
		{"snapshot behind the commit index", Update{Snapshot: snap(1, 1)}},
		{"snapshot with a commit beyond it", Update{Snapshot: snap(7, 2), HardState: &raftpb.HardState{Term: new(uint64(2)), Commit: new(uint64(8))}}},
		{"snapshot at MaxUint64", Update{Snapshot: snap(math.MaxUint64, 2)}},
		{"snapshot at 2^63", Update{Snapshot: snap(1<<63, 2)}},
		{"entries up to 2^63", Update{Snapshot: snap(1<<63-1, 2), Entries: ents(2, 1<<63, 1<<63, 1)}},
		{"entries wrapping to 0", Update{Snapshot: snap(math.MaxUint64-2, 2), Entries: []*raftpb.Entry{{Index: new(uint64(math.MaxUint64 - 1)), Term: new(uint64(2))}, {Index: new(uint64(math.MaxUint64)), Term: new(uint64(2))}, {Index: new(uint64(0)), Term: new(uint64(2))}}}},
		{"snapshot with gapped entries", Update{Snapshot: snap(9, 2), Entries: ents(2, 11, 12, 10)}},
		{"entry index 0", Update{Entries: []*raftpb.Entry{{Index: new(uint64(0)), Term: new(uint64(1))}, {Index: new(uint64(1)), Term: new(uint64(1))}}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mem := crashfs.New()
			l := open(t, crashDir, memOpts(mem, Options{}))
			// Entries 1-5, commit 3.
			if err := l.Save(Update{Group: 1, HardState: &raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(3))}, Entries: ents(1, 1, 5, 10)}); err != nil {
				t.Fatal(err)
			}
			u := tt.u
			u.Group = 1
			err := l.Save(u)
			if !errors.Is(err, ErrInvalidUpdate) && !errors.Is(err, raft.ErrSnapOutOfDate) {
				t.Fatalf("Save: %v, want ErrInvalidUpdate", err)
			}
			check := func(when string, l *Log) {
				s := l.Group(1)
				first, _ := s.FirstIndex()
				last, _ := s.LastIndex()
				hs, _, _ := s.InitialState()
				sn, _ := s.Snapshot()
				if first != 1 || last != 5 || hs.GetCommit() != 3 || hs.GetTerm() != 1 || sn.Metadata.GetIndex() != 0 {
					t.Fatalf("%s: first %d, last %d, %+v, snapshot %d: the rejected Save changed the state", when, first, last, hs, sn.Metadata.GetIndex())
				}
			}
			check("live", l)
			l.Close()
			l, err = Open(crashDir, memOpts(mem, Options{}))
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer l.Close()
			check("after reopen", l)
		})
	}
}

// A Save that carries a snapshot and is then found invalid must not leave
// the snapshot behind. The snapshot used to be staged before the entries
// were checked, so it was persisted although Save returned an error.
func TestRejectedSnapshotNotPersisted(t *testing.T) {
	mem := crashfs.New()
	l := open(t, crashDir, memOpts(mem, Options{SnapshotInlineBytes: 16}))
	if err := l.Save(Update{Group: 1, HardState: &raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(2))}, Entries: ents(1, 1, 3, 10)}); err != nil {
		t.Fatal(err)
	}
	big := make([]byte, 100) // kept in a file
	err := l.Save(Update{Group: 1, HardState: &raftpb.HardState{Term: new(uint64(2)), Commit: new(uint64(10))},
		Snapshot: &raftpb.Snapshot{Data: big, Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(10)), Term: new(uint64(2))}},
		Entries:  ents(2, 12, 13, 10)}) // a gap after the snapshot
	if err == nil {
		t.Fatal("accepted")
	}
	for _, when := range []string{"live", "after reopen"} {
		if when != "live" {
			l.Close()
			l = open(t, crashDir, memOpts(mem, Options{SnapshotInlineBytes: 16}))
		}
		s := l.Group(1)
		sn, _ := s.Snapshot()
		first, _ := s.FirstIndex()
		if sn.Metadata.GetIndex() != 0 || first != 1 {
			t.Fatalf("%s: snapshot %d, first %d: the rejected snapshot was applied", when, sn.Metadata.GetIndex(), first)
		}
	}
	l.Close()
	names, _ := mem.List(crashDir + "/snapshots")
	if len(names) != 0 {
		t.Fatalf("snapshot files left behind: %v", names)
	}
}

// Term of an index so large that index+1 wraps is unavailable, not
// compacted.
func TestTermOfHugeIndex(t *testing.T) {
	l := open(t, crashDir, memOpts(crashfs.New(), Options{}))
	defer l.Close()
	s := l.Group(1)
	if err := s.Save(&raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(2))}, ents(1, 1, 3, 1), &raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(2); err != nil {
		t.Fatal(err)
	}
	for _, i := range []uint64{math.MaxUint64, math.MaxUint64 - 1, 1 << 63} {
		if _, err := s.Term(i); !errors.Is(err, raft.ErrUnavailable) {
			t.Errorf("Term(%d): %v, want ErrUnavailable", i, err)
		}
	}
	if err := s.Compact(math.MaxUint64); err == nil || errors.Is(err, raft.ErrCompacted) {
		t.Errorf("Compact(MaxUint64): %v, want out of bounds", err)
	}
}

// Validation keeps one simulated state per group, in a small array or, for
// requests touching many groups, a map: both must see every update of a
// group, in order.
func TestValidateManyGroups(t *testing.T) {
	for _, groups := range []uint64{3, 40} {
		l := open(t, crashDir, memOpts(crashfs.New(), Options{}))
		var ups []Update
		for g := uint64(1); g <= groups; g++ {
			ups = append(ups, Update{Group: g, Entries: ents(1, 1, 2, 10)})
		}
		// Each group again, later in the same request: entries 3 and 4, and
		// a commit that only these make valid.
		for g := uint64(1); g <= groups; g++ {
			ups = append(ups, Update{Group: g, Entries: ents(1, 3, 4, 10), HardState: &raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(4))}})
		}
		if err := l.Save(ups...); err != nil {
			t.Fatalf("%d groups: %v", groups, err)
		}
		// One group's commit beyond its entries, at the end of a long request.
		ups = ups[:0]
		for g := uint64(1); g <= groups; g++ {
			ups = append(ups, Update{Group: g, Entries: ents(1, 5, 5, 10), HardState: &raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(5))}})
		}
		ups = append(ups, Update{Group: groups / 2, HardState: &raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(6))}})
		if err := l.Save(ups...); !errors.Is(err, ErrInvalidUpdate) {
			t.Fatalf("%d groups: %v", groups, err)
		}
		for g := uint64(1); g <= groups; g++ {
			if last, _ := l.Group(g).LastIndex(); last != 4 {
				t.Fatalf("%d groups: group %d last %d after a rejected request", groups, g, last)
			}
		}
		l.Close()
	}
}
