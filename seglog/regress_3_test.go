// Copyright 2026 The raftseg authors.

package seglog

import (
	"testing"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
)

func TestRegressCreateSnapshotConfState(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})
	s := l.Group(1)
	m := raft.NewMemoryStorage()
	e := ents(1, 1, 5, 10)
	s.Save(&raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(5))}, e, &raftpb.Snapshot{})
	m.Append(e)
	cs := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	s.CreateSnapshot(5, cs, []byte("x"))
	m.CreateSnapshot(5, cs, []byte("x"))
	_, mcs, _ := m.InitialState()
	_, gcs, _ := s.InitialState()
	l.Close()
	l = open(t, dir, Options{})
	defer l.Close()
	_, rcs, _ := l.Group(1).InitialState()
	if len(gcs.Voters) != len(mcs.Voters) || len(rcs.Voters) != len(mcs.Voters) {
		t.Fatalf("InitialState ConfState after CreateSnapshot: got %v (reopened %v), MemoryStorage %v", gcs.Voters, rcs.Voters, mcs.Voters)
	}
}
