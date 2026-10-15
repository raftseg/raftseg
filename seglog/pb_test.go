// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package seglog

import (
	"errors"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/raftseg/raft/raftpb"
)

// sameHS and sameCS compare hard states and configurations by value:
// protobuf messages are pointers, and an unset field reads as zero.
func sameHS(a, b *raftpb.HardState) bool {
	return a.GetTerm() == b.GetTerm() && a.GetVote() == b.GetVote() && a.GetCommit() == b.GetCommit()
}

func sameCS(a, b *raftpb.ConfState) bool {
	eq := func(x, y []uint64) bool {
		if len(x) != len(y) {
			return false
		}
		for i := range x {
			if x[i] != y[i] {
				return false
			}
		}
		return true
	}
	return eq(a.GetVoters(), b.GetVoters()) && eq(a.GetLearners(), b.GetLearners()) &&
		eq(a.GetVotersOutgoing(), b.GetVotersOutgoing()) && eq(a.GetLearnersNext(), b.GetLearnersNext()) &&
		a.GetAutoLeave() == b.GetAutoLeave()
}

// unknownFields returns 1 to 4 fields raftpb.Entry does not define, of
// every wire type, groups included: protobuf keeps such fields and encodes
// them again.
func unknownFields(r *rand.Rand) []byte {
	var b []byte
	for n := 1 + r.Intn(4); n > 0; n-- {
		num := protowire.Number(5 + r.Intn(1000))
		switch r.Intn(5) {
		case 0:
			b = protowire.AppendTag(b, num, protowire.VarintType)
			b = protowire.AppendVarint(b, r.Uint64()>>r.Intn(64))
		case 1:
			b = protowire.AppendTag(b, num, protowire.Fixed32Type)
			b = protowire.AppendFixed32(b, r.Uint32())
		case 2:
			b = protowire.AppendTag(b, num, protowire.Fixed64Type)
			b = protowire.AppendFixed64(b, r.Uint64())
		case 3:
			v := make([]byte, r.Intn(40))
			r.Read(v)
			b = protowire.AppendTag(b, num, protowire.BytesType)
			b = protowire.AppendBytes(b, v)
		case 4:
			b = protowire.AppendTag(b, num, protowire.StartGroupType)
			b = protowire.AppendTag(b, 1, protowire.VarintType)
			b = protowire.AppendVarint(b, r.Uint64())
			b = protowire.AppendTag(b, num, protowire.EndGroupType)
		}
	}
	return b
}

// randomEntry returns an entry with each field set or not, at random, and
// values at the edges of their encodings, and now and then fields
// raftpb.Entry does not define.
func randomEntry(r *rand.Rand) *raftpb.Entry {
	pick := func(vs ...uint64) uint64 { return vs[r.Intn(len(vs))] }
	e := &raftpb.Entry{}
	if r.Intn(4) != 0 {
		e.Type = raftpb.EntryType(pick(0, 1, 2, 7, math.MaxInt32, 1<<31)).Enum() // 1<<31: a negative int32
	}
	if e.GetType() == raftpb.EntryConfChange || e.GetType() == raftpb.EntryConfChangeV2 {
		e.Type = nil // a configuration change must decode; these are just data
	}
	if r.Intn(4) != 0 {
		e.Term = new(pick(0, 1, 127, 128, 1<<32, math.MaxUint64, r.Uint64()))
	}
	if r.Intn(4) != 0 {
		e.Index = new(pick(0, 1, 16383, 16384, math.MaxUint64, r.Uint64()))
	}
	switch r.Intn(4) {
	case 0: // nil
	case 1:
		e.Data = []byte{}
	default:
		e.Data = make([]byte, r.Intn(300))
		r.Read(e.Data)
	}
	if r.Intn(4) == 0 {
		e.ProtoReflect().SetUnknown(unknownFields(r))
	}
	return e
}

// Entries read back are the ones saved, unset and unknown fields included,
// as from raft.MemoryStorage: seglog stores protobuf's encoding. They read
// back so from the cache, from disk after a restart, and after the
// segments' index files are lost and rebuilt; and Scrub finds them sound.
func TestEntriesRoundTripExactly(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	dir := t.TempDir()
	// Small segments, so that most entries are in sealed segments, with
	// index files, by the end.
	opts := Options{SegmentSize: 64 << 10}
	l := open(t, dir, opts)
	defer func() { l.Close() }()
	s := l.Group(1)
	var saved, want []*raftpb.Entry
	unknown := 0
	const n = 1500
	for i := uint64(1); i <= n; i++ {
		e := randomEntry(r)
		e.Index, e.Term = new(i), new(uint64(1)) // a log needs these
		if r.Intn(3) == 0 {
			e.Type = nil // unset, read as EntryNormal
		}
		if len(e.ProtoReflect().GetUnknown()) > 0 {
			unknown++
		}
		saved = append(saved, e)
		want = append(want, proto.Clone(e).(*raftpb.Entry))
	}
	for lo := 0; lo < n; lo += 100 {
		if err := s.Save(&raftpb.HardState{Term: new(uint64(1))}, saved[lo:lo+100], &raftpb.Snapshot{}); err != nil {
			t.Fatal(err)
		}
	}
	if unknown == 0 || l.SegmentCount() < 3 {
		t.Fatalf("%d entries with unknown fields, %d segments", unknown, l.SegmentCount())
	}
	check := func(when string) {
		t.Helper()
		got, err := s.Entries(1, n+1, math.MaxUint64)
		if err != nil {
			t.Fatalf("%s: %v", when, err)
		}
		for i := range want {
			if !proto.Equal(got[i], want[i]) {
				t.Fatalf("%s: entry %d read back as %v, saved %v", when, i+1, got[i], want[i])
			}
			if entrySize(got[i]) != proto.Size(want[i]) {
				t.Fatalf("%s: entry %d: size %d, saved %d", when, i+1, entrySize(got[i]), proto.Size(want[i]))
			}
			if term, err := s.Term(uint64(i + 1)); err != nil || term != 1 {
				t.Fatalf("%s: Term(%d) = %d, %v", when, i+1, term, err)
			}
		}
		rep, err := l.Scrub()
		if err != nil || len(rep.Problems) > 0 {
			t.Fatalf("%s: scrub: %v %v", when, err, rep.Problems)
		}
	}
	check("cached")
	reopen := func() {
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		l = open(t, dir, Options{SegmentSize: 64 << 10, CacheEntries: -1})
		s = l.Group(1)
	}
	reopen()
	check("after reopening")
	idx, err := filepath.Glob(filepath.Join(dir, "segments", "*.idx"))
	if err != nil || len(idx) == 0 {
		t.Fatalf("index files: %v %v", idx, err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	for _, f := range idx {
		if err := os.Remove(f); err != nil {
			t.Fatal(err)
		}
	}
	l = open(t, dir, Options{SegmentSize: 64 << 10, CacheEntries: -1})
	s = l.Group(1)
	check("after the index files were rebuilt")
}

// A nil entry is an invalid update: Save refuses it and changes nothing,
// in the caller's goroutine (the writer must never see it).
func TestNilEntryRefused(t *testing.T) {
	l := open(t, t.TempDir(), Options{})
	defer l.Close()
	s := l.Group(1)
	if err := s.Save(&raftpb.HardState{Term: new(uint64(1))}, ents(1, 1, 2, 10), nil); err != nil {
		t.Fatal(err)
	}
	for _, save := range []func() error{
		func() error { return s.Save(nil, []*raftpb.Entry{nil}, nil) },
		func() error {
			return s.Save(&raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(3))}, append(ents(1, 3, 3, 10), nil), nil)
		},
		func() error { return l.Save(Update{Group: 2, Entries: []*raftpb.Entry{nil}}) },
	} {
		if err := save(); !errors.Is(err, ErrInvalidUpdate) {
			t.Fatalf("Save with a nil entry: %v", err)
		}
	}
	if last, _ := s.LastIndex(); last != 2 {
		t.Fatalf("last index %d after refused Saves", last)
	}
	if err := s.Save(nil, ents(1, 3, 3, 10), nil); err != nil {
		t.Fatalf("Save after refused ones: %v", err)
	}
}

// The copies seglog keeps and hands out are its own: changing a message
// after passing it to seglog, or one seglog returned, changes nothing in
// seglog.
func TestStorageCopiesMessages(t *testing.T) {
	l := open(t, t.TempDir(), Options{})
	defer l.Close()
	s := l.Group(1)
	hs := &raftpb.HardState{Term: new(uint64(2)), Vote: new(uint64(1)), Commit: new(uint64(3))}
	if err := s.Save(hs, ents(2, 1, 3, 10), &raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	hs.Commit = new(uint64(99)) // the caller's, after Save
	got, _, _ := s.InitialState()
	if got.GetCommit() != 3 {
		t.Fatalf("Save kept the caller's hard state: commit %d", got.GetCommit())
	}
	got.Term = new(uint64(77)) // a returned copy
	if again, _, _ := s.InitialState(); again.GetTerm() != 2 {
		t.Fatalf("InitialState handed out seglog's own hard state: term %d", again.GetTerm())
	}

	cs := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	if err := s.SetConfState(cs); err != nil {
		t.Fatal(err)
	}
	cs.Voters[0] = 9
	cs.AutoLeave = new(true)
	if _, got, _ := s.InitialState(); !sameCS(got, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}) {
		t.Fatalf("SetConfState kept the caller's configuration: %v", got)
	}

	cs2 := &raftpb.ConfState{Voters: []uint64{1, 2}}
	snap, err := s.CreateSnapshot(2, cs2, []byte("state"))
	if err != nil {
		t.Fatal(err)
	}
	cs2.Voters[1] = 8
	snap.Metadata.ConfState.Voters[0] = 7
	snap.Metadata.Index = new(uint64(55))
	stored, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if stored.GetMetadata().GetIndex() != 2 || !sameCS(stored.GetMetadata().GetConfState(), &raftpb.ConfState{Voters: []uint64{1, 2}}) {
		t.Fatalf("snapshot changed through a message seglog was given or returned: %v", stored.GetMetadata())
	}
	stored.Metadata.ConfState.Voters[0] = 6
	if again, _ := s.Snapshot(); again.GetMetadata().GetConfState().GetVoters()[0] != 1 {
		t.Fatal("Snapshot handed out seglog's own metadata")
	}
	if _, got, _ := s.InitialState(); !sameCS(got, &raftpb.ConfState{Voters: []uint64{1, 2}}) {
		t.Fatalf("configuration after CreateSnapshot: %v", got)
	}
}
