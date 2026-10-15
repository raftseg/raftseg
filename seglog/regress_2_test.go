// Copyright 2026 The raftseg authors.

package seglog

import (
	"errors"
	randv2 "math/rand/v2"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
)

// Crash during the very first Open, after segment 1 is created but before the
// index batch carrying the format key is committed.
func TestRegressFirstOpenCrashState(t *testing.T) {
	fs := crashfs.New()
	fs.MkdirAll(crashDir+"/segments", 0o755)
	f, _ := fs.Create(crashDir + "/segments/" + segName(1))
	f.Close()
	l, err := Open(crashDir, memOpts(fs, Options{}))
	if err != nil {
		t.Fatalf("Open of a legitimately crashed first-open state: %v", err)
	}
	l.Close()
}

// Snapshot is synced before the HardState that commits it; a crash in
// between leaves HardState.Commit < FirstIndex-1 and raft panics on restart.
func TestRegressSnapshotWithoutHardState(t *testing.T) {
	mem := crashfs.New()
	var armed atomic.Bool
	fs := crashfs.WithHook(mem, func(op crashfs.Op) error {
		if armed.Load() && op.Kind == crashfs.OpSyncData && strings.Contains(op.Path, "/segments/") {
			return crashfs.ErrInjected
		}
		return nil
	})
	l, err := Open(crashDir, memOpts(fs, Options{}))
	if err != nil {
		t.Fatal(err)
	}
	s := l.Group(1)
	if err := s.Save(&raftpb.HardState{Term: new(uint64(1)), Vote: new(uint64(1)), Commit: new(uint64(5))}, ents(1, 1, 5, 10), &raftpb.Snapshot{}); err != nil {
		t.Fatal(err)
	}
	armed.Store(true) // power fails before the segment fsync of this Ready
	snap := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(100)), Term: new(uint64(3)), ConfState: &raftpb.ConfState{Voters: []uint64{1}}}}
	err = s.Save(&raftpb.HardState{Term: new(uint64(3)), Commit: new(uint64(100))}, nil, snap)
	t.Logf("save: %v", err)
	clone := mem.CrashClone(0, randv2.New(randv2.NewPCG(0, 0)))
	l.crash()
	l2, err := Open(crashDir, memOpts(clone, Options{}))
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	s2 := l2.Group(1)
	fi, _ := s2.FirstIndex()
	hs, _, _ := s2.InitialState()
	t.Logf("after crash: first %d hs %+v", fi, hs)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("raft panics on restart: %v", r)
		}
	}()
	if _, err := raft.NewRawNode(&raft.Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1, Storage: s2, MaxInflightMsgs: 16}); err != nil {
		t.Fatal(err)
	}
}

// Corruption of an acknowledged commit in the last block of the newest
// segment, with later acknowledged commits in the same block, is treated as
// a torn tail and silently discarded.
func TestRegressClassifySameBlock(t *testing.T) {
	data, spans := buildCommits(5, 1, 100)
	d := append([]byte(nil), data...)
	d[spans[1][0]+20] ^= 0xff
	if !classifyAt(t, d, spans[1][0]) {
		t.Fatalf("corruption in commit 2 of 5 (all in one block) classified as torn tail")
	}
}

func TestRegressSilentLossOnOpen(t *testing.T) {
	fs := crashfs.New()
	l, err := Open(crashDir, memOpts(fs, Options{testDropIndexAfter: 1}))
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		if err := l.Save(Update{Group: 1, Entries: ents(1, i, i, 50), HardState: &raftpb.HardState{Term: new(uint64(1)), Commit: new(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	l.crash()
	// Flip one byte inside entry 2's record (acknowledged, fsynced).
	p := crashDir + "/segments/" + segName(1)
	f, _ := fs.OpenReadWrite(p)
	sc, _ := newSegScanner(f, l.key)
	var pos int64
	var target int64 = -1
	for n := 0; ; n++ {
		rec, err := sc.next(pos)
		if err != nil {
			break
		}
		typ, _, payload, _ := decodeRecord(rec.data)
		if typ == recEntry {
			var e = &raftpb.Entry{}
			proto.Unmarshal(payload, e)
			if e.GetIndex() == 2 {
				target = rec.start + 30
			}
		}
		pos = rec.end
	}
	b := make([]byte, 1)
	f.ReadAt(b, target)
	b[0] ^= 0xff
	f.WriteAt(b, target)
	f.Close()
	l2, err := Open(crashDir, memOpts(fs, Options{}))
	if errors.Is(err, ErrCorrupt) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	li, _ := l2.Group(1).LastIndex()
	st, _ := l2.Stats()
	t.Fatalf("Open succeeded after corrupting acknowledged entry 2: LastIndex %d (acked 5), tail %+v", li, st.Recovery.Tail)
}
