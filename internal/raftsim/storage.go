// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raftsim

import (
	"errors"
	"math"
	"math/rand"

	"google.golang.org/protobuf/proto"

	"github.com/raftseg/raft"
	pb "github.com/raftseg/raft/raftpb"
)

// Storage is a node's raft log storage as the simulator drives it. Raft
// reads it through raft.Storage; the simulator writes it on raft's behalf.
type Storage interface {
	raft.Storage
	// Save writes, in this order, a snapshot (unless empty), entries, and
	// a hard state (unless empty). The writes need not survive a crash
	// until Sync returns.
	Save(snap *pb.Snapshot, ents []*pb.Entry, hs *pb.HardState) error
	// Sync makes every earlier write durable.
	Sync() error
	// CreateSnapshot records the application's state as of index i, then
	// Compact discards entries up to i. Both are durable when they return.
	CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) error
	Compact(i uint64) error
}

// Backend creates node storage and simulates power loss.
type Backend interface {
	Name() string
	// Open returns empty storage for a new node.
	Open(id uint64) (Storage, error)
	// Crash simulates power loss under s and returns the storage as it is
	// found on restart. Synced writes must survive; which unsynced writes
	// survive is chosen with r.
	//
	// A crash can keep a snapshot and lose the hard state saved with it.
	// Raft refuses to start from a commit index below the snapshot index,
	// so on restart InitialState must report a commit index of at least
	// the snapshot index; a snapshot only ever holds committed state.
	Crash(id uint64, s Storage, r *rand.Rand) (Storage, error)
}

// MemBackend keeps each node's storage in a raft.MemoryStorage and models
// durability with a second MemoryStorage that receives writes only when
// they are synced. A crash keeps a random prefix of the unsynced writes, as
// a log-structured store would.
type MemBackend struct {
	// LyingSync makes Sync a no-op, so that a crash can lose acknowledged
	// writes. Used to check that the simulator notices.
	LyingSync bool
	// Scribble makes reads change, in place, an entry handed out by an
	// earlier read, as a storage engine that reuses its buffers would. Used
	// to check that the simulator notices (frozen.go).
	Scribble bool
}

func (b *MemBackend) Name() string {
	switch {
	case b.LyingSync:
		return "mem-lying"
	case b.Scribble:
		return "mem-scribble"
	}
	return "mem"
}

func (b *MemBackend) Open(uint64) (Storage, error) {
	return &memStorage{MemoryStorage: raft.NewMemoryStorage(), durable: raft.NewMemoryStorage(), lying: b.LyingSync, scribble: b.Scribble}, nil
}

func (b *MemBackend) Crash(_ uint64, s Storage, r *rand.Rand) (Storage, error) {
	ms := s.(*memStorage)
	keep := r.Intn(len(ms.pending) + 1)
	for _, op := range ms.pending[:keep] {
		if err := op(ms.durable); err != nil {
			return nil, err
		}
	}
	hs, _, _ := ms.durable.InitialState()
	snap, _ := ms.durable.Snapshot()
	if hs.GetCommit() < snap.GetMetadata().GetIndex() {
		// A new hard state: the stored one may be shared.
		hs = &pb.HardState{Term: new(hs.GetTerm()), Vote: new(hs.GetVote()), Commit: new(snap.GetMetadata().GetIndex())}
		if err := ms.durable.SetHardState(hs); err != nil {
			return nil, err
		}
	}
	live, err := cloneMemory(ms.durable)
	if err != nil {
		return nil, err
	}
	return &memStorage{MemoryStorage: live, durable: ms.durable, lying: b.LyingSync, scribble: b.Scribble}, nil
}

type memStorage struct {
	*raft.MemoryStorage                     // everything written: what raft reads
	durable             *raft.MemoryStorage // everything synced
	pending             []func(*raft.MemoryStorage) error
	lying               bool
	scribble            bool
	lastRead            []*pb.Entry // for scribble
}

func (s *memStorage) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	if s.scribble {
		for _, e := range s.lastRead {
			// An application entry, so that only the check sees it.
			if e.GetType() == pb.EntryNormal && len(e.GetData()) > 0 {
				d := append([]byte(nil), e.GetData()...)
				d[len(d)-1] ^= 1
				e.Data = d
				break
			}
		}
	}
	ents, err := s.MemoryStorage.Entries(lo, hi, maxSize)
	if s.scribble {
		s.lastRead = ents
	}
	return ents, err
}

func (s *memStorage) do(op func(*raft.MemoryStorage) error) error {
	if err := op(s.MemoryStorage); err != nil {
		return err
	}
	s.pending = append(s.pending, op)
	return nil
}

func (s *memStorage) Save(snap *pb.Snapshot, ents []*pb.Entry, hs *pb.HardState) error {
	if !raft.IsEmptySnap(snap) {
		if err := s.do(func(ms *raft.MemoryStorage) error { return ms.ApplySnapshot(snap) }); err != nil {
			return err
		}
	}
	if len(ents) > 0 {
		ents = append([]*pb.Entry(nil), ents...)
		if err := s.do(func(ms *raft.MemoryStorage) error { return ms.Append(ents) }); err != nil {
			return err
		}
	}
	if !raft.IsEmptyHardState(hs) {
		if err := s.do(func(ms *raft.MemoryStorage) error { return ms.SetHardState(hs) }); err != nil {
			return err
		}
	}
	return nil
}

func (s *memStorage) Sync() error {
	if s.lying {
		return nil
	}
	for _, op := range s.pending {
		if err := op(s.durable); err != nil {
			return err
		}
	}
	s.pending = s.pending[:0]
	return nil
}

func (s *memStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) error {
	c := proto.Clone(cs).(*pb.ConfState)
	err := s.do(func(ms *raft.MemoryStorage) error {
		_, err := ms.CreateSnapshot(i, c, data)
		return err
	})
	if err != nil {
		return err
	}
	return s.Sync()
}

func (s *memStorage) Compact(i uint64) error {
	if err := s.do(func(ms *raft.MemoryStorage) error { return ms.Compact(i) }); err != nil {
		return err
	}
	return s.Sync()
}

// cloneMemory copies a MemoryStorage. Entries below the snapshot index are
// not kept: a MemoryStorage cannot hold entries that precede its snapshot.
func cloneMemory(src *raft.MemoryStorage) (*raft.MemoryStorage, error) {
	dst := raft.NewMemoryStorage()
	snap, err := src.Snapshot()
	if err != nil {
		return nil, err
	}
	if !raft.IsEmptySnap(snap) {
		if err := dst.ApplySnapshot(snap); err != nil {
			return nil, err
		}
	}
	first, _ := src.FirstIndex()
	last, _ := src.LastIndex()
	if last >= first {
		ents, err := src.Entries(first, last+1, math.MaxUint64)
		if err != nil {
			return nil, err
		}
		for len(ents) > 0 && ents[0].GetIndex() <= snap.GetMetadata().GetIndex() {
			ents = ents[1:]
		}
		if err := dst.Append(ents); err != nil {
			return nil, err
		}
	}
	hs, _, err := src.InitialState()
	if err != nil {
		return nil, err
	}
	if err := dst.SetHardState(hs); err != nil {
		return nil, err
	}
	return dst, nil
}

// ignorable reports storage errors that are expected when the simulator
// snapshots or compacts opportunistically.
func ignorable(err error) bool {
	return errors.Is(err, raft.ErrCompacted) || errors.Is(err, raft.ErrSnapOutOfDate)
}
