// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raftsim

import (
	"errors"
	"fmt"
	"math/rand"
	randv2 "math/rand/v2"
	"sync"

	"github.com/raftseg/raft"
	pb "github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
	"github.com/raftseg/raftseg/seglog"
)

// SegBackend stores each node's raft log in a seglog.Log on its own
// crashfs filesystem, so that crashes have the on-disk semantics of a real
// filesystem: synced data survives, and a random part of what was not
// synced survives too, torn at sector granularity and reordered.
//
// seglog persists every Save before it returns, so Sync has nothing left to
// do. A compaction is made durable by the log's next checkpoint, so a crash
// may undo it; the node then keeps more entries, which raft tolerates.
type SegBackend struct {
	// Groups is how many raft groups share each node's log. The simulated
	// group is one of them; the others carry decoy traffic, so that the
	// node's log interleaves records of many groups, as in production.
	Groups int
	// DecoyBytes is the size of each decoy entry (default 64): larger ones
	// fill segments faster, so that they roll, checkpoint and are cleaned.
	DecoyBytes int
	// Options for each log. FS is set by the backend.
	Options seglog.Options
	// Observe, if set, receives each log's statistics before it crashes or
	// closes.
	Observe func(seglog.Stats)
}

const segDir = "/node"

func (b *SegBackend) Name() string { return fmt.Sprintf("seglog(%d groups)", max(1, b.Groups)) }

// segStorage is one node's storage: the simulated group in a seglog.Log.
//
// raft does not rely on a Save until Sync returns, so a crash may also
// strike in the middle of any Save since the last Sync. The node's
// filesystem is hooked: at a random one of the operations those Saves make,
// a crash image is taken, and Crash uses it half of the time. Recovery from
// it sees a torn final commit.
type segStorage struct {
	*seglog.Storage
	fs    *crashfs.FS
	log   *seglog.Log
	decoy uint64 // next decoy entry index
	b     *SegBackend

	mu        sync.Mutex
	rng       *rand.Rand
	armed     bool        // a Save since the last Sync
	ops       int         // filesystem operations since then
	candidate *crashfs.FS // a crash image taken at one of them
}

// simGroup is the id of the simulated group within the node's log.
const simGroup = 1

func (b *SegBackend) open(fs *crashfs.FS, seed int64) (*segStorage, error) {
	s := &segStorage{fs: fs, b: b, rng: rand.New(rand.NewSource(seed))}
	o := b.Options
	o.FS = crashfs.WithHook(fs, s.hook)
	l, err := seglog.Open(segDir, o)
	if err != nil {
		return nil, err
	}
	s.Storage, s.log = l.Group(simGroup), l
	if b.Groups > 1 {
		last, err := l.Group(simGroup + 1).LastIndex()
		if err != nil {
			l.Close()
			return nil, err
		}
		s.decoy = last + 1
	}
	return s, nil
}

// hook may take a crash image before a filesystem operation of an unsynced
// Save: each operation since the last Sync is equally likely to be the one.
func (s *segStorage) hook(op crashfs.Op) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.armed || !op.Kind.IsWrite() {
		return nil
	}
	s.ops++
	if s.rng.Intn(s.ops) == 0 {
		s.candidate = s.fs.CrashClone(s.rng.Intn(101), randv2.New(randv2.NewPCG(s.rng.Uint64(), s.rng.Uint64())))
	}
	return nil
}

func (s *segStorage) arm(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !on {
		s.ops, s.candidate = 0, nil
	}
	s.armed = on
}

func (b *SegBackend) Open(id uint64) (Storage, error) { return b.open(crashfs.New(), int64(id)) }

func (b *SegBackend) Crash(_ uint64, s Storage, r *rand.Rand) (Storage, error) {
	ss := s.(*segStorage)
	// The power fails now, or failed during an unsynced Save; the process
	// is gone after. (Closing the log first would checkpoint and sync it: a
	// clean shutdown, not a crash.)
	ss.mu.Lock()
	img := ss.candidate
	ss.armed = false
	ss.mu.Unlock()
	if img == nil || r.Intn(2) == 0 {
		img = ss.fs.CrashClone(r.Intn(101), randv2.New(randv2.NewPCG(r.Uint64(), r.Uint64())))
	}
	ss.observe()
	ss.log.Close()
	return b.open(img, r.Int63())
}

func (s *segStorage) Close() error {
	s.observe()
	return s.log.Close()
}

func (s *segStorage) observe() {
	if s.b.Observe != nil {
		if st, err := s.log.Stats(); err == nil {
			s.b.Observe(st)
		}
	}
}

func (s *segStorage) Save(snap pb.Snapshot, ents []pb.Entry, hs pb.HardState) error {
	ups := []seglog.Update{{Group: simGroup, HardState: hs, Entries: ents, Snapshot: snap}}
	// Decoy groups write in the same group commit.
	for g := uint64(2); g <= uint64(s.b.Groups); g++ {
		if len(ents) == 0 {
			break
		}
		e := pb.Entry{Index: s.decoy, Term: 1, Data: make([]byte, max(s.b.DecoyBytes, 64))}
		ups = append(ups, seglog.Update{Group: simGroup + g - 1, Entries: []pb.Entry{e}, HardState: pb.HardState{Term: 1, Commit: s.decoy}})
	}
	if len(ups) > 1 {
		s.decoy++
	}
	s.arm(true)
	return s.log.Save(ups...)
}

func (s *segStorage) Sync() error {
	err := s.log.Sync()
	s.arm(false)
	return err
}

func (s *segStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) error {
	if _, err := s.Storage.CreateSnapshot(i, cs, data); err != nil {
		return err
	}
	return s.Sync()
}

func (s *segStorage) Compact(i uint64) error {
	if err := s.Storage.Compact(i); err != nil {
		return err
	}
	// Keep the decoys short too, so that GC has whole segments to free.
	for g := uint64(2); g <= uint64(s.b.Groups); g++ {
		d := s.log.Group(simGroup + g - 1)
		if last, _ := d.LastIndex(); last > 64 {
			if err := d.Compact(last - 32); err != nil && !errors.Is(err, raft.ErrCompacted) {
				return err
			}
		}
	}
	return nil
}

// MixedBackend gives each node a backend chosen by its id, so that one
// cluster mixes storage engines.
type MixedBackend struct {
	Backends []Backend
}

func (b *MixedBackend) Name() string {
	name := "mixed("
	for i, x := range b.Backends {
		if i > 0 {
			name += ","
		}
		name += x.Name()
	}
	return name + ")"
}

func (b *MixedBackend) of(id uint64) Backend { return b.Backends[int(id)%len(b.Backends)] }

func (b *MixedBackend) Open(id uint64) (Storage, error) { return b.of(id).Open(id) }

func (b *MixedBackend) Crash(id uint64, s Storage, r *rand.Rand) (Storage, error) {
	return b.of(id).Crash(id, s, r)
}
