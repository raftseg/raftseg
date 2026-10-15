// Copyright 2026 The raftseg authors.

package seglog

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strconv"
	"strings"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
)

// A snapshot is a segment record holding its metadata and, if its data is
// at most SnapshotInlineBytes, the data too (kept in memory and in
// checkpoints). Larger data lives in snapshots/<group>-<index>.snap, written
// to a temporary file, synced, renamed and dir-synced before the record that
// refers to it (with its size and CRC-32C) is written. A replaced file is
// deleted only once the record replacing it is durable. Files nothing refers
// to are removed at Open.

func snapName(group, index uint64) string {
	return fmt.Sprintf("%020d-%020d.snap", group, index)
}

func parseSnapName(name string) (group, index uint64, ok bool) {
	base, found := strings.CutSuffix(name, ".snap")
	if !found {
		return 0, 0, false
	}
	a, b, found := strings.Cut(base, "-")
	if !found {
		return 0, 0, false
	}
	g, err1 := strconv.ParseUint(a, 10, 64)
	i, err2 := strconv.ParseUint(b, 10, 64)
	return g, i, err1 == nil && err2 == nil
}

func (l *Log) snapPath(group, index uint64) string {
	return l.fs.PathJoin(l.snapDir, snapName(group, index))
}

func (l *Log) writeSnapFile(group, index uint64, data []byte) (snapRef, error) {
	final := l.snapPath(group, index)
	tmp := final + ".tmp"
	ref := snapRef{index: index, size: uint64(len(data)), crc: crc32.Checksum(data, crcTable)}
	f, err := l.fs.Create(tmp)
	if err != nil {
		return snapRef{}, err
	}
	// Write through a private buffer: the data belongs to the caller (raft
	// may still send it), and a File may scribble on what it is given (some
	// test filesystems do, to catch exactly this).
	scratch := make([]byte, min(len(data), 1<<20))
	for off := 0; off < len(data) && err == nil; {
		n := copy(scratch, data[off:])
		_, err = f.WriteAt(scratch[:n], int64(off))
		off += n
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = l.fs.Rename(tmp, final)
	}
	if err == nil {
		err = syncDir(l.fs, l.snapDir)
	}
	if err != nil {
		l.fs.Remove(tmp)
		return snapRef{}, err
	}
	return ref, nil
}

func (l *Log) readSnapFile(group uint64, ref snapRef) ([]byte, error) {
	f, err := l.fs.Open(l.snapPath(group, ref.index))
	if err != nil {
		return nil, fmt.Errorf("%w: snapshot %d of group %d: %v", ErrCorrupt, ref.index, group, err)
	}
	defer f.Close()
	// Check the size before allocating: the reference is only as
	// trustworthy as the file it was read from.
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if uint64(st.Size()) != ref.size {
		return nil, fmt.Errorf("%w: snapshot %d of group %d: %d bytes, expected %d", ErrCorrupt, ref.index, group, st.Size(), ref.size)
	}
	data := make([]byte, ref.size)
	if _, err := f.ReadAt(data, 0); err != nil && !(errors.Is(err, io.EOF) && ref.size == 0) {
		return nil, fmt.Errorf("%w: snapshot %d of group %d: %v", ErrCorrupt, ref.index, group, err)
	}
	if crc32.Checksum(data, crcTable) != ref.crc {
		return nil, fmt.Errorf("%w: snapshot %d of group %d: checksum mismatch", ErrCorrupt, ref.index, group)
	}
	return data, nil
}

// newSnapInfo prepares a snapshot for storing, writing its data to a file
// if it is large.
func (l *Log) newSnapInfo(id uint64, snap *raftpb.Snapshot) (*snapInfo, error) {
	si := &snapInfo{meta: copySnapshotMetadata(snap.GetMetadata())}
	if len(snap.Data) > l.opts.SnapshotInlineBytes {
		ref, err := l.writeSnapFile(id, si.meta.GetIndex(), snap.Data)
		if err != nil {
			return nil, err
		}
		si.ref = &ref
	} else {
		si.data = append([]byte(nil), snap.Data...)
	}
	return si, nil
}

// dropSnapFile deletes a replaced snapshot file. It is called once the
// record replacing it is durable; leftovers are removed at Open.
func (l *Log) dropSnapFile(id, index uint64) {
	_ = l.fs.Remove(l.snapPath(id, index))
}

// stageSnapshots stages every snapshot of a request into j, which holds
// nothing yet, each with its group's HardState (written by stageRequest in
// the same commit), so a crash can never leave a snapshot without the
// HardState that commits it. The request is validated first; on error
// nothing is staged.
func (l *Log) stageSnapshots(j *job, r *request) error {
	type item struct {
		i int
		g *group
	}
	var items []item
	seen := map[uint64]bool{}
	for i, u := range r.updates {
		if raft.IsEmptySnap(u.Snapshot) {
			continue
		}
		if seen[u.Group] {
			return fmt.Errorf("%w: group %d: two snapshots in one Save", ErrInvalidUpdate, u.Group)
		}
		seen[u.Group] = true
		g := l.groupOrCreate(u.Group)
		if u.Snapshot.GetMetadata().GetIndex() <= g.wsnap.index() {
			return raft.ErrSnapOutOfDate
		}
		items = append(items, item{i, g})
	}
	infos := make([]*snapInfo, len(items))
	for k, it := range items {
		u := r.updates[it.i]
		si, err := l.newSnapInfo(u.Group, u.Snapshot)
		if err != nil {
			return err
		}
		infos[k] = si
	}
	for k, it := range items {
		u := r.updates[it.i]
		si := infos[k]
		idx := si.meta.GetIndex()
		rec, err := encodeSnapRecord(snapApplied, si)
		if err != nil {
			return err
		}
		// raft commits the snapshot in the same Ready; make sure the stored
		// HardState never says less, or raft refuses to restart.
		hs := r.enc[it.i].hsVal
		if raft.IsEmptyHardState(hs) {
			hs = it.g.whs
		}
		if hs.GetCommit() < idx {
			hs = withCommit(hs, idx) // a new one: hs may be the group's
		}
		if hs != r.enc[it.i].hsVal {
			if err := r.enc[it.i].setHardState(u.Group, hs); err != nil {
				return err
			}
		}
		l.writeRecord(j.st, recSnapshot, u.Group, rec)
		d := j.st.delta(l, u.Group)
		d.meta = groupMeta{first: idx + 1, last: idx, compactTerm: si.meta.GetTerm()}
		d.reset, d.snap = true, si
		d.cs = si.meta.GetConfState()
		l.runsClear(it.g)
		it.g.terms = nil
		if old := it.g.wsnap; old != nil && old.ref != nil && old.meta.GetIndex() != idx {
			oldIdx := old.meta.GetIndex()
			j.after = append(j.after, func() { l.dropSnapFile(u.Group, oldIdx) })
		}
		it.g.wsnap = si
	}
	return nil
}

// CreateSnapshot records a snapshot of the state machine at applied index i.
// Like raft.MemoryStorage, a non-nil cs also becomes the ConfState returned
// by InitialState, and the log is not compacted (call Compact). The
// snapshot is visible at once and durable with the next group commit; a
// crash before that keeps the previous snapshot.
func (s *Storage) CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) (*raftpb.Snapshot, error) {
	snap := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{}}
	err := s.writeOp(func(g *group) error {
		m := g.wmeta
		if i <= g.wsnap.index() {
			return raft.ErrSnapOutOfDate
		}
		if i > m.last {
			return fmt.Errorf("seglog: group %d: snapshot %d is out of bound lastindex(%d)", s.id, i, m.last)
		}
		term, err := s.l.termLocked(s.id, g, i)
		if err != nil {
			return err
		}
		s.l.mu.RLock()
		curCS := g.cs
		s.l.mu.RUnlock()
		snap.Metadata.Index, snap.Metadata.Term = new(i), new(term)
		var flags byte
		if cs != nil {
			snap.Metadata.ConfState = copyConfState(cs)
			flags = snapSetCS
		} else {
			snap.Metadata.ConfState = copyConfState(curCS)
		}
		snap.Data = data
		si, err := s.l.newSnapInfo(s.id, snap)
		if err != nil {
			return err
		}
		rec, err := encodeSnapRecord(flags, si)
		if err != nil {
			return err
		}
		j := &job{st: newStage()}
		s.l.writeRecord(j.st, recSnapshot, s.id, rec)
		if old := g.wsnap; old != nil && old.ref != nil && old.meta.GetIndex() != i {
			oldIdx := old.meta.GetIndex()
			j.after = append(j.after, func() { s.l.dropSnapFile(s.id, oldIdx) })
		}
		// Update the state before dispatching: a segment roll during dispatch
		// writes a checkpoint, which must include this snapshot.
		g.wsnap = si
		s.l.mu.Lock()
		g.snap = si
		if cs != nil {
			g.cs = si.meta.GetConfState()
		}
		s.l.mu.Unlock()
		s.l.dispatch(j)
		if len(j.after) > 0 {
			// Wait until the old file can go, so files do not pile up.
			s.l.drain()
		}
		return s.l.failure()
	})
	return snap, err
}
