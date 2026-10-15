// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package seglog

// raft's messages are protobuf messages held by pointer, with a pointer per
// field (since raft's switch to google.golang.org/protobuf). seglog follows
// two rules with them:
//
//   - It keeps copies: hard states, configurations and snapshot metadata
//     it is given are copied before it keeps them, and what it hands out is
//     a copy too, so that no caller can change seglog's state, or seglog a
//     caller's message.
//   - Entries are the exception, as in raft.MemoryStorage. They are encoded
//     when saved, but the entry cache keeps the entries Save was given, and
//     hands them out as they are; entries not in the cache are decoded
//     afresh. So an entry must not be changed once saved, nor one that
//     Entries returned. raft never does (raft's FORK.md, "Messages and
//     entries are shared"), and the cluster simulator checks it
//     (internal/raftsim/frozen.go).
//   - It never changes a message it keeps; it replaces it. So the copies it
//     keeps can be shared between its own structures.
//
// Messages are stored with protobuf's encoding, so a message read back is
// the one saved, unset fields included: the same as raft's MemoryStorage.

import (
	"google.golang.org/protobuf/proto"

	"github.com/raftseg/raft/raftpb"
)

// copyHardState returns a copy of hs, or an empty hard state if hs is nil.
func copyHardState(hs *raftpb.HardState) *raftpb.HardState {
	if hs == nil {
		return &raftpb.HardState{}
	}
	return proto.Clone(hs).(*raftpb.HardState)
}

// withCommit returns a copy of hs with commit index c.
func withCommit(hs *raftpb.HardState, c uint64) *raftpb.HardState {
	out := copyHardState(hs)
	out.Commit = new(c)
	return out
}

// copyConfState returns a copy of cs, or an empty configuration if cs is
// nil.
func copyConfState(cs *raftpb.ConfState) *raftpb.ConfState {
	if cs == nil {
		return &raftpb.ConfState{}
	}
	return proto.Clone(cs).(*raftpb.ConfState)
}

// copySnapshotMetadata returns a copy of md (empty if nil) with its index,
// term and configuration set, as raft.MemoryStorage keeps a snapshot's
// metadata (raftpb.EnsureSnapshotMetadata): a snapshot without a
// configuration has the empty one.
func copySnapshotMetadata(md *raftpb.SnapshotMetadata) *raftpb.SnapshotMetadata {
	if md == nil {
		return raftpb.EnsureSnapshotMetadata(nil)
	}
	return raftpb.EnsureSnapshotMetadata(proto.Clone(md).(*raftpb.SnapshotMetadata))
}

// entrySize is the size seglog accounts an entry for: its encoding, as
// raft measures entries.
func entrySize(e *raftpb.Entry) int { return proto.Size(e) }
