// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raftsim

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"math"

	"github.com/raftseg/raft"
	pb "github.com/raftseg/raft/raftpb"
)

// checker holds what the cluster has done so far and fails the run as soon
// as a property is violated:
//
//   - election safety: at most one leader per term;
//   - vote safety: a node grants at most one vote per term, even across
//     crashes, and restarts with every vote it granted;
//   - state machine safety: every node applies the same entry at each
//     index, in order and without gaps, and reaches the same state;
//   - snapshot integrity: a snapshot carries the state and configuration
//     of its index;
//   - configuration safety: every node sees the same configuration at
//     each index;
//   - log matching: two logs holding an entry with the same index and term
//     are identical up to it;
//   - linearizable reads: a read index covers every entry applied anywhere
//     before the read was requested;
//   - at-most-once proposals when the network does not duplicate;
//   - liveness: after faults stop, the cluster commits and applies a new
//     entry on every member (see Cluster.heal).
type checker struct {
	c          *Cluster
	leaders    map[uint64]uint64        // term -> leader
	entries    map[uint64]entryRec      // index -> first applied entry
	digests    map[uint64]uint64        // index -> state digest after it
	configs    map[uint64]*pb.ConfState // index -> configuration after it
	votes      map[[2]uint64]uint64     // (voter, term) -> candidate
	removed    map[uint64]uint64        // removed node ID -> index of its removal
	reads      map[string]uint64        // request context -> lower bound
	proposals  map[string]uint64        // proposal -> index
	maxApplied uint64
	probeIndex uint64
}

type entryRec struct {
	node uint64
	term uint64
	typ  pb.EntryType
	hash uint64
}

func newChecker(c *Cluster) *checker {
	return &checker{
		c:         c,
		leaders:   map[uint64]uint64{},
		entries:   map[uint64]entryRec{},
		digests:   map[uint64]uint64{},
		configs:   map[uint64]*pb.ConfState{},
		votes:     map[[2]uint64]uint64{},
		removed:   map[uint64]uint64{},
		reads:     map[string]uint64{},
		proposals: map[string]uint64{},
	}
}

func hashBytes(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

// observe checks election safety from n's current state.
func (k *checker) observe(n *node) {
	if !n.up {
		return
	}
	st := n.rn.BasicStatus()
	if st.RaftState != raft.StateLeader {
		return
	}
	if l, ok := k.leaders[st.GetTerm()]; ok && l != n.id {
		k.c.fail("election safety: nodes %d and %d both leader in term %d", l, n.id, st.GetTerm())
	}
	if _, ok := k.leaders[st.GetTerm()]; !ok {
		k.leaders[st.GetTerm()] = n.id
		k.c.tracef("node %d leader in term %d", n.id, st.GetTerm())
		k.c.mixFP(4, n.id, st.GetTerm())
	}
}

func (k *checker) onReady(n *node, rd raft.Ready) {
	for _, e := range rd.CommittedEntries {
		if e.GetIndex() > rd.HardState.GetCommit() && rd.HardState.GetCommit() != 0 {
			k.c.fail("node %d: Ready commits entry %d beyond commit index %d", n.id, e.GetIndex(), rd.HardState.GetCommit())
		}
	}
}

// onSend records granted votes. A vote response is sent only once the vote
// is durable, so a node must never grant two votes in one term.
func (k *checker) onSend(n *node, m *pb.Message) {
	if m.GetType() != pb.MsgVoteResp || m.GetReject() {
		return
	}
	key := [2]uint64{m.GetFrom(), m.GetTerm()}
	if prev, ok := k.votes[key]; ok && prev != m.GetTo() {
		k.c.fail("vote safety: node %d voted for %d and %d in term %d", m.GetFrom(), prev, m.GetTo(), m.GetTerm())
	}
	k.votes[key] = m.GetTo()
}

// onStart checks that n restarted with every vote it granted.
func (k *checker) onStart(n *node) {
	hs, _, err := n.st.InitialState()
	if err != nil {
		k.c.fail("node %d: initial state: %v", n.id, err)
	}
	for key, cand := range k.votes {
		if key[0] != n.id || key[1] < hs.GetTerm() {
			continue
		}
		if key[1] > hs.GetTerm() {
			k.c.fail("durability: node %d restarted in term %d, but had voted for %d in term %d", n.id, hs.GetTerm(), cand, key[1])
		}
		if hs.GetVote() != cand {
			k.c.fail("durability: node %d restarted in term %d with vote %d, but had voted for %d", n.id, hs.GetTerm(), hs.GetVote(), cand)
		}
	}
}

func (k *checker) onApply(n *node, e *pb.Entry) {
	rec := entryRec{node: n.id, term: e.GetTerm(), typ: e.GetType(), hash: hashBytes(e.Data)}
	if prev, ok := k.entries[e.GetIndex()]; ok {
		if prev.term != rec.term || prev.typ != rec.typ || prev.hash != rec.hash {
			k.c.fail("state machine safety: at index %d node %d applied term %d %s, node %d applied term %d %s",
				e.GetIndex(), prev.node, prev.term, prev.typ, n.id, rec.term, rec.typ)
		}
	} else {
		k.entries[e.GetIndex()] = rec
		k.c.mixFP(5, e.GetIndex(), e.GetTerm())
	}
	if d, ok := k.digests[e.GetIndex()]; ok && d != n.sm.digest {
		k.c.fail("state machine safety: node %d state at index %d differs", n.id, e.GetIndex())
	}
	k.digests[e.GetIndex()] = n.sm.digest
	k.maxApplied = max(k.maxApplied, e.GetIndex())
	if e.GetType() != pb.EntryNormal || len(e.Data) == 0 {
		return
	}
	if bytes.HasPrefix(e.Data, []byte("probe")) && k.probeIndex == 0 {
		k.probeIndex = e.GetIndex()
	}
	if k.c.o.Dup == 0 {
		key := string(e.Data)
		if i := bytes.IndexByte(e.Data, 0); i >= 0 {
			key = key[:i]
		}
		if idx, ok := k.proposals[key]; ok && idx != e.GetIndex() {
			k.c.fail("at-most-once: proposal %q applied at indexes %d and %d without network duplication", key, idx, e.GetIndex())
		}
		k.proposals[key] = e.GetIndex()
	}
}

// configAsOf returns the configuration recorded at the highest index <= i.
func (k *checker) configAsOf(i uint64) (*pb.ConfState, bool) {
	best, found := uint64(0), false
	for idx := range k.configs {
		if idx <= i && (!found || idx > best) {
			best, found = idx, true
		}
	}
	return k.configs[best], found
}

// onConfig records the configuration n has at index, from applying a
// configuration change there or from a snapshot.
func (k *checker) onConfig(n *node, index uint64, cs *pb.ConfState, fromEntry bool) {
	if prev, ok := k.configAsOf(index); ok {
		if _, exact := k.configs[index]; exact {
			if err := prev.Equivalent(cs); err != nil {
				k.c.fail("configuration safety: node %d has a different configuration at index %d: %v", n.id, index, err)
			}
		}
	}
	if _, ok := k.configs[index]; !ok {
		k.configs[index] = cs
		if fromEntry {
			k.c.stats.ConfApplied++
			if len(cs.VotersOutgoing) > 0 {
				k.c.stats.JointEntered++
			}
		}
		k.c.tracef("configuration at %d: %s", index, cs.String())
	}
	members := map[uint64]bool{}
	for _, ids := range [][]uint64{cs.Voters, cs.VotersOutgoing, cs.Learners, cs.LearnersNext} {
		for _, id := range ids {
			members[id] = true
		}
	}
	// A node takes part from the moment a configuration including it
	// exists; an operator starts it with empty storage.
	for _, id := range sortedKeys(members) {
		if _, ok := k.c.nodes[id]; !ok {
			nn := k.c.newNode(id)
			k.c.start(nn)
		}
	}
	// IDs are never reused, so leaving a configuration is final.
	for _, id := range k.c.ids {
		if _, gone := k.removed[id]; !members[id] && !gone && index >= k.latestConfigIndex() {
			k.removed[id] = index
			k.c.tracef("node %d removed at %d", id, index)
		}
	}
}

func (k *checker) latestConfigIndex() uint64 {
	var m uint64
	for i := range k.configs {
		m = max(m, i)
	}
	return m
}

func (k *checker) decodeSnapshot(n *node, snap *pb.Snapshot) smState {
	if len(snap.Data) != 16 {
		k.c.fail("node %d: snapshot %d has %d bytes of data", n.id, snap.GetMetadata().GetIndex(), len(snap.GetData()))
	}
	s := smState{applied: binary.BigEndian.Uint64(snap.Data), digest: binary.BigEndian.Uint64(snap.Data[8:])}
	if s.applied != snap.GetMetadata().GetIndex() {
		k.c.fail("snapshot integrity: node %d got snapshot at index %d of state at %d", n.id, snap.GetMetadata().GetIndex(), s.applied)
	}
	if d, ok := k.digests[s.applied]; ok && d != s.digest {
		k.c.fail("snapshot integrity: node %d got a snapshot at %d whose state differs", n.id, s.applied)
	}
	if cs, ok := k.configAsOf(s.applied); ok {
		if err := cs.Equivalent(snap.GetMetadata().GetConfState()); err != nil {
			k.c.fail("snapshot integrity: node %d got a snapshot at %d with configuration %s, want %s", n.id, s.applied, snap.GetMetadata().GetConfState().String(), cs.String())
		}
	}
	k.digests[s.applied] = s.digest
	return s
}

func (k *checker) onReadIndex(n *node, ctx []byte) {
	k.reads[string(ctx)] = k.maxApplied
}

func (k *checker) onReadState(n *node, rs raft.ReadState) {
	lower, ok := k.reads[string(rs.RequestCtx)]
	if !ok {
		k.c.fail("node %d: read state for unknown request %x", n.id, rs.RequestCtx)
	}
	if rs.Index < lower {
		k.c.fail("linearizability: node %d read at index %d, but index %d had been applied before the read began", n.id, rs.Index, lower)
	}
	k.c.stats.ReadsChecked++
}

// checkLogs checks log matching across the storage of every running node.
func (k *checker) checkLogs() {
	type log struct {
		id          uint64
		first, last uint64
		ents        []*pb.Entry
	}
	var logs []log
	for _, n := range k.c.upNodes() {
		first, _ := n.st.FirstIndex()
		last, _ := n.st.LastIndex()
		var ents []*pb.Entry
		if last >= first {
			var err error
			ents, err = n.st.Entries(first, last+1, math.MaxUint64)
			if err != nil {
				k.c.fail("node %d: entries [%d, %d]: %v", n.id, first, last, err)
			}
			if uint64(len(ents)) != last-first+1 {
				k.c.fail("node %d: storage returned %d entries for [%d, %d]", n.id, len(ents), first, last)
			}
			for i, e := range ents {
				if e.GetIndex() != first+uint64(i) {
					k.c.fail("node %d: storage entry %d has index %d", n.id, first+uint64(i), e.GetIndex())
				}
			}
		}
		logs = append(logs, log{n.id, first, last, ents})
	}
	for i := range logs {
		for j := i + 1; j < len(logs); j++ {
			a, b := logs[i], logs[j]
			lo, hi := max(a.first, b.first), min(a.last, b.last)
			if lo > hi {
				continue
			}
			at := func(l log, idx uint64) *pb.Entry { return l.ents[idx-l.first] }
			match := uint64(0)
			for idx := hi; idx >= lo; idx-- {
				if at(a, idx).GetTerm() == at(b, idx).GetTerm() {
					match = idx
					break
				}
				if idx == 0 {
					break
				}
			}
			for idx := lo; match != 0 && idx <= match; idx++ {
				ea, eb := at(a, idx), at(b, idx)
				if ea.GetTerm() != eb.GetTerm() || ea.GetType() != eb.GetType() || !bytes.Equal(ea.Data, eb.Data) {
					k.c.fail("log matching: nodes %d and %d agree on the term of entry %d but differ at %d (terms %d, %d)", a.id, b.id, match, idx, ea.GetTerm(), eb.GetTerm())
				}
			}
		}
	}
}
