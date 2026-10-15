// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raftsim

// raft's messages are protobuf messages held by pointer (since raft moved
// to google.golang.org/protobuf), so raft, the storage and the application
// share them. The entries of a Ready are the ones raft keeps in its log,
// and the ones raft.MemoryStorage keeps as it is given them; the entries
// raft reads from storage are the storage's own (seglog's cache, or
// MemoryStorage's slice). Nothing works unless each party leaves alone what
// it hands out or is handed: "entries ... are immutable", as
// raft.MemoryStorage puts it. A write to a shared message would change a
// log behind its owner's back, which no other check sees until it makes
// replicas diverge, if ever.
//
// So the simulator records the encoding of everything handed between raft
// and storage: every message, entry, hard state and snapshot of a Ready,
// and every entry, hard state, configuration and snapshot raft reads from
// storage. It encodes each again when the same message is handed out again
// (raft reads the same entries from storage many times), once frozenWindow
// later records have been made, and at the end of the run, and fails if
// anything changed.

import (
	"bytes"
	"fmt"

	"google.golang.org/protobuf/proto"

	pb "github.com/raftseg/raft/raftpb"
)

// frozenWindow is how many records are kept before the oldest is checked.
const frozenWindow = 4096

type frozenItem struct {
	node uint64
	what string
	m    proto.Message
	b    []byte
}

// frozen is a ring of the latest records, and where each message's record
// is in it.
type frozen struct {
	items []frozenItem
	next  int
	at    map[proto.Message]int
}

// deterministic encodes the same message to the same bytes every time.
var deterministic = proto.MarshalOptions{Deterministic: true}

// freeze records m, handed out by or to node n, to be checked later.
func (c *Cluster) freeze(n *node, what string, m proto.Message) {
	if m == nil || !m.ProtoReflect().IsValid() {
		return // a nil message: nothing to change
	}
	f := &c.frozen
	if f.at == nil {
		f.at = map[proto.Message]int{}
	}
	if i, ok := f.at[m]; ok {
		// Handed out again: it must be as it was.
		c.checkFrozen(f.items[i])
		return
	}
	b, err := deterministic.Marshal(m)
	if err != nil {
		c.fail("node %d: encode %s: %v", n.id, what, err)
	}
	it := frozenItem{node: n.id, what: what, m: m, b: b}
	if len(f.items) < frozenWindow {
		f.at[m] = len(f.items)
		f.items = append(f.items, it)
		return
	}
	old := f.items[f.next]
	c.checkFrozen(old)
	delete(f.at, old.m)
	f.items[f.next] = it
	f.at[m] = f.next
	f.next = (f.next + 1) % frozenWindow
}

func (c *Cluster) checkFrozen(it frozenItem) {
	c.stats.FrozenChecked++
	b, err := deterministic.Marshal(it.m)
	if err == nil && bytes.Equal(b, it.b) {
		return
	}
	was := it.m.ProtoReflect().New().Interface()
	if err := proto.Unmarshal(it.b, was); err != nil {
		was = nil
	}
	what := it.what
	if m, ok := it.m.(*pb.Message); ok {
		what += " " + m.GetType().String()
	}
	c.fail("shared message changed: node %d's %s changed after it was handed out: was %s, now %s",
		it.node, what, short(was), short(it.m))
}

// checkAllFrozen checks every record still in the ring.
func (c *Cluster) checkAllFrozen() {
	for _, it := range c.frozen.items {
		c.checkFrozen(it)
	}
}

func short(m proto.Message) string {
	s := fmt.Sprint(m)
	if len(s) > 300 {
		s = s[:300] + "..."
	}
	return "{" + s + "}"
}
