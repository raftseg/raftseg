// Copyright 2026 The raftseg authors.

package seglog

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"testing"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
)

// An in-process 3-node cluster running many raft groups per node, each node
// persisting all of its groups through one Log. Messages are delivered
// synchronously in rounds, so the test is deterministic.

type envelope struct {
	group uint64
	msg   *raftpb.Message
}

type stateMachine struct {
	Values  []string
	applied uint64
	snap    uint64
}

type testNode struct {
	id      uint64
	dir     string
	log     *Log
	up      bool
	rns     map[uint64]*raft.RawNode
	sms     map[uint64]*stateMachine
	snapsIn int // snapshots received from a leader
}

type cluster struct {
	t      *testing.T
	nodes  []*testNode
	groups []uint64
	opts   Options
	outbox []envelope
}

var voters = &raftpb.ConfState{Voters: []uint64{1, 2, 3}}

func newCluster(t *testing.T, ngroups int) *cluster {
	c := &cluster{t: t, opts: Options{SegmentSize: 64 << 10, CacheEntries: 64}}
	for g := 1; g <= ngroups; g++ {
		c.groups = append(c.groups, uint64(100+g))
	}
	root := t.TempDir()
	for id := uint64(1); id <= 3; id++ {
		n := &testNode{id: id, dir: filepath.Join(root, fmt.Sprint(id))}
		c.nodes = append(c.nodes, n)
		l, err := Open(n.dir, c.opts)
		if err != nil {
			t.Fatal(err)
		}
		// Bootstrap every group from an initial snapshot holding the config.
		for _, g := range c.groups {
			snap := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(1)), Term: new(uint64(1)), ConfState: voters}}
			if err := l.Save(Update{Group: g, Snapshot: snap}); err != nil {
				t.Fatal(err)
			}
		}
		l.Close()
		c.start(n)
	}
	return c
}

var discard = &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}

func (c *cluster) start(n *testNode) {
	l, err := Open(n.dir, c.opts)
	if err != nil {
		c.t.Fatalf("node %d open: %v", n.id, err)
	}
	n.log, n.up = l, true
	n.rns, n.sms = map[uint64]*raft.RawNode{}, map[uint64]*stateMachine{}
	for _, g := range c.groups {
		st := l.Group(g)
		snap, err := st.Snapshot()
		if err != nil {
			c.t.Fatal(err)
		}
		sm := &stateMachine{}
		if len(snap.Data) > 0 {
			if err := json.Unmarshal(snap.Data, sm); err != nil {
				c.t.Fatal(err)
			}
		}
		sm.applied, sm.snap = snap.Metadata.GetIndex(), snap.Metadata.GetIndex()
		rn, err := raft.NewRawNode(&raft.Config{
			ID: n.id, ElectionTick: 10, HeartbeatTick: 1, Storage: st,
			MaxSizePerMsg: 64 << 10, MaxInflightMsgs: 64, Applied: sm.applied,
			PreVote: true, Logger: discard,
		})
		if err != nil {
			c.t.Fatal(err)
		}
		n.rns[g], n.sms[g] = rn, sm
	}
}

func (c *cluster) stop(n *testNode, crash bool) {
	if crash {
		n.log.crash()
	} else if err := n.log.Close(); err != nil {
		c.t.Fatal(err)
	}
	n.up, n.rns = false, nil
}

// round processes one Ready per group on every node (one Save per node, i.e.
// one group commit for all of its groups), then delivers messages and ticks.
func (c *cluster) round(tick bool) {
	for _, n := range c.nodes {
		if !n.up {
			continue
		}
		var ups []Update
		type pending struct {
			g  uint64
			rd raft.Ready
		}
		var rds []pending
		for _, g := range c.groups {
			rn := n.rns[g]
			if !rn.HasReady() {
				continue
			}
			rd := rn.Ready()
			ups = append(ups, Update{Group: g, HardState: rd.HardState, Entries: rd.Entries, Snapshot: rd.Snapshot})
			rds = append(rds, pending{g, rd})
		}
		if err := n.log.Save(ups...); err != nil {
			c.t.Fatalf("node %d save: %v", n.id, err)
		}
		for _, p := range rds {
			sm := n.sms[p.g]
			if !raft.IsEmptySnap(p.rd.Snapshot) {
				restored := &stateMachine{}
				if err := json.Unmarshal(p.rd.Snapshot.Data, restored); err != nil {
					c.t.Fatal(err)
				}
				sm.Values, sm.applied, sm.snap = restored.Values, p.rd.Snapshot.Metadata.GetIndex(), p.rd.Snapshot.Metadata.GetIndex()
				n.snapsIn++
			}
			for _, e := range p.rd.CommittedEntries {
				if e.GetIndex() <= sm.applied {
					continue
				}
				if e.GetType() == raftpb.EntryNormal && len(e.Data) > 0 {
					sm.Values = append(sm.Values, string(e.Data))
				}
				sm.applied = e.GetIndex()
			}
			for _, m := range p.rd.Messages {
				c.outbox = append(c.outbox, envelope{p.g, m})
			}
			n.rns[p.g].Advance(p.rd)
			c.maybeCompact(n, p.g)
		}
	}
	out := c.outbox
	c.outbox = nil
	for _, env := range out {
		to, from := c.nodes[env.msg.GetTo()-1], c.nodes[env.msg.GetFrom()-1]
		delivered := to.up
		if delivered {
			_ = to.rns[env.group].Step(env.msg)
		}
		if from.up {
			if env.msg.GetType() == raftpb.MsgSnap {
				status := raft.SnapshotFinish
				if !delivered {
					status = raft.SnapshotFailure
				}
				from.rns[env.group].ReportSnapshot(env.msg.GetTo(), status)
			}
			if !delivered {
				from.rns[env.group].ReportUnreachable(env.msg.GetTo())
			}
		}
	}
	if tick {
		for _, n := range c.nodes {
			if n.up {
				for _, rn := range n.rns {
					rn.Tick()
				}
			}
		}
	}
}

// maybeCompact snapshots the state machine every 25 applied entries and
// compacts the log, keeping a short tail for slightly lagging followers.
func (c *cluster) maybeCompact(n *testNode, g uint64) {
	sm := n.sms[g]
	if sm.applied < sm.snap+25 {
		return
	}
	data, _ := json.Marshal(sm)
	st := n.log.Group(g)
	if _, err := st.CreateSnapshot(sm.applied, voters, data); err != nil {
		c.t.Fatalf("node %d group %d snapshot: %v", n.id, g, err)
	}
	sm.snap = sm.applied
	if first, _ := st.FirstIndex(); sm.applied-5 >= first {
		if err := st.Compact(sm.applied - 5); err != nil {
			c.t.Fatalf("compact: %v", err)
		}
	}
}

func (c *cluster) leader(g uint64) *testNode {
	for _, n := range c.nodes {
		if n.up && n.rns[g].Status().RaftState == raft.StateLeader {
			return n
		}
	}
	return nil
}

func (c *cluster) elect(id uint64) {
	for _, g := range c.groups {
		c.nodes[id-1].rns[g].Campaign()
	}
	for i := 0; i < 200; i++ {
		c.round(false)
		ok := true
		for _, g := range c.groups {
			if c.leader(g) == nil {
				ok = false
			}
		}
		if ok {
			return
		}
	}
	c.t.Fatal("no leaders elected")
}

// propose sends n values to every group's leader and runs until every up
// node has applied all of them.
func (c *cluster) propose(next map[uint64]int, n int) {
	for i := 0; i < n; i++ {
		for _, g := range c.groups {
			ld := c.leader(g)
			if ld == nil {
				c.t.Fatalf("group %d has no leader", g)
			}
			if err := ld.rns[g].Propose([]byte(fmt.Sprintf("g%d-v%d", g, next[g]))); err != nil {
				c.t.Fatal(err)
			}
			next[g]++
		}
		if i%7 == 0 {
			c.round(true)
		}
	}
	c.settle(next)
}

func (c *cluster) settle(next map[uint64]int) {
	for i := 0; i < 2000; i++ {
		c.round(i%4 == 0)
		done := true
		for _, n := range c.nodes {
			if !n.up {
				continue
			}
			for _, g := range c.groups {
				if len(n.sms[g].Values) != next[g] {
					done = false
				}
			}
		}
		if done {
			return
		}
	}
	for _, n := range c.nodes {
		if n.up {
			c.t.Logf("node %d group %d: %d values, want %d", n.id, c.groups[0], len(n.sms[c.groups[0]].Values), next[c.groups[0]])
		}
	}
	c.t.Fatal("cluster did not converge")
}

func (c *cluster) verify(next map[uint64]int) {
	for _, g := range c.groups {
		for _, n := range c.nodes {
			if !n.up {
				continue
			}
			vals := n.sms[g].Values
			if len(vals) != next[g] {
				c.t.Fatalf("node %d group %d: %d values, want %d", n.id, g, len(vals), next[g])
			}
			for i, v := range vals {
				if want := fmt.Sprintf("g%d-v%d", g, i); v != want {
					c.t.Fatalf("node %d group %d value %d = %q, want %q", n.id, g, i, v, want)
				}
			}
		}
	}
}

func TestCluster(t *testing.T) {
	c := newCluster(t, 8)
	next := map[uint64]int{}
	c.elect(1)

	c.propose(next, 150)
	c.verify(next)

	// Node 3 goes down; the others write on and compact far past it.
	n3 := c.nodes[2]
	c.stop(n3, false)
	c.propose(next, 200)
	c.verify(next)
	for _, n := range c.nodes[:2] {
		if _, err := n.log.GC(); err != nil {
			t.Fatal(err)
		}
		st, err := n.log.Stats()
		if err != nil {
			t.Fatal(err)
		}
		if st.RemovedSegments == 0 {
			t.Fatalf("node %d: no segments were ever garbage collected (%d live)", n.id, len(st.Segments))
		}
	}

	// Node 3 restarts from disk and must catch up through snapshots.
	c.start(n3)
	c.settle(next)
	c.verify(next)
	if n3.snapsIn == 0 {
		t.Fatal("node 3 caught up without receiving a snapshot")
	}

	// Node 2 crashes and restarts while traffic continues.
	c.stop(c.nodes[1], true)
	c.propose(next, 40)
	c.start(c.nodes[1])
	c.settle(next)
	c.verify(next)

	// Full cluster restart: everything is rebuilt from the logs.
	for _, n := range c.nodes {
		c.stop(n, false)
	}
	for _, n := range c.nodes {
		c.start(n)
	}
	for _, n := range c.nodes {
		for _, g := range c.groups {
			if got := n.rns[g].Status().GetCommit(); got == 0 {
				t.Fatalf("node %d group %d lost its commit index", n.id, g)
			}
		}
	}
	c.elect(2)
	c.propose(next, 60)
	c.verify(next)
	t.Logf("final: %d values per group, node segments %d/%d/%d",
		next[c.groups[0]], c.nodes[0].log.SegmentCount(), c.nodes[1].log.SegmentCount(), c.nodes[2].log.SegmentCount())
	for _, n := range c.nodes {
		c.stop(n, false)
	}
}
