// Copyright 2026 The raftseg authors.

package seglog

import (
	"encoding/json"
	"fmt"
	"math/rand"
	randv2 "math/rand/v2"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
)

// TestChaos runs multi-raft on several nodes, each persisting through its
// own Log on its own crashable filesystem, while the network drops,
// duplicates and reorders messages and partitions nodes, and nodes lose
// power at random filesystem operations (including in the middle of a
// Save). It checks raft's guarantees end to end:
//   - state machine safety: no two nodes ever apply different entries at the
//     same index of a group, including after a node restarts from disk;
//   - durability: after healing, every node has applied every entry any node
//     ever applied.
//
// A storage bug that loses a persisted vote, entry or HardState shows up as
// a safety violation.
func TestChaos(t *testing.T) {
	cfgs := []struct {
		nodes, groups, rounds int
		seed                  int64
	}{
		{3, 4, 4000, 1},
		{5, 3, 3000, 2},
		{3, 6, 3000, 3},
	}
	if testing.Short() {
		cfgs = cfgs[:1]
		cfgs[0].rounds = 1200
	}
	for _, c := range cfgs {
		t.Run(fmt.Sprintf("n%d-g%d-s%d", c.nodes, c.groups, c.seed), func(t *testing.T) {
			runChaos(t, c.nodes, c.groups, c.rounds, c.seed)
		})
	}
}

type chaosSM struct {
	Values  []string
	Applied uint64
	snap    uint64
}

type chaosNode struct {
	id       uint64
	mem      *crashfs.FS
	ops      atomic.Int64
	crashAt  atomic.Int64
	clone    atomic.Pointer[crashfs.FS] // set by the filesystem hook at the power cut
	log      *Log
	up       bool
	downFor  int
	rns      map[uint64]*raft.RawNode
	sms      map[uint64]*chaosSM
	restarts int
}

type chaosNet struct {
	q []envelopeTo
}

type envelopeTo struct {
	group uint64
	msg   raftpb.Message
}

type chaos struct {
	t        *testing.T
	rng      *rand.Rand
	nodes    []*chaosNode
	groups   []uint64
	opts     Options
	net      chaosNet
	truth    map[uint64]map[uint64]string // group -> index -> "term/data"
	votes    map[[3]uint64]uint64         // (group, voter, term) -> candidate
	leaders  map[[2]uint64]uint64         // (group, term) -> leader
	isolated map[uint64]bool
	drop     float64
	nextVal  int
	crashes  int
	snapsIn  int
}

func runChaos(t *testing.T, nnodes, ngroups, rounds int, seed int64) {
	c := &chaos{
		t:        t,
		rng:      rand.New(rand.NewSource(seed)),
		opts:     Options{SegmentSize: 64 << 10, CleanAfterSegments: 1, SnapshotInlineBytes: 2 << 10, CacheEntries: 32},
		truth:    map[uint64]map[uint64]string{},
		votes:    map[[3]uint64]uint64{},
		leaders:  map[[2]uint64]uint64{},
		isolated: map[uint64]bool{},
		drop:     0.05,
	}
	for g := 1; g <= ngroups; g++ {
		c.groups = append(c.groups, uint64(g))
		c.truth[uint64(g)] = map[uint64]string{}
	}
	var voters []uint64
	for id := uint64(1); id <= uint64(nnodes); id++ {
		voters = append(voters, id)
	}
	for id := uint64(1); id <= uint64(nnodes); id++ {
		n := &chaosNode{id: id, mem: crashfs.New()}
		c.nodes = append(c.nodes, n)
		l, err := Open(crashDir, memOpts(n.mem, c.opts))
		if err != nil {
			t.Fatal(err)
		}
		for _, g := range c.groups {
			snap := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: voters}}}
			if err := l.Save(Update{Group: g, Snapshot: snap, HardState: raftpb.HardState{Term: 1, Commit: 1}}); err != nil {
				t.Fatal(err)
			}
		}
		l.Close()
		c.start(n)
	}

	for r := 0; r < rounds; r++ {
		c.chaosEvents()
		c.round()
		c.propose(2)
	}
	// Heal: no faults, everyone up, until all groups converge.
	c.drop = 0
	c.isolated = map[uint64]bool{}
	for _, n := range c.nodes {
		n.crashAt.Store(0)
		if !n.up {
			c.start(n)
		}
	}
	converged := false
	for r := 0; r < 5000 && !converged; r++ {
		c.round()
		if r%50 == 0 {
			c.propose(1) // make sure every group gets a leader and commits
		}
		converged = r > 100 && c.converged()
	}
	if !converged {
		t.Fatalf("cluster did not converge after healing")
	}
	c.verifyDurability()
	total := 0
	for _, g := range c.groups {
		total += len(c.truth[g])
	}
	t.Logf("%d rounds, %d power cuts, %d snapshots installed, %d committed entries", rounds, c.crashes, c.snapsIn, total)
	for _, n := range c.nodes {
		n.log.Close()
	}
}

func (c *chaos) start(n *chaosNode) {
	if cl := n.clone.Swap(nil); cl != nil {
		n.mem = cl
	}
	n.ops.Store(0)
	mem := n.mem
	fs := countingFS(mem, func(crashfs.Op) error {
		k := n.ops.Add(1)
		if at := n.crashAt.Load(); at > 0 && k == at && n.clone.Load() == nil {
			pct := int((uint64(k)*2654435761 + n.id*97) % 101)
			n.clone.Store(mem.CrashClone(pct, randv2.New(randv2.NewPCG(uint64(k), n.id))))
		}
		return nil
	})
	l, err := Open(crashDir, memOpts(fs, c.opts))
	if err != nil {
		c.t.Fatalf("node %d restart %d: %v", n.id, n.restarts, err)
	}
	n.log, n.up, n.downFor = l, true, 0
	n.rns, n.sms = map[uint64]*raft.RawNode{}, map[uint64]*chaosSM{}
	for _, g := range c.groups {
		st := l.Group(g)
		snap, err := st.Snapshot()
		if err != nil {
			c.t.Fatalf("node %d: snapshot: %v", n.id, err)
		}
		sm := &chaosSM{}
		if len(snap.Data) > 0 {
			if err := json.Unmarshal(snap.Data, sm); err != nil {
				c.t.Fatal(err)
			}
		}
		sm.Applied, sm.snap = snap.Metadata.Index, snap.Metadata.Index
		rn, err := raft.NewRawNode(&raft.Config{
			ID: n.id, ElectionTick: 10, HeartbeatTick: 1, Storage: st,
			MaxSizePerMsg: 16 << 10, MaxInflightMsgs: 32, Applied: sm.Applied,
			PreVote: true, CheckQuorum: true, Logger: discard,
		})
		if err != nil {
			c.t.Fatalf("node %d group %d: %v", n.id, g, err)
		}
		n.rns[g], n.sms[g] = rn, sm
	}
}

// chaosEvents schedules power cuts, recoveries and partitions.
func (c *chaos) chaosEvents() {
	for _, n := range c.nodes {
		if !n.up {
			if n.downFor--; n.downFor <= 0 {
				n.restarts++
				c.start(n)
			}
			continue
		}
		if n.crashAt.Load() == 0 && c.rng.Float64() < 0.004 {
			n.crashAt.Store(n.ops.Load() + 1 + c.rng.Int63n(300))
		}
	}
	if c.rng.Float64() < 0.01 {
		c.isolated = map[uint64]bool{}
		if c.rng.Intn(2) == 0 {
			c.isolated[uint64(1+c.rng.Intn(len(c.nodes)))] = true
		}
	}
}

func (c *chaos) crashed(n *chaosNode) bool { return n.clone.Load() != nil }

func (c *chaos) round() {
	for _, n := range c.nodes {
		if !n.up {
			continue
		}
		c.process(n)
		if c.crashed(n) {
			// The power was cut during this round: everything the node did
			// after that instant never happened.
			n.log.crash()
			n.up, n.downFor = false, 1+c.rng.Intn(30)
			n.crashAt.Store(0)
			c.crashes++
		}
	}
	c.deliver()
	for _, n := range c.nodes {
		if n.up {
			for _, rn := range n.rns {
				rn.Tick()
			}
		}
	}
}

func (c *chaos) process(n *chaosNode) {
	type pending struct {
		g  uint64
		rd raft.Ready
	}
	var ups []Update
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
	if c.crashed(n) {
		return // died before the Save returned: nothing was sent or applied
	}
	for _, p := range rds {
		sm := n.sms[p.g]
		if !raft.IsEmptySnap(p.rd.Snapshot) {
			restored := &chaosSM{}
			if err := json.Unmarshal(p.rd.Snapshot.Data, restored); err != nil {
				c.t.Fatal(err)
			}
			sm.Values, sm.Applied, sm.snap = restored.Values, p.rd.Snapshot.Metadata.Index, p.rd.Snapshot.Metadata.Index
			c.snapsIn++
		}
		for _, m := range p.rd.Messages {
			if !c.crashed(n) {
				c.checkVote(p.g, m)
				c.net.q = append(c.net.q, envelopeTo{p.g, m})
			}
		}
		if st := n.rns[p.g].Status(); st.RaftState == raft.StateLeader {
			k := [2]uint64{p.g, st.Term}
			if prev, ok := c.leaders[k]; ok && prev != n.id {
				c.t.Fatalf("ELECTION SAFETY VIOLATION: group %d term %d has leaders %d and %d", p.g, st.Term, prev, n.id)
			}
			c.leaders[k] = n.id
		}
		for _, e := range p.rd.CommittedEntries {
			if e.Index <= sm.Applied {
				continue
			}
			c.checkSafety(n, p.g, e)
			if e.Type == raftpb.EntryNormal && len(e.Data) > 0 {
				sm.Values = append(sm.Values, string(e.Data))
			}
			sm.Applied = e.Index
		}
		n.rns[p.g].Advance(p.rd)
		c.maybeCompact(n, p.g)
		if c.crashed(n) {
			return
		}
	}
}

// checkVote: a node grants at most one vote per term, even across restarts.
// A vote is granted only after the HardState recording it was saved, so a
// second, different vote in the same term means the storage lost it.
func (c *chaos) checkVote(g uint64, m raftpb.Message) {
	if m.Type != raftpb.MsgVoteResp || m.Reject {
		return
	}
	k := [3]uint64{g, m.From, m.Term}
	if prev, ok := c.votes[k]; ok && prev != m.To {
		c.t.Fatalf("VOTE SAFETY VIOLATION: node %d voted for %d and %d in group %d term %d", m.From, prev, m.To, g, m.Term)
	}
	c.votes[k] = m.To
}

func (c *chaos) checkSafety(n *chaosNode, g uint64, e raftpb.Entry) {
	key := fmt.Sprintf("%d/%x", e.Term, e.Data)
	if prev, ok := c.truth[g][e.Index]; ok && prev != key {
		c.t.Fatalf("SAFETY VIOLATION: node %d applied %s at group %d index %d, another node applied %s", n.id, key, g, e.Index, prev)
	}
	c.truth[g][e.Index] = key
}

func (c *chaos) maybeCompact(n *chaosNode, g uint64) {
	sm := n.sms[g]
	if sm.Applied < sm.snap+20 {
		return
	}
	data, _ := json.Marshal(sm)
	st := n.log.Group(g)
	if _, err := st.CreateSnapshot(sm.Applied, nil, data); err != nil {
		c.t.Fatalf("node %d group %d snapshot at %d: %v", n.id, g, sm.Applied, err)
	}
	sm.snap = sm.Applied
	if first, _ := st.FirstIndex(); sm.Applied >= first+5 {
		if err := st.Compact(sm.Applied - 5); err != nil {
			c.t.Fatalf("compact: %v", err)
		}
	}
}

func (c *chaos) deliver() {
	q := c.net.q
	c.net.q = nil
	c.rng.Shuffle(len(q), func(i, j int) { q[i], q[j] = q[j], q[i] })
	for _, env := range q {
		to, from := c.nodes[env.msg.To-1], c.nodes[env.msg.From-1]
		ok := to.up && !c.isolated[to.id] && !c.isolated[from.id] && c.rng.Float64() >= c.drop
		if ok {
			_ = to.rns[env.group].Step(env.msg)
			if c.rng.Float64() < 0.02 {
				_ = to.rns[env.group].Step(env.msg) // duplicate
			}
		}
		if from.up {
			if env.msg.Type == raftpb.MsgSnap {
				status := raft.SnapshotFinish
				if !ok {
					status = raft.SnapshotFailure
				}
				from.rns[env.group].ReportSnapshot(env.msg.To, status)
			}
			if !ok {
				from.rns[env.group].ReportUnreachable(env.msg.To)
			}
		}
	}
}

func (c *chaos) propose(k int) {
	for _, g := range c.groups {
		for _, n := range c.nodes {
			if n.up && n.rns[g].Status().RaftState == raft.StateLeader {
				for i := 0; i < k; i++ {
					c.nextVal++
					_ = n.rns[g].Propose([]byte(fmt.Sprintf("v%d", c.nextVal)))
				}
				break
			}
		}
	}
}

func (c *chaos) converged() bool {
	for _, g := range c.groups {
		var max uint64
		for i := range c.truth[g] {
			if i > max {
				max = i
			}
		}
		for _, n := range c.nodes {
			if !n.up || n.sms[g].Applied != max {
				return false
			}
		}
	}
	return true
}

// verifyDurability: every node applied exactly the committed sequence.
func (c *chaos) verifyDurability() {
	for _, g := range c.groups {
		idx := make([]uint64, 0, len(c.truth[g]))
		for i := range c.truth[g] {
			idx = append(idx, i)
		}
		sort.Slice(idx, func(i, j int) bool { return idx[i] < idx[j] })
		var want []string
		for _, i := range idx {
			var term uint64
			var data []byte
			fmt.Sscanf(c.truth[g][i], "%d/%x", &term, &data)
			if len(data) > 0 {
				want = append(want, string(data))
			}
		}
		for _, n := range c.nodes {
			got := n.sms[g].Values
			if len(got) != len(want) {
				c.t.Fatalf("node %d group %d applied %d values, committed %d", n.id, g, len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					c.t.Fatalf("node %d group %d value %d = %q, committed %q", n.id, g, i, got[i], want[i])
				}
			}
		}
	}
}
