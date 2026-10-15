// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

// Package raftsim runs deterministic, randomized simulations of raft
// clusters and checks raft's safety and liveness properties throughout.
//
// One seeded random source drives everything: message delivery order,
// loss, duplication and delay; partitions; crashes that lose unsynced
// writes; restarts; client proposals, reads and configuration changes;
// snapshots and log compaction; leadership transfers. A run is a pure
// function of its Options, so any failure replays exactly from its seed.
//
// Node storage is pluggable (Backend), so the same simulations exercise
// every storage implementation and mixtures of them.
package raftsim

import (
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"runtime/debug"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/confchange"
	pb "github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raft/tracker"
)

// tickEvery is the number of simulation steps per raft tick. Message delays
// are measured in steps, so delivery interleaves finely with ticks.
const tickEvery = 10

// Options configure a run. RandomOptions draws a varied configuration.
type Options struct {
	Seed     int64
	Voters   int // initial voters
	Learners int // initial learners
	MaxNodes int // node IDs ever created, including ones added later
	Steps    int // steps of the faulty phase
	// HealTicks bounds the liveness phase: once faults stop, a leader must
	// commit an entry that every member applies within this many ticks.
	HealTicks int

	ElectionTick, HeartbeatTick int
	PreVote, CheckQuorum        bool
	Async                       bool // AsyncStorageWrites on every node
	MixedAsync                  bool // AsyncStorageWrites on a random half of the nodes
	StepDownOnRemoval           bool
	DisableForwarding           bool
	ReadOnly                    raft.ReadOnlyOption
	MaxSizePerMsg               uint64
	MaxInflight                 int
	MaxInflightBytes            uint64
	MaxUncommitted              uint64
	MaxCommittedPerReady        uint64

	// Network faults, as probabilities per message.
	Drop, Dup, LongDelay float64
	// Events, as probabilities per step.
	CrashRate, PartitionRate, ConfChangeRate, ProposeRate, ReadRate float64
	// Faults aimed at the current leader, which most safety bugs need.
	LeaderCrashRate, LeaderIsolateRate                   float64
	SnapshotRate, TransferRate, CampaignRate, ForgetRate float64
	// ClockSkew makes nodes miss ticks now and then. Lease-based reads
	// assume bounded skew, so RandomOptions never combines the two.
	ClockSkew bool

	// SendBeforePersist breaks raft's contract in synchronous mode by
	// sending messages before persisting. Used to check that the checker
	// notices.
	SendBeforePersist bool

	Backend Backend
	Trace   bool // keep a readable trace of the run (slow)
}

func pick[T any](r *rand.Rand, xs ...T) T { return xs[r.Intn(len(xs))] }

// RandomOptions draws a configuration from seed.
func RandomOptions(seed int64) Options {
	r := rand.New(rand.NewSource(seed))
	o := Options{Seed: seed}
	o.Voters = pick(r, 1, 3, 3, 3, 5, 5, 7)
	if r.Intn(3) == 0 {
		o.Learners = 1 + r.Intn(2)
	}
	o.MaxNodes = o.Voters + o.Learners + 4
	o.Steps = 6000
	o.HealTicks = 1500
	o.ElectionTick = 10
	o.HeartbeatTick = pick(r, 1, 1, 2, 3)
	o.PreVote = r.Intn(2) == 0
	o.CheckQuorum = r.Intn(2) == 0
	switch r.Intn(3) {
	case 0:
		o.Async = true
	case 1:
		o.MixedAsync = true
	}
	o.StepDownOnRemoval = r.Intn(2) == 0
	o.DisableForwarding = r.Intn(6) == 0
	if o.CheckQuorum && r.Intn(3) == 0 {
		o.ReadOnly = raft.ReadOnlyLeaseBased
	}
	o.MaxSizePerMsg = pick[uint64](r, 0, 64, 1024, math.MaxUint64)
	o.MaxInflight = pick(r, 1, 4, 256)
	if b := pick[uint64](r, 0, 0, 2048); b >= o.MaxSizePerMsg {
		o.MaxInflightBytes = b
	}
	o.MaxUncommitted = pick[uint64](r, 0, 0, 4096)
	o.MaxCommittedPerReady = pick[uint64](r, 0, 256, 1<<20)
	o.Drop = pick(r, 0, 0.01, 0.05, 0.2)
	o.Dup = pick(r, 0, 0, 0.02, 0.1)
	o.LongDelay = pick(r, 0, 0.01, 0.05)
	o.CrashRate = pick(r, 0, 0.0005, 0.002)
	o.PartitionRate = pick(r, 0, 0.001, 0.003)
	o.ConfChangeRate = pick(r, 0, 0.002, 0.006)
	o.ProposeRate = pick(r, 0.02, 0.05, 0.2)
	o.ReadRate = pick(r, 0, 0.01, 0.03)
	o.SnapshotRate = pick(r, 0, 0.002, 0.01)
	o.TransferRate = pick(r, 0, 0.0005, 0.002)
	o.CampaignRate = pick(r, 0, 0, 0.0005)
	o.ForgetRate = pick(r, 0, 0, 0.0005)
	o.ClockSkew = o.ReadOnly != raft.ReadOnlyLeaseBased && r.Intn(3) == 0
	o.LeaderCrashRate = pick(r, 0, 0, 0.0005, 0.002)
	o.LeaderIsolateRate = pick(r, 0, 0, 0.0005, 0.002)
	if r.Intn(5) == 0 {
		// Membership churn: configuration changes keep coming while
		// leaders fall, so nodes lag configurations behind each other.
		o.Voters = pick(r, 3, 3, 5)
		o.ConfChangeRate = 0.02
		o.LeaderCrashRate = 0.002
		o.LeaderIsolateRate = 0.003
		o.MaxNodes = o.Voters + o.Learners + 8
	} else if r.Intn(4) == 0 {
		// Chaos: leaders fall constantly, and the network is poor.
		o.Voters = pick(r, 3, 5, 5)
		o.CrashRate = 0.004
		o.LeaderCrashRate = 0.004
		o.LeaderIsolateRate = 0.004
		o.PartitionRate = 0.004
		o.Drop = pick(r, 0.05, 0.2, 0.3)
		o.LongDelay = pick(r, 0.02, 0.1)
		o.MaxSizePerMsg = pick[uint64](r, 0, 64)
		o.MaxNodes = o.Voters + o.Learners + 4
	}
	o.Backend = &MemBackend{}
	return o
}

func (o Options) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "seed=%d voters=%d learners=%d backend=%s", o.Seed, o.Voters, o.Learners, o.Backend.Name())
	flag := func(on bool, name string) {
		if on {
			b.WriteString(" " + name)
		}
	}
	flag(o.PreVote, "prevote")
	flag(o.CheckQuorum, "checkquorum")
	flag(o.Async, "async")
	flag(o.MixedAsync, "mixed-async")
	flag(o.StepDownOnRemoval, "stepdown")
	flag(o.DisableForwarding, "noforward")
	flag(o.ReadOnly == raft.ReadOnlyLeaseBased, "lease")
	flag(o.ClockSkew, "skew")
	flag(o.SendBeforePersist, "SEND-BEFORE-PERSIST")
	fmt.Fprintf(&b, " hb=%d msg=%d inflight=%d/%d uncommitted=%d", o.HeartbeatTick, o.MaxSizePerMsg, o.MaxInflight, o.MaxInflightBytes, o.MaxUncommitted)
	fmt.Fprintf(&b, " drop=%g dup=%g long=%g crash=%g part=%g conf=%g snap=%g", o.Drop, o.Dup, o.LongDelay, o.CrashRate, o.PartitionRate, o.ConfChangeRate, o.SnapshotRate)
	fmt.Fprintf(&b, " leadercrash=%g leaderisolate=%g", o.LeaderCrashRate, o.LeaderIsolateRate)
	return b.String()
}

// Stats count what a run exercised.
type Stats struct {
	Steps, Ticks                          int64
	Leaders                               int // terms in which a leader was seen
	Applied                               uint64
	Proposals, ConfChanges, ConfApplied   int
	JointEntered                          int
	Snapshots, SnapshotsSent, SnapRestore int
	Crashes, Restarts, Partitions         int
	Reads, ReadsChecked, ReadRetries      int
	Sent, Delivered, Dropped, Duplicated  int64
	Transfers, Campaigns                  int
	NodesAdded, NodesRemoved              int
	HealTicks                             int64
	FrozenChecked                         int64 // shared messages checked unchanged (frozen.go)
	Fingerprint                           uint64
}

func (s *Stats) Add(o Stats) {
	s.Steps += o.Steps
	s.Ticks += o.Ticks
	s.Leaders += o.Leaders
	s.Applied += o.Applied
	s.Proposals += o.Proposals
	s.ConfChanges += o.ConfChanges
	s.ConfApplied += o.ConfApplied
	s.JointEntered += o.JointEntered
	s.Snapshots += o.Snapshots
	s.SnapshotsSent += o.SnapshotsSent
	s.SnapRestore += o.SnapRestore
	s.Crashes += o.Crashes
	s.Restarts += o.Restarts
	s.Partitions += o.Partitions
	s.Reads += o.Reads
	s.ReadsChecked += o.ReadsChecked
	s.ReadRetries += o.ReadRetries
	s.Sent += o.Sent
	s.Delivered += o.Delivered
	s.Dropped += o.Dropped
	s.Duplicated += o.Duplicated
	s.FrozenChecked += o.FrozenChecked
	s.Transfers += o.Transfers
	s.Campaigns += o.Campaigns
	s.NodesAdded += o.NodesAdded
	s.NodesRemoved += o.NodesRemoved
	s.HealTicks += o.HealTicks
}

// Failure describes a violated property or a crash of the simulation.
type Failure struct {
	Options Options
	Step    int64
	Msg     string
	Stack   string // for panics inside raft
	Trace   []string
}

func (f *Failure) Error() string {
	return fmt.Sprintf("raftsim: %s\n  at step %d with %s", f.Msg, f.Step, f.Options)
}

type violation struct{ msg string }

type node struct {
	id             uint64
	async          bool // AsyncStorageWrites
	gen            int
	up             bool
	decommissioned bool
	downUntil      int64
	rn             *raft.RawNode
	st             Storage
	sm             smState
	cs             *pb.ConfState // as of sm.applied
	appendQ        []*pb.Message
	applyQ         []*pb.Message
}

type envelope struct {
	at  int64
	seq uint64
	m   *pb.Message
}

type msgQueue []envelope

func (q msgQueue) Len() int { return len(q) }
func (q msgQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q msgQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *msgQueue) Push(x any)   { *q = append(*q, x.(envelope)) }
func (q *msgQueue) Pop() any {
	old := *q
	e := old[len(old)-1]
	*q = old[:len(old)-1]
	return e
}

// Cluster is one simulation.
type Cluster struct {
	o       Options
	rng     *rand.Rand
	now     int64
	nodes   map[uint64]*node
	ids     []uint64 // in creation order
	q       msgQueue
	seq     uint64
	blocked map[[2]uint64]bool
	healAt  int64
	nextID  uint64
	propN   uint64
	readN   uint64
	// recentReads holds the contexts of the latest reads, which clients
	// retry (see read).
	recentReads [][]byte
	healing     bool
	chk         *checker
	frozen      frozen // see frozen.go
	stats       Stats
	fp          uint64 // fingerprint of the run, for determinism checks
	trace       []string
}

// Run simulates one cluster with o and returns what it exercised, or the
// first property it found violated.
func Run(o Options) (st Stats, err error) {
	c := newCluster(o)
	defer func() {
		if r := recover(); r != nil {
			f := &Failure{Options: o, Step: c.now, Trace: c.trace}
			if v, ok := r.(violation); ok {
				f.Msg = v.msg
			} else {
				f.Msg = fmt.Sprintf("panic: %v", r)
				f.Stack = string(debug.Stack())
			}
			err = f
		}
	}()
	defer c.closeStorage()
	c.bootstrap()
	for i := 0; i < o.Steps; i++ {
		c.step(true)
	}
	c.heal()
	c.finalChecks()
	c.stats.Fingerprint = c.fp
	return c.stats, nil
}

// closeStorage releases what node storage holds (files, goroutines).
func (c *Cluster) closeStorage() {
	for _, id := range c.ids {
		if n := c.nodes[id]; n != nil && n.st != nil {
			if cl, ok := n.st.(interface{ Close() error }); ok {
				cl.Close()
			}
		}
	}
}

func newCluster(o Options) *Cluster {
	if o.Backend == nil {
		o.Backend = &MemBackend{}
	}
	c := &Cluster{
		o:       o,
		rng:     rand.New(rand.NewSource(o.Seed)),
		nodes:   map[uint64]*node{},
		blocked: map[[2]uint64]bool{},
	}
	c.chk = newChecker(c)
	return c
}

func (c *Cluster) fail(format string, args ...any) {
	panic(violation{fmt.Sprintf(format, args...)})
}

func (c *Cluster) tracef(format string, args ...any) {
	if c.o.Trace {
		c.trace = append(c.trace, fmt.Sprintf("%6d ", c.now)+fmt.Sprintf(format, args...))
		if len(c.trace) > 40000 {
			c.trace = append([]string(nil), c.trace[20000:]...)
		}
	}
}

func (c *Cluster) mixFP(vals ...uint64) {
	h := fnv.New64a()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], c.fp)
	h.Write(b[:])
	for _, v := range vals {
		binary.LittleEndian.PutUint64(b[:], v)
		h.Write(b[:])
	}
	c.fp = h.Sum64()
}

type quietLogger struct{ c *Cluster }

func (quietLogger) Debug(...any)          {}
func (quietLogger) Debugf(string, ...any) {}
func (quietLogger) Info(...any)           {}
func (quietLogger) Infof(string, ...any)  {}
func (l quietLogger) Warning(v ...any) {
	if l.c.o.Trace {
		l.c.tracef("raft warning: %s", fmt.Sprint(v...))
	}
}
func (l quietLogger) Warningf(f string, v ...any) {
	l.c.tracef("raft warning: "+f, v...)
}
func (l quietLogger) Error(v ...any) {
	if l.c.o.Trace {
		l.c.tracef("raft error: %s", fmt.Sprint(v...))
	}
}
func (l quietLogger) Errorf(f string, v ...any) { l.c.tracef("raft error: "+f, v...) }
func (quietLogger) Fatal(v ...any)              { panic(fmt.Sprint(v...)) }
func (quietLogger) Fatalf(f string, v ...any)   { panic(fmt.Sprintf(f, v...)) }
func (quietLogger) Panic(v ...any)              { panic(fmt.Sprint(v...)) }
func (quietLogger) Panicf(f string, v ...any)   { panic(fmt.Sprintf(f, v...)) }

func (c *Cluster) raftConfig(n *node) *raft.Config {
	o := c.o
	return &raft.Config{
		ID:                        n.id,
		ElectionTick:              o.ElectionTick,
		HeartbeatTick:             o.HeartbeatTick,
		Storage:                   nodeStorage{Storage: n.st, c: c, n: n},
		Applied:                   n.sm.applied,
		AsyncStorageWrites:        n.async,
		MaxSizePerMsg:             o.MaxSizePerMsg,
		MaxCommittedSizePerReady:  o.MaxCommittedPerReady,
		MaxUncommittedEntriesSize: o.MaxUncommitted,
		MaxInflightMsgs:           o.MaxInflight,
		MaxInflightBytes:          o.MaxInflightBytes,
		CheckQuorum:               o.CheckQuorum,
		PreVote:                   o.PreVote,
		ReadOnlyOption:            o.ReadOnly,
		Logger:                    quietLogger{c},
		DisableProposalForwarding: o.DisableForwarding,
		StepDownOnRemoval:         o.StepDownOnRemoval,
		ElectionRand:              c.rng,
	}
}

// bootstrapIndex is the index of the snapshot every initial node starts
// from (MemoryStorage cannot start from index 1).
const bootstrapIndex = 2

func (c *Cluster) bootstrap() {
	var cs = &pb.ConfState{}
	for i := 0; i < c.o.Voters+c.o.Learners; i++ {
		id := uint64(i + 1)
		if i < c.o.Voters {
			cs.Voters = append(cs.Voters, id)
		} else {
			cs.Learners = append(cs.Learners, id)
		}
	}
	c.nextID = uint64(c.o.Voters+c.o.Learners) + 1
	initial := smState{applied: bootstrapIndex}
	c.chk.digests[bootstrapIndex] = initial.digest
	c.chk.configs[bootstrapIndex] = cs
	snap := &pb.Snapshot{Data: initial.encode(), Metadata: &pb.SnapshotMetadata{Index: new(uint64(bootstrapIndex)), Term: new(uint64(1)), ConfState: cs}}
	for id := uint64(1); id < c.nextID; id++ {
		n := c.newNode(id)
		if err := n.st.Save(snap, nil, &pb.HardState{}); err != nil {
			c.fail("bootstrap: %v", err)
		}
		if err := n.st.Sync(); err != nil {
			c.fail("bootstrap: %v", err)
		}
		c.start(n)
	}
}

func (c *Cluster) newNode(id uint64) *node {
	st, err := c.o.Backend.Open(id)
	if err != nil {
		c.fail("open storage for %d: %v", id, err)
	}
	n := &node{id: id, st: st, async: c.o.Async || (c.o.MixedAsync && c.rng.Intn(2) == 0)}
	c.nodes[id] = n
	c.ids = append(c.ids, id)
	return n
}

// start brings n up from what its storage holds.
func (c *Cluster) start(n *node) {
	snap, err := n.st.Snapshot()
	if err != nil {
		c.fail("node %d: snapshot: %v", n.id, err)
	}
	n.sm = smState{}
	n.cs = &pb.ConfState{}
	if !raft.IsEmptySnap(snap) {
		n.sm = c.chk.decodeSnapshot(n, snap)
		n.cs = snap.GetMetadata().GetConfState()
	}
	rn, err := raft.NewRawNode(c.raftConfig(n))
	if err != nil {
		c.fail("node %d: NewRawNode: %v", n.id, err)
	}
	n.rn = rn
	n.up = true
	n.gen++
	c.chk.onStart(n)
	c.tracef("start %d gen %d applied %d", n.id, n.gen, n.sm.applied)
	c.mixFP(1, n.id, uint64(n.gen))
}

func (c *Cluster) crash(n *node) {
	st, err := c.o.Backend.Crash(n.id, n.st, c.rng)
	if err != nil {
		c.fail("node %d: crash: %v", n.id, err)
	}
	n.st = st
	n.up = false
	n.rn = nil
	n.appendQ, n.applyQ = nil, nil
	n.downUntil = c.now + int64(1+c.rng.Intn(5*c.o.ElectionTick))*tickEvery
	c.stats.Crashes++
	c.tracef("crash %d", n.id)
	c.mixFP(2, n.id)
}

func (c *Cluster) upNodes() []*node {
	var out []*node
	for _, id := range c.ids {
		if n := c.nodes[id]; n.up {
			out = append(out, n)
		}
	}
	return out
}

// leader returns the up node that believes itself leader in the highest
// term, if any.
func (c *Cluster) leader() *node {
	var best *node
	var term uint64
	for _, n := range c.upNodes() {
		st := n.rn.BasicStatus()
		if st.RaftState == raft.StateLeader && st.GetTerm() >= term {
			best, term = n, st.GetTerm()
		}
	}
	return best
}

func (c *Cluster) step(faults bool) {
	c.now++
	c.stats.Steps++
	for c.q.Len() > 0 && c.q[0].at <= c.now {
		c.deliver(heap.Pop(&c.q).(envelope))
	}
	for _, n := range c.upNodes() {
		if len(n.appendQ) > 0 && (c.healing || c.rng.Intn(2) == 0) {
			c.runAppend(n)
		}
		if n.up && len(n.applyQ) > 0 && (c.healing || c.rng.Intn(2) == 0) {
			c.runApply(n)
		}
	}
	if c.now%tickEvery == 0 {
		c.stats.Ticks++
		for _, n := range c.upNodes() {
			if c.o.ClockSkew && !c.healing && c.rng.Intn(20) == 0 {
				continue
			}
			n.rn.Tick()
		}
	}
	for _, id := range c.ids {
		n := c.nodes[id]
		if !n.up && !n.decommissioned && n.downUntil <= c.now {
			c.start(n)
			c.stats.Restarts++
		}
	}
	if c.healAt != 0 && c.now >= c.healAt {
		c.blocked = map[[2]uint64]bool{}
		c.healAt = 0
		c.tracef("partition healed")
	}
	if faults {
		c.randomEvent()
	}
	c.drain()
	if c.now%500 == 0 {
		c.chk.checkLogs()
	}
}

// drain handles Ready on every node until none has work.
func (c *Cluster) drain() {
	for round := 0; ; round++ {
		if round > 1000 {
			c.fail("Ready never drained")
		}
		busy := false
		for _, n := range c.upNodes() {
			if n.up && n.rn.HasReady() {
				busy = true
				c.handleReady(n)
			}
		}
		if !busy {
			return
		}
	}
}

func (c *Cluster) handleReady(n *node) {
	rd := n.rn.Ready()
	c.freezeReady(n, rd)
	c.chk.onReady(n, rd)
	if n.async {
		for _, m := range rd.Messages {
			switch m.GetTo() {
			case raft.LocalAppendThread:
				n.appendQ = append(n.appendQ, m)
			case raft.LocalApplyThread:
				n.applyQ = append(n.applyQ, m)
			default:
				c.send(n, m)
			}
		}
		c.readStates(n, rd.ReadStates)
		return
	}
	if c.o.SendBeforePersist {
		for _, m := range rd.Messages {
			c.send(n, m)
		}
	}
	if err := n.st.Save(rd.Snapshot, rd.Entries, rd.HardState); err != nil {
		c.fail("node %d: save: %v", n.id, err)
	}
	if c.maybeCrash(n) {
		return
	}
	if rd.MustSync {
		if err := n.st.Sync(); err != nil {
			c.fail("node %d: sync: %v", n.id, err)
		}
	}
	if !raft.IsEmptySnap(rd.Snapshot) {
		c.restore(n, rd.Snapshot)
	}
	if !c.o.SendBeforePersist {
		for _, m := range rd.Messages {
			c.send(n, m)
		}
	}
	if c.maybeCrash(n) {
		return
	}
	for _, e := range rd.CommittedEntries {
		c.apply(n, e)
	}
	c.readStates(n, rd.ReadStates)
	n.rn.Advance(rd)
	c.chk.observe(n)
}

// freezeReady records what rd hands out (frozen.go).
func (c *Cluster) freezeReady(n *node, rd raft.Ready) {
	c.freeze(n, "Ready hard state", rd.HardState)
	c.freeze(n, "Ready snapshot", rd.Snapshot)
	for _, e := range rd.Entries {
		c.freeze(n, "Ready entry", e)
	}
	for _, e := range rd.CommittedEntries {
		c.freeze(n, "Ready committed entry", e)
	}
	for _, m := range rd.Messages {
		c.freeze(n, "Ready message", m)
	}
}

// maybeCrash crashes n part way through handling its storage, now and then.
func (c *Cluster) maybeCrash(n *node) bool {
	if c.healing || c.o.CrashRate == 0 || c.rng.Float64() >= c.o.CrashRate*5 {
		return false
	}
	c.crash(n)
	return true
}

func hardStateOf(m *pb.Message) *pb.HardState {
	return &pb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit}
}

// runAppend is the local append thread: it persists the head of appendQ,
// then delivers its responses.
func (c *Cluster) runAppend(n *node) {
	m := n.appendQ[0]
	n.appendQ = n.appendQ[1:]
	snap := m.Snapshot
	if snap == nil {
		snap = &pb.Snapshot{}
	}
	if err := n.st.Save(snap, m.Entries, hardStateOf(m)); err != nil {
		c.fail("node %d: save: %v", n.id, err)
	}
	if c.maybeCrash(n) {
		return
	}
	if len(m.Responses) > 0 {
		if err := n.st.Sync(); err != nil {
			c.fail("node %d: sync: %v", n.id, err)
		}
	}
	if !raft.IsEmptySnap(snap) {
		c.restore(n, snap)
	}
	c.respond(n, m.Responses)
}

// runApply is the local apply thread.
func (c *Cluster) runApply(n *node) {
	m := n.applyQ[0]
	n.applyQ = n.applyQ[1:]
	for _, e := range m.Entries {
		c.apply(n, e)
	}
	c.respond(n, m.Responses)
}

func (c *Cluster) respond(n *node, resps []*pb.Message) {
	for _, r := range resps {
		if r.GetTo() == n.id {
			// A node that has applied its own removal no longer knows
			// itself as a peer.
			if err := n.rn.Step(r); err != nil && !errors.Is(err, raft.ErrStepPeerNotFound) {
				c.fail("node %d: stepping local response %s: %v", n.id, r.GetType(), err)
			}
		} else {
			c.send(n, r)
		}
	}
	c.chk.observe(n)
}

func (c *Cluster) readStates(n *node, rss []raft.ReadState) {
	for _, rs := range rss {
		c.chk.onReadState(n, rs)
	}
}

func (c *Cluster) restore(n *node, snap *pb.Snapshot) {
	if snap.GetMetadata().GetIndex() <= n.sm.applied {
		return
	}
	n.sm = c.chk.decodeSnapshot(n, snap)
	n.cs = snap.GetMetadata().GetConfState()
	c.stats.SnapRestore++
	c.tracef("node %d restored snapshot %d", n.id, snap.GetMetadata().GetIndex())
	c.chk.onConfig(n, snap.GetMetadata().GetIndex(), n.cs, false)
}

func (c *Cluster) apply(n *node, e *pb.Entry) {
	if e.GetIndex() <= n.sm.applied {
		return // covered by a snapshot restored meanwhile
	}
	if e.GetIndex() != n.sm.applied+1 {
		c.fail("node %d applied entry %d after %d", n.id, e.GetIndex(), n.sm.applied)
	}
	switch e.GetType() {
	case pb.EntryConfChange, pb.EntryConfChangeV2:
		var cc pb.ConfChangeI
		if e.GetType() == pb.EntryConfChange {
			var v1 = &pb.ConfChange{}
			if err := proto.Unmarshal(e.Data, v1); err != nil {
				c.fail("node %d: entry %d: %v", n.id, e.GetIndex(), err)
			}
			cc = v1
		} else {
			var v2 = &pb.ConfChangeV2{}
			if err := proto.Unmarshal(e.Data, v2); err != nil {
				c.fail("node %d: entry %d: %v", n.id, e.GetIndex(), err)
			}
			cc = v2
		}
		n.cs = n.rn.ApplyConfChange(cc)
		c.chk.onConfig(n, e.GetIndex(), n.cs, true)
	}
	n.sm.apply(e)
	c.chk.onApply(n, e)
}

func (c *Cluster) send(from *node, m *pb.Message) {
	c.stats.Sent++
	c.chk.onSend(from, m)
	// Every message a correct node sends must pass the receiver's checks.
	if err := raft.ValidateMessage(m); err != nil {
		c.fail("node %d sent a message that peers reject: %v", from.id, err)
	}
	if m.GetType() == pb.MsgSnap {
		c.stats.SnapshotsSent++
	}
	if !c.healing && c.rng.Float64() < c.o.Drop {
		c.dropped(m)
		return
	}
	c.enqueue(m)
	if !c.healing && c.rng.Float64() < c.o.Dup {
		c.stats.Duplicated++
		c.enqueue(m)
	}
}

// wire passes m through the wire encoding, as a network would: the copy
// shares no memory with what the sender keeps (raft assigns indexes to
// proposed entries in place, for one).
func (c *Cluster) wire(m *pb.Message) *pb.Message {
	b, err := proto.Marshal(m)
	if err != nil {
		c.fail("marshal %s: %v", m.GetType(), err)
	}
	var out = &pb.Message{}
	if err := proto.Unmarshal(b, out); err != nil {
		c.fail("unmarshal %s: %v", m.GetType(), err)
	}
	return out
}

func (c *Cluster) enqueue(m *pb.Message) {
	m = c.wire(m)
	d := int64(1 + c.rng.Intn(tickEvery))
	if !c.healing && c.rng.Float64() < c.o.LongDelay {
		d = int64(c.rng.Intn(3 * c.o.ElectionTick * tickEvery))
	}
	c.seq++
	heap.Push(&c.q, envelope{at: c.now + d, seq: c.seq, m: m})
}

func (c *Cluster) dropped(m *pb.Message) {
	c.stats.Dropped++
	from := c.nodes[m.GetFrom()]
	if from == nil || !from.up {
		return
	}
	switch {
	case m.GetType() == pb.MsgSnap:
		// The transport noticed. Now and then it claims success anyway;
		// raft must cope by probing.
		status := raft.SnapshotFailure
		if c.rng.Intn(4) == 0 {
			status = raft.SnapshotFinish
		}
		from.rn.ReportSnapshot(m.GetTo(), status)
	case c.rng.Intn(4) == 0:
		from.rn.ReportUnreachable(m.GetTo())
	}
}

func (c *Cluster) deliver(e envelope) {
	m := e.m
	to := c.nodes[m.GetTo()]
	if to == nil || !to.up || c.blocked[[2]uint64{m.GetFrom(), m.GetTo()}] {
		c.dropped(m)
		return
	}
	c.stats.Delivered++
	c.mixFP(3, m.GetFrom(), m.GetTo(), uint64(m.GetType()), m.GetTerm(), m.GetIndex())
	if c.o.Trace { // describing every message is costly
		c.tracef("deliver %s", raft.DescribeMessage(m, nil))
	}
	err := to.rn.Step(m)
	if err != nil {
		c.tracef("step: %v", err)
	}
	if m.GetType() == pb.MsgSnap {
		if from := c.nodes[m.GetFrom()]; from != nil && from.up {
			from.rn.ReportSnapshot(m.GetTo(), raft.SnapshotFinish)
		}
	}
	c.chk.observe(to)
}

func (c *Cluster) randomEvent() {
	r := c.rng.Float64()
	o := c.o
	for _, ev := range []struct {
		p float64
		f func()
	}{
		{o.ProposeRate, c.propose},
		{o.ReadRate, c.read},
		{o.ConfChangeRate, c.confChange},
		{o.SnapshotRate, c.snapshot},
		{o.CrashRate, c.crashRandom},
		{o.PartitionRate, c.partition},
		{o.TransferRate, c.transfer},
		{o.LeaderCrashRate, c.crashLeader},
		{o.LeaderIsolateRate, c.isolateLeader},
		{o.CampaignRate, c.campaign},
		{o.ForgetRate, c.forget},
	} {
		if r < ev.p {
			ev.f()
			return
		}
		r -= ev.p
	}
}

func (c *Cluster) randomUp() *node {
	up := c.upNodes()
	if len(up) == 0 {
		return nil
	}
	return up[c.rng.Intn(len(up))]
}

func (c *Cluster) propose() {
	n := c.randomUp()
	if n == nil {
		return
	}
	c.propN++
	data := []byte(fmt.Sprintf("p%d", c.propN))
	if c.rng.Intn(8) == 0 {
		data = append(data, make([]byte, c.rng.Intn(600))...)
	}
	c.stats.Proposals++
	err := n.rn.Propose(data)
	c.tracef("propose %q at %d: %v", data[:min(len(data), 8)], n.id, err)
}

func (c *Cluster) read() {
	n := c.randomUp()
	if n == nil {
		return
	}
	// A client whose read timed out retries it with the same request
	// context, possibly at another node. The read began at its first
	// attempt, and the checker keeps that bound. (Upstream raft matched
	// heartbeat acknowledgements to reads by this context, and a retry
	// could get a later read confirmed with stale acknowledgements.)
	if len(c.recentReads) > 0 && c.rng.Intn(4) == 0 {
		ctx := c.recentReads[c.rng.Intn(len(c.recentReads))]
		n.rn.ReadIndex(ctx)
		c.stats.ReadRetries++
		return
	}
	c.readN++
	ctx := binary.BigEndian.AppendUint64(nil, c.readN)
	c.chk.onReadIndex(n, ctx)
	n.rn.ReadIndex(ctx)
	c.stats.Reads++
	if len(c.recentReads) == 8 {
		c.recentReads = c.recentReads[1:]
	}
	c.recentReads = append(c.recentReads, ctx)
}

// snapshot snapshots a random node's applied state and compacts its log.
func (c *Cluster) snapshot() {
	n := c.randomUp()
	if n == nil || !c.takeSnapshot(n) {
		return
	}
	i := n.sm.applied
	first, _ := n.st.FirstIndex()
	if i >= first {
		j := i - uint64(c.rng.Intn(int(min(i-first+1, 30))))
		if err := n.st.Compact(j); err != nil && !ignorable(err) {
			c.fail("node %d: compact %d: %v", n.id, j, err)
		}
	}
}

// takeSnapshot records n's applied state as its storage's snapshot, if that
// is newer than the snapshot it has and is covered by the commit index in
// storage (a snapshot ahead of the stored commit index cannot be restarted
// from).
func (c *Cluster) takeSnapshot(n *node) bool {
	hs, _, _ := n.st.InitialState()
	i := n.sm.applied
	if i > hs.GetCommit() {
		return false
	}
	snap, _ := n.st.Snapshot()
	if i <= snap.GetMetadata().GetIndex() {
		return false
	}
	if err := n.st.CreateSnapshot(i, n.cs, n.sm.encode()); err != nil {
		if ignorable(err) {
			return false
		}
		c.fail("node %d: create snapshot %d: %v", n.id, i, err)
	}
	c.stats.Snapshots++
	c.tracef("node %d snapshot at %d", n.id, i)
	return true
}

// nodeStorage is the raft.Storage a RawNode reads. When raft asks for a
// snapshot to send, it snapshots the applied state first, as applications
// do: an older snapshot can predate the configuration that added the node
// it is meant for, which would then refuse it.
type nodeStorage struct {
	Storage
	c *Cluster
	n *node
}

func (s nodeStorage) Snapshot() (*pb.Snapshot, error) {
	if s.n.st == s.Storage && s.n.up {
		s.c.takeSnapshot(s.n)
	}
	snap, err := s.Storage.Snapshot()
	s.c.freeze(s.n, "snapshot read from storage", snap)
	return snap, err
}

// The rest of what raft reads from storage is recorded too: raft must not
// change it (frozen.go).

func (s nodeStorage) InitialState() (*pb.HardState, *pb.ConfState, error) {
	hs, cs, err := s.Storage.InitialState()
	s.c.freeze(s.n, "hard state read from storage", hs)
	s.c.freeze(s.n, "configuration read from storage", cs)
	return hs, cs, err
}

func (s nodeStorage) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	ents, err := s.Storage.Entries(lo, hi, maxSize)
	for _, e := range ents {
		s.c.freeze(s.n, "entry read from storage", e)
	}
	return ents, err
}

func (c *Cluster) crashRandom() {
	if n := c.randomUp(); n != nil {
		c.crash(n)
	}
}

func (c *Cluster) crashLeader() {
	if l := c.leader(); l != nil {
		c.tracef("crash leader %d", l.id)
		c.crash(l)
	}
}

// isolateLeader cuts the leader off from the other nodes for a while, now
// and then with one follower, to which it keeps appending entries that the
// majority will replace: stale log tails are where many bugs hide.
func (c *Cluster) isolateLeader() {
	l := c.leader()
	if l == nil {
		return
	}
	side := map[uint64]bool{l.id: true}
	if c.rng.Intn(2) == 0 {
		var others []uint64
		for _, id := range c.ids {
			if n := c.nodes[id]; id != l.id && n.up {
				others = append(others, id)
			}
		}
		if len(others) >= 2 {
			side[others[c.rng.Intn(len(others))]] = true
		}
	}
	c.stats.Partitions++
	c.blocked = map[[2]uint64]bool{}
	for _, a := range c.ids {
		for _, b := range c.ids {
			if side[a] != side[b] {
				c.blocked[[2]uint64{a, b}] = true
			}
		}
	}
	c.healAt = c.now + int64(1+c.rng.Intn(6*c.o.ElectionTick))*tickEvery
	c.tracef("partition: isolate leader %d with %v", l.id, side)
}

func (c *Cluster) partition() {
	c.stats.Partitions++
	c.blocked = map[[2]uint64]bool{}
	var live []uint64
	for _, id := range c.ids {
		if !c.nodes[id].decommissioned {
			live = append(live, id)
		}
	}
	block := func(a, b uint64) { c.blocked[[2]uint64{a, b}] = true }
	switch c.rng.Intn(3) {
	case 0: // isolate one node
		x := live[c.rng.Intn(len(live))]
		for _, y := range live {
			if y != x {
				block(x, y)
				block(y, x)
			}
		}
		c.tracef("partition: isolate %d", x)
	case 1: // split in two
		side := map[uint64]bool{}
		for _, id := range live {
			side[id] = c.rng.Intn(2) == 0
		}
		for _, a := range live {
			for _, b := range live {
				if side[a] != side[b] {
					block(a, b)
				}
			}
		}
		c.tracef("partition: split %v", side)
	default: // one-way links
		for _, a := range live {
			for _, b := range live {
				if a != b && c.rng.Intn(4) == 0 {
					block(a, b)
				}
			}
		}
		c.tracef("partition: one-way %v", c.blocked)
	}
	c.healAt = c.now + int64(1+c.rng.Intn(10*c.o.ElectionTick))*tickEvery
}

func (c *Cluster) transfer() {
	l := c.leader()
	if l == nil {
		return
	}
	cs := confStateOf(l.rn.Status().Config)
	if len(cs.Voters) == 0 {
		return
	}
	to := cs.Voters[c.rng.Intn(len(cs.Voters))]
	c.stats.Transfers++
	l.rn.TransferLeader(to)
	c.tracef("transfer leadership %d -> %d", l.id, to)
}

func (c *Cluster) campaign() {
	if n := c.randomUp(); n != nil {
		c.stats.Campaigns++
		_ = n.rn.Campaign()
		c.tracef("campaign %d", n.id)
	}
}

func (c *Cluster) forget() {
	if n := c.randomUp(); n != nil {
		_ = n.rn.ForgetLeader()
	}
}

// confChange proposes a configuration change that is valid against the
// leader's active configuration, as an application that serializes its
// configuration changes would.
func (c *Cluster) confChange() {
	l := c.leader()
	if l == nil {
		return
	}
	cs := confStateOf(l.rn.Status().Config)
	var cc pb.ConfChangeI
	if len(cs.VotersOutgoing) > 0 {
		if cs.GetAutoLeave() {
			return
		}
		cc = &pb.ConfChangeV2{}
	} else {
		changes := c.pickChanges(cs)
		if len(changes) == 0 {
			return
		}
		if len(changes) == 1 && c.rng.Intn(2) == 0 {
			cc = &pb.ConfChange{Type: changes[0].Type, NodeId: changes[0].NodeId}
		} else {
			cc = &pb.ConfChangeV2{
				Transition: pb.ConfChangeTransition(pick(c.rng, pb.ConfChangeTransitionAuto, pb.ConfChangeTransitionJointImplicit, pb.ConfChangeTransitionJointExplicit)).Enum(),
				Changes:    changes,
			}
		}
	}
	if err := raft.ValidateConfChange(cs, cc.AsV2()); err != nil {
		return
	}
	for _, ch := range cc.AsV2().Changes {
		if ch.GetType() != pb.ConfChangeRemoveNode && ch.GetNodeId() >= c.nextID {
			c.nextID = ch.GetNodeId() + 1 // reserve the ID even if this never commits
		}
	}
	c.stats.ConfChanges++
	err := l.rn.ProposeConfChange(cc)
	if c.o.Trace {
		c.tracef("conf change at %d: %s (%v)", l.id, confchange.Describe(cc.AsV2().Changes...), err)
	}
}

func (c *Cluster) pickChanges(cs *pb.ConfState) []*pb.ConfChangeSingle {
	k := 1
	if c.rng.Intn(3) == 0 {
		k = 2 + c.rng.Intn(2)
	}
	used := map[uint64]bool{}
	next := c.nextID
	var out []*pb.ConfChangeSingle
	voters := len(cs.Voters)
	for len(out) < k {
		var ch = &pb.ConfChangeSingle{}
		switch c.rng.Intn(6) {
		case 0, 1: // add a new voter or learner
			if next > uint64(c.o.MaxNodes) {
				continue
			}
			ch = &pb.ConfChangeSingle{Type: pb.ConfChangeType(pick(c.rng, pb.ConfChangeAddNode, pb.ConfChangeAddLearnerNode)).Enum(), NodeId: new(next)}
			next++
		case 2: // promote a learner
			if len(cs.Learners) == 0 {
				continue
			}
			ch = &pb.ConfChangeSingle{Type: pb.ConfChangeAddNode.Enum(), NodeId: new(cs.Learners[c.rng.Intn(len(cs.Learners))])}
		case 3: // demote a voter
			ch = &pb.ConfChangeSingle{Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: new(cs.Voters[c.rng.Intn(len(cs.Voters))])}
		case 4: // remove a voter
			ch = &pb.ConfChangeSingle{Type: pb.ConfChangeRemoveNode.Enum(), NodeId: new(cs.Voters[c.rng.Intn(len(cs.Voters))])}
		default: // remove a learner
			if len(cs.Learners) == 0 {
				continue
			}
			ch = &pb.ConfChangeSingle{Type: pb.ConfChangeRemoveNode.Enum(), NodeId: new(cs.Learners[c.rng.Intn(len(cs.Learners))])}
		}
		if used[ch.GetNodeId()] {
			if c.rng.Intn(4) != 0 {
				continue // a few repeated IDs, to exercise validation
			}
		}
		used[ch.GetNodeId()] = true
		if ch.GetType() == pb.ConfChangeAddNode {
			voters++
		}
		if voters > 7 {
			return nil
		}
		out = append(out, ch)
	}
	return out
}

// canDecommission reports whether a careful operator would shut down node
// id for good: it has been removed, and every voter of the latest
// configuration has applied its removal. Shutting it down earlier can
// leave the voters without the vote of a node they still count.
func (c *Cluster) canDecommission(id uint64) bool {
	n := c.nodes[id]
	at, removed := c.chk.removed[id]
	if !removed || n.decommissioned {
		return false
	}
	latest := c.chk.configs[c.chk.latestConfigIndex()]
	for _, v := range append(append([]uint64(nil), latest.Voters...), latest.VotersOutgoing...) {
		if vn := c.nodes[v]; vn == nil || !vn.up || vn.sm.applied < at {
			return false
		}
	}
	return true
}

// heal stops all faults, restarts every member and waits for the cluster
// to commit and apply a new entry everywhere.
func (c *Cluster) heal() {
	c.healing = true
	c.blocked = map[[2]uint64]bool{}
	c.healAt = 0
	start := c.now
	deadline := c.now + int64(c.o.HealTicks)*tickEvery
	var lastPropose, lastLeave int64 = -1 << 40, -1 << 40
	for c.now < deadline {
		for _, id := range c.ids {
			n := c.nodes[id]
			if c.canDecommission(id) {
				n.decommissioned, n.up, n.rn = true, false, nil
				n.appendQ, n.applyQ = nil, nil
				c.tracef("decommission %d", id)
			} else if !n.up && !n.decommissioned {
				n.downUntil = c.now
			}
		}
		c.step(false)
		l := c.leader()
		if l == nil {
			continue
		}
		cs := confStateOf(l.rn.Status().Config)
		if len(cs.VotersOutgoing) > 0 && !cs.GetAutoLeave() && c.now-lastLeave > 20*tickEvery {
			lastLeave = c.now
			_ = l.rn.ProposeConfChange(&pb.ConfChangeV2{})
		}
		if c.chk.probeIndex == 0 && c.now-lastPropose > 30*tickEvery {
			lastPropose = c.now
			c.propN++
			_ = l.rn.Propose([]byte(fmt.Sprintf("probe%d", c.propN)))
		}
		if c.chk.probeIndex == 0 || len(cs.VotersOutgoing) > 0 {
			continue
		}
		done := true
		for _, id := range append(append([]uint64(nil), cs.Voters...), cs.Learners...) {
			n := c.nodes[id]
			if n == nil || !n.up || n.sm.applied < c.chk.probeIndex {
				done = false
				break
			}
		}
		if done {
			c.stats.HealTicks = (c.now - start) / tickEvery
			return
		}
	}
	var b strings.Builder
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.up {
			st := n.rn.Status()
			last, _ := n.st.LastIndex()
			lastTerm, _ := n.st.Term(last)
			fmt.Fprintf(&b, "\n  node %d: %s term %d vote %d lead %d log %d/%d commit %d applied %d config %s", id, st.RaftState, st.GetTerm(), st.GetVote(), st.Lead, lastTerm, last, st.GetCommit(), n.sm.applied, st.Config)
		} else {
			fmt.Fprintf(&b, "\n  node %d: down (decommissioned %v)", id, n.decommissioned)
		}
	}
	c.fail("no progress %d ticks after healing (probe index %d):%s", c.o.HealTicks, c.chk.probeIndex, b.String())
}

func (c *Cluster) finalChecks() {
	c.checkAllFrozen()
	c.chk.checkLogs()
	c.stats.Leaders = len(c.chk.leaders)
	c.stats.Applied = c.chk.maxApplied
	c.stats.NodesAdded = len(c.ids) - c.o.Voters - c.o.Learners
	c.stats.NodesRemoved = len(c.chk.removed)
}

// smState is the replicated state machine: a digest of every entry applied.
type smState struct {
	applied uint64
	digest  uint64
}

func (s *smState) apply(e *pb.Entry) {
	h := fnv.New64a()
	var b [8]byte
	for _, v := range []uint64{s.digest, e.GetIndex(), e.GetTerm(), uint64(e.GetType())} {
		binary.LittleEndian.PutUint64(b[:], v)
		h.Write(b[:])
	}
	h.Write(e.Data)
	s.digest = h.Sum64()
	s.applied = e.GetIndex()
}

func (s smState) encode() []byte {
	b := binary.BigEndian.AppendUint64(nil, s.applied)
	return binary.BigEndian.AppendUint64(b, s.digest)
}

func sortedKeys[V any](m map[uint64]V) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func confStateOf(cfg tracker.Config) *pb.ConfState {
	pt := tracker.ProgressTracker{Config: cfg}
	return pt.ConfState()
}
