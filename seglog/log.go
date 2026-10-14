// Copyright 2026 The raftseg authors.

// Package seglog is a durable log for many raft groups sharing one node.
//
// Every change to the log's state (entries, HardStates, snapshots, conf
// states, group removals) is a record appended to one segmented log in
// pebble/record's chunk format (32 KiB blocks, CRC'd chunks). Each group
// commit ends with a commit marker and is made durable by fdatasync; commits
// are pipelined, so while one batch is being synced the next is framed and
// written.
//
// There is no separate key-value store. Where each entry lives is kept by a
// per-segment entry index (in memory for the active segment, in a sidecar
// file for sealed ones); which segment holds which entries of a group, and
// their terms, are kept in memory. A checkpoint file of all of this is
// written at every segment seal, so after a crash Open replays at most what
// followed it. GC tracks which segments still hold live entries, copies the
// few live entries out of mostly-dead segments, and deletes dead segments
// whole.
package seglog

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
)

// Options configures a Log. The zero value is usable.
type Options struct {
	// FS is the filesystem for all files. Default OSFS.
	FS FS
	// SegmentSize is the size at which the active segment is sealed and a new
	// one started. Default 64 MiB, minimum 64 KiB, maximum 1 GiB.
	SegmentSize int64
	// NoPreallocate disables reserving SegmentSize bytes for new segments.
	NoPreallocate bool
	// NoSync skips the fsync of segment writes. Unsafe across OS crashes or
	// power loss; intended for tests and benchmarks.
	NoSync bool
	// PipelineDepth is how many group commits may be written ahead of the
	// fsync that makes them durable. Default 4; 1 disables pipelining.
	PipelineDepth int
	// CacheEntries is the number of most recent entries kept in memory per
	// group. Default 1024; negative disables the cache.
	CacheEntries int
	// CacheBytes bounds the memory of all group caches together.
	// Default 64 MiB.
	CacheBytes int64
	// MaxBatchUpdates and MaxBatchBytes bound one group commit.
	// Defaults 8192 updates and 16 MiB of entry data (at most 1 GiB).
	MaxBatchUpdates int
	MaxBatchBytes   int64
	// CheckpointInterval, if positive, also writes a checkpoint this often
	// (one is always written when a segment is sealed, before GC deletes
	// segments, and at Close). Open replays what followed the last one.
	CheckpointInterval time.Duration
	// IndexCacheBytes bounds the memory of the entry indexes of sealed
	// segments kept in memory. Default 64 MiB.
	IndexCacheBytes int64
	// NoAutoGC disables running GC whenever a segment is sealed.
	NoAutoGC bool
	// CleanAfterSegments is how many segments behind the active one a sealed
	// segment must be before GC copies its live entries forward. Default 2.
	CleanAfterSegments int
	// CleanMaxLiveRatio: only segments whose live fraction is at most this
	// are cleaned (mostly-live segments have nothing to reclaim). Default
	// 0.5; negative disables cleaning (dead segments are still deleted).
	CleanMaxLiveRatio float64
	// CleanBudgetBytes caps the bytes one GC run copies. Default 64 MiB.
	CleanBudgetBytes int64
	// SnapshotInlineBytes: snapshot data larger than this is kept in its own
	// checksummed file; smaller data is kept in memory and in checkpoints.
	// Default 4 KiB, at most 1 MiB.
	SnapshotInlineBytes int
	// DiscardDamagedTail makes Open discard, instead of refusing to open, the
	// newest commits of the log when they are damaged in a way an interrupted
	// write does not explain (non-zero bytes failing their checksum, rather
	// than zeroes or a short file). No later write vouches that those commits
	// were synced, but they may have been: discarding them can lose an
	// acknowledged raft entry or vote. Damage that looks like an interrupted
	// write is always discarded, and reported in RecoveryStats.Tail.
	DiscardDamagedTail bool
	// ScrubOnOpen verifies every segment, snapshot and indexed entry before
	// Open returns.
	ScrubOnOpen bool

	// testZeroKey creates the log with checksum key 0 (pebble/record's own
	// checksums), as the format did before keys: tests use it to show what
	// the key prevents.
	testZeroKey bool

	// testDropIndexAfter, when > 0, stops writing checkpoints and index
	// sidecars after that many group commits while still writing and syncing
	// segments, simulating a crash that loses them: Open must replay
	// everything since.
	testDropIndexAfter int64
}

func (o *Options) setDefaults() {
	if o.FS == nil {
		o.FS = OSFS
	}
	if o.SegmentSize <= 0 {
		o.SegmentSize = 64 << 20
	}
	o.SegmentSize = min(max(o.SegmentSize, 2*blockSize), maxSegmentBytes)
	if o.PipelineDepth <= 0 {
		o.PipelineDepth = 4
	}
	if o.CacheEntries == 0 {
		o.CacheEntries = 1024
	}
	if o.CacheBytes <= 0 {
		o.CacheBytes = 64 << 20
	}
	if o.MaxBatchUpdates <= 0 {
		o.MaxBatchUpdates = 8192
	}
	if o.MaxBatchBytes <= 0 {
		o.MaxBatchBytes = 16 << 20
	}
	o.MaxBatchBytes = min(o.MaxBatchBytes, maxSegmentBytes)
	if o.IndexCacheBytes <= 0 {
		o.IndexCacheBytes = 64 << 20
	}
	if o.CleanAfterSegments <= 0 {
		o.CleanAfterSegments = 2
	}
	if o.CleanMaxLiveRatio == 0 {
		o.CleanMaxLiveRatio = 0.5 // negative disables cleaning
	}
	if o.CleanBudgetBytes <= 0 {
		o.CleanBudgetBytes = 64 << 20
	}
	if o.SnapshotInlineBytes <= 0 {
		o.SnapshotInlineBytes = 4 << 10
	}
	o.SnapshotInlineBytes = min(o.SnapshotInlineBytes, 1<<20)
}

// Update is what one raft group needs persisted from one Ready.
type Update struct {
	Group     uint64
	HardState raftpb.HardState
	Entries   []raftpb.Entry
	Snapshot  raftpb.Snapshot
}

var (
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("seglog: closed")
	// ErrCorrupt wraps errors about data that fails integrity checks.
	ErrCorrupt = errors.New("seglog: corruption")
	// ErrGroupRemoved is returned by a Storage whose group was removed.
	ErrGroupRemoved = errors.New("seglog: group removed")
	// ErrInvalidUpdate wraps errors about a Save that raft would never
	// issue. The Save changes nothing.
	ErrInvalidUpdate = errors.New("seglog: invalid update")
)

// Log is a multi-group raft log. It is safe for concurrent use, including
// reads of a group while that group is being saved.
type Log struct {
	fs                   FS
	dir, segDir, snapDir string
	opts                 Options
	lock                 io.Closer

	// mu guards groups and their published state. Readers that go to disk
	// wait while a group's version is odd (a change is in flight) and retry
	// if it changed underneath them.
	mu         sync.RWMutex
	cond       *sync.Cond
	groups     map[uint64]*group
	cacheBytes int64

	filesMu  sync.Mutex
	segs     map[uint64]*segState // published segment state
	idxBytes int64                // memory of cached sealed-segment indexes
	idxTick  uint64

	closeMu sync.RWMutex
	closed  bool

	// key is the checksum key of every chunk (see chunkCRC); fixed for the
	// log's life, kept in its checkpoints.
	key uint32

	// Owned by the writer goroutine after Open.
	active    uint64
	wf        File
	w         *segWriter
	wsegCount uint64 // records written to the active segment
	wsegEnts  uint64 // entry and relocation records among them
	seq       uint64 // last commit sequence written
	ckptGen   uint64
	live      map[uint64]uint64
	liveSeen  map[uint64]uint64 // live count of each sealed segment at the last GC
	lastJob   *job
	inGC      bool
	buf       []byte

	durable        atomic.Pointer[durablePos] // set by the syncer
	lastCheckpoint atomic.Int64               // unix nanos
	commits        atomic.Int64               // group commits completed
	stats          counters
	recovery       RecoveryStats

	reqs     chan *request
	syncq    chan *job
	quit     chan struct{}
	done     chan struct{}
	syncDone chan struct{}
	failMu   sync.Mutex
	failed   error
}

type counters struct {
	commits, fsyncs, records, bytes   atomic.Uint64
	cleanedSegments, relocatedEntries atomic.Uint64
	relocatedBytes, gcSegments        atomic.Uint64
	checkpoints, checkpointBytes      atomic.Uint64
	sidecarBytes, indexRebuilds       atomic.Uint64
}

// durablePos is how far a segment is known to be fsynced.
type durablePos struct {
	seg uint64
	end int64
}

type segState struct {
	meta    segMeta
	rf      File // read handle, opened lazily
	refs    int
	deleted bool
	idx     *segIndex // entry index; nil if not loaded
	idxUsed uint64    // for LRU eviction
	pinned  bool      // the active segment's index is never evicted
}

type group struct {
	// Published state, guarded by Log.mu.
	meta       groupMeta
	hs         raftpb.HardState
	cs         raftpb.ConfState
	snap       *snapInfo
	pruns      []segRun  // never mutated once published
	pterms     []termRun // never mutated once published
	version    uint64    // odd while a change is in flight
	cache      []raftpb.Entry
	cacheBytes int64
	removed    bool
	// born is where the group's first entry record is (zero until it has
	// one). A group id can be reused after RemoveGroup, by a raft group with
	// another history; records of the id written before born belong to a
	// removed group, and may hold the same index and term with other data.
	// Set once, by the writer under Log.mu or by recovery.
	born position

	// Writer-owned state, including commits not yet published.
	wmeta groupMeta
	whs   raftpb.HardState
	wsnap *snapInfo
	runs  []segRun  // which segment holds which index range
	terms []termRun // term of every entry in [first, last]
}

type request struct {
	updates []Update
	enc     []encodedUpdate
	bound   *group // set by Storage.Save: fail if this group was removed
	fn      func() error
	done    chan error
}

// encodedUpdate holds an Update's records, marshalled by the calling
// goroutine so the writer only frames and copies bytes.
type encodedUpdate struct {
	ents  [][]byte // one record per entry
	hsRec []byte   // HardState record, nil if empty
	hsVal raftpb.HardState
}

// MaxEntryBytes is the largest encoded entry Save accepts.
const MaxEntryBytes = 256 << 20

func encodeUpdates(us []Update) ([]encodedUpdate, error) {
	out := make([]encodedUpdate, len(us))
	total := 0
	for i, u := range us {
		if len(u.Entries) > 0 {
			out[i].ents = make([][]byte, len(u.Entries))
		}
		for k := range u.Entries {
			data, err := u.Entries[k].Marshal()
			if err != nil {
				return nil, err
			}
			if len(data) > MaxEntryBytes {
				return nil, fmt.Errorf("seglog: group %d entry %d is %d bytes, limit %d", u.Group, u.Entries[k].Index, len(data), MaxEntryBytes)
			}
			out[i].ents[k] = encodeRecord(make([]byte, 0, len(data)+11), recEntry, u.Group, data)
			total += len(data)
		}
		if err := out[i].setHardState(u.Group, u.HardState); err != nil {
			return nil, err
		}
	}
	if total > maxSegmentBytes {
		return nil, fmt.Errorf("seglog: one Save of %d bytes exceeds the limit of %d", total, maxSegmentBytes)
	}
	return out, nil
}

func (e *encodedUpdate) setHardState(group uint64, hs raftpb.HardState) error {
	e.hsRec, e.hsVal = nil, hs
	if raft.IsEmptyHardState(hs) {
		return nil
	}
	data, err := hs.Marshal()
	if err != nil {
		return err
	}
	e.hsRec = encodeRecord(nil, recHardState, group, data)
	return nil
}

// job is one group commit travelling from the writer to the syncer.
type job struct {
	st       *stage
	reqs     []*request
	errs     []error
	f        File
	seg      uint64
	segMeta  segMeta
	idx      *segIndex   // the active segment's index
	vis      []visUpdate // extents to make visible once published
	after    []func()    // run once the commit is durable and published
	finished chan struct{}
}

type visUpdate struct {
	e *extent
	n int
}

func (j *job) add(r *request, err error) {
	j.reqs = append(j.reqs, r)
	j.errs = append(j.errs, err)
}

// Open opens or creates a log in dir and recovers it.
func Open(dir string, opts Options) (*Log, error) {
	opts.setDefaults()
	fs := opts.FS
	l := &Log{
		fs:       fs,
		dir:      dir,
		segDir:   fs.PathJoin(dir, "segments"),
		snapDir:  fs.PathJoin(dir, "snapshots"),
		opts:     opts,
		groups:   map[uint64]*group{},
		segs:     map[uint64]*segState{},
		live:     map[uint64]uint64{},
		liveSeen: map[uint64]uint64{},
		reqs:     make(chan *request),
		syncq:    make(chan *job, opts.PipelineDepth),
		quit:     make(chan struct{}),
		done:     make(chan struct{}),
		syncDone: make(chan struct{}),
	}
	l.cond = sync.NewCond(l.mu.RLocker())
	for _, d := range []string{l.segDir, l.snapDir} {
		if err := fs.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	// Make the directories themselves durable, not just their contents.
	for _, d := range []string{dir, fs.PathDir(dir)} {
		if err := syncDir(fs, d); err != nil {
			return nil, err
		}
	}
	lock, err := fs.Lock(fs.PathJoin(dir, "LOCK"))
	if err != nil {
		return nil, err
	}
	l.lock = lock
	if err := l.recover(); err != nil {
		l.closeFiles()
		if l.wf != nil {
			l.wf.Close()
		}
		lock.Close()
		return nil, err
	}
	l.lastCheckpoint.Store(time.Now().UnixNano())
	go l.syncer()
	go l.run()
	if opts.ScrubOnOpen {
		rep, err := l.Scrub()
		if err == nil && len(rep.Problems) > 0 {
			err = fmt.Errorf("%w: scrub found %d problems, first: %s", ErrCorrupt, len(rep.Problems), rep.Problems[0])
		}
		if err != nil {
			l.Close()
			return nil, err
		}
	}
	return l, nil
}

// enter guards public reads against Close.
func (l *Log) enter() error {
	l.closeMu.RLock()
	if l.closed {
		l.closeMu.RUnlock()
		return ErrClosed
	}
	return nil
}

func (l *Log) exit() { l.closeMu.RUnlock() }

// Close stops the writer, waits for in-flight commits and reads, writes a
// checkpoint and closes all files. If the log had failed, Close returns that
// failure. Later calls return nil.
func (l *Log) Close() error { return l.shutdown(true) }

// crash simulates a process crash: nothing buffered is flushed or synced.
func (l *Log) crash() { l.shutdown(false) }

func (l *Log) shutdown(clean bool) error {
	l.closeMu.Lock()
	if l.closed {
		l.closeMu.Unlock()
		return nil
	}
	l.closed = true
	l.closeMu.Unlock()
	close(l.quit)
	<-l.done
	var err error
	if l.wf != nil {
		if clean && l.failure() == nil {
			if e := l.w.flush(); e != nil {
				err = e
			}
			if e := l.wf.Sync(); e != nil && err == nil {
				err = e
			}
			// The writer and syncer have stopped: the state is stable. Save
			// the active segment's index too, so Open need not rescan it.
			if err == nil && !l.dropIndex() {
				if x := l.activeIndex(); x != nil {
					err = l.writeSidecar(l.active, x, l.w.pos())
				}
				if err == nil {
					err = l.checkpoint(nil)
				}
			}
		}
		l.wf.Close()
		l.wf = nil
	}
	l.closeFiles()
	if e := l.lock.Close(); e != nil && err == nil {
		err = e
	}
	if f := l.failure(); f != nil && clean && err == nil {
		err = f
	}
	return err
}

func (l *Log) closeFiles() {
	l.filesMu.Lock()
	defer l.filesMu.Unlock()
	for _, s := range l.segs {
		if s.rf != nil {
			s.rf.Close()
			s.rf = nil
		}
	}
}

// Save durably persists updates for one or more groups. Concurrent Save calls
// are coalesced into one segment fsync (group commit). When Save returns nil
// the updates are durable and visible to readers. An I/O error is sticky:
// the Log must then be closed and reopened.
func (l *Log) Save(updates ...Update) error {
	return l.save(nil, updates)
}

func (l *Log) save(bound *group, updates []Update) error {
	if len(updates) == 0 {
		return nil
	}
	enc, err := encodeUpdates(updates)
	if err != nil {
		return err
	}
	return l.submit(&request{updates: updates, enc: enc, bound: bound, done: make(chan error, 1)})
}

// Sync returns once every change made before it is durable: in particular
// snapshots made by CreateSnapshot, which otherwise become durable with the
// next group commit. (Compactions become durable with the next checkpoint;
// a crash before it only keeps more entries.) It returns the log's failure,
// if it has failed.
func (l *Log) Sync() error {
	return l.exec(func() error { return nil })
}

// exec runs fn on the writer goroutine once every earlier commit has been
// published, serialised with later commits.
func (l *Log) exec(fn func() error) error {
	return l.submit(&request{fn: fn, done: make(chan error, 1)})
}

func (l *Log) submit(r *request) error {
	select {
	case l.reqs <- r:
		return <-r.done
	case <-l.quit:
		return ErrClosed
	}
}

func (l *Log) fail(err error) error {
	l.failMu.Lock()
	defer l.failMu.Unlock()
	if l.failed == nil {
		l.failed = fmt.Errorf("seglog: log failed, reopen required: %w", err)
	}
	return l.failed
}

func (l *Log) failure() error {
	l.failMu.Lock()
	defer l.failMu.Unlock()
	return l.failed
}

func updatesBytes(us []Update) int64 {
	var n int64
	for _, u := range us {
		for i := range u.Entries {
			n += int64(len(u.Entries[i].Data)) + 32
		}
		n += int64(len(u.Snapshot.Data))
	}
	return n
}

// run is the writer: it drains every pending request, frames and writes a
// group commit, and hands it to the syncer without waiting for the fsync.
func (l *Log) run() {
	defer func() {
		l.drain()
		close(l.syncq)
		<-l.syncDone
		close(l.done)
	}()
	for {
		var first *request
		select {
		case first = <-l.reqs:
		case <-l.quit:
			return
		}
		if first.fn != nil {
			l.drain()
			first.done <- l.runFn(first.fn)
			l.maybeCheckpoint()
			continue
		}
		batch := []*request{first}
		n, size := len(first.updates), updatesBytes(first.updates)
		var fnReq *request
	drain:
		for n < l.opts.MaxBatchUpdates && size < l.opts.MaxBatchBytes {
			select {
			case r := <-l.reqs:
				if r.fn != nil {
					fnReq = r
					break drain
				}
				batch = append(batch, r)
				n += len(r.updates)
				size += updatesBytes(r.updates)
			default:
				break drain
			}
		}
		l.process(batch)
		if fnReq != nil {
			l.drain()
			fnReq.done <- l.runFn(fnReq.fn)
		}
		l.maybeCheckpoint()
	}
}

// maybeCheckpoint writes a periodic checkpoint when CheckpointInterval has
// passed since the last one.
func (l *Log) maybeCheckpoint() {
	iv := l.opts.CheckpointInterval
	if iv <= 0 || time.Now().UnixNano()-l.lastCheckpoint.Load() < int64(iv) || l.dropIndex() || l.failure() != nil {
		return
	}
	l.drain()
	if l.failure() == nil {
		if err := l.checkpoint(nil); err != nil {
			l.fail(err)
		}
	}
}

func (l *Log) runFn(fn func() error) error {
	if err := l.failure(); err != nil {
		return err
	}
	return fn()
}

// drain waits until every dispatched commit has been synced and published.
func (l *Log) drain() {
	if l.lastJob != nil {
		<-l.lastJob.finished
		l.lastJob = nil
	}
}

// ---- staging ----

type stage struct {
	records    uint64
	entries    uint64 // entry and relocation records among records
	firstStart int64
	deltas     map[uint64]*delta
	order      []uint64
	idx        []pendingIdx // entry records to add to the segment index
}

type pendingIdx struct {
	g, index   uint64
	start, end int64
}

// delta is what one commit changes in one group.
type delta struct {
	id       uint64
	g        *group
	meta     groupMeta
	hs       *raftpb.HardState
	cs       *raftpb.ConfState
	snap     *snapInfo
	reset    bool // a received snapshot replaced the log
	remove   bool
	appended [][]raftpb.Entry
	runs     []segRun  // published with the commit (set by dispatch)
	terms    []termRun // likewise
}

func newStage() *stage { return &stage{deltas: map[uint64]*delta{}} }

func (st *stage) delta(l *Log, id uint64) *delta {
	if d, ok := st.deltas[id]; ok {
		return d
	}
	g := l.groupOrCreate(id)
	d := &delta{id: id, g: g, meta: g.wmeta}
	st.deltas[id] = d
	st.order = append(st.order, id)
	return d
}

func newGroup() *group {
	return &group{meta: groupMeta{first: 1}, wmeta: groupMeta{first: 1}}
}

func (l *Log) groupOrCreate(id uint64) *group {
	l.mu.RLock()
	g, ok := l.groups[id]
	l.mu.RUnlock()
	if ok {
		return g
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if g, ok = l.groups[id]; !ok {
		g = newGroup()
		l.groups[id] = g
	}
	return g
}

func (l *Log) isRemoved(g *group) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return g.removed
}

// process stages a batch of requests into one or more group commits. A
// request carrying a snapshot starts a new commit, once every earlier one is
// published, so the snapshot's group has no other change staged before it.
func (l *Log) process(batch []*request) {
	cur := &job{st: newStage()}
	for _, r := range batch {
		if f := l.failure(); f != nil {
			cur.add(r, f)
			continue
		}
		if r.bound != nil && l.isRemoved(r.bound) {
			cur.add(r, ErrGroupRemoved)
			continue
		}
		snap := hasSnapshot(r.updates)
		if snap {
			// The snapshot's HardState may be filled in from the group's
			// current one, so everything before it must be published.
			l.dispatch(cur)
			l.drain()
			cur = &job{st: newStage()}
			if f := l.failure(); f != nil {
				cur.add(r, f)
				continue
			}
		}
		if err := l.validate(cur.st, r.updates); err != nil {
			cur.add(r, err)
			continue
		}
		if snap {
			if err := l.stageSnapshots(cur, r); err != nil {
				if f := l.failure(); f != nil {
					err = f
				}
				// Nothing was staged for this request.
				cur.add(r, err)
				continue
			}
		}
		cur.add(r, l.stageRequest(cur.st, r))
	}
	l.dispatch(cur)
}

func hasSnapshot(us []Update) bool {
	for _, u := range us {
		if !raft.IsEmptySnap(u.Snapshot) {
			return true
		}
	}
	return false
}

// maxIndex bounds every log index Save accepts, so that index arithmetic
// (last+1, first-1, counts) never wraps. raft numbers entries from 1, one at
// a time; it will not reach 2^63.
const maxIndex = 1<<63 - 1

// validate checks a request against the state it will be staged on, before
// anything is staged, so that a rejected request changes nothing. Besides
// raft.MemoryStorage's own checks (no gaps, no snapshot older than the
// current one), it refuses states raft never produces and could not restart
// from: an index above maxIndex, and a HardState whose commit index lies
// beyond the group's last entry once the request is applied. (raft panics
// on restart with such a HardState. It would arise from a commit index
// ahead of the entries, or from a snapshot or an overwrite behind it.)
// It runs on the writer goroutine.
func (l *Log) validate(st *stage, ups []Update) error {
	type sim struct {
		group    uint64
		meta     groupMeta
		hs       raftpb.HardState
		snap     uint64
		snapSeen bool
	}
	// Most requests touch one group (raft's Ready of one group), so the
	// simulated groups live in a small array, searched linearly; a map
	// indexes them only for large requests.
	var small [4]sim
	sims := small[:0]
	var byGroup map[uint64]int
	for _, u := range ups {
		i := -1
		if byGroup != nil {
			if k, ok := byGroup[u.Group]; ok {
				i = k
			}
		} else {
			for k := range sims {
				if sims[k].group == u.Group {
					i = k
					break
				}
			}
		}
		if i < 0 {
			n := sim{group: u.Group, meta: groupMeta{first: 1}}
			if d := st.deltas[u.Group]; d != nil {
				n.meta, n.hs, n.snap = d.meta, d.g.whs, d.g.wsnap.index()
				if d.hs != nil {
					n.hs = *d.hs
				}
			} else {
				l.mu.RLock()
				g := l.groups[u.Group]
				l.mu.RUnlock()
				if g != nil {
					n.meta, n.hs, n.snap = g.wmeta, g.whs, g.wsnap.index()
				}
			}
			sims = append(sims, n)
			i = len(sims) - 1
			if byGroup != nil {
				byGroup[u.Group] = i
			} else if len(sims) > 16 {
				byGroup = make(map[uint64]int, len(ups))
				for k := range sims {
					byGroup[sims[k].group] = k
				}
			}
		}
		s := &sims[i]
		if !raft.IsEmptySnap(u.Snapshot) {
			idx := u.Snapshot.Metadata.Index
			switch {
			case s.snapSeen:
				return fmt.Errorf("%w: group %d: two snapshots in one Save", ErrInvalidUpdate, u.Group)
			case idx <= s.snap:
				return raft.ErrSnapOutOfDate
			case idx > maxIndex:
				return fmt.Errorf("%w: group %d: snapshot index %d is out of range", ErrInvalidUpdate, u.Group, idx)
			}
			s.snapSeen, s.snap = true, idx
			s.meta = groupMeta{first: idx + 1, last: idx}
			// The stored HardState commits the snapshot (see stageSnapshots).
			if !raft.IsEmptyHardState(u.HardState) {
				s.hs = u.HardState
			}
			s.hs.Commit = max(s.hs.Commit, idx)
		} else if !raft.IsEmptyHardState(u.HardState) {
			s.hs = u.HardState
		}
		if ents := u.Entries; len(ents) > 0 {
			for j, e := range ents {
				switch {
				case e.Index == 0:
					return fmt.Errorf("%w: group %d: entry index 0", ErrInvalidUpdate, u.Group)
				case e.Index > maxIndex:
					return fmt.Errorf("%w: group %d: entry index %d is out of range", ErrInvalidUpdate, u.Group, e.Index)
				case j > 0 && e.Index != ents[j-1].Index+1:
					return fmt.Errorf("%w: group %d: non-contiguous entries %d, %d", ErrInvalidUpdate, u.Group, ents[j-1].Index, e.Index)
				}
			}
			if lastNew := ents[len(ents)-1].Index; lastNew >= s.meta.first {
				if max(ents[0].Index, s.meta.first) > s.meta.last+1 {
					return fmt.Errorf("%w: group %d: missing log entry [last: %d, append at: %d]", ErrInvalidUpdate, u.Group, s.meta.last, ents[0].Index)
				}
				s.meta.last = lastNew
			}
		}
	}
	for i := range sims {
		if s := &sims[i]; !isEmptyHS(s.hs) && s.hs.Commit > s.meta.last {
			return fmt.Errorf("%w: group %d: commit index %d would be beyond the last entry %d", ErrInvalidUpdate, s.group, s.hs.Commit, s.meta.last)
		}
	}
	return nil
}

// plan returns, per update of a validated request, the entries that will be
// written (raft.MemoryStorage.Append semantics: entries below FirstIndex are
// ignored, overlapping entries replace the existing suffix). Its own checks
// repeat validate's, as a safeguard.
func (l *Log) plan(st *stage, ups []Update) ([][]raftpb.Entry, error) {
	sim := map[uint64]groupMeta{}
	out := make([][]raftpb.Entry, len(ups))
	for i, u := range ups {
		ents := u.Entries
		if len(ents) == 0 {
			continue
		}
		for j := 1; j < len(ents); j++ {
			if ents[j].Index != ents[j-1].Index+1 {
				return nil, fmt.Errorf("%w: group %d: non-contiguous entries %d, %d", ErrInvalidUpdate, u.Group, ents[j-1].Index, ents[j].Index)
			}
		}
		if ents[0].Index == 0 {
			return nil, fmt.Errorf("%w: group %d: entry index 0", ErrInvalidUpdate, u.Group)
		}
		m, ok := sim[u.Group]
		if !ok {
			m = st.delta(l, u.Group).meta
		}
		lastNew := ents[len(ents)-1].Index
		if lastNew < m.first {
			continue
		}
		if m.first > ents[0].Index {
			ents = ents[m.first-ents[0].Index:]
		}
		if ents[0].Index > m.last+1 {
			return nil, fmt.Errorf("%w: group %d: missing log entry [last: %d, append at: %d]", ErrInvalidUpdate, u.Group, m.last, ents[0].Index)
		}
		m.last = lastNew
		sim[u.Group] = m
		out[i] = ents
	}
	return out, nil
}

func (l *Log) stageRequest(st *stage, r *request) error {
	ups := r.updates
	planned, err := l.plan(st, ups)
	if err != nil {
		return err // nothing written
	}
	for i, u := range ups {
		d := st.delta(l, u.Group)
		if ents := planned[i]; len(ents) > 0 {
			l.writeEntries(st, d, ents, r.enc[i].ents[ents[0].Index-u.Entries[0].Index:])
		}
		if r.enc[i].hsRec != nil {
			l.writeEncoded(st, r.enc[i].hsRec)
			hs := r.enc[i].hsVal
			d.hs = &hs
		}
	}
	return nil
}

// writeEntries writes encoded entry records, replacing any suffix of the
// group's log they overlap.
func (l *Log) writeEntries(st *stage, d *delta, ents []raftpb.Entry, recs [][]byte) {
	g := d.g
	if ents[0].Index <= d.meta.last {
		l.runsRemoveFrom(g, ents[0].Index)
		g.terms = termsTruncate(g.terms, ents[0].Index)
	}
	for k := range ents {
		loc := l.writeEncoded(st, recs[k])
		st.entries++
		st.idx = append(st.idx, pendingIdx{d.id, ents[k].Index, loc.start, loc.end})
		if k == 0 && g.born.seg == 0 {
			l.mu.Lock()
			g.born = position{loc.seg, loc.start}
			l.mu.Unlock()
		}
	}
	l.runsAppend(g, l.active, ents[0].Index, ents[len(ents)-1].Index)
	g.terms = termsAppend(g.terms, ents)
	d.meta.last = ents[len(ents)-1].Index
	d.appended = append(d.appended, ents)
}

// writeRecord buffers one record in the active segment.
func (l *Log) writeRecord(st *stage, typ byte, group uint64, payload []byte) location {
	if typ == recEntry || typ == recRelocate {
		st.entries++
	}
	l.buf = encodeRecord(l.buf, typ, group, payload)
	return l.writeEncoded(st, l.buf)
}

func (l *Log) writeEncoded(st *stage, rec []byte) location {
	start, end := l.w.add(rec)
	if st.records == 0 {
		st.firstStart = start
	}
	st.records++
	return location{seg: l.active, start: start, end: end}
}

// dispatch ends a staged group commit with its marker, writes it and hands
// it to the syncer. The writer then moves on; the requests are answered by
// the syncer once the commit is durable and published.
func (l *Log) dispatch(j *job) {
	if len(j.reqs) == 0 && j.st.records == 0 {
		return
	}
	j.finished = make(chan struct{})
	if j.st.records > 0 && l.failure() == nil {
		m := commitMarker{seq: l.seq + 1, nrecs: j.st.records, start: j.st.firstStart}
		if d := l.durable.Load(); d != nil && d.seg == l.active && d.end <= m.start {
			m.synced = d.end
		}
		l.buf = encodeRecord(l.buf, recCommit, 0, m.encode(nil))
		l.w.add(l.buf)
		written := l.w.pos() - l.w.flushed
		if err := l.w.flush(); err != nil {
			l.fail(err)
		} else {
			l.seq = m.seq
			l.wsegCount += j.st.records + 1
			l.wsegEnts += j.st.entries
			j.f, j.seg = l.wf, l.active
			j.segMeta = segMeta{count: l.wsegCount, entries: l.wsegEnts, end: l.w.pos()}
			// Index the commit's entries; readers see them once it is published.
			if len(j.st.idx) > 0 {
				x := l.activeIndex()
				j.idx = x
				x.mu.Lock()
				for _, p := range j.st.idx {
					e := x.add(p.g, p.index, p.start, p.end)
					if n := len(j.vis); n > 0 && j.vis[n-1].e == e {
						j.vis[n-1].n = e.n()
					} else {
						j.vis = append(j.vis, visUpdate{e, e.n()})
					}
				}
				x.mu.Unlock()
			}
			for _, id := range j.st.order {
				d := j.st.deltas[id]
				d.g.wmeta = d.meta
				if d.hs != nil {
					d.g.whs = *d.hs
				}
				if !d.remove {
					d.runs = append([]segRun(nil), d.g.runs...)
					d.terms = append([]termRun(nil), d.g.terms...)
				}
			}
			l.stats.records.Add(j.st.records)
			l.stats.bytes.Add(uint64(written))
		}
	}
	l.syncq <- j
	l.lastJob = j
	if l.failure() == nil && l.w.pos() >= l.opts.SegmentSize {
		l.drain()
		if l.failure() == nil {
			l.roll()
		}
	}
}

func (l *Log) activeIndex() *segIndex {
	l.filesMu.Lock()
	defer l.filesMu.Unlock()
	return l.segs[l.active].idx
}

// ---- syncer ----

// syncer makes dispatched commits durable in order. Commits that queued up
// while it was busy share one fsync.
func (l *Log) syncer() {
	defer close(l.syncDone)
	for j := range l.syncq {
		jobs := []*job{j}
		open := true
	more:
		for {
			select {
			case nj, ok := <-l.syncq:
				if !ok {
					open = false
					break more
				}
				jobs = append(jobs, nj)
			default:
				break more
			}
		}
		if l.failure() == nil && !l.opts.NoSync {
			var synced File
			for _, j := range jobs {
				if j.st.records == 0 || j.f == nil || j.f == synced {
					continue
				}
				if err := j.f.SyncData(); err != nil {
					l.fail(err)
					l.forgetUnsynced(j.f, j.seg)
					break
				}
				synced = j.f
				l.stats.fsyncs.Add(1)
			}
			if l.failure() == nil {
				for _, j := range jobs {
					if j.st.records > 0 && j.f != nil {
						l.durable.Store(&durablePos{seg: j.seg, end: j.segMeta.end})
					}
				}
			}
		}
		for _, j := range jobs {
			l.complete(j)
		}
		if !open {
			return
		}
	}
}

// forgetUnsynced runs after a failed sync of segment seg, whose commits past
// the last durable offset were therefore never acknowledged. On Linux a
// failed fsync can leave those writes in the page cache, readable but never
// to be written: a restart without a power cut would replay them, and a
// later power cut would lose them under commits acknowledged since. So they
// are overwritten with zeroes (an interrupted write, to Open), and the
// zeroes are synced if the disk allows. Best effort: the log has failed.
func (l *Log) forgetUnsynced(f File, seg uint64) {
	from := int64(0)
	if d := l.durable.Load(); d != nil && d.seg == seg {
		from = d.end
	}
	st, err := f.Stat()
	if err != nil {
		return
	}
	zeros := make([]byte, blockSize)
	for p := from; p < st.Size(); {
		n := min(int64(blockSize), st.Size()-p)
		if _, err := f.WriteAt(zeros[:n], p); err != nil {
			return
		}
		p += n
	}
	_ = f.SyncData()
}

// complete publishes one durable commit, then answers its requests.
func (l *Log) complete(j *job) {
	if l.failure() == nil && j.st.records > 0 && j.f != nil {
		l.commits.Add(1)
		l.stats.commits.Add(1)
		l.bump(j.st)
		// Make the commit's index entries visible before its metadata, so a
		// reader that sees the new metadata finds them. Readers that started
		// earlier see the version change and retry.
		if j.idx != nil {
			j.idx.mu.Lock()
			for _, v := range j.vis {
				if v.n > v.e.vis {
					v.e.vis = v.n
				}
			}
			j.idx.mu.Unlock()
		}
		l.publish(j)
		for _, fn := range j.after {
			fn()
		}
	}
	f := l.failure()
	for i, r := range j.reqs {
		err := j.errs[i]
		if err == nil {
			err = f
		}
		r.done <- err
	}
	close(j.finished)
}

func (l *Log) dropIndex() bool {
	d := l.opts.testDropIndexAfter
	return d > 0 && l.commits.Load() >= d
}

// bump marks the staged groups as being written: disk readers wait.
func (l *Log) bump(st *stage) {
	l.mu.Lock()
	for _, d := range st.deltas {
		d.g.version++
	}
	l.mu.Unlock()
}

func (l *Log) publish(j *job) {
	l.mu.Lock()
	for _, d := range j.st.deltas {
		g := d.g
		if d.remove {
			g.removed = true
			l.cacheClear(g)
			if l.groups[d.id] == g {
				delete(l.groups, d.id)
			}
			g.version++
			continue
		}
		if d.reset {
			l.cacheClear(g)
		}
		g.meta = d.meta
		if d.hs != nil {
			g.hs = *d.hs
		}
		if d.cs != nil {
			g.cs = *d.cs
		}
		if d.snap != nil {
			g.snap = d.snap
		}
		g.pruns, g.pterms = d.runs, d.terms
		for _, ents := range d.appended {
			l.cacheAppend(g, ents)
		}
		g.version++
	}
	l.evictCaches()
	l.mu.Unlock()
	l.cond.Broadcast()
	if j.f != nil {
		l.filesMu.Lock()
		if ss := l.segs[j.seg]; ss != nil {
			ss.meta.count, ss.meta.entries, ss.meta.end = j.segMeta.count, j.segMeta.entries, j.segMeta.end
		}
		l.filesMu.Unlock()
	}
}

// ---- cache (callers hold l.mu for writing) ----

func entriesBytes(es []raftpb.Entry) int64 {
	var n int64
	for i := range es {
		n += int64(es[i].Size())
	}
	return n
}

func (l *Log) cacheAdjust(g *group, delta int64) {
	g.cacheBytes += delta
	l.cacheBytes += delta
}

// cacheAppend keeps the newest CacheEntries entries of a group; the cache
// always ends at the group's last index. Accounting is incremental.
func (l *Log) cacheAppend(g *group, ents []raftpb.Entry) {
	max := l.opts.CacheEntries
	if max <= 0 || len(ents) == 0 {
		return
	}
	start := ents[0].Index
	if n := len(g.cache); n > 0 && start >= g.cache[0].Index && start <= g.cache[n-1].Index+1 {
		cut := start - g.cache[0].Index
		l.cacheAdjust(g, -entriesBytes(g.cache[cut:]))
		g.cache = append(g.cache[:cut], ents...)
	} else {
		l.cacheAdjust(g, -g.cacheBytes)
		g.cache = append(make([]raftpb.Entry, 0, len(ents)), ents...)
	}
	l.cacheAdjust(g, entriesBytes(ents))
	if len(g.cache) > max {
		l.cacheDropOldest(g, len(g.cache)-max)
	}
}

func (l *Log) cacheDropOldest(g *group, n int) {
	if n >= len(g.cache) {
		l.cacheAdjust(g, -g.cacheBytes)
		g.cache = nil
		return
	}
	l.cacheAdjust(g, -entriesBytes(g.cache[:n]))
	rest := g.cache[n:]
	if cap(rest) > 2*len(rest)+16 {
		rest = append(make([]raftpb.Entry, 0, len(rest)), rest...)
	}
	g.cache = rest
}

func (l *Log) cacheClear(g *group) { l.cacheDropOldest(g, len(g.cache)) }

func (l *Log) cacheTrimBelow(g *group, first uint64) {
	if len(g.cache) > 0 && first > g.cache[0].Index {
		l.cacheDropOldest(g, int(first-g.cache[0].Index))
	}
}

// evictCaches drops the older half (at least one entry) of every group's
// cache until the global budget is respected. Each pass removes entries, so
// it terminates.
func (l *Log) evictCaches() {
	for l.cacheBytes > l.opts.CacheBytes {
		progress := false
		for _, g := range l.groups {
			if n := len(g.cache); n > 0 {
				l.cacheDropOldest(g, n-n/2)
				progress = true
				if l.cacheBytes <= l.opts.CacheBytes {
					return
				}
			}
		}
		if !progress {
			return
		}
	}
}

// ---- segments ----

func (l *Log) segPath(n uint64) string { return l.fs.PathJoin(l.segDir, segName(n)) }

// roll seals the active segment, writes its index sidecar, starts the next
// segment and writes a checkpoint. The caller has drained the pipeline.
func (l *Log) roll() {
	if err := l.wf.Sync(); err != nil { // full sync: size metadata too
		l.fail(err)
		return
	}
	err := l.wf.Close()
	l.wf = nil // never close twice, even if opening the next segment fails
	if err != nil {
		l.fail(err)
		return
	}
	sealed := l.active
	l.filesMu.Lock()
	ss := l.segs[sealed]
	ss.meta = segMeta{count: l.wsegCount, entries: l.wsegEnts, end: l.w.pos()}
	x := ss.idx
	l.filesMu.Unlock()
	if !l.dropIndex() {
		if err := l.writeSidecar(sealed, x, l.w.pos()); err != nil {
			l.fail(err)
			return
		}
	}
	l.filesMu.Lock()
	ss.meta.sealed = true // only now: a sealed segment has its sidecar
	ss.pinned = false
	l.idxBytes += x.memBytes()
	l.evictIndexes()
	l.filesMu.Unlock()
	if err := l.openSegment(sealed + 1); err != nil { // also syncs the directory
		l.fail(err)
		return
	}
	if l.dropIndex() {
		return
	}
	if err := l.checkpoint(nil); err != nil {
		l.fail(err)
		return
	}
	if !l.opts.NoAutoGC && !l.inGC {
		l.gcLocked() // errors other than I/O failures only mean nothing was done
	}
}

// openSegment creates segment n and makes it the active segment. It never
// overwrites an existing file.
func (l *Log) openSegment(n uint64) error {
	if n > maxSegment {
		return fmt.Errorf("%w: segment number %d is out of range", ErrCorrupt, n)
	}
	if _, err := l.fs.Stat(l.segPath(n)); err == nil {
		return fmt.Errorf("%w: segment %d already exists", ErrCorrupt, n)
	}
	f, err := l.fs.Create(l.segPath(n))
	if err != nil {
		return err
	}
	if !l.opts.NoPreallocate {
		_ = f.Preallocate(0, l.opts.SegmentSize) // best effort
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := syncDir(l.fs, l.segDir); err != nil {
		f.Close()
		return err
	}
	l.filesMu.Lock()
	l.segs[n] = &segState{idx: newSegIndex(), pinned: true}
	l.filesMu.Unlock()
	l.active, l.wf, l.w, l.wsegCount, l.wsegEnts = n, f, newSegWriter(f, 0, l.key), 0, 0
	l.durable.Store(&durablePos{seg: n, end: 0})
	return nil
}

// ---- files ----

func (l *Log) acquire(seg uint64) (File, error) {
	l.filesMu.Lock()
	defer l.filesMu.Unlock()
	ss, ok := l.segs[seg]
	if !ok || ss.deleted {
		return nil, fmt.Errorf("seglog: segment %d not available", seg)
	}
	if ss.rf == nil {
		f, err := l.fs.Open(l.segPath(seg))
		if err != nil {
			return nil, err
		}
		ss.rf = f
	}
	ss.refs++
	return ss.rf, nil
}

func (l *Log) release(seg uint64) {
	l.filesMu.Lock()
	defer l.filesMu.Unlock()
	ss, ok := l.segs[seg]
	if !ok {
		return
	}
	ss.refs--
	if ss.deleted && ss.refs == 0 {
		if ss.rf != nil {
			ss.rf.Close()
		}
		delete(l.segs, seg)
	}
}

func (l *Log) sortedSegs() []uint64 {
	l.filesMu.Lock()
	defer l.filesMu.Unlock()
	out := make([]uint64, 0, len(l.segs))
	for n, ss := range l.segs {
		if !ss.deleted {
			out = append(out, n)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
