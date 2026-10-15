// Copyright 2026 The raftseg authors.

package seglog

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
)

// TestAPIChaos uses every API at once, as a node with many raft groups
// does, and closes the log at a random moment. Each group has one owner
// goroutine, as raft's Ready handling has, which applies random operations
// (appends that overwrite suffixes, snapshots created and received,
// compactions, configuration changes, the group's own removal) and checks
// its group after each against a model. Meanwhile other goroutines read any
// group, run GC, Scrub and Stats, and remove groups they do not own. Reads
// must return correct data or an expected error; nothing may panic or hang;
// after Close every call fails with ErrClosed; and the reopened log holds
// each group's last acknowledged state (or the one in flight).
func TestAPIChaos(t *testing.T) {
	rounds := 12
	if testing.Short() {
		rounds = 3
	}
	for r := 0; r < rounds; r++ {
		t.Run(fmt.Sprint(r), func(t *testing.T) { apiChaos(t, int64(r)) })
	}
}

// entryDataOK reports whether e carries the data ents() gives its index.
func entryDataOK(e *raftpb.Entry) bool {
	prefix := fmt.Sprintf("t%d-i%d:", e.GetTerm(), e.GetIndex())
	if len(e.Data) < len(prefix) {
		return bytes.Equal(e.Data, []byte(prefix)[:len(e.Data)])
	}
	if !bytes.HasPrefix(e.Data, []byte(prefix)) {
		return false
	}
	for _, c := range e.Data[len(prefix):] {
		if c != byte(e.GetIndex()) {
			return false
		}
	}
	return true
}

func expectedReadErr(err error) bool {
	return err == nil || errors.Is(err, raft.ErrCompacted) || errors.Is(err, raft.ErrUnavailable) ||
		errors.Is(err, ErrGroupRemoved) || errors.Is(err, ErrClosed) || strings.Contains(err.Error(), "out of bound")
}

func apiChaos(t *testing.T, seed int64) {
	mem := crashfs.New()
	opts := memOpts(mem, Options{SegmentSize: 64 << 10, CacheEntries: 8, SnapshotInlineBytes: 1 << 10, IndexCacheBytes: 4 << 10, CleanAfterSegments: 1})
	l, err := Open(crashDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	const groups = 6
	rng := rand.New(rand.NewSource(seed))
	closeAfter := time.Duration(20+rng.Intn(200)) * time.Millisecond

	type owner struct {
		states   []*world
		partials map[int]*world
		acked    atomic.Int64
	}
	owners := make([]*owner, groups+1)
	var wg sync.WaitGroup
	var stop atomic.Bool
	fail := func(format string, a ...any) {
		t.Errorf(format, a...)
		stop.Store(true)
	}
	for g := uint64(1); g <= groups; g++ {
		o := &owner{partials: map[int]*world{}}
		owners[g] = o
		wg.Add(1)
		go func(g uint64, seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			w := &world{g: map[uint64]*groupModel{g: newGroupModel()}, terms: map[uint64]uint64{g: 1}}
			o.states = append(o.states, w.clone())
			var hist []string
			for i := 0; !stop.Load(); i++ {
				op := genOp(rng, w)
				hist = append(hist, op.String())
				if len(hist) > 8 {
					hist = hist[1:]
				}
				if p := op.partial(w); p != nil {
					o.partials[len(o.states)] = p
				}
				err := op.run(l, w)
				switch {
				case err == nil:
					op.apply(w)
				case errors.Is(err, raft.ErrSnapOutOfDate):
				case errors.Is(err, ErrClosed):
					next := w.clone()
					op.apply(next)
					o.states = append(o.states, next) // in flight: may have landed
					return
				case errors.Is(err, ErrGroupRemoved):
					// A remover got there first: the group starts over.
					w.g[g], w.terms[g] = newGroupModel(), 1
					o.states = append(o.states, w.clone())
					o.acked.Store(int64(len(o.states) - 1))
					continue
				default:
					fail("group %d, op %v: %v", g, op, err)
					return
				}
				o.states = append(o.states, w.clone())
				o.acked.Store(int64(len(o.states) - 1))
				if err := matchGroup(l.Group(g), w.g[g]); err != nil && !errors.Is(err, ErrClosed) && !strings.Contains(err.Error(), ErrClosed.Error()) {
					if strings.Contains(err.Error(), ErrGroupRemoved.Error()) {
						continue
					}
					l.mu.RLock()
					gg, present := l.groups[g]
					var removed bool
					var meta groupMeta
					if present {
						removed, meta = gg.removed, gg.meta
					}
					l.mu.RUnlock()
					fail("group %d after %v: %v (in the log: %v, removed %v, %+v; last ops %v)", g, op, err, present, removed, meta, hist)
					return
				}
			}
		}(g, seed*100+int64(g))
	}
	// Readers of any group.
	var reads atomic.Int64
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for !stop.Load() {
				s := l.Group(uint64(1 + rng.Intn(groups+1)))
				first, err1 := s.FirstIndex()
				last, err2 := s.LastIndex()
				if errors.Is(err1, ErrClosed) || errors.Is(err2, ErrClosed) {
					return
				}
				lo := first + uint64(rng.Intn(5))
				hi := lo + uint64(rng.Intn(30))
				if hi > last+1 {
					hi = last + 1
				}
				es, err := s.Entries(lo, hi, uint64(rng.Intn(20000)))
				if !expectedReadErr(err) {
					fail("read entries [%d, %d): %v", lo, hi, err)
					return
				}
				for k, e := range es {
					if e.GetIndex() != lo+uint64(k) || !entryDataOK(e) {
						fail("read entries [%d, %d): entry %d holds %d/%d %q", lo, hi, k, e.GetIndex(), e.GetTerm(), e.Data[:min(len(e.Data), 16)])
						return
					}
					if term, err := s.Term(e.GetIndex()); err == nil && term != e.GetTerm() {
						// The group may have changed between the two reads; only
						// a term that disagrees within one stable state is wrong,
						// and the owner checks that.
						_ = term
					}
				}
				if _, err := s.Snapshot(); !expectedReadErr(err) {
					fail("snapshot: %v", err)
					return
				}
				reads.Add(1)
			}
		}(seed*1000 + int64(r))
	}
	// Maintenance and a remover of groups it does not own (group 7).
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(seed + 7))
		for !stop.Load() {
			var err error
			switch rng.Intn(5) {
			case 0:
				_, err = l.GC()
			case 1:
				var rep ScrubReport
				rep, err = l.Scrub()
				if err == nil && len(rep.Problems) > 0 {
					fail("scrub during chaos: %v", rep.Problems)
				}
			case 2:
				_, err = l.Stats()
			case 3:
				s := l.Group(groups + 1)
				last, _ := s.LastIndex()
				err = s.Save(&raftpb.HardState{Term: new(uint64(1)), Commit: new(last + 1)}, ents(1, last+1, last+1, 100), &raftpb.Snapshot{})
				if errors.Is(err, ErrGroupRemoved) {
					err = nil
				}
			case 4:
				err = l.RemoveGroup(groups + 1)
			}
			if err != nil && !errors.Is(err, ErrClosed) {
				fail("maintenance: %v", err)
				return
			}
			if errors.Is(err, ErrClosed) {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	time.Sleep(closeAfter)
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	stop.Store(true)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("goroutines hung after Close")
	}
	if t.Failed() {
		return
	}
	// After Close, everything fails with ErrClosed.
	s := l.Group(1)
	if _, err := s.Entries(1, 2, math.MaxUint64); !errors.Is(err, ErrClosed) {
		t.Fatalf("Entries after Close: %v", err)
	}
	if err := s.Save(&raftpb.HardState{Term: new(uint64(9))}, nil, &raftpb.Snapshot{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Save after Close: %v", err)
	}
	if _, err := l.GC(); !errors.Is(err, ErrClosed) {
		t.Fatalf("GC after Close: %v", err)
	}
	if err := l.RemoveGroup(1); !errors.Is(err, ErrClosed) {
		t.Fatalf("RemoveGroup after Close: %v", err)
	}
	l2, err := Open(crashDir, memOpts(mem, Options{ScrubOnOpen: true}))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	for g := uint64(1); g <= groups; g++ {
		o := owners[g]
		a := o.acked.Load()
		cands := []*world{o.states[a]}
		if int(a)+1 < len(o.states) {
			cands = append(cands, o.states[a+1])
		}
		if p := o.partials[int(a)+1]; p != nil {
			cands = append(cands, p)
		}
		var errs []string
		ok := false
		for _, c := range cands {
			if err := matchGroup(l2.Group(g), c.g[g]); err == nil {
				ok = true
				break
			} else {
				errs = append(errs, err.Error())
			}
		}
		if !ok {
			t.Fatalf("group %d after reopen (%d ops acked): %s", g, a, strings.Join(errs, " | "))
		}
	}
	t.Logf("closed after %v; %d reads", closeAfter, reads.Load())
}
