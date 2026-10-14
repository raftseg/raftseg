// Copyright 2026 The raftseg authors.

package seglog

import (
	"errors"
	"fmt"
	"math/rand"
	randv2 "math/rand/v2"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raftseg/raft"
	"github.com/raftseg/raftseg/internal/crashfs"
)

// TestSyncLostAtEveryOp fails each file sync of a workload in turn the way
// Linux does after a writeback error ("fsyncgate"): the sync returns an
// error, and the unsynced writes stay readable but never reach the disk,
// even if a later sync succeeds. Nothing written before that sync may be
// acknowledged, and the log must fail closed. Then:
//
//   - after a power cut, Open must recover exactly what was acknowledged;
//   - after a restart without a power cut, which still sees the lost writes
//     in the page cache, Open must recover an acceptable state, and new
//     writes acknowledged after it must survive a later power cut.
func TestSyncLostAtEveryOp(t *testing.T) {
	const nops = 80
	seed := int64(11)
	isSync := func(op crashfs.Op) bool { return op.Kind == crashfs.OpSync || op.Kind == crashfs.OpSyncData }
	var total atomic.Int64
	l, err := Open(crashDir, memOpts(crashfs.WithHook(crashfs.New(), func(op crashfs.Op) error {
		if isSync(op) {
			total.Add(1)
		}
		return nil
	}), harnessOpts))
	if err != nil {
		t.Fatal(err)
	}
	var a atomic.Int64
	workload(t, l, seed, nops, &a)
	l.Close()
	n := int(total.Load())
	stride := 1
	if testing.Short() {
		stride = n/30 + 1
	}
	checked := 0
	for k := 1; k <= n; k += stride {
		mem := crashfs.New()
		var count atomic.Int64
		var path string
		fs := crashfs.WithHook(mem, func(op crashfs.Op) error {
			if isSync(op) && count.Add(1) == int64(k) {
				path = op.Path
				return crashfs.ErrSyncLost
			}
			return nil
		})
		l, err := Open(crashDir, memOpts(fs, harnessOpts))
		if err != nil {
			continue // the failure hit Open, which reported it
		}
		var acked atomic.Int64
		states, partials := workloadUntilError(t, l, seed, nops, &acked)
		failed := l.failure() != nil
		if err := l.Close(); err != nil {
			failed = true // the lost sync was the one Close makes
		}
		what := fmt.Sprintf("sync %d (%s) lost", k, path)
		ack := acked.Load()
		cands := candidates(states, partials, ack)
		if !failed && strings.Contains(path, "/segments/") && strings.HasSuffix(path, segSuffix) {
			t.Fatalf("%s: neither the log nor Close failed after a segment sync failed", what)
		}

		// A power cut: only what reached the disk counts.
		for _, keep := range []int{0, 100, 50} {
			img := mem.CrashClone(keep, randv2.New(randv2.NewPCG(uint64(k), uint64(keep))))
			verifyClone(t, fmt.Sprintf("%s, power cut keeping %d%%", what, keep), img, cands)
		}

		// A restart without a power cut, more acknowledged work, then a power
		// cut.
		l2, err := Open(crashDir, memOpts(mem, harnessOpts))
		if err != nil {
			t.Fatalf("%s: restart: %v", what, err)
		}
		var w *world
		for _, c := range cands {
			if matchWorld(l2, c) == nil {
				w = c.clone()
				break
			}
		}
		if w == nil {
			t.Fatalf("%s: restart: %v", what, matchWorld(l2, cands...))
		}
		rng := rand.New(rand.NewSource(int64(k)))
		for i := 0; i < 15; i++ {
			o := genOp(rng, w)
			if err := o.run(l2, w); err != nil {
				if errors.Is(err, raft.ErrSnapOutOfDate) {
					continue
				}
				t.Fatalf("%s: after restart, op %v: %v", what, o, err)
			}
			o.apply(w)
		}
		l2.Close()
		for _, keep := range []int{0, 100} {
			img := mem.CrashClone(keep, randv2.New(randv2.NewPCG(uint64(k), 7)))
			verifyClone(t, fmt.Sprintf("%s, restart, more writes, power cut keeping %d%%", what, keep), img, []*world{w})
		}
		checked++
	}
	t.Logf("%d syncs, %d checked", n, checked)
}

// TestErrorAtAnyOp fails each filesystem operation of a workload in turn,
// of every kind and on every file: reads, stats, listings, opens and
// closes as well as writes, syncs, renames and removals, in segments,
// sidecars, checkpoints and snapshot files. Nothing may panic or hang. The
// failed operation's caller gets an error; reads afterwards return either
// the right data or an error; and a reopen recovers every acknowledged
// operation.
func TestErrorAtAnyOp(t *testing.T) {
	uncached := harnessOpts
	uncached.CacheEntries = -1      // every read goes to disk
	uncached.IndexCacheBytes = 1024 // and loads index sidecars
	for name, opts := range map[string]Options{"cached": harnessOpts, "uncached": uncached} {
		t.Run(name, func(t *testing.T) { errorAtAnyOp(t, opts) })
	}
}

func errorAtAnyOp(t *testing.T, opts Options) {
	const nops = 60
	seed := int64(5)
	var total atomic.Int64
	l, err := Open(crashDir, memOpts(crashfs.WithHook(crashfs.New(), func(op crashfs.Op) error {
		total.Add(1)
		return nil
	}), opts))
	if err != nil {
		t.Fatal(err)
	}
	var a atomic.Int64
	workloadReading(t, l, seed, nops, &a)
	l.Close()
	n := int(total.Load())
	stride := 1
	if testing.Short() {
		stride = n/50 + 1
	}
	kinds := map[string]int{}
	for k := 1; k <= n; k += stride {
		mem := crashfs.New()
		var count atomic.Int64
		var hitp atomic.Pointer[crashfs.Op]
		fs := crashfs.WithHook(mem, func(op crashfs.Op) error {
			if count.Add(1) == int64(k) {
				hitp.Store(&op)
				return crashfs.ErrInjected
			}
			return nil
		})
		hit := func() crashfs.Op {
			if p := hitp.Load(); p != nil {
				return *p
			}
			return crashfs.Op{Kind: -1}
		}
		l, err := Open(crashDir, memOpts(fs, opts))
		if err != nil {
			if !errors.Is(err, crashfs.ErrInjected) {
				t.Fatalf("op %d (%v) failed Open with an unrelated error: %v", k, hit(), err)
			}
			kinds["open"]++
			continue
		}
		var acked atomic.Int64
		done := make(chan struct{})
		var states []*world
		var partials map[int]*world
		go func() {
			defer close(done)
			states, partials = workloadReading(t, l, seed, nops, &acked)
		}()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("op %d (%v): workload hung after the error", k, hit())
		}
		h := hit()
		what := fmt.Sprintf("error at op %d (%s %s)", k, h.Kind, h.Path)
		ack := acked.Load()
		cands := candidates(states, partials, ack)
		// Live reads: right data, or an error.
		if l.failure() == nil {
			for id := range cands[0].g {
				var errs []error
				for _, c := range cands {
					errs = append(errs, matchGroup(l.Group(id), c.g[id]))
				}
				ok := false
				for _, e := range errs {
					ok = ok || e == nil
				}
				if !ok && !strings.Contains(errs[0].Error(), crashfs.ErrInjected.Error()) {
					// Different from every acceptable state, without an error.
					t.Fatalf("%s: group %d reads wrong: %v", what, id, errs)
				}
			}
		}
		l.Close()
		l2, err := Open(crashDir, memOpts(mem, Options{ScrubOnOpen: true}))
		if err != nil {
			t.Fatalf("%s: reopen: %v", what, err)
		}
		if err := matchWorld(l2, cands...); err != nil {
			t.Fatalf("%s (%d acked): %v", what, ack, err)
		}
		l2.Close()
		if h.Kind >= 0 {
			kinds[h.Kind.String()]++
		}
	}
	t.Logf("%d operations; errors injected by kind: %v", n, kinds)
}

// workloadReading is workloadUntilError with reads of a group after every
// operation (through a small cache, so mostly from disk). A read must
// return the model's data or an injected error; it stops the workload only
// if the log has failed.
func workloadReading(t testing.TB, l *Log, seed int64, nops int, acked *atomic.Int64) ([]*world, map[int]*world) {
	rng := rand.New(rand.NewSource(seed))
	w := newWorld(4)
	states := []*world{w.clone()}
	partials := map[int]*world{}
	for i := 0; i < nops; i++ {
		o := genOp(rng, w)
		if p := o.partial(w); p != nil {
			partials[i+1] = p
		}
		err := o.run(l, w)
		if err != nil && errors.Is(err, raft.ErrSnapOutOfDate) {
			states = append(states, w.clone())
			acked.Store(int64(i + 1))
			continue
		}
		next := w.clone()
		o.apply(next)
		if err != nil {
			if l.failure() == nil && o.kind == opGC {
				// A failed GC changes nothing logical.
				states = append(states, w.clone())
				acked.Store(int64(i + 1))
				continue
			}
			return append(states, next), partials
		}
		w = next
		states = append(states, w.clone())
		acked.Store(int64(i + 1))
		g := uint64(1 + rng.Intn(len(w.g)))
		if err := matchGroup(l.Group(g), w.g[g]); err != nil && !strings.Contains(err.Error(), crashfs.ErrInjected.Error()) {
			t.Fatalf("after op %d (%v): group %d reads wrong: %v", i, o, g, err)
		}
	}
	return states, partials
}
