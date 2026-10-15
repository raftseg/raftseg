// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raftsim

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/raftseg/raftseg/seglog"
)

// seeds returns how many seeds to run: RAFTSIM_SEEDS, or def (short when
// testing.Short).
func seeds(t *testing.T, def, short int) int {
	if s := os.Getenv("RAFTSIM_SEEDS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if testing.Short() {
		return short
	}
	return def
}

// firstSeed lets a run start elsewhere (RAFTSIM_FIRST), so that repeated
// soak runs cover new ground.
func firstSeed(t *testing.T) int64 {
	if s := os.Getenv("RAFTSIM_FIRST"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	return 1
}

// report reruns a failed simulation with tracing and prints how to replay
// it.
func report(t *testing.T, o Options, err error) {
	t.Helper()
	var f *Failure
	if !errors.As(err, &f) {
		t.Fatal(err)
	}
	o.Trace = true
	_, err2 := Run(o)
	var f2 *Failure
	trace := ""
	if errors.As(err2, &f2) {
		lines := f2.Trace
		if len(lines) > 80 {
			lines = lines[len(lines)-80:]
		}
		trace = strings.Join(lines, "\n")
		if f2.Msg != f.Msg {
			trace += "\n(replay with tracing failed differently: " + f2.Msg + ")"
		}
	} else {
		trace = "(replay with tracing passed: tracing changed the run)"
	}
	t.Errorf("%v\n%s\nlast events:\n%s\nreplay: RAFTSIM_SEED=%d go test ./internal/raftsim -run '%s' -v", f, f.Stack, trace, o.Seed, t.Name())
}

func runSeeds(t *testing.T, n int, mk func(seed int64) Options) Stats {
	t.Helper()
	if s := os.Getenv("RAFTSIM_SEED"); s != "" {
		seed, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		o := mk(seed)
		t.Logf("%s", o)
		st, err := Run(o)
		if err != nil {
			report(t, o, err)
		}
		t.Logf("%+v", st)
		return st
	}
	first := firstSeed(t)
	var (
		mu     sync.Mutex
		total  Stats
		failed []int64
		wg     sync.WaitGroup
	)
	work := make(chan int64)
	for w := 0; w < runtime.GOMAXPROCS(0); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seed := range work {
				o := mk(seed)
				st, err := Run(o)
				mu.Lock()
				if err != nil {
					failed = append(failed, seed)
					if len(failed) <= 3 {
						report(t, o, err)
					}
				} else {
					total.Add(st)
				}
				mu.Unlock()
			}
		}()
	}
	for s := first; s < first+int64(n); s++ {
		work <- s
	}
	close(work)
	wg.Wait()
	if len(failed) > 0 {
		t.Errorf("%d of %d seeds failed: %v", len(failed), n, failed)
	}
	t.Logf("%d seeds: %+v", n, total)
	return total
}

// TestSimulation runs randomized clusters under every kind of fault and
// checks raft's properties throughout. It also checks that the runs
// exercised what they were meant to, so that a change that quietly stops
// elections, configuration changes or snapshots fails here.
func TestSimulation(t *testing.T) {
	n := seeds(t, 300, 40)
	total := runSeeds(t, n, RandomOptions)
	checkCoverage(t, n, total)
}

// checkCoverage fails if the runs did not exercise what they were meant
// to, so that a change that quietly stops elections, configuration changes
// or snapshots fails here.
func checkCoverage(t *testing.T, n int, total Stats) {
	t.Helper()
	if os.Getenv("RAFTSIM_SEED") != "" {
		return
	}
	per := func(x int) float64 { return float64(x) / float64(n) }
	for _, c := range []struct {
		name string
		got  float64
		min  float64
	}{
		{"leaders per run", per(total.Leaders), 3},
		{"configuration changes applied per run", per(total.ConfApplied), 1},
		{"joint configurations per run", per(total.JointEntered), 0.2},
		{"snapshots restored per run", per(total.SnapRestore), 0.5},
		{"crashes per run", per(total.Crashes), 1},
		{"reads checked per run", per(total.ReadsChecked), 5},
		{"read retries per run", per(total.ReadRetries), 1},
		{"nodes added per run", per(total.NodesAdded), 0.3},
		{"nodes removed per run", per(total.NodesRemoved), 0.2},
		{"entries applied per run", float64(total.Applied) / float64(n), 50},
		{"shared messages checked unchanged per run", float64(total.FrozenChecked) / float64(n), 1000},
	} {
		if c.got < c.min {
			t.Errorf("coverage: %s %.2f, want at least %.2f", c.name, c.got, c.min)
		}
	}
}

func TestDeterminism(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		a, err := Run(RandomOptions(seed))
		if err != nil {
			t.Fatal(err)
		}
		b, err := Run(RandomOptions(seed))
		if err != nil {
			t.Fatal(err)
		}
		if a != b {
			t.Fatalf("seed %d ran differently:\n%+v\n%+v", seed, a, b)
		}
	}
}

// The checks must have teeth: with storage that does not keep synced
// writes, or with messages sent before the state they depend on is
// persisted, crashes must lead to a reported violation.
func TestCatchesBrokenPersistence(t *testing.T) {
	for _, tc := range []struct {
		name string
		mk   func(seed int64) Options
	}{
		{"lying sync", func(seed int64) Options {
			o := RandomOptions(seed)
			o.Backend = &MemBackend{LyingSync: true}
			o.CrashRate = 0.004
			return o
		}},
		{"send before persist", func(seed int64) Options {
			o := RandomOptions(seed)
			o.Async, o.MixedAsync = false, false
			o.SendBeforePersist = true
			o.CrashRate = 0.004
			return o
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kinds := map[string]int{}
			const n = 60
			for seed := int64(1); seed <= n; seed++ {
				_, err := Run(tc.mk(seed))
				var f *Failure
				if errors.As(err, &f) {
					kind := f.Msg
					if i := strings.Index(kind, ":"); i > 0 {
						kind = kind[:i]
					}
					kinds[kind]++
				}
			}
			t.Logf("violations found in %d seeds: %v", n, kinds)
			safety := 0
			for k, v := range kinds {
				if !strings.HasPrefix(k, "no progress") {
					safety += v
				}
			}
			if safety == 0 {
				t.Fatalf("no safety or durability violation found in %d seeds", n)
			}
		})
	}
}

// Nothing may change a message once it is shared (frozen.go): with storage
// that changes entries it has handed out, the simulator must say so.
func TestCatchesSharedMessageChanges(t *testing.T) {
	found := 0
	const n = 20
	for seed := int64(1); seed <= n; seed++ {
		o := RandomOptions(seed)
		o.Backend = &MemBackend{Scribble: true}
		_, err := Run(o)
		var f *Failure
		if errors.As(err, &f) && strings.HasPrefix(f.Msg, "shared message changed:") {
			found++
		}
	}
	t.Logf("shared message changes found in %d of %d seeds", found, n)
	if found < n/2 {
		t.Fatalf("shared message changes found in only %d of %d seeds", found, n)
	}
}

var _ = fmt.Sprint

// SegRandomOptions is RandomOptions with every node's raft log in seglog on
// a crashable filesystem: small segments (rolls, checkpoints and GC all the
// time), caches on or off, snapshot data inline or in files, several groups
// sharing a node's log.
func SegRandomOptions(seed int64) Options {
	o := RandomOptions(seed)
	r := rand.New(rand.NewSource(seed ^ 0x5e9))
	o.Backend = segBackend(r)
	return o
}

func segBackend(r *rand.Rand) *SegBackend {
	return &SegBackend{
		Groups:     pick(r, 1, 1, 2, 4),
		DecoyBytes: pick(r, 64, 2048),
		Options: seglog.Options{
			SegmentSize:         pick[int64](r, 64<<10, 128<<10, 1<<20),
			CacheEntries:        pick(r, -1, 4, 1024),
			SnapshotInlineBytes: pick(r, 16, 4096),
			PipelineDepth:       pick(r, 1, 4),
			CleanAfterSegments:  pick(r, 1, 2),
			IndexCacheBytes:     pick[int64](r, 4<<10, 64<<20),
		},
	}
}

// MixedRandomOptions mixes storage engines in one cluster: some nodes keep
// their log in memory (MemoryStorage with a modelled durability), the
// others in seglog.
func MixedRandomOptions(seed int64) Options {
	o := RandomOptions(seed)
	r := rand.New(rand.NewSource(seed ^ 0x313))
	o.Backend = &MixedBackend{Backends: []Backend{&MemBackend{}, segBackend(r)}}
	return o
}

// TestSimulationSeglog runs the simulation with seglog storage, and
// TestSimulationMixed with mixed storage. The coverage checks of
// TestSimulation apply to both.
func TestSimulationSeglog(t *testing.T) {
	n := seeds(t, 150, 20)
	var seg segCoverage
	total := runSeeds(t, n, func(seed int64) Options {
		o := SegRandomOptions(seed)
		o.Backend.(*SegBackend).Observe = seg.observe
		return o
	})
	checkCoverage(t, n, total)
	seg.check(t, n)
}

// segCoverage sums what the logs of a set of runs did, to check that the
// runs reached seglog's interesting paths.
type segCoverage struct {
	logs, rolled, removed, cleaned, replayed, tails, rebuilt atomic.Int64
}

func (c *segCoverage) observe(st seglog.Stats) {
	c.logs.Add(1)
	if st.ActiveSegment > 1 {
		c.rolled.Add(int64(st.ActiveSegment - 1))
	}
	c.removed.Add(int64(st.RemovedSegments))
	c.cleaned.Add(int64(st.CleanedSegments))
	c.replayed.Add(int64(st.Recovery.ReplayedCommits))
	if st.Recovery.Tail != nil {
		c.tails.Add(1)
	}
	c.rebuilt.Add(int64(st.Recovery.RebuiltIndexes))
}

func (c *segCoverage) check(t *testing.T, n int) {
	t.Helper()
	t.Logf("seglog: %d logs, %d segments rolled, %d removed by GC, %d cleaned, %d commits replayed, %d torn tails discarded, %d index sidecars rebuilt",
		c.logs.Load(), c.rolled.Load(), c.removed.Load(), c.cleaned.Load(), c.replayed.Load(), c.tails.Load(), c.rebuilt.Load())
	if os.Getenv("RAFTSIM_SEED") != "" || n < 100 {
		return // too few runs to expect every path
	}
	for _, x := range []struct {
		name string
		n    int64
	}{{"segments rolled", c.rolled.Load()}, {"segments removed", c.removed.Load()}, {"commits replayed", c.replayed.Load()}, {"torn tails", c.tails.Load()}} {
		if x.n == 0 {
			t.Errorf("coverage: no %s", x.name)
		}
	}
}

func TestSimulationMixed(t *testing.T) {
	n := seeds(t, 150, 20)
	total := runSeeds(t, n, MixedRandomOptions)
	checkCoverage(t, n, total)
}

func TestDeterminismSeglog(t *testing.T) {
	for seed := int64(1); seed <= 4; seed++ {
		a, err := Run(SegRandomOptions(seed))
		if err != nil {
			t.Fatal(err)
		}
		b, err := Run(SegRandomOptions(seed))
		if err != nil {
			t.Fatal(err)
		}
		if a != b {
			t.Fatalf("seed %d ran differently:\n%+v\n%+v", seed, a, b)
		}
	}
}
