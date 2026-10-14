// Copyright 2026 The raftseg authors.

package seglog

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
)

// TestPerfReport benchmarks the log with each index backend on the real
// filesystem and writes a markdown report. It is skipped unless
// SEGLOG_PERF is set:
//
//	SEGLOG_PERF=1 SEGLOG_PERF_DIR=/data/bench go test ./seglog/ -run TestPerfReport -v -timeout 2h
//
// SEGLOG_PERF_SCALE (default 1) multiplies durations and data sizes.
func TestPerfReport(t *testing.T) {
	if os.Getenv("SEGLOG_PERF") == "" {
		t.Skip("set SEGLOG_PERF=1 to run the benchmark report")
	}
	scale := 1.0
	fmt.Sscan(os.Getenv("SEGLOG_PERF_SCALE"), &scale)
	root := os.Getenv("SEGLOG_PERF_DIR")
	if root == "" {
		root = t.TempDir()
	}
	p := &perf{t: t, root: root, scale: scale}
	native := perfCfg{name: "native index", o: Options{}}
	var rep strings.Builder
	fmt.Fprintf(&rep, "# seglog benchmark: native index\n\n%s\n\n", p.environment())

	// 1. Writes.
	fmt.Fprintf(&rep, "## 1. Writes (durable Saves)\n\nEach Save is one raft Ready: 4 entries plus a HardState, fdatasync'd before it returns. One goroutine per group, saving back to back. Each row is the median-throughput run of %d runs of %s; after each run the log is reopened and every acknowledged Save is checked.\n\n", perfReps, p.dur(3*time.Second))
	rep.WriteString(writeHeader)
	for _, conc := range []int{1, 16, 128} {
		for _, size := range []int{256, 4096} {
			for _, r := range p.writeMedian([]perfCfg{native}, conc, size) {
				rep.WriteString(r.row())
			}
		}
	}
	rep.WriteString("\n### Durability and pipelining (16 groups, 1 KiB entries)\n\n")
	rep.WriteString(writeHeader)
	for _, r := range p.writeMedian([]perfCfg{
		native,
		{name: "no pipelining", o: Options{PipelineDepth: 1}},
		{name: "NoSync (unsafe)", o: Options{NoSync: true}},
		{name: "checkpoint every 100 ms", o: Options{CheckpointInterval: 100 * time.Millisecond}},
	}, 16, 1024) {
		rep.WriteString(r.row())
	}

	// 2. Reads.
	rep.WriteString("\n## 2. Reads from disk (entry cache off, OS page cache warm)\n\n64 groups of 1 KiB entries in 16 MiB segments, read by 8 goroutines at random positions. A Term lookup is answered from memory; a point read finds the entry's offset in its segment's index (in memory, or loaded from the segment's sidecar file into a cache bounded by IndexCacheBytes) and reads the record.\n\n")
	rep.WriteString("| config | entries | point reads/s | point p50 | point p99 | 100-entry range reads/s | range MB/s | Term lookups/s |\n|---|---|---|---|---|---|---|---|\n")
	recov := map[string]string{}
	for _, c := range []perfCfg{native, {name: "index cache 1 MiB (most lookups load a sidecar)", o: Options{IndexCacheBytes: 1 << 20}}} {
		r, reopen := p.read(c, 64, int(5000*p.scale), p.dur(3*time.Second))
		rep.WriteString(r)
		recov[c.name] = reopen
	}

	// 3. Queue churn.
	rep.WriteString("\n## 3. Queue churn: append and compact continuously\n\n32 groups each keep only their newest 512 entries (1 KiB), compacting every 8 Saves, like queues whose consumers keep up. The entry cache is off, so every read goes through the index; a reader samples 8-entry reads at queue heads. Segments are 8 MiB.\n\n")
	rep.WriteString("| config | Saves/s | head read p50, first → last quarter | head read p99, first → last quarter | segment bytes | index and checkpoint bytes | live bytes | space amplification | segments deleted | entries relocated |\n|---|---|---|---|---|---|---|---|---|---|\n")
	rep.WriteString(p.churn(native, p.dur(20*time.Second)))

	// 4. Recovery.
	rep.WriteString("\n## 4. Recovery\n\n")
	fmt.Fprintf(&rep, "Clean reopen is Open on the read benchmark's log (%d entries) after Close, which writes a checkpoint. Crash replay is Open after a crash in which no checkpoint or index sidecar was written after the first commit, so every commit is replayed from the segments and re-indexed; all entries are checked afterwards.\n\n", 64*(int(5000*p.scale)/64*64))
	rep.WriteString("| config | clean reopen | crash replay | replayed commits | replayed records | segment bytes scanned | replay rate |\n|---|---|---|---|---|---|---|\n")
	d, rs, segBytes := p.replay(native, int(20000*p.scale))
	fmt.Fprintf(&rep, "| %s | %s | %s | %d | %d | %s | %.0f MB/s |\n", native.name, recov[native.name], fmtDur(d), rs.ReplayedCommits, rs.ReplayedRecords, fmtBytes(segBytes), float64(segBytes)/d.Seconds()/1e6)

	out := os.Getenv("SEGLOG_PERF_OUT")
	if out == "" {
		out = filepath.Join(p.root, "perf_report.md")
	}
	if err := os.WriteFile(out, []byte(rep.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("report written to %s\n%s", out, rep.String())
}

const writeHeader = "| config | groups | entry | Saves/s | MB/s | p50 | p99 | p99.9 | Saves per fsync | checkpoints/s | bytes written per user byte | index and checkpoint bytes per user byte |\n|---|---|---|---|---|---|---|---|---|---|---|---|\n"

type perfCfg struct {
	name string
	o    Options
}

type perf struct {
	t     *testing.T
	root  string
	scale float64
}

func (p *perf) dur(d time.Duration) time.Duration { return time.Duration(float64(d) * p.scale) }

func (p *perf) dir() string {
	d, err := os.MkdirTemp(p.root, "run-")
	if err != nil {
		p.t.Fatal(err)
	}
	return d
}

func (p *perf) open(dir string, c perfCfg, extra func(*Options)) *Log {
	o := c.o
	if extra != nil {
		extra(&o)
	}
	l, err := Open(dir, o)
	if err != nil {
		p.t.Fatal(err)
	}
	return l
}

func (p *perf) environment() string {
	host, _ := os.Hostname()
	cpu := ""
	if f, err := os.Open("/proc/cpuinfo"); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "model name") {
				cpu = strings.TrimSpace(strings.SplitN(sc.Text(), ":", 2)[1])
				break
			}
		}
		f.Close()
	}
	// fdatasync latency of the target filesystem.
	f, _ := os.CreateTemp(p.root, "probe")
	var lat []time.Duration
	buf := make([]byte, 4096)
	for i := 0; i < 200; i++ {
		f.Write(buf)
		t0 := time.Now()
		f.Sync()
		lat = append(lat, time.Since(t0))
	}
	f.Close()
	os.Remove(f.Name())
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	return fmt.Sprintf("Host %s, CPU %s, %d cores. Data directory %s. fsync after a 4 KiB append: p50 %s, p99 %s. Run %s.",
		host, cpu, numCPU(), p.root, fmtDur(lat[100]), fmtDur(lat[198]), time.Now().Format("2006-01-02 15:04"))
}

func numCPU() int {
	n := 0
	if f, err := os.Open("/proc/cpuinfo"); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "processor") {
				n++
			}
		}
		f.Close()
	}
	return n
}

// procWrites returns the bytes this process has written through syscalls.
func procWrites() int64 {
	f, err := os.Open("/proc/self/io")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var v int64
		if n, _ := fmt.Sscanf(sc.Text(), "wchar: %d", &v); n == 1 {
			return v
		}
	}
	return 0
}

type writeResult struct {
	name                  string
	conc, size            int
	ops                   int64
	elapsed               time.Duration
	lat                   []time.Duration
	fsyncs, indexSyncs    uint64
	written, indexWritten int64
	userBytes             int64
}

func (r writeResult) row() string {
	sort.Slice(r.lat, func(i, j int) bool { return r.lat[i] < r.lat[j] })
	q := func(f float64) time.Duration {
		if len(r.lat) == 0 {
			return 0
		}
		return r.lat[min(len(r.lat)-1, int(f*float64(len(r.lat))))]
	}
	perFsync := "n/a"
	if r.fsyncs > 0 {
		perFsync = fmt.Sprintf("%.1f", float64(r.ops)/float64(r.fsyncs))
	}
	idxSyncs, idxAmp := fmt.Sprintf("%.1f", float64(r.indexSyncs)/r.elapsed.Seconds()), fmt.Sprintf("%.2f", float64(r.indexWritten)/float64(max(r.userBytes, 1)))
	if r.fsyncs == 0 {
		idxSyncs, idxAmp = "n/a", "n/a"
	}
	return fmt.Sprintf("| %s | %d | %s | %.0f | %.1f | %s | %s | %s | %s | %s | %.2f | %s |\n",
		r.name, r.conc, fmtBytes(int64(r.size)), float64(r.ops)/r.elapsed.Seconds(),
		float64(r.userBytes)/r.elapsed.Seconds()/1e6, fmtDur(q(0.5)), fmtDur(q(0.99)), fmtDur(q(0.999)),
		perFsync, idxSyncs, float64(r.written)/float64(max(r.userBytes, 1)), idxAmp)
}

const perfReps = 3

// writeMedian runs each config perfReps times, interleaved so that drift in
// the machine's disk affects all configs alike, and keeps the median run.
func (p *perf) writeMedian(cfgs []perfCfg, conc, size int) []writeResult {
	runs := make([][]writeResult, len(cfgs))
	for rep := 0; rep < perfReps; rep++ {
		for i, c := range cfgs {
			runs[i] = append(runs[i], p.write(c, conc, size, p.dur(3*time.Second)))
		}
	}
	out := make([]writeResult, len(cfgs))
	for i, rs := range runs {
		sort.Slice(rs, func(a, b int) bool { return rs[a].rate() < rs[b].rate() })
		out[i] = rs[len(rs)/2]
	}
	return out
}

func (r writeResult) rate() float64 { return float64(r.ops) / r.elapsed.Seconds() }

// drive runs conc writers, one per group, each saving a 4-entry Ready back
// to back until d has passed.
func (p *perf) drive(conc, size int, d time.Duration, save func(g uint64, hs raftpb.HardState, es []raftpb.Entry) error) (int64, [][]time.Duration, time.Duration) {
	data := make([]byte, size)
	rand.New(rand.NewSource(1)).Read(data)
	var ops atomic.Int64
	lats := make([][]time.Duration, conc)
	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(d)
	for g := 0; g < conc; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			next := uint64(1)
			for time.Now().Before(deadline) {
				es := make([]raftpb.Entry, 4)
				for k := range es {
					es[k] = raftpb.Entry{Term: 1, Index: next + uint64(k), Data: data}
				}
				t0 := time.Now()
				if err := save(uint64(g+1), raftpb.HardState{Term: 1, Commit: next + 3}, es); err != nil {
					p.t.Error(err)
					return
				}
				lats[g] = append(lats[g], time.Since(t0))
				next += 4
				ops.Add(1)
			}
		}(g)
	}
	wg.Wait()
	return ops.Load(), lats, time.Since(start)
}

func (p *perf) write(c perfCfg, conc, size int, d time.Duration) writeResult {
	dir := p.dir()
	defer os.RemoveAll(dir)
	l := p.open(dir, c, nil)
	st0, _ := l.Stats()
	w0 := procWrites()
	ops, lats, elapsed := p.drive(conc, size, d, func(g uint64, hs raftpb.HardState, es []raftpb.Entry) error {
		return l.Group(g).Save(hs, es, raftpb.Snapshot{})
	})
	st, _ := l.Stats()
	if err := l.Close(); err != nil {
		p.t.Fatal(err)
	}
	w1 := procWrites()
	r := writeResult{name: c.name, conc: conc, size: size, ops: ops, elapsed: elapsed,
		fsyncs: st.Fsyncs - st0.Fsyncs, indexSyncs: st.Checkpoints - st0.Checkpoints,
		written: w1 - w0, indexWritten: int64(st.CheckpointBytes + st.IndexBytes - st0.CheckpointBytes - st0.IndexBytes),
		userBytes: ops * 4 * int64(size)}
	for _, ls := range lats {
		r.lat = append(r.lat, ls...)
	}
	// Every acknowledged Save must be there after a reopen.
	if !c.o.NoSync {
		l = p.open(dir, c, nil)
		for g := 0; g < conc; g++ {
			li, _ := l.Group(uint64(g + 1)).LastIndex()
			if want := uint64(len(lats[g]) * 4); li != want {
				p.t.Errorf("%s: group %d has %d entries after reopen, %d acknowledged", c.name, g+1, li, want)
			}
		}
		l.Close()
	}
	p.t.Logf("write %s c=%d size=%d: %d Saves", c.name, conc, size, r.ops)
	return r
}

func (p *perf) read(c perfCfg, groups, perGroup int, d time.Duration) (string, string) {
	dir := p.dir()
	defer os.RemoveAll(dir)
	l := p.open(dir, c, func(o *Options) { o.CacheEntries = -1; o.SegmentSize = 16 << 20 })
	data := make([]byte, 1024)
	rand.New(rand.NewSource(2)).Read(data)
	var wg sync.WaitGroup
	for g := 0; g < groups; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			s := l.Group(uint64(g + 1))
			for i := 1; i <= perGroup; i += 64 {
				es := make([]raftpb.Entry, 64)
				for k := range es {
					es[k] = raftpb.Entry{Term: 1, Index: uint64(i + k), Data: data}
				}
				if err := s.Save(raftpb.HardState{Term: 1, Commit: uint64(i + 63)}, es, raftpb.Snapshot{}); err != nil {
					p.t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	l.Close()
	t0 := time.Now()
	l = p.open(dir, c, func(o *Options) { o.CacheEntries = -1; o.SegmentSize = 16 << 20 })
	reopen := time.Since(t0)
	defer l.Close()
	n := groups * (perGroup / 64) * 64

	bench := func(fn func(rng *rand.Rand) int64) (float64, []time.Duration, int64) {
		var ops, bytes atomic.Int64
		lats := make([][]time.Duration, 8)
		deadline := time.Now().Add(d)
		start := time.Now()
		var wg sync.WaitGroup
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(w)))
				for time.Now().Before(deadline) {
					t1 := time.Now()
					bytes.Add(fn(rng))
					lats[w] = append(lats[w], time.Since(t1))
					ops.Add(1)
				}
			}(w)
		}
		wg.Wait()
		var all []time.Duration
		for _, x := range lats {
			all = append(all, x...)
		}
		sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
		return float64(ops.Load()) / time.Since(start).Seconds(), all, bytes.Load()
	}
	last := uint64(perGroup / 64 * 64)
	point, plat, _ := bench(func(rng *rand.Rand) int64 {
		s := l.Group(uint64(1 + rng.Intn(groups)))
		i := 1 + uint64(rng.Int63n(int64(last)))
		es, err := s.Entries(i, i+1, math.MaxUint64)
		if err != nil {
			p.t.Error(err)
		}
		return int64(len(es))
	})
	start := time.Now()
	rangeRate, _, rbytes := bench(func(rng *rand.Rand) int64 {
		s := l.Group(uint64(1 + rng.Intn(groups)))
		i := 1 + uint64(rng.Int63n(int64(last-100)))
		es, err := s.Entries(i, i+100, math.MaxUint64)
		if err != nil {
			p.t.Error(err)
		}
		return int64(len(es)) * 1024
	})
	rangeMBs := float64(rbytes) / time.Since(start).Seconds() / 1e6
	termRate, _, _ := bench(func(rng *rand.Rand) int64 {
		s := l.Group(uint64(1 + rng.Intn(groups)))
		if _, err := s.Term(1 + uint64(rng.Int63n(int64(last)))); err != nil && err != raft.ErrCompacted {
			p.t.Error(err)
		}
		return 0
	})
	p.t.Logf("read %s done", c.name)
	return fmt.Sprintf("| %s | %d | %.0f | %s | %s | %.0f | %.0f | %.0f |\n", c.name, n, point,
			fmtDur(plat[len(plat)/2]), fmtDur(plat[len(plat)*99/100]), rangeRate, rangeMBs, termRate),
		fmtDur(reopen)
}

func (p *perf) churn(c perfCfg, d time.Duration) string {
	dir := p.dir()
	defer os.RemoveAll(dir)
	l := p.open(dir, c, func(o *Options) { o.SegmentSize = 8 << 20; o.CacheEntries = -1 })
	defer l.Close()
	const groups, keep = 32, 512
	data := make([]byte, 1024)
	var ops atomic.Int64
	var stop atomic.Bool
	var wg sync.WaitGroup
	for g := 0; g < groups; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			s := l.Group(uint64(g + 1))
			next := uint64(1)
			for n := 0; !stop.Load(); n++ {
				es := make([]raftpb.Entry, 4)
				for k := range es {
					es[k] = raftpb.Entry{Term: 1, Index: next + uint64(k), Data: data}
				}
				if err := s.Save(raftpb.HardState{Term: 1, Commit: next + 3}, es, raftpb.Snapshot{}); err != nil {
					p.t.Error(err)
					return
				}
				next += 4
				ops.Add(1)
				if n%8 == 7 && next > keep+1 {
					ci := next - 1 - keep
					if _, err := s.CreateSnapshot(ci, nil, []byte("state")); err != nil && err != raft.ErrSnapOutOfDate {
						p.t.Error(err)
					}
					if err := s.Compact(ci); err != nil && err != raft.ErrCompacted {
						p.t.Error(err)
					}
				}
			}
		}(g)
	}
	var lat []time.Duration
	var first, last []time.Duration // first and last quarter of the run
	rng := rand.New(rand.NewSource(3))
	start := time.Now()
	for time.Since(start) < d {
		s := l.Group(uint64(1 + rng.Intn(groups)))
		fi, _ := s.FirstIndex()
		li, _ := s.LastIndex()
		if li >= fi+8 {
			t0 := time.Now()
			if _, err := s.Entries(fi, fi+8, math.MaxUint64); err != nil && err != raft.ErrCompacted {
				p.t.Error(err)
			}
			x := time.Since(t0)
			lat = append(lat, x)
			switch el := time.Since(start); {
			case el < d/4:
				first = append(first, x)
			case el > d*3/4:
				last = append(last, x)
			}
		}
		time.Sleep(200 * time.Microsecond)
	}
	stop.Store(true)
	wg.Wait()
	elapsed := time.Since(start)
	l.GC()
	st, _ := l.Stats()
	idxBytes := filesBytes(dir, idxSuffix) + filesBytes(dir, "CHECKPOINT-")
	live := int64(groups * keep * 1024)
	pct := func(x []time.Duration, q float64) time.Duration {
		sort.Slice(x, func(i, j int) bool { return x[i] < x[j] })
		return x[int(q*float64(len(x)-1))]
	}
	p.t.Logf("churn %s done", c.name)
	return fmt.Sprintf("| %s | %.0f | %s → %s | %s → %s | %s | %s | %s | %.1f× | %d | %d |\n", c.name, float64(ops.Load())/elapsed.Seconds(),
		fmtDur(pct(first, 0.5)), fmtDur(pct(last, 0.5)), fmtDur(pct(first, 0.99)), fmtDur(pct(last, 0.99)), fmtBytes(st.SegmentBytes), fmtBytes(idxBytes), fmtBytes(live),
		float64(st.SegmentBytes+idxBytes)/float64(live), st.RemovedSegments, st.RelocatedEntries)
}

// replay measures Open after a crash in which the index kept none of the
// commits (the worst case the segments must cover).
func (p *perf) replay(c perfCfg, commits int) (time.Duration, RecoveryStats, int64) {
	dir := p.dir()
	defer os.RemoveAll(dir)
	l := p.open(dir, c, func(o *Options) { o.testDropIndexAfter = 1; o.SegmentSize = 1 << 30 })
	data := make([]byte, 512)
	var wg sync.WaitGroup
	per := commits / 16
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			s := l.Group(uint64(g + 1))
			for i := 0; i < per; i++ {
				es := []raftpb.Entry{{Term: 1, Index: uint64(i*2 + 1), Data: data}, {Term: 1, Index: uint64(i*2 + 2), Data: data}}
				if err := s.Save(raftpb.HardState{Term: 1, Commit: uint64(i*2 + 2)}, es, raftpb.Snapshot{}); err != nil {
					p.t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	st0, _ := l.Stats()
	l.crash()
	t0 := time.Now()
	l = p.open(dir, c, nil)
	d := time.Since(t0)
	st, _ := l.Stats()
	for g := 0; g < 16; g++ {
		if li, _ := l.Group(uint64(g + 1)).LastIndex(); li != uint64(per*2) {
			p.t.Errorf("%s replay: group %d has %d entries, want %d", c.name, g+1, li, per*2)
		}
	}
	l.Close()
	return d, st.Recovery, st0.SegmentBytes
}

func fmtDur(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%.0fµs", float64(d)/float64(time.Microsecond))
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}

func fmtBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.0f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

// filesBytes sums the sizes of the files under dir whose names contain part.
func filesBytes(dir, part string) int64 {
	var n int64
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.Contains(info.Name(), part) {
			n += info.Size()
		}
		return nil
	})
	return n
}
