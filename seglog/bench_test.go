// Copyright 2026 The raftseg authors.

package seglog

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/raftseg/raft/raftpb"
)

// Each op persists one Ready of one group: 4 entries of 1 KiB plus a
// HardState, issued by 64 concurrent groups. Run with -benchtime to taste;
// numbers depend heavily on the disk's fsync latency.
const (
	benchGroups  = 64
	benchEntries = 4
	benchSize    = 1 << 10
)

func BenchmarkSegmentLog(b *testing.B) {
	for _, c := range []struct {
		name string
		o    Options
	}{
		{"native", Options{}},
		{"no-pipelining", Options{PipelineDepth: 1}},
	} {
		b.Run(c.name, func(b *testing.B) { benchSegmentLog(b, c.o) })
	}
}

func benchSegmentLog(b *testing.B, o Options) {
	l, err := Open(b.TempDir(), o)
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	var next [benchGroups + 1]uint64
	var gid atomic.Uint64
	b.SetBytes(benchEntries * benchSize)
	b.SetParallelism(benchGroups / 8) // GOMAXPROCS * this many writers
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		g := gid.Add(1)%benchGroups + 1
		s := l.Group(g)
		for pb.Next() {
			lo := next[g] + 1
			e := ents(1, lo, lo+benchEntries-1, benchSize)
			next[g] += benchEntries
			if err := s.Save(&raftpb.HardState{Term: new(uint64(1)), Commit: new(lo)}, e, &raftpb.Snapshot{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.ReportMetric(float64(l.stats.fsyncs.Load())/float64(b.N), "fsyncs/op")
}

// BenchmarkSaveCPU measures the CPU time a Save costs without fsync: 128
// groups, each saving 4 entries of 1 KiB and a hard state (prepared
// beforehand, as raft prepares a Ready). seglog/README.md, Performance,
// compares it across versions.
func BenchmarkSaveCPU(b *testing.B) {
	l, err := Open(b.TempDir(), Options{NoSync: true, NoPreallocate: true})
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	const groups = 128
	data := make([]byte, 1024)
	per := b.N/groups + 1
	type save struct {
		hs *raftpb.HardState
		es []*raftpb.Entry
	}
	work := make([][]save, groups+1)
	for g := 1; g <= groups; g++ {
		next := uint64(1)
		for k := 0; k < per; k++ {
			es := make([]*raftpb.Entry, 4)
			for j := range es {
				es[j] = &raftpb.Entry{Term: new(uint64(1)), Index: new(next), Type: raftpb.EntryNormal.Enum(), Data: data}
				next++
			}
			work[g] = append(work[g], save{&raftpb.HardState{Term: new(uint64(1)), Vote: new(uint64(1)), Commit: new(next - 1)}, es})
		}
	}
	b.ResetTimer()
	c0 := cpuTime()
	var wg sync.WaitGroup
	for g := 1; g <= groups; g++ {
		wg.Add(1)
		go func(g uint64) {
			defer wg.Done()
			s := l.Group(g)
			for _, w := range work[g] {
				if err := s.Save(w.hs, w.es, nil); err != nil {
					b.Error(err)
					return
				}
			}
		}(uint64(g))
	}
	wg.Wait()
	b.StopTimer()
	if c0 >= 0 {
		b.ReportMetric(float64(cpuTime()-c0)/float64(per*groups)/1e3, "cpu-us/save")
	}
}

// BenchmarkReadDisk measures the CPU time of reading 100 entries of 1 KiB
// from disk (the entry cache off, the OS page cache warm).
func BenchmarkReadDisk(b *testing.B) {
	l, err := Open(b.TempDir(), Options{NoSync: true, NoPreallocate: true, CacheEntries: -1})
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	s := l.Group(1)
	data := make([]byte, 1024)
	const n = 20000
	for lo := uint64(1); lo <= n; lo += 100 {
		var es []*raftpb.Entry
		for i := lo; i < lo+100; i++ {
			es = append(es, &raftpb.Entry{Term: new(uint64(1)), Index: new(i), Type: raftpb.EntryNormal.Enum(), Data: data})
		}
		if err := s.Save(&raftpb.HardState{Term: new(uint64(1)), Commit: new(lo + 99)}, es, nil); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	c0 := cpuTime()
	for k := 0; k < b.N; k++ {
		lo := uint64(1 + (k*7919)%(n-100))
		if _, err := s.Entries(lo, lo+100, math.MaxUint64); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if c0 >= 0 {
		b.ReportMetric(float64(cpuTime()-c0)/float64(b.N)/1e3, "cpu-us/read100")
	}
}
