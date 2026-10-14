// Copyright 2026 The raftseg authors.

package seglog

import (
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
			if err := s.Save(raftpb.HardState{Term: 1, Commit: lo}, e, raftpb.Snapshot{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.ReportMetric(float64(l.stats.fsyncs.Load())/float64(b.N), "fsyncs/op")
}
