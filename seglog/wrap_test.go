// Copyright 2026 The raftseg authors.

package seglog

import (
	"errors"
	"math"
	"testing"

	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
)

// A stray segment file with a huge number used to be resumed as the
// active segment. The next roll then wrapped the segment number to 0 and
// went on to create segments 1, 2, ... over the existing ones, truncating
// live data, and the log could not be reopened. Open now refuses segment
// numbers out of sequence, and a roll never overwrites a file.
func TestStraySegmentNumbers(t *testing.T) {
	for _, n := range []uint64{math.MaxUint64, math.MaxUint64 - 1, 1 << 63, 1000} {
		mem := crashfs.New()
		l := open(t, crashDir, memOpts(mem, harnessOpts))
		for i := uint64(0); i < 50; i++ {
			if err := l.Save(Update{Group: 1, Entries: ents(1, i*10+1, i*10+10, 1000), HardState: raftpb.HardState{Term: 1, Commit: i*10 + 10}}); err != nil {
				t.Fatal(err)
			}
		}
		l.Close()
		rewrite(t, mem, segPathN(n), make([]byte, 3*blockSize))
		if _, err := Open(crashDir, memOpts(mem, harnessOpts)); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("stray segment %d: open: %v", n, err)
		}
		mem.Remove(segPathN(n))
		l = open(t, crashDir, memOpts(mem, harnessOpts))
		if last, _ := l.Group(1).LastIndex(); last != 500 {
			t.Fatalf("after removing the stray segment: last %d", last)
		}
		l.Close()
	}
}

// A roll never truncates an existing segment file.
func TestRollNeverOverwrites(t *testing.T) {
	mem := crashfs.New()
	l := open(t, crashDir, memOpts(mem, harnessOpts))
	defer l.Close()
	next := l.active + 1
	rewrite(t, mem, segPathN(next), []byte("precious"))
	var err error
	for i := uint64(0); i < 50 && err == nil; i++ {
		err = l.Save(Update{Group: 1, Entries: ents(1, i*10+1, i*10+10, 1000), HardState: raftpb.HardState{Term: 1, Commit: i*10 + 10}})
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("roll onto an existing file: %v", err)
	}
	if got := string(mustRead(t, mem, segPathN(next))); got != "precious" {
		t.Fatalf("the file was overwritten: %q", got)
	}
}
