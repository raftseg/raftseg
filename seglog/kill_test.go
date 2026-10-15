// Copyright 2026 The raftseg authors.

package seglog

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
)

// TestKillNine runs a writer in a separate process on a real directory and
// SIGKILLs it at random moments, many times over, with segment rolls,
// compaction, snapshots and GC in flight. Every Save the child reported as
// acknowledged must be present and intact after each kill. (A process kill
// keeps the page cache, so this checks the real-filesystem code paths, file
// locking and recovery; power loss is covered by the crash-clone tests.)
func TestKillNine(t *testing.T) {
	if os.Getenv("SEGLOG_CHILD") == "1" {
		killChild()
		return
	}
	rounds := 12
	if testing.Short() {
		rounds = 4
	}
	dir := t.TempDir()
	acked := map[uint64]uint64{}
	rng := rand.New(rand.NewSource(1))
	totalAcks := 0
	for r := 0; r < rounds; r++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestKillNine$")
		cmd.Env = append(os.Environ(), "SEGLOG_CHILD=1", "SEGLOG_DIR="+dir, fmt.Sprintf("SEGLOG_SEED=%d", r))
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		killAt := time.Now().Add(time.Duration(150+rng.Intn(900)) * time.Millisecond)
		killed := make(chan struct{})
		go func() {
			time.Sleep(time.Until(killAt))
			cmd.Process.Kill() // SIGKILL
			close(killed)
		}()
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			var g, idx uint64
			if n, _ := fmt.Sscanf(sc.Text(), "ACK %d %d", &g, &idx); n == 2 {
				if idx > acked[g] {
					acked[g] = idx
				}
				totalAcks++
			}
		}
		<-killed
		cmd.Wait()
		if bytes.Contains(stderr.Bytes(), []byte("FATAL")) {
			t.Fatalf("child failed: %s", stderr.String())
		}
		l, err := Open(dir, Options{ScrubOnOpen: true, SegmentSize: 256 << 10})
		if err != nil {
			t.Fatalf("round %d: reopen after SIGKILL: %v", r, err)
		}
		for g, a := range acked {
			s := l.Group(g)
			fi, _ := s.FirstIndex()
			li, _ := s.LastIndex()
			if li < a {
				t.Fatalf("round %d group %d: last %d but %d was acknowledged", r, g, li, a)
			}
			es, err := s.Entries(fi, li+1, math.MaxUint64)
			if err != nil {
				t.Fatalf("round %d group %d: %v", r, g, err)
			}
			for _, e := range es {
				if !bytes.Equal(e.Data, killPayload(g, e.GetIndex())) {
					t.Fatalf("round %d group %d: entry %d has wrong data", r, g, e.GetIndex())
				}
			}
		}
		l.Close()
	}
	t.Logf("%d SIGKILLs, %d acknowledged Saves verified", rounds, totalAcks)
}

func killPayload(g, i uint64) []byte {
	n := 64 + int((g*7919+i*104729)%3000)
	b := make([]byte, n)
	copy(b, strconv.FormatUint(g, 10)+"/"+strconv.FormatUint(i, 10)+"/")
	for k := 20; k < n; k++ {
		b[k] = byte(g + i + uint64(k))
	}
	return b
}

// killChild writes until it is killed, printing ACK lines after each Save.
func killChild() {
	fail := func(err error) {
		fmt.Fprintln(os.Stderr, "FATAL", err)
		os.Exit(2)
	}
	dir := os.Getenv("SEGLOG_DIR")
	seed, _ := strconv.ParseInt(os.Getenv("SEGLOG_SEED"), 10, 64)
	rng := rand.New(rand.NewSource(seed))
	l, err := Open(dir, Options{SegmentSize: 256 << 10, CleanAfterSegments: 1, CheckpointInterval: 50 * time.Millisecond})
	if err != nil {
		fail(err)
	}
	w := bufio.NewWriterSize(os.Stdout, 0)
	const groups = 8
	for {
		var ups []Update
		for g := uint64(1); g <= groups; g++ {
			if rng.Intn(3) != 0 {
				continue
			}
			s := l.Group(g)
			li, _ := s.LastIndex()
			fi, _ := s.FirstIndex()
			if li+1 < fi {
				li = fi - 1
			}
			n := uint64(1 + rng.Intn(6))
			var es []*raftpb.Entry
			for i := li + 1; i <= li+n; i++ {
				es = append(es, &raftpb.Entry{Term: new(uint64(1)), Index: new(i), Data: killPayload(g, i)})
			}
			ups = append(ups, Update{Group: g, Entries: es, HardState: &raftpb.HardState{Term: new(uint64(1)), Commit: new(li + n)}})
		}
		if len(ups) == 0 {
			continue
		}
		if err := l.Save(ups...); err != nil {
			fail(err)
		}
		for _, u := range ups {
			fmt.Fprintf(w, "ACK %d %d\n", u.Group, u.Entries[len(u.Entries)-1].GetIndex())
		}
		if rng.Intn(20) == 0 {
			g := uint64(1 + rng.Intn(groups))
			s := l.Group(g)
			li, _ := s.LastIndex()
			fi, _ := s.FirstIndex()
			if li > fi+20 {
				if _, err := s.CreateSnapshot(li-10, nil, bytes.Repeat([]byte{byte(g)}, 100<<10)); err != nil && !errors.Is(err, raft.ErrSnapOutOfDate) {
					fail(err)
				}
				if err := s.Compact(li - 10); err != nil {
					fail(err)
				}
			}
		}
		if rng.Intn(200) == 0 {
			if _, err := l.GC(); err != nil {
				fail(err)
			}
		}
	}
}
