// Copyright 2026 The raftseg authors.

package seglog

import (
	"bytes"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raftseg/internal/crashfs"
)

// evictCaches never terminates when single-entry caches exceed CacheBytes.
func TestRegressEvictHang(t *testing.T) {
	l := open(t, t.TempDir(), Options{CacheBytes: 1024})
	done := make(chan error, 1)
	go func() { done <- l.Save(Update{Group: 1, Entries: ents(1, 1, 1, 4096)}) }()
	select {
	case err := <-done:
		t.Logf("save returned %v", err)
		l.Close()
	case <-time.After(3 * time.Second):
		t.Fatal("Save hung: evictCaches loops forever holding l.mu")
	}
}

// A failed finalize before a snapshot request acknowledges earlier requests.
func TestRegressSnapshotPathAcksFailedCommit(t *testing.T) {
	mem := crashfs.New()
	var armed atomic.Bool
	fs := crashfs.WithHook(mem, func(op crashfs.Op) error {
		if armed.Load() && op.Kind == crashfs.OpSyncData && strings.Contains(op.Path, "/segments/") {
			return crashfs.ErrInjected
		}
		return nil
	})
	l, err := Open(crashDir, memOpts(fs, Options{}))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	r0 := &request{updates: []Update{{Group: 1, Entries: ents(1, 1, 3, 100)}}, done: make(chan error, 1)}
	r1 := &request{updates: []Update{{Group: 2, Snapshot: &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(10)), Term: new(uint64(2))}}}}, done: make(chan error, 1)}
	if err := l.exec(func() error {
		for _, r := range []*request{r0, r1} {
			enc, err := encodeUpdates(r.updates)
			if err != nil {
				return err
			}
			r.enc = enc
		}
		armed.Store(true)
		l.process([]*request{r0, r1})
		l.drain()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	errs := []error{<-r0.done, <-r1.done}
	if errs[0] == nil {
		t.Fatalf("request 0 acknowledged although its fsync failed: errs=%v", errs)
	}
}

var _ = bytes.Equal
