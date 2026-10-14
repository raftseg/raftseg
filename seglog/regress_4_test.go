// Copyright 2026 The raftseg authors.

package seglog

import (
	"testing"
	"time"
)

// Many groups, each with a small cache, together over budget: the halving
// loop reaches one entry per group and spins forever.
func TestRegressEvictHangManyGroups(t *testing.T) {
	l := open(t, t.TempDir(), Options{CacheBytes: 8 << 10})
	done := make(chan struct{})
	go func() {
		for g := uint64(1); g <= 20; g++ {
			l.Save(Update{Group: g, Entries: ents(1, 1, 1, 500)})
		}
		close(done)
	}()
	select {
	case <-done:
		l.Close()
	case <-time.After(3 * time.Second):
		t.Fatal("hang: 20 groups x 1 entry x ~520B > 8KiB budget")
	}
}
