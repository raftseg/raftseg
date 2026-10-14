// Copyright 2026 The raftseg authors.

package seglog

// Liveness tracking. Each group keeps, in index order, the runs of its live
// entries and the segment each run lives in; Log.live counts live entries
// per segment. A segment with no live entries can be deleted, whatever the
// age of its neighbours, and a mostly-dead segment can be cleaned by copying
// its few live entries forward. All of this is writer-owned state: it is
// restored from the checkpoint and replay at Open and changed only on the
// writer goroutine.

type segRun struct {
	seg, lo, hi uint64 // entries [lo, hi] live in segment seg
}

func (l *Log) liveAdd(seg, n uint64) { l.live[seg] += n }

func (l *Log) liveSub(seg, n uint64) {
	if v := l.live[seg]; v > n {
		l.live[seg] = v - n
	} else {
		delete(l.live, seg)
	}
}

// runsAppend records entries [lo, hi] written to seg. lo must follow the
// group's last live entry.
func (l *Log) runsAppend(g *group, seg, lo, hi uint64) {
	l.liveAdd(seg, hi-lo+1)
	if n := len(g.runs); n > 0 && g.runs[n-1].seg == seg && g.runs[n-1].hi+1 == lo {
		g.runs[n-1].hi = hi
		return
	}
	g.runs = append(g.runs, segRun{seg, lo, hi})
}

// runsRemoveFrom drops entries >= i (a suffix being overwritten).
func (l *Log) runsRemoveFrom(g *group, i uint64) {
	for k := len(g.runs) - 1; k >= 0; k-- {
		r := &g.runs[k]
		if r.lo >= i {
			l.liveSub(r.seg, r.hi-r.lo+1)
			g.runs = g.runs[:k]
			continue
		}
		if r.hi >= i {
			l.liveSub(r.seg, r.hi-i+1)
			r.hi = i - 1
		}
		return
	}
}

// runsRemoveBelow drops entries < i (compaction).
func (l *Log) runsRemoveBelow(g *group, i uint64) {
	k := 0
	for ; k < len(g.runs); k++ {
		r := &g.runs[k]
		if r.hi < i {
			l.liveSub(r.seg, r.hi-r.lo+1)
			continue
		}
		if r.lo < i {
			l.liveSub(r.seg, i-r.lo)
			r.lo = i
		}
		break
	}
	g.runs = append([]segRun(nil), g.runs[k:]...)
}

func (l *Log) runsClear(g *group) {
	for _, r := range g.runs {
		l.liveSub(r.seg, r.hi-r.lo+1)
	}
	g.runs = nil
}

// runsMove records that the live entries [lo, hi] were copied to seg.
func (l *Log) runsMove(g *group, lo, hi, seg uint64) {
	out := make([]segRun, 0, len(g.runs)+2)
	add := func(r segRun) {
		if n := len(out); n > 0 && out[n-1].seg == r.seg && out[n-1].hi+1 == r.lo {
			out[n-1].hi = r.hi
			return
		}
		out = append(out, r)
	}
	inserted := false
	for _, r := range g.runs {
		switch {
		case r.hi < lo:
			add(r)
		case r.lo > hi:
			if !inserted {
				add(segRun{seg, lo, hi})
				inserted = true
			}
			add(r)
		default:
			if r.lo < lo {
				add(segRun{r.seg, r.lo, lo - 1})
			}
			l.liveSub(r.seg, minU(r.hi, hi)-maxU(r.lo, lo)+1)
			if !inserted {
				add(segRun{seg, lo, hi})
				inserted = true
			}
			if r.hi > hi {
				add(segRun{r.seg, hi + 1, r.hi})
			}
		}
	}
	if !inserted {
		add(segRun{seg, lo, hi})
	}
	l.liveAdd(seg, hi-lo+1)
	g.runs = out
}

func minU(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxU(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
