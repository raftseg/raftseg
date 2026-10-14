// Copyright 2026 The raftseg authors.

package seglog

// Whole files that are missing, extra, swapped, stale or foreign: what an
// operator's mistake during a restore, a backup tool, or a bug can leave in
// a log's directory. The rules are those of hostile_disk_test.go, plus: a
// log that opens must keep working (it is written to, rolls segments, and is
// reopened), and must still hold every acknowledged write.

import (
	"fmt"
	"math"
	"math/rand"
	randv2 "math/rand/v2"
	"strings"
	"testing"

	"github.com/raftseg/raft"
	"github.com/raftseg/raftseg/internal/crashfs"
	"github.com/raftseg/raftseg/vfs"
)

// historyFixture is a disk fixture that also kept copies of its checkpoint
// and segment files as they were at several points of its history.
type historyFixture struct {
	*diskFixture
	ckpts [][]byte          // CHECKPOINT-A after every 40th operation
	segs  map[string][]byte // every segment file as last seen before its deletion
}

func buildHistoryFixture(t *testing.T, crash bool, seed int64) *historyFixture {
	t.Helper()
	mem := crashfs.New()
	hf := &historyFixture{segs: map[string][]byte{}}
	fs := crashfs.WithHook(mem, func(op crashfs.Op) error {
		if op.Kind == crashfs.OpRemove && strings.HasSuffix(op.Path, segSuffix) {
			if b, err := readWhole(mem, op.Path); err == nil {
				hf.segs[op.Path] = b
			}
		}
		return nil
	})
	l, err := Open(crashDir, memOpts(fs, harnessOpts))
	if err != nil {
		t.Fatal(err)
	}
	rng := newRand(seed)
	w := newWorld(4)
	states := []*world{w.clone()}
	for i := 0; i < 400; i++ {
		o := genOp(rng, w)
		if err := o.run(l, w); err != nil && err != raft.ErrSnapOutOfDate {
			t.Fatal(err)
		} else if err == nil {
			o.apply(w)
		}
		states = append(states, w.clone())
		if i%40 == 39 {
			l.exec(func() error { return nil }) // everything published
			if b, err := readWhole(mem, crashDir+"/CHECKPOINT-A"); err == nil {
				hf.ckpts = append(hf.ckpts, b)
			}
		}
	}
	name := "clean"
	if crash {
		l.crash()
		name = "crashed"
	} else if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	hf.diskFixture = &diskFixture{name: name, fs: mem, states: states, partials: map[int]*world{}}
	return hf
}

func readWhole(fs vfs.FS, path string) ([]byte, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	b := make([]byte, st.Size())
	if _, err := f.ReadAt(b, 0); err != nil && len(b) > 0 {
		return nil, err
	}
	return b, nil
}

func segmentNumbers(t *testing.T, fs vfs.FS) []uint64 {
	t.Helper()
	ns, err := listSegments(fs, crashDir+"/segments")
	if err != nil {
		t.Fatal(err)
	}
	return ns
}

func segPathN(n uint64) string { return crashDir + "/segments/" + segName(n) }

// keepsWorking writes to an opened image until segments roll, then reopens
// it, and checks every write since.
func keepsWorking(t *testing.T, what string, img *crashfs.FS, w *world) {
	t.Helper()
	l, err := Open(crashDir, memOpts(img, harnessOpts))
	if err != nil {
		t.Fatalf("%s: reopen to keep working: %v", what, err)
	}
	w = w.clone()
	rng := newRand(99)
	segs := len(segmentNumbers(t, img))
	for i := 0; i < 200 && len(segmentNumbers(t, img)) < segs+2; i++ {
		o := genOp(rng, w)
		if err := o.run(l, w); err != nil {
			if err == raft.ErrSnapOutOfDate {
				continue
			}
			t.Fatalf("%s: keep working, op %v: %v", what, o, err)
		}
		o.apply(w)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("%s: close: %v", what, err)
	}
	l, err = Open(crashDir, memOpts(img, Options{ScrubOnOpen: true}))
	if err != nil {
		t.Fatalf("%s: reopen after working: %v", what, err)
	}
	defer l.Close()
	if err := matchWorld(l, w); err != nil {
		t.Fatalf("%s: after working: %v", what, err)
	}
}

func TestHostileFiles(t *testing.T) {
	for _, crash := range []bool{false, true} {
		hf := buildHistoryFixture(t, crash, 3)
		other := buildHistoryFixture(t, crash, 4) // an unrelated log
		t.Run(hf.name, func(t *testing.T) {
			segs := segmentNumbers(t, hf.fs)
			if len(segs) < 3 || len(hf.ckpts) < 5 || len(hf.segs) == 0 {
				t.Fatalf("fixture too small: segments %v, %d checkpoints, %d deleted segments", segs, len(hf.ckpts), len(hf.segs))
			}
			newest, oldest := segs[len(segs)-1], segs[0]
			type tc struct {
				name string
				do   func(t *testing.T, fs vfs.FS)
				must verdict // -1: any outcome but silent wrong data
			}
			put := func(path string, b []byte) func(t *testing.T, fs vfs.FS) {
				return func(t *testing.T, fs vfs.FS) { rewrite(t, fs, path, b) }
			}
			zeros := make([]byte, 3*blockSize)
			var cases []tc
			for _, n := range []uint64{math.MaxUint64, math.MaxUint64 - 1, 1 << 63, newest + 1, newest + 7} {
				cases = append(cases,
					tc{fmt.Sprintf("stray zeroed segment %d", n), put(segPathN(n), zeros), -1},
					tc{fmt.Sprintf("stray empty segment %d", n), put(segPathN(n), nil), -1},
				)
			}
			cases = append(cases,
				tc{"stray segment 0", put(segPathN(0), zeros), -1},
				tc{"newest segment from an unrelated log appended", put(segPathN(newest+1), mustRead(t, other.fs, segPathN(segmentNumbers(t, other.fs)[0]))), -1},
				tc{"newest segment replaced by an unrelated log's", put(segPathN(newest), mustRead(t, other.fs, segPathN(segmentNumbers(t, other.fs)[len(segmentNumbers(t, other.fs))-1]))), -1},
				tc{"oldest segment replaced by the newest", put(segPathN(oldest), mustRead(t, hf.fs, segPathN(newest))), -1},
				tc{"two segments swapped", func(t *testing.T, fs vfs.FS) {
					a, b := mustRead(t, fs, segPathN(segs[0])), mustRead(t, fs, segPathN(segs[1]))
					rewrite(t, fs, segPathN(segs[0]), b)
					rewrite(t, fs, segPathN(segs[1]), a)
				}, -1},
				tc{"index sidecar of another segment", put(crashDir+"/segments/"+idxName(segs[0]), mustReadOr(t, hf.fs, crashDir+"/segments/"+idxName(segs[1]))), vIntact},
				tc{"checkpoints from an unrelated log", func(t *testing.T, fs vfs.FS) {
					b := mustRead(t, other.fs, crashDir+"/CHECKPOINT-A")
					rewrite(t, fs, crashDir+"/CHECKPOINT-A", b)
					rewrite(t, fs, crashDir+"/CHECKPOINT-B", b)
				}, -1},
				tc{"one checkpoint copy empty", put(crashDir+"/CHECKPOINT-B", nil), vIntact},
				tc{"both checkpoint copies empty", func(t *testing.T, fs vfs.FS) {
					rewrite(t, fs, crashDir+"/CHECKPOINT-A", nil)
					rewrite(t, fs, crashDir+"/CHECKPOINT-B", nil)
				}, vRefused},
				tc{"checkpoints removed", func(t *testing.T, fs vfs.FS) {
					fs.Remove(crashDir + "/CHECKPOINT-A")
					fs.Remove(crashDir + "/CHECKPOINT-B")
				}, vRefused},
				tc{"segments directory removed", func(t *testing.T, fs vfs.FS) {
					for _, n := range segmentNumbers(t, fs) {
						fs.Remove(segPathN(n))
					}
					names, _ := fs.List(crashDir + "/segments")
					for _, n := range names {
						fs.Remove(crashDir + "/segments/" + n)
					}
					fs.Remove(crashDir + "/segments")
				}, vRefused},
				tc{"snapshot files removed", func(t *testing.T, fs vfs.FS) {
					names, _ := fs.List(crashDir + "/snapshots")
					for _, n := range names {
						fs.Remove(crashDir + "/snapshots/" + n)
					}
				}, -1},
				tc{"a directory named like the next segment", func(t *testing.T, fs vfs.FS) {
					fs.MkdirAll(segPathN(newest+1), 0o755)
				}, -1},
				tc{"a directory named like a snapshot", func(t *testing.T, fs vfs.FS) {
					fs.MkdirAll(crashDir+"/snapshots/"+snapName(1, 5), 0o755)
				}, -1},
				tc{"unknown files everywhere", func(t *testing.T, fs vfs.FS) {
					for _, p := range []string{"/README", "/segments/README", "/snapshots/README", "/segments/x.seg", "/segments/-1.seg", "/segments/00000000000000000001.seg.bak", "/snapshots/1-2-3.snap", "/snapshots/a-b.snap", "/CHECKPOINT-C", "/segments/99999999999999999999999.seg"} {
						rewrite(t, fs, crashDir+p, []byte("operator was here"))
					}
				}, vIntact},
				tc{"LOCK removed", func(t *testing.T, fs vfs.FS) { fs.Remove(crashDir + "/LOCK") }, vIntact},
			)
			for i, ck := range hf.ckpts {
				ck := ck
				cases = append(cases, tc{fmt.Sprintf("checkpoints rolled back to after op %d", 40*(i+1)), func(t *testing.T, fs vfs.FS) {
					rewrite(t, fs, crashDir+"/CHECKPOINT-A", ck)
					rewrite(t, fs, crashDir+"/CHECKPOINT-B", ck)
				}, -1})
				cases = append(cases, tc{fmt.Sprintf("one checkpoint copy rolled back to after op %d", 40*(i+1)), put(crashDir+"/CHECKPOINT-A", ck), vIntact})
			}
			for path, b := range hf.segs {
				cases = append(cases, tc{fmt.Sprintf("deleted %s restored", path[len(crashDir)+1:]), put(path, b), -1})
			}
			counts := map[string]int{}
			for _, c := range cases {
				img := hf.fs.CrashClone(100, randv2.New(randv2.NewPCG(1, 1)))
				c.do(t, img)
				what := fmt.Sprintf("%s image, %s", hf.name, c.name)
				v, _ := judge(t, what, img, hf.diskFixture, c.must == vIntact)
				if c.must >= 0 && v != c.must {
					t.Errorf("%s: %s, want %s", what, verdictNames[v], verdictNames[c.must])
				}
				counts[verdictNames[v]]++
				t.Logf("%s: %s", c.name, verdictNames[v])
				if v == vIntact {
					keepsWorking(t, what, img, hf.final())
				}
			}
			t.Logf("%d cases: %v", len(cases), counts)
		})
	}
}

func mustRead(t *testing.T, fs vfs.FS, path string) []byte {
	t.Helper()
	b, err := readWhole(fs, path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustReadOr(t *testing.T, fs vfs.FS, path string) []byte {
	b, _ := readWhole(fs, path)
	return b
}

func newRand(seed int64) *rand.Rand { return rand.New(rand.NewSource(seed)) }
