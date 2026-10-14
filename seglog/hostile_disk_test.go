// Copyright 2026 The raftseg authors.

package seglog

// Hostile on-disk data. A log is built, then every one of its files is
// damaged in turn: single bytes flipped or zeroed, whole sectors zeroed or
// overwritten with another sector (a misdirected write), the file cut short.
// For each damaged image, Open must do one of:
//
//   - recover every acknowledged write (required when the damaged file is
//     redundant: an index sidecar, or one of the two checkpoint copies);
//   - refuse, with an error;
//   - open, and fail with ErrCorrupt on reading what was damaged, with
//     Scrub reporting it;
//   - discard the newest commits, but only those that no later write
//     vouches for, only if the damage looks like an interrupted write, and
//     only while reporting it in RecoveryStats.Tail. (DiscardDamagedTail
//     lifts the second condition.)
//
// One case is let through silently, because no log can tell it apart from
// commits that were never written: the newest segment cut short exactly at
// a commit boundary, in the part no checkpoint covers. The log then opens
// without those commits, in a state it acknowledged earlier. (A cut there
// is below the filesystem's contract, which keeps a file's size once
// fdatasync returns: a filesystem bug, or someone truncating the file.)
//
// It must never panic, and never return data that differs from what was
// written without an error.
//
// SEGLOG_EXHAUSTIVE=1 damages every byte; by default a prime stride is used.

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/raftseg/raftseg/internal/crashfs"
	"github.com/raftseg/raftseg/vfs"
	randv2 "math/rand/v2"
)

type diskFixture struct {
	name     string
	fs       *crashfs.FS
	states   []*world
	partials map[int]*world
	// unvouched is the newest segment's path and the offset from which no
	// checkpoint covers it.
	unvouchedPath string
	unvouchedFrom int64
}

func (fx *diskFixture) final() *world { return fx.states[len(fx.states)-1] }

// buildDiskFixture runs the harness workload, then closes the log cleanly
// (everything checkpointed) or simulates a process crash (the newest commits
// are only in the segments).
func buildDiskFixture(t *testing.T, crash bool, seed int64, nops int) *diskFixture {
	t.Helper()
	mem := crashfs.New()
	l, err := Open(crashDir, memOpts(mem, harnessOpts))
	if err != nil {
		t.Fatal(err)
	}
	var acked atomic.Int64
	states, partials := workloadP(t, l, seed, nops, &acked)
	name := "clean"
	if crash {
		l.crash()
		name = "crashed"
	} else if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	fx := &diskFixture{name: name, fs: mem, states: states, partials: partials}
	c, err := decodeCheckpoint(fileBytes(t, mem, crashDir+"/CHECKPOINT-A"))
	if err != nil {
		t.Fatal(err)
	}
	segs, _ := listSegments(mem, crashDir+"/segments")
	if newest := segs[len(segs)-1]; newest >= c.seg {
		fx.unvouchedPath = crashDir + "/segments/" + segName(newest)
		if newest == c.seg {
			fx.unvouchedFrom = c.off
		}
	}
	return fx
}

// imageFiles lists every file of the log in a fixed order.
func imageFiles(fs vfs.FS) []string {
	var files []string
	for _, d := range []string{crashDir, crashDir + "/segments", crashDir + "/snapshots"} {
		names, _ := fs.List(d)
		sort.Strings(names)
		for _, n := range names {
			p := d + "/" + n
			if st, err := fs.Stat(p); err == nil && !st.IsDir() {
				files = append(files, p)
			}
		}
	}
	return files
}

// redundant reports whether the log must survive the loss of this file
// with nothing lost: index sidecars are rebuilt from their segments, and
// each checkpoint has an identical twin.
func redundant(path string) bool {
	return strings.HasSuffix(path, idxSuffix) || strings.HasSuffix(path, "/CHECKPOINT-A") || strings.HasSuffix(path, "/CHECKPOINT-B") || strings.HasSuffix(path, "/LOCK")
}

func fileSize(t *testing.T, fs vfs.FS, path string) int64 {
	t.Helper()
	st, err := fs.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

// rewrite replaces a file's contents.
func rewrite(t *testing.T, fs vfs.FS, path string, data []byte) {
	t.Helper()
	f, err := fs.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > 0 {
		if _, err := f.WriteAt(data, 0); err != nil {
			t.Fatal(err)
		}
	}
	f.Close()
}

type verdict int

const (
	vIntact    verdict = iota // every acknowledged write recovered
	vRefused                  // Open failed
	vDetected                 // damaged data fails to read with ErrCorrupt
	vTail                     // newest commits discarded, reported as a tail
	vCutCommit                // newest segment cut at a commit boundary nothing vouches for
	nVerdicts
)

var verdictNames = [nVerdicts]string{"intact", "refused", "detected", "tail discarded", "cut at an unvouched commit boundary"}

func isCorruptErr(err error) bool {
	return err != nil && (errors.Is(err, ErrCorrupt) || strings.Contains(err.Error(), "corruption"))
}

// judge opens a damaged image and checks the outcome against the rules at
// the top of this file. It also opens a copy with DiscardDamagedTail, which
// may in addition discard newest commits whose damage does not look torn.
func judge(t *testing.T, what string, img *crashfs.FS, fx *diskFixture, mustSurvive bool) (verdict, *TailDamage) {
	return judgeCut(t, what, img, fx, mustSurvive, false)
}

// judgeCut is judge for damage that may be a cut of the newest segment in
// its unvouched part (see the top of this file).
func judgeCut(t *testing.T, what string, img *crashfs.FS, fx *diskFixture, mustSurvive, unvouchedCut bool) (verdict, *TailDamage) {
	t.Helper()
	lenient := img.CrashClone(100, randv2.New(randv2.NewPCG(1, 1)))
	v, tail := judgeOpen(t, what, img, fx, mustSurvive, Options{}, unvouchedCut)
	if tail != nil && !tail.TornLike {
		t.Fatalf("%s: discarded a tail that does not look torn (%+v) without DiscardDamagedTail", what, *tail)
	}
	lv, ltail := judgeOpen(t, what+" (DiscardDamagedTail)", lenient, fx, mustSurvive, Options{DiscardDamagedTail: true}, unvouchedCut)
	switch {
	case lv == v:
	case v == vRefused && ltail != nil && !ltail.TornLike:
		// The one difference the option makes: it discards damage that does
		// not look torn (which may have held nothing acknowledged at all).
		return v, ltail
	default:
		t.Fatalf("%s: %s by default, %s with DiscardDamagedTail", what, verdictNames[v], verdictNames[lv])
	}
	return v, tail
}

func judgeOpen(t *testing.T, what string, img *crashfs.FS, fx *diskFixture, mustSurvive bool, opts Options, unvouchedCut bool) (verdict, *TailDamage) {
	t.Helper()
	l, err := Open(crashDir, memOpts(img, opts))
	if err != nil {
		if mustSurvive {
			t.Fatalf("%s: damage to a redundant file: open: %v", what, err)
		}
		return vRefused, nil
	}
	defer l.Close()
	final := fx.final()
	if err := matchWorld(l, final); err == nil {
		return vIntact, l.recovery.Tail
	}
	if mustSurvive {
		t.Fatalf("%s: damage to a redundant file lost data: %v", what, matchWorld(l, final))
	}
	detected, silent := false, false
	var silentErr error
	for id, m := range final.g {
		err := matchGroup(l.Group(id), m)
		switch {
		case err == nil:
		case isCorruptErr(err):
			detected = true
		default:
			silent, silentErr = true, fmt.Errorf("group %d: %v", id, err)
		}
	}
	if detected {
		rep, err := l.Scrub()
		if err != nil {
			t.Fatalf("%s: scrub: %v", what, err)
		}
		if len(rep.Problems) == 0 {
			t.Fatalf("%s: reads fail with ErrCorrupt but Scrub finds nothing", what)
		}
	}
	if !silent {
		return vDetected, l.recovery.Tail
	}
	tail := l.recovery.Tail
	// What remains must be the state after some earlier acknowledged
	// operation: a prefix of the history.
	prefix := false
	for k := len(fx.states) - 2; k >= 0 && !prefix; k-- {
		prefix = matchWorld(l, fx.states[k]) == nil
		if p := fx.partials[k+1]; p != nil && !prefix {
			prefix = matchWorld(l, p) == nil
		}
	}
	if tail == nil {
		if unvouchedCut && prefix && !detected {
			return vCutCommit, nil
		}
		t.Fatalf("%s: data differs without an error or a reported tail: %v", what, silentErr)
	}
	if !prefix && !detected {
		t.Fatalf("%s: discarded a tail (%+v) but the rest is no earlier state: %v", what, *tail, silentErr)
	}
	return vTail, tail
}

type damage struct {
	what  string
	apply func(t *testing.T, fs vfs.FS)
	cut   int64 // for a truncation, the new size; otherwise -1
}

// damages lists the corruptions applied to path, a file of size n.
func damages(path string, n int64, stride int64) []damage {
	var out []damage
	flip := func(off int64, mask byte, zero bool) damage {
		what := fmt.Sprintf("%s: byte %d ^ %#x", path, off, mask)
		if zero {
			what = fmt.Sprintf("%s: byte %d zeroed", path, off)
		}
		return damage{what: what, cut: -1, apply: func(t *testing.T, fs vfs.FS) {
			f, err := fs.OpenReadWrite(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			b := []byte{0}
			f.ReadAt(b, off)
			if zero {
				b[0] = 0
			} else {
				b[0] ^= mask
			}
			f.WriteAt(b, off)
		}}
	}
	masks := []byte{0x01, 0x80, 0xff, 0x10}
	k := 0
	for off := int64(0); off < n; off += stride {
		if k%5 == 4 {
			out = append(out, flip(off, 0, true))
		} else {
			out = append(out, flip(off, masks[k%len(masks)], false))
		}
		k++
	}
	// Whole sectors: lost (zeroed), or replaced by another sector of the
	// same file (a misdirected write).
	const sector = 512
	nsec := (n + sector - 1) / sector
	secStride := max(1, stride/sector)
	for s := int64(0); s < nsec; s += secStride {
		s := s
		out = append(out, damage{what: fmt.Sprintf("%s: sector %d zeroed", path, s), cut: -1, apply: func(t *testing.T, fs vfs.FS) {
			f, _ := fs.OpenReadWrite(path)
			defer f.Close()
			lo := s * sector
			f.WriteAt(make([]byte, min(sector, n-lo)), lo)
		}})
		if nsec > 1 {
			src := (s + 1 + s*7%(nsec-1)) % nsec
			out = append(out, damage{what: fmt.Sprintf("%s: sector %d replaced by sector %d", path, s, src), cut: -1, apply: func(t *testing.T, fs vfs.FS) {
				f, _ := fs.OpenReadWrite(path)
				defer f.Close()
				buf := make([]byte, sector)
				m, _ := f.ReadAt(buf, src*sector)
				lo := s * sector
				f.WriteAt(buf[:min(int64(m), n-lo)], lo)
			}})
		}
	}
	// Truncation.
	for cut := int64(0); cut < n; cut += max(1, stride*3) {
		cut := cut
		out = append(out, damage{what: fmt.Sprintf("%s: cut to %d bytes", path, cut), cut: cut, apply: func(t *testing.T, fs vfs.FS) {
			f, _ := fs.Open(path)
			buf := make([]byte, cut)
			f.ReadAt(buf, 0)
			f.Close()
			rewrite(t, fs, path, buf)
		}})
	}
	out = append(out, damage{what: fmt.Sprintf("%s: deleted", path), cut: -1, apply: func(t *testing.T, fs vfs.FS) {
		if err := fs.Remove(path); err != nil {
			t.Fatal(err)
		}
	}})
	return out
}

func TestDamageEveryFile(t *testing.T) {
	stride := int64(61)
	if testing.Short() {
		stride = 1009
	}
	if os.Getenv("SEGLOG_EXHAUSTIVE") != "" {
		stride = 1
	}
	for _, crash := range []bool{false, true} {
		fx := buildDiskFixture(t, crash, 7, 90)
		t.Run(fx.name, func(t *testing.T) {
			var counts [nVerdicts]int
			var bitRotTails []string
			files := imageFiles(fx.fs)
			t.Logf("%d files", len(files))
			for _, path := range files {
				n := fileSize(t, fx.fs, path)
				st := stride
				if n < 8<<10 && stride > 1 {
					st = max(1, stride/16) // small files: nearly every byte
				}
				for _, d := range damages(path, n, st) {
					img := fx.fs.CrashClone(100, randv2.New(randv2.NewPCG(1, 1)))
					d.apply(t, img)
					unvouched := d.cut >= 0 && path == fx.unvouchedPath && d.cut >= fx.unvouchedFrom
					v, tail := judgeCut(t, fmt.Sprintf("%s image, %s", fx.name, d.what), img, fx, redundant(path), unvouched)
					counts[v]++
					if tail != nil && !tail.TornLike {
						bitRotTails = append(bitRotTails, d.what)
					}
				}
			}
			var parts []string
			for v, c := range counts {
				parts = append(parts, fmt.Sprintf("%s %d", verdictNames[v], c))
			}
			t.Logf("outcomes: %s", strings.Join(parts, ", "))
			if len(bitRotTails) > 0 {
				t.Logf("%d images are refused by default because their newest commits are damaged in a way an interrupted write does not explain; DiscardDamagedTail discards those commits. For example: %s", len(bitRotTails), bitRotTails[0])
			}
		})
	}
}
