// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0 (see LICENSE).

package datadriven

import (
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var hunkRE = regexp.MustCompile(`^@@ -(\d+),(\d+) \+(\d+),(\d+) @@$`)

// applyDiff applies a unified diff produced by unifiedDiff to a, checking
// every hunk header and context line, and returns the result.
func applyDiff(t *testing.T, a []string, diff string) []string {
	t.Helper()
	lines := strings.SplitAfter(diff, "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) < 2 || lines[0] != "--- expected\n" || lines[1] != "+++ actual\n" {
		t.Fatalf("bad header in\n%s", diff)
	}
	var out []string
	ai := 0 // next line of a not yet copied
	for k := 2; k < len(lines); {
		h := hunkRE.FindStringSubmatch(strings.TrimSuffix(lines[k], "\n"))
		if h == nil {
			t.Fatalf("expected hunk header, got %q in\n%s", lines[k], diff)
		}
		aStart, _ := strconv.Atoi(h[1])
		aLen, _ := strconv.Atoi(h[2])
		bStart, _ := strconv.Atoi(h[3])
		bLen, _ := strconv.Atoi(h[4])
		if aLen > 0 {
			aStart-- // 1-based
		}
		if aStart < ai {
			t.Fatalf("hunks overlap or go backwards at %q", lines[k])
		}
		out = append(out, a[ai:aStart]...)
		ai = aStart
		if bLen > 0 {
			bStart--
		}
		if bStart != len(out) {
			t.Fatalf("hunk %q: b starts at %d, have %d lines", lines[k], bStart, len(out))
		}
		k++
		na, nb := 0, 0
		for k < len(lines) && !strings.HasPrefix(lines[k], "@@") {
			l := lines[k]
			body := l[1:]
			switch l[0] {
			case ' ':
				if a[ai] != body {
					t.Fatalf("context %q does not match a[%d]=%q", body, ai, a[ai])
				}
				out = append(out, body)
				ai++
				na++
				nb++
			case '-':
				if a[ai] != body {
					t.Fatalf("removed %q does not match a[%d]=%q", body, ai, a[ai])
				}
				ai++
				na++
			case '+':
				out = append(out, body)
				nb++
			default:
				t.Fatalf("bad line %q", l)
			}
			k++
		}
		if na != aLen || nb != bLen {
			t.Fatalf("hunk %v counts %d,%d, lines %d,%d", h, aLen, bLen, na, nb)
		}
	}
	return append(out, a[ai:]...)
}

func randLines(r *rand.Rand, n, alphabet int) []string {
	l := make([]string, n)
	for i := range l {
		l[i] = fmt.Sprintf("line %d\n", r.Intn(alphabet))
	}
	return l
}

func TestUnifiedDiffApplies(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for iter := 0; iter < 3000; iter++ {
		a := randLines(r, r.Intn(30), 1+r.Intn(6))
		var b []string
		switch r.Intn(3) {
		case 0:
			b = randLines(r, r.Intn(30), 1+r.Intn(6))
		default: // b is an edit of a
			for _, l := range a {
				switch r.Intn(6) {
				case 0: // drop
				case 1:
					b = append(b, l, fmt.Sprintf("new %d\n", r.Intn(4)))
				case 2:
					b = append(b, fmt.Sprintf("changed %d\n", r.Intn(4)))
				default:
					b = append(b, l)
				}
			}
		}
		context := r.Intn(5)
		d := unifiedDiff(a, b, context)
		got := applyDiff(t, a, d)
		if strings.Join(got, "") != strings.Join(b, "") {
			t.Fatalf("iteration %d: patch gives\n%q\nwant\n%q\ndiff:\n%s", iter, got, b, d)
		}
		// Only changed lines and at most context lines around them are shown.
		changed := 0
		for _, l := range strings.Split(d, "\n") {
			if strings.HasPrefix(l, "+") && !strings.HasPrefix(l, "+++") || strings.HasPrefix(l, "-") && !strings.HasPrefix(l, "---") {
				changed++
			}
		}
		if lcs := lcsLen(a, b); changed != len(a)+len(b)-2*lcs {
			t.Fatalf("iteration %d: %d changed lines, minimal is %d\n%s", iter, changed, len(a)+len(b)-2*lcs, d)
		}
	}
}

func lcsLen(a, b []string) int {
	prev := make([]int, len(b)+1)
	for i := range a {
		cur := make([]int, len(b)+1)
		for j := range b {
			if a[i] == b[j] {
				cur[j+1] = prev[j] + 1
			} else {
				cur[j+1] = max(prev[j+1], cur[j])
			}
		}
		prev = cur
	}
	return prev[len(b)]
}

func TestUnifiedDiffShape(t *testing.T) {
	a := splitLines("a\nb\nc\nd\ne\nf\ng\nh\n")
	b := splitLines("a\nb\nc\nX\ne\nf\ng\nh\n")
	want := "--- expected\n+++ actual\n@@ -2,5 +2,5 @@\n b\n c\n-d\n+X\n e\n f\n"
	if got := unifiedDiff(a, b, 2); got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
	if got := unifiedDiff(a, a, 3); got != "--- expected\n+++ actual\n" {
		t.Fatalf("identical inputs gave %q", got)
	}
	// A line without a trailing newline is still printed on its own line.
	if got := unifiedDiff([]string{"x"}, []string{"y"}, 0); got != "--- expected\n+++ actual\n@@ -1,1 +1,1 @@\n-x\n+y\n" {
		t.Fatalf("got %q", got)
	}
}

// Large inputs must not allocate a quadratic table.
func TestUnifiedDiffLarge(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	a := randLines(r, 20000, 1000)
	b := append([]string{"head\n"}, randLines(r, 20000, 1000)...)
	b = append(b, a[len(a)-10:]...)
	d := unifiedDiff(a, b, 3)
	got := applyDiff(t, a, d)
	if strings.Join(got, "") != strings.Join(b, "") {
		t.Fatal("large diff does not apply")
	}
}

func FuzzUnifiedDiff(f *testing.F) {
	f.Add("a\nb\nc\n", "a\nc\nd\n", uint8(3))
	f.Add("", "x", uint8(0))
	f.Fuzz(func(t *testing.T, x, y string, context uint8) {
		a, b := splitLines(x), splitLines(y)
		if len(a) > 200 || len(b) > 200 {
			return
		}
		d := unifiedDiff(a, b, int(context%8))
		if got := applyDiff(t, a, d); strings.Join(got, "") != strings.Join(b, "") {
			t.Fatalf("patch does not reproduce b\n%s", d)
		}
	})
}
