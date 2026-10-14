// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0 (see LICENSE).
//
// This file replaces the github.com/pmezard/go-difflib dependency of the
// upstream package with a small standard-library implementation.

package datadriven

import (
	"fmt"
	"strings"
)

// splitLines splits s after each newline, keeping the newlines, and ensures
// the last line ends in one (like difflib.SplitLines).
func splitLines(s string) []string {
	lines := strings.SplitAfter(s, "\n")
	lines[len(lines)-1] += "\n"
	return lines
}

// maxLCSCells bounds the quadratic table of the line diff (about 64 MiB).
const maxLCSCells = 16 << 20

// unifiedDiff returns a unified diff from a to b with the given number of
// context lines, based on a longest common subsequence of lines.
func unifiedDiff(a, b []string, context int) string {
	type op struct {
		kind byte // ' ', '-', '+'
		ai   int  // index in a (for ' ' and '-')
		bi   int  // index in b (for ' ' and '+')
	}
	// Common prefix and suffix need no search.
	p := 0
	for p < len(a) && p < len(b) && a[p] == b[p] {
		p++
	}
	s := 0
	for s < len(a)-p && s < len(b)-p && a[len(a)-1-s] == b[len(b)-1-s] {
		s++
	}
	var ops []op
	for k := 0; k < p; k++ {
		ops = append(ops, op{' ', k, k})
	}
	ma, mb := a[p:len(a)-s], b[p:len(b)-s]
	n, m := len(ma), len(mb)
	if n*m > maxLCSCells {
		// Too large for a quadratic search: report the middle as replaced.
		for k := range ma {
			ops = append(ops, op{'-', p + k, p})
		}
		for k := range mb {
			ops = append(ops, op{'+', p + n, p + k})
		}
	} else {
		// lcs[i][j] is the LCS length of ma[i:] and mb[j:].
		lcs := make([][]int32, n+1)
		for i := range lcs {
			lcs[i] = make([]int32, m+1)
		}
		for i := n - 1; i >= 0; i-- {
			for j := m - 1; j >= 0; j-- {
				if ma[i] == mb[j] {
					lcs[i][j] = lcs[i+1][j+1] + 1
				} else {
					lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
				}
			}
		}
		i, j := 0, 0
		for i < n || j < m {
			switch {
			case i < n && j < m && ma[i] == mb[j]:
				ops = append(ops, op{' ', p + i, p + j})
				i++
				j++
			case i < n && (j == m || lcs[i+1][j] >= lcs[i][j+1]):
				// Removals before insertions, as difflib prints them.
				ops = append(ops, op{'-', p + i, p + j})
				i++
			default:
				ops = append(ops, op{'+', p + i, p + j})
				j++
			}
		}
	}
	for k := 0; k < s; k++ {
		ops = append(ops, op{' ', len(a) - s + k, len(b) - s + k})
	}
	var sb strings.Builder
	sb.WriteString("--- expected\n+++ actual\n")
	for k := 0; k < len(ops); {
		if ops[k].kind == ' ' {
			k++
			continue
		}
		// A hunk: from context before the first change to context after the
		// last change that is within 2*context of the previous one.
		start := max(0, k-context)
		end := k
		for end < len(ops) {
			if ops[end].kind != ' ' {
				end++
				continue
			}
			run := end
			for run < len(ops) && ops[run].kind == ' ' {
				run++
			}
			if run == len(ops) || run-end > 2*context {
				end = min(len(ops), end+context)
				break
			}
			end = run
		}
		aStart, bStart, aLen, bLen := -1, -1, 0, 0
		for _, o := range ops[start:end] {
			if o.kind != '+' {
				if aStart < 0 {
					aStart = o.ai
				}
				aLen++
			}
			if o.kind != '-' {
				if bStart < 0 {
					bStart = o.bi
				}
				bLen++
			}
		}
		if aStart < 0 {
			aStart = ops[start].ai
		}
		if bStart < 0 {
			bStart = ops[start].bi
		}
		// An empty range is numbered by the line before it, as in GNU diff.
		if aLen > 0 {
			aStart++
		}
		if bLen > 0 {
			bStart++
		}
		fmt.Fprintf(&sb, "@@ -%d,%d +%d,%d @@\n", aStart, aLen, bStart, bLen)
		for _, o := range ops[start:end] {
			line := ""
			if o.kind == '+' {
				line = b[o.bi]
			} else {
				line = a[o.ai]
			}
			sb.WriteByte(o.kind)
			sb.WriteString(line)
			if !strings.HasSuffix(line, "\n") {
				sb.WriteByte('\n')
			}
		}
		k = end
	}
	return sb.String()
}
