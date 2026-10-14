// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package testutil

import (
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"
)

// The full comparison with testify lives in testdata/testifycompat, outside
// this module. These cases pin the behaviour that matters most: a check must
// never pass where testify would fail.

type rec struct {
	failed  bool
	msg     string
	stopped bool
}

func (r *rec) Helper() {}
func (r *rec) Errorf(f string, a ...any) {
	r.failed = true
	r.msg = fmt.Sprintf(f, a...)
}
func (r *rec) FailNow() { r.stopped = true }

func passes(f func(T) bool) bool {
	r := &rec{}
	ok := f(r)
	if ok == r.failed {
		panic("return value disagrees with reported failure")
	}
	return ok
}

type myErr struct{}

func (*myErr) Error() string { return "e" }

func TestChecks(t *testing.T) {
	one := 1
	var typedNil *myErr
	cases := []struct {
		name string
		f    func(T) bool
		want bool
	}{
		{"bytes equal", func(t T) bool { return Equal(t, []byte("a"), []byte("a"), nil) }, true},
		{"nil vs empty bytes", func(t T) bool { return Equal(t, []byte(nil), []byte{}, nil) }, false},
		{"nil vs empty slice", func(t T) bool { return Equal(t, []int(nil), []int{}, nil) }, false},
		{"int vs int64", func(t T) bool { return Equal(t, 1, int64(1), nil) }, false},
		{"nil vs typed nil", func(t T) bool { return Equal(t, nil, (*int)(nil), nil) }, false},
		{"pointers to equal", func(t T) bool { a, b := 1, 1; return Equal(t, &a, &b, nil) }, true},
		{"funcs never equal", func(t T) bool { f := func() {}; return Equal(t, f, f, nil) }, false},
		{"funcs never not-equal", func(t T) bool { return NotEqual(t, func() {}, 1, nil) }, false},
		{"NaN not equal to itself", func(t T) bool { return Equal(t, math.NaN(), math.NaN(), nil) }, false},
		{"NaN unordered", func(t T) bool { return GreaterOrEqualTest(t, math.NaN(), 1.0) }, false},
		{"NaN unordered 2", func(t T) bool { return GreaterOrEqualTest(t, 1.0, math.NaN()) }, false},
		{"mixed types unordered", func(t T) bool { return GreaterOrEqualTest(t, 2, int64(1)) }, false},
		{"time ordered", func(t T) bool { return GreaterOrEqualTest(t, time.Unix(2, 0), time.Unix(1, 0)) }, true},
		{"bytes ordered", func(t T) bool { return GreaterOrEqualTest(t, []byte("b"), []byte("a")) }, true},
		{"structs unordered", func(t T) bool { return GreaterOrEqualTest(t, struct{}{}, struct{}{}) }, false},
		{"typed nil error is an error", func(t T) bool { return NoError(t, error(typedNil), nil) }, false},
		{"ErrorIs wrapped", func(t T) bool { return ErrorIs(t, fmt.Errorf("x: %w", io.EOF), io.EOF, nil) }, true},
		{"ErrorIs other", func(t T) bool { return ErrorIs(t, errors.New("EOF"), io.EOF, nil) }, false},
		{"Empty pointer to zero", func(t T) bool { z := 0; return Empty(t, &z, nil) }, true},
		{"Empty pointer to one", func(t T) bool { return Empty(t, &one, nil) }, false},
		{"Empty zero array", func(t T) bool { return Empty(t, [2]int{}, nil) }, true},
		{"Empty zero time", func(t T) bool { return Empty(t, time.Time{}, nil) }, true},
		{"Zero empty slice", func(t T) bool { return Zero(t, []int{}, nil) }, false},
		{"Nil typed nil", func(t T) bool { return Nil(t, (*int)(nil), nil) }, true},
		{"Nil zero int", func(t T) bool { return Nil(t, 0, nil) }, false},
		{"Len of int", func(t T) bool { return LenIs(t, 3, 0, nil) }, false},
		{"Len of nil map", func(t T) bool { return LenIs(t, map[int]int(nil), 0, nil) }, true},
		{"Panics nil", func(t T) bool { return Panics(t, func() { panic(nil) }, nil) }, true},
		{"Panics none", func(t T) bool { return Panics(t, func() {}, nil) }, false},
	}
	for _, c := range cases {
		if got := passes(c.f); got != c.want {
			t.Errorf("%s: passed=%v, want %v", c.name, got, c.want)
		}
	}
}

func GreaterOrEqualTest(t T, a, b any) bool {
	return Ordered(t, a, b, func(c int) bool { return c >= 0 }, ">=", nil)
}

func TestMessages(t *testing.T) {
	r := &rec{}
	Equal(r, 1, 2, []any{"step %d of %s", 3, "x"})
	if !strings.Contains(r.msg, "step 3 of x") || !strings.Contains(r.msg, "expected: 1") || !strings.Contains(r.msg, "actual  : 2") {
		t.Fatalf("message %q", r.msg)
	}
	if r.stopped {
		t.Fatal("a check must not stop the test by itself")
	}
}
