// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

// Package assert reports failed checks and lets the test continue, with the
// same semantics as github.com/stretchr/testify/assert for the functions
// provided.
package assert

import "github.com/raftseg/raftseg/internal/testutil"

// TestingT is what the checks need from a test.
type TestingT = testutil.T

func Equal(t TestingT, expected, actual any, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.Equal(t, expected, actual, msgAndArgs)
}

func NotEqual(t TestingT, expected, actual any, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.NotEqual(t, expected, actual, msgAndArgs)
}

func NoError(t TestingT, err error, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.NoError(t, err, msgAndArgs)
}

func Error(t TestingT, err error, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.Error(t, err, msgAndArgs)
}

func ErrorIs(t TestingT, err, target error, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.ErrorIs(t, err, target, msgAndArgs)
}

func True(t TestingT, value bool, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.True(t, value, msgAndArgs)
}

func False(t TestingT, value bool, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.False(t, value, msgAndArgs)
}

func Len(t TestingT, object any, length int, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.LenIs(t, object, length, msgAndArgs)
}

func Empty(t TestingT, object any, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.Empty(t, object, msgAndArgs)
}

func NotEmpty(t TestingT, object any, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.NotEmpty(t, object, msgAndArgs)
}

func Zero(t TestingT, i any, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.Zero(t, i, msgAndArgs)
}

func NotZero(t TestingT, i any, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.NotZero(t, i, msgAndArgs)
}

func Nil(t TestingT, object any, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.Nil(t, object, msgAndArgs)
}

func NotNil(t TestingT, object any, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.NotNil(t, object, msgAndArgs)
}

func Panics(t TestingT, f func(), msgAndArgs ...any) bool {
	t.Helper()
	return testutil.Panics(t, f, msgAndArgs)
}

func GreaterOrEqual(t TestingT, e1, e2 any, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.Ordered(t, e1, e2, func(c int) bool { return c >= 0 }, "greater than or equal to", msgAndArgs)
}

func LessOrEqual(t TestingT, e1, e2 any, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.Ordered(t, e1, e2, func(c int) bool { return c <= 0 }, "less than or equal to", msgAndArgs)
}

func Greater(t TestingT, e1, e2 any, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.Ordered(t, e1, e2, func(c int) bool { return c > 0 }, "greater than", msgAndArgs)
}

func Less(t TestingT, e1, e2 any, msgAndArgs ...any) bool {
	t.Helper()
	return testutil.Ordered(t, e1, e2, func(c int) bool { return c < 0 }, "less than", msgAndArgs)
}
