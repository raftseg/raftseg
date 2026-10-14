// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

// Package require reports a failed check and stops the test, with the same
// semantics as github.com/stretchr/testify/require for the functions
// provided.
package require

import "github.com/raftseg/raftseg/internal/testutil"

// TestingT is what the checks need from a test.
type TestingT = testutil.T

func Equal(t TestingT, expected, actual any, msgAndArgs ...any) {
	t.Helper()
	if !testutil.Equal(t, expected, actual, msgAndArgs) {
		t.FailNow()
	}
}

func NotEqual(t TestingT, expected, actual any, msgAndArgs ...any) {
	t.Helper()
	if !testutil.NotEqual(t, expected, actual, msgAndArgs) {
		t.FailNow()
	}
}

func NoError(t TestingT, err error, msgAndArgs ...any) {
	t.Helper()
	if !testutil.NoError(t, err, msgAndArgs) {
		t.FailNow()
	}
}

func Error(t TestingT, err error, msgAndArgs ...any) {
	t.Helper()
	if !testutil.Error(t, err, msgAndArgs) {
		t.FailNow()
	}
}

func ErrorIs(t TestingT, err, target error, msgAndArgs ...any) {
	t.Helper()
	if !testutil.ErrorIs(t, err, target, msgAndArgs) {
		t.FailNow()
	}
}

func True(t TestingT, value bool, msgAndArgs ...any) {
	t.Helper()
	if !testutil.True(t, value, msgAndArgs) {
		t.FailNow()
	}
}

func False(t TestingT, value bool, msgAndArgs ...any) {
	t.Helper()
	if !testutil.False(t, value, msgAndArgs) {
		t.FailNow()
	}
}

func Len(t TestingT, object any, length int, msgAndArgs ...any) {
	t.Helper()
	if !testutil.LenIs(t, object, length, msgAndArgs) {
		t.FailNow()
	}
}

func Empty(t TestingT, object any, msgAndArgs ...any) {
	t.Helper()
	if !testutil.Empty(t, object, msgAndArgs) {
		t.FailNow()
	}
}

func NotEmpty(t TestingT, object any, msgAndArgs ...any) {
	t.Helper()
	if !testutil.NotEmpty(t, object, msgAndArgs) {
		t.FailNow()
	}
}

func Zero(t TestingT, i any, msgAndArgs ...any) {
	t.Helper()
	if !testutil.Zero(t, i, msgAndArgs) {
		t.FailNow()
	}
}

func NotZero(t TestingT, i any, msgAndArgs ...any) {
	t.Helper()
	if !testutil.NotZero(t, i, msgAndArgs) {
		t.FailNow()
	}
}

func Nil(t TestingT, object any, msgAndArgs ...any) {
	t.Helper()
	if !testutil.Nil(t, object, msgAndArgs) {
		t.FailNow()
	}
}

func NotNil(t TestingT, object any, msgAndArgs ...any) {
	t.Helper()
	if !testutil.NotNil(t, object, msgAndArgs) {
		t.FailNow()
	}
}

func Panics(t TestingT, f func(), msgAndArgs ...any) {
	t.Helper()
	if !testutil.Panics(t, f, msgAndArgs) {
		t.FailNow()
	}
}

func GreaterOrEqual(t TestingT, e1, e2 any, msgAndArgs ...any) {
	t.Helper()
	if !testutil.Ordered(t, e1, e2, func(c int) bool { return c >= 0 }, "greater than or equal to", msgAndArgs) {
		t.FailNow()
	}
}

func LessOrEqual(t TestingT, e1, e2 any, msgAndArgs ...any) {
	t.Helper()
	if !testutil.Ordered(t, e1, e2, func(c int) bool { return c <= 0 }, "less than or equal to", msgAndArgs) {
		t.FailNow()
	}
}

func Greater(t TestingT, e1, e2 any, msgAndArgs ...any) {
	t.Helper()
	if !testutil.Ordered(t, e1, e2, func(c int) bool { return c > 0 }, "greater than", msgAndArgs) {
		t.FailNow()
	}
}

func Less(t TestingT, e1, e2 any, msgAndArgs ...any) {
	t.Helper()
	if !testutil.Ordered(t, e1, e2, func(c int) bool { return c < 0 }, "less than", msgAndArgs) {
		t.FailNow()
	}
}
