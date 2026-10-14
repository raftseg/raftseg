// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

// Package testutil implements the subset of github.com/stretchr/testify's
// assertions that the raft tests use, with the same semantics, using only
// the standard library. Packages assert (report and continue) and require
// (report and stop the test) wrap it.
package testutil

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"
)

// T is the part of testing.TB the assertions need.
type T interface {
	Helper()
	Errorf(format string, args ...any)
	FailNow()
}

// Message formats testify-style msgAndArgs.
func Message(msgAndArgs []any) string {
	switch len(msgAndArgs) {
	case 0:
		return ""
	case 1:
		if s, ok := msgAndArgs[0].(string); ok {
			return s
		}
		return fmt.Sprintf("%+v", msgAndArgs[0])
	default:
		if s, ok := msgAndArgs[0].(string); ok {
			return fmt.Sprintf(s, msgAndArgs[1:]...)
		}
		return fmt.Sprint(msgAndArgs...)
	}
}

// Fail reports a failure with an optional user message.
func Fail(t T, failure string, msgAndArgs []any) {
	t.Helper()
	msg := Message(msgAndArgs)
	if msg != "" {
		t.Errorf("\n\tError:    %s\n\tMessages: %s", indent(failure), msg)
		return
	}
	t.Errorf("\n\tError:    %s", indent(failure))
}

func indent(s string) string { return strings.ReplaceAll(s, "\n", "\n\t          ") }

// ObjectsAreEqual is testify's equality: reflect.DeepEqual, except that two
// []byte are compared by content (nil and empty differ).
func ObjectsAreEqual(expected, actual any) bool {
	if expected == nil || actual == nil {
		return expected == actual
	}
	exp, ok := expected.([]byte)
	if !ok {
		return reflect.DeepEqual(expected, actual)
	}
	act, ok := actual.([]byte)
	if !ok {
		return false
	}
	if exp == nil || act == nil {
		return exp == nil && act == nil
	}
	return bytes.Equal(exp, act)
}

// IsEmpty is testify's emptiness: nil, a zero value, an empty
// slice/map/channel, or a pointer to an empty value.
func IsEmpty(object any) bool {
	if object == nil {
		return true
	}
	v := reflect.ValueOf(object)
	switch v.Kind() {
	case reflect.Chan, reflect.Map, reflect.Slice:
		return v.Len() == 0
	case reflect.Pointer:
		if v.IsNil() {
			return true
		}
		return IsEmpty(v.Elem().Interface())
	default:
		return reflect.DeepEqual(object, reflect.Zero(v.Type()).Interface())
	}
}

// IsNil reports whether object is nil or a nil value of a nillable kind.
func IsNil(object any) bool {
	if object == nil {
		return true
	}
	v := reflect.ValueOf(object)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
		return v.IsNil()
	}
	return false
}

// IsZero reports whether i is nil or the zero value of its type.
func IsZero(i any) bool {
	return i == nil || reflect.DeepEqual(i, reflect.Zero(reflect.TypeOf(i)).Interface())
}

// Len returns the length of object, or false if len does not apply.
func Len(object any) (n int, ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return reflect.ValueOf(object).Len(), true
}

// Compare orders two values of the same type: integers, floats (NaN is not
// ordered), strings, []byte and time.Time. It returns -1, 0 or 1, or false
// if they cannot be compared. It is stricter than testify, which also orders
// values of different types that share a kind (int and a named int type).
func Compare(a, b any) (int, bool) {
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	if !va.IsValid() || !vb.IsValid() || va.Type() != vb.Type() {
		return 0, false
	}
	switch x := a.(type) {
	case time.Time:
		return x.Compare(b.(time.Time)), true
	case []byte:
		return bytes.Compare(x, b.([]byte)), true
	}
	sign := func(less, greater bool) int {
		switch {
		case less:
			return -1
		case greater:
			return 1
		}
		return 0
	}
	switch va.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return sign(va.Int() < vb.Int(), va.Int() > vb.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return sign(va.Uint() < vb.Uint(), va.Uint() > vb.Uint()), true
	case reflect.Float32, reflect.Float64:
		if math.IsNaN(va.Float()) || math.IsNaN(vb.Float()) {
			return 0, false
		}
		return sign(va.Float() < vb.Float(), va.Float() > vb.Float()), true
	case reflect.String:
		return sign(va.String() < vb.String(), va.String() > vb.String()), true
	}
	return 0, false
}

// Checks shared by assert and require. Each returns whether it passed.

func isFunc(v any) bool { return v != nil && reflect.TypeOf(v).Kind() == reflect.Func }

func Equal(t T, expected, actual any, msgAndArgs []any) bool {
	t.Helper()
	if isFunc(expected) || isFunc(actual) {
		Fail(t, "invalid operation: functions cannot be compared", msgAndArgs)
		return false
	}
	if !ObjectsAreEqual(expected, actual) {
		Fail(t, fmt.Sprintf("Not equal:\nexpected: %#v\nactual  : %#v", expected, actual), msgAndArgs)
		return false
	}
	return true
}

func NotEqual(t T, expected, actual any, msgAndArgs []any) bool {
	t.Helper()
	if isFunc(expected) || isFunc(actual) {
		Fail(t, "invalid operation: functions cannot be compared", msgAndArgs)
		return false
	}
	if ObjectsAreEqual(expected, actual) {
		Fail(t, fmt.Sprintf("Should not be: %#v", actual), msgAndArgs)
		return false
	}
	return true
}

func NoError(t T, err error, msgAndArgs []any) bool {
	t.Helper()
	if err != nil {
		Fail(t, fmt.Sprintf("Received unexpected error:\n%+v", err), msgAndArgs)
		return false
	}
	return true
}

func Error(t T, err error, msgAndArgs []any) bool {
	t.Helper()
	if err == nil {
		Fail(t, "An error is expected but got nil.", msgAndArgs)
		return false
	}
	return true
}

func ErrorIs(t T, err, target error, msgAndArgs []any) bool {
	t.Helper()
	if !errors.Is(err, target) {
		Fail(t, fmt.Sprintf("Target error should be in err chain:\nexpected: %v\nin chain: %v", target, err), msgAndArgs)
		return false
	}
	return true
}

func True(t T, value bool, msgAndArgs []any) bool {
	t.Helper()
	if !value {
		Fail(t, "Should be true", msgAndArgs)
		return false
	}
	return true
}

func False(t T, value bool, msgAndArgs []any) bool {
	t.Helper()
	if value {
		Fail(t, "Should be false", msgAndArgs)
		return false
	}
	return true
}

func LenIs(t T, object any, length int, msgAndArgs []any) bool {
	t.Helper()
	n, ok := Len(object)
	if !ok {
		Fail(t, fmt.Sprintf("\"%v\" could not be applied builtin len()", object), msgAndArgs)
		return false
	}
	if n != length {
		Fail(t, fmt.Sprintf("\"%v\" should have %d item(s), but has %d", object, length, n), msgAndArgs)
		return false
	}
	return true
}

func Empty(t T, object any, msgAndArgs []any) bool {
	t.Helper()
	if !IsEmpty(object) {
		Fail(t, fmt.Sprintf("Should be empty, but was %v", object), msgAndArgs)
		return false
	}
	return true
}

func NotEmpty(t T, object any, msgAndArgs []any) bool {
	t.Helper()
	if IsEmpty(object) {
		Fail(t, fmt.Sprintf("Should NOT be empty, but was %v", object), msgAndArgs)
		return false
	}
	return true
}

func Zero(t T, i any, msgAndArgs []any) bool {
	t.Helper()
	if !IsZero(i) {
		Fail(t, fmt.Sprintf("Should be zero, but was %v", i), msgAndArgs)
		return false
	}
	return true
}

func NotZero(t T, i any, msgAndArgs []any) bool {
	t.Helper()
	if IsZero(i) {
		Fail(t, fmt.Sprintf("Should not be zero, but was %v", i), msgAndArgs)
		return false
	}
	return true
}

func Nil(t T, object any, msgAndArgs []any) bool {
	t.Helper()
	if !IsNil(object) {
		Fail(t, fmt.Sprintf("Expected nil, but got: %#v", object), msgAndArgs)
		return false
	}
	return true
}

func NotNil(t T, object any, msgAndArgs []any) bool {
	t.Helper()
	if IsNil(object) {
		Fail(t, "Expected value not to be nil.", msgAndArgs)
		return false
	}
	return true
}

func Panics(t T, f func(), msgAndArgs []any) bool {
	t.Helper()
	panicked := func() (p bool) {
		defer func() {
			if recover() != nil {
				p = true
			}
		}()
		f()
		return false
	}()
	if !panicked {
		Fail(t, "func should panic", msgAndArgs)
		return false
	}
	return true
}

func Ordered(t T, e1, e2 any, ok func(int) bool, rel string, msgAndArgs []any) bool {
	t.Helper()
	c, comparable := Compare(e1, e2)
	if !comparable {
		Fail(t, fmt.Sprintf("Cannot compare %#v and %#v", e1, e2), msgAndArgs)
		return false
	}
	if !ok(c) {
		Fail(t, fmt.Sprintf("\"%v\" is not %s \"%v\"", e1, rel, e2), msgAndArgs)
		return false
	}
	return true
}
