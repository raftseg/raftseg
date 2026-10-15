// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

//go:build unix

package seglog

import (
	"syscall"
	"time"
)

// cpuTime is the CPU time the process has used.
func cpuTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return -1
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
