// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

//go:build !unix

package seglog

import "time"

// cpuTime is not measured here: the benchmarks then report time only.
func cpuTime() time.Duration { return -1 }
