package main

import (
	"fmt"
	"os"
	"sort"
	"time"
)

// percentile returns the requested percentile (0-100) of the input. Mutates
// (sorts) the slice as a side effect.
func percentile(d []time.Duration, p int) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	idx := (p * (len(d) - 1)) / 100
	return d[idx]
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "fatal: "+format+"\n", args...)
	os.Exit(1)
}

func minOrZero(xs []int) int {
	if len(xs) == 0 {
		return 0
	}
	return xs[0]
}

func medOrZero(xs []int) int {
	if len(xs) == 0 {
		return 0
	}
	return xs[len(xs)/2]
}

func maxOrZero(xs []int) int {
	if len(xs) == 0 {
		return 0
	}
	return xs[len(xs)-1]
}
