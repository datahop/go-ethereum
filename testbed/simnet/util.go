package main

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/marcopolo/simnet"
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

// monitorBuffers polls sim.Stats() periodically and reports router/link
// buffer occupancy. Stops when stop is closed and signals via done.
//
// Output is interleaved with the rest of the workload's stdout; lines are
// prefixed with "[buf]". A final summary with peak occupancies prints when
// the monitor exits.
//
// The motivation: the buffer/shard fixes only matter if the buffers stay
// well under capacity in practice. If max occupancy approaches the cap,
// senders are being backpressured and we'd see the same symptoms as the
// pre-fix simnet — just delayed by the buffer's worth of packets.
func monitorBuffers(sim *simnet.Simnet, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	const sampleEvery = 1 * time.Second
	const reportEvery = 10 * time.Second

	tickSample := time.NewTicker(sampleEvery)
	defer tickSample.Stop()
	tickReport := time.NewTicker(reportEvery)
	defer tickReport.Stop()

	var (
		peakRouterMax, peakRouterSum int
		peakLinkMax, peakLinkSum     int
		routerCap, linkCap           int
		routerShards, linkCount      int
	)

	for {
		select {
		case <-stop:
			fmt.Printf("[buf] final peak — router: max %d / cap %d (sum peak %d across %d shards); links: max %d / cap %d (sum peak %d across %d drivers)\n",
				peakRouterMax, routerCap, peakRouterSum, routerShards,
				peakLinkMax, linkCap, peakLinkSum, linkCount)
			return

		case <-tickSample.C:
			s := sim.Stats()
			routerCap = s.RouterShardCap
			linkCap = s.LinkCap
			routerShards = s.RouterShardCount
			linkCount = s.LinkCount
			if s.RouterMax > peakRouterMax {
				peakRouterMax = s.RouterMax
			}
			if s.RouterSum > peakRouterSum {
				peakRouterSum = s.RouterSum
			}
			if s.LinkMax > peakLinkMax {
				peakLinkMax = s.LinkMax
			}
			if s.LinkSum > peakLinkSum {
				peakLinkSum = s.LinkSum
			}

		case <-tickReport.C:
			s := sim.Stats()
			rPct, lPct := 0, 0
			if routerCap > 0 {
				rPct = (peakRouterMax * 100) / routerCap
			}
			if linkCap > 0 {
				lPct = (peakLinkMax * 100) / linkCap
			}
			fmt.Printf("[buf] router peak %d/%d (%d%%, sum %d); links peak %d/%d (%d%%, sum %d); now router max=%d sum=%d, links max=%d sum=%d\n",
				peakRouterMax, routerCap, rPct, peakRouterSum,
				peakLinkMax, linkCap, lPct, peakLinkSum,
				s.RouterMax, s.RouterSum, s.LinkMax, s.LinkSum)
		}
	}
}
