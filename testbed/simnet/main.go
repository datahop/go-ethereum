// Command simnet-testbed runs an in-process discv5 / DISC-NG testbed using
// github.com/marcopolo/simnet for simulated UDP transport.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/marcopolo/simnet"
)

func main() {
	nodes := flag.Int("nodes", 5, "number of discv5 nodes to spawn")
	latencyMs := flag.Int("latency", 30, "static per-pair latency in milliseconds")
	bandwidthMibps := flag.Int("bandwidth-mibps", 100, "per-direction bandwidth (Mibps)")
	bootstrapWait := flag.Duration("bootstrap-wait", 3*time.Second, "wait after spawning before starting workload")
	registerWait := flag.Duration("register-wait", 5*time.Second, "wait after starting registrations before starting searches")
	searchTimeout := flag.Duration("search-timeout", 30*time.Second, "max time per search before giving up")
	registerFrac := flag.Float64("register-frac", 0.5, "fraction of nodes that register the test topic; rest search for it (single-topic mode only)")
	numTopics := flag.Int("topics", 1, "number of distinct topics; if > 1 each node draws one via Zipf and both registers and searches it")
	zipfS := flag.Float64("zipf-s", 1.07, "Zipf skew parameter for topic assignment when -topics > 1")
	seed := flag.Int64("seed", 0, "RNG seed for Zipf draws (0 = use current time)")
	legacyFrac := flag.Float64("legacy-frac", 0.0, "fraction of nodes that are 'legacy' (no DISC-NG ENR flag); enables incremental-deployment validation workload — see issue #6")
	regProbePeriod := flag.Duration("reg-probe-period", 500*time.Millisecond, "polling period for the registration probe; smaller = finer-grained timing, more CPU")
	metricsOut := flag.String("metrics-out", "", "if set, write workload metrics to this JSON file")
	flag.Parse()

	fmt.Printf("simnet-testbed: spawning %d nodes (latency=%dms, bw=%dMibps)\n",
		*nodes, *latencyMs, *bandwidthMibps)

	sim := &simnet.Simnet{
		LatencyFunc: simnet.StaticLatency(time.Duration(*latencyMs) * time.Millisecond),
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
	settings := simnet.NodeBiDiLinkSettings{
		Downlink: simnet.LinkSettings{BitsPerSecond: *bandwidthMibps * simnet.Mibps},
		Uplink:   simnet.LinkSettings{BitsPerSecond: *bandwidthMibps * simnet.Mibps},
	}

	legacySet := pickLegacySet(*nodes, *legacyFrac, *seed)
	all := spawnNodes(sim, settings, *nodes, legacySet)

	sim.Start()
	defer sim.Close()
	defer func() {
		for _, n := range all {
			n.disc.Close()
		}
	}()

	fmt.Printf("simnet up; bootstrap-wait=%s\n", *bootstrapWait)
	time.Sleep(*bootstrapWait)

	switch {
	case *legacyFrac > 0:
		runDiscNGValidationWorkload(all, *registerWait, *searchTimeout, *metricsOut)
	case *numTopics <= 1:
		runSingleTopicWorkload(all, *registerWait, *searchTimeout, *registerFrac, *metricsOut)
	default:
		runMultiTopicWorkload(all, *numTopics, *zipfS, *seed, *registerWait, *searchTimeout, *regProbePeriod, *metricsOut)
	}
	fmt.Println("teardown complete")
}
