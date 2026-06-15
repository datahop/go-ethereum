// Command simnet-testbed runs an in-process discv5 / DISC-NG testbed using
// github.com/marcopolo/simnet for simulated UDP transport.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync"
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
	registerStagger := flag.Duration("register-stagger", 0, "per-slot delay before each registrant calls RegisterTopic; spreads initial admission times so AdLifetime expiries don't synchronize and cause a renewal storm")
	metricsOut := flag.String("metrics-out", "", "if set, write workload metrics to this JSON file")
	routerShards := flag.Int("router-shards", 0, "VariableLatencyRouter shard count (0 = simnet default of 16)")
	routerBuf := flag.Int("router-buf", 0, "VariableLatencyRouter per-shard buffer (0 = simnet default of 8192)")
	linkBuf := flag.Int("link-buf", 0, "Simlink per-direction buffer size (0 = simnet default of 1024)")
	linkNoAQM := flag.Bool("link-no-aqm", false, "disable fq_codel + rate limiting on Simlink (~5-10x per-packet drain rate; useful at high node counts where AQM modeling is noise)")
	spawnDelay := flag.Duration("spawn-delay", 0, "delay between spawning each node; staggers when each node starts pinging bootnodes (e.g. 1ms × N nodes spreads bootstrap burst)")
	maxBootnodes := flag.Int("max-bootnodes", 20, "max bootnodes each newly-spawned node uses to discover the network; smaller = less startup traffic, slower routing-table convergence")
	searchStagger := flag.Duration("search-stagger", 0, "per-slot delay before each searcher starts its TopicSearch; spreads search activity across a window")
	searchPauseMax := flag.Duration("search-pause-max", 0, "upper bound for random sleep between iter.Next() calls per searcher; models paced consumption instead of full-speed polling")
	searchTargetCount := flag.Int("search-target-count", 0, "stop each searcher once it has seen this many distinct registrants (0 = no limit, run for full search-timeout)")
	checkpointInterval := flag.Duration("checkpoint-interval", 0, "if > 0, print per-topic coverage snapshot at this cadence during the search phase; useful for long continuous runs to see progress without waiting for the final report")
	refreshInterval := flag.Duration("refresh-interval", 0, "discv5 routing table refresh interval (0 = use discv5 default of 30 min). Lower values run more background random lookups; useful for long-running simnets where coverage plateaus if routing tables freeze")
	flag.Parse()

	fmt.Printf("simnet-testbed: spawning %d nodes (latency=%dms, bw=%dMibps)\n",
		*nodes, *latencyMs, *bandwidthMibps)

	sim := &simnet.Simnet{
		LatencyFunc:      simnet.StaticLatency(time.Duration(*latencyMs) * time.Millisecond),
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
		RouterShardCount: *routerShards,
		RouterBufferSize: *routerBuf,
	}
	settings := simnet.NodeBiDiLinkSettings{
		Downlink: simnet.LinkSettings{BitsPerSecond: *bandwidthMibps * simnet.Mibps, BufferSize: *linkBuf, NoAQM: *linkNoAQM},
		Uplink:   simnet.LinkSettings{BitsPerSecond: *bandwidthMibps * simnet.Mibps, BufferSize: *linkBuf, NoAQM: *linkNoAQM},
	}

	legacySet := pickLegacySet(*nodes, *legacyFrac, *seed)

	// Start the simnet BEFORE spawning nodes, so each node's discv5 stack
	// sends its bootstrap packets into an already-running network instead
	// of queueing them in pre-Start buffers and releasing all at once when
	// Start fires. Combined with -spawn-delay, this spreads the bootstrap
	// burst across the spawn window instead of concentrating it at t=0.
	sim.Start()
	defer sim.Close()

	all := spawnNodes(sim, settings, *nodes, legacySet, *maxBootnodes, *spawnDelay, *refreshInterval)
	defer func() {
		// Parallelize disc.Close across nodes. Sequential close of N
		// nodes takes O(N × per-node-shutdown) which becomes minutes at
		// 5k+ nodes — each Close waits for that node's dispatch
		// goroutine to drain. Fanning out lets them all shut down in
		// parallel, bounded by the Go scheduler.
		var wg sync.WaitGroup
		wg.Add(len(all))
		for _, n := range all {
			go func(n nodeRec) {
				defer wg.Done()
				n.disc.Close()
			}(n)
		}
		wg.Wait()
	}()

	// Periodic buffer-occupancy monitor. Flags when the router or any
	// link driver is approaching saturation, which indicates the shard /
	// buffer fix is being overwhelmed and senders are about to block.
	monitorStop := make(chan struct{})
	monitorDone := make(chan struct{})
	go monitorBuffers(sim, monitorStop, monitorDone)
	defer func() {
		close(monitorStop)
		<-monitorDone
	}()

	fmt.Printf("simnet up; bootstrap-wait=%s\n", *bootstrapWait)
	time.Sleep(*bootstrapWait)

	pacing := searchPacing{
		Stagger:     *searchStagger,
		MaxPause:    *searchPauseMax,
		TargetCount: *searchTargetCount,
		Checkpoint:  *checkpointInterval,
	}

	switch {
	case *legacyFrac > 0 && *numTopics <= 1:
		runDiscNGValidationWorkload(all, *registerWait, *searchTimeout, *metricsOut)
	case *numTopics <= 1:
		runSingleTopicWorkload(all, *registerWait, *searchTimeout, *registerFrac, *metricsOut)
	default:
		// runMultiTopicWorkload skips nodes with n.legacy=true (they
		// stay as passive Discv5 peers and only contribute to the
		// routing-table substrate).
		runMultiTopicWorkload(all, *numTopics, *zipfS, *seed, *registerWait, *searchTimeout, *regProbePeriod, *registerStagger, *metricsOut, pacing)
	}
	fmt.Println("teardown complete")

	// Watchdog: the deferred cleanup below (monitor stop, parallel
	// disc.Close, sim.Close) can hang at scale when one or more UDPv5
	// dispatchers get stuck. Metrics are already on disk by this
	// point, so any time spent here is pure overhead. Force-exit if
	// cleanup does not finish in 30s so the process does not sit in
	// futex_wait indefinitely.
	go func() {
		time.Sleep(30 * time.Second)
		fmt.Println("teardown grace expired (30s); force-exiting")
		os.Exit(0)
	}()
}
