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

	"github.com/ethereum/go-ethereum/p2p/discover"
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
	churnInterval := flag.Duration("churn-interval", 0, "if > 0, run the churn workload: kill -churn-frac of the active nodes every interval during the search phase, exercising failure-driven blacklist/eviction (#71)")
	churnFrac := flag.Float64("churn-frac", 0.1, "fraction of the active population churned each round (only used when -churn-interval > 0)")
	churnMode := flag.String("churn-mode", "steadystate", "churn model when -churn-interval > 0: 'steadystate' (each action is 50/50 leave/join, keeping population ~constant) or 'killonly' (kill -churn-frac each round; population decays to zero)")
	vanillaFrac := flag.Float64("vanilla-frac", 0, "if > 0, run the mixed-binary interop workload: this fraction of nodes run stock upstream geth v1.17.3 discv5 as routing substrate (real separate stack), the rest run TopDisc; measures whether TopDisc discovery interoperates with real upstream geth. TopDisc penetration = 1 - vanilla-frac")
	adLifetime := flag.Duration("ad-lifetime", 0, "topic ad lifetime (0 = discv5 default of 15m); also drives RegAttemptTimeout = 1.5x this")
	allRegister := flag.Bool("all-register", false, "single shared topic where every node both registers and searches it (uniform membership, no Zipf); routes through the multi-topic engine with 1 topic")
	snapshotDirFlag := flag.String("snapshot-dir", "", "if set, write periodic per-registrant find-count snapshots + registrant manifest (id+logdist) here for offline spatial analysis")
	searchBucketSize := flag.Int("search-bucket-size", 0, "topic search bucket size per distance bucket (0 = default 8); raises the 18*size per-search registrar ceiling")
	regBucketSize := flag.Int("reg-bucket-size", 0, "registration bucket size (active registrars per distance bucket; 0 = protocol default)")
	nodesPerSourceBucket := flag.Int("nodes-per-source-bucket", 0, "max nodes accepted per source per bucket in search+registration tables (0 = default 1)")
	regAttemptTimeout := flag.Duration("reg-attempt-timeout", 0, "max time a registrant waits on one registrar before giving up (0 = default 1.5x ad-lifetime)")
	overheadOutFlag := flag.String("overhead-out", "", "if set, write per-node sent/received packet+byte counts to this JSON file")
	reachOutFlag := flag.String("reach-out", "", "if set, write per-searcher queried-registrar sets + every registrar's topic-table contents here (bottleneck analysis)")
	removeOnExpiryFlag := flag.Bool("remove-on-expiry", false, "on ad expiry, remove the registration instead of renewing (rotation experiment)")
	commonTopicFlag := flag.Bool("common-topic", false, "with -topics N>1: every node registers+searches universal topic 0 plus one Zipf-drawn topic from 1..N-1")
	flag.Parse()
	commonTopicMode = *commonTopicFlag
	nodeRemoveOnExpiry = *removeOnExpiryFlag
	reachOut = *reachOutFlag
	if reachOut != "" {
		discover.EnableReach()
	}
	if *overheadOutFlag != "" {
		discover.EnableTQRcv()
	}
	snapshotDir = *snapshotDirFlag
	nodeSearchBucketSize = *searchBucketSize
	nodeRegBucketSize = *regBucketSize
	nodeRegAttemptTimeout = *regAttemptTimeout
	nodeNodesPerSourceBucket = *nodesPerSourceBucket

	// Absolute watchdog: guarantee the process exits even if the workload or
	// teardown wedges. The discv5 search-shutdown path can deadlock when a
	// heavily- or fully-churned network leaves searcher goroutines stuck, and
	// the post-teardown watchdog below only arms after the workload returns —
	// so it cannot help if the workload itself hangs. This one is armed up
	// front. It must clear every healthy-run delay before search even starts —
	// the per-node spawn and register staggers (spawnDelay×N, registerStagger×N
	// are minutes at 10k), plus bootstrap-wait, register-wait and the full
	// search-timeout — then an 8-minute grace for teardown.
	n := time.Duration(*nodes)
	hardCap := n*(*spawnDelay) + *bootstrapWait + n*(*registerStagger) + *registerWait + *searchTimeout + 20*time.Minute
	go func() {
		time.Sleep(hardCap)
		fmt.Printf("absolute watchdog (%s) expired; force-exiting\n", hardCap)
		os.Exit(0)
	}()

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

	// Mixed-binary interop workload: a fraction of nodes run the real stock
	// upstream geth v1.17.3 discv5 stack as substrate; the rest run TopDisc.
	// This path has its own spawn/teardown (two stacks) and bypasses the normal
	// single-stack path below.
	if *vanillaFrac > 0 {
		monitorStop := make(chan struct{})
		monitorDone := make(chan struct{})
		go monitorBuffers(sim, monitorStop, monitorDone)
		pacing := searchPacing{Stagger: *searchStagger, MaxPause: *searchPauseMax, TargetCount: *searchTargetCount, Checkpoint: *checkpointInterval}
		runVanillaInterop(sim, settings, *nodes, *vanillaFrac, *numTopics, *zipfS, *seed,
			*bootstrapWait, *registerWait, *searchTimeout, *regProbePeriod, *registerStagger, *refreshInterval,
			*maxBootnodes, *spawnDelay, *metricsOut, pacing)
		close(monitorStop)
		<-monitorDone
		fmt.Println("teardown complete")
		return
	}

	all := spawnNodes(sim, settings, *nodes, legacySet, *maxBootnodes, *spawnDelay, *refreshInterval, *adLifetime)
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
	case *churnInterval > 0:
		runChurnWorkload(sim, settings, *maxBootnodes, *refreshInterval, all, *numTopics, *zipfS, *seed, *registerWait, *searchTimeout, *regProbePeriod, *registerStagger,
			churnParams{Interval: *churnInterval, Frac: *churnFrac, SteadyState: *churnMode == "steadystate"}, *metricsOut, pacing)
	case *allRegister:
		nt := *numTopics
		if nt < 1 {
			nt = 1
		}
		runMultiTopicWorkload(all, nt, *zipfS, *seed, *registerWait, *searchTimeout, *regProbePeriod, *registerStagger, *metricsOut, pacing)
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
	if *overheadOutFlag != "" {
		tqByIdx := make(map[int]int64, len(all))
		idByIdx := make(map[int]string, len(all))
		for _, nr := range all {
			tqByIdx[nr.idx] = discover.TopicQueryRcvCount(nr.ln.ID())
			idByIdx[nr.idx] = nr.ln.ID().String()
		}
		dumpOverhead(*overheadOutFlag, tqByIdx, idByIdx)
		fmt.Printf("overhead written to: %s\n", *overheadOutFlag)
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
