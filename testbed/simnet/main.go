package main

import (
	"crypto/ecdsa"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/marcopolo/simnet"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// maxBootnodes caps how many of the previously-spawned nodes a fresh node
// uses as its bootstrap set. Without a cap, node N tries to PING all N-1
// predecessors at startup, producing O(N^2) bootstrap traffic that stalls
// at scale (n=2000 stalls indefinitely). Mirrors the Python testbed's
// select_bootnodes helper which uses ~20.
const maxBootnodes = 20

// testTopic is the legacy single-topic ID used when -topics is unset or 1.
// Identical across runs so behaviour is reproducible.
var testTopic = topicindex.TopicID{0x55, 0x49, 0x43, 0x4e, 0x47, 0x54, 0x45, 0x53, 0x54}

// makeTopic returns a deterministic 32-byte topic ID for index i, used when
// -topics > 1.
func makeTopic(i int) topicindex.TopicID {
	var t topicindex.TopicID
	t[0] = 0x44 // 'D'
	t[1] = 0x4e // 'N'
	t[2] = 0x47 // 'G'
	t[3] = byte(i >> 24)
	t[4] = byte(i >> 16)
	t[5] = byte(i >> 8)
	t[6] = byte(i)
	return t
}

type nodeRec struct {
	idx    int
	key    *ecdsa.PrivateKey
	ln     *enode.LocalNode
	disc   *discover.UDPv5
	legacy bool // true if the DiscNG ENR flag was stripped — used by the validation workload
}

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

// pickLegacySet deterministically chooses which node indices will have the
// DISC-NG ENR flag stripped, simulating legacy Discv5 peers in a mixed
// population.
func pickLegacySet(total int, frac float64, seed int64) map[int]bool {
	count := int(float64(total) * frac)
	if count <= 0 {
		return nil
	}
	s := seed
	if s == 0 {
		s = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(s))
	set := make(map[int]bool, count)
	for _, i := range rng.Perm(total)[:count] {
		set[i] = true
	}
	return set
}

func runSingleTopicWorkload(all []nodeRec, registerWait, searchTimeout time.Duration, registerFrac float64, metricsOut string) {
	_ = registerFrac
	registrants := all
	searchers := all

	registrantIDs := make(map[enode.ID]struct{}, len(registrants))
	for _, n := range registrants {
		registrantIDs[n.ln.ID()] = struct{}{}
	}

	fmt.Printf("workload: %d nodes (each registers AND searches), single topic=%x\n",
		len(all), testTopic[:])

	for _, n := range registrants {
		n.disc.RegisterTopic(testTopic, uint64(n.idx))
	}
	fmt.Printf("registrations started; register-wait=%s\n", registerWait)
	time.Sleep(registerWait)

	regCoverage := snapshotRegistrationCoverage(all, registrantIDs)
	printRegistrationCoverage(regCoverage, len(registrants), len(all))

	results := runSearches(searchers, registrantIDs, len(registrants)-1, searchTimeout)
	report(results, len(registrants), metricsOut, regCoverage)
}

func runMultiTopicWorkload(all []nodeRec, numTopics int, zipfS float64, seed int64, registerWait, searchTimeout, regProbePeriod time.Duration, metricsOut string) {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))
	zipf := rand.NewZipf(rng, zipfS, 1.0, uint64(numTopics-1))

	// Pre-build topic IDs.
	topics := make([]topicindex.TopicID, numTopics)
	for i := range topics {
		topics[i] = makeTopic(i)
	}

	// Per-node topic assignment.
	nodeTopic := make([]int, len(all))
	registrantsByTopic := make(map[int]map[enode.ID]struct{}, numTopics)
	for i := range all {
		t := int(zipf.Uint64())
		nodeTopic[i] = t
		if registrantsByTopic[t] == nil {
			registrantsByTopic[t] = make(map[enode.ID]struct{})
		}
		registrantsByTopic[t][all[i].ln.ID()] = struct{}{}
	}

	// Print topic distribution.
	dist := make([]int, numTopics)
	for _, t := range nodeTopic {
		dist[t]++
	}
	fmt.Printf("workload: %d nodes across %d topics (Zipf s=%.2f, seed=%d), each registers AND searches its own topic\n",
		len(all), numTopics, zipfS, seed)
	for t, c := range dist {
		fmt.Printf("  topic %d: %d nodes\n", t, c)
	}

	// Phase 1: register.
	regStart := time.Now()
	for i, n := range all {
		n.disc.RegisterTopic(topics[nodeTopic[i]], uint64(n.idx))
	}
	fmt.Printf("registrations started; register-wait=%s probe-period=%s\n", registerWait, regProbePeriod)

	// Run the registration probe in parallel with register-wait so we can
	// timestamp the first time each registrant shows up in any registrar's
	// topic table.
	probeStop := make(chan struct{})
	probeDone := make(chan map[string]map[string]int64, 1)
	go func() {
		probeDone <- runRegistrationProbe(all, topics, nodeTopic, regStart, probeStop, regProbePeriod)
	}()
	time.Sleep(registerWait)
	close(probeStop)
	regTimingNs := <-probeDone

	// Per-topic coverage snapshot.
	allCov := snapshotMultiTopicCoverage(all, registrantsByTopic, topics)
	printMultiTopicCoverage(allCov, registrantsByTopic, len(all))

	// Phase 2: searches.
	results := runMultiTopicSearches(all, nodeTopic, topics, registrantsByTopic, searchTimeout)
	reportMultiTopic(results, registrantsByTopic, topics, regTimingNs, metricsOut, allCov)
}

// runRegistrationProbe polls each node's LocalTopicNodes(topic) every
// probePeriod and records, per registrant, the first wall-clock time
// (relative to regStart, in nanoseconds) that the registrant appears in any
// other host's topic table. Self-registrations are excluded (we want
// time-to-first-remote-admission, not local pre-population).
//
// Returned map: topicHex -> registrantIdHex -> ns since regStart.
func runRegistrationProbe(all []nodeRec, topics []topicindex.TopicID, nodeTopic []int, regStart time.Time, stop <-chan struct{}, period time.Duration) map[string]map[string]int64 {
	regTopic := make(map[enode.ID]topicindex.TopicID, len(all))
	for i, n := range all {
		regTopic[n.ln.ID()] = topics[nodeTopic[i]]
	}

	out := make(map[string]map[string]int64, len(topics))
	for _, t := range topics {
		out[t.String()] = make(map[string]int64)
	}

	probe := func() {
		nowNs := time.Since(regStart).Nanoseconds()
		for _, host := range all {
			hostID := host.ln.ID()
			for _, topic := range topics {
				visible := host.disc.LocalTopicNodes(topic)
				m := out[topic.String()]
				for _, n := range visible {
					id := n.ID()
					if id == hostID {
						continue
					}
					// Only count if this node is a registrant of this topic.
					if regTopic[id] != topic {
						continue
					}
					if _, already := m[id.String()]; !already {
						m[id.String()] = nowNs
					}
				}
			}
		}
	}

	tick := time.NewTicker(period)
	defer tick.Stop()

	probe()
	for {
		select {
		case <-stop:
			probe() // one final sweep
			return out
		case <-tick.C:
			probe()
		}
	}
}

// registrationCoverage holds the post-register-wait snapshot:
//
//	byRegistrant[reg_id]   = how many distinct nodes have reg_id in their topic table
//	byHost[host_id]        = how many distinct registrants this host knows
type registrationCoverage struct {
	ByRegistrant map[string]int `json:"byRegistrant"`
	ByHost       map[string]int `json:"byHost"`
}

func snapshotRegistrationCoverage(all []nodeRec, registrantIDs map[enode.ID]struct{}) registrationCoverage {
	cov := registrationCoverage{
		ByRegistrant: make(map[string]int),
		ByHost:       make(map[string]int),
	}
	for _, host := range all {
		visible := host.disc.LocalTopicNodes(testTopic)
		hostID := host.ln.ID()
		seen := make(map[enode.ID]struct{})
		for _, n := range visible {
			id := n.ID()
			if id == hostID {
				continue // ignore self
			}
			if _, ok := registrantIDs[id]; !ok {
				continue // non-registrant noise
			}
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			cov.ByRegistrant[id.String()]++
		}
		cov.ByHost[hostID.String()] = len(seen)
	}
	return cov
}

func printRegistrationCoverage(cov registrationCoverage, numRegistrants, numHosts int) {
	// Per-registrant: how many hosts know each registrant.
	regCounts := make([]int, 0, numRegistrants)
	for _, c := range cov.ByRegistrant {
		regCounts = append(regCounts, c)
	}
	sort.Ints(regCounts)

	// Per-host: how many registrants each host sees.
	hostCounts := make([]int, 0, numHosts)
	for _, c := range cov.ByHost {
		hostCounts = append(hostCounts, c)
	}
	sort.Ints(hostCounts)

	fmt.Println()
	fmt.Println("=== post-register-wait coverage ===")
	if len(regCounts) == 0 {
		fmt.Println("no registrants visible on any remote node")
	} else {
		fmt.Printf("per-registrant fan-out (hosts that know each registrant):\n")
		fmt.Printf("  registrants visible somewhere: %d / %d\n", len(regCounts), numRegistrants)
		fmt.Printf("  fan-out                        min=%d  med=%d  max=%d  (cap = %d hosts)\n",
			regCounts[0], regCounts[len(regCounts)/2], regCounts[len(regCounts)-1], numHosts)
	}
	if len(hostCounts) > 0 {
		fmt.Printf("per-host registrant view (registrants known by each host):\n")
		fmt.Printf("  view size                      min=%d  med=%d  max=%d  (cap = %d registrants)\n",
			hostCounts[0], hostCounts[len(hostCounts)/2], hostCounts[len(hostCounts)-1], numRegistrants)
	}
}

func spawnNodes(sim *simnet.Simnet, settings simnet.NodeBiDiLinkSettings, count int, legacySet map[int]bool) []nodeRec {
	all := make([]nodeRec, 0, count)
	for i := 0; i < count; i++ {
		key, err := crypto.GenerateKey()
		if err != nil {
			fatalf("generate key %d: %v", i, err)
		}

		// Use 4-byte IP form so endpoint addresses match what the discv5 ->
		// adapter -> WriteTo path produces; 16-byte forms surface as router
		// "unknown destination" drops.
		//
		// Use a non-LAN public-style range (33.x.x.x). RFC1918 ranges like
		// 10.0.0.0/8 are treated as LAN by go-ethereum's netutil.IsLAN,
		// which silently disables the IP-bucket cap (regBucketSubnet=24)
		// and the IP-similarity score in §6 eq. 1. Spreading nodes across
		// distinct /24s (byte 33.N1.N2.1 with N1*256+N2 = node index) makes
		// the cap actually fire, exercising the full DISC-NG IP-defence
		// pathway.
		addr := &net.UDPAddr{
			IP:   net.IP{33, byte(i / 256), byte(i % 256), 1},
			Port: 30303,
		}
		conn := &simUDPConn{SimConn: sim.NewEndpoint(addr, settings)}

		db, err := enode.OpenDB("")
		if err != nil {
			fatalf("open enode db %d: %v", i, err)
		}
		ln := enode.NewLocalNode(db, key)
		ln.SetStaticIP(addr.IP)
		ln.SetFallbackUDP(addr.Port)

		cfg := discover.Config{PrivateKey: key}
		if len(all) > 0 {
			cfg.Bootnodes = append(cfg.Bootnodes, all[0].ln.Node())
			pool := all[1:]
			n := maxBootnodes - 1
			if n > len(pool) {
				n = len(pool)
			}
			for _, idx := range rand.Perm(len(pool))[:n] {
				cfg.Bootnodes = append(cfg.Bootnodes, pool[idx].ln.Node())
			}
		}

		disc, err := discover.ListenV5(conn, ln, cfg)
		if err != nil {
			fatalf("listen v5 on node %d: %v", i, err)
		}

		// Strip the DISC-NG capability flag from the LocalNode's record
		// before sim.Start() so this node looks like a stock-Discv5 peer
		// on the wire. ListenV5 sets the flag inside newUDPv5; we delete
		// here, before any packet is sent, so peers only ever see the
		// legacy version of this node's ENR.
		legacy := legacySet[i]
		if legacy {
			ln.Delete(new(topicindex.DiscNG))
		}

		all = append(all, nodeRec{idx: i, key: key, ln: ln, disc: disc, legacy: legacy})
	}
	return all
}

// runDiscNGValidationWorkload exercises the incremental-deployment design
// from issue #6: in a mixed population, flagged nodes register and search a
// topic while legacy nodes (no DISC-NG ENR flag) stay passive. The expected
// outcome is that filterDiscNG keeps REGTOPIC/TOPICQUERY off the legacy
// peers — so legacy hosts must end up with zero entries in their topic
// tables for the test topic, and flagged hosts should see the full
// flagged-registrant set.
func runDiscNGValidationWorkload(all []nodeRec, registerWait, searchTimeout time.Duration, metricsOut string) {
	var flagged, legacy []nodeRec
	flaggedIDs := make(map[enode.ID]struct{})
	for _, n := range all {
		if n.legacy {
			legacy = append(legacy, n)
		} else {
			flagged = append(flagged, n)
			flaggedIDs[n.ln.ID()] = struct{}{}
		}
	}
	fmt.Printf("DISC-NG validation: %d flagged (register+search), %d legacy (passive), topic=%x\n",
		len(flagged), len(legacy), testTopic[:])

	for _, n := range flagged {
		n.disc.RegisterTopic(testTopic, uint64(n.idx))
	}
	fmt.Printf("registrations started; register-wait=%s\n", registerWait)
	time.Sleep(registerWait)

	cov := snapshotRegistrationCoverage(all, flaggedIDs)
	printRegistrationCoverage(cov, len(flagged), len(all))

	var flaggedSum, legacySum int
	for _, n := range flagged {
		flaggedSum += cov.ByHost[n.ln.ID().String()]
	}
	for _, n := range legacy {
		legacySum += cov.ByHost[n.ln.ID().String()]
	}
	fmt.Println()
	fmt.Println("=== DISC-NG filter validation (issue #6) ===")
	fmt.Printf("  flagged hosts (%d): %d flagged-registrant entries summed across hosts\n", len(flagged), flaggedSum)
	fmt.Printf("  legacy hosts  (%d): %d flagged-registrant entries summed across hosts (expected 0)\n", len(legacy), legacySum)
	if legacySum == 0 && flaggedSum > 0 {
		fmt.Println("  RESULT: PASS — no REGTOPIC leaked to legacy peers")
	} else {
		fmt.Println("  RESULT: FAIL — filterDiscNG did not isolate the populations")
	}

	results := runSearches(flagged, flaggedIDs, len(flagged)-1, searchTimeout)
	report(results, len(flagged)-1, metricsOut, cov)
}

// searchResult captures the metrics for one searcher's call to TopicSearch.
type searchResult struct {
	NodeIdx          int           `json:"nodeIdx"`
	NodeID           string        `json:"nodeId"`
	Topic            int           `json:"topic"` // 0 in single-topic mode
	Target           int           `json:"target"`
	Found            int           `json:"found"`
	FoundRegistrant  int           `json:"foundRegistrant"` // distinct found nodes that are real registrants
	FoundExtra       int           `json:"foundExtra"`      // returned but not in the registrant set
	TimeToFirst      time.Duration `json:"timeToFirstNs"`
	TimeToCompletion time.Duration `json:"timeToCompletionNs"`
	HitTimeoutBefore bool          `json:"hitTimeout"`
	FoundIDs         []string      `json:"foundIds"`
	// UniqueFoundAtMs is the wall-clock millisecond timestamp (relative to
	// search start) at which the i-th *distinct* registrant was first seen
	// by this searcher. Drives the "unique-found over time" figure.
	UniqueFoundAtMs []int64 `json:"uniqueFoundAtMs"`
}

func runSearches(searchers []nodeRec, registrants map[enode.ID]struct{}, target int, timeout time.Duration) []searchResult {
	results := make([]searchResult, len(searchers))
	var wg sync.WaitGroup
	wg.Add(len(searchers))

	for i, n := range searchers {
		go func(slot int, n nodeRec) {
			defer wg.Done()

			iter := n.disc.TopicSearch(testTopic, uint64(n.idx))
			defer iter.Close()

			// Cancel after timeout.
			done := make(chan struct{})
			closer := time.AfterFunc(timeout, func() {
				iter.Close()
				close(done)
			})
			defer closer.Stop()

			start := time.Now()
			var (
				found      []enode.ID
				timeFirst  time.Duration
				registered int
				extra      int
				seenReg    = make(map[enode.ID]struct{})
				uniqueAtMs []int64
			)
			selfID := n.ln.ID()
			for iter.Next() {
				if timeFirst == 0 {
					timeFirst = time.Since(start)
				}
				id := iter.Node().ID()
				if id == selfID {
					continue
				}
				found = append(found, id)
				if _, ok := registrants[id]; ok {
					if _, dup := seenReg[id]; !dup {
						seenReg[id] = struct{}{}
						uniqueAtMs = append(uniqueAtMs, time.Since(start).Milliseconds())
					}
					registered++
				} else {
					extra++
				}
				// Early-break removed on purpose: search ends only via the
				// timeout firing (external Close) or iter.Next() returning
				// false (natural IsDone) — letting us observe whether the
				// IsDone fix (#27) and search-tracking fix (#28) trigger
				// natural termination in real workloads.
			}
			elapsed := time.Since(start)

			ids := make([]string, 0, len(found))
			for _, id := range found {
				ids = append(ids, id.TerminalString())
			}
			hitTimeout := false
			select {
			case <-done:
				hitTimeout = true
			default:
			}
			results[slot] = searchResult{
				NodeIdx:          n.idx,
				NodeID:           n.ln.ID().TerminalString(),
				Found:            len(found),
				FoundRegistrant:  registered,
				FoundExtra:       extra,
				TimeToFirst:      timeFirst,
				TimeToCompletion: elapsed,
				HitTimeoutBefore: hitTimeout,
				FoundIDs:         ids,
				UniqueFoundAtMs:  uniqueAtMs,
			}
		}(i, n)
	}

	wg.Wait()
	return results
}

func report(results []searchResult, target int, metricsOut string, regCoverage registrationCoverage) {
	if len(results) == 0 {
		fmt.Println("no searchers")
		return
	}

	// Aggregate.
	var (
		latenciesFirst     []time.Duration
		latenciesComplete  []time.Duration
		recallSum          float64
		hitTimeout         int
		fullRecall         int
	)
	for _, r := range results {
		if r.TimeToFirst > 0 {
			latenciesFirst = append(latenciesFirst, r.TimeToFirst)
		}
		latenciesComplete = append(latenciesComplete, r.TimeToCompletion)
		recallSum += float64(r.FoundRegistrant) / float64(target)
		if r.HitTimeoutBefore {
			hitTimeout++
		}
		if r.FoundRegistrant >= target {
			fullRecall++
		}
	}

	fmt.Println()
	fmt.Println("=== workload summary ===")
	fmt.Printf("searchers:                 %d\n", len(results))
	fmt.Printf("registrants (target):      %d\n", target)
	fmt.Printf("full recall (found all):   %d / %d\n", fullRecall, len(results))
	fmt.Printf("mean recall:               %.2f\n", recallSum/float64(len(results)))
	fmt.Printf("hit timeout:               %d / %d\n", hitTimeout, len(results))
	if len(latenciesFirst) > 0 {
		fmt.Printf("time to first result:      median=%s p95=%s\n",
			percentile(latenciesFirst, 50), percentile(latenciesFirst, 95))
	}
	fmt.Printf("time to completion:        median=%s p95=%s\n",
		percentile(latenciesComplete, 50), percentile(latenciesComplete, 95))

	if metricsOut != "" {
		writeMetrics(metricsOut, results, target, regCoverage)
		fmt.Printf("metrics written to: %s\n", metricsOut)
	}
}

func writeMetrics(path string, results []searchResult, target int, regCoverage registrationCoverage) {
	out := map[string]any{
		"target":               target,
		"numSearchers":         len(results),
		"results":              results,
		"registrationCoverage": regCoverage,
	}
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "metrics: open %s: %v\n", path, err)
		return
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintf(os.Stderr, "metrics: encode: %v\n", err)
	}
}

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

// multiTopicCoverage holds per-topic registration coverage post-register-wait.
type multiTopicCoverage struct {
	ByTopic map[int]registrationCoverage `json:"byTopic"`
}

func snapshotMultiTopicCoverage(all []nodeRec, registrantsByTopic map[int]map[enode.ID]struct{}, topics []topicindex.TopicID) multiTopicCoverage {
	out := multiTopicCoverage{ByTopic: make(map[int]registrationCoverage)}
	for t, regSet := range registrantsByTopic {
		cov := registrationCoverage{
			ByRegistrant: make(map[string]int),
			ByHost:       make(map[string]int),
		}
		for _, host := range all {
			visible := host.disc.LocalTopicNodes(topics[t])
			hostID := host.ln.ID()
			seen := make(map[enode.ID]struct{})
			for _, n := range visible {
				id := n.ID()
				if id == hostID {
					continue
				}
				if _, ok := regSet[id]; !ok {
					continue
				}
				if _, dup := seen[id]; dup {
					continue
				}
				seen[id] = struct{}{}
				cov.ByRegistrant[id.String()]++
			}
			cov.ByHost[hostID.String()] = len(seen)
		}
		out.ByTopic[t] = cov
	}
	return out
}

func printMultiTopicCoverage(all multiTopicCoverage, registrantsByTopic map[int]map[enode.ID]struct{}, numHosts int) {
	fmt.Println()
	fmt.Println("=== post-register-wait coverage (per topic) ===")
	topics := make([]int, 0, len(all.ByTopic))
	for t := range all.ByTopic {
		topics = append(topics, t)
	}
	sort.Ints(topics)
	for _, t := range topics {
		cov := all.ByTopic[t]
		regSet := registrantsByTopic[t]
		fan := make([]int, 0, len(cov.ByRegistrant))
		for _, c := range cov.ByRegistrant {
			fan = append(fan, c)
		}
		sort.Ints(fan)
		fmt.Printf("  topic %d (%d registrants): visible=%d  fan-out min=%d med=%d max=%d (cap=%d)\n",
			t, len(regSet), len(fan),
			minOrZero(fan), medOrZero(fan), maxOrZero(fan), numHosts-1)
	}
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

func runMultiTopicSearches(all []nodeRec, nodeTopic []int, topics []topicindex.TopicID, registrantsByTopic map[int]map[enode.ID]struct{}, timeout time.Duration) []searchResult {
	results := make([]searchResult, len(all))
	var wg sync.WaitGroup
	wg.Add(len(all))

	for i, n := range all {
		go func(slot int, n nodeRec, topicIdx int) {
			defer wg.Done()
			topic := topics[topicIdx]
			regSet := registrantsByTopic[topicIdx]
			target := len(regSet) - 1 // exclude self
			if target < 0 {
				target = 0
			}

			iter := n.disc.TopicSearch(topic, uint64(n.idx))
			done := make(chan struct{})
			closer := time.AfterFunc(timeout, func() {
				iter.Close()
				close(done)
			})
			defer closer.Stop()
			defer iter.Close()

			start := time.Now()
			var (
				found      []enode.ID
				timeFirst  time.Duration
				registered int
				extra      int
				seenReg    = make(map[enode.ID]struct{})
				uniqueAtMs []int64
			)
			selfID := n.ln.ID()
			for iter.Next() {
				if timeFirst == 0 {
					timeFirst = time.Since(start)
				}
				id := iter.Node().ID()
				if id == selfID {
					continue
				}
				found = append(found, id)
				if _, ok := regSet[id]; ok {
					if _, dup := seenReg[id]; !dup {
						seenReg[id] = struct{}{}
						uniqueAtMs = append(uniqueAtMs, time.Since(start).Milliseconds())
					}
					registered++
				} else {
					extra++
				}
			}
			elapsed := time.Since(start)
			ids := make([]string, 0, len(found))
			for _, id := range found {
				ids = append(ids, id.TerminalString())
			}
			r := searchResult{
				NodeIdx:          n.idx,
				NodeID:           n.ln.ID().TerminalString(),
				Topic:            topicIdx,
				Target:           target,
				Found:            len(found),
				FoundRegistrant:  registered,
				FoundExtra:       extra,
				TimeToFirst:      timeFirst,
				TimeToCompletion: elapsed,
				FoundIDs:         ids,
				UniqueFoundAtMs:  uniqueAtMs,
			}
			select {
			case <-done:
				r.HitTimeoutBefore = true
			default:
			}
			results[slot] = r
		}(i, n, nodeTopic[i])
	}
	wg.Wait()
	return results
}

func reportMultiTopic(results []searchResult, registrantsByTopic map[int]map[enode.ID]struct{}, topics []topicindex.TopicID, regTimingNs map[string]map[string]int64, metricsOut string, cov multiTopicCoverage) {
	// Per-topic search aggregation.
	type topicReport struct {
		Topic         int     `json:"topic"`
		NumSearchers  int     `json:"numSearchers"`
		Target        int     `json:"target"`
		FullRecall    int     `json:"fullRecall"`
		MeanRecall    float64 `json:"meanRecall"`
		HitTimeout    int     `json:"hitTimeout"`
	}
	byTopic := make(map[int][]searchResult)
	for _, r := range results {
		byTopic[r.Topic] = append(byTopic[r.Topic], r)
	}
	topicIdxs := make([]int, 0, len(byTopic))
	for t := range byTopic {
		topicIdxs = append(topicIdxs, t)
	}
	sort.Ints(topicIdxs)

	reports := make([]topicReport, 0, len(topicIdxs))
	fmt.Println()
	fmt.Println("=== per-topic search summary ===")
	fmt.Printf("%-6s %12s %8s %12s %10s %10s\n", "topic", "searchers", "target", "fullRecall", "meanRec", "timeout")
	for _, t := range topicIdxs {
		rs := byTopic[t]
		var (
			full, timeout int
			recallSum     float64
			counted       int
		)
		for _, r := range rs {
			if r.Target > 0 {
				recallSum += float64(r.FoundRegistrant) / float64(r.Target)
				counted++
				if r.FoundRegistrant >= r.Target {
					full++
				}
			}
			if r.HitTimeoutBefore {
				timeout++
			}
		}
		mean := 0.0
		if counted > 0 {
			mean = recallSum / float64(counted)
		}
		target := 0
		if rs[0].Target > 0 {
			target = rs[0].Target
		}
		fmt.Printf("%-6d %12d %8d %12s %10.4f %10d\n", t, len(rs), target,
			fmt.Sprintf("%d/%d", full, len(rs)), mean, timeout)
		reports = append(reports, topicReport{
			Topic: t, NumSearchers: len(rs), Target: target,
			FullRecall: full, MeanRecall: mean, HitTimeout: timeout,
		})
	}

	// Brief stdout summary of registration timing (per topic mean/std in ms).
	if len(regTimingNs) > 0 {
		fmt.Println()
		fmt.Println("=== registration timing (time to first remote admission, ms) ===")
		fmt.Printf("%-6s %12s %12s %12s\n", "topic", "n", "mean", "std")
		for _, t := range topicIdxs {
			th := topics[t].String()
			vals := regTimingNs[th]
			if len(vals) == 0 {
				continue
			}
			var sum, sumSq float64
			for _, ns := range vals {
				ms := float64(ns) / 1e6
				sum += ms
				sumSq += ms * ms
			}
			n := float64(len(vals))
			mean := sum / n
			variance := sumSq/n - mean*mean
			if variance < 0 {
				variance = 0
			}
			fmt.Printf("%-6d %12d %12.1f %12.1f\n", t, len(vals), mean, variance)
		}
	}

	if metricsOut != "" {
		out := map[string]any{
			"perTopic":             reports,
			"results":              results,
			"registrationCoverage": cov,
			"registrationTimingNs": regTimingNs,
		}
		f, err := os.Create(metricsOut)
		if err != nil {
			fmt.Fprintf(os.Stderr, "metrics: open %s: %v\n", metricsOut, err)
			return
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			fmt.Fprintf(os.Stderr, "metrics: encode: %v\n", err)
		}
		fmt.Printf("metrics written to: %s\n", metricsOut)
	}
}
