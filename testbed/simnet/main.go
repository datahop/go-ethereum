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

type nodeRec struct {
	idx    int
	key    *ecdsa.PrivateKey
	ln     *enode.LocalNode
	disc   *discover.UDPv5
	discng bool
}

// runParams captures all command-line knobs so they can be embedded in the
// JSON metrics output and rendered in reports.
type runParams struct {
	Nodes            int           `json:"nodes"`
	Topics           int           `json:"topics"`
	ZipfS            float64       `json:"zipfS"`
	LatencyMs        int           `json:"latencyMs"`
	BandwidthMibps   int           `json:"bandwidthMibps"`
	BootstrapWaitNs  time.Duration `json:"bootstrapWaitNs"`
	RegisterWaitNs   time.Duration `json:"registerWaitNs"`
	SearchTimeoutNs  time.Duration `json:"searchTimeoutNs"`
	DiscNGFrac       float64       `json:"discngFrac"`
	RegProbePeriodNs time.Duration `json:"regProbePeriodNs"`
	Seed             int64         `json:"seed"`
	MaxBootnodes     int           `json:"maxBootnodes"`
	NumDiscNG        int           `json:"numDiscNG"`
}

// makeTopic returns a deterministic 32-byte topic ID for index i.
func makeTopic(i int) topicindex.TopicID {
	var t topicindex.TopicID
	t[0] = 0x44 // 'D' for DISC-NG
	t[1] = 0x4e
	t[2] = 0x47
	t[3] = byte(i >> 24)
	t[4] = byte(i >> 16)
	t[5] = byte(i >> 8)
	t[6] = byte(i)
	return t
}

func main() {
	nodes := flag.Int("nodes", 5, "number of discv5 nodes to spawn")
	latencyMs := flag.Int("latency", 30, "static per-pair latency in milliseconds")
	bandwidthMibps := flag.Int("bandwidth-mibps", 100, "per-direction bandwidth (Mibps)")
	bootstrapWait := flag.Duration("bootstrap-wait", 3*time.Second, "wait after spawning before starting workload")
	registerWait := flag.Duration("register-wait", 5*time.Second, "wait after starting registrations before starting searches")
	searchTimeout := flag.Duration("search-timeout", 30*time.Second, "max time per search before giving up")
	numTopics := flag.Int("topics", 1, "number of distinct topics; each node is assigned one via Zipf")
	zipfS := flag.Float64("zipf-s", 1.07, "Zipf skew parameter (s>1, larger = more concentration on top topics)")
	seed := flag.Int64("seed", 0, "RNG seed (0 = use current time)")
	discngFrac := flag.Float64("discng-frac", 1.0, "fraction of nodes that advertise DISC-NG support (0..1); the rest are vanilla discv5 routing peers that do not register or search")
	regProbePeriod := flag.Duration("reg-probe-period", 500*time.Millisecond, "registration probe period during register-wait (smaller = finer-grained timing, more CPU)")
	metricsOut := flag.String("metrics-out", "", "if set, write per-search metrics to this JSON file")
	flag.Parse()

	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(*seed))

	params := runParams{
		Nodes:            *nodes,
		Topics:           *numTopics,
		ZipfS:            *zipfS,
		LatencyMs:        *latencyMs,
		BandwidthMibps:   *bandwidthMibps,
		BootstrapWaitNs:  *bootstrapWait,
		RegisterWaitNs:   *registerWait,
		SearchTimeoutNs:  *searchTimeout,
		DiscNGFrac:       *discngFrac,
		RegProbePeriodNs: *regProbePeriod,
		Seed:             *seed,
		MaxBootnodes:     maxBootnodes,
	}

	fmt.Printf("simnet-testbed: %d nodes (discng-frac=%.2f), %d topics (zipf s=%.2f), latency=%dms bw=%dMibps, seed=%d\n",
		*nodes, *discngFrac, *numTopics, *zipfS, *latencyMs, *bandwidthMibps, *seed)

	sim := &simnet.Simnet{
		LatencyFunc: simnet.StaticLatency(time.Duration(*latencyMs) * time.Millisecond),
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
	settings := simnet.NodeBiDiLinkSettings{
		Downlink: simnet.LinkSettings{BitsPerSecond: *bandwidthMibps * simnet.Mibps},
		Uplink:   simnet.LinkSettings{BitsPerSecond: *bandwidthMibps * simnet.Mibps},
	}

	// Decide which node indices are discng-capable. We spread non-discng
	// nodes evenly through the spawn order (bootnode first stays discng) so
	// the network's bootstrap connectivity isn't biased.
	numDiscng := int(float64(*nodes) * *discngFrac)
	if numDiscng < 1 {
		numDiscng = 1 // keep at least one discng node so the experiment is meaningful
	}
	if numDiscng > *nodes {
		numDiscng = *nodes
	}
	discngFlags := make([]bool, *nodes)
	for i := 0; i < numDiscng; i++ {
		discngFlags[i] = true
	}
	rng.Shuffle(len(discngFlags), func(i, j int) {
		discngFlags[i], discngFlags[j] = discngFlags[j], discngFlags[i]
	})
	discngFlags[0] = true // first node = bootnode, force discng

	all := spawnNodes(sim, settings, *nodes, discngFlags)
	params.NumDiscNG = numDiscng

	sim.Start()
	defer sim.Close()
	defer func() {
		for _, n := range all {
			n.disc.Close()
		}
	}()

	fmt.Printf("simnet up; bootstrap-wait=%s\n", *bootstrapWait)
	time.Sleep(*bootstrapWait)

	// Every node is both a registrant and a searcher for the SAME topic,
	// drawn once per node from a Zipf distribution.
	topics := make([]topicindex.TopicID, *numTopics)
	for i := range topics {
		topics[i] = makeTopic(i)
	}
	zipf := rand.NewZipf(rng, *zipfS, 1.0, uint64(*numTopics-1))
	pickTopic := func() int { return int(zipf.Uint64()) }
	if *numTopics == 1 {
		pickTopic = func() int { return 0 }
	}

	// Only DISC-NG nodes participate in the topic workload; vanilla
	// discv5 nodes are routing-table peers only.
	discngNodes := make([]nodeRec, 0, len(all))
	for _, n := range all {
		if n.discng {
			discngNodes = append(discngNodes, n)
		}
	}

	nodeTopics := make([]int, len(discngNodes))
	registrantsByTopic := make(map[int]map[enode.ID]struct{}, *numTopics)
	for i := range discngNodes {
		t := pickTopic()
		nodeTopics[i] = t
		if registrantsByTopic[t] == nil {
			registrantsByTopic[t] = make(map[enode.ID]struct{})
		}
		registrantsByTopic[t][discngNodes[i].ln.ID()] = struct{}{}
	}

	// Print topic distribution.
	type topicStats struct{ topic, count int }
	stats := make([]topicStats, *numTopics)
	for i := range stats {
		stats[i].topic = i
	}
	for _, t := range nodeTopics {
		stats[t].count++
	}
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].count != stats[j].count {
			return stats[i].count > stats[j].count
		}
		return stats[i].topic < stats[j].topic
	})
	fmt.Printf("workload: %d total nodes (%d DISC-NG, %d vanilla); DISC-NG nodes register + search the same topic across %d topics\n",
		len(all), len(discngNodes), len(all)-len(discngNodes), *numTopics)
	if *numTopics > 1 {
		fmt.Println("topic distribution (sorted by node count, DISC-NG nodes only):")
		for _, s := range stats {
			if s.count == 0 {
				continue
			}
			fmt.Printf("  topic %d: %d nodes\n", s.topic, s.count)
		}
	}

	// Phase 1: registrations only on DISC-NG nodes.
	regStart := time.Now()
	for i, n := range discngNodes {
		n.disc.RegisterTopic(topics[nodeTopics[i]], uint64(n.idx))
	}
	fmt.Printf("registrations started; register-wait=%s\n", *registerWait)

	// Run the registration probe in parallel with register-wait.
	probeStop := make(chan struct{})
	probeDone := make(chan map[string]map[string]time.Duration)
	go func() {
		probeDone <- runRegistrationProbe(discngNodes, topics, nodeTopics, regStart, probeStop, *regProbePeriod)
	}()
	time.Sleep(*registerWait)
	close(probeStop)
	regProbe := <-probeDone

	// Phase 2: every DISC-NG node searches its own topic.
	results := runSearches(discngNodes, nodeTopics, topics, registrantsByTopic, *searchTimeout)

	report(results, registrantsByTopic, regProbe, params, *metricsOut)
	fmt.Println("teardown complete")
}

func spawnNodes(sim *simnet.Simnet, settings simnet.NodeBiDiLinkSettings, count int, discngFlags []bool) []nodeRec {
	all := make([]nodeRec, 0, count)
	for i := 0; i < count; i++ {
		key, err := crypto.GenerateKey()
		if err != nil {
			fatalf("generate key %d: %v", i, err)
		}

		// Use 4-byte IP form so endpoint addresses match what the discv5 ->
		// adapter -> WriteTo path produces; 16-byte forms surface as router
		// "unknown destination" drops.
		addr := &net.UDPAddr{
			IP:   net.IP{10, 100, byte(i / 256), byte(i%256 + 1)},
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

		// Strip the DISC-NG ENR flag for vanilla nodes so peers' filterDiscNG
		// excludes them from REGTOPIC/TOPICQUERY targets. The local topic
		// system still exists but no remote node will register on it.
		if !discngFlags[i] {
			ln.Delete(topicindex.DiscNG{})
		}

		all = append(all, nodeRec{idx: i, key: key, ln: ln, disc: disc, discng: discngFlags[i]})
	}
	return all
}

// searchResult captures the metrics for one searcher's call to TopicSearch.
type searchResult struct {
	NodeIdx          int           `json:"nodeIdx"`
	NodeID           string        `json:"nodeId"`
	Topic            int           `json:"topic"`
	Target           int           `json:"target"`
	Found            int           `json:"found"`
	FoundRegistrant  int           `json:"foundRegistrant"`
	FoundExtra       int           `json:"foundExtra"`
	FoundIDs         []string      `json:"foundIds"` // hex IDs of registrants returned by the iterator
	TimeToFirst      time.Duration `json:"timeToFirstNs"`
	TimeToCompletion time.Duration `json:"timeToCompletionNs"`
	HitTimeoutBefore bool          `json:"hitTimeout"`
}

func runSearches(searchers []nodeRec, searchTopics []int, topics []topicindex.TopicID, registrantsByTopic map[int]map[enode.ID]struct{}, timeout time.Duration) []searchResult {
	results := make([]searchResult, len(searchers))
	var wg sync.WaitGroup
	wg.Add(len(searchers))

	for i, n := range searchers {
		go func(slot int, n nodeRec, topicIdx int) {
			defer wg.Done()

			topic := topics[topicIdx]
			registrants := registrantsByTopic[topicIdx]
			// Exclude self: a node searching for a topic it also registered
			// shouldn't count itself as a discovery.
			target := len(registrants)
			if _, self := registrants[n.ln.ID()]; self {
				target--
			}

			iter := n.disc.TopicSearch(topic, uint64(n.idx))

			// Bound the search by timeout. With the cancellation fix in
			// p2p/discover (datahop/go-ethereum#30, PR fix/topicquery-cancel),
			// iter.Close() promptly unblocks any in-flight iter.Next().
			closer := time.AfterFunc(timeout, func() { iter.Close() })
			defer closer.Stop()

			start := time.Now()
			var (
				timeFirst  time.Duration
				registered int
				extra      int
				totalFound int
				foundIDs   []string
				seen       = make(map[enode.ID]struct{})
			)
			for iter.Next() {
				id := iter.Node().ID()
				if id == n.ln.ID() {
					continue
				}
				if _, dup := seen[id]; dup {
					continue
				}
				seen[id] = struct{}{}
				if timeFirst == 0 {
					timeFirst = time.Since(start)
				}
				totalFound++
				if _, ok := registrants[id]; ok {
					registered++
					foundIDs = append(foundIDs, id.String())
				} else {
					extra++
				}
				if target > 0 && registered >= target {
					iter.Close()
					break
				}
			}
			elapsed := time.Since(start)
			hitTimeout := !closer.Stop() && registered < target

			results[slot] = searchResult{
				NodeIdx:          n.idx,
				NodeID:           n.ln.ID().String(),
				Topic:            topicIdx,
				Target:           target,
				Found:            totalFound,
				FoundRegistrant:  registered,
				FoundExtra:       extra,
				FoundIDs:         foundIDs,
				TimeToFirst:      timeFirst,
				TimeToCompletion: elapsed,
				HitTimeoutBefore: hitTimeout,
			}
		}(i, n, searchTopics[i])
	}

	wg.Wait()
	return results
}

type topicReport struct {
	Topic              int           `json:"topic"`
	NumSearchers       int           `json:"numSearchers"`
	Target             int           `json:"target"`
	FullRecall         int           `json:"fullRecall"`
	MeanRecall         float64       `json:"meanRecall"`
	HitTimeout         int           `json:"hitTimeout"`
	MedianTimeToFirst  time.Duration `json:"medianTimeToFirstNs"`
	MedianTimeToFinish time.Duration `json:"medianTimeToFinishNs"`
	P95TimeToFinish    time.Duration `json:"p95TimeToFinishNs"`
}

// runRegistrationProbe polls each DISC-NG node's local topic table every
// probePeriod and records, per registrant, the first time it shows up in
// any remote node's table for its assigned topic.
//
// The returned map is keyed by topic id (hex) → registrant id (hex) →
// first-seen latency (relative to regStart).
func runRegistrationProbe(nodes []nodeRec, topics []topicindex.TopicID, nodeTopics []int, regStart time.Time, stop <-chan struct{}, probePeriod time.Duration) map[string]map[string]time.Duration {
	// Identify each registrant -> its topic.
	regTopic := make(map[enode.ID]topicindex.TopicID, len(nodes))
	for i, n := range nodes {
		regTopic[n.ln.ID()] = topics[nodeTopics[i]]
	}

	out := make(map[string]map[string]time.Duration)
	getOut := func(topic topicindex.TopicID) map[string]time.Duration {
		k := topic.String()
		if m, ok := out[k]; ok {
			return m
		}
		m := make(map[string]time.Duration)
		out[k] = m
		return m
	}

	tick := time.NewTicker(probePeriod)
	defer tick.Stop()

	probe := func() {
		now := time.Since(regStart)
		// For each topic, ask every host node what registrants it knows.
		// Exclude self-registrations: we want time-to-first-acceptance on a
		// REMOTE host, not the local table that the registrant pre-populates.
		seenByTopic := make(map[topicindex.TopicID]map[enode.ID]bool)
		for _, host := range nodes {
			hostID := host.ln.ID()
			for topic := range topicSet(topics) {
				visible := host.disc.LocalTopicNodes(topic)
				if seenByTopic[topic] == nil {
					seenByTopic[topic] = make(map[enode.ID]bool)
				}
				for _, n := range visible {
					if n.ID() == hostID {
						continue
					}
					seenByTopic[topic][n.ID()] = true
				}
			}
		}
		for topic, ids := range seenByTopic {
			m := getOut(topic)
			for id := range ids {
				if regTopic[id] == topic {
					if _, already := m[id.String()]; !already {
						m[id.String()] = now
					}
				}
			}
		}
	}

	probe()
	for {
		select {
		case <-stop:
			probe()
			return out
		case <-tick.C:
			probe()
		}
	}
}

func topicSet(topics []topicindex.TopicID) map[topicindex.TopicID]struct{} {
	s := make(map[topicindex.TopicID]struct{}, len(topics))
	for _, t := range topics {
		s[t] = struct{}{}
	}
	return s
}

func report(results []searchResult, registrantsByTopic map[int]map[enode.ID]struct{}, regProbe map[string]map[string]time.Duration, params runParams, metricsOut string) {
	if len(results) == 0 {
		fmt.Println("no searchers")
		return
	}

	// Group by topic.
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
	for _, t := range topicIdxs {
		rs := byTopic[t]
		var (
			recallSum             float64
			full, timeout         int
			counted               int
			latFirst, latComplete []time.Duration
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
			if r.TimeToFirst > 0 {
				latFirst = append(latFirst, r.TimeToFirst)
			}
			latComplete = append(latComplete, r.TimeToCompletion)
		}
		mean := 0.0
		if counted > 0 {
			mean = recallSum / float64(counted)
		}
		reports = append(reports, topicReport{
			Topic:              t,
			NumSearchers:       len(rs),
			Target:             len(registrantsByTopic[t]),
			FullRecall:         full,
			MeanRecall:         mean,
			HitTimeout:         timeout,
			MedianTimeToFirst:  percentile(latFirst, 50),
			MedianTimeToFinish: percentile(latComplete, 50),
			P95TimeToFinish:    percentile(latComplete, 95),
		})
	}

	// Print summary.
	fmt.Println()
	fmt.Println("=== per-topic summary ===")
	fmt.Printf("%-7s %10s %10s %12s %10s %10s %15s %15s\n",
		"topic", "searchers", "target", "fullRecall", "meanRec", "timeout", "t1stMedian", "tcompletionP50")
	for _, tr := range reports {
		fmt.Printf("%-7d %10d %10d %12s %10.2f %10d %15s %15s\n",
			tr.Topic, tr.NumSearchers, tr.Target,
			fmt.Sprintf("%d/%d", tr.FullRecall, tr.NumSearchers),
			tr.MeanRecall, tr.HitTimeout,
			tr.MedianTimeToFirst, tr.MedianTimeToFinish)
	}

	// Aggregate.
	var (
		totalFull, totalTimeout, counted int
		recallSum                        float64
		latFirst, latComplete            []time.Duration
	)
	for _, r := range results {
		if r.Target > 0 {
			recallSum += float64(r.FoundRegistrant) / float64(r.Target)
			counted++
			if r.FoundRegistrant >= r.Target {
				totalFull++
			}
		}
		if r.HitTimeoutBefore {
			totalTimeout++
		}
		if r.TimeToFirst > 0 {
			latFirst = append(latFirst, r.TimeToFirst)
		}
		latComplete = append(latComplete, r.TimeToCompletion)
	}
	meanAgg := 0.0
	if counted > 0 {
		meanAgg = recallSum / float64(counted)
	}
	fmt.Println()
	fmt.Println("=== aggregate ===")
	fmt.Printf("searchers:                 %d\n", len(results))
	fmt.Printf("topics with searchers:     %d\n", len(reports))
	fmt.Printf("full recall (found all):   %d / %d\n", totalFull, counted)
	fmt.Printf("mean recall:               %.2f\n", meanAgg)
	fmt.Printf("hit timeout:               %d / %d\n", totalTimeout, len(results))
	fmt.Printf("time to first result:      median=%s p95=%s\n",
		percentile(latFirst, 50), percentile(latFirst, 95))
	fmt.Printf("time to completion:        median=%s p95=%s\n",
		percentile(latComplete, 50), percentile(latComplete, 95))

	// Per-topic registration timing summary.
	if len(regProbe) > 0 {
		fmt.Println()
		fmt.Println("=== registration timing (first-seen-on-any-remote, by topic) ===")
		fmt.Printf("%-20s %10s %12s %12s %12s\n", "topic", "registered", "p50", "p90", "p99")
		topicHexs := make([]string, 0, len(regProbe))
		for k := range regProbe {
			topicHexs = append(topicHexs, k)
		}
		sort.Slice(topicHexs, func(i, j int) bool {
			return len(regProbe[topicHexs[i]]) > len(regProbe[topicHexs[j]])
		})
		for _, hex := range topicHexs {
			latencies := make([]time.Duration, 0, len(regProbe[hex]))
			for _, d := range regProbe[hex] {
				latencies = append(latencies, d)
			}
			fmt.Printf("%-20s %10d %12s %12s %12s\n",
				hex[:16]+"...", len(latencies),
				percentile(latencies, 50), percentile(latencies, 90), percentile(latencies, 99))
		}
	}

	if metricsOut != "" {
		writeMetrics(metricsOut, results, reports, regProbe, registrantsByTopic, params)
		fmt.Printf("metrics written to: %s\n", metricsOut)
	}
}

func writeMetrics(path string, results []searchResult, perTopic []topicReport, regProbe map[string]map[string]time.Duration, registrantsByTopic map[int]map[enode.ID]struct{}, params runParams) {
	// Convert registrantsByTopic to JSON-friendly: topicIdx -> [hex IDs].
	registrants := make(map[int][]string, len(registrantsByTopic))
	for t, ids := range registrantsByTopic {
		ss := make([]string, 0, len(ids))
		for id := range ids {
			ss = append(ss, id.String())
		}
		sort.Strings(ss)
		registrants[t] = ss
	}
	// Convert regProbe to JSON-friendly with ns durations.
	regTimingNs := make(map[string]map[string]int64, len(regProbe))
	for topicHex, m := range regProbe {
		mm := make(map[string]int64, len(m))
		for id, d := range m {
			mm[id] = d.Nanoseconds()
		}
		regTimingNs[topicHex] = mm
	}
	out := map[string]any{
		"perTopic":             perTopic,
		"results":              results,
		"registrantsByTopic":   registrants,
		"registrationTimingNs": regTimingNs,
		"params":               params,
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
