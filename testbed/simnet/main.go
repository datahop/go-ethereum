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

// testTopic is a fixed topic ID used by the workload. Identical across runs
// so behaviour is reproducible.
var testTopic = topicindex.TopicID{0x55, 0x49, 0x43, 0x4e, 0x47, 0x54, 0x45, 0x53, 0x54}

type nodeRec struct {
	idx  int
	key  *ecdsa.PrivateKey
	ln   *enode.LocalNode
	disc *discover.UDPv5
}

func main() {
	nodes := flag.Int("nodes", 5, "number of discv5 nodes to spawn")
	latencyMs := flag.Int("latency", 30, "static per-pair latency in milliseconds")
	bandwidthMibps := flag.Int("bandwidth-mibps", 100, "per-direction bandwidth (Mibps)")
	bootstrapWait := flag.Duration("bootstrap-wait", 3*time.Second, "wait after spawning before starting workload")
	registerWait := flag.Duration("register-wait", 5*time.Second, "wait after starting registrations before starting searches")
	searchTimeout := flag.Duration("search-timeout", 30*time.Second, "max time per search before giving up")
	registerFrac := flag.Float64("register-frac", 0.5, "fraction of nodes that register the test topic; rest search for it")
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

	all := spawnNodes(sim, settings, *nodes)

	sim.Start()
	defer sim.Close()
	defer func() {
		for _, n := range all {
			n.disc.Close()
		}
	}()

	fmt.Printf("simnet up; bootstrap-wait=%s\n", *bootstrapWait)
	time.Sleep(*bootstrapWait)

	// Split nodes into registrants and searchers.
	numRegistrants := int(float64(*nodes) * *registerFrac)
	if numRegistrants < 1 {
		numRegistrants = 1
	}
	if numRegistrants >= *nodes {
		numRegistrants = *nodes - 1
	}
	registrants := all[:numRegistrants]
	searchers := all[numRegistrants:]

	registrantIDs := make(map[enode.ID]struct{}, len(registrants))
	for _, n := range registrants {
		registrantIDs[n.ln.ID()] = struct{}{}
	}

	fmt.Printf("workload: %d registrants, %d searchers, topic=%x\n",
		len(registrants), len(searchers), testTopic[:])

	// Phase 1: registrations.
	for _, n := range registrants {
		n.disc.RegisterTopic(testTopic, uint64(n.idx))
	}
	fmt.Printf("registrations started; register-wait=%s\n", *registerWait)
	time.Sleep(*registerWait)

	// Phase 2: searches.
	results := runSearches(searchers, registrantIDs, len(registrants), *searchTimeout)

	report(results, len(registrants), *metricsOut)
	fmt.Println("teardown complete")
}

func spawnNodes(sim *simnet.Simnet, settings simnet.NodeBiDiLinkSettings, count int) []nodeRec {
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

		all = append(all, nodeRec{idx: i, key: key, ln: ln, disc: disc})
	}
	return all
}

// searchResult captures the metrics for one searcher's call to TopicSearch.
type searchResult struct {
	NodeIdx           int           `json:"nodeIdx"`
	NodeID            string        `json:"nodeId"`
	Found             int           `json:"found"`
	FoundRegistrant   int           `json:"foundRegistrant"` // distinct found nodes that are real registrants
	FoundExtra        int           `json:"foundExtra"`      // returned but not in the registrant set
	TimeToFirst       time.Duration `json:"timeToFirstNs"`
	TimeToCompletion  time.Duration `json:"timeToCompletionNs"`
	HitTimeoutBefore  bool          `json:"hitTimeout"`
	FoundIDs          []string      `json:"foundIds"`
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
			)
			for iter.Next() {
				if timeFirst == 0 {
					timeFirst = time.Since(start)
				}
				id := iter.Node().ID()
				found = append(found, id)
				if _, ok := registrants[id]; ok {
					registered++
				} else {
					extra++
				}
				if registered >= target {
					iter.Close()
					break
				}
			}
			elapsed := time.Since(start)

			ids := make([]string, 0, len(found))
			for _, id := range found {
				ids = append(ids, id.TerminalString())
			}
			select {
			case <-done:
				results[slot] = searchResult{
					NodeIdx:          n.idx,
					NodeID:           n.ln.ID().TerminalString(),
					Found:            len(found),
					FoundRegistrant:  registered,
					FoundExtra:       extra,
					TimeToFirst:      timeFirst,
					TimeToCompletion: elapsed,
					HitTimeoutBefore: true,
					FoundIDs:         ids,
				}
			default:
				results[slot] = searchResult{
					NodeIdx:          n.idx,
					NodeID:           n.ln.ID().TerminalString(),
					Found:            len(found),
					FoundRegistrant:  registered,
					FoundExtra:       extra,
					TimeToFirst:      timeFirst,
					TimeToCompletion: elapsed,
					HitTimeoutBefore: false,
					FoundIDs:         ids,
				}
			}
		}(i, n)
	}

	wg.Wait()
	return results
}

func report(results []searchResult, target int, metricsOut string) {
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
		writeMetrics(metricsOut, results, target)
		fmt.Printf("metrics written to: %s\n", metricsOut)
	}
}

func writeMetrics(path string, results []searchResult, target int) {
	out := map[string]any{
		"target":     target,
		"numSearchers": len(results),
		"results":    results,
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
