package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/marcopolo/simnet"

	"github.com/ethereum/go-ethereum/p2p/enode"
)

// spawnMixed spawns a mixed population of fork (TopDisc) and vanilla (stock geth
// v1.17.3) discv5 nodes on the simnet transport, cross-bootstrapped through a
// shared pool of ENR strings so the two stacks form a single DHT. vanillaFrac of
// the nodes run the vanilla stack; the rest run TopDisc. Bootstrap URLs are
// sampled from all previously-spawned nodes regardless of type, and each node
// parses them with its own stack's parser.
func spawnMixed(sim *simnet.Simnet, settings simnet.NodeBiDiLinkSettings, total int, vanillaFrac float64, maxBootnodes int, spawnDelay, refreshInterval time.Duration, seed int64) ([]nodeRec, []vanillaRec) {
	if maxBootnodes <= 0 {
		maxBootnodes = defaultMaxBootnodes
	}
	vanillaSet := pickLegacySet(total, vanillaFrac, seed)
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))

	var (
		forks    []nodeRec
		vanillas []vanillaRec
		urlPool  []string // ENR strings of all nodes spawned so far
	)
	for i := 0; i < total; i++ {
		if i > 0 && spawnDelay > 0 {
			time.Sleep(spawnDelay)
		}
		// Bootstrap set: the first node, plus a random sample of predecessors,
		// capped at maxBootnodes (see defaultMaxBootnodes for the rationale).
		var bootURLs []string
		if len(urlPool) > 0 {
			bootURLs = append(bootURLs, urlPool[0])
			pool := urlPool[1:]
			n := maxBootnodes - 1
			if n > len(pool) {
				n = len(pool)
			}
			for _, idx := range rng.Perm(len(pool))[:n] {
				bootURLs = append(bootURLs, pool[idx])
			}
		}

		if vanillaSet[i] {
			v := spawnVanillaNode(sim, settings, i, bootURLs, refreshInterval)
			vanillas = append(vanillas, v)
			urlPool = append(urlPool, v.url())
			continue
		}
		// Fork (TopDisc) node: parse bootstrap URLs with the fork's own parser.
		var boot []*enode.Node
		for _, u := range bootURLs {
			if nn, err := enode.Parse(enode.ValidSchemes, u); err == nil {
				boot = append(boot, nn)
			}
		}
		f := spawnNode(sim, settings, i, false, boot, refreshInterval)
		forks = append(forks, f)
		urlPool = append(urlPool, f.ln.Node().String())
	}
	return forks, vanillas
}

// closeMixed shuts down both node populations in parallel.
func closeMixed(forks []nodeRec, vanillas []vanillaRec) {
	var wg sync.WaitGroup
	wg.Add(len(forks) + len(vanillas))
	for _, f := range forks {
		go func(f nodeRec) { defer wg.Done(); f.disc.Close() }(f)
	}
	for _, v := range vanillas {
		go func(v vanillaRec) { defer wg.Done(); v.disc.Close() }(v)
	}
	wg.Wait()
}

// runVanillaInterop spawns a mixed TopDisc + stock-geth network, runs the
// multi-topic register+search workload on the TopDisc nodes (with vanilla nodes
// as pure routing substrate), and reports both topic-discovery coverage and the
// cross-stack DHT-merge metric. This is the real-binary analog of the
// -legacy-frac penetration sweep: it measures whether TopDisc discovery holds up
// when most of the network is actual upstream geth.
func runVanillaInterop(sim *simnet.Simnet, settings simnet.NodeBiDiLinkSettings, nodes int, vanillaFrac float64, numTopics int, zipfS float64, seed int64, bootstrapWait, registerWait, searchTimeout, regProbePeriod, registerStagger, refreshInterval time.Duration, maxBootnodes int, spawnDelay time.Duration, metricsOut string, pacing searchPacing) {
	forks, vanillas := spawnMixed(sim, settings, nodes, vanillaFrac, maxBootnodes, spawnDelay, refreshInterval, seed)
	defer closeMixed(forks, vanillas)

	fmt.Printf("vanilla-interop: %d total = %d TopDisc + %d vanilla (stock geth v1.17.3); penetration=%.0f%%\n",
		nodes, len(forks), len(vanillas), 100*(1-vanillaFrac))
	fmt.Printf("simnet up; bootstrap-wait=%s\n", bootstrapWait)
	time.Sleep(bootstrapWait)

	// Topic register+search runs on the TopDisc nodes only; vanilla nodes route
	// but never register/search.
	runMultiTopicWorkload(forks, numTopics, zipfS, seed, registerWait, searchTimeout, regProbePeriod, registerStagger, metricsOut, pacing)

	reportInterop(forks, vanillas, 2000)
}
