package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// churnParams configures node churn during the search phase.
type churnParams struct {
	Interval time.Duration // time between churn rounds (0 = no churn)
	Frac     float64       // fraction of the active population killed each round
}

// runChurnWorkload runs the multi-topic register+search workload, but kills a
// fraction of the active (DISC-NG) nodes at a fixed cadence during the search
// phase. It exercises the failure-driven blacklist/eviction path (#71): a
// killed node stops answering discv5 RPCs, so peers' revalidation pings and
// topic queries to it time out, and after MaxNodeFailures it is blacklisted and
// evicted from registration/search tables and ad caches.
//
// It reports, over time and at the end: how many nodes are blacklisted across
// the surviving network, and how many killed registrants are still visible in
// any topic table (which should decay toward zero as eviction fires).
func runChurnWorkload(all []nodeRec, numTopics int, zipfS float64, seed int64, registerWait, searchTimeout, regProbePeriod, registerStagger time.Duration, churn churnParams, metricsOut string, pacing searchPacing) {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))
	var zipf *rand.Zipf
	if numTopics > 1 {
		zipf = rand.NewZipf(rng, zipfS, 1.0, uint64(numTopics-1))
	}

	topics := make([]topicindex.TopicID, numTopics)
	for i := range topics {
		topics[i] = makeTopic(i)
	}

	// Topic assignment: only DISC-NG-active (non-legacy) nodes get a topic.
	nodeTopic := make([]int, len(all))
	registrantsByTopic := make(map[int]map[enode.ID]struct{}, numTopics)
	regTopicOf := make(map[enode.ID]int, len(all))
	var activeIdx []int
	for i := range all {
		if all[i].legacy {
			nodeTopic[i] = -1
			continue
		}
		t := 0
		if zipf != nil {
			t = int(zipf.Uint64())
		}
		nodeTopic[i] = t
		if registrantsByTopic[t] == nil {
			registrantsByTopic[t] = make(map[enode.ID]struct{})
		}
		registrantsByTopic[t][all[i].ln.ID()] = struct{}{}
		regTopicOf[all[i].ln.ID()] = t
		activeIdx = append(activeIdx, i)
	}
	fmt.Printf("churn workload: %d nodes total, %d active across %d topics; churn frac=%.2f interval=%s\n",
		len(all), len(activeIdx), numTopics, churn.Frac, churn.Interval)

	// Phase 1: register (with optional stagger).
	regStart := time.Now()
	staggered := 0
	for i, n := range all {
		if nodeTopic[i] < 0 {
			continue
		}
		if registerStagger > 0 && staggered > 0 {
			time.Sleep(registerStagger)
		}
		staggered++
		n.disc.RegisterTopic(topics[nodeTopic[i]], uint64(n.idx))
	}
	fmt.Printf("registrations started; register-wait=%s\n", registerWait)

	probeStop := make(chan struct{})
	probeDone := make(chan map[string]map[string]int64, 1)
	go func() {
		probeDone <- runRegistrationProbe(all, topics, nodeTopic, regStart, probeStop, regProbePeriod)
	}()
	time.Sleep(registerWait)
	close(probeStop)
	regTimingNs := <-probeDone

	// Baseline coverage snapshot (pre-churn).
	allCov := snapshotMultiTopicCoverage(all, registrantsByTopic, topics)
	printMultiTopicCoverage(allCov, registrantsByTopic, len(all))

	// Shared kill state.
	var (
		killMu    sync.Mutex
		killedSet = make(map[enode.ID]bool)
	)
	// Tracks kill times to measure dead results returned in searches.
	deadTracker := newDeadResultTracker()
	snapshotKilled := func() map[enode.ID]bool {
		killMu.Lock()
		defer killMu.Unlock()
		c := make(map[enode.ID]bool, len(killedSet))
		for id := range killedSet {
			c[id] = true
		}
		return c
	}

	// Pre-shuffled pool of active node records to kill from.
	pool := make([]nodeRec, 0, len(activeIdx))
	for _, i := range activeIdx {
		pool = append(pool, all[i])
	}
	rng.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })

	// Churn goroutine: kill Frac of the active population each round.
	churnStop := make(chan struct{})
	churnDone := make(chan struct{})
	go func() {
		defer close(churnDone)
		if churn.Interval <= 0 || churn.Frac <= 0 {
			return
		}
		perRound := int(float64(len(activeIdx)) * churn.Frac)
		if perRound < 1 {
			perRound = 1
		}
		tick := time.NewTicker(churn.Interval)
		defer tick.Stop()
		next, round := 0, 0
		for {
			select {
			case <-churnStop:
				return
			case <-tick.C:
				k := perRound
				if rem := len(pool) - next; k > rem {
					k = rem
				}
				if k <= 0 {
					return // exhausted the pool
				}
				round++
				killMu.Lock()
				now := time.Now()
				for c := 0; c < k; c++ {
					rec := pool[next]
					next++
					killedSet[rec.ln.ID()] = true
					deadTracker.markKilled(rec.ln.ID(), now)
					go rec.disc.Close() // detached: Close can be slow at scale
				}
				total := len(killedSet)
				killMu.Unlock()
				fmt.Printf("[churn round %d t=%ds] killed %d (total killed=%d/%d active)\n",
					round, int(time.Since(regStart).Seconds()), k, total, len(activeIdx))
			}
		}
	}()

	// Monitor goroutine: blacklist growth + killed-registrant visibility decay.
	monInterval := churn.Interval
	if monInterval <= 0 {
		monInterval = 10 * time.Second
	}
	monStop := make(chan struct{})
	monDone := make(chan struct{})
	go func() {
		defer close(monDone)
		tick := time.NewTicker(monInterval)
		defer tick.Stop()
		for {
			select {
			case <-monStop:
				return
			case <-tick.C:
				killSnap := snapshotKilled()
				totalBL, alive := 0, 0
				for _, n := range all {
					if killSnap[n.ln.ID()] {
						continue
					}
					alive++
					totalBL += n.disc.BlacklistLen()
				}
				visible := countKilledStillVisible(all, killSnap, topics, registrantsByTopic, regTopicOf)
				fmt.Printf("[monitor t=%ds] alive=%d killed=%d blacklisted(sum)=%d killedRegistrantsStillVisible=%d\n",
					int(time.Since(regStart).Seconds()), alive, len(killSnap), totalBL, visible)
			}
		}
	}()

	// Phase 2: searches run for the full search-timeout, concurrently with churn.
	results := runMultiTopicSearches(all, nodeTopic, topics, registrantsByTopic, searchTimeout, pacing, deadTracker)

	close(churnStop)
	<-churnDone
	close(monStop)
	<-monDone

	// Final churn summary.
	killSnap := snapshotKilled()
	totalBL, alive := 0, 0
	for _, n := range all {
		if killSnap[n.ln.ID()] {
			continue
		}
		alive++
		totalBL += n.disc.BlacklistLen()
	}
	visible := countKilledStillVisible(all, killSnap, topics, registrantsByTopic, regTopicOf)
	killedRegistrants := 0
	for id := range killSnap {
		if _, ok := regTopicOf[id]; ok {
			killedRegistrants++
		}
	}
	fmt.Println()
	fmt.Println("=== churn summary ===")
	fmt.Printf("  nodes killed during run:                              %d\n", len(killSnap))
	fmt.Printf("  alive nodes:                                          %d\n", alive)
	fmt.Printf("  blacklist entries across alive nodes (sum):           %d\n", totalBL)
	fmt.Printf("  killed registrants still visible in any topic table:  %d / %d  (lower = eviction working)\n",
		visible, killedRegistrants)
	deadTracker.report()

	reportMultiTopic(results, registrantsByTopic, topics, regTimingNs, metricsOut, allCov)
}

// countKilledStillVisible returns the number of distinct killed registrants
// that are still present in at least one alive node's topic table. As the
// blacklist/eviction path fires, this should decay toward zero.
func countKilledStillVisible(all []nodeRec, killed map[enode.ID]bool, topics []topicindex.TopicID, registrantsByTopic map[int]map[enode.ID]struct{}, regTopicOf map[enode.ID]int) int {
	visible := make(map[enode.ID]bool)
	for _, host := range all {
		if killed[host.ln.ID()] {
			continue // skip dead hosts
		}
		for t := range registrantsByTopic {
			for _, n := range host.disc.LocalTopicNodes(topics[t]) {
				id := n.ID()
				if killed[id] {
					visible[id] = true
				}
			}
		}
	}
	return len(visible)
}
