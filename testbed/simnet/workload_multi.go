package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

func runMultiTopicWorkload(all []nodeRec, numTopics int, zipfS float64, seed int64, registerWait, searchTimeout, regProbePeriod, registerStagger time.Duration, metricsOut string, pacing searchPacing) {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))
	zipf := rand.NewZipf(rng, zipfS, 1.0, uint64(numTopics-1))

	topics := make([]topicindex.TopicID, numTopics)
	for i := range topics {
		topics[i] = makeTopic(i)
	}

	// Topic assignment: only non-legacy (DISC-NG-flagged) nodes get a
	// topic. Legacy nodes stay passive — they participate in ordinary
	// Discv5 routing but do not register or search. nodeTopic[i] == -1
	// marks "no topic" for legacy nodes; registrantsByTopic only contains
	// flagged registrants.
	nodeTopic := make([]int, len(all))
	registrantsByTopic := make(map[int]map[enode.ID]struct{}, numTopics)
	var activeCount, legacyCount int
	for i := range all {
		if all[i].legacy {
			nodeTopic[i] = -1
			legacyCount++
			continue
		}
		t := int(zipf.Uint64())
		nodeTopic[i] = t
		if registrantsByTopic[t] == nil {
			registrantsByTopic[t] = make(map[enode.ID]struct{})
		}
		registrantsByTopic[t][all[i].ln.ID()] = struct{}{}
		activeCount++
	}

	dist := make([]int, numTopics)
	for _, t := range nodeTopic {
		if t >= 0 {
			dist[t]++
		}
	}
	fmt.Printf("workload: %d nodes total, %d DISC-NG-active across %d topics (Zipf s=%.2f, seed=%d), %d legacy passive\n",
		len(all), activeCount, numTopics, zipfS, seed, legacyCount)
	for t, c := range dist {
		fmt.Printf("  topic %d: %d nodes\n", t, c)
	}

	// Phase 1: register. Optionally stagger the start of each registrant's
	// RegisterTopic call. Synchronized starts cause synchronized AdLifetime
	// expiries: when the renewal path demotes a Registered attempt back to
	// Standby (registration.go), N nodes try to renew at the same wall-clock
	// moment, saturating router/link buffers. Spreading initial calls across
	// a window keeps expiries/renewals spread across the same window. Legacy
	// nodes are skipped — they remain passive Discv5 peers.
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
	fmt.Printf("registrations started; register-wait=%s probe-period=%s register-stagger=%s\n", registerWait, regProbePeriod, registerStagger)

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
	results := runMultiTopicSearches(all, nodeTopic, topics, registrantsByTopic, searchTimeout, pacing)
	reportMultiTopic(results, registrantsByTopic, topics, regTimingNs, metricsOut, allCov)
}

// runRegistrationProbe polls each node's LocalTopicNodes(topic) every
// probePeriod and records, per registrant, the first wall-clock time
// (relative to regStart, in nanoseconds) that the registrant appears in any
// other host's topic table. Self-registrations are excluded — we want
// time-to-first-remote-admission, not local pre-population.
//
// Returned map: topicHex -> registrantIdHex -> ns since regStart.
func runRegistrationProbe(all []nodeRec, topics []topicindex.TopicID, nodeTopic []int, regStart time.Time, stop <-chan struct{}, period time.Duration) map[string]map[string]int64 {
	regTopic := make(map[enode.ID]topicindex.TopicID, len(all))
	for i, n := range all {
		if nodeTopic[i] < 0 {
			continue // legacy node, not a registrant
		}
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
