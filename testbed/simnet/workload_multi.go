package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// commonTopicMode, set via -common-topic, makes every node register+search a
// universal topic 0 plus one Zipf-drawn topic from 1..numTopics-1.
var commonTopicMode bool

// reachOut, set via -reach-out, triggers the per-searcher reach dump.
var reachOut string

// dumpReach writes every registrar's topic-table contents plus the sampled
// per-searcher queried-registrar sets, for offline bottleneck localization.
func dumpReach(path string, all []nodeRec, topics []topicindex.TopicID) {
	// Per-searcher, per-registrar stats: [firstCycle, nQueries, nDistinctAds].
	// In-memory low-contention sampling; no end-of-run table read, nothing to corrupt.
	sr := make(map[string]map[string][]int)
	for self, recs := range topicindex.ReachData() {
		m := make(map[string][]int, len(recs))
		for _, r := range recs {
			m[fmt.Sprintf("%x", r.Reg)] = []int{r.FirstCycle, r.NQueries, r.NDistinct}
		}
		sr[fmt.Sprintf("%x", self)] = m
	}
	// Registrar contents (compact): per topic, fan-out count per registrant and
	// load count per registrar (both plain int maps), plus the full registrar
	// list only for near-topic registrants (the funnel sample). This avoids
	// building/encoding a giant registrant->[registrars] map that stalls the
	// shutdown dump at 10k.
	type regContents struct {
		Fanout map[string]int      `json:"fanout"` // registrant -> #registrars holding its ad
		Load   map[string]int      `json:"load"`   // registrar  -> #ads held for this topic
		Sample map[string][]string `json:"sample"` // near-topic registrant -> [registrar ids]
	}
	contents := make(map[string]*regContents, len(topics))
	for _, topic := range topics {
		tid := enode.ID(topic)
		rc := &regContents{Fanout: make(map[string]int), Load: make(map[string]int), Sample: make(map[string][]string)}
		for _, host := range all {
			hid := host.ln.ID().String()
			held := host.disc.LocalTopicNodes(topic)
			rc.Load[hid] = len(held)
			for _, n := range held {
				rid := n.ID().String()
				rc.Fanout[rid]++
				if enode.LogDist(tid, n.ID()) <= 250 { // near-topic funnel: keep full list
					rc.Sample[rid] = append(rc.Sample[rid], hid)
				}
			}
		}
		contents[topic.String()] = rc
	}
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reach: %v\n", err)
		return
	}
	defer f.Close()
	json.NewEncoder(f).Encode(map[string]any{"searchers": sr, "registrarContents": contents})
	fmt.Printf("reach written to: %s (%d searchers, %d topics contents)\n", path, len(sr), len(contents))
}
func runMultiTopicWorkload(all []nodeRec, numTopics int, zipfS float64, seed int64, registerWait, searchTimeout, regProbePeriod, registerStagger time.Duration, metricsOut string, pacing searchPacing) {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))
	var zipf *rand.Zipf
	if numTopics > 1 {
		zmax := numTopics - 1
		if commonTopicMode {
			zmax = numTopics - 2 // second topic drawn from 1..numTopics-1
		}
		zipf = rand.NewZipf(rng, zipfS, 1.0, uint64(zmax))
	}

	topics := make([]topicindex.TopicID, numTopics)
	for i := range topics {
		topics[i] = makeTopic(i)
	}

	// Topic assignment: only non-legacy (DISC-NG-flagged) nodes get a
	// topic. Legacy nodes stay passive — they participate in ordinary
	// Discv5 routing but do not register or search. nodeTopic[i] == -1
	// marks "no topic" for legacy nodes; registrantsByTopic only contains
	// flagged registrants.
	nodeTopics := make([][]int, len(all))
	registrantsByTopic := make(map[int]map[enode.ID]struct{}, numTopics)
	var activeCount, legacyCount int
	for i := range all {
		if all[i].legacy {
			nodeTopics[i] = []int{-1}
			legacyCount++
			continue
		}
		var ts []int
		if commonTopicMode {
			ts = []int{0, 1 + int(zipf.Uint64())}
		} else if numTopics > 1 {
			ts = []int{int(zipf.Uint64())}
		} else {
			ts = []int{0}
		}
		nodeTopics[i] = ts
		for _, t := range ts {
			if registrantsByTopic[t] == nil {
				registrantsByTopic[t] = make(map[enode.ID]struct{})
			}
			registrantsByTopic[t][all[i].ln.ID()] = struct{}{}
		}
		activeCount++
	}

	dist := make([]int, numTopics)
	for _, ts := range nodeTopics {
		for _, t := range ts {
			if t >= 0 {
				dist[t]++
			}
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
		if nodeTopics[i][0] < 0 {
			continue
		}
		if registerStagger > 0 && staggered > 0 {
			time.Sleep(registerStagger)
		}
		staggered++
		for _, t := range nodeTopics[i] {
			n.disc.RegisterTopic(topics[t], uint64(n.idx))
		}
	}
	fmt.Printf("registrations started; register-wait=%s probe-period=%s register-stagger=%s\n", registerWait, regProbePeriod, registerStagger)

	// Run the registration probe in parallel with register-wait so we can
	// timestamp the first time each registrant shows up in any registrar's
	// topic table.
	probeStop := make(chan struct{})
	probeDone := make(chan map[string]map[string]int64, 1)
	go func() {
		probeDone <- runRegistrationProbe(all, topics, nodeTopics, regStart, probeStop, regProbePeriod)
	}()
	time.Sleep(registerWait)
	close(probeStop)
	regTimingNs := <-probeDone

	// Per-topic coverage snapshot.
	allCov := snapshotMultiTopicCoverage(all, registrantsByTopic, topics)
	printMultiTopicCoverage(allCov, registrantsByTopic, len(all))

	// Phase 2: searches.
	results := runMultiTopicSearches(all, nodeTopics, topics, registrantsByTopic, searchTimeout, pacing, nil)
	reportMultiTopic(results, registrantsByTopic, topics, regTimingNs, metricsOut, allCov)
	if reachOut != "" {
		dumpReach(reachOut, all, topics)
	}
}

// runRegistrationProbe polls each node's LocalTopicNodes(topic) every
// probePeriod and records, per registrant, the first wall-clock time
// (relative to regStart, in nanoseconds) that the registrant appears in any
// other host's topic table. Self-registrations are excluded — we want
// time-to-first-remote-admission, not local pre-population.
//
// Returned map: topicHex -> registrantIdHex -> ns since regStart.
func runRegistrationProbe(all []nodeRec, topics []topicindex.TopicID, nodeTopics [][]int, regStart time.Time, stop <-chan struct{}, period time.Duration) map[string]map[string]int64 {
	member := make(map[enode.ID]map[topicindex.TopicID]bool, len(all))
	for i, n := range all {
		for _, ti := range nodeTopics[i] {
			if ti < 0 {
				continue // legacy node, not a registrant
			}
			if member[n.ln.ID()] == nil {
				member[n.ln.ID()] = make(map[topicindex.TopicID]bool)
			}
			member[n.ln.ID()][topics[ti]] = true
		}
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
					if !member[id][topic] {
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
