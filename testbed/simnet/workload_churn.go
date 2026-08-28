package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/marcopolo/simnet"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// churnParams configures node churn during the search phase.
type churnParams struct {
	Interval    time.Duration // time between churn rounds (0 = no churn)
	Frac        float64       // fraction of the active population churned each round
	SteadyState bool          // if true, each churn action is 50/50 leave/join (stable population); else kill-only decay
}

// churnState tracks the live node population as it changes under steady-state
// churn (nodes leaving and joining). It is the source of truth for which nodes
// are alive (kill candidates and bootstrap sources) and which have been killed.
type churnState struct {
	mu       sync.Mutex
	all      []nodeRec            // every node ever created (alive + dead); for monitor + teardown
	alive    []nodeRec            // currently-alive active nodes (leave candidates / bootstrap pool)
	aliveIdx map[enode.ID]int     // id -> index in alive, for swap-remove
	killed   map[enode.ID]bool    // ids that have left
	nextIdx  int                  // next node index (endpoint IP) for a joiner
}

func newChurnState(initial []nodeRec, activeIdx []int, nextIdx int) *churnState {
	cs := &churnState{
		all:      append([]nodeRec(nil), initial...),
		aliveIdx: make(map[enode.ID]int, len(activeIdx)),
		killed:   make(map[enode.ID]bool),
		nextIdx:  nextIdx,
	}
	for _, i := range activeIdx {
		cs.aliveIdx[initial[i].ln.ID()] = len(cs.alive)
		cs.alive = append(cs.alive, initial[i])
	}
	return cs
}

// leaveRandom removes a random alive node, marks it killed, and returns it.
func (cs *churnState) leaveRandom(rng *rand.Rand) (nodeRec, bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if len(cs.alive) == 0 {
		return nodeRec{}, false
	}
	i := rng.Intn(len(cs.alive))
	rec := cs.alive[i]
	last := len(cs.alive) - 1
	cs.alive[i] = cs.alive[last]
	cs.aliveIdx[cs.alive[i].ln.ID()] = i
	cs.alive = cs.alive[:last]
	delete(cs.aliveIdx, rec.ln.ID())
	cs.killed[rec.ln.ID()] = true
	return rec, true
}

// join adds a newly-spawned node to the alive population.
func (cs *churnState) join(rec nodeRec) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.all = append(cs.all, rec)
	cs.aliveIdx[rec.ln.ID()] = len(cs.alive)
	cs.alive = append(cs.alive, rec)
}

// nextIndex returns a fresh node index (and the endpoint IP it implies).
func (cs *churnState) nextIndex() int {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	idx := cs.nextIdx
	cs.nextIdx++
	return idx
}

func (cs *churnState) aliveSample(max int, rng *rand.Rand) []*enode.Node {
	cs.mu.Lock()
	pool := append([]nodeRec(nil), cs.alive...)
	cs.mu.Unlock()
	return sampleBootnodes(pool, max, rng)
}

func (cs *churnState) snapshotAll() []nodeRec {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return append([]nodeRec(nil), cs.all...)
}

func (cs *churnState) snapshotKilled() map[enode.ID]bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	c := make(map[enode.ID]bool, len(cs.killed))
	for id := range cs.killed {
		c[id] = true
	}
	return c
}

func (cs *churnState) counts() (alive, killed, total int) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return len(cs.alive), len(cs.killed), len(cs.all)
}

// sampleAliveBlacklist sums BlacklistLen() over up to max randomly-sampled alive
// nodes, returning the sum and the sample size. Sampling bounds the cost so the
// monitor stays responsive while 10k searchers contend on the topic systems.
func (cs *churnState) sampleAliveBlacklist(max int, rng *rand.Rand) (sum, sampled int) {
	cs.mu.Lock()
	alive := append([]nodeRec(nil), cs.alive...)
	cs.mu.Unlock()
	if max > 0 && len(alive) > max {
		rng.Shuffle(len(alive), func(i, j int) { alive[i], alive[j] = alive[j], alive[i] })
		alive = alive[:max]
	}
	for _, n := range alive {
		sum += n.disc.BlacklistLen()
	}
	return sum, len(alive)
}

// runChurnWorkload runs the multi-topic register+search workload while churning
// the active population during the search phase.
//
// In steady-state mode (the default) each churn action is a coin flip: with
// probability 0.5 a random live node leaves (is killed), and with probability
// 0.5 a fresh node joins — it spawns, bootstraps off the live network, registers
// its (Zipf-drawn) topic, and starts searching. The population therefore stays
// roughly constant rather than draining to zero, so the network keeps
// functioning and the dead-result metrics reflect steady-state churn instead of
// collapse. In kill-only mode it only kills, as before.
//
// It reports, over time and at the end: alive/killed counts, blacklist growth
// across the surviving network, how many killed registrants are still visible in
// some topic table (should decay as eviction fires), and the dead-result metrics
// (how often searches return already-dead registrants, and how stale they were).
func runChurnWorkload(sim *simnet.Simnet, settings simnet.NodeBiDiLinkSettings, maxBootnodes int, refreshInterval time.Duration, all []nodeRec, numTopics int, zipfS float64, seed int64, registerWait, searchTimeout, regProbePeriod, registerStagger time.Duration, churn churnParams, metricsOut string, pacing searchPacing) {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))
	var zipf *rand.Zipf
	if numTopics > 1 {
		zipf = rand.NewZipf(rng, zipfS, 1.0, uint64(numTopics-1))
	}
	drawTopic := func() int {
		if zipf == nil {
			return 0
		}
		return int(zipf.Uint64())
	}

	topics := make([]topicindex.TopicID, numTopics)
	for i := range topics {
		topics[i] = makeTopic(i)
	}

	// Topic assignment for the initial population. Only DISC-NG-active
	// (non-legacy) nodes get a topic; legacy nodes stay passive substrate.
	mem := newTopicMembership(numTopics)
	nodeTopic := make([]int, len(all))
	var activeIdx []int
	for i := range all {
		if all[i].legacy {
			nodeTopic[i] = -1
			continue
		}
		t := drawTopic()
		nodeTopic[i] = t
		mem.add(all[i].ln.ID(), t)
		activeIdx = append(activeIdx, i)
	}
	mode := "kill-only"
	if churn.SteadyState {
		mode = "steady-state (50/50 leave/join)"
	}
	fmt.Printf("churn workload: %d nodes total, %d active across %d topics; mode=%s frac=%.2f interval=%s\n",
		len(all), len(activeIdx), numTopics, mode, churn.Frac, churn.Interval)

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
	baselineReg := mem.snapshot()
	allCov := snapshotMultiTopicCoverage(all, baselineReg, topics)
	printMultiTopicCoverage(allCov, baselineReg, len(all))

	// Live population state + dead-result tracker.
	cs := newChurnState(all, activeIdx, len(all))
	deadTracker := newDeadResultTracker()

	// Search phase: all searchers share one absolute deadline so late
	// joiners stop with everyone else. Joiners are launched dynamically by
	// the churn goroutine via sm.launch.
	searchDeadline := time.Now().Add(searchTimeout)
	sm := &searchManager{
		deadline:    searchDeadline,
		pacing:      pacing,
		topics:      topics,
		mem:         mem,
		stats:       newLiveStats(numTopics, baselineReg),
		deadTracker: deadTracker,
	}
	checkpointStop := make(chan struct{})
	checkpointDone := make(chan struct{})
	if pacing.Checkpoint > 0 {
		go sm.stats.runCheckpoints(pacing.Checkpoint, checkpointStop, checkpointDone)
	} else {
		close(checkpointDone)
	}
	for i := range all {
		sm.launch(all[i], nodeTopic[i])
	}

	// Churn goroutine: each round performs perRound actions. In steady-state
	// mode each action is 50/50 leave/join; in kill-only mode each is a leave.
	var (
		churnMu sync.Mutex
		joins   int
	)
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
		round := 0
		for {
			select {
			case <-churnStop:
				return
			case <-tick.C:
				// Stop churning once the search deadline passes, so no
				// searchers are launched after sm.wait begins draining.
				if !time.Now().Before(searchDeadline) {
					return
				}
				round++
				now := time.Now()
				leaves, roundJoins := 0, 0
				for c := 0; c < perRound; c++ {
					if churn.SteadyState && rng.Float64() < 0.5 {
						// JOIN: spawn a fresh node, register, start searching.
						idx := cs.nextIndex()
						boot := cs.aliveSample(maxBootnodes, rng)
						rec := spawnNode(sim, settings, idx, false, boot, refreshInterval)
						t := drawTopic()
						mem.add(rec.ln.ID(), t)
						sm.stats.addAssigned(t)
						cs.join(rec)
						rec.disc.RegisterTopic(topics[t], uint64(rec.idx))
						sm.launch(rec, t)
						roundJoins++
						continue
					}
					// LEAVE: kill a random live node.
					rec, ok := cs.leaveRandom(rng)
					if !ok {
						if churn.SteadyState {
							continue // nothing to kill; next action may join
						}
						return // kill-only: pool exhausted
					}
					deadTracker.markKilled(rec.ln.ID(), now)
					go rec.disc.Close() // detached: Close can be slow at scale
					leaves++
				}
				churnMu.Lock()
				joins += roundJoins
				churnMu.Unlock()
				alive, killed, total := cs.counts()
				fmt.Printf("[churn round %d t=%ds] leaves=%d joins=%d | alive=%d killed=%d total=%d\n",
					round, int(time.Since(regStart).Seconds()), leaves, roundJoins, alive, killed, total)
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
	monRng := rand.New(rand.NewSource(1))
	go func() {
		defer close(monDone)
		tick := time.NewTicker(monInterval)
		defer tick.Stop()
		for {
			select {
			case <-monStop:
				return
			case <-tick.C:
				// Keep each tick cheap: live counts are O(1) under the
				// churnState lock, and the blacklist sum is sampled. The
				// expensive full topic-table walk (countKilledStillVisible)
				// is deferred to the final summary, when searches have
				// stopped and lock contention is gone — doing it per-tick
				// here stalls the monitor for minutes at 10k nodes.
				alive, killed, total := cs.counts()
				blSum, sampled := cs.sampleAliveBlacklist(2000, monRng)
				fmt.Printf("[monitor t=%ds] alive=%d killed=%d total=%d blacklisted(sum over %d sampled)=%d\n",
					int(time.Since(regStart).Seconds()), alive, killed, total, sampled, blSum)
			}
		}
	}()

	// Wait for searches (initial + joined) to finish, bounded so a wedged
	// searcher cannot hang the run; the report is produced from whatever
	// completed.
	results := sm.wait(searchTimeout + 120*time.Second)

	close(churnStop)
	<-churnDone
	close(monStop)
	<-monDone
	close(checkpointStop)
	<-checkpointDone

	// Deliverables first. These are the point of the run and are cheap: they
	// process already-collected results, not live per-node state. Emit them
	// before any full-population scan so a slow (or watchdog-truncated) probe
	// below can never prevent the metrics from being written.
	alive, killed, total := cs.counts()
	churnMu.Lock()
	totalJoins := joins
	churnMu.Unlock()
	blSum, blSampled := cs.sampleAliveBlacklist(2000, rand.New(rand.NewSource(3)))
	fmt.Println()
	fmt.Println("=== churn summary ===")
	fmt.Printf("  mode:                                              %s\n", mode)
	fmt.Printf("  nodes that joined during run:                      %d\n", totalJoins)
	fmt.Printf("  nodes killed during run:                           %d\n", killed)
	fmt.Printf("  alive nodes at end:                                %d (of %d ever created)\n", alive, total)
	fmt.Printf("  blacklist entries (sum over %d sampled alive):    %d\n", blSampled, blSum)
	deadTracker.report()
	reportMultiTopic(results, mem.snapshot(), topics, regTimingNs, metricsOut, allCov)

	// Best-effort eviction-health probe LAST. The per-host topic-table walk is
	// slow at 10k (every node is still running and contends on its topic
	// system), so it is host-sampled and placed after the metrics above; if the
	// watchdog truncates it, nothing important is lost.
	nodes := cs.snapshotAll()
	killSnap := cs.snapshotKilled()
	killedRegistrants := 0
	for id := range killSnap {
		if _, ok := mem.topicFor(id); ok {
			killedRegistrants++
		}
	}
	visible, visibleHosts := countKilledStillVisible(nodes, killSnap, topics, 1000, rand.New(rand.NewSource(2)))
	fmt.Printf("killed registrants still visible (sampled %d hosts): %d / %d  (lower = eviction working)\n",
		visibleHosts, visible, killedRegistrants)
}

// countKilledStillVisible returns the number of distinct killed registrants that
// are still present in at least one (sampled) alive host's topic table, plus the
// number of hosts actually inspected. As the blacklist/eviction path fires this
// should decay toward zero. Hosts are sampled to maxHosts because each host walk
// copies that host's topic tables; an unbounded scan is too slow at 10k nodes.
func countKilledStillVisible(nodes []nodeRec, killed map[enode.ID]bool, topics []topicindex.TopicID, maxHosts int, rng *rand.Rand) (visibleCount, hosts int) {
	if maxHosts > 0 && len(nodes) > maxHosts {
		nodes = append([]nodeRec(nil), nodes...)
		rng.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
		nodes = nodes[:maxHosts]
	}
	visible := make(map[enode.ID]bool)
	inspected := 0
	for _, host := range nodes {
		if killed[host.ln.ID()] {
			continue // skip dead hosts
		}
		inspected++
		for t := range topics {
			for _, n := range host.disc.LocalTopicNodes(topics[t]) {
				id := n.ID()
				if killed[id] {
					visible[id] = true
				}
			}
		}
	}
	return len(visible), inspected
}
