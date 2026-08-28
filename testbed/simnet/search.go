package main

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// searchPacing controls how a searcher progresses through the iterator.
// All three fields are optional (zero = disabled). Together they let a
// workload simulate paced consumption rather than maxed-out polling, and
// avoid 10k searchers hammering the network simultaneously.
type searchPacing struct {
	// Stagger is the per-slot delay before a goroutine kicks off its
	// TopicSearch call. Goroutine N waits N*Stagger. Spreads search
	// activity across a window rather than starting all simultaneously.
	Stagger time.Duration

	// MaxPause is the upper bound for the random sleep inserted between
	// each iter.Next() call. A random duration in [0, MaxPause) is drawn
	// per iteration. Models a real application that doesn't consume
	// results back-to-back.
	MaxPause time.Duration

	// TargetCount, if > 0, causes a searcher to close its iterator as
	// soon as it has seen this many *distinct* registrants. Stops the
	// search early — a real application typically needs a handful of
	// peers, not all of them.
	TargetCount int

	// Checkpoint, if > 0, prints a per-topic find-count snapshot at this
	// cadence while searches are running. Lets a long search-timeout run
	// produce progress signal instead of one final report.
	Checkpoint time.Duration
}

// searchResult captures the metrics for one searcher's call to TopicSearch.
type searchResult struct {
	NodeIdx          int           `json:"nodeIdx"`
	NodeID           string        `json:"nodeId"`
	Topic            int           `json:"topic"` // 0 in single-topic mode
	Target           int           `json:"target"`
	Found            int           `json:"found"`
	FoundRegistrant  int           `json:"foundRegistrant"` // duplicate-counting (every yield of a registrant counts)
	UniqueRegistrant int           `json:"uniqueRegistrant"` // distinct registrant IDs seen by this searcher
	FoundExtra       int           `json:"foundExtra"`      // returned but not in the registrant set
	TimeToFirst      time.Duration `json:"timeToFirstNs"`
	TimeToCompletion time.Duration `json:"timeToCompletionNs"`
	HitTimeoutBefore bool          `json:"hitTimeout"`
	FoundIDs         []string      `json:"foundIds"`
	// FoundRegistrantIDs is the SET of registrant IDs this searcher
	// found (duplicates collapsed). Used to compute per-registrant
	// find-count distributions in the report: "how many of the
	// searchers found each registrant".
	FoundRegistrantIDs []string `json:"foundRegistrantIds"`
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
			foundRegIDs := make([]string, 0, len(seenReg))
			for rid := range seenReg {
				foundRegIDs = append(foundRegIDs, rid.TerminalString())
			}
			results[slot] = searchResult{
				NodeIdx:            n.idx,
				NodeID:             n.ln.ID().TerminalString(),
				Found:              len(found),
				FoundRegistrant:    registered,
				UniqueRegistrant:   len(seenReg),
				FoundExtra:         extra,
				TimeToFirst:        timeFirst,
				TimeToCompletion:   elapsed,
				HitTimeoutBefore:   hitTimeout,
				FoundIDs:           ids,
				FoundRegistrantIDs: foundRegIDs,
				UniqueFoundAtMs:    uniqueAtMs,
			}
		}(i, n)
	}

	wg.Wait()
	return results
}

func runMultiTopicSearches(all []nodeRec, nodeTopic []int, topics []topicindex.TopicID, registrantsByTopic map[int]map[enode.ID]struct{}, timeout time.Duration, pacing searchPacing, deadTracker *deadResultTracker) []searchResult {
	results := make([]searchResult, len(all))
	var wg sync.WaitGroup
	wg.Add(len(all))

	stats := newLiveStats(len(topics), registrantsByTopic)
	checkpointStop := make(chan struct{})
	checkpointDone := make(chan struct{})
	if pacing.Checkpoint > 0 {
		go stats.runCheckpoints(pacing.Checkpoint, checkpointStop, checkpointDone)
	} else {
		close(checkpointDone)
	}

	// Static registrant sets: membership never changes here, so a plain map
	// read is race-free.
	has := func(id enode.ID, topic int) bool {
		_, ok := registrantsByTopic[topic][id]
		return ok
	}
	deadline := time.Now().Add(timeout)

	for i, n := range all {
		go func(slot int, n nodeRec, topicIdx int) {
			defer wg.Done()
			if topicIdx < 0 {
				return // legacy nodes stay passive
			}
			// Stagger spreads concurrent search activity across the run.
			if pacing.Stagger > 0 {
				time.Sleep(time.Duration(slot) * pacing.Stagger)
			}
			target := len(registrantsByTopic[topicIdx]) - 1 // exclude self
			if target < 0 {
				target = 0
			}
			results[slot] = runOneSearcher(n, topicIdx, topics[topicIdx], deadline,
				pacing, has, target, int64(slot)*2654435761, stats, deadTracker)
		}(i, n, nodeTopic[i])
	}
	wg.Wait()
	close(checkpointStop)
	<-checkpointDone
	return results
}

// runOneSearcher executes a single node's TopicSearch until the absolute
// deadline (or early-termination), and returns its result. Membership is
// queried via has() so the caller can back it with either a static map
// (multi-topic workload) or a concurrent registry (steady-state churn, where
// registrants join and leave while searches run). The deadline is absolute so
// that nodes which join late stop at the same wall-clock instant as everyone
// else rather than each running for a full timeout past their start.
func runOneSearcher(n nodeRec, topicIdx int, topic topicindex.TopicID, deadlineAt time.Time,
	pacing searchPacing, has func(enode.ID, int) bool, target int, rngSeed int64,
	stats *liveStats, deadTracker *deadResultTracker) searchResult {

	iter := n.disc.TopicSearch(topic, uint64(n.idx))
	deadline := time.After(time.Until(deadlineAt))
	// Best-effort close. The discv5 topicSearch shutdown path
	// (search.stop -> wg.Wait) can hang at high node counts when internal
	// runLoop/runRequests goroutines are stuck on a saturated UDP socket and
	// never observe quit. Calling Close in a detached goroutine lets the
	// searcher's outer goroutine return so the workload can complete; the
	// leaked shutdown goroutine is reaped by main()'s teardown watchdog.
	closeIter := func() { go iter.Close() }
	defer closeIter()

	// Decouple iter.Next() from the main loop via a pump goroutine: iter.Close()
	// does not reliably unblock a parked iter.Next() at scale, so reading via a
	// channel lets us bail when the deadline fires even if iter.Next() is stuck.
	nodeCh := make(chan *enode.Node, 1)
	go func() {
		defer close(nodeCh)
		for iter.Next() {
			select {
			case nodeCh <- iter.Node():
			case <-deadline:
				return
			}
		}
	}()

	rng := rand.New(rand.NewSource(rngSeed)) // per-searcher, so pacing pauses don't align
	start := time.Now()
	var (
		found       []enode.ID
		timeFirst   time.Duration
		registered  int
		extra       int
		seenReg     = make(map[enode.ID]struct{})
		uniqueAtMs  []int64
		hitDeadline bool
		dl          deadLocal
	)
	selfID := n.ln.ID()
loop:
	for {
		var nd *enode.Node
		var ok bool
		select {
		case <-deadline:
			hitDeadline = true
			closeIter()
			break loop
		case nd, ok = <-nodeCh:
			if !ok {
				break loop
			}
		}
		if timeFirst == 0 {
			timeFirst = time.Since(start)
		}
		id := nd.ID()
		if id == selfID {
			continue
		}
		found = append(found, id)
		if has(id, topicIdx) {
			if _, dup := seenReg[id]; !dup {
				seenReg[id] = struct{}{}
				uniqueAtMs = append(uniqueAtMs, time.Since(start).Milliseconds())
				stats.recordUniqueFind(topicIdx, id)
				// Record whether this registrant was already dead when first
				// returned to this searcher, and how stale it was.
				if deadTracker != nil {
					dl.record(deadTracker, id, time.Now())
				}
			}
			registered++
		} else {
			extra++
		}
		if pacing.TargetCount > 0 && len(seenReg) >= pacing.TargetCount {
			closeIter()
			break loop
		}
		if pacing.MaxPause > 0 {
			select {
			case <-deadline:
				hitDeadline = true
				closeIter()
				break loop
			case <-time.After(time.Duration(rng.Int63n(int64(pacing.MaxPause)))):
			}
		}
	}
	elapsed := time.Since(start)
	ids := make([]string, 0, len(found))
	for _, id := range found {
		ids = append(ids, id.TerminalString())
	}
	foundRegIDs := make([]string, 0, len(seenReg))
	for rid := range seenReg {
		foundRegIDs = append(foundRegIDs, rid.TerminalString())
	}
	if deadTracker != nil {
		deadTracker.merge(dl.total, dl.dead, dl.ages)
	}
	return searchResult{
		NodeIdx:            n.idx,
		NodeID:             n.ln.ID().TerminalString(),
		Topic:              topicIdx,
		Target:             target,
		Found:              len(found),
		FoundRegistrant:    registered,
		UniqueRegistrant:   len(seenReg),
		FoundExtra:         extra,
		TimeToFirst:        timeFirst,
		TimeToCompletion:   elapsed,
		FoundIDs:           ids,
		FoundRegistrantIDs: foundRegIDs,
		UniqueFoundAtMs:    uniqueAtMs,
		HitTimeoutBefore:   hitDeadline,
	}
}

// searchManager runs searchers that can be launched dynamically while the
// search phase is in progress, so that nodes joining mid-run under steady-state
// churn participate as searchers too. All searchers share one absolute deadline
// and write their results into a mutex-guarded slice as they finish.
type searchManager struct {
	deadline    time.Time
	pacing      searchPacing
	topics      []topicindex.TopicID
	mem         *topicMembership
	stats       *liveStats
	deadTracker *deadResultTracker

	wg      sync.WaitGroup
	mu      sync.Mutex
	results []searchResult
	seqCtr  int64 // distinct rng seeds across dynamically-launched searchers
}

// launch starts a searcher for n on its assigned topic. Legacy nodes (topicIdx
// < 0) are ignored. Safe to call concurrently and at any time before wait().
func (sm *searchManager) launch(n nodeRec, topicIdx int) {
	if topicIdx < 0 {
		return
	}
	sm.wg.Add(1)
	sm.mu.Lock()
	seed := sm.seqCtr * 2654435761
	sm.seqCtr++
	sm.mu.Unlock()
	go func() {
		defer sm.wg.Done()
		target := sm.mem.countTopic(topicIdx) - 1
		if target < 0 {
			target = 0
		}
		r := runOneSearcher(n, topicIdx, sm.topics[topicIdx], sm.deadline,
			sm.pacing, sm.mem.has, target, seed, sm.stats, sm.deadTracker)
		sm.mu.Lock()
		sm.results = append(sm.results, r)
		sm.mu.Unlock()
	}()
}

// wait blocks until all launched searchers finish or until hardLimit elapses,
// whichever comes first, then returns the results collected so far. The
// hard limit guards against the discv5 search-shutdown path wedging a searcher
// goroutine (observed when a node is killed mid-search): the run still produces
// its report from partial results instead of hanging indefinitely.
func (sm *searchManager) wait(hardLimit time.Duration) []searchResult {
	done := make(chan struct{})
	go func() { sm.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(hardLimit):
		fmt.Printf("search wait exceeded %s past deadline; reporting partial results\n", hardLimit)
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return append([]searchResult(nil), sm.results...)
}

// liveStats accumulates per-topic, per-registrant find counts while searches
// are running. Used by the checkpoint goroutine to print progress mid-run.
//
// Concurrency: a single mutex per topic protects the per-registrant counter
// map. Searcher goroutines call recordUniqueFind once per distinct registrant
// they discover; the checkpoint goroutine snapshots under the same lock.
type liveStats struct {
	mus      []sync.Mutex
	counts   []map[enode.ID]int // [topic] -> regID -> #searchers that found it
	assigned []int              // [topic] -> total assigned registrants
	start    time.Time
}

func newLiveStats(numTopics int, registrantsByTopic map[int]map[enode.ID]struct{}) *liveStats {
	ls := &liveStats{
		mus:      make([]sync.Mutex, numTopics),
		counts:   make([]map[enode.ID]int, numTopics),
		assigned: make([]int, numTopics),
		start:    time.Now(),
	}
	for i := 0; i < numTopics; i++ {
		ls.counts[i] = make(map[enode.ID]int)
		ls.assigned[i] = len(registrantsByTopic[i])
	}
	return ls
}

// addAssigned increments the assigned-registrant denominator for a topic, used
// when a node joins mid-run under steady-state churn and registers.
func (ls *liveStats) addAssigned(topicIdx int) {
	ls.mus[topicIdx].Lock()
	ls.assigned[topicIdx]++
	ls.mus[topicIdx].Unlock()
}

func (ls *liveStats) recordUniqueFind(topicIdx int, regID enode.ID) {
	ls.mus[topicIdx].Lock()
	ls.counts[topicIdx][regID]++
	ls.mus[topicIdx].Unlock()
}

func (ls *liveStats) runCheckpoints(interval time.Duration, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			ls.printCheckpoint("final")
			return
		case <-tick.C:
			ls.printCheckpoint("checkpoint")
		}
	}
}

func (ls *liveStats) printCheckpoint(tag string) {
	elapsed := time.Since(ls.start)
	for t := range ls.counts {
		ls.mus[t].Lock()
		assigned := ls.assigned[t]
		founders := len(ls.counts[t]) // registrants found by ≥1 searcher
		dist := make([]int, 0, assigned)
		for _, c := range ls.counts[t] {
			dist = append(dist, c)
		}
		ls.mus[t].Unlock()
		neverFound := assigned - founders
		// Pad with zeros for never-found registrants so percentiles
		// reflect the full assigned population, not just the seen ones.
		for i := 0; i < neverFound; i++ {
			dist = append(dist, 0)
		}
		sort.Ints(dist)
		var p50, p95, maxC int
		if n := len(dist); n > 0 {
			p50 = dist[n*50/100]
			p95 = dist[n*95/100]
			if p95 >= n {
				p95 = dist[n-1]
			}
			maxC = dist[n-1]
		}
		var coveragePct float64
		if assigned > 0 {
			coveragePct = float64(founders) * 100 / float64(assigned)
		}
		fmt.Printf("[%s t=%ds] topic %d: registrants=%d coveredBy>=1=%d (%.1f%%) neverFound=%d p50=%d p95=%d max=%d\n",
			tag, int(elapsed.Seconds()), t, assigned, founders, coveragePct, neverFound, p50, p95, maxC)
	}
}
