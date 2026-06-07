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

	for i, n := range all {
		go func(slot int, n nodeRec, topicIdx int) {
			defer wg.Done()

			// Legacy (non-DISC-NG) nodes don't search. They stay
			// passive in the routing-table substrate.
			if topicIdx < 0 {
				return
			}

			// Stagger the start of this searcher. Spreads concurrent
			// search activity across the run instead of having all
			// searchers hammer simultaneously.
			if pacing.Stagger > 0 {
				time.Sleep(time.Duration(slot) * pacing.Stagger)
			}

			topic := topics[topicIdx]
			regSet := registrantsByTopic[topicIdx]
			target := len(regSet) - 1 // exclude self
			if target < 0 {
				target = 0
			}

			iter := n.disc.TopicSearch(topic, uint64(n.idx))
			deadline := time.After(timeout)
			// Best-effort close. The discv5 topicSearch shutdown path
			// (search.stop -> wg.Wait) can hang at high node counts when
			// internal runLoop/runRequests goroutines are stuck on a
			// saturated UDP socket and never observe quit. Calling Close
			// in a detached goroutine lets the searcher's outer
			// wg.Done() fire so the workload can complete; the leaked
			// shutdown goroutine is reaped by main()'s teardown
			// watchdog.
			closeIter := func() { go iter.Close() }
			defer closeIter()

			// Decouple iter.Next() from the searcher's main loop via a
			// helper goroutine. Reason: iter.Close() does not reliably
			// unblock a parked iter.Next() at scale (the underlying
			// search machinery can hang inside its own shutdown path).
			// Reading via a channel lets us bail when the deadline
			// fires even if iter.Next() is stuck.
			//
			// The pump goroutine may leak if iter.Next() blocks forever
			// after the deadline; the watchdog in main() force-exits
			// after teardown so leaked goroutines do not delay process
			// exit.
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

			// Per-searcher RNG, so pacing pauses don't all align.
			rng := rand.New(rand.NewSource(int64(slot) * 2654435761))

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
				if _, ok := regSet[id]; ok {
					if _, dup := seenReg[id]; !dup {
						seenReg[id] = struct{}{}
						uniqueAtMs = append(uniqueAtMs, time.Since(start).Milliseconds())
						stats.recordUniqueFind(topicIdx, id)
						// Record whether this registrant was already dead when
						// first returned to this searcher, and how stale it was.
						if deadTracker != nil {
							dl.record(deadTracker, id, time.Now())
						}
					}
					registered++
				} else {
					extra++
				}
				// Early termination: enough unique registrants found.
				if pacing.TargetCount > 0 && len(seenReg) >= pacing.TargetCount {
					closeIter()
					break loop
				}
				// Per-iteration random pause to avoid running the
				// iterator at full polling speed.
				if pacing.MaxPause > 0 {
					select {
					case <-deadline:
						hitDeadline = true
						iter.Close()
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
			r := searchResult{
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
			results[slot] = r
			if deadTracker != nil {
				deadTracker.merge(dl.total, dl.dead, dl.ages)
			}
		}(i, n, nodeTopic[i])
	}
	wg.Wait()
	close(checkpointStop)
	<-checkpointDone
	return results
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
