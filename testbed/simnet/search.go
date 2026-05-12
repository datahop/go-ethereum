package main

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

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
