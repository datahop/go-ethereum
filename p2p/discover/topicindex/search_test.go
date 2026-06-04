// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package topicindex

import (
	"testing"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

// func TestSearchLookups(t *testing.T) {
// 	config := testConfig(t)
// 	s := NewSearch(topic1, config)
//
// 	t.Log(s.LookupTarget())
// }

// This checks that search buckets are filled correctly
// with nodes at various distances.
func TestSearchBuckets(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	var (
		far256  = nodesAtDistanceFrom(enode.ID(topic1), 256, 3, 1)
		far255  = nodesAtDistanceFrom(enode.ID(topic1), 255, 3, 10)
		close5  = nodesAtDistanceFrom(enode.ID(topic1), 5, 1, 20)
		close20 = nodesAtDistanceFrom(enode.ID(topic1), 20, 1, 30)
	)
	s.AddNodes(nil, far256)
	s.AddNodes(nil, far255)
	s.AddNodes(nil, close5)
	s.AddNodes(nil, close20)

	last := len(s.buckets) - 1
	if !sbContainsAll(s.buckets[0], far256) {
		t.Fatal("far256 nodes missing in bucket[0]")
	}
	if !sbContainsAll(s.buckets[1], far255) {
		t.Fatal("far255 nodes missing in bucket[1]")
	}
	if !sbContainsAll(s.buckets[last], close5) {
		t.Fatalf("close5 nodes missing in bucket[%d]", last)
	}
	if !sbContainsAll(s.buckets[last], close20) {
		t.Fatalf("close20 nodes missing in bucket[%d]", last)
	}
}

func sbContainsAll(b searchBucket, nodes []*enode.Node) bool {
	for _, n := range nodes {
		if !b.contains(n.ID()) {
			return false
		}
	}
	return true
}

// TestSearchIsDoneNoUnaskedNodes verifies that IsDone returns true once every
// bucket's `new` set is empty and no results are buffered. Without this, the
// search state would deadlock: once `new` is empty across all buckets,
// QueryTarget returns nil, no further query is sent, no AddNodes call can
// occur, and any counter-based termination heuristic can never advance.
// See datahop/go-ethereum#27.
func TestSearchIsDoneNoUnaskedNodes(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	// A freshly created Search with no candidates is immediately done — the
	// caller should roll over to a new search rather than spin.
	if !s.IsDone() {
		t.Fatal("IsDone should return true on an empty Search (no unasked nodes, no buffered results)")
	}

	// Seed one bucket with unasked nodes. IsDone should now be false.
	nodes := nodesAtDistanceFrom(enode.ID(topic1), 255, 3, 1)
	s.AddNodes(nil, nodes)
	if s.IsDone() {
		t.Fatal("IsDone should return false while unasked nodes remain")
	}

	// Mark every unasked node as asked. With no buffered results and no
	// remaining `new` entries, IsDone must return true even though no query
	// produced any new nodes (queriesWithoutNewNodes-style counters can't
	// progress in this state).
	for i := range s.buckets {
		b := &s.buckets[i]
		for id, n := range b.new {
			b.setAsked(n)
			delete(b.new, id)
		}
	}
	if !s.IsDone() {
		t.Fatal("IsDone should return true once all buckets are exhausted")
	}
}

// TestSearchIsDoneBufferedResults verifies that IsDone keeps returning false
// while buffered results have not yet been consumed by the caller, even when
// no unasked nodes remain.
func TestSearchIsDoneBufferedResults(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	src := nodeAtDistance(enode.ID(topic1), 255, intIP(1))
	results := nodesAtDistanceFrom(enode.ID(topic1), 200, 2, 100)
	s.AddQueryResults(src, results)

	if s.IsDone() {
		t.Fatal("IsDone should return false while results are buffered")
	}
	for range results {
		s.PopResult()
	}
	if !s.IsDone() {
		t.Fatal("IsDone should return true after all results have been popped")
	}
}

// TestSearchQueryTargetSkipsEmptyFarBucket verifies that QueryTarget makes
// progress when the farthest bucket is empty: it must skip empty buckets and
// return a node from the next non-empty one. See datahop/go-ethereum#65.
func TestSearchQueryTargetSkipsEmptyFarBucket(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	// Populate bucket 5 (logdist 251) only. Buckets 0..4 remain empty.
	mid := nodesAtDistanceFrom(enode.ID(topic1), 251, 3, 1)
	s.AddNodes(nil, mid)

	target := s.QueryTarget()
	if target == nil {
		t.Fatal("QueryTarget returned nil while bucket[5] has unasked nodes")
	}
	if !s.buckets[5].contains(target.ID()) {
		t.Fatalf("QueryTarget returned node not in bucket[5]: %v", target.ID())
	}
}

// TestSearchQueryTargetPrefersFarthest verifies that QueryTarget drains the
// farthest non-empty bucket before advancing to closer ones. Nodes close to
// the topic are queried last, limiting load on the small set responsible
// for the topic.
func TestSearchQueryTargetPrefersFarthest(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	far := nodesAtDistanceFrom(enode.ID(topic1), 256, 3, 1)
	mid := nodesAtDistanceFrom(enode.ID(topic1), 251, 3, 10)
	s.AddNodes(nil, far)
	s.AddNodes(nil, mid)

	// Drain bucket 0 entirely; every pick must come from bucket 0 first.
	for i := 0; i < len(far); i++ {
		target := s.QueryTarget()
		if target == nil {
			t.Fatalf("QueryTarget returned nil with bucket[0] still populated (iteration %d)", i)
		}
		if !s.buckets[0].contains(target.ID()) {
			t.Fatalf("iteration %d: QueryTarget returned %v from a non-farthest bucket while bucket[0] has unasked nodes", i, target.ID())
		}
		s.buckets[0].setAsked(target)
	}
	// With bucket 0 drained, next pick must come from bucket 5.
	target := s.QueryTarget()
	if target == nil {
		t.Fatal("QueryTarget returned nil after draining bucket[0] while bucket[5] is populated")
	}
	if !s.buckets[5].contains(target.ID()) {
		t.Fatalf("QueryTarget returned %v after bucket[0] drained; expected a bucket[5] node", target.ID())
	}
}

// TestSearchQueryTargetWidensFrontierAfterResponse verifies that the
// warm-up frontier admits a closer bucket to the random pool once the
// current bucket has received at least one response. Before any response,
// only bucket 0 is in the pool; after bucket 0 is queried, both bucket 0
// (if refilled) and bucket 5 are eligible.
func TestSearchQueryTargetWidensFrontierAfterResponse(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	far := nodesAtDistanceFrom(enode.ID(topic1), 256, 1, 1)
	mid := nodesAtDistanceFrom(enode.ID(topic1), 251, 1, 10)
	s.AddNodes(nil, far)
	s.AddNodes(nil, mid)

	// Simulate a response on bucket 0: setAsked + numRequests++.
	// This widens the frontier so bucket 5 becomes eligible.
	s.AddQueryResults(far[0], nil)

	// Refill bucket 0 so both bucket 0 and bucket 5 hold an unasked node.
	refill := nodesAtDistanceFrom(enode.ID(topic1), 256, 1, 100)
	s.AddNodes(nil, refill)

	// Both buckets must now appear in QueryTarget picks across many calls.
	seen := map[int]int{}
	for i := 0; i < 200; i++ {
		target := s.QueryTarget()
		if target == nil {
			t.Fatalf("QueryTarget returned nil at iter %d", i)
		}
		bi := s.bucketIndex(target.ID())
		seen[bi]++
	}
	if seen[0] == 0 || seen[5] == 0 {
		t.Fatalf("expected picks from both bucket[0] and bucket[5] after warm-up; got %v", seen)
	}
}

// TestSearchQueryTargetRestartsAfterRefill verifies that newly-added far
// nodes are picked up before further queries continue draining closer
// buckets. Each QueryTarget call re-scans from the farthest bucket, so an
// AddNodes that lands in bucket 0 takes priority over remaining bucket-5
// candidates.
func TestSearchQueryTargetRestartsAfterRefill(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	// Start with bucket 5 only.
	mid := nodesAtDistanceFrom(enode.ID(topic1), 251, 2, 1)
	s.AddNodes(nil, mid)

	first := s.QueryTarget()
	if first == nil || !s.buckets[5].contains(first.ID()) {
		t.Fatalf("expected first pick from bucket[5], got %v", first)
	}
	s.buckets[5].setAsked(first)

	// Now bucket 0 receives a node. QueryTarget must prefer it over the
	// remaining bucket-5 candidate.
	far := nodesAtDistanceFrom(enode.ID(topic1), 256, 1, 10)
	s.AddNodes(nil, far)

	target := s.QueryTarget()
	if target == nil {
		t.Fatal("QueryTarget returned nil with refilled bucket[0]")
	}
	if !s.buckets[0].contains(target.ID()) {
		t.Fatalf("expected pick from refilled bucket[0], got %v", target.ID())
	}
}

// TestSearchAddNodesOnePerBucketRule verifies that within a single AddNodes
// call from a non-nil src, at most one node is admitted to any given search
// bucket. Regression test for an earlier bug where bucketCheck was read but
// never written, so the rule never triggered.
func TestSearchAddNodesOnePerBucketRule(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	src := enode.SignNull(new(enr.Record), enode.ID{})
	// Two nodes from the same source at the same distance from the topic
	// hash to the same search bucket. With the rule enforced, only one is
	// kept.
	sameBucket := nodesAtDistanceFrom(enode.ID(topic1), 250, 2, 1)
	s.AddNodes(src, sameBucket)

	bi := s.bucketIndex(sameBucket[0].ID())
	if got := s.buckets[bi].count(); got != 1 {
		t.Fatalf("expected 1 node in bucket[%d] under one-per-bucket-rule, got %d", bi, got)
	}
}

// This checks (de)queueing of topic search results.
func TestSearchResultsTracking(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	var (
		src   = enode.SignNull(new(enr.Record), enode.ID{})
		nodes = nodesAtDistance(src.ID(), 256, 10)
	)
	s.AddQueryResults(src, nodes)

	for i, n := range nodes {
		result := s.PeekResult()
		if result.ID() != n.ID() {
			t.Logf("wrong result %d: got %v, want %v", i, result.ID(), n.ID())
		}
		s.PopResult()
	}
}
