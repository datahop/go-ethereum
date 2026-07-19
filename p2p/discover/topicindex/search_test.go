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
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

// TestSearchBucketDistanceClamp verifies that nodes closer to the topic than
// the table depth are clamped into the last (closest) bucket instead of being
// dropped.
func TestSearchBucketDistanceClamp(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	var (
		close5  = nodesAtDistanceFrom(enode.ID(topic1), 5, 1, 20)
		close20 = nodesAtDistanceFrom(enode.ID(topic1), 20, 1, 30)
	)
	s.AddNodes(nil, close5)
	s.AddNodes(nil, close20)

	last := len(s.buckets) - 1
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

// TestSearchIsDone walks IsDone through the search lifecycle: done on a
// fresh empty search (the caller should roll over to a new search rather
// than spin), not done while unasked nodes remain, not done while buffered
// results await consumption, and done again once both are exhausted.
func TestSearchIsDone(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	// A freshly created Search with no candidates is immediately done.
	if !s.IsDone() {
		t.Fatal("IsDone should return true on an empty Search (no unasked nodes, no buffered results)")
	}

	// Unasked nodes keep the search running.
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 255, 2, 1))
	if s.IsDone() {
		t.Fatal("IsDone should return false while unasked nodes remain")
	}

	// Ask every node. The responses carry results, which keep the search
	// alive even though no unasked node is left.
	results := nodesAtDistanceFrom(enode.ID(topic1), 200, 2, 100)
	for n := s.QueryTarget(); n != nil; n = s.QueryTarget() {
		s.AddQueryResults(n, results)
	}
	if s.IsDone() {
		t.Fatal("IsDone should return false while results are buffered")
	}

	// Consuming the buffered results finishes the search.
	for s.PeekResult() != nil {
		s.PopResult()
	}
	if !s.IsDone() {
		t.Fatal("IsDone should return true once all nodes are asked and all results consumed")
	}
}

// TestSearchQueryTarget checks how QueryTarget selects nodes: picks come
// from the farthest bucket holding unasked candidates (empty buckets are
// skipped), closer buckets become eligible only after that bucket has
// received a response, and QueryTarget returns nil exactly when every node
// has been asked.
func TestSearchQueryTarget(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	// An empty table has no target.
	if n := s.QueryTarget(); n != nil {
		t.Fatalf("QueryTarget on empty table returned %v, want nil", n.ID())
	}

	// Populate buckets 3 (logdist 253) and 7 (logdist 249) with two nodes
	// each. Buckets 0-2 and 4-6 stay empty.
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 253, 2, 1))
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 249, 2, 10))

	// Before any response, every pick must come from bucket 3: it is the
	// farthest bucket with candidates (the empty buckets before it are
	// skipped), and while it has no response yet it gates the walk,
	// keeping bucket 7 out of the pool.
	for i := 0; i < 20; i++ {
		n := s.QueryTarget()
		if n == nil {
			t.Fatal("QueryTarget returned nil on a populated table")
		}
		if bi := s.bucketIndex(n.ID()); bi != 3 {
			t.Fatalf("pick from bucket[%d] before any response, want bucket[3]", bi)
		}
	}

	// Drain the table, responding to every query. The first response warms
	// bucket 3, letting bucket 7 join the pool. Every node must be picked
	// exactly once, and QueryTarget must return nil only at exhaustion.
	asked := make(map[enode.ID]bool)
	for {
		n := s.QueryTarget()
		if n == nil {
			break
		}
		if asked[n.ID()] {
			t.Fatalf("node %v picked twice", n.ID())
		}
		asked[n.ID()] = true
		s.AddQueryResults(n, nil)
	}
	if len(asked) != 4 {
		t.Fatalf("%d nodes asked at exhaustion, want all 4", len(asked))
	}
	if !s.IsDone() {
		t.Fatal("IsDone should report true once every node has been asked")
	}
}

// TestSearchAddNodesOnePerBucketRule verifies that within a single AddNodes
// call from a non-nil src, at most one node is admitted to any given search
// bucket.
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

// TestSearchHandleErrorResponse checks that a failed topic query drops the
// queried node from the table and frees its bucket slot and IP-limit entry.
func TestSearchHandleErrorResponse(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	// Two nodes in bucket 0 (logdist 256), one in bucket 5 (logdist 251).
	far := nodesAtDistanceFrom(enode.ID(topic1), 256, 2, 1)
	mid := nodesAtDistanceFrom(enode.ID(topic1), 251, 1, 10)
	s.AddNodes(nil, far)
	s.AddNodes(nil, mid)

	// The query to far[0] fails: the node must leave the table entirely.
	s.HandleErrorResponse(far[0], errors.New("timeout"))
	if s.buckets[0].contains(far[0].ID()) {
		t.Fatal("failed node still present in its bucket")
	}
	if got := s.buckets[0].count(); got != 1 {
		t.Fatalf("bucket[0] count is %d after eviction, want 1", got)
	}

	// The IP-limit slot is freed: a replacement in the same /24 is accepted.
	replacement := nodeAtDistance(enode.ID(topic1), 256, far[0].IP())
	s.AddNodes(nil, []*enode.Node{replacement})
	if !s.buckets[0].contains(replacement.ID()) {
		t.Fatal("replacement with the failed node's IP was not admitted")
	}

	// The failure did not count as a response: bucket 0 still has candidates
	// and no response yet, so it keeps gating the walk.
	if n := s.QueryTarget(); n == nil || !s.buckets[0].contains(n.ID()) {
		t.Fatalf("QueryTarget should keep picking from unwarmed bucket[0], got %v", n)
	}

	// Failing all of bucket 0 empties it; the empty bucket no longer blocks
	// the walk, so QueryTarget advances to bucket 5.
	s.HandleErrorResponse(far[1], errors.New("timeout"))
	s.HandleErrorResponse(replacement, errors.New("timeout"))
	target := s.QueryTarget()
	if target == nil {
		t.Fatal("QueryTarget returned nil after bucket[0] failed out; want bucket[5] node")
	}
	if !s.buckets[5].contains(target.ID()) {
		t.Fatalf("QueryTarget returned %v, want the bucket[5] node", target.ID())
	}

	// When every node has failed, the search is done and rolls over.
	s.HandleErrorResponse(mid[0], errors.New("timeout"))
	if !s.IsDone() {
		t.Fatal("IsDone should report true once every node has failed out")
	}
}

// TestSearchRemoveAskedNodeFreesIP verifies that removing a node that has moved
// to the 'asked' set (it responded before being removed) still releases its
// IP-limit entry, so a same-/24 replacement is admitted.
func TestSearchRemoveAskedNodeFreesIP(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	nodes := nodesAtDistanceFrom(enode.ID(topic1), 256, 1, 1)
	n := nodes[0]
	s.AddNodes(nil, nodes)

	// Move n into the 'asked' set by recording a query response from it.
	s.AddQueryResults(n, nil)
	if _, inAsked := s.buckets[0].asked[n.ID()]; !inAsked {
		t.Fatal("node was not moved to the asked set")
	}

	// Removing it now must free the IP-limit slot even though it is in 'asked'.
	s.HandleErrorResponse(n, errors.New("timeout"))
	if s.buckets[0].contains(n.ID()) {
		t.Fatal("removed asked-node still present")
	}
	replacement := nodeAtDistance(enode.ID(topic1), 256, n.IP())
	s.AddNodes(nil, []*enode.Node{replacement})
	if !s.buckets[0].contains(replacement.ID()) {
		t.Fatal("replacement with the asked node's IP was not admitted (IP slot leaked)")
	}
}

// This checks (de)queueing of topic search results: results come out of the
// buffer in the order they were received.
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
			t.Fatalf("wrong result %d: got %v, want %v", i, result.ID(), n.ID())
		}
		s.PopResult()
	}
}

// TestSearchBucketsWithFreeSpace verifies that BucketsWithFreeSpace reports
// the topic distance of every bucket with room left, that a full bucket
// drops out of the list, and that asked nodes keep occupying their slot.
func TestSearchBucketsWithFreeSpace(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	// On a fresh table, every bucket has free space, covering the full
	// distance range 256 .. 256-searchTableDepth+1.
	dists := s.BucketsWithFreeSpace(nil)
	if len(dists) != searchTableDepth {
		t.Fatalf("fresh table reports %d buckets with free space, want %d", len(dists), searchTableDepth)
	}
	seen := make(map[uint]bool, len(dists))
	for _, d := range dists {
		seen[d] = true
	}
	for d := uint(256); d > uint(256-searchTableDepth); d-- {
		if !seen[d] {
			t.Fatalf("distance %d missing from free-space list %v", d, dists)
		}
	}

	// Fill bucket 0 (distance 256) to capacity: it must drop out of the
	// list while all other buckets remain.
	full := nodesAtDistanceFrom(enode.ID(topic1), 256, s.cfg.SearchBucketSize, 1)
	s.AddNodes(nil, full)
	if got := s.buckets[0].count(); got != s.cfg.SearchBucketSize {
		t.Fatalf("setup: bucket[0] holds %d nodes, want %d", got, s.cfg.SearchBucketSize)
	}
	dists = s.BucketsWithFreeSpace(nil)
	if len(dists) != searchTableDepth-1 {
		t.Fatalf("got %d buckets with free space, want %d", len(dists), searchTableDepth-1)
	}
	for _, d := range dists {
		if d == 256 {
			t.Fatal("full bucket[0] (distance 256) still reported as having free space")
		}
	}

	// Querying a node moves it from `new` to `asked`, but it keeps
	// occupying its slot: the bucket must remain full.
	s.AddQueryResults(full[0], nil)
	for _, d := range s.BucketsWithFreeSpace(nil) {
		if d == 256 {
			t.Fatal("bucket[0] reported free after a response; asked nodes must keep their slot")
		}
	}
}
