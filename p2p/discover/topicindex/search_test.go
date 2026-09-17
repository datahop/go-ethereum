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

func adaptiveSearch(t *testing.T) *Search {
	config := testConfig(t)
	config.SearchYieldFloor = 4
	s := NewSearch(topic1, config)
	// Two candidates in every bucket the tests move through.
	for bi := 0; bi <= 12; bi++ {
		s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 256-bi, 2, 1+10*bi))
	}
	return s
}

// reply answers a query from bucket bi with n ads and returns the node asked.
func reply(t *testing.T, s *Search, bi int, n int) *enode.Node {
	t.Helper()
	target := s.QueryTarget()
	if target == nil {
		t.Fatalf("no target while bucket[%d] is expected", bi)
	}
	if got := s.bucketIndex(target.ID()); got != bi {
		t.Fatalf("query to bucket[%d], want bucket[%d]", got, bi)
	}
	s.AddQueryResults(target, nodesAtDistanceFrom(enode.ID(topic1), 100, n, 200))
	return target
}

// TestSearchAdaptiveAdvance checks that an adaptive search moves toward the
// topic by the number of density doublings needed to meet the floor: one ad
// per reply at floor 4 is two buckets, and empty replies are the maximum jump.
func TestSearchAdaptiveAdvance(t *testing.T) {
	s := adaptiveSearch(t)
	if s.ActiveBucket() != 0 {
		t.Fatalf("active bucket %d at start, want 0", s.ActiveBucket())
	}
	reply(t, s, 0, 1)
	if s.ActiveBucket() != 0 {
		t.Fatal("moved on a single sample")
	}
	reply(t, s, 0, 1)
	if s.ActiveBucket() != 2 {
		t.Fatalf("active bucket %d after two replies of 1 ad, want 2", s.ActiveBucket())
	}
	reply(t, s, 2, 0)
	reply(t, s, 2, 0)
	if s.ActiveBucket() != 2+searchMaxJump {
		t.Fatalf("active bucket %d after empty replies, want %d", s.ActiveBucket(), 2+searchMaxJump)
	}
}

// TestSearchAdaptiveRetreat checks that full replies move the search one
// bucket farther from the topic, that replies at the floor keep it in place,
// and that a single full reply among short ones does not move it back.
func TestSearchAdaptiveRetreat(t *testing.T) {
	s := adaptiveSearch(t)
	s.SetActiveBucket(6)
	reply(t, s, 6, TopicNodesLimit)
	reply(t, s, 6, TopicNodesLimit)
	if s.ActiveBucket() != 5 {
		t.Fatalf("active bucket %d after full replies, want 5", s.ActiveBucket())
	}
	reply(t, s, 5, 5)
	reply(t, s, 5, 6)
	if s.ActiveBucket() != 5 {
		t.Fatalf("active bucket %d after replies at the floor, want 5", s.ActiveBucket())
	}
	s.SetActiveBucket(8)
	reply(t, s, 8, TopicNodesLimit)
	reply(t, s, 8, 2)
	if s.ActiveBucket() != 8 {
		t.Fatalf("active bucket %d after one full and one short reply, want 8", s.ActiveBucket())
	}
}

// TestSearchAdaptiveLiar checks that one reply cannot steer the search on its
// own: a full reply among short ones delays the move by one sample but does
// not hold the search, and an empty reply among replies at the floor does
// not advance it.
func TestSearchAdaptiveLiar(t *testing.T) {
	s := adaptiveSearch(t)
	s.SetActiveBucket(4)
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 252, 2, 100))
	reply(t, s, 4, TopicNodesLimit)
	reply(t, s, 4, 1)
	if s.ActiveBucket() != 4 {
		t.Fatalf("active bucket %d after (full, 1), want 4", s.ActiveBucket())
	}
	reply(t, s, 4, 1)
	if s.ActiveBucket() != 6 {
		t.Fatalf("active bucket %d after (full, 1, 1), want 6", s.ActiveBucket())
	}

	s.SetActiveBucket(2)
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 254, 2, 100))
	for _, n := range []int{0, 4, 4, 4} {
		reply(t, s, 2, n)
		if s.ActiveBucket() != 2 {
			t.Fatalf("active bucket %d, want 2: one empty reply must not advance the search", s.ActiveBucket())
		}
	}
}

// TestSearchAdaptiveHelper checks that a search whose active bucket has no
// candidates asks the nearest populated bucket, farther side first, and
// requests aux nodes only around the active distance.
func TestSearchAdaptiveHelper(t *testing.T) {
	config := testConfig(t)
	config.SearchYieldFloor = 4
	s := NewSearch(topic1, config)
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 254, 1, 1)) // bucket 2
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 249, 1, 2)) // bucket 7
	s.SetActiveBucket(5)

	// Aux nodes are requested around the active bucket first, then at every
	// other distance with free space.
	dists := s.BucketsWithFreeSpace(nil)
	if len(dists) != searchTableDepth || dists[0] != 252 || dists[1] != 251 || dists[2] != 250 || dists[3] != 256 {
		t.Fatalf("aux distances %v, want [252 251 250 256 ...]", dists)
	}
	n := s.QueryTarget()
	if n == nil || s.bucketIndex(n.ID()) != 7 {
		t.Fatalf("helper query went to %v, want the nearest populated bucket (7)", n)
	}
	s.AddQueryResults(n, nil)
	n = s.QueryTarget()
	if n == nil || s.bucketIndex(n.ID()) != 2 {
		t.Fatalf("second helper query went to %v, want bucket 2", n)
	}
}

// TestSearchAdaptiveIsDone checks that an adaptive pass ends once the bucket
// the search settled in is exhausted, even though other buckets still hold
// unasked nodes, and not while results are buffered.
func TestSearchAdaptiveIsDone(t *testing.T) {
	s := adaptiveSearch(t)
	s.SetActiveBucket(3)
	if s.IsDone() {
		t.Fatal("done before the active bucket was asked")
	}
	reply(t, s, 3, 4)
	reply(t, s, 3, 4)
	if s.IsDone() {
		t.Fatal("done while results are buffered")
	}
	for s.PeekResult() != nil {
		s.PopResult()
	}
	if !s.IsDone() {
		t.Fatal("not done with the active bucket exhausted and no results buffered")
	}
	if s.ActiveBucket() != 3 {
		t.Fatalf("active bucket %d, want 3 for the next pass", s.ActiveBucket())
	}

	// A bucket with a single candidate cannot decide on its own: the pass
	// goes on through the nearest populated bucket instead of ending.
	s = NewSearch(topic1, adaptiveSearch(t).cfg)
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(s.topic), 251, 1, 1))  // bucket 5
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(s.topic), 249, 2, 10)) // bucket 7
	s.SetActiveBucket(5)
	reply(t, s, 5, 0)
	if s.IsDone() {
		t.Fatal("done after one reply from a single-node bucket")
	}
	if n := s.QueryTarget(); n == nil || s.bucketIndex(n.ID()) != 7 {
		t.Fatalf("query went to %v, want the nearest populated bucket (7)", n)
	}
}

// TestSearchAdaptiveExplores checks that an adaptive search walks its bucket:
// an asked node leaves the table so its slot can take an aux node at that
// distance, and it is not re-admitted, in this pass or the next one sharing
// the asked filter.
func TestSearchAdaptiveExplores(t *testing.T) {
	config := testConfig(t)
	config.SearchYieldFloor = 4
	config.SearchBucketSize = 2
	s := NewSearch(topic1, config)
	nodes := nodesAtDistanceFrom(enode.ID(topic1), 256, 2, 1)
	s.AddNodes(nil, nodes)
	if dists := s.BucketsWithFreeSpace(nil); len(dists) > 0 && dists[0] == 256 {
		t.Fatal("full active bucket listed as having free space")
	}
	asked := reply(t, s, 0, 1)
	if s.buckets[0].contains(asked.ID()) {
		t.Fatal("asked node still in the table")
	}
	if dists := s.BucketsWithFreeSpace(nil); len(dists) == 0 || dists[0] != 256 {
		t.Fatalf("aux distances %v, want the active distance first", dists)
	}
	s.AddNodes(nodes[1], []*enode.Node{asked})
	if s.buckets[0].contains(asked.ID()) {
		t.Fatal("asked node re-admitted within the pass")
	}
	fresh := nodesAtDistanceFrom(enode.ID(topic1), 256, 1, 50)
	s.AddNodes(nodes[1], fresh)
	if !s.buckets[0].contains(fresh[0].ID()) {
		t.Fatal("aux node not admitted into the freed slot")
	}

	next := NewSearch(topic1, config)
	next.SetAskedFilter(s.asked)
	next.AddNodes(nil, nodes)
	if next.buckets[0].contains(asked.ID()) {
		t.Fatal("asked node re-admitted by the next pass")
	}
	if !next.buckets[0].contains(nodes[1].ID()) {
		t.Fatal("unasked node not admitted by the next pass")
	}
}

// TestSearchAdaptiveWalkedDry checks that a search with nothing left to ask
// moves one bucket closer for its next pass, and stops at the closest one.
func TestSearchAdaptiveWalkedDry(t *testing.T) {
	config := testConfig(t)
	config.SearchYieldFloor = 4
	s := NewSearch(topic1, config)
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 256, 1, 1))
	reply(t, s, 0, 0)
	if !s.IsDone() {
		t.Fatal("not done with the only candidate asked")
	}
	if s.ActiveBucket() != 1 {
		t.Fatalf("active bucket %d after walking bucket 0 dry, want 1", s.ActiveBucket())
	}
	s.SetActiveBucket(len(s.buckets) - 1)
	if !s.IsDone() || s.ActiveBucket() != len(s.buckets)-1 {
		t.Fatalf("active bucket %d, want to stay at the closest bucket", s.ActiveBucket())
	}
}

// TestSearchAdaptiveSpare checks that a pass with every known node already
// asked re-asks one of them, once, to fetch aux nodes, and walks on from any
// unasked node those bring.
func TestSearchAdaptiveSpare(t *testing.T) {
	config := testConfig(t)
	config.SearchYieldFloor = 4
	s := NewSearch(topic1, config)
	seeds := nodesAtDistanceFrom(enode.ID(topic1), 256, 2, 1)
	s.AddNodes(nil, seeds)
	reply(t, s, 0, 0)
	reply(t, s, 0, 0)

	next := NewSearch(topic1, config)
	next.SetAskedFilter(s.asked)
	next.AddNodes(nil, seeds)
	if next.IsDone() {
		t.Fatal("done with spare nodes still to ask")
	}
	helper := next.QueryTarget()
	if helper == nil {
		t.Fatal("no spare node offered")
	}
	if n := next.QueryTarget(); n != nil {
		t.Fatalf("second spare %v offered in the same pass", n.ID())
	}
	fresh := nodesAtDistanceFrom(enode.ID(topic1), 256, 1, 50)
	next.AddNodes(helper, fresh)
	next.AddQueryResults(helper, nil)
	if n := next.QueryTarget(); n == nil || n.ID() != fresh[0].ID() {
		t.Fatalf("walk did not resume from the aux node, got %v", n)
	}
}
