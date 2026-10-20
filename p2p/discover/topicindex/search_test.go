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
	"slices"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
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

// TestSearchQueryTarget checks that QueryTarget picks from the populated
// bucket nearest the active one, asks every node once, and returns nil once
// every node has been asked.
func TestSearchQueryTarget(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	// An empty table has no target.
	if n := s.QueryTarget(); n != nil {
		t.Fatalf("QueryTarget on empty table returned %v, want nil", n.ID())
	}

	// Populate buckets 3 (logdist 253) and 7 (logdist 249) with two nodes
	// each. The active bucket 0 is empty, so the first pick comes from the
	// nearest populated bucket, 3.
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 253, 2, 1))
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 249, 2, 10))
	if n := s.QueryTarget(); n == nil || s.bucketIndex(n.ID()) != 3 {
		t.Fatalf("first pick %v, want a bucket[3] node", n)
	}

	// Drain the table, responding to every query. Every node must be picked
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

	// The failure did not move the search: it keeps picking from the active
	// bucket 0 while that has candidates.
	if n := s.QueryTarget(); n == nil || !s.buckets[0].contains(n.ID()) {
		t.Fatalf("QueryTarget should keep picking from bucket[0], got %v", n)
	}

	// Failing all of bucket 0 empties it, so QueryTarget moves on to the
	// nearest populated bucket, 5.
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

// TestSearchRepliedNodeFreesIP verifies that a node that replied leaves the
// table and releases its IP-limit entry, so a same-/24 replacement is admitted.
func TestSearchRepliedNodeFreesIP(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)

	nodes := nodesAtDistanceFrom(enode.ID(topic1), 256, 1, 1)
	n := nodes[0]
	s.AddNodes(nil, nodes)
	s.AddQueryResults(n, nil)
	if s.buckets[0].contains(n.ID()) {
		t.Fatal("node still in the table after its reply")
	}
	replacement := nodeAtDistance(enode.ID(topic1), 256, n.IP())
	s.AddNodes(nil, []*enode.Node{replacement})
	if !s.buckets[0].contains(replacement.ID()) {
		t.Fatal("replacement with the replied node's IP was not admitted (IP slot leaked)")
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

// TestSearchBucketsWithFreeSpace checks that a query asks to refill only the
// buckets within searchAuxRadius of the active one.
func TestSearchBucketsWithFreeSpace(t *testing.T) {
	s := adaptiveSearch(t)
	s.SetActiveBucket(3)
	if dists, want := s.BucketsWithFreeSpace(nil), []uint{254, 253, 252}; !slices.Equal(dists, want) {
		t.Fatalf("active bucket 3 lists %v, want %v", dists, want)
	}
	s.SetActiveBucket(0)
	if dists, want := s.BucketsWithFreeSpace(nil), []uint{256, 255}; !slices.Equal(dists, want) {
		t.Fatalf("active bucket 0 lists %v, want %v", dists, want)
	}
}

// IsDone never moves the active bucket, however often it is asked; a pass
// that ran dry starts the next one a bucket in, through NextPassBucket.
func TestSearchAdaptiveIsDoneOnce(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)
	s.SetActiveBucket(3)
	for i := 0; i < 3; i++ {
		if !s.IsDone() {
			t.Fatal("empty search not done")
		}
		if got := s.ActiveBucket(); got != 3 {
			t.Fatalf("IsDone moved the active bucket to %d", got)
		}
	}
	if got := s.NextPassBucket(); got != 4 {
		t.Fatalf("next pass at %d, want 4: a pass that ran dry starts one bucket in", got)
	}
}

// The density sample of a reply is the number of ads it carried, not the
// number of results taken from it: a full reply moves the search farther out
// even when the registrar limit took only part of it.
func TestSearchAdaptiveReplySize(t *testing.T) {
	s := adaptiveSearch(t)
	s.SetActiveBucket(3)
	for i := 0; i < 2; i++ {
		n := s.QueryTarget()
		if n == nil || s.bucketIndex(n.ID()) != 3 {
			t.Fatalf("query %d did not go to bucket 3", i)
		}
		s.AddReply(n, nodesAtDistanceFrom(enode.ID(topic1), 100, 6, 300+10*i), TopicNodesLimit)
	}
	if got := s.ActiveBucket(); got != 2 {
		t.Fatalf("active bucket %d after two full replies, want 2", got)
	}
}

func adaptiveSearch(t *testing.T) *Search {
	config := testConfig(t)
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
// per reply at floor 8 is three buckets, and empty replies are the maximum jump.
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
	if s.ActiveBucket() != 3 {
		t.Fatalf("active bucket %d after two replies of 1 ad, want 3", s.ActiveBucket())
	}
	reply(t, s, 3, 0)
	reply(t, s, 3, 0)
	if s.ActiveBucket() != 3+searchMaxJump {
		t.Fatalf("active bucket %d after empty replies, want %d", s.ActiveBucket(), 3+searchMaxJump)
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
	reply(t, s, 5, 8)
	reply(t, s, 5, 9)
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
	if s.ActiveBucket() != 7 {
		t.Fatalf("active bucket %d after (full, 1, 1), want 7", s.ActiveBucket())
	}

	s.SetActiveBucket(2)
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 254, 2, 100))
	for _, n := range []int{0, 8, 8, 8} {
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
	s := NewSearch(topic1, config)
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 254, 1, 1)) // bucket 2
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 249, 1, 2)) // bucket 7
	s.SetActiveBucket(5)

	// Aux nodes are requested around the active bucket only.
	dists := s.BucketsWithFreeSpace(nil)
	if want := []uint{252, 251, 250}; !slices.Equal(dists, want) {
		t.Fatalf("aux distances %v, want %v", dists, want)
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
// unasked nodes, and not while results are buffered. No query goes out once
// the bucket is exhausted.
func TestSearchAdaptiveIsDone(t *testing.T) {
	s := adaptiveSearch(t)
	s.SetActiveBucket(3)
	if s.IsDone() {
		t.Fatal("done before the active bucket was asked")
	}
	reply(t, s, 3, 8)
	reply(t, s, 3, 8)
	if s.IsDone() {
		t.Fatal("done while results are buffered")
	}
	if n := s.QueryTarget(); n != nil {
		t.Fatalf("query to %v after the active bucket was exhausted; the pass would end with it in flight", n.ID())
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
	config.SearchBucketSize = 2
	s := NewSearch(topic1, config)
	nodes := nodesAtDistanceFrom(enode.ID(topic1), 256, 2, 1)
	s.AddNodes(nil, nodes)
	if dists := s.BucketsWithFreeSpace(nil); len(dists) > 0 && dists[0] == 256 {
		t.Fatal("full active bucket listed as having free space")
	}
	asked := reply(t, s, 0, 1)
	other := nodes[0]
	if asked.ID() == other.ID() {
		other = nodes[1]
	}
	if s.buckets[0].contains(asked.ID()) {
		t.Fatal("asked node still in the table")
	}
	if dists := s.BucketsWithFreeSpace(nil); len(dists) == 0 || dists[0] != 256 {
		t.Fatalf("aux distances %v, want the active distance first", dists)
	}
	s.AddNodes(other, []*enode.Node{asked})
	if s.buckets[0].contains(asked.ID()) {
		t.Fatal("asked node re-admitted within the pass")
	}
	fresh := nodesAtDistanceFrom(enode.ID(topic1), 256, 1, 50)
	s.AddNodes(other, fresh)
	if !s.buckets[0].contains(fresh[0].ID()) {
		t.Fatal("aux node not admitted into the freed slot")
	}

	next := NewSearch(topic1, config)
	next.SetAsked(s.asked)
	next.AddNodes(nil, nodes)
	if next.buckets[0].contains(asked.ID()) {
		t.Fatal("asked node re-admitted by the next pass")
	}
	if !next.buckets[0].contains(other.ID()) {
		t.Fatal("unasked node not admitted by the next pass")
	}
}

// TestSearchAdaptiveWalkedDry checks that a search with nothing left to ask
// moves one bucket closer for its next pass, and stops at the closest one.
func TestSearchAdaptiveWalkedDry(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 256, 1, 1))
	reply(t, s, 0, 0)
	if !s.IsDone() {
		t.Fatal("not done with the only candidate asked")
	}
	if s.NextPassBucket() != 1 {
		t.Fatalf("next pass at %d after walking bucket 0 dry, want 1", s.NextPassBucket())
	}
	// At the closest bucket with no ads seen, the next pass starts wide again.
	s.SetActiveBucket(len(s.buckets) - 1)
	if !s.IsDone() || s.NextPassBucket() != 0 {
		t.Fatalf("next pass at %d, want 0 from the closest bucket of an empty topic", s.NextPassBucket())
	}
}

// TestSearchAdaptiveSpare checks that a pass with every known node already
// asked re-asks up to searchSpareQueries of them to fetch aux nodes, and
// walks on from any unasked node those bring.
func TestSearchAdaptiveSpare(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)
	seeds := nodesAtDistanceFrom(enode.ID(topic1), 256, searchSpareQueries+1, 1)
	s.AddNodes(nil, seeds)
	for range seeds {
		reply(t, s, 0, 0)
	}

	next := NewSearch(topic1, config)
	next.SetAsked(s.asked)
	next.AddNodes(nil, seeds)
	if next.IsDone() {
		t.Fatal("done with spare nodes still to ask")
	}
	// Every seed is a spare: the pass re-asks searchSpareQueries of them.
	var helper *enode.Node
	for i := 0; i < searchSpareQueries; i++ {
		n := next.QueryTarget()
		if n == nil {
			t.Fatalf("spare %d not offered", i+1)
		}
		helper = n
	}
	if n := next.QueryTarget(); n != nil {
		t.Fatalf("spare %d offered, want at most %d per pass", searchSpareQueries+1, searchSpareQueries)
	}
	fresh := nodesAtDistanceFrom(enode.ID(topic1), 256, 1, 50)
	next.AddNodes(helper, fresh)
	next.AddQueryResults(helper, nil)
	if n := next.QueryTarget(); n == nil || n.ID() != fresh[0].ID() {
		t.Fatalf("walk did not resume from the aux node, got %v", n)
	}
}

// TestSearchResultNewerRecord checks that a node already returned in this
// pass is not returned again, with a newer record or an older one: a bumped
// record must not keep a node at the front of the results.
func TestSearchResultNewerRecord(t *testing.T) {
	s := NewSearch(topic1, testConfig(t))
	regs := nodesAtDistanceFrom(enode.ID(topic1), 256, 2, 1)
	regA, regB := regs[0], regs[1]
	id := enode.SignNull(new(enr.Record), enode.ID{7}).ID()
	old, updated := nodeWithSeq(id, intIP(1), 1), nodeWithSeq(id, intIP(1), 2)
	s.AddNodes(nil, regs)

	s.AddQueryResults(regA, []*enode.Node{old})
	s.PopResult()
	s.AddQueryResults(regB, []*enode.Node{updated})
	if s.PeekResult() != nil {
		t.Fatal("newer record of a returned node returned again in the same pass")
	}
	s.AddResults([]*enode.Node{old})
	if s.PeekResult() != nil {
		t.Fatal("older record returned again")
	}
}

// AddQueryResults adds a response whose ads are all taken as results.
func (s *Search) AddQueryResults(from *enode.Node, results []*enode.Node) {
	s.AddReply(from, results, len(results))
}

// TestSearchAdaptiveHelperKeepsBucket checks that replies from helpers outside
// the active neighbourhood do not move the search: a rare topic settled close
// to the hash is not dragged back out by empty replies from far seeds.
func TestSearchAdaptiveHelperKeepsBucket(t *testing.T) {
	s := NewSearch(topic1, testConfig(t))
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 255, 2, 1)) // bucket 1
	s.SetActiveBucket(8)
	reply(t, s, 1, 0)
	reply(t, s, 1, 0)
	if s.ActiveBucket() != 8 {
		t.Fatalf("active bucket %d after two helper replies, want 8", s.ActiveBucket())
	}
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 249, 2, 10)) // bucket 7
	reply(t, s, 7, 16)
	reply(t, s, 7, 16)
	if s.ActiveBucket() != 6 {
		t.Fatalf("active bucket %d after full replies from a neighbour, want 6", s.ActiveBucket())
	}
}

// TestSearchAdaptivePassBudget checks that a pass ends after searchPassQueries
// replies even while every reply refills the active bucket.
func TestSearchAdaptivePassBudget(t *testing.T) {
	s := NewSearch(topic1, testConfig(t))
	s.SetActiveBucket(3)
	s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 253, 2, 1))
	for i := 0; i < searchPassQueries; i++ {
		n := s.QueryTarget()
		if n == nil {
			t.Fatalf("no target at query %d", i)
		}
		s.AddReply(n, nil, searchYieldFloor)
		s.AddNodes(n, nodesAtDistanceFrom(enode.ID(topic1), 253, 1, 100+i))
	}
	if n := s.QueryTarget(); n != nil {
		t.Fatalf("query %v offered after the pass budget", n.ID())
	}
	if !s.IsDone() {
		t.Fatal("pass not done after its query budget")
	}
	if s.ActiveBucket() != 3 {
		t.Fatalf("active bucket %d, want 3 kept for the next pass", s.ActiveBucket())
	}
}

// TestSearchAdaptiveFailedAsked checks that a node whose query failed is left
// alone for the rest of the pass and asked again by the next one.
func TestSearchAdaptiveFailedAsked(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)
	nodes := nodesAtDistanceFrom(enode.ID(topic1), 256, 1, 1)
	s.AddNodes(nil, nodes)
	n := s.QueryTarget()
	s.HandleErrorResponse(n, errors.New("timeout"))
	s.AddNodes(nil, nodes)
	if s.buckets[0].contains(n.ID()) {
		t.Fatal("failed node re-admitted in the same pass")
	}

	// One dropped packet must not exclude a registrar for an ad lifetime:
	// the next pass asks it again.
	next := NewSearch(topic1, config)
	next.SetAsked(s.asked)
	next.AddNodes(nil, nodes)
	if !next.buckets[0].contains(n.ID()) {
		t.Fatal("failed node not re-admitted by the next pass")
	}
}

// TestSearchAdaptiveFarHelperPullsOut checks that a search that walked past
// the populated region comes back: full replies from a farther bucket pull it
// out to the bucket before them, and a farther bucket at the floor pulls it out
// to that bucket, while sparse replies from a farther bucket are ignored.
func TestSearchAdaptiveFarHelperPullsOut(t *testing.T) {
	far := func() *Search {
		s := NewSearch(topic1, testConfig(t))
		s.AddNodes(nil, nodesAtDistanceFrom(enode.ID(topic1), 247, 2, 1)) // bucket 9
		s.SetActiveBucket(12)
		return s
	}
	s := far()
	reply(t, s, 9, 2)
	reply(t, s, 9, 2)
	if s.ActiveBucket() != 12 {
		t.Fatalf("active bucket %d after sparse replies from a far helper, want 12", s.ActiveBucket())
	}
	s = far()
	reply(t, s, 9, 16)
	reply(t, s, 9, 16)
	if s.ActiveBucket() != 8 {
		t.Fatalf("active bucket %d after full replies from bucket 9, want 8", s.ActiveBucket())
	}
	s = far()
	reply(t, s, 9, 8)
	reply(t, s, 9, 9)
	if s.ActiveBucket() != 9 {
		t.Fatalf("active bucket %d after replies at the floor from bucket 9, want 9", s.ActiveBucket())
	}
}

// TestSearchAdaptiveAskedByID checks that an asked node with a newer record
// is still not asked again within the ad lifetime.
func TestSearchAdaptiveAskedByID(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)
	n := nodesAtDistanceFrom(enode.ID(topic1), 256, 1, 1)[0]
	s.AddNodes(nil, []*enode.Node{n})
	s.AddQueryResults(s.QueryTarget(), nil)

	next := NewSearch(topic1, config)
	next.SetAsked(s.asked)
	bumped := nodeWithSeq(n.ID(), n.IP(), n.Seq()+1)
	next.AddNodes(nil, []*enode.Node{bumped})
	if next.buckets[0].contains(n.ID()) {
		t.Fatal("asked node re-admitted with a newer record")
	}
}

// A reply cut short by an error is a lower bound: its ads count toward the
// ads the pass saw, and it is a density sample only when it reaches the
// floor on its own, so a lost packet neither pulls the search in nor hides
// a full reply.
func TestSearchAdaptivePartialReply(t *testing.T) {
	s := adaptiveSearch(t)
	s.SetActiveBucket(3)
	reply(t, s, 3, 0) // one empty sample: a second would jump the search in
	regs := nodesAtDistanceFrom(enode.ID(s.topic), s.buckets[3].dist, 2, 50)
	s.AddNodes(nil, regs)
	s.AddPartialReply(regs[0], nil, 0)
	if s.ActiveBucket() != 3 {
		t.Fatalf("active bucket moved to %d on an empty partial reply", s.ActiveBucket())
	}
	// Enough ads before the cut make a sample: with the empty one they read
	// "at least the floor", and the bucket stays; a second full one moves out.
	s.AddPartialReply(regs[1], nil, TopicNodesLimit)
	if s.ActiveBucket() != 3 {
		t.Fatalf("active bucket %d after a full partial reply and an empty one, want 3", s.ActiveBucket())
	}
	if s.AdsSeen() != TopicNodesLimit {
		t.Fatalf("ads seen %d, want %d: a partial reply's ads count", s.AdsSeen(), TopicNodesLimit)
	}
	more := nodesAtDistanceFrom(enode.ID(s.topic), s.buckets[3].dist, 1, 60)
	s.AddNodes(nil, more)
	s.AddPartialReply(more[0], nil, TopicNodesLimit)
	if s.ActiveBucket() != 2 {
		t.Fatalf("active bucket %d after two full partial replies, want 2: full samples move the search out", s.ActiveBucket())
	}
}

// A density sample that moved the active bucket places the next pass: a pass
// that then runs dry must not undo the move by stepping in again.
func TestSearchAdaptiveMoveSurvivesPassEnd(t *testing.T) {
	s := adaptiveSearch(t)
	s.SetActiveBucket(5)
	reply(t, s, 5, TopicNodesLimit)
	reply(t, s, 5, TopicNodesLimit)
	if s.ActiveBucket() != 4 {
		t.Fatalf("active bucket %d after two full replies, want 4", s.ActiveBucket())
	}
	for s.PeekResult() != nil {
		s.PopResult()
	}
	// The rest of the pass fails without a sample and runs dry.
	for n := s.QueryTarget(); n != nil; n = s.QueryTarget() {
		s.HandleErrorResponse(n, errors.New("timeout"))
	}
	if !s.IsDone() {
		t.Fatal("pass not done with nothing left to ask")
	}
	if got := s.NextPassBucket(); got != 4 {
		t.Fatalf("next pass at %d, want 4: the move out must survive the pass end", got)
	}
}

// A node that answered in this pass is not re-asked as a spare when it comes
// back as an aux node.
func TestSearchAdaptiveNoReaskInPass(t *testing.T) {
	config := testConfig(t)
	s := NewSearch(topic1, config)
	seeds := nodesAtDistanceFrom(enode.ID(topic1), 256, 1, 1)
	s.AddNodes(nil, seeds)
	n := reply(t, s, 0, 0)
	s.AddNodes(n, []*enode.Node{n}) // its own record among the aux nodes
	if len(s.spare) != 0 {
		t.Fatal("a node that answered in this pass was queued as a spare")
	}
	if got := s.QueryTarget(); got != nil {
		t.Fatalf("node %v re-asked in the pass it answered", got.ID())
	}
}

// A pass with no ads that reached the closest bucket starts the next one at
// the far end, so a topic that gets its first advertisers later is found.
func TestSearchAdaptiveEmptyTopicRestartsWide(t *testing.T) {
	s := adaptiveSearch(t)
	s.SetActiveBucket(len(s.buckets) - 1)
	if got := s.NextPassBucket(); got != 0 {
		t.Fatalf("next pass at %d after an empty pass at the closest bucket, want 0", got)
	}
	s.SetActiveBucket(3)
	if got := s.NextPassBucket(); got != 4 {
		t.Fatalf("next pass at %d after an empty pass at bucket 3, want 4", got)
	}
}

// A sample that confirms the active bucket places it as much as one that
// moves it: the next pass starts there, not one bucket in.
func TestSearchAdaptiveConfirmedBucketHolds(t *testing.T) {
	s := adaptiveSearch(t)
	s.SetActiveBucket(5)
	reply(t, s, 5, searchYieldFloor)
	reply(t, s, 5, searchYieldFloor)
	for s.PeekResult() != nil {
		s.PopResult()
	}
	if s.ActiveBucket() != 5 {
		t.Fatalf("active bucket %d after two replies at the floor, want 5", s.ActiveBucket())
	}
	for n := s.QueryTarget(); n != nil; n = s.QueryTarget() {
		s.HandleErrorResponse(n, errors.New("timeout"))
	}
	if got := s.NextPassBucket(); got != 5 {
		t.Fatalf("next pass at %d, want 5: the samples placed the bucket", got)
	}
}

// Spares are re-asked longest unasked first, so the passes rotate through
// them instead of re-asking the same few.
func TestSearchAdaptiveSpareOrder(t *testing.T) {
	clock := new(mclock.Simulated)
	config := testConfig(t)
	config.Clock = clock
	s := NewSearch(topic1, config)
	seeds := nodesAtDistanceFrom(enode.ID(topic1), 256, 3, 1)
	s.AddNodes(nil, seeds)
	var order []*enode.Node
	for range seeds {
		order = append(order, reply(t, s, 0, 0))
		clock.Run(time.Second)
	}

	next := NewSearch(topic1, config)
	next.SetAsked(s.asked)
	next.AddNodes(nil, seeds)
	for i, want := range order {
		if got := next.QueryTarget(); got == nil || got.ID() != want.ID() {
			t.Fatalf("spare %d is %v, want the one asked %d-th", i, got, i)
		}
	}
}

// Two passes over more spares than a pass re-asks cover all of them.
func TestSearchAdaptiveSpareRotation(t *testing.T) {
	clock := new(mclock.Simulated)
	config := testConfig(t)
	config.Clock = clock
	s := NewSearch(topic1, config)
	seeds := nodesAtDistanceFrom(enode.ID(topic1), 256, 2*searchSpareQueries, 1)
	s.AddNodes(nil, seeds)
	for range seeds {
		reply(t, s, 0, 0)
		clock.Run(time.Second)
	}
	asked := map[enode.ID]int{}
	for pass := 0; pass < 2; pass++ {
		next := NewSearch(topic1, config)
		next.SetAsked(s.asked)
		next.AddNodes(nil, seeds)
		for n := next.QueryTarget(); n != nil; n = next.QueryTarget() {
			asked[n.ID()]++
			next.AddQueryResults(n, nil)
			clock.Run(time.Second)
		}
	}
	for _, n := range seeds {
		if asked[n.ID()] != 1 {
			t.Fatalf("node asked %d times over two passes, want once each", asked[n.ID()])
		}
	}
}

// A spare whose asked entry expired after it answered in this pass is not
// asked again in the pass.
func TestSearchAdaptiveSpareNoReaskAfterExpiry(t *testing.T) {
	clock := new(mclock.Simulated)
	config := testConfig(t)
	config.Clock = clock
	config.AdLifetime = time.Minute
	s := NewSearch(topic1, config)
	seeds := nodesAtDistanceFrom(enode.ID(topic1), 256, 2, 1)
	s.AddNodes(nil, seeds)
	reply(t, s, 0, 0)
	reply(t, s, 0, 0)

	next := NewSearch(topic1, config)
	next.SetAsked(s.asked)
	next.AddNodes(nil, seeds) // both are spares
	spare := next.QueryTarget()
	next.AddQueryResults(spare, nil)
	clock.Run(2 * time.Minute) // every asked entry expires mid-pass
	next.AddNodes(nil, seeds)  // the table offers both again
	for n := next.QueryTarget(); n != nil; n = next.QueryTarget() {
		if n.ID() == spare.ID() {
			t.Fatal("a node that answered in this pass was asked again")
		}
		next.AddQueryResults(n, nil)
	}
}
