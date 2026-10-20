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
	"math"
	"slices"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/netutil"
)

const (
	// searchTableDepth is the number of buckets kept in the search table.
	//
	// The table only keeps nodes at logdist(topic, n) > (256 - searchTableDepth).
	// Should there be any nodes which are closer than this, they just go into the last
	// (closest) bucket.
	searchTableDepth = 18

	// IP subnet limit.
	searchBucketSubnet, searchBucketIPLimit = 24, 1

	// Adaptive distance.
	searchYieldFloor  = 8
	searchYieldWindow = 4
	searchMaxJump     = 4
	searchAuxRadius   = 1
	// searchPassQueries bounds a pass: aux nodes can refill the active bucket
	// for as long as the search keeps asking.
	searchPassQueries = 32
)

// Search is the state associated with searching for a single topic.
type Search struct {
	topic TopicID
	cfg   Config
	log   log.Logger

	// Note: search buckets are ordered far -> close.
	buckets [searchTableDepth]searchBucket

	active    int
	asked     *SearchFilter            // nodes queried within the last ad lifetime
	spare     map[enode.ID]*enode.Node // asked nodes, one re-asked per pass if nothing else is left
	spareUsed bool
	advanced  bool                  // the pass moved the active bucket at its end
	replies   int                   // queries answered or failed in this pass
	ads       int                   // ads the replies of this pass carried
	failed    map[enode.ID]struct{} // nodes that failed in this pass, not re-admitted before the next one

	bucketCheck  map[int]struct{}
	resultBuffer []*enode.Node
	resultSeen   map[enode.ID]uint64 // record seq of every result so far
}

type searchBucket struct {
	dist  int
	new   map[enode.ID]*enode.Node
	yield []int // ad counts of the last searchYieldWindow replies

	ips netutil.DistinctNetSet
}

// NewSearch creates a new topic search state.
func NewSearch(topic TopicID, cfg Config) *Search {
	cfg = cfg.withDefaults()
	s := &Search{
		cfg:         cfg,
		log:         cfg.Log.New("topic", topic),
		topic:       topic,
		resultSeen:  make(map[enode.ID]uint64),
		asked:       NewSearchFilter(cfg),
		spare:       make(map[enode.ID]*enode.Node),
		failed:      make(map[enode.ID]struct{}),
		bucketCheck: make(map[int]struct{}, searchTableDepth),
	}
	dist := 256
	for i := range s.buckets {
		s.buckets[i] = searchBucket{
			dist: dist,
			new:  make(map[enode.ID]*enode.Node, cfg.SearchBucketSize),
			ips: netutil.DistinctNetSet{
				Subnet: searchBucketSubnet,
				Limit:  searchBucketIPLimit,
			},
		}
		dist--
	}
	return s
}

// ActiveBucket returns the bucket the search queries.
func (s *Search) ActiveBucket() int {
	return s.active
}

// SetActiveBucket sets the bucket to query first.
func (s *Search) SetActiveBucket(i int) {
	s.active = max(0, min(i, len(s.buckets)-1))
}

// AtClosestBucket reports whether the active bucket is the one closest to the
// topic, past which an empty topic has nowhere left to walk.
func (s *Search) AtClosestBucket() bool {
	return s.active == len(s.buckets)-1
}

// AdsSeen returns the number of ads the replies of this pass carried.
func (s *Search) AdsSeen() int {
	return s.ads
}

// SetAskedFilter shares the record of queried nodes between search passes.
func (s *Search) SetAskedFilter(f *SearchFilter) {
	s.asked = f
}

// IsDone reports when the search table peers are all consumed. When it returns true,
// this search state should be abandoned and a new search started using a
// fresh Search instance.
func (s *Search) IsDone() bool {
	// The search cannot be done while there are unused results in the buffer.
	if len(s.resultBuffer) > 0 {
		return false
	}
	if s.activeDone() {
		return true
	}
	// The search cannot be done while there are still nodes that could be asked.
	for _, b := range s.buckets {
		if len(b.new) > 0 {
			return false
		}
	}
	if !s.spareUsed && len(s.spare) > 0 {
		return false
	}
	// Nothing left to ask: the next pass starts one bucket closer.
	if !s.advanced && s.active < len(s.buckets)-1 {
		s.active++
		s.advanced = true
	}
	return true
}

// activeDone reports whether the pass is over: the active bucket is exhausted
// with enough replies to place the next pass, or the pass spent its queries.
func (s *Search) activeDone() bool {
	b := &s.buckets[s.active]
	return s.replies >= searchPassQueries || (len(b.new) == 0 && len(b.yield) >= 2)
}

// BucketsWithFreeSpace gives the distances from the topic within
// searchAuxRadius of the active bucket at which the table has space available.
func (s *Search) BucketsWithFreeSpace(dists []uint) []uint {
	for i := max(0, s.active-searchAuxRadius); i <= min(len(s.buckets)-1, s.active+searchAuxRadius); i++ {
		if s.buckets[i].count() < s.cfg.SearchBucketSize {
			dists = append(dists, uint(s.buckets[i].dist))
		}
	}
	return dists
}

// AddNodes adds potential registrars to the table.
// If src is non-nil, it is assumed that the nodes were sent by that node.
func (s *Search) AddNodes(src *enode.Node, nodes []*enode.Node) {
	// Clear the one-per-bucket check table.
	for k := range s.bucketCheck {
		delete(s.bucketCheck, k)
	}

	for _, n := range nodes {
		id := n.ID()
		if id == s.cfg.Self {
			continue
		}

		bi := s.bucketIndex(n.ID())
		b := &s.buckets[bi]

		if _, ok := s.failed[id]; ok {
			continue
		}
		if s.asked.Has(n.ID()) {
			s.spare[id] = n
			continue
		}
		if b.contains(id) || b.count() >= s.cfg.SearchBucketSize {
			continue
		}
		// Apply one-per-bucket rule.
		if src != nil {
			if _, ok := s.bucketCheck[bi]; ok {
				s.cfg.Log.Debug("Ignoring search node", "id", n.ID(), "reason", "one-per-bucket-rule")
				continue
			}
			s.bucketCheck[bi] = struct{}{}
		}
		// Apply IP restriction.
		ip := n.IP()
		if ip != nil && !netutil.IsLAN(ip) && !b.ips.Add(n.IP()) {
			s.cfg.Log.Debug("Ignoring search node", "id", n.ID(), "reason", "iplimit")
			continue
		}

		// All checks passed, add the node.
		b.new[id] = n
	}
}

// HandleErrorResponse drops a failed node from the table for the rest of the
// pass, freeing its slot and IP-limit entry. It is not recorded as asked: a
// node that did not answer may have dropped one packet, and the next pass
// may ask it again, while a dead one leaves the routing table on its own.
func (s *Search) HandleErrorResponse(from *enode.Node, err error) {
	s.log.Debug("Topic query failed", "id", from.ID(), "err", err)
	s.replies++
	s.failed[from.ID()] = struct{}{}
	s.removeNode(from.ID())
}

// removeNode drops a node and releases its IP-limit entry.
func (s *Search) removeNode(id enode.ID) {
	b := s.bucket(id)
	if n, ok := b.new[id]; ok {
		if ip := n.IP(); ip != nil && !netutil.IsLAN(ip) {
			b.ips.Remove(ip)
		}
	}
	delete(b.new, id)
}

// QueryTarget picks an unasked node in the active bucket, or in the nearest
// bucket that has one. It returns nil once the pass is over.
func (s *Search) QueryTarget() *enode.Node {
	if s.activeDone() {
		return nil
	}
	for d := 0; d < len(s.buckets); d++ {
		for _, i := range [2]int{s.active - d, s.active + d} {
			if i < 0 || i >= len(s.buckets) {
				continue
			}
			for _, n := range s.buckets[i].new {
				return n
			}
		}
	}
	if !s.spareUsed {
		for id, n := range s.spare {
			s.spareUsed = true
			delete(s.spare, id)
			return n
		}
	}
	return nil
}

// observe records the ad count of a reply and moves the active bucket by
// the median of the recent replies from that bucket.
func (s *Search) observe(bi int, ads int) {
	b := &s.buckets[bi]
	b.yield = append(b.yield, ads)
	if len(b.yield) > searchYieldWindow {
		b.yield = b.yield[1:]
	}
	if len(b.yield) < 2 {
		return
	}
	sorted := append([]int(nil), b.yield...)
	slices.Sort(sorted)
	lower, upper := sorted[(len(sorted)-1)/2], sorted[len(sorted)/2]
	var next int
	switch {
	case lower >= TopicNodesLimit:
		next = max(0, bi-1)
	case upper >= searchYieldFloor:
		next = bi
	case upper == 0:
		next = min(len(s.buckets)-1, bi+searchMaxJump)
	default:
		jump := int(math.Ceil(math.Log2(float64(searchYieldFloor) / float64(upper))))
		next = min(len(s.buckets)-1, bi+min(jump, searchMaxJump))
	}
	// Ads get denser toward the topic, so a helper farther out than the
	// active bucket can only pull the search out, and one closer in can only
	// push it in.
	switch {
	case bi >= s.active-searchAuxRadius && bi <= s.active+searchAuxRadius:
	case bi < s.active && next <= bi:
	case bi > s.active && next > bi:
	default:
		return
	}
	s.active = next
}

// AddReply adds the results of a topic query response. ads is the number of
// ads the response carried, the density sample that moves the active bucket.
func (s *Search) AddReply(from *enode.Node, results []*enode.Node, ads int) {
	s.replies++
	s.ads += ads
	s.observe(s.bucketIndex(from.ID()), ads)
	s.asked.Add(from)
	s.removeNode(from.ID())

	// The results may come from several registrars when the filter mixes
	// them, so they are not attributed to from.
	s.cfg.Log.Debug("Topic query answered", "topic", s.topic, "fromid", from.ID(), "n", len(results))
	s.AddResults(results)
}

// AddResults adds nodes to the result set.
func (s *Search) AddResults(nodes []*enode.Node) {
	for _, n := range nodes {
		if n.ID() != s.cfg.Self {
			s.addResult(n)
		}
	}
}

// addResult buffers a result unless the node was already a result of this
// search. A newer record of a node is left for the next pass.
func (s *Search) addResult(n *enode.Node) {
	if _, seen := s.resultSeen[n.ID()]; seen {
		return
	}
	s.resultSeen[n.ID()] = n.Seq()
	s.resultBuffer = append(s.resultBuffer, n)
}

// Buffered returns the number of results waiting to be handed out.
func (s *Search) Buffered() int {
	return len(s.resultBuffer)
}

// PeekResult returns a node from the result set.
// When no result is available, it returns nil.
func (s *Search) PeekResult() *enode.Node {
	if len(s.resultBuffer) > 0 {
		return s.resultBuffer[0]
	}
	return nil
}

// PopResult removes a result node.
func (s *Search) PopResult() {
	if len(s.resultBuffer) == 0 {
		panic("PopResult with len(results) == 0")
	}
	s.resultBuffer = append(s.resultBuffer[:0], s.resultBuffer[1:]...)
}

func (s *Search) bucketIndex(id enode.ID) int {
	dist := 256 - enode.LogDist(enode.ID(s.topic), id)
	if dist > len(s.buckets)-1 {
		dist = len(s.buckets) - 1
	}
	return dist
}

func (s *Search) bucket(id enode.ID) *searchBucket {
	return &s.buckets[s.bucketIndex(id)]
}

func (b *searchBucket) contains(id enode.ID) bool {
	_, ok := b.new[id]
	return ok
}

func (b *searchBucket) count() int {
	return len(b.new)
}
