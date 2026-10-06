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
	advanced  bool // the pass moved the active bucket at its end

	bucketCheck  map[int]struct{}
	resultBuffer []*enode.Node
	resultSeen   map[enode.ID]struct{}
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
		resultSeen:  make(map[enode.ID]struct{}),
		asked:       NewSearchFilter(cfg),
		spare:       make(map[enode.ID]*enode.Node),
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

// activeDone reports whether the active bucket is exhausted, ending the pass.
func (s *Search) activeDone() bool {
	b := &s.buckets[s.active]
	return len(b.new) == 0 && len(b.yield) >= 2
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

		if s.asked.Seen(n) {
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

// HandleErrorResponse drops a failed node from the table, freeing its slot and
// IP-limit entry.
func (s *Search) HandleErrorResponse(from *enode.Node, err error) {
	s.log.Debug("Topic query failed", "id", from.ID(), "err", err)
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
	switch {
	case lower >= TopicNodesLimit:
		s.active = max(0, bi-1)
	case upper >= searchYieldFloor:
		s.active = bi
	case upper == 0:
		s.active = min(len(s.buckets)-1, bi+searchMaxJump)
	default:
		jump := int(math.Ceil(math.Log2(float64(searchYieldFloor) / float64(upper))))
		s.active = min(len(s.buckets)-1, bi+min(jump, searchMaxJump))
	}
}

// AddQueryResults adds the response nodes for a topic query to the table.
func (s *Search) AddQueryResults(from *enode.Node, results []*enode.Node) {
	s.AddReply(from, results, len(results))
}

// AddReply adds the results of a topic query response. ads is the number of
// ads the response carried, the density sample that moves the active bucket.
func (s *Search) AddReply(from *enode.Node, results []*enode.Node, ads int) {
	s.observe(s.bucketIndex(from.ID()), ads)
	s.asked.Add(from)
	s.removeNode(from.ID())

	for _, n := range results {
		if n.ID() == s.cfg.Self {
			continue
		}
		s.cfg.Log.Debug("Added topic search result", "topic", s.topic, "fromid", from.ID(), "rid", n.ID())
		_, seen := s.resultSeen[n.ID()]
		if !seen {
			s.resultSeen[n.ID()] = struct{}{}
			s.resultBuffer = append(s.resultBuffer, n)
		}
	}
}

// AddResults adds nodes to the result set.
func (s *Search) AddResults(nodes []*enode.Node) {
	for _, n := range nodes {
		if _, seen := s.resultSeen[n.ID()]; !seen && n.ID() != s.cfg.Self {
			s.resultSeen[n.ID()] = struct{}{}
			s.resultBuffer = append(s.resultBuffer, n)
		}
	}
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
