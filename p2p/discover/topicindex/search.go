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
	"math/rand"
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

	// Adaptive distance: replies per bucket that estimate its ad density,
	// and the most buckets one estimate may move the search.
	searchYieldWindow = 4
	searchMaxJump     = 4
)

// Search is the state associated with searching for a single topic.
type Search struct {
	topic TopicID
	cfg   Config
	log   log.Logger

	// Note: search buckets are ordered far -> close.
	buckets [searchTableDepth]searchBucket

	// active is the bucket queried when SearchYieldFloor is set. Ads are
	// placed in every bucket, and their density per node doubles with each
	// bucket closer to the topic, so a reply's ad count from one bucket says
	// how many buckets away the floor is met.
	active int
	// asked holds the nodes an adaptive search has queried. They leave the
	// table so their slots refill with aux nodes, and stay out until the
	// ad lifetime has passed, so the search walks its bucket once instead
	// of re-asking the same nodes every pass.
	asked *SearchFilter
	// spare holds already-asked nodes offered to this pass. When nothing
	// unasked is left, one of them is asked again, once per pass, only for
	// the aux nodes its reply carries: the walk resumes from those.
	spare     map[enode.ID]*enode.Node
	spareUsed bool

	bucketCheck  map[int]struct{}
	resultBuffer []*enode.Node
	resultSeen   map[enode.ID]struct{}
}

type searchBucket struct {
	dist        int
	new         map[enode.ID]*enode.Node
	asked       map[enode.ID]*enode.Node
	numRequests int
	yield       []int // ad counts of the last searchYieldWindow replies

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
			dist:  dist,
			new:   make(map[enode.ID]*enode.Node, cfg.SearchBucketSize),
			asked: make(map[enode.ID]*enode.Node, cfg.SearchBucketSize),
			ips: netutil.DistinctNetSet{
				Subnet: searchBucketSubnet,
				Limit:  searchBucketIPLimit,
			},
		}
		dist--
	}
	return s
}

func (s *Search) adaptive() bool {
	return s.cfg.SearchYieldFloor > 0
}

// ActiveBucket returns the bucket an adaptive search queries.
func (s *Search) ActiveBucket() int {
	return s.active
}

// SetActiveBucket sets the bucket to query first, so a new search pass can
// start where the previous one settled.
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
	// An adaptive pass ends when the active bucket has been worked through
	// and had enough replies to decide where the next pass goes. A bucket
	// too sparse to decide falls through: the search keeps asking the
	// nearest populated buckets until one of them settles it.
	if s.adaptive() {
		if b := &s.buckets[s.active]; len(b.new) == 0 && len(b.yield) >= 2 {
			return true
		}
	}
	// The search cannot be done while there are still nodes that could be asked.
	for _, b := range s.buckets {
		if len(b.new) > 0 {
			return false
		}
	}
	if s.adaptive() && !s.spareUsed && len(s.spare) > 0 {
		return false
	}
	// No unasked nodes remain and no results are buffered: the search is
	// done. There is no more nodes to query. An adaptive search that walked
	// everything it could reach continues one bucket closer next pass, where
	// the ads are denser and the nodes are different ones.
	if s.adaptive() && s.active < len(s.buckets)-1 {
		s.active++
	}
	return true
}

// BucketsWithFreeSpace gives n distances from the topic at which
// the table has space available. An adaptive search lists the distances
// around its active bucket first: registrars answer the first few requested
// distances only, and those are the ones the next pass needs filled.
func (s *Search) BucketsWithFreeSpace(dists []uint) []uint {
	free := func(i int) bool { return s.buckets[i].count() < s.cfg.SearchBucketSize }
	if s.adaptive() {
		for i := max(0, s.active-1); i <= min(len(s.buckets)-1, s.active+1); i++ {
			if free(i) {
				dists = append(dists, uint(s.buckets[i].dist))
			}
		}
	}
	for i := range s.buckets {
		if free(i) && !slices.Contains(dists, uint(s.buckets[i].dist)) {
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

		if s.adaptive() && s.asked.Seen(n) {
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

// removeNode drops a node from the search table. The node is removed from both
// the unasked ('new') and asked sets of its bucket, and its IP-limit entry is
// released regardless of which set it was in.
func (s *Search) removeNode(id enode.ID) {
	b := s.bucket(id)
	n, ok := b.new[id]
	if !ok {
		n, ok = b.asked[id]
	}
	if ok {
		if ip := n.IP(); ip != nil && !netutil.IsLAN(ip) {
			b.ips.Remove(ip)
		}
	}
	delete(b.new, id)
	delete(b.asked, id)
}

// QueryTarget returns a random node to which a topic query should be sent.
// Random nodes are collected from buckets progressively: only buckets with unasked nodes
// that have received at least one response, plus the next unqueried bucket
// with candidates, join the random pool.
func (s *Search) QueryTarget() *enode.Node {
	if s.adaptive() {
		return s.adaptiveTarget()
	}
	// Collect buckets with new nodes.
	withnew := make([]*searchBucket, 0, searchTableDepth)
	for i := range s.buckets {
		if len(s.buckets[i].new) > 0 {
			withnew = append(withnew, &s.buckets[i])
			// Stop here if no request was ever sent in this bucket.
			if s.buckets[i].numRequests == 0 {
				break
			}
		}
	}

	if len(withnew) > 0 {
		// Select an unasked node in a random bucket.
		b := withnew[rand.Intn(len(withnew))]
		for _, n := range b.new {
			return n
		}
	}
	return nil
}

// adaptiveTarget picks an unasked node in the active bucket. While that bucket
// has no candidates, it asks the nearest bucket that has some, farther side
// first: the reply carries nodes at the active distance.
func (s *Search) adaptiveTarget() *enode.Node {
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

// observe records a reply's ad count for the bucket it came from and moves
// the active bucket once the bucket has two samples: one closer to the topic
// per halving of the density needed to reach the floor, one farther when
// replies are full. Medians keep a single lying reply from steering.
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
	case upper >= s.cfg.SearchYieldFloor:
		s.active = bi
	case upper == 0:
		s.active = min(len(s.buckets)-1, bi+searchMaxJump)
	default:
		jump := int(math.Ceil(math.Log2(float64(s.cfg.SearchYieldFloor) / float64(upper))))
		s.active = min(len(s.buckets)-1, bi+min(jump, searchMaxJump))
	}
}

// AddQueryResults adds the response nodes for a topic query to the table.
func (s *Search) AddQueryResults(from *enode.Node, results []*enode.Node) {
	b := s.bucket(from.ID())
	b.setAsked(from)
	b.numRequests++
	if s.adaptive() {
		s.observe(s.bucketIndex(from.ID()), len(results))
		s.asked.Add(from)
		s.removeNode(from.ID())
	}

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
	_, inNew := b.new[id]
	_, inAsked := b.asked[id]
	return inNew || inAsked
}

func (b *searchBucket) count() int {
	return len(b.new) + len(b.asked)
}

func (b *searchBucket) setAsked(n *enode.Node) {
	b.asked[n.ID()] = n
	delete(b.new, n.ID())
}
