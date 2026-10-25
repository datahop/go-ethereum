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

	"github.com/ethereum/go-ethereum/common/mclock"
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
	// searchPassQueries ends a pass after this many queries whatever their
	// outcome: aux nodes can refill the active bucket for as long as the
	// search keeps asking.
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
	start     int                      // the bucket this pass started at
	asked     *AskedSet                // nodes queried within the last ad lifetime
	failed    *AskedSet                // nodes whose query failed within the last ad lifetime
	spare     map[enode.ID]*enode.Node // asked nodes; the longest unasked is re-asked once if nothing else is left
	spareUsed bool                     // the pass re-asked its one spare
	placed    bool                     // a density sample placed the active bucket in this pass
	replies   int                      // queries answered or failed in this pass
	askedNow  map[enode.ID]struct{}    // nodes that answered or failed in this pass, not asked again in it
	ads       int                      // ads the replies of this pass carried

	bucketCheck  map[int]struct{}
	resultBuffer []*enode.Node
	resultSeen   map[enode.ID]uint64 // record seq of every result so far
}

type searchBucket struct {
	dist   int
	new    map[enode.ID]*enode.Node
	yield  []int // ad counts of the last searchYieldWindow replies
	done   int   // replies from this bucket in the pass, cut short or not
	failed int   // queries to this bucket that failed in the pass

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
		asked:       NewAskedSet(cfg),
		failed:      NewAskedSet(cfg),
		spare:       make(map[enode.ID]*enode.Node),
		askedNow:    make(map[enode.ID]struct{}),
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
	s.start = s.active
}

// ActiveFailed reports whether the active bucket was asked in this pass and
// every query to it failed, with no density sample placing it.
func (s *Search) ActiveFailed() bool {
	b := &s.buckets[s.active]
	return !s.placed && b.done == 0 && b.failed > 0
}

// AdsSeen returns the number of ads the replies of this pass carried.
func (s *Search) AdsSeen() int {
	return s.ads
}

// SetAsked shares the record of queried nodes between search passes.
func (s *Search) SetAsked(a *AskedSet) {
	s.asked = a
}

// SetFailed shares the record of failed nodes between search passes.
func (s *Search) SetFailed(f *AskedSet) {
	s.failed = f
}

// IsDone reports when the pass is over. When it returns true, this search
// state should be abandoned and a new search started using a fresh Search
// instance, at NextPassBucket.
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
	return s.spareUsed || len(s.spare) == 0
}

// NextPassBucket returns the bucket the next pass starts from once this one
// is done: the active one, or the next one in when the pass ran dry before
// the bucket could decide, unless every query to it failed and its density
// is still unknown. A pass that started at the closest bucket and saw no ads
// anywhere starts wide again, so that an empty topic's first advertisers
// are found.
func (s *Search) NextPassBucket() int {
	next := s.active
	if !s.activeDone() && !s.placed && !s.ActiveFailed() {
		next = min(s.active+1, len(s.buckets)-1)
	}
	if last := len(s.buckets) - 1; s.ads == 0 && next == last && s.start == last {
		return 0
	}
	return next
}

// activeDone reports whether the pass is over: the pass spent its queries, or
// the active bucket is exhausted with enough samples to place the next pass.
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
		if s.failed.Has(id) {
			continue
		}

		bi := s.bucketIndex(n.ID())
		b := &s.buckets[bi]

		if _, ok := s.askedNow[id]; ok {
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

// HandleErrorResponse drops a failed node from the table and keeps it out for
// an ad lifetime, across the passes that share the failed set: a dead
// routing-table entry would otherwise be picked again every pass.
func (s *Search) HandleErrorResponse(from *enode.Node, err error) {
	s.log.Debug("Topic query failed", "id", from.ID(), "err", err)
	s.replies++
	s.askedNow[from.ID()] = struct{}{}
	s.failed.Add(from.ID())
	delete(s.spare, from.ID())
	s.buckets[s.bucketIndex(from.ID())].failed++
	s.removeNode(from.ID(), false)
}

// removeNode drops a node from its bucket. With keepIP its subnet stays
// counted against the bucket's IP limit for the pass, so a node that
// answered cannot be replaced by another from its own subnet.
func (s *Search) removeNode(id enode.ID, keepIP bool) {
	b := s.bucket(id)
	if n, ok := b.new[id]; ok && !keepIP {
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
	// Spares go longest unasked first: their ads are the likeliest to have
	// changed.
	if !s.spareUsed {
		var pick *enode.Node
		var at mclock.AbsTime
		for id, n := range s.spare {
			if t := s.asked.LastAsked(id); pick == nil || t < at {
				pick, at = n, t
			}
		}
		if pick != nil {
			s.spareUsed = true
			delete(s.spare, pick.ID())
			return pick
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
	s.active, s.placed = next, true
}

// AddReply adds the results of a topic query response. ads is the number of
// usable ads the response carried, counted by AdsSeen. sent is the number of
// records as the registrar sent them, the density sample that moves the
// active bucket.
func (s *Search) AddReply(from *enode.Node, results []*enode.Node, ads, sent int) {
	s.addReply(from, results, ads, sent, true)
}

// AddPartialReply adds the results of a response cut short by an error. Its
// record count is a lower bound: it counts as a density sample only when it
// reaches the floor on its own.
func (s *Search) AddPartialReply(from *enode.Node, results []*enode.Node, ads, sent int) {
	s.addReply(from, results, ads, sent, sent >= searchYieldFloor)
}

func (s *Search) addReply(from *enode.Node, results []*enode.Node, ads, sent int, sample bool) {
	s.replies++
	s.ads += ads
	s.askedNow[from.ID()] = struct{}{}
	delete(s.spare, from.ID())
	s.buckets[s.bucketIndex(from.ID())].done++
	if sample {
		s.observe(s.bucketIndex(from.ID()), sent)
	}
	s.asked.Add(from.ID())
	s.removeNode(from.ID(), true)

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
