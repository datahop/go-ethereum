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
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/netutil"
)

const (
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
	buckets []searchBucket

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
	advanced  bool // the pass moved the active bucket at its end
	sampled   bool // the pass added its bucket occupancy to the statistics

	bucketCheck  map[int]struct{}
	cycle        int               // search-cycle index, set by runLoop each rollover
	origin       map[enode.ID]bool // first-seen source: true=referral, false=DHT
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
		bucketCheck: make(map[int]struct{}, cfg.SearchTableDepth),
		origin:      make(map[enode.ID]bool),
		buckets:     make([]searchBucket, cfg.SearchTableDepth),
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
	if s.adaptive() && s.activeDone() {
		return true
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
	// done. There is no more nodes to query.
	if !s.sampled {
		s.sampled = true
		for i := range s.buckets {
			provBucketOcc[i].Add(int64(s.buckets[i].count()))
		}
		provBucketCount.Store(int64(len(s.buckets)))
		provBucketSamples.Add(1)
	}
	// An adaptive search that walked everything it could reach continues one
	// bucket closer next pass, where the ads are denser and the nodes differ.
	// IsDone can be asked again after held results are handed out: move once.
	if s.adaptive() && !s.advanced && s.active < len(s.buckets)-1 {
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

// BucketsWithFreeSpace gives n distances from the topic at which
// the table has space available. An adaptive search lists the distances
// within SearchAuxRadius of its active bucket, and only those unless the
// radius is negative.
func (s *Search) BucketsWithFreeSpace(dists []uint) []uint {
	free := func(i int) bool { return s.buckets[i].count() < s.cfg.SearchBucketSize }
	if s.adaptive() {
		r := max(1, s.cfg.SearchAuxRadius)
		for i := max(0, s.active-r); i <= min(len(s.buckets)-1, s.active+r); i++ {
			if free(i) {
				dists = append(dists, uint(s.buckets[i].dist))
			}
		}
		if s.cfg.SearchAuxRadius > 0 {
			return dists
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
			provRejectFull.Add(1)
			continue
		}
		// Apply one-per-bucket rule.
		if src != nil {
			if _, ok := s.bucketCheck[bi]; ok {
				s.cfg.Log.Debug("Ignoring search node", "id", n.ID(), "reason", "one-per-bucket-rule")
				provRejectOnePerBucket.Add(1)
				continue
			}
			s.bucketCheck[bi] = struct{}{}
		}
		// Apply IP restriction.
		ip := n.IP()
		if ip != nil && !netutil.IsLAN(ip) && !b.ips.Add(n.IP()) {
			s.cfg.Log.Debug("Ignoring search node", "id", n.ID(), "reason", "iplimit")
			provRejectIP.Add(1)
			continue
		}

		// All checks passed, add the node.
		b.new[id] = n
		if src == nil {
			provAddedDHT.Add(1)
			if _, ok := s.origin[id]; !ok {
				s.origin[id] = false
			}
		} else {
			provAddedReferral.Add(1)
			if _, ok := s.origin[id]; !ok {
				s.origin[id] = true
			}
		}
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
	withnew := make([]*searchBucket, 0, len(s.buckets))
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
	case lower >= s.cfg.TopicNodesLimit:
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
// AddQueryResults returns how many results were new to this search.
func (s *Search) AddQueryResults(from *enode.Node, results []*enode.Node) int {
	s.RecordReach(from, results)
	return s.AddReply(from, results, len(results))
}

// RecordReach records the ads of a registrar's reply (reach instrumentation).
func (s *Search) RecordReach(from *enode.Node, ads []*enode.Node) {
	recordReach(s.cfg.Self, from.ID(), s.cycle, ads)
}

// AddReply adds the results taken from a topic query response. ads is the
// number of ads the response carried, which can be more than the results
// taken from it. It is the density sample of an adaptive search. AddReply
// returns how many results were new to this search.
func (s *Search) AddReply(from *enode.Node, results []*enode.Node, ads int) int {
	b := s.bucket(from.ID())
	b.setAsked(from)
	b.numRequests++
	if s.adaptive() {
		s.observe(s.bucketIndex(from.ID()), ads)
		s.asked.Add(from)
		s.removeNode(from.ID())
	}

	referral := s.origin[from.ID()]
	newAds := 0
	for _, n := range results {
		if n.ID() == s.cfg.Self {
			continue
		}
		s.cfg.Log.Debug("Added topic search result", "topic", s.topic, "fromid", from.ID(), "rid", n.ID())
		_, seen := s.resultSeen[n.ID()]
		if !seen {
			s.resultSeen[n.ID()] = struct{}{}
			s.resultBuffer = append(s.resultBuffer, n)
			newAds++
		}
	}
	if referral {
		provQueriedReferral.Add(1)
		provAdsReferral.Add(int64(newAds))
	} else {
		provQueriedDHT.Add(1)
		provAdsDHT.Add(int64(newAds))
	}
	return newAds
}

// AddResults adds nodes to the result set and returns how many were new.
func (s *Search) AddResults(nodes []*enode.Node) int {
	added := 0
	for _, n := range nodes {
		if _, seen := s.resultSeen[n.ID()]; !seen && n.ID() != s.cfg.Self {
			s.resultSeen[n.ID()] = struct{}{}
			s.resultBuffer = append(s.resultBuffer, n)
			added++
		}
	}
	return added
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

// BucketIndex returns the index of the bucket holding id, 0 the farthest
// from the topic.
func (s *Search) BucketIndex(id enode.ID) int { return s.bucketIndex(id) }

// NumBuckets returns the table depth.
func (s *Search) NumBuckets() int { return len(s.buckets) }

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

// reachData records, per searcher (self ID), the set of registrars it queried,
// for a small ID-sampled subset. Used to localize the search bottleneck.
type regStat struct {
	firstCycle int
	nQueries   int
	ads        map[enode.ID]struct{}
}

type reachSet struct {
	mu   sync.Mutex
	regs map[enode.ID]*regStat
}

var (
	reachData    sync.Map // self enode.ID -> *reachSet ; sharded, ~1 writer per self
	reachEnabled bool
)

// EnableReach turns on per-searcher reach sampling.
func EnableReach() { reachEnabled = true }

func reachSampled(id enode.ID) bool { return id[0] < 3 } // ~1% of nodes (heavier per-reg recording)

func recordReach(self, reg enode.ID, cycle int, results []*enode.Node) {
	if !reachEnabled || !reachSampled(self) {
		return
	}
	v, ok := reachData.Load(self)
	if !ok {
		v, _ = reachData.LoadOrStore(self, &reachSet{regs: make(map[enode.ID]*regStat)})
	}
	rs := v.(*reachSet)
	rs.mu.Lock()
	st := rs.regs[reg]
	if st == nil {
		st = &regStat{firstCycle: cycle, ads: make(map[enode.ID]struct{})}
		rs.regs[reg] = st
	}
	st.nQueries++
	for _, n := range results {
		st.ads[n.ID()] = struct{}{}
	}
	rs.mu.Unlock()
}

// ReachRec is one searcher's reach record for a single registrar.
type ReachRec struct {
	Reg        enode.ID
	FirstCycle int
	NQueries   int
	NDistinct  int
}

// ReachData returns the sampled per-searcher per-registrar reach stats.
func ReachData() map[enode.ID][]ReachRec {
	out := make(map[enode.ID][]ReachRec)
	reachData.Range(func(k, v any) bool {
		rs := v.(*reachSet)
		rs.mu.Lock()
		l := make([]ReachRec, 0, len(rs.regs))
		for reg, st := range rs.regs {
			l = append(l, ReachRec{reg, st.firstCycle, st.nQueries, len(st.ads)})
		}
		rs.mu.Unlock()
		out[k.(enode.ID)] = l
		return true
	})
	return out
}

var (
	provAddedDHT           atomic.Int64
	provAddedReferral      atomic.Int64
	provQueriedDHT         atomic.Int64
	provQueriedReferral    atomic.Int64
	provAdsDHT             atomic.Int64
	provAdsReferral        atomic.Int64
	provRejectFull         atomic.Int64
	provRejectOnePerBucket atomic.Int64
	provRejectIP           atomic.Int64
	provBucketOcc          [maxTableDepth]atomic.Int64
	provBucketCount        atomic.Int64
	provBucketSamples      atomic.Int64
)

// SearchProvenance returns cumulative provenance counts.
func SearchProvenance() map[string]int64 {
	return map[string]int64{
		"addedDHT": provAddedDHT.Load(), "addedReferral": provAddedReferral.Load(),
		"queriedDHT": provQueriedDHT.Load(), "queriedReferral": provQueriedReferral.Load(),
		"adsDHT": provAdsDHT.Load(), "adsReferral": provAdsReferral.Load(),
	}
}

// SearchBucketStats returns mean occupancy per search-table bucket (index 0 =
// farthest from topic, last = closest), sampled when a search saturates, plus
// AddNodes rejection counts.
func SearchBucketStats() ([]float64, map[string]int64) {
	n := provBucketSamples.Load()
	occ := make([]float64, provBucketCount.Load())
	for i := range occ {
		if n > 0 {
			occ[i] = float64(provBucketOcc[i].Load()) / float64(n)
		}
	}
	return occ, map[string]int64{
		"full": provRejectFull.Load(), "onePerBucket": provRejectOnePerBucket.Load(),
		"ip": provRejectIP.Load(), "samples": n,
	}
}

// SetCycle records which rollover/cycle this Search instance is (reach instrumentation).
func (s *Search) SetCycle(c int) { s.cycle = c }
