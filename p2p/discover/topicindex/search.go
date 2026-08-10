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
	"math/rand"
	"sync"
	"sync/atomic"

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
)

// Search is the state associated with searching for a single topic.
type Search struct {
	topic TopicID
	cfg   Config
	log   log.Logger

	// Note: search buckets are ordered far -> close.
	buckets [searchTableDepth]searchBucket

	bucketCheck  map[int]int
	resultBuffer []*enode.Node
	resultSeen   map[enode.ID]struct{}
	origin       map[enode.ID]bool // first-seen source: true=referral, false=DHT
	cycle        int               // search-cycle index, set by runLoop each rollover
}

// SetCycle records which rollover/cycle this Search instance is (reach instrumentation).
func (s *Search) SetCycle(c int) { s.cycle = c }

type searchBucket struct {
	dist        int
	new         map[enode.ID]*enode.Node
	asked       map[enode.ID]struct{}
	numRequests int

	ips netutil.DistinctNetSet
}

// Search-table provenance counters (process-global; aggregated across all
// concurrent searches). They distinguish registrars/ads that entered a search
// table via the DHT routing table (search seed, src==nil) versus referrals
// returned by other registrars' TOPICQUERY responses (src!=nil). Used to
// diagnose whether topic search is seed-driven or referral-driven.
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
	provBucketOcc          [searchTableDepth]atomic.Int64
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
	occ := make([]float64, len(provBucketOcc))
	for i := range provBucketOcc {
		if n > 0 {
			occ[i] = float64(provBucketOcc[i].Load()) / float64(n)
		}
	}
	return occ, map[string]int64{
		"full": provRejectFull.Load(), "onePerBucket": provRejectOnePerBucket.Load(),
		"ip": provRejectIP.Load(), "samples": n,
	}
}

// NewSearch creates a new topic search state.
func NewSearch(topic TopicID, cfg Config) *Search {
	cfg = cfg.withDefaults()
	s := &Search{
		cfg:         cfg,
		log:         cfg.Log.New("topic", topic),
		topic:       topic,
		resultSeen:  make(map[enode.ID]struct{}),
		bucketCheck: make(map[int]int, searchTableDepth),
		origin:      make(map[enode.ID]bool),
	}
	dist := 256
	for i := range s.buckets {
		s.buckets[i] = searchBucket{
			dist:  dist,
			new:   make(map[enode.ID]*enode.Node, cfg.SearchBucketSize),
			asked: make(map[enode.ID]struct{}, cfg.SearchBucketSize),
			ips: netutil.DistinctNetSet{
				Subnet: searchBucketSubnet,
				Limit:  searchBucketIPLimit,
			},
		}
		dist--
	}
	return s
}

// IsDone reports whether the search table is saturated. When it returns true,
// this search state should be abandoned and a new search started using a
// fresh Search instance.
func (s *Search) IsDone() bool {
	// The search cannot be done while there are unused results in the buffer.
	if len(s.resultBuffer) > 0 {
		return false
	}
	// The search cannot be done while there are still nodes that could be asked.
	for _, b := range s.buckets {
		if len(b.new) > 0 {
			return false
		}
	}
	// No unasked nodes remain and no results are buffered: the search is
	// saturated. There is no deeper "stalled but not yet done" state to wait
	// for, because once every bucket's `new` set is empty, QueryTarget
	// returns nil and no further queries (and thus no further AddNodes calls
	// that could change this state) can occur.
	for i := range s.buckets {
		provBucketOcc[i].Add(int64(s.buckets[i].count()))
	}
	provBucketSamples.Add(1)
	return true
}

// BucketsWithFreeSpace gives n distances from the topic at which
// the table has space available.
func (s *Search) BucketsWithFreeSpace(dists []uint) []uint {
	for _, b := range s.buckets {
		if b.count() < s.cfg.SearchBucketSize {
			dists = append(dists, uint(b.dist))
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
		// Skip nodes that are temporarily blacklisted for repeated RPC failures.
		if s.cfg.Blacklist.Contains(id) {
			continue
		}

		bi := s.bucketIndex(n.ID())
		b := &s.buckets[bi]

		if b.contains(id) {
			continue
		}
		if b.count() >= s.cfg.SearchBucketSize {
			provRejectFull.Add(1)
			continue
		}
		// Apply one-per-bucket rule.
		if src != nil {
			if s.bucketCheck[bi] >= s.cfg.MaxNodesPerSourcePerBucket {
				provRejectOnePerBucket.Add(1)
				s.cfg.Log.Debug("Ignoring search node", "id", n.ID(), "reason", "max-per-source-per-bucket")
				continue
			}
			s.bucketCheck[bi]++
		}
		// Apply IP restriction.
		ip := n.IP()
		if ip != nil && !netutil.IsLAN(ip) && !b.ips.Add(n.IP()) {
			provRejectIP.Add(1)
			s.cfg.Log.Debug("Ignoring search node", "id", n.ID(), "reason", "iplimit")
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

// RemoveNode drops a node from the search table. It is used to evict nodes that
// have become unresponsive. The node is removed from both the unasked ('new')
// and asked sets of its bucket.
func (s *Search) RemoveNode(id enode.ID) {
	b := s.bucket(id)
	if n, ok := b.new[id]; ok {
		if ip := n.IP(); ip != nil && !netutil.IsLAN(ip) {
			b.ips.Remove(ip)
		}
		delete(b.new, id)
	}
	delete(b.asked, id)
}

// QueryTarget returns a random node to which a topic query should be sent.
// The walk is gated by a warm-up frontier: only buckets with unasked nodes
// that have received at least one response (plus the next unqueried bucket
// with candidates) join the random pool. Empty buckets are invisible to
// the frontier, so they do not block progress to closer buckets.
func (s *Search) QueryTarget() *enode.Node {
	// Collect buckets with new nodes.
	withnew := make([]*searchBucket, 0, searchTableDepth)
	for i := range s.buckets {
		if len(s.buckets[i].new) > 0 {
			withnew = append(withnew, &s.buckets[i])
			// Stop here if no request was ever sent in this bucket.
			// This is to avoid spamming nodes close to the topic.
			// (Empty unqueried buckets fall through: they have no
			// candidate to warm up with, so the walk continues.)
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

// AddQueryResults adds the response nodes for a topic query to the table.
func (s *Search) AddQueryResults(from *enode.Node, results []*enode.Node) {
	b := s.bucket(from.ID())
	b.setAsked(from)
	b.numRequests++
	recordReach(s.cfg.Self, from.ID(), s.cycle, results)

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
	b.asked[n.ID()] = struct{}{}
	delete(b.new, n.ID())
}
