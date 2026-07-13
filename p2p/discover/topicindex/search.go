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

	bucketCheck  map[int]struct{}
	resultBuffer []*enode.Node
	resultSeen   map[enode.ID]struct{}
}

type searchBucket struct {
	dist        int
	new         map[enode.ID]*enode.Node
	asked       map[enode.ID]*enode.Node
	numRequests int

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

// IsDone reports when the search table peers are all consumed. When it returns true,
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
	// done. There is no more nodes to query.
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

		bi := s.bucketIndex(n.ID())
		b := &s.buckets[bi]

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

// HandleErrorResponse should be called when a topic query to a node fails.
// The node is dropped from the table, freeing its bucket slot and IP-limit
// entry for replacements. Unlike AddQueryResults, the failure does not count
// as a response: the bucket is not warmed, so QueryTarget keeps preferring the
// bucket's remaining candidates. A bucket whose nodes all fail becomes empty
// and no longer blocks the walk, and a search whose nodes all fail becomes
// IsDone and rolls over.
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

// AddQueryResults adds the response nodes for a topic query to the table.
func (s *Search) AddQueryResults(from *enode.Node, results []*enode.Node) {
	b := s.bucket(from.ID())
	b.setAsked(from)
	b.numRequests++

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
