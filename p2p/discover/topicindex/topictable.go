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
	"container/list"
	"math"
	"math/rand"
	"net"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/p2p/netutil"
)

// wait time computation constants.
const (
	occupancyExp = 10

	// scales the occupancy-based part of the waiting time
	waitTimeBaseModifier = 0.1
)

// If a node has less than this time to wait, they will be accepted anyway.
const topicTableWaitTimeFloor = 1 * time.Second

// TopicTable holds node registrations.
type TopicTable struct {
	all *list.List
	reg map[TopicID]*list.List
	wt  waitTimeState

	config Config
}

type topicTableEntry struct {
	exp   mclock.AbsTime
	node  *enode.Node
	topic TopicID

	topicElem *list.Element
	allElem   *list.Element
}

// NewTopicTable creates a TopicTable.
func NewTopicTable(cfg Config) *TopicTable {
	return &TopicTable{
		reg:    make(map[TopicID]*list.List),
		all:    list.New(),
		wt:     newWaitTimeState(),
		config: cfg.withDefaults(),
	}
}

// AdLifetime returns the configured advertisement lifetime.
func (tab *TopicTable) AdLifetime() time.Duration {
	return tab.config.AdLifetime
}

// Nodes returns all nodes registered for a topic.
func (tab *TopicTable) Nodes(topic TopicID) []*enode.Node {
	now := tab.config.Clock.Now()
	reglist := tab.reg[topic]
	if reglist == nil {
		return []*enode.Node{}
	}
	nodes := make([]*enode.Node, 0, reglist.Len())
	for e := reglist.Front(); e != nil; e = e.Next() {
		reg := e.Value.(*topicTableEntry)
		if reg.exp > now {
			nodes = append(nodes, reg.node)
		}
	}
	return nodes
}

// RandomNodes returns n random nodes registered for a topic.
// It only collects nodes for which the 'check' function returns true.
func (tab *TopicTable) RandomNodes(topic TopicID, n int, check func(*enode.Node) bool) []*enode.Node {
	reglist := tab.reg[topic]
	if reglist == nil || n == 0 {
		return []*enode.Node{}
	}
	if n > reglist.Len() {
		n = reglist.Len()
	}

	// Collect the nodes using 'reservoir sampling'.
	// First, fill the result with initial entries.
	nodes := make([]*enode.Node, 0, n)
	e := reglist.Front()
	for ; e != nil && len(nodes) < n; e = e.Next() {
		reg := e.Value.(*topicTableEntry)
		if check(reg.node) {
			nodes = append(nodes, reg.node)
		}
	}

	// Add remaining items conditionally.
	seen := len(nodes)
	for ; e != nil; e = e.Next() {
		reg := e.Value.(*topicTableEntry)
		if !check(reg.node) {
			continue
		}
		seen++
		x := rand.Intn(seen)
		if x < len(nodes) {
			nodes[x] = reg.node
		}
	}
	return nodes
}

// NextExpiryTime returns the time when the next registration expires.
func (tab *TopicTable) NextExpiryTime() mclock.AbsTime {
	e := tab.all.Front()
	if e != nil {
		return e.Value.(*topicTableEntry).exp
	}
	return Never
}

// Expire removes inactive registrations.
func (tab *TopicTable) Expire() {
	now := tab.config.Clock.Now()
	// Capture Next() before remove; list.Remove nils the element's next/prev
	// pointers, so e.Next() after remove would return nil.
	for e := tab.all.Front(); e != nil; {
		next := e.Next()
		reg := e.Value.(*topicTableEntry)
		if reg.exp > now {
			break
		}
		tab.remove(reg)
		tab.wt.removeReg(reg)
		e = next
	}
}

// isRegistered reports whether n is currently registered for topic t.
func (tab *TopicTable) isRegistered(n *enode.Node, t TopicID) bool {
	list := tab.reg[t]
	if list != nil {
		for el := list.Front(); el != nil; el = el.Next() {
			if el.Value.(*topicTableEntry).node.ID() == n.ID() {
				return true
			}
		}
	}
	return false
}

// Add adds a registration of node n for a topic. This only works when the table
// has space available.
func (tab *TopicTable) Add(n *enode.Node, topic TopicID) bool {
	if tab.all.Len() < tab.config.AdCacheSize {
		tab.add(n, topic)
		return true
	}
	return false
}

func (tab *TopicTable) add(n *enode.Node, topic TopicID) *topicTableEntry {
	reg := &topicTableEntry{
		node:  n,
		exp:   tab.config.Clock.Now().Add(tab.config.AdLifetime),
		topic: topic,
	}
	if tab.reg[topic] == nil {
		tab.reg[topic] = list.New()
	}
	reg.topicElem = tab.reg[topic].PushFront(reg)
	reg.allElem = tab.all.PushBack(reg)
	tab.wt.addReg(reg)
	return reg
}

// RemoveNode removes all registrations advertised by the given node, across all
// topics. It is used to evict ads pointing at a node that has become
// unresponsive.
func (tab *TopicTable) RemoveNode(id enode.ID) {
	for e := tab.all.Front(); e != nil; {
		next := e.Next()
		reg := e.Value.(*topicTableEntry)
		if reg.node.ID() == id {
			tab.remove(reg)
			tab.wt.removeReg(reg)
		}
		e = next
	}
}

func (tab *TopicTable) remove(reg *topicTableEntry) {
	tab.all.Remove(reg.allElem)
	topicList := tab.reg[reg.topic]
	if topicList.Len() == 1 {
		delete(tab.reg, reg.topic)
		// The topic's last ad is leaving the cache, so its service component
		// returns to zero and its lower-bound entry can be dropped. This keeps
		// topicBounds bounded by the number of distinct topics in the cache.
		delete(tab.wt.topicBounds, reg.topic)
	} else {
		topicList.Remove(reg.topicElem)
	}
	reg.topicElem = nil
	reg.allElem = nil
}

// topicSize returns the number of nodes registered for topic t.
func (tab *TopicTable) topicSize(t TopicID) int {
	list := tab.reg[t]
	if list == nil {
		return 0
	}
	return list.Len()
}

// WaitTime returns the amount of time that node n must have waited to register for topic t.
func (tab *TopicTable) WaitTime(n *enode.Node, t TopicID) time.Duration {
	total, _ := tab.waitTime(n, t, tab.config.Clock.Now())
	return total
}

// computes the required waiting time for (n, t) as of 'now'
func (tab *TopicTable) waitTime(n *enode.Node, t TopicID, now mclock.AbsTime) (time.Duration, waitTimeComponents) {
	regCount := tab.all.Len()

	// occupancy is the *inverse* of the table fill-ratio.
	occupancy := 1.0 - (float64(regCount) / float64(tab.config.AdCacheSize))

	// baseTime is the required wait-time, purely based on occupancy. When occupancy is
	// near 1.0 (i.e. the table is empty), baseTime is AdLifetime/10. As the table gets
	// fuller, baseTime goes up and will eventually exceed AdLifetime.
	baseTime := waitTimeBaseModifier * tab.config.AdLifetime.Seconds() / math.Pow(occupancy, occupancyExp)

	// topicMod changes the waiting time based on the ratio of registrations in the
	// requested topic vs. all topics.
	topicMod := float64(tab.topicSize(t)) / float64(regCount+1)

	// ipMod changes the waiting time based on IP address diversity.
	ipMod, ipNodes, ipFloor := tab.wt.ipScore(n, now)

	comps := waitTimeComponents{
		topic:       t,
		serviceSecs: baseTime * topicMod,
		ipSecs:      baseTime * ipMod,
		ipNodes:     ipNodes,
	}

	// Apply the per-component lower bounds (read-only).
	chargedService := comps.serviceSecs
	if rem := tab.wt.topicBounds[t].remaining(now).Seconds(); rem > chargedService {
		chargedService = rem
	}
	// The IP floor is the maximum over all nodes on the IP's tree path: bounds
	// are recorded on the longest-prefix-match node at quote time, and later
	// inserts can create deeper nodes, so reading only the current deepest node
	// would skip previously recorded floors.
	chargedIP := comps.ipSecs
	if rem := ipFloor.Seconds(); rem > chargedIP {
		chargedIP = rem
	}

	// The occupancy-scaled safety floor (baseTime * a tiny constant) was dropped:
	// as a baseTime multiplier it did nothing at light load — sub-second, below
	// the topicTableWaitTimeFloor admission slack — yet exploded at high
	// occupancy, quoting an empty-topic / diverse-IP registrant an absurd wait
	// even with free space. The wait is just the (lower-bounded) service and IP
	// components.
	neededTime := chargedService + chargedIP
	return time.Duration(math.Ceil(neededTime * float64(time.Second))), comps
}

// recordWaitTime updates the per-component lower bounds after a wait ticket has
// been issued, so a subsequent request for the same topic / IP prefix cannot
// obtain a smaller waiting time by more than the elapsed time.
func (tab *TopicTable) recordWaitTime(c waitTimeComponents, now mclock.AbsTime) {
	// Service component. serviceSecs > 0 implies the topic has at least one ad in
	// the cache, so the entry count is bounded by the cache capacity; the entry is
	// dropped in remove() when the topic's last ad leaves.
	if c.serviceSecs > 0 {
		b := tab.wt.topicBounds[c.topic]
		b.bump(secondsToDuration(c.serviceSecs), now)
		tab.wt.topicBounds[c.topic] = b
	}
	// IP component, stored on the longest-prefix-match node(s). Those nodes exist
	// only because of admitted ads, so nothing is allocated for a request that is
	// never admitted.
	if ipDur := secondsToDuration(c.ipSecs); ipDur > 0 {
		for _, nd := range c.ipNodes {
			if nd != nil {
				nd.bound.bump(ipDur, now)
			}
		}
	}
}

// secondsToDuration converts a wait-time value in (fractional) seconds to a
// Duration, rounding up so a stored floor is never below the value it represents.
func secondsToDuration(s float64) time.Duration {
	if s <= 0 {
		return 0
	}
	return time.Duration(math.Ceil(s * float64(time.Second)))
}

// Register adds node n for topic t if it has waited long enough.
//
// Returns 0 if the node is admitted, otherwise the new wait time the
// registrant should observe before retrying.
func (tab *TopicTable) Register(n *enode.Node, t TopicID, waitTime time.Duration) time.Duration {
	// Reject attempt if node is already registered.
	if tab.isRegistered(n, t) {
		return 0
	}

	now := tab.config.Clock.Now()
	requiredTime, comps := tab.waitTime(n, t, now)

	// Check if the node has waited enough.
	if waitTime < requiredTime {
		remaining := requiredTime - waitTime
		if remaining > topicTableWaitTimeFloor {
			// A wait ticket is being issued: record the per-component lower bounds
			// so the node can't obtain a cheaper quote by re-requesting.
			tab.recordWaitTime(comps, now)
			return remaining
		}
	}

	// Check if there is space. If not, the node needs to come back when a slot opens.
	if tab.all.Len() >= tab.config.AdCacheSize {
		return tab.NextExpiryTime().Sub(now)
	}

	tab.add(n, t)
	return 0
}

// lowerBound is a lower bound on a waiting-time component (§6, "Lower Bound").
//
// It records the largest value recently quoted for a component together with the
// time it was recorded. The bound decays 1:1 with elapsed time, which enforces
// the invariant that a re-request cannot obtain a value smaller than a previous
// quote by more than the time that has passed (w1 - w2 <= t2 - t1). The zero
// value is a valid, already-expired bound (no floor).
type lowerBound struct {
	value time.Duration  // floor value as of 'since'
	since mclock.AbsTime // when the floor was recorded
}

// remaining returns the still-effective floor at 'now'. It never goes negative.
func (lb lowerBound) remaining(now mclock.AbsTime) time.Duration {
	d := lb.value - now.Sub(lb.since)
	if d < 0 {
		return 0
	}
	return d
}

// bump raises the floor to v (recorded at 'now') when v exceeds the currently
// remaining floor, and returns the value that should actually be charged.
func (lb *lowerBound) bump(v time.Duration, now mclock.AbsTime) time.Duration {
	rem := lb.remaining(now)
	if v > rem {
		lb.value, lb.since = v, now
		return v
	}
	return rem
}

// waitTimeComponents holds the freshly-computed (un-floored) waiting-time
// components for a single request
type waitTimeComponents struct {
	topic       TopicID
	serviceSecs float64
	ipSecs      float64
	ipNodes     [2]*ipTreeNode // longest-prefix-match nodes for IPv4/IPv6
}

// waitTimeState holds the state of waiting time modifier functions (including the lower bounds).
type waitTimeState struct {
	ipv4        *ipTree
	ipv6        *ipTree
	topicBounds map[TopicID]lowerBound
}

func newWaitTimeState() waitTimeState {
	return waitTimeState{
		ipv4:        newIPTree(32),
		ipv6:        newIPTree(128),
		topicBounds: make(map[TopicID]lowerBound),
	}
}

func (wt *waitTimeState) ipScore(n *enode.Node, now mclock.AbsTime) (float64, [2]*ipTreeNode, time.Duration) {
	var (
		ip4    enr.IPv4
		ip6    enr.IPv6
		score4 float64
		score6 float64
		nodes  [2]*ipTreeNode
		floor  time.Duration
	)
	if n.Load(&ip4) == nil && !netutil.IsLAN(net.IP(ip4)) {
		score4, nodes[0] = wt.ipv4.scoreNode(net.IP(ip4))
		if f := wt.ipv4.pathFloor(net.IP(ip4), now); f > floor {
			floor = f
		}
	}
	if n.Load(&ip6) == nil && !netutil.IsLAN(net.IP(ip6)) {
		score6, nodes[1] = wt.ipv6.scoreNode(net.IP(ip6))
		if f := wt.ipv6.pathFloor(net.IP(ip6), now); f > floor {
			floor = f
		}
	}
	return math.Max(score4, score6), nodes, floor
}

func (wt *waitTimeState) addReg(reg *topicTableEntry) {
	var ip4 enr.IPv4
	var ip6 enr.IPv6
	if reg.node.Load(&ip4) == nil {
		wt.ipv4.insert(net.IP(ip4))
	}
	if reg.node.Load(&ip6) == nil {
		wt.ipv6.insert(net.IP(ip6))
	}
}

func (wt *waitTimeState) removeReg(reg *topicTableEntry) {
	var ip4 enr.IPv4
	var ip6 enr.IPv6
	if reg.node.Load(&ip4) == nil {
		wt.ipv4.remove(net.IP(ip4))
	}
	if reg.node.Load(&ip6) == nil {
		wt.ipv6.remove(net.IP(ip6))
	}
}
