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
	"net/netip"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/p2p/netutil"
)

// wait time computation constants.
const (
	occupancyExp = 10

	// waitBaseModifier scales the occupancy-driven base wait (paper's E factor,
	// tuned down 10x for the testbed).
	waitBaseModifier = 0.1

	// waitTimeFloor is the paper's §6 safety floor G, expressed as a real
	// duration: the minimum wait a fresh registrant owes at an empty table (both
	// modifiers ~0). G is derived from it as waitTimeFloor / (waitBaseModifier *
	// AdLifetime) so the floor is this duration regardless of AdLifetime. Set
	// above topicTableWaitTimeFloor (the 1s admission slack) so it actually
	// applies; ~1s effective wait after that slack. Scales up with occupancy.
	waitTimeFloor = 2 * time.Second
)

// If a node has less than this time to wait, they will be accepted anyway.
// Acts as an admission slack absorbing one-way network latency, queueing
// delay, and minor clock drift, so honest registrants whose REGTOPIC arrives
// fractionally before the formula's requirement aren't bounced into a
// retry. Not a security feature; the bucket-slot squat via a huge quote is
// handled on the registrant side in registration.go.
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
	// Drop lower-bound tuples that have fully decayed. They only matter while a
	// registrant is actively waiting, so this keeps the maps from growing
	// unboundedly with churn.
	tab.wt.expireBounds(now)
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
	reg.allElem = tab.all.PushFront(reg)
	tab.wt.addReg(reg)
	return reg
}

func (tab *TopicTable) remove(reg *topicTableEntry) {
	tab.all.Remove(reg.allElem)
	topicList := tab.reg[reg.topic]
	if topicList.Len() == 1 {
		delete(tab.reg, reg.topic)
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
	regCount := tab.all.Len()

	// occupancy is the *inverse* of the table fill-ratio.
	occupancy := 1.0 - (float64(regCount) / float64(tab.config.AdCacheSize))

	// baseTime is the required wait-time, purely based on occupancy. When occupancy is
	// near 1.0 (i.e. the table is empty), baseTime is AdLifetime/10. As the table gets
	// fuller, baseTime goes up and will eventually exceed AdLifetime.
	baseTime := waitBaseModifier * tab.config.AdLifetime.Seconds() / math.Pow(occupancy, occupancyExp)

	// topicMod changes the waiting time based on the ratio of registrations in the
	// requested topic vs. all topics.
	topicMod := float64(tab.topicSize(t)) / float64(regCount+1)

	// ipMod changes the waiting time based on IP address diversity.
	ipMod := tab.wt.ipModifier(n)

	// g is the §6 safety floor, derived so the empty-table floor equals
	// waitTimeFloor independent of AdLifetime (AdLifetime cancels baseTime).
	g := waitTimeFloor.Seconds() / (waitBaseModifier * tab.config.AdLifetime.Seconds())
	neededTime := baseTime * (topicMod + ipMod + g)
	computed := time.Duration(math.Ceil(neededTime * float64(time.Second)))

	// Apply the §6 anti-gaming lower bound: a re-quote can't drop below the
	// prior quote by more than the elapsed time, so an incumbent can't reset its
	// accumulated wait through a brief occupancy dip.
	return tab.wt.lowerBound(n, t, computed, tab.config.Clock.Now())
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

	// Check if the node has waited enough.
	requiredTime := tab.WaitTime(n, t)
	if waitTime < requiredTime {
		remaining := requiredTime - waitTime
		if remaining > topicTableWaitTimeFloor {
			return remaining
		}
	}

	// Check if there is space. If not, the node needs to come back when a slot opens.
	if tab.all.Len() >= tab.config.AdCacheSize {
		now := tab.config.Clock.Now()
		return tab.NextExpiryTime().Sub(now)
	}

	tab.add(n, t)
	return 0
}

// Note about lower bound removal: Lower bound information only needs to be kept for
// active registration topic/id/ip, because only active registrations influence the
// waiting time modifier value. The lower-bound value kept is a tuple of (value,
// timestamp). After time wt has expired (at timestamp+wt), the tuple can be deleted.

// waitTimeState holds the state of waiting time modifier functions.
type waitTimeState struct {
	ipv4 *ipTree
	ipv6 *ipTree

	// Lower-bound anti-gaming state (§6 / spec §2.1.5): per active registrant, the
	// last wait quoted and when. idBounds keys by (topic, id); ipBounds by
	// (topic, IP prefix) so ID rotation from one address can't evade it.
	idBounds map[idBoundKey]waitBound
	ipBounds map[ipBoundKey]waitBound
}

// idBoundKey keys the per-(topic, advertiser id) lower bound.
type idBoundKey struct {
	topic TopicID
	id    enode.ID
}

// ipBoundKey keys the per-(topic, IP-prefix) lower bound. The prefix is stored
// in its canonical string form so v4 and v6 addresses share one map.
type ipBoundKey struct {
	topic TopicID
	ip    string
}

// waitBound is a (value, timestamp) lower-bound tuple.
type waitBound struct {
	value time.Duration
	time  mclock.AbsTime
}

// floor returns the lower bound on the wait time at 'now', decayed by the real
// time elapsed since the bound was recorded: bound - (now - timestamp).
func (b waitBound) floor(now mclock.AbsTime) time.Duration {
	return b.value - time.Duration(now.Sub(b.time))
}

// expired reports whether the bound has fully decayed and can be dropped.
func (b waitBound) expired(now mclock.AbsTime) bool {
	return b.floor(now) <= 0
}

func newWaitTimeState() waitTimeState {
	return waitTimeState{
		ipv4:     newIPTree(32),
		ipv6:     newIPTree(128),
		idBounds: make(map[idBoundKey]waitBound),
		ipBounds: make(map[ipBoundKey]waitBound),
	}
}

// lowerBound raises 'computed' to the highest still-active bound for n's
// (topic, id) and (topic, IP) keys, then records the result as the new bound so
// a later re-quote can't drop faster than real elapsed time.
func (wt *waitTimeState) lowerBound(n *enode.Node, t TopicID, computed time.Duration, now mclock.AbsTime) time.Duration {
	result := computed

	idKey := idBoundKey{topic: t, id: n.ID()}
	if b, ok := wt.idBounds[idKey]; ok {
		if f := b.floor(now); f > result {
			result = f
		}
	}

	ipKeys := wt.ipBoundKeys(t, n)
	for _, k := range ipKeys {
		if b, ok := wt.ipBounds[k]; ok {
			if f := b.floor(now); f > result {
				result = f
			}
		}
	}

	// Record the (possibly raised) result as the new bound.
	nb := waitBound{value: result, time: now}
	wt.idBounds[idKey] = nb
	for _, k := range ipKeys {
		wt.ipBounds[k] = nb
	}
	return result
}

// The per-IP bound aggregates by prefix so it can't be evaded by rotating
// addresses within one allocation: /24 for v4 (matching regBucketSubnet), /64
// for v6 per docs/disc-ng-ipv6-policy.md §3.2 (#53). Transition-address
// re-routing is deferred to #54.
const (
	waitBoundPrefix4 = 24
	waitBoundPrefix6 = 64
)

// ipBoundKeys returns the per-IP lower-bound keys for n, one per address family,
// each aggregated to its prefix.
func (wt *waitTimeState) ipBoundKeys(t TopicID, n *enode.Node) []ipBoundKey {
	var (
		ip4  enr.IPv4
		ip6  enr.IPv6
		keys []ipBoundKey
	)
	if n.Load(&ip4) == nil {
		if k, ok := ipBoundKeyFor(t, net.IP(ip4)); ok {
			keys = append(keys, k)
		}
	}
	if n.Load(&ip6) == nil {
		if k, ok := ipBoundKeyFor(t, net.IP(ip6)); ok {
			keys = append(keys, k)
		}
	}
	return keys
}

// ipBoundKeyFor builds the bound key for ip, aggregated to its family's prefix
// (/24 for v4, /64 for v6). It reports ok=false for LAN or malformed addresses.
func ipBoundKeyFor(t TopicID, ip net.IP) (ipBoundKey, bool) {
	if ip == nil || netutil.IsLAN(ip) {
		return ipBoundKey{}, false
	}
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return ipBoundKey{}, false
	}
	addr = addr.Unmap()
	bits := waitBoundPrefix6
	if addr.Is4() {
		bits = waitBoundPrefix4
	}
	prefix, err := addr.Prefix(bits)
	if err != nil {
		return ipBoundKey{}, false
	}
	return ipBoundKey{topic: t, ip: prefix.String()}, true
}

// expireBounds drops lower-bound tuples that have fully decayed at 'now'.
func (wt *waitTimeState) expireBounds(now mclock.AbsTime) {
	for k, b := range wt.idBounds {
		if b.expired(now) {
			delete(wt.idBounds, k)
		}
	}
	for k, b := range wt.ipBounds {
		if b.expired(now) {
			delete(wt.ipBounds, k)
		}
	}
}

func (wt *waitTimeState) ipModifier(n *enode.Node) float64 {
	var (
		ip4    enr.IPv4
		ip6    enr.IPv6
		score4 float64
		score6 float64
	)
	if n.Load(&ip4) == nil && !netutil.IsLAN(net.IP(ip4)) {
		score4 = wt.ipv4.score(net.IP(ip4))
	}
	if n.Load(&ip6) == nil && !netutil.IsLAN(net.IP(ip6)) {
		score6 = wt.ipv6.score(net.IP(ip6))
	}
	return math.Max(score4, score6)
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
