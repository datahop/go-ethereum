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
	"bytes"
	"fmt"
	"maps"
	mrand "math/rand"
	"net"
	"net/netip"
	"sort"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/internal/testlog"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

var (
	topic1 = TopicID{1, 2, 3, 4, 5, 6}
	topic2 = TopicID{8, 8, 8, 8, 8, 8, 8, 8, 8, 8}
)

func TestTopicTableWait(t *testing.T) {
	cfg := testConfig(t)
	tab := NewTopicTable(cfg)

	n := newNode()
	wt := tab.registerNode(n, topic1, 0)

	t.Log("initial wait time", wt)

	wt2 := tab.registerNode(n, topic1, wt)
	if wt2 != 0 {
		t.Fatal("node not registered after waiting")
	}
}

func TestTopicTableRegisterTwice(t *testing.T) {
	cfg := testConfig(t)
	tab := NewTopicTable(cfg)

	n := newNode()
	tab.addNode(n, topic1)

	wt := tab.registerNode(n, topic1, 0)
	if wt != 0 {
		t.Fatalf("wrong wait time %v for already-registered node", wt)
	}
}

func TestTopicTableRandomNodes(t *testing.T) {
	cfg := testConfig(t)
	tab := NewTopicTable(cfg)

	const N = 20

	var topic1nodes []enode.ID
	for i := 0; i < N; i++ {
		n := newNode()
		ok := tab.addNode(n, topic1)
		if !ok {
			t.Fatalf("can't add node %d", i)
		}
		topic1nodes = append(topic1nodes, n.ID())
	}
	sortIDs(topic1nodes)

	alwaysTrue := func(*enode.Node) bool { return true }

	check := func(t *testing.T, n int, expectedResults int) {
		result := tab.RandomNodes(topic1, n, alwaysTrue)
		ids := uniqueNodeIDs(result)
		sortIDs(ids)

		if len(ids) != len(result) {
			t.Error("results are not unique")
		}
		if len(ids) != expectedResults {
			t.Errorf("wrong number of results: %d, want %d", len(ids), expectedResults)
		}
	}

	t.Run(fmt.Sprint(N), func(t *testing.T) { check(t, N, N) })
	t.Run(fmt.Sprint(N-1), func(t *testing.T) { check(t, N-1, N-1) })
	t.Run(fmt.Sprint(N+1), func(t *testing.T) { check(t, N+1, N) })
}

// TestTopicTableRandomNodesExpired checks that RandomNodes does not return
// ads past their expiry before Expire runs.
func TestTopicTableRandomNodesExpired(t *testing.T) {
	simclock := new(mclock.Simulated)
	cfg := testConfig(t)
	cfg.Clock = simclock
	tab := NewTopicTable(cfg)
	all := func(*enode.Node) bool { return true }

	tab.addNode(newNode(), topic1)
	simclock.Run(tab.AdLifetime() + time.Second)
	if got := tab.RandomNodes(topic1, 10, all); len(got) != 0 {
		t.Fatalf("RandomNodes returned %d expired ads", len(got))
	}

	var old []*enode.Node
	for i := 0; i < 5; i++ {
		n := newNode()
		tab.addNode(n, topic1)
		old = append(old, n)
	}
	simclock.Run(time.Minute)
	live := make(map[enode.ID]bool)
	for i := 0; i < 5; i++ {
		n := newNode()
		tab.addNode(n, topic1)
		live[n.ID()] = true
	}
	simclock.Run(tab.AdLifetime() - time.Second)
	for _, n := range []int{3, 5, 10} {
		got := uniqueNodeIDs(tab.RandomNodes(topic1, n, all))
		want := min(n, len(live))
		if len(got) != want {
			t.Fatalf("RandomNodes(%d) returned %d nodes, want %d", n, len(got), want)
		}
		for _, id := range got {
			if !live[id] {
				t.Fatalf("RandomNodes(%d) returned expired ad %v", n, id)
			}
		}
	}
	for _, n := range old {
		if tab.isRegistered(n, topic1) {
			t.Fatal("expired ad still in the table")
		}
	}
}

// TestTopicTableReregisterExpired checks that a node whose ad has expired, but
// not yet been removed, gets a new ad instead of being reported registered.
func TestTopicTableReregisterExpired(t *testing.T) {
	simclock := new(mclock.Simulated)
	cfg := testConfig(t)
	cfg.Clock = simclock
	tab := NewTopicTable(cfg)

	n := newNode()
	tab.addNode(n, topic1)
	simclock.Run(tab.AdLifetime() + time.Second)
	if wt := tab.registerNode(n, topic1, maxWaitTime); wt != 0 {
		t.Fatalf("Register returned %v, want 0", wt)
	}
	if tab.all.Len() != 1 || !tab.isRegistered(n, topic1) {
		t.Fatalf("table has %d ads, want the new one only", tab.all.Len())
	}
	if got, want := tab.NextExpiryTime(), simclock.Now().Add(tab.AdLifetime()); got != want {
		t.Fatalf("ad expires at %v, want a fresh expiry %v", got, want)
	}
}

// TestTopicTableWaitTimeExpired checks that expired ads do not count toward
// the waiting time before Expire runs.
func TestTopicTableWaitTimeExpired(t *testing.T) {
	simclock := new(mclock.Simulated)
	cfg := testConfig(t)
	cfg.Clock = simclock
	cfg.AdCacheSize = 100
	newExpired := func() *TopicTable {
		tab := NewTopicTable(cfg)
		fillOther(t, tab, 92)
		tab.addNode(nodeWithIP(net.IP{11, 2, 3, 4}), topic1)
		simclock.Run(tab.AdLifetime() + time.Second)
		return tab
	}
	n := nodeWithIP(net.IP{11, 2, 3, 5})

	empty := NewTopicTable(cfg).waitTimeOf(n, topic1)
	if wt := newExpired().waitTimeOf(n, topic1); wt != empty {
		t.Fatalf("WaitTime = %v, want the empty-table quote %v", wt, empty)
	}
	tab := newExpired()
	if wt := tab.registerNode(n, topic1, 0); wt != 0 {
		t.Fatalf("Register returned %v, want 0", wt)
	}
	if tab.all.Len() != 1 {
		t.Fatalf("table has %d ads, want 1", tab.all.Len())
	}
}

// TestLowerBoundDecay checks the decay and max-aggregation semantics of the
// per-component lower bound (§6, "Lower Bound").
func TestLowerBoundDecay(t *testing.T) {
	var lb lowerBound
	t0 := mclock.AbsTime(0)

	if lb.remaining(t0) != 0 {
		t.Fatal("zero-value bound should have no floor")
	}
	if got := lb.bump(10*time.Second, t0); got != 10*time.Second {
		t.Fatalf("bump returned %v, want 10s", got)
	}
	// The floor decays 1:1 with elapsed time.
	t1 := t0.Add(3 * time.Second)
	if got := lb.remaining(t1); got != 7*time.Second {
		t.Fatalf("remaining %v, want 7s", got)
	}
	// A smaller bump must not lower the floor.
	if got := lb.bump(2*time.Second, t1); got != 7*time.Second {
		t.Fatalf("smaller bump changed floor: %v", got)
	}
	// A larger bump raises it and resets the decay origin.
	if got := lb.bump(20*time.Second, t1); got != 20*time.Second {
		t.Fatalf("larger bump returned %v, want 20s", got)
	}
	if got := lb.remaining(t1.Add(5 * time.Second)); got != 15*time.Second {
		t.Fatalf("remaining after raise %v, want 15s", got)
	}
	// Once fully elapsed, no floor remains.
	if got := lb.remaining(t1.Add(30 * time.Second)); got != 0 {
		t.Fatalf("expected fully decayed, got %v", got)
	}
}

// TestTopicTableWaitLowerBound checks that WaitTime floors the service component
// at the topic's recorded lower bound and that the floor decays over time.
func TestTopicTableWaitLowerBound(t *testing.T) {
	simclock := new(mclock.Simulated)
	cfg := testConfig(t)
	cfg.Clock = simclock
	tab := NewTopicTable(cfg)

	n := newNode()
	now := simclock.Now()

	// Inject a service-component lower bound for topic1. With an otherwise-empty
	// table the natural components are ~0, so the wait time must be floored here.
	tab.wt.topicBounds[topic1] = lowerBound{value: 5 * time.Minute, since: now}

	if wt := tab.waitTimeOf(n, topic1); wt < 5*time.Minute {
		t.Fatalf("wait time %v below lower bound of 5m", wt)
	}
	// An unrelated topic is not affected by topic1's bound.
	if wt := tab.waitTimeOf(n, topic2); wt > time.Second {
		t.Fatalf("unrelated topic floored: %v", wt)
	}

	// The floor decays 1:1 with elapsed time.
	simclock.Run(1 * time.Minute)
	if wt := tab.waitTimeOf(n, topic1); wt < 4*time.Minute || wt > 4*time.Minute+time.Second {
		t.Fatalf("decayed wait time %v, want ~4m", wt)
	}

	// After the bound has fully elapsed, no floor remains.
	simclock.Run(5 * time.Minute)
	if wt := tab.waitTimeOf(n, topic1); wt > time.Second {
		t.Fatalf("bound did not decay away: %v", wt)
	}
}

// fillAndQuote fills the cache with topic1 ads and has a fresh node register,
// so that a wait ticket is issued and topic1's lower bound recorded. It returns
// the recorded bound.
func fillAndQuote(t *testing.T, tab *TopicTable, nads int) lowerBound {
	t.Helper()
	for i := 0; i < nads; i++ {
		if !tab.addNode(newNode(), topic1) {
			t.Fatalf("could not add ad %d", i)
		}
	}
	if wt := tab.registerNode(newNode(), topic1, 0); wt <= topicTableWaitTimeFloor {
		t.Fatalf("expected a wait ticket, got %v", wt)
	}
	b, ok := tab.wt.topicBounds[topic1]
	if !ok {
		t.Fatal("no lower bound recorded for topic1 after issuing a wait ticket")
	}
	return b
}

// TestTopicTableWaitBoundGC checks that a topic's lower bound is dropped when
// the topic's last ad expires, so a new advertiser of the topic gets the
// empty-topic quote even while the bound has not decayed.
func TestTopicTableWaitBoundGC(t *testing.T) {
	simclock := new(mclock.Simulated)
	cfg := testConfig(t)
	cfg.Clock = simclock
	tab := NewTopicTable(cfg)

	b := fillAndQuote(t, tab, 16)
	if b.value <= tab.AdLifetime()+time.Second {
		t.Fatalf("bound %v does not outlive the ads", b.value)
	}

	simclock.Run(tab.AdLifetime() + time.Second)
	n := newNode()
	empty := NewTopicTable(cfg).waitTimeOf(n, topic1)
	if wt := tab.waitTimeOf(n, topic1); wt != empty {
		t.Fatalf("WaitTime = %v, want the empty-topic quote %v", wt, empty)
	}
	if _, ok := tab.wt.topicBounds[topic1]; ok {
		t.Fatal("lower bound not dropped after topic emptied")
	}
	if wt := tab.registerNode(n, topic1, 0); wt != 0 {
		t.Fatalf("Register returned %v, want 0", wt)
	}
}

// TestTopicTableRecordsIPBound checks that issuing a wait ticket records an
// IP-component lower bound on the longest-prefix-match node in the IP tree.
func TestTopicTableRecordsIPBound(t *testing.T) {
	simclock := new(mclock.Simulated)
	cfg := testConfig(t)
	cfg.Clock = simclock
	cfg.AdCacheSize = 200
	tab := NewTopicTable(cfg)

	// Fill the cache with ads from a single /24 so that another address in that
	// subnet receives a positive IP-similarity score (a non-zero IP component).
	for i := 1; i <= 60; i++ {
		ip := net.IPv4(203, 0, 113, byte(i))
		if !tab.addNode(nodeWithIP(ip), topic1) {
			t.Fatalf("could not add clustered ad %d", i)
		}
	}

	// A fresh node from the same subnet, registering for an *empty* topic so the
	// service component is zero and only the IP component is exercised.
	target := net.IPv4(203, 0, 113, 200)
	score, node := tab.wt.ipv4.scoreNode(target.To4())
	if score == 0 {
		t.Fatal("expected a positive IP-similarity score for the clustered subnet")
	}
	if node == nil {
		t.Fatal("scoreNode returned no longest-prefix-match node")
	}

	now := simclock.Now()
	if wt := tab.registerNode(nodeWithIP(target), topic2, 0); wt <= topicTableWaitTimeFloor {
		t.Fatalf("expected a wait ticket, got %v", wt)
	}
	if node.bound.remaining(now) <= 0 {
		t.Fatal("Register did not record an IP-component lower bound")
	}
}

// TestTopicTableIPBoundGC checks that IP-component lower bounds are freed
// together with their tree nodes when the ads keeping the branch alive expire.
func TestTopicTableIPBoundGC(t *testing.T) {
	simclock := new(mclock.Simulated)
	cfg := testConfig(t)
	cfg.Clock = simclock
	cfg.AdCacheSize = 200
	tab := NewTopicTable(cfg)

	for i := 1; i <= 60; i++ {
		ip := net.IPv4(203, 0, 113, byte(i))
		if !tab.addNode(nodeWithIP(ip), topic1) {
			t.Fatalf("could not add clustered ad %d", i)
		}
	}
	target := net.IPv4(203, 0, 113, 200)
	if wt := tab.registerNode(nodeWithIP(target), topic2, 0); wt <= topicTableWaitTimeFloor {
		t.Fatalf("expected a wait ticket, got %v", wt)
	}
	if tab.wt.ipv4.pathFloor(target.To4(), simclock.Now()) <= 0 {
		t.Fatal("no IP-component lower bound recorded")
	}

	// Expire all ads: the branch carrying the bound must be freed with them.
	simclock.Run(tab.AdLifetime() + time.Second)
	tab.Expire()
	if c := tab.wt.ipv4.count(); c != 0 {
		t.Fatalf("IP tree not empty after expiry: count %d", c)
	}
	if tab.wt.ipv4.root.left != nil || tab.wt.ipv4.root.right != nil {
		t.Fatal("IP tree branches (and their bounds) not freed after expiry")
	}
}

// TestTopicTableIPBoundDeeperLPM checks that a recorded IP floor is still
// charged after later inserts move the longest-prefix-match node deeper than
// the node the floor was recorded on.
func TestTopicTableIPBoundDeeperLPM(t *testing.T) {
	simclock := new(mclock.Simulated)
	cfg := testConfig(t)
	cfg.Clock = simclock
	cfg.AdCacheSize = 2000
	tab := NewTopicTable(cfg)

	for i := 1; i <= 60; i++ {
		ip := net.IPv4(203, 0, 113, byte(i))
		if !tab.addNode(nodeWithIP(ip), topic1) {
			t.Fatalf("could not add clustered ad %d", i)
		}
	}
	target := net.IPv4(203, 0, 113, 200)
	if wt := tab.registerNode(nodeWithIP(target), topic2, 0); wt <= topicTableWaitTimeFloor {
		t.Fatalf("expected a wait ticket, got %v", wt)
	}
	now := simclock.Now()
	_, boundNode := tab.wt.ipv4.scoreNode(target.To4())
	if boundNode.bound.remaining(now) <= 0 {
		t.Fatal("no bound recorded on the LPM node")
	}

	// Deepen the tree on target's path: ads in the surrounding /23 raise the
	// effective depth, and their inserts create nodes below boundNode.
	for i := 1; i <= 200; i++ {
		ip := net.IPv4(203, 0, 112, byte(i))
		if !tab.addNode(nodeWithIP(ip), topic1) {
			t.Fatalf("could not add filler ad %d", i)
		}
	}
	_, lpm := tab.wt.ipv4.scoreNode(target.To4())
	if lpm == boundNode {
		t.Fatal("setup failed: LPM node did not move deeper")
	}
	if lpm.bound.remaining(now) > 0 {
		t.Fatal("setup failed: new LPM node unexpectedly carries a bound")
	}
	if tab.wt.ipv4.pathFloor(target.To4(), now) <= 0 {
		t.Fatal("recorded floor not found via path walk")
	}

	// Raise the ancestor's floor far above any fresh component: WaitTime must
	// charge it even though the current LPM node carries no bound.
	boundNode.bound.bump(2*time.Hour, now)
	if wt := tab.waitTimeOf(nodeWithIP(target), topic2); wt < 2*time.Hour {
		t.Fatalf("WaitTime %v does not charge the floor on the ancestor node", wt)
	}
}

func TestTopicTableEviction(t *testing.T) {
	simclock := new(mclock.Simulated)
	cfg := testConfig(t)
	cfg.AdCacheSize = 3
	cfg.Clock = simclock
	tab := NewTopicTable(cfg)
	L := tab.AdLifetime()
	t0 := simclock.Now()

	// Two ads with staggered expiry. The table is not full, so any eviction here
	// is driven by expiry time, not by capacity.
	a, b := newNode(), newNode()
	tab.addNode(a, topic1) // A: exp = t0 + L
	simclock.Run(time.Minute)
	tab.addNode(b, topic1) // B: exp = t0 + L + 1m

	// The next expiry must be A's (the soonest), even with free space.
	if got, want := tab.NextExpiryTime(), t0.Add(L); got != want {
		t.Fatalf("NextExpiryTime = %v, want A's expiry %v", got, want)
	}

	// Just before A expires: nothing is evicted (not too early).
	simclock.Run(L - time.Minute - time.Second) // now = t0 + L - 1s
	tab.Expire()
	if got := tab.all.Len(); got != 2 {
		t.Fatalf("evicted before expiry: Len=%d, want 2", got)
	}

	// Just after A expires (B still live): only A is dropped.
	simclock.Run(2 * time.Second) // now = t0 + L + 1s
	tab.Expire()
	if tab.isRegistered(a, topic1) {
		t.Fatal("expired ad A was not evicted")
	}
	if !tab.isRegistered(b, topic1) {
		t.Fatal("live ad B was wrongly evicted")
	}

	// Fill the table (B + two fresh ads), confirm it's full, then let B expire so
	// exactly one slot frees and a new registrant can be admitted.
	tab.addNode(newNode(), topic1)
	tab.addNode(newNode(), topic1)
	if tab.addNode(newNode(), topic1) {
		t.Fatal("table should be full")
	}
	simclock.Run(time.Minute) // now = t0 + L + 1m + 1s : B expired, the two fresh ads live
	tab.Expire()
	if !tab.addNode(newNode(), topic1) {
		t.Fatal("full table did not admit a new ad after eviction")
	}
}

// TestTopicTableFullWait checks that a full table quotes the next ad expiry
// and does not admit a registrant before it.
func TestTopicTableFullWait(t *testing.T) {
	simclock := new(mclock.Simulated)
	cfg := testConfig(t)
	cfg.Clock = simclock
	tab := NewTopicTable(cfg)
	t0 := simclock.Now()

	for i := 0; i < cfg.AdCacheSize; i++ {
		tab.addNode(newNode(), topic1)
	}
	adExp := t0.Add(tab.AdLifetime())
	if got := tab.NextExpiryTime(); got != adExp {
		t.Fatalf("NextExpiryTime = %v, want %v", got, adExp)
	}

	n := newNode()
	if wt, want := tab.registerNode(n, topic1, maxWaitTime), adExp.Sub(simclock.Now()); wt != want {
		t.Fatalf("full table quoted %v, want the next ad expiry in %v", wt, want)
	}

	simclock.Run(5 * time.Minute)
	if wt, want := tab.registerNode(n, topic1, maxWaitTime), adExp.Sub(simclock.Now()); wt != want {
		t.Fatalf("after 5m: quoted %v, want %v", wt, want)
	}
	if tab.isRegistered(n, topic1) || tab.all.Len() != cfg.AdCacheSize {
		t.Fatal("registrant admitted to a full table")
	}
}

// TestTopicTableFullRegisterExpires checks that Register on a full table expires
// an ad that is past its expiry and admits the registrant in its slot.
func TestTopicTableFullRegisterExpires(t *testing.T) {
	simclock := new(mclock.Simulated)
	cfg := testConfig(t)
	cfg.Clock = simclock
	tab := NewTopicTable(cfg)

	old := newNode()
	tab.addNode(old, topic1)
	simclock.Run(time.Minute)
	for i := 1; i < cfg.AdCacheSize; i++ {
		tab.addNode(newNode(), topic1)
	}

	simclock.Run(tab.AdLifetime() - time.Minute)
	n := newNode()
	if wt := tab.registerNode(n, topic1, maxWaitTime); wt != 0 {
		t.Fatalf("Register returned %v, want 0", wt)
	}
	if !tab.isRegistered(n, topic1) {
		t.Fatal("registrant not in the table")
	}
	if tab.isRegistered(old, topic1) {
		t.Fatal("expired ad still in the table")
	}
	if tab.all.Len() != cfg.AdCacheSize {
		t.Fatalf("table has %d ads, want %d", tab.all.Len(), cfg.AdCacheSize)
	}
}

// TestTopicTableWaitTimeCap checks that the wait time stays positive and capped
// at high occupancy, where the uncapped value overflows a Duration.
func TestTopicTableWaitTimeCap(t *testing.T) {
	cfg := testConfig(t)
	cfg.Clock = new(mclock.Simulated)
	cfg.AdCacheSize = 100
	tab := NewTopicTable(cfg)

	var prev time.Duration
	for i := 0; i < 90; i++ {
		tab.addNode(newNode(), topic1)
		wt := tab.waitTimeOf(newNode(), topic1)
		if wt < prev {
			t.Errorf("wait time decreased at %d ads: %v < %v", i+1, wt, prev)
			break
		}
		prev = wt
	}

	n := newNode()
	wt := tab.registerNode(n, topic1, 0)
	if wt <= 0 || wt > maxWaitTime {
		t.Errorf("Register at 90%% fill returned wait %v, want in (0, %v]", wt, maxWaitTime)
	}
	if tab.isRegistered(n, topic1) {
		t.Error("node admitted at 90% fill without waiting")
	}
}

// fillOther adds n topic2 ads from nodes without an IP.
func fillOther(t *testing.T, tab *TopicTable, n int) []*enode.Node {
	t.Helper()
	ads := make([]*enode.Node, n)
	for i := range ads {
		ads[i] = newNode()
		if !tab.addNode(ads[i], topic2) {
			t.Fatalf("could not add ad %d", i)
		}
	}
	return ads
}

// TestTopicTableWaitSafetyLowOccupancy checks that the G term stays below
// topicTableWaitTimeFloor at low occupancy, so a fresh topic is admitted.
func TestTopicTableWaitSafetyLowOccupancy(t *testing.T) {
	for _, fill := range []int{10, 50} {
		cfg := testConfig(t)
		cfg.Clock = new(mclock.Simulated)
		cfg.AdCacheSize = 100
		tab := NewTopicTable(cfg)
		fillOther(t, tab, fill)

		n := newNode()
		if wt := tab.registerNode(n, topic1, 0); wt != 0 {
			t.Errorf("%d%% fill: Register returned %v, want 0", fill, wt)
		}
		if !tab.isRegistered(n, topic1) {
			t.Errorf("%d%% fill: fresh topic not admitted", fill)
		}
	}
}

// TestTopicTableWaitSafetyHighOccupancy checks that the G term alone quotes a
// fresh topic from a score-0 address above the attempt budget at high occupancy.
func TestTopicTableWaitSafetyHighOccupancy(t *testing.T) {
	for _, fill := range []int{86, 93} {
		cfg := testConfig(t)
		cfg.Clock = new(mclock.Simulated)
		cfg.AdCacheSize = 100
		tab := NewTopicTable(cfg)
		fillOther(t, tab, fill)

		n := newNode()
		budget := 3 * tab.AdLifetime() / 2
		if wt := tab.registerNode(n, topic1, 0); wt <= budget || wt > maxWaitTime {
			t.Errorf("%d%% fill: Register returned %v, want in (%v, %v]", fill, wt, budget, maxWaitTime)
		}
		if tab.isRegistered(n, topic1) {
			t.Errorf("%d%% fill: fresh topic admitted", fill)
		}
	}
}

// TestTopicTableWaitSafetyFull checks that a full table quotes maxWaitTime both
// to a fresh topic from a score-0 address and to an existing topic from a
// similar IP, rather than the 0 that 0*Inf would give.
func TestTopicTableWaitSafetyFull(t *testing.T) {
	cfg := testConfig(t)
	cfg.Clock = new(mclock.Simulated)
	tab := NewTopicTable(cfg)
	for i := 0; i < cfg.AdCacheSize; i++ {
		tab.addNode(nodeWithIP(net.IP{11, 2, 3, byte(i + 1)}), topic1)
	}

	if wt := tab.registerNode(newNode(), topic2, 0); wt != maxWaitTime {
		t.Errorf("fresh topic: Register returned %v, want %v", wt, maxWaitTime)
	}
	if wt := tab.registerNode(nodeWithIP(net.IP{11, 2, 3, 200}), topic1, 0); wt != maxWaitTime {
		t.Errorf("existing topic, similar IP: Register returned %v, want %v", wt, maxWaitTime)
	}
}

// TestTopicTableWaitSafetyNotBounded checks that the G term is not recorded as
// a lower bound: once the cache drains, the quote drops with occupancy.
func TestTopicTableWaitSafetyNotBounded(t *testing.T) {
	cfg := testConfig(t)
	cfg.Clock = new(mclock.Simulated)
	cfg.AdCacheSize = 100
	tab := NewTopicTable(cfg)
	ads := fillOther(t, tab, 93)

	n := newNode()
	if wt := tab.registerNode(n, topic1, 0); wt <= tab.AdLifetime() {
		t.Fatalf("93%% fill: Register returned %v, want above %v", wt, tab.AdLifetime())
	}
	if _, ok := tab.wt.topicBounds[topic1]; ok {
		t.Fatal("G term recorded as a topic lower bound")
	}

	for _, ad := range ads[50:] {
		tab.RemoveNode(ad.ID())
	}
	if wt := tab.registerNode(n, topic1, 0); wt != 0 {
		t.Fatalf("after draining to 50%%: Register returned %v, want 0", wt)
	}
}

// TestTopicTableCountsSourceOnly checks that an unverified address of the other
// family in a record is neither inserted nor scored, and that the signed record
// is stored as received.
func TestTopicTableCountsSourceOnly(t *testing.T) {
	tests := []struct {
		name  string
		crowd func(byte) netip.Addr // family of the unverified address
		src   netip.Addr
	}{
		{"ipv6 source", testAddr4, testAddr6(200)},
		{"ipv4 source", testAddr6, testAddr4(200)},
		{"mapped ipv4 source", testAddr6, netip.AddrFrom16(testAddr4(200).As16())},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.Clock = new(mclock.Simulated)
			cfg.AdCacheSize = 200
			tab := NewTopicTable(cfg)

			// The quote to the prefix's holder records an IP floor on its path.
			for i := byte(1); i <= 60; i++ {
				tab.addNode(nodeWithAddrs(test.crowd(i)), topic1)
			}
			claimed := test.crowd(200)
			if wt := tab.registerNode(nodeWithAddrs(claimed), topic2, 0); wt <= topicTableWaitTimeFloor {
				t.Fatalf("expected a wait ticket, got %v", wt)
			}

			forged := nodeWithAddrs(test.src, claimed)
			honest := nodeWithAddrs(test.src)
			if got, want := tab.WaitTime(forged, test.src, topic2), tab.WaitTime(honest, test.src, topic2); got != want {
				t.Fatalf("quote with unverified address %v, want %v", got, want)
			}

			other := tab.wt.ipTreeOf(claimed)
			before := ipTreeState(other)
			if wt := tab.Register(forged, test.src, topic2, maxWaitTime); wt != 0 {
				t.Fatalf("Register returned %v, want 0", wt)
			}
			if after := ipTreeState(other); !maps.Equal(after, before) {
				t.Fatalf("other family's tree changed: %v, want %v", after, before)
			}
			if c := tab.wt.ipTreeOf(test.src.Unmap()).count(); c != 1 {
				t.Fatalf("source family's tree has %d addresses, want 1", c)
			}
			if got := tab.Nodes(topic2); len(got) != 1 || got[0] != forged {
				t.Fatalf("stored nodes %v, want the record as received", got)
			}
		})
	}
}

// removalTests are registrations that removal has to undo exactly.
var removalTests = []struct {
	name     string
	ip4, ip6 netip.Addr
	src      netip.Addr
}{
	{"ipv4 record", testAddr4(200), netip.Addr{}, testAddr4(200)},
	{"ipv6 record", netip.Addr{}, testAddr6(200), testAddr6(200)},
	{"dual record over ipv4", testAddr4(200), testAddr6(200), testAddr4(200)},
	{"dual record over ipv6", testAddr4(200), testAddr6(200), testAddr6(200)},
	{"dual record over mapped ipv4", testAddr4(200), testAddr6(200), netip.AddrFrom16(testAddr4(200).As16())},
}

// TestTopicTableRemoveRestoresTrees checks that removing a registration undoes
// its insertion, whether the tree has grown or shrunk since then, so that the
// insertion depth differs from what a removal would compute.
func TestTopicTableRemoveRestoresTrees(t *testing.T) {
	for _, test := range removalTests {
		for _, first := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/first=%v", test.name, first), func(t *testing.T) {
				cfg := testConfig(t)
				cfg.AdCacheSize = 400
				tab := NewTopicTable(cfg)
				fillBothFamilies(t, tab, 1, 8)
				before4, before6 := ipTreeState(tab.wt.ipv4), ipTreeState(tab.wt.ipv6)

				n := nodeWithAddrs(test.ip4, test.ip6)
				var later []*enode.Node
				if !first {
					later = fillBothFamilies(t, tab, 100, 56)
				}
				if !tab.Add(n, test.src, topic1) {
					t.Fatal("could not add ad")
				}
				if first {
					later = fillBothFamilies(t, tab, 100, 56)
				}

				order := append([]*enode.Node{n}, later...)
				if !first {
					order = append(later, n)
				}
				for _, ad := range order {
					tab.RemoveNode(ad.ID())
					checkTableTrees(t, tab)
				}
				if after := ipTreeState(tab.wt.ipv4); !maps.Equal(after, before4) {
					t.Fatalf("ipv4 tree %v, want %v", after, before4)
				}
				if after := ipTreeState(tab.wt.ipv6); !maps.Equal(after, before6) {
					t.Fatalf("ipv6 tree %v, want %v", after, before6)
				}
			})
		}
	}
}

// TestTopicTableExpiryRestoresTrees is TestTopicTableRemoveRestoresTrees for ads
// leaving through expiry.
func TestTopicTableExpiryRestoresTrees(t *testing.T) {
	for _, test := range removalTests {
		t.Run(test.name, func(t *testing.T) {
			simclock := new(mclock.Simulated)
			cfg := testConfig(t)
			cfg.Clock = simclock
			cfg.AdCacheSize = 400
			tab := NewTopicTable(cfg)

			fillBothFamilies(t, tab, 1, 8)
			simclock.Run(time.Minute)
			n := nodeWithAddrs(test.ip4, test.ip6)
			if !tab.Add(n, test.src, topic1) {
				t.Fatal("could not add ad")
			}
			simclock.Run(time.Minute)
			later := fillBothFamilies(t, tab, 100, 56)

			// Only the ads added after n outlive it.
			simclock.Run(tab.AdLifetime() - time.Minute + time.Second)
			tab.Expire()
			if tab.isRegistered(n, topic1) || !tab.isRegistered(later[0], topic1) {
				t.Fatal("setup failed: wrong ads expired")
			}
			checkTableTrees(t, tab)
			for _, ad := range later {
				tab.RemoveNode(ad.ID())
				checkTableTrees(t, tab)
			}
			empty := map[string]int{"": 0}
			if after := ipTreeState(tab.wt.ipv4); !maps.Equal(after, empty) {
				t.Fatalf("ipv4 tree %v, want it empty", after)
			}
			if after := ipTreeState(tab.wt.ipv6); !maps.Equal(after, empty) {
				t.Fatalf("ipv6 tree %v, want it empty", after)
			}
		})
	}
}

// TestTopicTableBoundOnSourcePath checks that a quote records the IP lower bound
// only on the path of the source address.
func TestTopicTableBoundOnSourcePath(t *testing.T) {
	simclock := new(mclock.Simulated)
	cfg := testConfig(t)
	cfg.Clock = simclock
	cfg.AdCacheSize = 200
	tab := NewTopicTable(cfg)
	for i := byte(1); i <= 60; i++ {
		tab.addNode(nodeWithAddrs(testAddr4(i)), topic1)
		tab.addNode(nodeWithAddrs(testAddr6(i)), topic1)
	}

	claimed, src := testAddr4(200), testAddr6(200)
	if wt := tab.Register(nodeWithAddrs(claimed, src), src, topic2, 0); wt <= topicTableWaitTimeFloor {
		t.Fatalf("expected a wait ticket, got %v", wt)
	}
	now := simclock.Now()
	if tab.wt.ipv6.pathFloor(src.AsSlice(), now) <= 0 {
		t.Fatal("no lower bound on the source address's path")
	}
	if f := tab.wt.ipv4.pathFloor(claimed.AsSlice(), now); f != 0 {
		t.Fatalf("lower bound %v on the unverified address's path", f)
	}
}

// TestTopicTableDualFamilyCount checks that a registrant holding both families
// is counted only in the family it registered over.
func TestTopicTableDualFamilyCount(t *testing.T) {
	ip4, ip6 := testAddr4(200), testAddr6(200)
	tests := []struct {
		name         string
		src          netip.Addr
		want4, want6 int
	}{
		{"over ipv4", ip4, 1, 0},
		{"over mapped ipv4", netip.AddrFrom16(ip4.As16()), 1, 0},
		{"over ipv6", ip6, 0, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tab := NewTopicTable(testConfig(t))
			if wt := tab.Register(nodeWithAddrs(ip4, ip6), test.src, topic1, maxWaitTime); wt != 0 {
				t.Fatalf("Register returned %v, want 0", wt)
			}
			if c4, c6 := tab.wt.ipv4.count(), tab.wt.ipv6.count(); c4 != test.want4 || c6 != test.want6 {
				t.Fatalf("counts ipv4 %d, ipv6 %d, want %d and %d", c4, c6, test.want4, test.want6)
			}
		})
	}
}

// TestTopicTableMappedSourceQuote checks that an IPv4-mapped IPv6 source is
// quoted from the IPv4 tree, like its plain form.
func TestTopicTableMappedSourceQuote(t *testing.T) {
	cfg := testConfig(t)
	cfg.Clock = new(mclock.Simulated)
	cfg.AdCacheSize = 200
	tab := NewTopicTable(cfg)
	for i := byte(1); i <= 60; i++ {
		tab.addNode(nodeWithAddrs(testAddr4(i)), topic1)
	}

	plain := testAddr4(200)
	mapped := netip.AddrFrom16(plain.As16())
	n := nodeWithAddrs(plain)
	quote := tab.WaitTime(n, plain, topic2)
	if got := tab.WaitTime(n, mapped, topic2); got != quote {
		t.Fatalf("mapped source quoted %v, want the plain quote %v", got, quote)
	}
	uncrowded := netip.MustParseAddr("200.9.9.9")
	if base := tab.WaitTime(nodeWithAddrs(uncrowded), uncrowded, topic2); quote <= base {
		t.Fatalf("crowded quote %v not above the uncrowded quote %v", quote, base)
	}
}

// TestTopicTableLocalSourceNotScored checks that local addresses are counted in
// the tree but not scored.
func TestTopicTableLocalSourceNotScored(t *testing.T) {
	tests := []struct {
		name  string
		addr  func(byte) netip.Addr
		count func(*TopicTable) int
	}{
		{"lan", func(i byte) netip.Addr { return netip.AddrFrom4([4]byte{10, 0, 0, i}) }, ipv4Count},
		{"loopback", func(i byte) netip.Addr { return netip.AddrFrom4([4]byte{127, 0, 0, i}) }, ipv4Count},
		{"link-local", func(i byte) netip.Addr { return netip.AddrFrom4([4]byte{169, 254, 0, i}) }, ipv4Count},
		{"ipv6 link-local", func(i byte) netip.Addr { return netip.AddrFrom16([16]byte{0xfe, 0x80, 15: i}) }, ipv6Count},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.Clock = new(mclock.Simulated)
			cfg.AdCacheSize = 200
			tab := NewTopicTable(cfg)
			for i := byte(1); i <= 60; i++ {
				tab.addNode(nodeWithAddrs(test.addr(i)), topic1)
			}
			if c := test.count(tab); c != 60 {
				t.Fatalf("tree counts %d addresses, want 60", c)
			}
			if got, want := tab.waitTimeOf(nodeWithAddrs(test.addr(200)), topic2), tab.waitTimeOf(newNode(), topic2); got != want {
				t.Fatalf("local address quoted %v, want the unscored quote %v", got, want)
			}
		})
	}
}

func ipv4Count(tab *TopicTable) int { return tab.wt.ipv4.count() }
func ipv6Count(tab *TopicTable) int { return tab.wt.ipv6.count() }

// fillBothFamilies adds n single-family ads in each family, from addresses
// starting at index from.
func fillBothFamilies(t *testing.T, tab *TopicTable, from, n int) []*enode.Node {
	t.Helper()
	ads := make([]*enode.Node, 0, 2*n)
	for i := from; i < from+n; i++ {
		for _, addr := range []netip.Addr{testAddr4(byte(i)), testAddr6(byte(i))} {
			ad := nodeWithAddrs(addr)
			if !tab.addNode(ad, topic1) {
				t.Fatalf("could not add ad %v", addr)
			}
			ads = append(ads, ad)
		}
	}
	return ads
}

// checkTableTrees verifies that the trees hold exactly the table's live ads.
func checkTableTrees(t *testing.T, tab *TopicTable) {
	t.Helper()
	var entries4, entries6 []ipTreeEntry
	for e := tab.all.Front(); e != nil; e = e.Next() {
		reg := e.Value.(*topicTableEntry)
		entry := ipTreeEntry{ip: reg.src.AsSlice(), levels: reg.ipLevels}
		if reg.src.Is4() {
			entries4 = append(entries4, entry)
		} else {
			entries6 = append(entries6, entry)
		}
	}
	checkIPTreeCounts(t, tab.wt.ipv4, entries4)
	checkIPTreeCounts(t, tab.wt.ipv6, entries6)
}

// ipTreeState returns the counter of every node in the tree by prefix.
func ipTreeState(it *ipTree) map[string]int {
	state := make(map[string]int)
	var walk func(n *ipTreeNode, prefix string)
	walk = func(n *ipTreeNode, prefix string) {
		if n != nil {
			state[prefix] = n.counter
			walk(n.left, prefix+"1")
			walk(n.right, prefix+"0")
		}
	}
	walk(it.root, "")
	return state
}

func testAddr4(i byte) netip.Addr { return netip.AddrFrom4([4]byte{11, 2, 3, i}) }
func testAddr6(i byte) netip.Addr { return netip.AddrFrom16([16]byte{0x2a, 0x01, 15: i}) }

func testConfig(t *testing.T) Config {
	return Config{
		AdCacheSize: 20,
		Log:         testlog.Logger(t, log.LvlTrace),
	}
}

func newNode() *enode.Node {
	var r enr.Record
	var id enode.ID
	mrand.Read(id[:])
	return enode.SignNull(&r, id)
}

// nodeWithAddrs returns a node whose record holds the given addresses.
func nodeWithAddrs(addrs ...netip.Addr) *enode.Node {
	var r enr.Record
	for _, addr := range addrs {
		switch {
		case addr.Is4():
			r.Set(enr.IPv4Addr(addr))
		case addr.Is6():
			r.Set(enr.IPv6Addr(addr))
		}
	}
	var id enode.ID
	mrand.Read(id[:])
	return enode.SignNull(&r, id)
}

func nodeWithIP(ip net.IP) *enode.Node {
	var r enr.Record
	r.Set(enr.IP(ip))
	var id enode.ID
	mrand.Read(id[:])
	return enode.SignNull(&r, id)
}

// addNode, registerNode and waitTimeOf use the record's own address as the
// packet source.
func (tab *TopicTable) addNode(n *enode.Node, topic TopicID) bool {
	return tab.Add(n, n.IPAddr(), topic)
}

func (tab *TopicTable) registerNode(n *enode.Node, topic TopicID, wait time.Duration) time.Duration {
	return tab.Register(n, n.IPAddr(), topic, wait)
}

func (tab *TopicTable) waitTimeOf(n *enode.Node, topic TopicID) time.Duration {
	return tab.WaitTime(n, n.IPAddr(), topic)
}

func uniqueNodeIDs(nodes []*enode.Node) []enode.ID {
	byID := make(map[enode.ID]struct{}, len(nodes))
	for _, n := range nodes {
		byID[n.ID()] = struct{}{}
	}
	ids := make([]enode.ID, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	return ids
}

func sortIDs(ids []enode.ID) {
	sort.Slice(ids, func(i, j int) bool {
		return bytes.Compare(ids[i][:], ids[j][:]) < 0
	})
}
