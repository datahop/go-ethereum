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
	mrand "math/rand"
	"net"
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
	wt := tab.Register(n, topic1, 0)

	t.Log("initial wait time", wt)

	wt2 := tab.Register(n, topic1, wt)
	if wt2 != 0 {
		t.Fatal("node not registered after waiting")
	}
}

func TestTopicTableRegisterTwice(t *testing.T) {
	cfg := testConfig(t)
	tab := NewTopicTable(cfg)

	n := newNode()
	tab.Add(n, topic1)

	wt := tab.Register(n, topic1, 0)
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
		ok := tab.Add(n, topic1)
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

	if wt := tab.WaitTime(n, topic1); wt < 5*time.Minute {
		t.Fatalf("wait time %v below lower bound of 5m", wt)
	}
	// An unrelated topic is not affected by topic1's bound.
	if wt := tab.WaitTime(n, topic2); wt > time.Second {
		t.Fatalf("unrelated topic floored: %v", wt)
	}

	// The floor decays 1:1 with elapsed time.
	simclock.Run(1 * time.Minute)
	if wt := tab.WaitTime(n, topic1); wt < 4*time.Minute || wt > 4*time.Minute+time.Second {
		t.Fatalf("decayed wait time %v, want ~4m", wt)
	}

	// After the bound has fully elapsed, no floor remains.
	simclock.Run(5 * time.Minute)
	if wt := tab.WaitTime(n, topic1); wt > time.Second {
		t.Fatalf("bound did not decay away: %v", wt)
	}
}

// TestTopicTableWaitBoundGC checks that a topic's lower bound is recorded when a
// wait ticket is issued and dropped when the topic's last ad leaves the cache.
func TestTopicTableWaitBoundGC(t *testing.T) {
	simclock := new(mclock.Simulated)
	cfg := testConfig(t)
	cfg.Clock = simclock
	tab := NewTopicTable(cfg)

	// Fill the cache with topic1 ads so the service component is large and
	// Register issues a wait ticket instead of admitting immediately.
	for i := 0; i < 16; i++ {
		if !tab.Add(newNode(), topic1) {
			t.Fatalf("could not add ad %d", i)
		}
	}

	m := newNode()
	if wt := tab.Register(m, topic1, 0); wt <= topicTableWaitTimeFloor {
		t.Fatalf("expected a wait ticket, got %v", wt)
	}
	if _, ok := tab.wt.topicBounds[topic1]; !ok {
		t.Fatal("no lower bound recorded for topic1 after issuing a wait ticket")
	}

	// When all topic1 ads expire, the bound must be GC'd. Note: testConfig leaves
	// AdLifetime unset, so read the effective value from the table.
	simclock.Run(tab.AdLifetime() + time.Second)
	tab.Expire()
	if _, ok := tab.wt.topicBounds[topic1]; ok {
		t.Fatal("lower bound not dropped after topic emptied")
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
		if !tab.Add(nodeWithIP(ip), topic1) {
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
	if wt := tab.Register(nodeWithIP(target), topic2, 0); wt <= topicTableWaitTimeFloor {
		t.Fatalf("expected a wait ticket, got %v", wt)
	}
	if node.bound.remaining(now) <= 0 {
		t.Fatal("Register did not record an IP-component lower bound")
	}
}

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

func nodeWithIP(ip net.IP) *enode.Node {
	var r enr.Record
	r.Set(enr.IP(ip))
	var id enode.ID
	mrand.Read(id[:])
	return enode.SignNull(&r, id)
}

func randomNodes(n int) []*enode.Node {
	nodes := make([]*enode.Node, n)
	for i := range nodes {
		nodes[i] = newNode()
	}
	return nodes
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
