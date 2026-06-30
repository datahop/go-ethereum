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

// TestTopicTableWaitTimeLowerBound verifies the paper §6 / spec §2.1.5
// anti-gaming lower bound: once a wait time has been quoted to a registrant, a
// later re-quote for the same (topic, id) cannot drop faster than real elapsed
// time, i.e. w(t2) >= w(t1) - (t2 - t1). Without the lower bound, a registrant
// that re-requests through a momentary occupancy dip is re-quoted the (much
// lower) instantaneous wait, which is exactly the incumbent lock-in the bound
// is meant to prevent.
func TestTopicTableWaitTimeLowerBound(t *testing.T) {
	clock := new(mclock.Simulated)
	cfg := Config{
		AdCacheSize: 100,
		AdLifetime:  30 * time.Second,
		Clock:       clock,
		Log:         testlog.Logger(t, log.LvlTrace),
	}
	tab := NewTopicTable(cfg)

	// Load the table with same-topic registrations so the computed wait for an
	// outsider is large (high occupancy + topic-similarity modifier).
	const fillers = 50
	for i := 0; i < fillers; i++ {
		n := nodeAtDistance(enode.ID(topic1), 200, intIP(i+1))
		if !tab.Add(n, topic1) {
			t.Fatalf("can't add filler node %d", i)
		}
	}

	// Quote an outsider. This records the lower bound for (topic1, n.ID()).
	n := nodeAtDistance(enode.ID(topic1), 100, net.IP{203, 0, 113, 1})
	w1 := tab.WaitTime(n, topic1)
	if w1 <= cfg.AdLifetime {
		t.Fatalf("test setup: expected a large initial wait, got %v", w1)
	}
	t.Log("initial wait", w1)

	// Advance to the filler lifetime and expire them. The table is now empty, so
	// the *instantaneous* computed wait collapses toward zero — this is the
	// occupancy dip an incumbent would exploit to reset its accumulated wait.
	clock.Run(cfg.AdLifetime)
	tab.Expire()
	if tab.all.Len() != 0 {
		t.Fatalf("expected empty table after expiry, got %d entries", tab.all.Len())
	}

	// A genuinely fresh outsider now gets a near-zero wait (nothing to lower-bound).
	fresh := nodeAtDistance(enode.ID(topic1), 100, net.IP{203, 0, 113, 2})
	if wf := tab.WaitTime(fresh, topic1); wf > time.Second {
		t.Fatalf("fresh node should see a tiny wait on an empty table, got %v", wf)
	}

	// The previously-quoted node must NOT be reset to that tiny wait. Its quote
	// may only have decayed by the elapsed time (AdLifetime).
	w2 := tab.WaitTime(n, topic1)
	t.Log("re-quoted wait after dip", w2)
	if lb := w1 - cfg.AdLifetime; w2 < lb {
		t.Fatalf("lower bound violated: w2=%v < w1-elapsed=%v", w2, lb)
	}
	if w2 <= time.Second {
		t.Fatalf("incumbent reset to a tiny wait through the occupancy dip: %v", w2)
	}

	// Decay to completion: once timestamp+value has passed, the bound is dropped
	// and the node is quoted the (now tiny) instantaneous wait again.
	clock.Run(w2 + time.Second)
	tab.Expire()
	if got := len(tab.wt.idBounds) + len(tab.wt.ipBounds); got != 0 {
		t.Fatalf("expected all lower-bound tuples expired, got %d", got)
	}
	if w3 := tab.WaitTime(n, topic1); w3 > time.Second {
		t.Fatalf("expected tiny wait after the bound fully decayed, got %v", w3)
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
