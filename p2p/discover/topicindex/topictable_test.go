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

	// A genuinely fresh outsider now gets only the small G floor (nothing to
	// lower-bound). It must be in a different /24 than n, since the bound
	// aggregates per /24.
	fresh := nodeAtDistance(enode.ID(topic1), 100, net.IP{198, 51, 100, 2})
	if wf := tab.WaitTime(fresh, topic1); wf > 5*time.Second {
		t.Fatalf("fresh node should see only the small floor on an empty table, got %v", wf)
	}

	// The previously-quoted node must NOT be reset to the floor. Its quote may
	// only have decayed by the elapsed time (AdLifetime).
	w2 := tab.WaitTime(n, topic1)
	t.Log("re-quoted wait after dip", w2)
	if lb := w1 - cfg.AdLifetime; w2 < lb {
		t.Fatalf("lower bound violated: w2=%v < w1-elapsed=%v", w2, lb)
	}
	if w2 <= 5*time.Second {
		t.Fatalf("incumbent reset to the floor through the occupancy dip: %v", w2)
	}

	// Decay to completion: once timestamp+value has passed, the bound is dropped
	// and the node is quoted only the small floor again.
	clock.Run(w2 + time.Second)
	tab.Expire()
	if got := len(tab.wt.idBounds) + len(tab.wt.ipBounds); got != 0 {
		t.Fatalf("expected all lower-bound tuples expired, got %d", got)
	}
	if w3 := tab.WaitTime(n, topic1); w3 > 5*time.Second {
		t.Fatalf("expected only the small floor after the bound fully decayed, got %v", w3)
	}
}

// TestTopicTableWaitTimeBoundPrefix verifies that the per-IP lower bound
// aggregates by prefix — /24 for IPv4, /64 for IPv6 — so it can't be evaded by
// rotating addresses within one allocation, while a different prefix stays
// independent. Each node gets a distinct random id, so the only thing that can
// carry the bound from one node to another is a shared IP prefix.
func TestTopicTableWaitTimeBoundPrefix(t *testing.T) {
	check := func(name string, first, samePrefix, otherPrefix net.IP) {
		t.Run(name, func(t *testing.T) {
			clock := new(mclock.Simulated)
			cfg := Config{
				AdCacheSize: 100,
				AdLifetime:  30 * time.Second,
				Clock:       clock,
				Log:         testlog.Logger(t, log.LvlTrace),
			}
			tab := NewTopicTable(cfg)
			for i := 0; i < 50; i++ {
				if !tab.Add(nodeAtDistance(enode.ID(topic1), 200, intIP(i+1)), topic1) {
					t.Fatalf("can't add filler %d", i)
				}
			}

			// Quote `first` to record a bound for its prefix, then drain the table
			// so the instantaneous computed wait collapses to ~0.
			w1 := tab.WaitTime(nodeAtDistance(enode.ID(topic1), 100, first), topic1)
			if w1 <= cfg.AdLifetime {
				t.Fatalf("setup: expected a large initial wait, got %v", w1)
			}
			clock.Run(cfg.AdLifetime)
			tab.Expire()

			// A node in the same prefix inherits the still-active bound.
			if w := tab.WaitTime(nodeAtDistance(enode.ID(topic1), 100, samePrefix), topic1); w <= 5*time.Second {
				t.Errorf("same-prefix node was not bounded: got %v", w)
			}
			// A node in a different prefix is unaffected (only the small floor).
			if w := tab.WaitTime(nodeAtDistance(enode.ID(topic1), 100, otherPrefix), topic1); w > 5*time.Second {
				t.Errorf("different-prefix node was bounded: got %v", w)
			}
		})
	}

	check("ipv4",
		net.IP{203, 0, 113, 1}, net.IP{203, 0, 113, 9}, net.IP{198, 51, 100, 9})
	check("ipv6",
		net.ParseIP("2001:db8:1:1::1"), net.ParseIP("2001:db8:1:1::9"), net.ParseIP("2001:db8:2:2::9"))
}

// TestTopicTableWaitTimeFloor checks the paper §6 safety floor G: a fresh
// registrant on an empty table (both modifiers ~0) owes ~waitTimeFloor, above
// the admission slack, and — since G is derived as waitTimeFloor/(baseMod*
// AdLifetime) — the floor is that duration independent of AdLifetime.
func TestTopicTableWaitTimeFloor(t *testing.T) {
	for _, adLifetime := range []time.Duration{15 * time.Minute, time.Hour, 30 * time.Second} {
		cfg := Config{
			AdCacheSize: 100,
			AdLifetime:  adLifetime,
			Clock:       new(mclock.Simulated),
			Log:         testlog.Logger(t, log.LvlTrace),
		}
		tab := NewTopicTable(cfg)

		n := nodeAtDistance(enode.ID(topic1), 100, net.IP{203, 0, 113, 1})
		w := tab.WaitTime(n, topic1)
		t.Logf("adLifetime=%v floor wait=%v", adLifetime, w)
		if w <= topicTableWaitTimeFloor {
			t.Fatalf("adLifetime=%v: fresh registrant wait %v does not exceed the admission slack %v",
				adLifetime, w, topicTableWaitTimeFloor)
		}
		// AdLifetime-independent: the empty-table floor is ~waitTimeFloor.
		if w < waitTimeFloor || w > waitTimeFloor+time.Second {
			t.Fatalf("adLifetime=%v: floor %v not ~waitTimeFloor %v", adLifetime, w, waitTimeFloor)
		}
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
	tab.Add(a, topic1) // A: exp = t0 + L
	simclock.Run(time.Minute)
	tab.Add(b, topic1) // B: exp = t0 + L + 1m

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
	tab.Add(newNode(), topic1)
	tab.Add(newNode(), topic1)
	if tab.Add(newNode(), topic1) {
		t.Fatal("table should be full")
	}
	simclock.Run(time.Minute) // now = t0 + L + 1m + 1s : B expired, the two fresh ads live
	tab.Expire()
	if !tab.Add(newNode(), topic1) {
		t.Fatal("full table did not admit a new ad after eviction")
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
