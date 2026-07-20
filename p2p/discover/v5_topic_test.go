// Copyright 2024 The go-ethereum Authors
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

package discover

import (
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/discover/v5wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

var testTopic1 = topicindex.TopicID{1, 1, 1, 1}

func makeTopic(name string) topicindex.TopicID {
	var topic topicindex.TopicID
	copy(topic[:], []byte(name))
	return topic
}

func TestTopicReg(t *testing.T) {
	bootnode := startLocalhostV5(t, Config{})
	defer bootnode.Close()
	client := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{bootnode.Self()}})
	defer client.Close()

	client.RegisterTopic(topicindex.TopicID{}, 0)

	deadline := time.After(15 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("client not registered on bootnode within deadline")
		case <-tick.C:
			reg := bootnode.LocalTopicNodes(topicindex.TopicID{})
			if len(reg) == 1 && reg[0].ID() == client.Self().ID() {
				return
			}
		}
	}
}

func TestTopicSearch(t *testing.T) {
	node0 := startLocalhostV5(t, Config{})
	node1 := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{node0.Self()}})
	node2 := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{node0.Self()}})
	node3 := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{node0.Self()}})
	defer func() {
		for _, n := range []*UDPv5{node0, node1, node2, node3} {
			n.Close()
		}
	}()

	// Seed node1's topic table with the registrants. The topic table is
	// owned by node1's dispatch goroutine, so the writes must be performed
	// there (via onDispatchCh) rather than directly from the test goroutine,
	// which would race with the dispatch loop's NextExpiryTime read.
	seedTopicTable(t, node1, testTopic1, node0.Self(), node3.Self())

	it := node2.TopicSearch(testTopic1, 0)
	defer it.Close()
	found := enode.ReadNodes(it, 2)
	sortByID(found)

	want := []*enode.Node{node0.Self(), node3.Self()}
	sortByID(want)
	if err := checkNodesEqual(found, want); err != nil {
		t.Error(err)
	}
}

// seedTopicTable registers the given nodes for a topic in t's local topic
// table. The work runs on the dispatch goroutine, which owns the table.
func seedTopicTable(t *testing.T, node *UDPv5, topic topicindex.TopicID, regs ...*enode.Node) {
	t.Helper()
	done := make(chan struct{})
	fn := func() {
		for _, n := range regs {
			node.topicTable.Add(n, topic)
		}
		close(done)
	}
	select {
	case node.onDispatchCh <- fn:
		<-done
	case <-node.closeCtx.Done():
		t.Fatal("node closed before topic table could be seeded")
	}
}

// TestTopicStopRegister verifies that StopRegisterTopic stops the
// registration goroutines cleanly.
func TestTopicStopRegister(t *testing.T) {
	t.Parallel()

	node := startLocalhostV5(t, Config{})
	defer node.Close()

	topic := makeTopic("test-stop-reg-0000000")

	// Register then stop.
	node.RegisterTopic(topic, 1)
	time.Sleep(100 * time.Millisecond)
	node.StopRegisterTopic(topic)

	// Registering again should not panic.
	node.RegisterTopic(topic, 2)
	time.Sleep(100 * time.Millisecond)
	node.StopRegisterTopic(topic)
}

// TestTopicSearchIteratorClose verifies that closing the search iterator
// doesn't leak goroutines.
func TestTopicSearchIteratorClose(t *testing.T) {
	t.Parallel()

	const numNodes = 4
	nodes := make([]*UDPv5, numNodes)
	for i := 0; i < numNodes; i++ {
		var cfg Config
		if i > 0 {
			cfg.Bootnodes = []*enode.Node{nodes[0].Self()}
		}
		nodes[i] = startLocalhostV5(t, cfg)
		defer nodes[i].Close()
	}

	topic := makeTopic("test-iter-close-00000")

	// Start a search and close it immediately.
	iter := nodes[0].TopicSearch(topic, 1)
	time.Sleep(200 * time.Millisecond)
	iter.Close()

	// Closing again should not panic.
	iter.Close()
}

// TestTopicLocalTopicNodes verifies that LocalTopicNodes returns the
// correct nodes from the local topic table.
func TestTopicLocalTopicNodes(t *testing.T) {
	t.Parallel()

	node := startLocalhostV5(t, Config{})
	defer node.Close()

	topic := makeTopic("test-local-nodes-0000")

	// Initially empty.
	nodes := node.LocalTopicNodes(topic)
	if len(nodes) != 0 {
		t.Fatalf("expected 0 local topic nodes, got %d", len(nodes))
	}
}

// TestTopicRegNodeTableUpdates verifies that a running topic registration
// picks up new nodes arriving via the main node table feed.
func TestTopicRegNodeTableUpdates(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t)
	defer test.close()

	key1 := newkey()
	addr1 := netip.MustParseAddrPort("10.0.1.101:30303")
	ln1 := test.getNode(key1, addr1)
	ln1.Set(topicindex.TopicDiscoveryVersion)

	key2 := newkey()
	addr2 := netip.MustParseAddrPort("10.0.1.102:30303")
	ln2 := test.getNode(key2, addr2)
	ln2.Set(topicindex.TopicDiscoveryVersion)

	test.table.addFoundNode(ln1.Node(), true)
	test.udp.RegisterTopic(testTopic1, 1)

	test.waitPacketOut(func(p *v5wire.Regtopic, addr netip.AddrPort, _ v5wire.Nonce) {
		if addr != addr1 {
			t.Fatalf("REGTOPIC sent to wrong node: got %v, want %v", addr, addr1)
		}
		test.packetInFrom(key1, addr, &v5wire.Regconfirmation{
			ReqID:    p.ReqID,
			Ticket:   nil,
			WaitTime: 900000,
		})
	})

	test.table.addFoundNode(ln2.Node(), true)

	test.waitPacketOut(func(p *v5wire.Regtopic, addr netip.AddrPort, _ v5wire.Nonce) {
		if addr != addr2 {
			t.Fatalf("REGTOPIC sent to wrong node: got %v, want %v", addr, addr2)
		}
		test.packetInFrom(key2, addr, &v5wire.Regconfirmation{
			ReqID:    p.ReqID,
			Ticket:   nil,
			WaitTime: 900000,
		})
	})
}

// TestTopicDiscoveryENRFlag verifies that the topic-discovery ENR entry is
// set on nodes and that the support check works correctly.
func TestTopicDiscoveryENRFlag(t *testing.T) {
	t.Parallel()

	node := startLocalhostV5(t, Config{})
	defer node.Close()

	if !topicindex.SupportsTopicDiscovery(node.Self()) {
		t.Fatal("local node should advertise topic-discovery ENR entry")
	}
}

// TestTopicDiscoveryFilterNodes verifies that filterTopicDiscovery correctly
// filters nodes based on the topic-discovery ENR capability.
func TestTopicDiscoveryFilterNodes(t *testing.T) {
	t.Parallel()

	// Node with topic-discovery entry (startLocalhostV5 sets it).
	withFlag := startLocalhostV5(t, Config{})
	defer withFlag.Close()

	// Node record without the topic-discovery entry.
	var r enr.Record
	r.Set(enr.IP(net.IPv4(127, 0, 0, 1)))
	r.Set(enr.UDP(9999))
	withoutFlag := enode.SignNull(&r, enode.ID{1, 2, 3})

	nodes := []*enode.Node{withFlag.Self(), withoutFlag}
	filtered := filterTopicDiscovery(nodes)

	if len(filtered) != 1 {
		t.Fatalf("expected 1 node after filter, got %d", len(filtered))
	}
	if filtered[0].ID() != withFlag.Self().ID() {
		t.Fatal("wrong node passed filter")
	}
}

func countRegistrants(found map[enode.ID]bool, registrants map[enode.ID]bool) int {
	count := 0
	for id := range found {
		if registrants[id] {
			count++
		}
	}
	return count
}

// waitForCond polls cond until it returns true or the deadline passes.
func waitForCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// TestTopicDHTEvictionEvictsAd checks that when the DHT routing table evicts a
// node, that node's ads are removed from the local ad cache too. The ad cache
// needs this signal because advertisers are never the target of topic RPCs, so
// it cannot observe their liveness itself.
func TestTopicDHTEvictionEvictsAd(t *testing.T) {
	test := newUDPV5Test(t)
	defer test.close()

	topic := topicindex.TopicID{1, 2, 3}
	n := nodeAtDistance(test.table.self().ID(), 128, net.IP{203, 0, 113, 9})

	// Seed the ad cache with n. The topic table is owned by the dispatch
	// goroutine, so add it there.
	done := make(chan struct{})
	test.udp.onDispatchCh <- func() { test.udp.topicTable.Add(n, topic); close(done) }
	<-done

	if got := test.udp.LocalTopicNodes(topic); len(got) != 1 || got[0].ID() != n.ID() {
		t.Fatalf("ad not present before eviction: %v", got)
	}

	// Fire a DHT routing-table eviction for n; the topic system subscribes to
	// this feed and should drop n's ads from the ad cache.
	test.table.removedFeed.Send(n.ID())

	waitForCond(t, "ad removed from cache after DHT eviction", func() bool {
		return len(test.udp.LocalTopicNodes(topic)) == 0
	})
}
