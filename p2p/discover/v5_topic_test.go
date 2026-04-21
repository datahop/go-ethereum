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
	t.Skip("disabled: topicSearch.runRequests can block past Close; see datahop/go-ethereum#30")

	node0 := startLocalhostV5(t, Config{})
	node1 := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{node0.Self()}})
	node2 := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{node0.Self()}})
	node3 := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{node0.Self()}})
	defer func() {
		for _, n := range []*UDPv5{node0, node1, node2, node3} {
			n.Close()
		}
	}()

	node1.topicTable.Add(node0.Self(), testTopic1)
	node1.topicTable.Add(node3.Self(), testTopic1)

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

	key2 := newkey()
	addr2 := netip.MustParseAddrPort("10.0.1.102:30303")
	ln2 := test.getNode(key2, addr2)

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

	if got := test.udp.topicSys.reg[testTopic1].state.NodeCount(); got != 2 {
		t.Fatalf("wrong node count in reg state: got %d, want 2", got)
	}
}

// TestHandleRegtopicEndpointCheck verifies that handleRegtopic rejects
// REGTOPIC requests whose advertised ENR endpoint does not match the packet
// source address — otherwise any peer could register an ad pointing at an
// arbitrary IP.
func TestHandleRegtopicEndpointCheck(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t)
	defer test.close()

	key := newkey()
	addrReal := netip.MustParseAddrPort("10.0.1.101:30303")
	addrLie := netip.MustParseAddrPort("10.0.99.99:30303")

	// Matching ENR: advertises the real source endpoint.
	matchNode := test.getNode(key, addrReal)
	matchRecord := matchNode.Node().Record()

	// Mismatched ENR: same key (ID matches), but advertises a different endpoint.
	db, _ := enode.OpenDB("")
	defer db.Close()
	lying := enode.NewLocalNode(db, key)
	lying.SetStaticIP(addrLie.Addr().AsSlice())
	lying.Set(enr.UDP(addrLie.Port()))
	lyingRecord := lying.Node().Record()

	// Case A: REGTOPIC with mismatched ENR — should be silently dropped.
	test.packetInFrom(key, addrReal, &v5wire.Regtopic{
		ReqID: []byte{1},
		Topic: testTopic1,
		ENR:   lyingRecord,
	})
	if nodes := test.udp.LocalTopicNodes(testTopic1); len(nodes) != 0 {
		t.Fatalf("mismatched-endpoint REGTOPIC was accepted: %d nodes in topic", len(nodes))
	}

	// Case B: REGTOPIC with matching ENR — should be accepted and confirmed.
	test.packetInFrom(key, addrReal, &v5wire.Regtopic{
		ReqID: []byte{2},
		Topic: testTopic1,
		ENR:   matchRecord,
	})
	test.waitPacketOut(func(p *v5wire.Regconfirmation, addr netip.AddrPort, _ v5wire.Nonce) {
		if addr != addrReal {
			t.Fatalf("REGCONFIRMATION sent to wrong addr: got %v, want %v", addr, addrReal)
		}
	})
	if nodes := test.udp.LocalTopicNodes(testTopic1); len(nodes) != 1 {
		t.Fatalf("matching REGTOPIC was not stored: %d nodes in topic", len(nodes))
	}
}
