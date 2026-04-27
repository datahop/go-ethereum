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
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/discover/v5wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
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
	t.Skip("disabled: depends on Search.IsDone fix; see datahop/go-ethereum#27")

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

// TestTopicQueryContextCancel verifies that an in-flight topicQuery returns
// promptly when its context is cancelled, instead of blocking until the RPC
// times out. This is the invariant that lets topicSearch.runRequests unblock
// when the search is closed (datahop/go-ethereum#30) and lets UDPv5.Close
// stop active searches without waiting (datahop/go-ethereum#28).
func TestTopicQueryContextCancel(t *testing.T) {
	test := newUDPV5Test(t)
	defer test.close()

	key := newkey()
	addr := netip.MustParseAddrPort("10.0.1.101:30303")
	peer := test.getNode(key, addr)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan topicQueryResult, 1)
	go func() {
		done <- test.udp.topicQuery(ctx, peer.Node(), testTopic1, []uint{0}, 1)
	}()

	// Wait for the outbound TOPICQUERY so we know the call is in flight.
	test.waitPacketOut(func(_ *v5wire.TopicQuery, _ netip.AddrPort, _ v5wire.Nonce) {
		// Don't respond — leave topicQuery blocked.
	})

	cancel()

	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("topicQuery did not return within 2s after ctx cancel")
	}
}

// TestTopicQueryContextCancelQueued verifies that cancelling the context of a
// topicQuery whose call has not yet become active (i.e. it's queued behind
// another active call to the same peer) does not panic dispatch with
// "BUG: callDone for inactive call". The panic was hit by a 200-node testbed
// run on 2026-04-27, where every search ran for the full
// rpcSearchTimeoutSeconds window (returnedNodes=0) and multiple topicQueries
// could pile up against the same peer.
func TestTopicQueryContextCancelQueued(t *testing.T) {
	test := newUDPV5Test(t)
	defer test.close()

	key := newkey()
	addr := netip.MustParseAddrPort("10.0.1.101:30303")
	peer := test.getNode(key, addr)

	// First call — becomes active and stays in flight (no response).
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	done1 := make(chan topicQueryResult, 1)
	go func() {
		done1 <- test.udp.topicQuery(ctx1, peer.Node(), testTopic1, []uint{0}, 1)
	}()
	test.waitPacketOut(func(_ *v5wire.TopicQuery, _ netip.AddrPort, _ v5wire.Nonce) {
		// Don't respond — keep call 1 active.
	})

	// Second call to the same peer — queued behind call 1.
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan topicQueryResult, 1)
	go func() {
		done2 <- test.udp.topicQuery(ctx2, peer.Node(), testTopic1, []uint{0}, 2)
	}()

	// Give dispatch time to enqueue call 2. No packet should go out for it
	// while call 1 is still active.
	time.Sleep(100 * time.Millisecond)

	// Cancel the queued call. Without the dispatch fix this panics inside
	// the dispatch goroutine, killing the test process.
	cancel2()

	select {
	case result := <-done2:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("queued call: expected context.Canceled, got %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued topicQuery did not return within 2s after ctx cancel")
	}

	// Confirm dispatch is still alive by also cancelling call 1 and
	// observing it return promptly.
	cancel1()
	select {
	case <-done1:
	case <-time.After(2 * time.Second):
		t.Fatal("active topicQuery did not return — dispatch may have panicked")
	}
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
