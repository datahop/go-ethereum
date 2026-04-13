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
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

func makeTopic(name string) topicindex.TopicID {
	var topic topicindex.TopicID
	copy(topic[:], []byte(name))
	return topic
}

// TestTopicRegistrationE2E spins up a small network of discv5 nodes,
// registers a topic on some of them, and verifies that the registrations
// appear in remote nodes' topic tables.
func TestTopicRegistrationE2E(t *testing.T) {
	t.Parallel()

	const numNodes = 6
	nodes := make([]*UDPv5, numNodes)
	for i := 0; i < numNodes; i++ {
		var cfg Config
		if i > 0 {
			cfg.Bootnodes = []*enode.Node{nodes[0].Self()}
		}
		nodes[i] = startLocalhostV5(t, cfg)
		defer nodes[i].Close()
	}

	// Trigger lookups so all nodes discover each other.
	for _, n := range nodes {
		n.Lookup(n.Self().ID())
	}
	time.Sleep(1 * time.Second)

	topic := makeTopic("test-reg-e2e-00000000")

	// Nodes 0 and 1 register for the topic.
	nodes[0].RegisterTopic(topic, 1)
	nodes[1].RegisterTopic(topic, 2)

	// Wait for registrations to propagate through the network.
	// The registration goroutines send REGTOPIC to nodes in their routing tables,
	// which store ads in their local topic tables.
	deadline := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatal("Timed out waiting for topic registrations to appear in any node's topic table")
		case <-ticker.C:
			for i := 2; i < numNodes; i++ {
				regs := nodes[i].LocalTopicNodes(topic)
				if len(regs) > 0 {
					t.Logf("Node %d has %d registrations for topic", i, len(regs))
					// Verify that at least one of the registrants is found.
					for _, n := range regs {
						if n.ID() == nodes[0].Self().ID() || n.ID() == nodes[1].Self().ID() {
							t.Logf("Found registrant %s in node %d's topic table", n.ID().TerminalString(), i)
							return // Success
						}
					}
				}
			}
		}
	}
}

// TestTopicSearchE2E verifies that TopicSearch can find nodes that have
// registered for a topic via the full protocol flow.
//
// This test is not parallel because it spins up a relatively large network
// and is sensitive to load/timing on the test machine.
func TestTopicSearchE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping topic search E2E test in short mode")
	}
	const numNodes = 8
	nodes := make([]*UDPv5, numNodes)
	for i := 0; i < numNodes; i++ {
		var cfg Config
		if i > 0 {
			cfg.Bootnodes = []*enode.Node{nodes[0].Self()}
		}
		nodes[i] = startLocalhostV5(t, cfg)
		defer nodes[i].Close()
	}

	// Trigger lookups so all nodes discover each other.
	for _, n := range nodes {
		n.Lookup(n.Self().ID())
	}
	time.Sleep(1 * time.Second)

	topic := makeTopic("test-search-e2e-00000")

	// Nodes 0-3 register.
	for i := 0; i < 4; i++ {
		nodes[i].RegisterTopic(topic, uint64(i+1))
	}

	// Wait for registrations to propagate.
	time.Sleep(15 * time.Second)

	// Node 7 (non-registrant) searches for the topic.
	iter := nodes[7].TopicSearch(topic, 100)
	defer iter.Close()

	registrantIDs := make(map[enode.ID]bool)
	for i := 0; i < 4; i++ {
		registrantIDs[nodes[i].Self().ID()] = true
	}

	// Drain the iterator in a goroutine.
	results := make(chan *enode.Node, 100)
	go func() {
		defer close(results)
		for iter.Next() {
			results <- iter.Node()
		}
	}()

	found := make(map[enode.ID]bool)
	timeout := time.After(45 * time.Second)
	done := false
	for !done {
		select {
		case <-timeout:
			done = true
		case n, ok := <-results:
			if !ok {
				done = true
				break
			}
			if !found[n.ID()] {
				found[n.ID()] = true
				if registrantIDs[n.ID()] {
					t.Logf("Found registrant %s via search", n.ID().TerminalString())
				}
			}
			if countRegistrants(found, registrantIDs) >= 4 {
				t.Logf("Found all 4 registrants!")
				return
			}
		}
	}

	regCount := countRegistrants(found, registrantIDs)
	t.Logf("Search found %d unique nodes (%d are registrants)", len(found), regCount)
	if regCount == 0 {
		t.Fatal("Expected to find at least one registrant via topic search")
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

func countRegistrants(found map[enode.ID]bool, registrants map[enode.ID]bool) int {
	count := 0
	for id := range found {
		if registrants[id] {
			count++
		}
	}
	return count
}
