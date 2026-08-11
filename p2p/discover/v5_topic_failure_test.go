// Copyright 2026 The go-ethereum Authors
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
	"crypto/ecdsa"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/discover/v5wire"
)

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

// TestTopicEvictNodeRemovesRegistration checks that evictNode (raised when the
// DHT drops a node after repeated wire failures) removes the node from a
// registration table, not just the ad cache. Two nodes are registered so
// evicting one keeps NodeCount > 0 and the session from restarting and
// re-seeding the evicted node from the table.
func TestTopicEvictNodeRemovesRegistration(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t)
	defer test.close()

	confirm := func(key *ecdsa.PrivateKey, addr netip.AddrPort) {
		test.waitPacketOut(func(p *v5wire.Regtopic, gotAddr netip.AddrPort, _ v5wire.Nonce) {
			test.packetInFrom(key, gotAddr, &v5wire.Regconfirmation{
				ReqID:    p.ReqID,
				WaitTime: 900000,
			})
		})
	}

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
	confirm(key1, addr1)

	test.table.addFoundNode(ln2.Node(), true)
	confirm(key2, addr2)

	reg := test.udp.topicSys.reg[testTopic1]
	waitForCond(t, "both nodes registered", func() bool { return reg.nodeCount() == 2 })

	// Evicting ln1 as a dead node must drop it from the registration table.
	test.udp.topicSys.evictNode(ln1.Node().ID())
	waitForCond(t, "evicted node removed from reg", func() bool { return reg.nodeCount() == 1 })
}

// TestTopicRegTimeoutTracksNotEvicts checks the local-vs-global split after
// topic RPC failures were wired into the DHT wire-failure counter: a single
// REGTOPIC timeout is counted toward the node's consecutive-failure tally (via
// trackRequest) but does NOT evict the node's ad. Eviction is deferred to the
// DHT's threshold — the old behaviour evicted on the first timeout, which this
// test guards against.
func TestTopicRegTimeoutTracksNotEvicts(t *testing.T) {
	test := newUDPV5Test(t)
	defer test.close()

	topic := topicindex.TopicID{9, 9, 9}
	key := newkey()
	addr := netip.MustParseAddrPort("10.0.2.55:30303")
	ln := test.getNode(key, addr)
	ln.Set(topicindex.TopicDiscoveryVersion)
	n := ln.Node()

	// Seed n's ad in the local topic table (owned by the dispatch goroutine).
	done := make(chan struct{})
	test.udp.onDispatchCh <- func() { test.udp.topicTable.Add(n, topic); close(done) }
	<-done

	// Make n a registration target and start registering; catch the REGTOPIC and
	// never answer it, so the call times out.
	test.table.addFoundNode(n, true)
	test.udp.RegisterTopic(topic, 1)
	test.waitPacketOut(func(p *v5wire.Regtopic, _ netip.AddrPort, _ v5wire.Nonce) {})

	// The timeout is counted toward n's consecutive wire-failure tally...
	waitForCond(t, "regtopic timeout counted toward wire-failure tally", func() bool {
		return test.db.FindFails(n.ID(), n.IPAddr()) >= 1
	})
	// ...but must not evict n's ad on the first timeout.
	if got := test.udp.LocalTopicNodes(topic); len(got) != 1 || got[0].ID() != n.ID() {
		t.Fatalf("ad evicted on first timeout; eviction should defer to the DHT threshold: %v", got)
	}
}
