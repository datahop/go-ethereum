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
	"bytes"
	"crypto/ecdsa"
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

// TestFilterTopicDiscovery checks the gate that keeps non-topic-discovery nodes
// out of the reg/search tables: only nodes advertising the topic-discovery ENR
// entry are kept. This is what stops a plain devp2p node from being sent (and
// then penalised for not answering) a topic RPC it never supported.
func TestFilterTopicDiscovery(t *testing.T) {
	t.Parallel()
	mkNode := func(id byte, topdisc bool) *enode.Node {
		var r enr.Record
		r.Set(enr.IPv4{127, 0, 0, 1})
		if topdisc {
			r.Set(topicindex.TopicDiscoveryVersion)
		}
		return enode.SignNull(&r, enode.ID{id})
	}
	withFlag1 := mkNode(1, true)
	without := mkNode(2, false)
	withFlag2 := mkNode(3, true)

	got := filterTopicDiscovery([]*enode.Node{withFlag1, without, withFlag2})
	if len(got) != 2 {
		t.Fatalf("filterTopicDiscovery kept %d nodes, want 2 (topic-discovery only)", len(got))
	}
	for _, n := range got {
		if !topicindex.SupportsTopicDiscovery(n) {
			t.Fatalf("kept node %v without the topic-discovery ENR entry", n.ID())
		}
		if n.ID() == without.ID() {
			t.Fatal("non-topic-discovery node was not filtered out")
		}
	}
}

// TestTopicRegSuccessResetsFailures checks that a successful REGTOPIC resets the
// node's consecutive wire-failure counter: a first timeout increments it, and a
// subsequent REGCONFIRMATION brings it back to zero. This is the reset side of
// the local-vs-global split — a briefly-flaky node that starts responding again
// does not accumulate toward eviction.
func TestTopicRegSuccessResetsFailures(t *testing.T) {
	test := newUDPV5Test(t)
	defer test.close()

	topic := topicindex.TopicID{7, 7, 7}
	key := newkey()
	addr := netip.MustParseAddrPort("10.0.3.44:30303")
	ln := test.getNode(key, addr)
	ln.Set(topicindex.TopicDiscoveryVersion)
	n := ln.Node()

	test.table.addFoundNode(n, true)
	test.udp.RegisterTopic(topic, 1)

	// First REGTOPIC: never answered → timeout → counter increments.
	test.waitPacketOut(func(p *v5wire.Regtopic, _ netip.AddrPort, _ v5wire.Nonce) {})
	waitForCond(t, "regtopic timeout counted toward wire-failure tally", func() bool {
		return test.db.FindFails(n.ID(), n.IPAddr()) >= 1
	})

	// Retry REGTOPIC: answered with a REGCONFIRMATION → success → counter resets.
	test.waitPacketOut(func(p *v5wire.Regtopic, a netip.AddrPort, _ v5wire.Nonce) {
		test.packetInFrom(key, a, &v5wire.Regconfirmation{
			ReqID:    p.ReqID,
			Ticket:   nil,
			WaitTime: 900000,
		})
	})
	waitForCond(t, "successful REGTOPIC reset the wire-failure tally", func() bool {
		return test.db.FindFails(n.ID(), n.IPAddr()) == 0
	})
}

// signedRecord builds a signed record carrying the given addresses. An invalid
// netip.Addr leaves the corresponding entry out.
func signedRecord(t *testing.T, key *ecdsa.PrivateKey, ip4, ip6 netip.Addr, port uint16) *enr.Record {
	t.Helper()
	var r enr.Record
	if ip4.IsValid() {
		r.Set(enr.IPv4Addr(ip4))
	}
	if ip6.IsValid() {
		r.Set(enr.IPv6Addr(ip6))
	}
	r.Set(enr.UDP(port))
	if err := enode.SignV4(&r, key); err != nil {
		t.Fatal(err)
	}
	return &r
}

// TestCheckRegtopicRecordIP covers the address rules applied to a REGTOPIC
// record: the entry for the source's address family must be present and equal to
// the source, and the entry for the other family, which this packet cannot
// prove, must at least be publicly routable.
func TestCheckRegtopicRecordIP(t *testing.T) {
	t.Parallel()

	var (
		none    netip.Addr
		src4    = netip.MustParseAddr("203.0.113.7")
		src6    = netip.MustParseAddr("2001:db8::7")
		other4  = netip.MustParseAddr("198.51.100.9")
		pub4    = netip.MustParseAddr("1.2.3.4")
		pub6    = netip.MustParseAddr("2606:4700::1111")
		lan4    = netip.MustParseAddr("192.168.1.5")
		loop4   = netip.MustParseAddr("127.0.0.1")
		linkl6  = netip.MustParseAddr("fe80::1")
		mapped4 = netip.MustParseAddr("::ffff:203.0.113.7")
	)

	tests := []struct {
		name     string
		src      netip.Addr
		ip4, ip6 netip.Addr
		port     uint16
		wantErr  bool
	}{
		{name: "v4 match", src: src4, ip4: src4, port: 30303},
		{name: "v4 mismatch", src: src4, ip4: other4, port: 30303, wantErr: true},
		{name: "v4 source, port differs", src: src4, ip4: src4, port: 40404},
		{name: "v4 source, no v4 entry", src: src4, ip6: src6, port: 30303, wantErr: true},
		{name: "v4 source mapped in v6", src: mapped4, ip4: src4, port: 30303},
		{name: "v6 match", src: src6, ip6: src6, port: 30303},
		{name: "v6 mismatch", src: src6, ip6: pub6, port: 30303, wantErr: true},
		{name: "v6 source, no v6 entry", src: src6, ip4: src4, port: 30303, wantErr: true},
		{name: "dual stack, other is public", src: src4, ip4: src4, ip6: pub6, port: 30303},
		{name: "dual stack over v6, other is public", src: src6, ip4: pub4, ip6: src6, port: 30303},
		{name: "dual stack, other is LAN", src: src4, ip4: src4, ip6: linkl6, port: 30303, wantErr: true},
		{name: "dual stack over v6, other is private", src: src6, ip4: lan4, ip6: src6, port: 30303, wantErr: true},
		{name: "dual stack over v6, other is loopback", src: src6, ip4: loop4, ip6: src6, port: 30303, wantErr: true},
		{name: "no addresses", src: src4, ip4: none, ip6: none, port: 30303, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			n, err := enode.New(enode.ValidSchemes, signedRecord(t, newkey(), test.ip4, test.ip6, test.port))
			if err != nil {
				t.Fatal(err)
			}
			if err := checkRegtopicRecordIP(n, test.src); (err != nil) != test.wantErr {
				t.Fatalf("got %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

// TestHandleRegtopicRecordIP checks that handleRegtopic actually drops the ad
// when the record fails the address rules, and stores it when it passes.
func TestHandleRegtopicRecordIP(t *testing.T) {
	t.Parallel()

	t.Run("mismatch rejected", func(t *testing.T) {
		t.Parallel()
		test := newUDPV5Test(t)
		defer test.close()

		var (
			spooferKey  = newkey()
			spooferAddr = netip.MustParseAddrPort("10.0.1.1:30303")
			victimAddr  = netip.MustParseAddr("10.0.9.9")
			honestKey   = newkey()
			honestAddr  = netip.MustParseAddrPort("10.0.1.2:30303")
		)

		// Signed by the sender, so the ID check passes, but advertising a third party.
		test.packetInFrom(spooferKey, spooferAddr, &v5wire.Regtopic{
			ReqID: []byte{1}, Topic: testTopic1,
			ENR: signedRecord(t, spooferKey, victimAddr, netip.Addr{}, spooferAddr.Port()),
		})
		test.packetInFrom(honestKey, honestAddr, &v5wire.Regtopic{
			ReqID: []byte{2}, Topic: testTopic1,
			ENR: signedRecord(t, honestKey, honestAddr.Addr(), netip.Addr{}, honestAddr.Port()),
		})

		// Only the honest registration is confirmed. Waiting for it also synchronizes
		// with the dispatch goroutine, so the table is settled below.
		test.waitPacketOut(func(p *v5wire.Regconfirmation, _ netip.AddrPort, _ v5wire.Nonce) {
			if !bytes.Equal(p.ReqID, []byte{2}) {
				t.Errorf("got REGCONFIRMATION for ReqID %x, want 02", p.ReqID)
			}
		})

		nodes := test.udp.LocalTopicNodes(testTopic1)
		if len(nodes) != 1 {
			t.Fatalf("wrong number of ads: got %d, want 1", len(nodes))
		}
		if nodes[0].ID() != enode.PubkeyToIDV4(&honestKey.PublicKey) {
			t.Fatal("ad with mismatched source address was stored")
		}
	})

	t.Run("port mismatch accepted", func(t *testing.T) {
		t.Parallel()
		test := newUDPV5Test(t)
		defer test.close()

		key := newkey()
		addr := netip.MustParseAddrPort("10.0.1.1:30303")

		test.packetInFrom(key, addr, &v5wire.Regtopic{
			ReqID: []byte{1}, Topic: testTopic1,
			ENR: signedRecord(t, key, addr.Addr(), netip.Addr{}, 40404),
		})
		test.waitPacketOut(func(p *v5wire.Regconfirmation, _ netip.AddrPort, _ v5wire.Nonce) {})

		if nodes := test.udp.LocalTopicNodes(testTopic1); len(nodes) != 1 {
			t.Fatalf("registration from rebound port was rejected: %d ads, want 1", len(nodes))
		}
	})
}
