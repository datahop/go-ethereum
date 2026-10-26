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
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/internal/testlog"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/discover/v5wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/rlp"
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
	// The searcher bootstraps from the registrar directly. Via node0 alone, it can
	// only learn node1 from node0's aux nodes, which carry one node per distance.
	node2 := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{node0.Self(), node1.Self()}})
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
	timeout := time.AfterFunc(30*time.Second, it.Close)
	defer timeout.Stop()
	found := enode.ReadNodes(it, 2)
	sortByID(found)

	want := []*enode.Node{node0.Self(), node3.Self()}
	sortByID(want)
	if err := checkNodesEqual(found, want); err != nil {
		t.Error(err)
	}
}

// capableRecord is the signed record of a TopDisc node that is not running:
// a registrar can hold it as an ad without it joining anyone's routing table.
func capableRecord(t *testing.T, port int) *enode.Node {
	db, err := enode.OpenDB("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	ln := enode.NewLocalNode(db, newkey())
	ln.Set(topicindex.TopicDiscoveryVersion)
	ln.SetStaticIP(net.IP{127, 0, 0, 1})
	ln.SetFallbackUDP(port)
	return ln.Node()
}

// waitForTable waits until the node's routing table holds every node in nodes,
// so a search's first pass seeds from all of them.
func waitForTable(t *testing.T, node *UDPv5, nodes []*enode.Node) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		known := make(map[enode.ID]bool)
		for _, n := range node.AllNodes() {
			known[n.ID()] = true
		}
		have := 0
		for _, n := range nodes {
			if known[n.ID()] {
				have++
			}
		}
		if have == len(nodes) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node knows %d of %d nodes", have, len(nodes))
		}
	}
}

// TestTopicSearchMixesRegistrars runs a search over five registrars that each
// hold two ads of their own: the first five results come from five different
// registrars, and every ad is returned.
func TestTopicSearchMixesRegistrars(t *testing.T) {
	node0 := startLocalhostV5(t, Config{})
	var all []*UDPv5
	defer func() {
		node0.Close()
		for _, n := range all {
			n.Close()
		}
	}()
	regOf := make(map[enode.ID]int)
	var regs []*enode.Node
	var ads []*enode.Node
	for i := 0; i < 5; i++ {
		reg := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{node0.Self()}})
		all = append(all, reg)
		regs = append(regs, reg.Self())
		var own []*enode.Node
		for j := 0; j < 2; j++ {
			ad := capableRecord(t, 40000+10*i+j)
			own = append(own, ad)
			regOf[ad.ID()] = i
		}
		seedTopicTable(t, reg, testTopic1, own...)
		ads = append(ads, own...)
	}
	searcher := startLocalhostV5(t, Config{Bootnodes: append([]*enode.Node{node0.Self()}, regs...)})
	all = append(all, searcher)
	waitForTable(t, searcher, regs)

	it := searcher.TopicSearch(testTopic1, 0)
	defer it.Close()
	timeout := time.AfterFunc(30*time.Second, it.Close)
	defer timeout.Stop()
	// Results are checked in the order the search hands them out.
	var found []*enode.Node
	for len(found) < len(ads) && it.Next() {
		found = append(found, it.Node())
	}
	if len(found) != len(ads) {
		t.Fatalf("got %d results, want %d", len(found), len(ads))
	}
	seen := make(map[int]bool)
	for _, n := range found[:5] {
		seen[regOf[n.ID()]] = true
	}
	if len(seen) != 5 {
		t.Fatalf("first five results came from %d registrars, want 5", len(seen))
	}
	sortByID(found)
	sortByID(ads)
	if err := checkNodesEqual(found, ads); err != nil {
		t.Error(err)
	}
}

// TestTopicSearchStopsWhenUnread checks that a search sends no queries while
// its consumer leaves results unread, and queries again once they are read.
func TestTopicSearchStopsWhenUnread(t *testing.T) {
	node0 := startLocalhostV5(t, Config{})
	var all []*UDPv5
	defer func() {
		node0.Close()
		for _, n := range all {
			n.Close()
		}
	}()
	var regs []*enode.Node
	nads := 0
	for i := 0; i < 6; i++ {
		reg := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{node0.Self()}})
		all = append(all, reg)
		regs = append(regs, reg.Self())
		var own []*enode.Node
		for j := 0; j < 16; j++ {
			own = append(own, capableRecord(t, 41000+100*i+j))
		}
		seedTopicTable(t, reg, testTopic1, own...)
		nads += len(own)
	}
	searcher := startLocalhostV5(t, Config{Bootnodes: append([]*enode.Node{node0.Self()}, regs...)})
	all = append(all, searcher)
	waitForTable(t, searcher, regs)

	it := searcher.TopicSearch(testTopic1, 0)
	defer it.Close()
	s := it.(*topicSearchIterator).search
	if !it.Next() {
		t.Fatal("no first result")
	}
	// The consumer stops reading. The queries must stop too.
	time.Sleep(time.Second)
	sent := s.queries.Load()
	time.Sleep(2 * time.Second)
	if again := s.queries.Load(); again != sent {
		t.Fatalf("%d queries sent while results waited unread", again-sent)
	}
	// At most every registrar and the bootnode: nothing was asked twice.
	if sent > uint64(len(regs)+1) {
		t.Fatalf("%d queries before the consumer stopped, want at most %d", sent, len(regs)+1)
	}
	// Reading again lets the search go on: every ad comes out.
	for n := 1; n < nads; n++ {
		if !it.Next() {
			t.Fatalf("search ended after %d results, want %d", n, nads)
		}
	}
}

// TestTopicSearchNoRepeats checks that a search doesn't return a node again
// in later passes until the ad lifetime has passed.
func TestTopicSearchNoRepeats(t *testing.T) {
	const lifetime = 3 * regloopMinTime
	node0 := startLocalhostV5(t, Config{})
	node1 := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{node0.Self()}})
	node2 := startLocalhostV5(t, Config{
		Bootnodes: []*enode.Node{node0.Self(), node1.Self()},
		Topic:     topicindex.Config{AdLifetime: lifetime},
	})
	defer func() {
		for _, n := range []*UDPv5{node0, node1, node2} {
			n.Close()
		}
	}()
	seedTopicTable(t, node1, testTopic1, node0.Self())

	it := node2.TopicSearch(testTopic1, 0)
	defer it.Close()
	timeout := time.AfterFunc(30*time.Second, it.Close)
	defer timeout.Stop()
	if !it.Next() {
		t.Fatal("search ended without results")
	}
	first := time.Now()
	if !it.Next() {
		t.Fatal("search ended without second result")
	}
	if it.Node().ID() != node0.Self().ID() {
		t.Fatalf("unexpected result %v", it.Node().ID())
	}
	if d := time.Since(first); d < lifetime-200*time.Millisecond {
		t.Fatalf("node returned again after %v, want about %v", d, lifetime)
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

// TestTopicSearchPassGap checks the gap between passes: the minimum after a
// pass with results, doubling from it after every empty pass up to
// searchPassBackoff.
func TestTopicSearchPassGap(t *testing.T) {
	s := new(topicSearch)
	want := []time.Duration{4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, searchPassBackoff, searchPassBackoff}
	for i, w := range want {
		if got := s.passGap(0, false); got != w {
			t.Fatalf("empty pass %d: gap %v, want %v", i+1, got, w)
		}
	}
	if got := s.passGap(3, false); got != regloopMinTime {
		t.Fatalf("pass with results: gap %v, want %v", got, regloopMinTime)
	}
	if got := s.passGap(0, false); got != 4*time.Second {
		t.Fatalf("empty pass after a reset: gap %v, want 4s", got)
	}
	if got := s.passGap(0, true); got != regloopMinTime {
		t.Fatalf("empty pass that moved the active bucket: gap %v, want %v", got, regloopMinTime)
	}
	if got := s.passGap(0, false); got != 4*time.Second {
		t.Fatalf("empty pass after a move: gap %v, want 4s", got)
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
	// The registration loop re-seeds from the table, but n is not admitted again.
	time.Sleep(regloopMinTime + 500*time.Millisecond)
	if q := pipeQueued(test); q != 0 {
		t.Fatalf("%d packets sent to a failed node", q)
	}
}

// TestTopicSearchTimeoutNotReasked checks that a node whose TOPICQUERY timed
// out is not queried again by the next search pass.
func TestTopicSearchTimeoutNotReasked(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t)
	defer test.close()

	ln := test.getNode(newkey(), netip.MustParseAddrPort("10.0.2.56:30303"))
	ln.Set(topicindex.TopicDiscoveryVersion)
	n := ln.Node()
	test.table.addFoundNode(n, true)

	it := test.udp.TopicSearch(testTopic1, 0)
	defer it.Close()
	test.waitPacketOut(func(p *v5wire.TopicQuery, _ netip.AddrPort, _ v5wire.Nonce) {})
	waitForCond(t, "topicquery timeout counted toward wire-failure tally", func() bool {
		return test.db.FindFails(n.ID(), n.IPAddr()) >= 1
	})
	// The next pass starts after a gap of 2*regloopMinTime and skips the node.
	time.Sleep(2*regloopMinTime + time.Second)
	if q := pipeQueued(test); q != 0 {
		t.Fatalf("%d packets sent to a failed node", q)
	}
}

func pipeQueued(test *udpV5Test) int {
	test.pipe.mu.Lock()
	defer test.pipe.mu.Unlock()
	return len(test.pipe.queue)
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
// node's consecutive wire-failure counter. This is the reset side of the
// local-vs-global split — a briefly-flaky node that starts responding again
// does not accumulate toward eviction. The increment side is covered by
// TestTopicRegTimeoutTracksNotEvicts.
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
	test.db.UpdateFindFails(n.ID(), n.IPAddr(), 1)
	test.udp.RegisterTopic(topic, 1)

	// REGTOPIC answered with a REGCONFIRMATION → success → counter resets.
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

		// The spoofer gets a rejection, then the honest registration is confirmed.
		// Waiting for it also synchronizes with the dispatch goroutine, so the
		// table is settled below.
		test.waitPacketOut(func(p *v5wire.Regconfirmation, _ netip.AddrPort, _ v5wire.Nonce) {
			if !bytes.Equal(p.ReqID, []byte{1}) {
				t.Errorf("got REGCONFIRMATION for ReqID %x, want 01", p.ReqID)
			}
			if len(p.Ticket) != 0 || p.WaitTime != 0 {
				t.Errorf("spoofer not rejected: ticket %d bytes, wait time %d", len(p.Ticket), p.WaitTime)
			}
		})
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

// sealTicket seals a ticket with the node's ticket sealer. The work runs on the
// dispatch goroutine, which owns the sealer.
func sealTicket(test *udpV5Test, ticket *topicindex.Ticket) []byte {
	test.t.Helper()
	done := make(chan []byte, 1)
	select {
	case test.udp.onDispatchCh <- func() { done <- test.udp.ticketSealer.Pack(ticket) }:
		return <-done
	case <-test.udp.closeCtx.Done():
		test.t.Fatal("node closed before ticket could be sealed")
		return nil
	}
}

// badSigRecord returns a record for the key and endpoint whose signature does
// not verify.
func badSigRecord(t *testing.T, key *ecdsa.PrivateKey, addr netip.AddrPort) *enr.Record {
	t.Helper()
	enc, err := rlp.EncodeToBytes(signedRecord(t, key, addr.Addr(), netip.Addr{}, addr.Port()))
	if err != nil {
		t.Fatal(err)
	}
	var elems []rlp.RawValue
	if err := rlp.DecodeBytes(enc, &elems); err != nil {
		t.Fatal(err)
	}
	var sig []byte
	if err := rlp.DecodeBytes(elems[0], &sig); err != nil {
		t.Fatal(err)
	}
	sig[0] ^= 0xff
	elems[0], _ = rlp.EncodeToBytes(sig)
	enc, _ = rlp.EncodeToBytes(elems)
	var r enr.Record
	if err := rlp.DecodeBytes(enc, &r); err != nil {
		t.Fatal(err)
	}
	return &r
}

// TestHandleRegtopicReplies checks that the registrar answers every REGTOPIC:
// valid and late tickets are credited, an early ticket is re-quoted for its
// window, an invalid or expired ticket or one issued to another node starts a
// new attempt, and a request with an unusable record is rejected. One ad is seeded so that a new attempt has to
// wait (about 45s) and the credited wait decides admission.
func TestHandleRegtopicReplies(t *testing.T) {
	t.Parallel()

	type outcome int
	const (
		admitted outcome = iota
		waiting
		requoted
		rejected
	)
	var (
		key       = newkey()
		addr      = netip.MustParseAddrPort("10.0.1.1:30303")
		otherAddr = netip.MustParseAddrPort("10.0.1.2:30303")
		otherKey  = newkey()
		record    = func(t *testing.T) *enr.Record {
			return signedRecord(t, key, addr.Addr(), netip.Addr{}, addr.Port())
		}
		sealFor = func(test *udpV5Test, node enode.ID, topic topicindex.TopicID, lastUsed, wait, sinceFirst time.Duration) []byte {
			now := test.udp.clock.Now()
			return sealTicket(test, &topicindex.Ticket{
				Topic:          topic,
				Node:           node,
				LastUsed:       now.Add(-lastUsed),
				WaitTimeIssued: wait,
				FirstIssued:    now.Add(-sinceFirst),
			})
		}
		seal = func(test *udpV5Test, topic topicindex.TopicID, lastUsed, wait, sinceFirst time.Duration) []byte {
			return sealFor(test, enode.PubkeyToIDV4(&key.PublicKey), topic, lastUsed, wait, sinceFirst)
		}
		onTime = func(test *udpV5Test) []byte { return seal(test, testTopic1, 2*time.Second, time.Second, time.Minute) }
	)
	tests := []struct {
		name             string
		ticket           func(test *udpV5Test) []byte
		enr              func(t *testing.T) *enr.Record
		want             outcome
		minWait, maxWait uint // bounds on WaitTime in ms, if maxWait > 0
	}{
		{name: "no ticket", want: waiting},
		{name: "on time", ticket: onTime, want: admitted},
		{name: "late", ticket: func(test *udpV5Test) []byte {
			// 10s past the window start, inside the grace: credited the scheduled 60s.
			return seal(test, testTopic1, 20*time.Second, 10*time.Second, 70*time.Second)
		}, want: admitted},
		{name: "expired", ticket: func(test *udpV5Test) []byte {
			return seal(test, testTopic1, 20*time.Minute, 50*time.Second, 20*time.Minute)
		}, want: waiting, minWait: 40000, maxWait: 50000},
		{name: "early", ticket: func(test *udpV5Test) []byte {
			return seal(test, testTopic1, 0, time.Minute, 0)
		}, want: requoted, minWait: 59000, maxWait: 61000},
		{name: "bad mac", ticket: func(test *udpV5Test) []byte {
			enc := onTime(test)
			enc[0] ^= 0xff
			return enc
		}, want: waiting},
		{name: "wrong topic", ticket: func(test *udpV5Test) []byte {
			return seal(test, makeTopic("other"), 2*time.Second, time.Second, time.Minute)
		}, want: waiting},
		{name: "unknown key", ticket: func(test *udpV5Test) []byte {
			enc := onTime(test)
			enc[32], enc[33] = 0xff, 0xff
			return enc
		}, want: waiting},
		{name: "wrong size", ticket: func(*udpV5Test) []byte { return []byte{1} }, want: waiting},
		{name: "other node", ticket: func(test *udpV5Test) []byte {
			return sealFor(test, enode.PubkeyToIDV4(&otherKey.PublicKey), testTopic1, 2*time.Second, time.Second, time.Minute)
		}, want: waiting, minWait: 40000, maxWait: 50000},
		{name: "other node early", ticket: func(test *udpV5Test) []byte {
			return sealFor(test, enode.PubkeyToIDV4(&otherKey.PublicKey), testTopic1, 0, time.Minute, 0)
		}, want: waiting, minWait: 40000, maxWait: 50000},
		{name: "bad signature", enr: func(t *testing.T) *enr.Record { return badSigRecord(t, key, addr) }, want: rejected},
		{name: "other key", enr: func(t *testing.T) *enr.Record {
			return signedRecord(t, otherKey, addr.Addr(), netip.Addr{}, addr.Port())
		}, want: rejected},
		{name: "other address", enr: func(t *testing.T) *enr.Record {
			return signedRecord(t, key, otherAddr.Addr(), netip.Addr{}, addr.Port())
		}, want: rejected},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			test := newUDPV5Test(t)
			defer test.close()
			seedTopicTable(t, test.udp, testTopic1, enode.NewV4(&newkey().PublicKey, net.IP{10, 0, 9, 1}, 30303, 30303))

			req := &v5wire.Regtopic{ReqID: []byte{1}, Topic: testTopic1, ENR: record(t)}
			if tt.ticket != nil {
				req.Ticket = tt.ticket(test)
			}
			if tt.enr != nil {
				req.ENR = tt.enr(t)
			}
			test.packetInFrom(key, addr, req)
			test.waitPacketOut(func(p *v5wire.Regconfirmation, _ netip.AddrPort, _ v5wire.Nonce) {
				if !bytes.Equal(p.ReqID, req.ReqID) {
					t.Errorf("wrong ReqID %x", p.ReqID)
				}
				if p.RespCount != 1 {
					t.Errorf("RespCount = %d, want 1", p.RespCount)
				}
				switch tt.want {
				case admitted:
					if len(p.Ticket) != 0 || p.WaitTime != waitTimeToMs(test.udp.topicTable.AdLifetime()) {
						t.Errorf("not admitted: ticket %d bytes, wait time %d", len(p.Ticket), p.WaitTime)
					}
				case waiting, requoted:
					if len(p.Ticket) == 0 || p.WaitTime == 0 {
						t.Errorf("no ticket issued: ticket %d bytes, wait time %d", len(p.Ticket), p.WaitTime)
					}
					if tt.want == requoted && !bytes.Equal(p.Ticket, req.Ticket) {
						t.Errorf("early ticket not echoed")
					}
					if tt.want == waiting && bytes.Equal(p.Ticket, req.Ticket) {
						t.Errorf("ticket echoed on a new attempt")
					}
				case rejected:
					if len(p.Ticket) != 0 || p.WaitTime != 0 {
						t.Errorf("not rejected: ticket %d bytes, wait time %d", len(p.Ticket), p.WaitTime)
					}
				}
				if tt.maxWait > 0 && (p.WaitTime < tt.minWait || p.WaitTime > tt.maxWait) {
					t.Errorf("wait time %d, want within [%d, %d]", p.WaitTime, tt.minWait, tt.maxWait)
				}
			})
			wantAds := 1
			if tt.want == admitted {
				wantAds = 2
			}
			if n := len(test.udp.LocalTopicNodes(testTopic1)); n != wantAds {
				t.Fatalf("%d ads stored, want %d", n, wantAds)
			}
		})
	}
}

// TestRegtopicTicketCredit checks the waiting time credited to a ticket: the
// actual wait when on time, the scheduled wait when late, with FirstIssued
// re-based so that the time past the window is not credited later, and nothing
// when expired.
func TestRegtopicTicketCredit(t *testing.T) {
	t.Parallel()
	first := mclock.AbsTime(time.Hour)
	ticket := &topicindex.Ticket{
		FirstIssued:    first,
		LastUsed:       first.Add(10 * time.Second),
		WaitTimeIssued: 20 * time.Second,
	}
	start := ticket.WindowStart()

	now := start.Add(time.Second)
	waited, firstIssued := regtopicCredit(ticket, topicindex.TicketOnTime, now)
	if waited != now.Sub(first) || firstIssued != first {
		t.Errorf("on time: got (%v, %v), want (%v, %v)", waited, firstIssued, now.Sub(first), first)
	}

	now = start.Add(10 * time.Second)
	waited, firstIssued = regtopicCredit(ticket, topicindex.TicketLate, now)
	if waited != start.Sub(first) || firstIssued != now.Add(-waited) {
		t.Errorf("late: got (%v, %v), want (%v, %v)", waited, firstIssued, start.Sub(first), now.Add(-start.Sub(first)))
	}

	now = start.Add(time.Minute)
	waited, firstIssued = regtopicCredit(ticket, topicindex.TicketExpired, now)
	if waited != 0 || firstIssued != now {
		t.Errorf("expired: got (%v, %v), want (0, %v)", waited, firstIssued, now)
	}
}

// TestRegtopicTicketReplay presents one ticket again and again to a topic table
// and ticket sealer driven the way handleRegtopic drives them. A ticket is
// credited once: presented again, after an eviction or inside the grace, it is
// worth nothing. Past its grace it is worth nothing either, so that it cannot
// re-admit its holder after every ad expiry.
func TestRegtopicTicketReplay(t *testing.T) {
	t.Parallel()
	topic := makeTopic("replay")

	type replayEnv struct {
		clock     *mclock.Simulated
		tab       *topicindex.TopicTable
		seed, adv *enode.Node
		t0        []byte        // ticket issued to adv at time zero
		w         time.Duration // wait quoted in t0
		// present mirrors handleRegtopic for a ticket from adv.
		present func(enc []byte) (admitted bool, wait time.Duration, timing topicindex.TicketTiming, replay bool)
	}
	setup := func(t *testing.T) *replayEnv {
		env := &replayEnv{
			clock: new(mclock.Simulated),
			seed:  enode.NewV4(&newkey().PublicKey, net.IP{10, 0, 1, 1}, 30303, 30303),
			adv:   enode.NewV4(&newkey().PublicKey, net.IP{10, 0, 2, 1}, 30303, 30303),
		}
		env.tab = topicindex.NewTopicTable(topicindex.Config{Clock: env.clock, Log: testlog.Logger(t, log.LvlInfo)})
		sealer := topicindex.NewTicketSealer(env.clock)
		used := topicindex.NewUsedTickets(env.clock)
		env.present = func(enc []byte) (admitted bool, wait time.Duration, timing topicindex.TicketTiming, replay bool) {
			t.Helper()
			now := env.clock.Now()
			ticket, err := sealer.Unpack(topic, enc)
			if err != nil {
				t.Fatal(err)
			}
			timing = ticket.TimingAt(now)
			credited := timing != topicindex.TicketExpired && !used.Use(enc)
			replay = timing != topicindex.TicketExpired && !credited
			waited, firstIssued := time.Duration(0), now
			if credited {
				waited, firstIssued = regtopicCredit(ticket, timing, now)
			}
			wait = env.tab.Register(env.adv, topic, waited)
			if wait > 0 {
				sealer.Pack(&topicindex.Ticket{Topic: topic, Node: env.adv.ID(), WaitTimeIssued: wait, LastUsed: now, FirstIssued: firstIssued})
			}
			return wait == 0, wait, timing, replay
		}

		// With one ad in the topic, a fresh attempt has to wait.
		env.tab.Add(env.seed, topic)
		now := env.clock.Now()
		env.w = env.tab.Register(env.adv, topic, 0)
		if env.w <= 0 {
			t.Fatal("fresh attempt admitted without waiting")
		}
		env.t0 = sealer.Pack(&topicindex.Ticket{Topic: topic, Node: env.adv.ID(), WaitTimeIssued: env.w, LastUsed: now, FirstIssued: now})
		t.Logf("ticket issued at %v with wait %v", time.Duration(now), env.w)
		return env
	}

	t.Run("after ad expiry", func(t *testing.T) {
		env := setup(t)
		env.clock.Run(env.w)
		if admitted, wait, timing, _ := env.present(env.t0); !admitted {
			t.Fatalf("on-time ticket not admitted: timing %v, wait %v", timing, wait)
		}
		for cycle := 1; cycle <= 3; cycle++ {
			env.clock.Run(env.tab.AdLifetime() + time.Second)
			env.tab.Expire()
			if n := len(env.tab.Nodes(topic)); n != 0 {
				t.Fatalf("cycle %d: %d ads left after expiry", cycle, n)
			}
			env.tab.Add(env.seed, topic)
			fresh := env.tab.WaitTime(env.adv, topic)
			if fresh <= 0 {
				t.Fatalf("cycle %d: fresh attempt need not wait", cycle)
			}
			admitted, wait, timing, _ := env.present(env.t0)
			t.Logf("cycle %d at %v: timing %v, admitted %v, wait %v (fresh %v)", cycle, time.Duration(env.clock.Now()), timing, admitted, wait, fresh)
			if admitted {
				t.Errorf("cycle %d: replayed ticket admitted at %v", cycle, time.Duration(env.clock.Now()))
				continue
			}
			if wait != fresh || timing != topicindex.TicketExpired {
				t.Errorf("cycle %d: replayed ticket got timing %v and wait %v, want expired and %v", cycle, timing, wait, fresh)
			}
		}
	})

	t.Run("after eviction", func(t *testing.T) {
		env := setup(t)
		// 6s late: past the 5s window, inside the 10s grace.
		env.clock.Run(env.w + 6*time.Second)
		if admitted, wait, timing, _ := env.present(env.t0); !admitted || timing != topicindex.TicketLate {
			t.Fatalf("late ticket: timing %v, admitted %v, wait %v", timing, admitted, wait)
		}
		env.tab.RemoveNode(env.adv.ID())
		for i := 1; i <= 2; i++ {
			if slices.ContainsFunc(env.tab.Nodes(topic), func(n *enode.Node) bool { return n.ID() == env.adv.ID() }) {
				t.Fatalf("replay %d: advertiser still registered after eviction", i)
			}
			env.clock.Run(time.Second)
			fresh := env.tab.WaitTime(env.adv, topic)
			if fresh <= 0 {
				t.Fatalf("replay %d: fresh attempt need not wait", i)
			}
			admitted, wait, timing, replay := env.present(env.t0)
			t.Logf("replay %d at %v: timing %v, replay %v, admitted %v, wait %v (fresh %v)", i, time.Duration(env.clock.Now()), timing, replay, admitted, wait, fresh)
			if admitted || !replay || wait != fresh {
				t.Errorf("replay %d: admitted %v, replay %v, wait %v, want a new attempt waiting %v", i, admitted, replay, wait, fresh)
			}
			env.tab.RemoveNode(env.adv.ID())
		}
	})

	t.Run("within grace", func(t *testing.T) {
		env := setup(t)
		// 6s late: past the 5s window, inside the 10s grace.
		env.clock.Run(env.w + 6*time.Second)
		if admitted, wait, timing, _ := env.present(env.t0); !admitted || timing != topicindex.TicketLate {
			t.Fatalf("late ticket: timing %v, admitted %v, wait %v", timing, admitted, wait)
		}
		ads := len(env.tab.Nodes(topic))
		env.clock.Run(time.Second)
		if admitted, wait, _, replay := env.present(env.t0); !admitted || !replay {
			t.Fatalf("second presentation not a no-op replay: admitted %v, replay %v, wait %v", admitted, replay, wait)
		}
		if n := len(env.tab.Nodes(topic)); n != ads {
			t.Fatalf("second presentation changed the ads: %d, want %d", n, ads)
		}
	})
}

// TestHandleRegtopicReplay checks that a ticket credited once is worth nothing
// when presented again, even after the ad it earned was evicted.
func TestHandleRegtopicReplay(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t)
	defer test.close()
	seedTopicTable(t, test.udp, testTopic1, enode.NewV4(&newkey().PublicKey, net.IP{10, 0, 9, 1}, 30303, 30303))

	key := newkey()
	addr := netip.MustParseAddrPort("10.0.1.1:30303")
	now := test.udp.clock.Now()
	ticket := sealTicket(test, &topicindex.Ticket{
		Topic:          testTopic1,
		Node:           enode.PubkeyToIDV4(&key.PublicKey),
		LastUsed:       now.Add(-2 * time.Second),
		WaitTimeIssued: time.Second,
		FirstIssued:    now.Add(-time.Minute),
	})
	register := func(reqID byte) *v5wire.Regconfirmation {
		var conf *v5wire.Regconfirmation
		test.packetInFrom(key, addr, &v5wire.Regtopic{
			ReqID: []byte{reqID}, Topic: testTopic1, Ticket: ticket,
			ENR: signedRecord(t, key, addr.Addr(), netip.Addr{}, addr.Port()),
		})
		test.waitPacketOut(func(p *v5wire.Regconfirmation, _ netip.AddrPort, _ v5wire.Nonce) { conf = p })
		return conf
	}

	if conf := register(1); len(conf.Ticket) != 0 || conf.WaitTime != waitTimeToMs(test.udp.topicTable.AdLifetime()) {
		t.Fatalf("on-time ticket not admitted: ticket %d bytes, wait time %d", len(conf.Ticket), conf.WaitTime)
	}
	if n := len(test.udp.LocalTopicNodes(testTopic1)); n != 2 {
		t.Fatalf("%d ads after admission, want 2", n)
	}
	test.udp.evictTopicTableNode(enode.PubkeyToIDV4(&key.PublicKey))
	if n := len(test.udp.LocalTopicNodes(testTopic1)); n != 1 {
		t.Fatalf("%d ads after eviction, want 1", n)
	}
	conf := register(2)
	if len(conf.Ticket) == 0 || conf.WaitTime < 40000 || conf.WaitTime > 50000 {
		t.Errorf("replayed ticket: ticket %d bytes, wait time %d, want a new ticket and a wait in [40000, 50000]", len(conf.Ticket), conf.WaitTime)
	}
	if n := len(test.udp.LocalTopicNodes(testTopic1)); n != 1 {
		t.Fatalf("%d ads after the replay, want 1", n)
	}
}

// TestHandleRegtopicForeignTicket checks that a ticket is bound to the node it
// was issued to: presented by another node it is worth nothing and is not used
// up, so that its owner can still redeem it in its window.
func TestHandleRegtopicForeignTicket(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t)
	defer test.close()
	seedTopicTable(t, test.udp, testTopic1, enode.NewV4(&newkey().PublicKey, net.IP{10, 0, 9, 1}, 30303, 30303))

	var (
		ownerKey   = newkey()
		ownerAddr  = netip.MustParseAddrPort("10.0.1.1:30303")
		holderKey  = newkey()
		holderAddr = netip.MustParseAddrPort("10.0.2.1:30303")
		now        = test.udp.clock.Now()
	)
	ticket := sealTicket(test, &topicindex.Ticket{
		Topic:          testTopic1,
		Node:           enode.PubkeyToIDV4(&ownerKey.PublicKey),
		LastUsed:       now.Add(-2 * time.Second),
		WaitTimeIssued: time.Second,
		FirstIssued:    now.Add(-time.Minute),
	})
	register := func(key *ecdsa.PrivateKey, addr netip.AddrPort, reqID byte) *v5wire.Regconfirmation {
		var conf *v5wire.Regconfirmation
		test.packetInFrom(key, addr, &v5wire.Regtopic{
			ReqID: []byte{reqID}, Topic: testTopic1, Ticket: ticket,
			ENR: signedRecord(t, key, addr.Addr(), netip.Addr{}, addr.Port()),
		})
		test.waitPacketOut(func(p *v5wire.Regconfirmation, _ netip.AddrPort, _ v5wire.Nonce) { conf = p })
		return conf
	}

	conf := register(holderKey, holderAddr, 1)
	if len(conf.Ticket) == 0 || conf.WaitTime < 40000 || conf.WaitTime > 50000 {
		t.Fatalf("foreign ticket: ticket %d bytes, wait time %d, want a new ticket and a wait in [40000, 50000]", len(conf.Ticket), conf.WaitTime)
	}
	if n := len(test.udp.LocalTopicNodes(testTopic1)); n != 1 {
		t.Fatalf("%d ads after the foreign ticket, want 1", n)
	}
	issued := make(chan *topicindex.Ticket, 1)
	test.udp.onDispatchCh <- func() {
		tk, err := test.udp.ticketSealer.Unpack(testTopic1, conf.Ticket)
		if err != nil {
			t.Error(err)
		}
		issued <- tk
	}
	if tk := <-issued; tk == nil || tk.Node != enode.PubkeyToIDV4(&holderKey.PublicKey) {
		t.Fatal("new ticket not issued to its holder")
	}

	conf = register(ownerKey, ownerAddr, 2)
	if len(conf.Ticket) != 0 || conf.WaitTime != waitTimeToMs(test.udp.topicTable.AdLifetime()) {
		t.Fatalf("owner's ticket not admitted after foreign use: ticket %d bytes, wait time %d", len(conf.Ticket), conf.WaitTime)
	}
	if n := len(test.udp.LocalTopicNodes(testTopic1)); n != 2 {
		t.Fatalf("%d ads after the owner's ticket, want 2", n)
	}
}

// TestTopicRegZeroWaitTicketClamped checks that a ticket quoting less than the
// wait time floor is retried after the floor, and counts no liveness failure.
func TestTopicRegZeroWaitTicketClamped(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t)
	defer test.close()

	key := newkey()
	addr := netip.MustParseAddrPort("10.0.3.47:30303")
	ln := test.getNode(key, addr)
	ln.Set(topicindex.TopicDiscoveryVersion)
	n := ln.Node()

	test.table.addFoundNode(n, true)
	test.udp.RegisterTopic(testTopic1, 1)
	var replied time.Time
	test.waitPacketOut(func(p *v5wire.Regtopic, a netip.AddrPort, _ v5wire.Nonce) {
		replied = time.Now()
		test.packetInFrom(key, a, &v5wire.Regconfirmation{ReqID: p.ReqID, RespCount: 1, Ticket: []byte("t"), WaitTime: 0})
	})
	test.waitPacketOut(func(p *v5wire.Regtopic, _ netip.AddrPort, _ v5wire.Nonce) {
		if d := time.Since(replied); d < time.Second {
			t.Errorf("zero-wait ticket retried after %v, want at least 1s", d)
		}
		if !bytes.Equal(p.Ticket, []byte("t")) {
			t.Errorf("retry carries ticket %x, want the quoted one", p.Ticket)
		}
	})
	if fails := test.db.FindFails(n.ID(), n.IPAddr()); fails != 0 {
		t.Errorf("zero-wait ticket led to %d wire failures, want 0", fails)
	}
}

// TestTopicRegHugeWaitTime checks that a wait time too large for a Duration
// saturates: a ticket is dropped as above budget rather than retried, and an
// ad lifetime is capped rather than expiring at once.
func TestTopicRegHugeWaitTime(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		ticket []byte
		check  func(t *testing.T, test *udpV5Test)
	}{
		// Dropped above budget, the registrar is asked again only as a first
		// contact. A wrapped wait would be clamped and retried with the ticket.
		{"ticket", []byte("t"), func(t *testing.T, test *udpV5Test) {
			test.waitPacketOut(func(p *v5wire.Regtopic, _ netip.AddrPort, _ v5wire.Nonce) {
				if len(p.Ticket) != 0 {
					t.Errorf("maximal-wait ticket %x retried", p.Ticket)
				}
			})
		}},
		// Registered for an ad lifetime. A wrapped lifetime would expire at
		// once and the next pass would register again.
		{"ad lifetime", nil, func(t *testing.T, test *udpV5Test) {
			quiet := 2*regloopMinTime + 500*time.Millisecond
			time.Sleep(quiet)
			if q := pipeQueued(test); q != 0 {
				t.Errorf("%d packets sent within %v of a maximal ad lifetime", q, quiet)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			test := newUDPV5Test(t)
			defer test.close()

			key := newkey()
			addr := netip.MustParseAddrPort("10.0.3.48:30303")
			ln := test.getNode(key, addr)
			ln.Set(topicindex.TopicDiscoveryVersion)

			test.table.addFoundNode(ln.Node(), true)
			test.udp.RegisterTopic(testTopic1, 1)
			test.waitPacketOut(func(p *v5wire.Regtopic, a netip.AddrPort, _ v5wire.Nonce) {
				test.packetInFrom(key, a, &v5wire.Regconfirmation{ReqID: p.ReqID, RespCount: 1, Ticket: tc.ticket, WaitTime: ^uint(0)})
			})
			tc.check(t, test)
		})
	}
}

// TestTopicRegRetriesNotBlockedByDeadRegistrars checks that ticket retries go
// out within their window while most first contacts time out at dead
// registrars.
func TestTopicRegRetriesNotBlockedByDeadRegistrars(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t)
	defer test.close()

	const (
		nlive    = 4
		ndead    = 12
		waitTime = 1500 * time.Millisecond
	)
	topic := makeTopic("retries")

	// At most four registrars per registration bucket, so that every one of
	// them is contacted right away.
	type registrar struct {
		key     *ecdsa.PrivateKey
		node    *enode.Node
		ticket  []byte
		replied time.Time
		retried bool
	}
	var (
		regs    []*registrar
		byAddr  = make(map[netip.AddrPort]*registrar)
		perDist = make(map[int]int)
	)
	for len(regs) < nlive+ndead {
		key := newkey()
		d := min(256-enode.LogDist(enode.ID(topic), enode.PubkeyToIDV4(&key.PublicKey)), 17)
		if perDist[d] == 4 {
			continue
		}
		perDist[d]++
		addr := netip.AddrPortFrom(netip.AddrFrom4([4]byte{10, 0, byte(len(regs) + 1), 1}), 30303)
		ln := test.getNode(key, addr)
		ln.Set(topicindex.TopicDiscoveryVersion)
		r := &registrar{key: key, node: ln.Node()}
		regs = append(regs, r)
		byAddr[addr] = r
	}
	live, dead := regs[:nlive], regs[nlive:]

	for _, r := range live {
		test.table.addFoundNode(r.node, true)
	}
	test.udp.RegisterTopic(topic, 1)
	for i := range live {
		test.waitPacketOut(func(p *v5wire.Regtopic, addr netip.AddrPort, _ v5wire.Nonce) {
			r := byAddr[addr]
			if r.ticket != nil || len(p.Ticket) != 0 {
				t.Fatalf("unexpected REGTOPIC to %v", addr)
			}
			r.ticket = []byte{byte(i + 1)}
			r.replied = time.Now()
			test.packetInFrom(r.key, addr, &v5wire.Regconfirmation{
				ReqID:    p.ReqID,
				Ticket:   r.ticket,
				WaitTime: uint(waitTime.Milliseconds()),
			})
		})
	}

	for _, r := range dead {
		test.table.addFoundNode(r.node, true)
	}
	var retried, deadSeen int
	for retried < nlive || deadSeen < ndead {
		test.waitPacketOut(func(p *v5wire.Regtopic, addr netip.AddrPort, _ v5wire.Nonce) {
			r := byAddr[addr]
			if r.ticket == nil {
				deadSeen++
				return
			}
			if r.retried || !bytes.Equal(p.Ticket, r.ticket) {
				t.Fatalf("unexpected REGTOPIC to %v with ticket %x", addr, p.Ticket)
			}
			delay := time.Since(r.replied)
			t.Logf("retry to %v after %v (expected below %v)", addr, delay, waitTime+2*test.udp.respTimeout)
			if delay < waitTime || delay >= waitTime+5*time.Second {
				t.Errorf("retry to %v after %v, want within [%v, %v)", addr, delay, waitTime, waitTime+5*time.Second)
			}
			r.retried = true
			retried++
			test.packetInFrom(r.key, addr, &v5wire.Regconfirmation{ReqID: p.ReqID, WaitTime: 900000})
		})
	}
}

// TestTopicRegRejectedNotPenalized checks that a rejected registration keeps
// the registrar out of the registration table without counting toward its
// DHT eviction.
func TestTopicRegRejectedNotPenalized(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t)
	defer test.close()

	key := newkey()
	addr := netip.MustParseAddrPort("10.0.3.45:30303")
	ln := test.getNode(key, addr)
	ln.Set(topicindex.TopicDiscoveryVersion)
	n := ln.Node()

	test.table.addFoundNode(n, true)
	test.udp.RegisterTopic(testTopic1, 1)
	test.waitPacketOut(func(p *v5wire.Regtopic, a netip.AddrPort, _ v5wire.Nonce) {
		test.packetInFrom(key, a, &v5wire.Regconfirmation{ReqID: p.ReqID, RespCount: 1})
	})

	// The registration loop re-seeds from the table, but n is not admitted again.
	time.Sleep(2*regloopMinTime + 500*time.Millisecond)
	if q := pipeQueued(test); q != 0 {
		t.Fatalf("%d packets sent to a registrar that rejected the request", q)
	}
	if fails := test.db.FindFails(n.ID(), n.IPAddr()); fails != 0 {
		t.Fatalf("rejection counted as %d wire failures, want 0", fails)
	}
}

// TestTopicRegTicketRetry checks that the advertiser retries with the ticket
// once the quoted wait time has passed.
func TestTopicRegTicketRetry(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t)
	defer test.close()

	key := newkey()
	addr := netip.MustParseAddrPort("10.0.3.46:30303")
	ln := test.getNode(key, addr)
	ln.Set(topicindex.TopicDiscoveryVersion)
	test.table.addFoundNode(ln.Node(), true)
	test.udp.RegisterTopic(testTopic1, 1)

	// The shortest wait a registrar quotes; the advertiser discards shorter ones.
	const waitTime = time.Second
	ticket := []byte("ticket")
	var replied time.Time
	test.waitPacketOut(func(p *v5wire.Regtopic, a netip.AddrPort, _ v5wire.Nonce) {
		replied = time.Now()
		test.packetInFrom(key, a, &v5wire.Regconfirmation{ReqID: p.ReqID, Ticket: ticket, WaitTime: uint(waitTime.Milliseconds())})
	})
	test.waitPacketOut(func(p *v5wire.Regtopic, a netip.AddrPort, _ v5wire.Nonce) {
		if !bytes.Equal(p.Ticket, ticket) {
			t.Errorf("retry carries ticket %x, want %x", p.Ticket, ticket)
		}
		if d := time.Since(replied); d < waitTime {
			t.Errorf("retry after %v, want at least %v", d, waitTime)
		}
		test.packetInFrom(key, a, &v5wire.Regconfirmation{ReqID: p.ReqID, WaitTime: 900000})
	})
}

// TestTopicSearchDrainsTableFeed checks that a search blocked on a consumer
// that stops reading keeps draining the table's node feed: the feed sends
// under the table mutex, so a full subscription channel would stall every
// lookup. The result channel buffers 200 results, so the pass must carry more.
func TestTopicSearchDrainsTableFeed(t *testing.T) {
	node0 := startLocalhostV5(t, Config{})
	var all []*UDPv5
	defer func() {
		node0.Close()
		for _, n := range all {
			n.Close()
		}
	}()
	var regs []*enode.Node
	for i := 0; i < 14; i++ {
		reg := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{node0.Self()}})
		all = append(all, reg)
		regs = append(regs, reg.Self())
		var own []*enode.Node
		for j := 0; j < 16; j++ {
			own = append(own, capableRecord(t, 41000+100*i+j))
		}
		seedTopicTable(t, reg, testTopic1, own...)
	}
	searcher := startLocalhostV5(t, Config{Bootnodes: append([]*enode.Node{node0.Self()}, regs...)})
	all = append(all, searcher)

	it := searcher.TopicSearch(testTopic1, 0)
	defer it.Close()
	if !it.Next() {
		t.Fatal("no first result")
	}
	// The consumer reads nothing more; the pass fills the result channel and
	// the search waits on it. The feed must still get through.
	time.Sleep(2 * time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 300; i++ {
			searcher.tab.nodeFeed.Send(capableRecord(t, 43000+i))
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the table feed blocked behind the search's full subscription channel")
	}
}

// TestTopicSearchNextPassEmptyTopic checks that a pass without a single ad
// counts as idle wherever it lands, and that the search state starts it wide
// again once a pass at the closest bucket saw none, while a pass with ads that
// moved counts as a move.
func TestTopicSearchNextPassEmptyTopic(t *testing.T) {
	s := &topicSearch{activeBucket: 3}
	state := topicindex.NewSearch(topicindex.TopicID{1}, topicindex.Config{})
	state.SetActiveBucket(5)
	if s.nextPass(state) {
		t.Fatal("a pass without ads counted as a move")
	}
	if s.activeBucket != 6 {
		t.Fatalf("next pass at %d, want 6: an empty pass that ran dry walks one bucket in", s.activeBucket)
	}
	state.SetActiveBucket(1 << 10) // clamps to the closest bucket
	last := state.ActiveBucket()
	state.SetActiveBucket(last - 1)
	if s.nextPass(state) || s.activeBucket != last {
		t.Fatalf("next pass at %d after landing at the closest bucket, want %d", s.activeBucket, last)
	}
	// A pass at the closest bucket with nothing found: start wide again.
	state.SetActiveBucket(last)
	near := enode.SignNull(new(enr.Record), enode.ID{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
	state.AddReply(near, nil, 0, 0)
	if s.nextPass(state) || s.activeBucket != 0 {
		t.Fatalf("next pass at %d after an empty reply at the closest bucket, want 0", s.activeBucket)
	}
	state = topicindex.NewSearch(topicindex.TopicID{1}, topicindex.Config{})
	state.SetActiveBucket(0)
	state.AddReply(enode.SignNull(new(enr.Record), enode.ID{7}), nil, 3, 3)
	s.activeBucket = 2
	if !s.nextPass(state) || s.activeBucket != 1 {
		t.Fatalf("next pass at %d after a pass with ads that ran dry, want 1 and a move", s.activeBucket)
	}
}

// TestTopicSearchNextPassDeadFrontier checks that a bucket whose only node
// never answers is kept for one more pass, which does not ask the dead node
// again, and not more: the search then walks on.
func TestTopicSearchNextPassDeadFrontier(t *testing.T) {
	topic := topicindex.TopicID{1}
	dead := nodeAtDistance(enode.ID(topic), 256-10, net.IP{203, 0, 113, 1})
	helper := nodeAtDistance(enode.ID(topic), 256-3, net.IP{198, 51, 100, 1})
	s := &topicSearch{
		activeBucket: 10,
		asked:        topicindex.NewAskedSet(topicindex.Config{}),
		failed:       topicindex.NewAskedSet(topicindex.Config{}),
	}
	var got []int
	var deadAsked int
	for range 6 {
		state := topicindex.NewSearch(topic, topicindex.Config{})
		state.SetActiveBucket(s.activeBucket)
		state.SetAsked(s.asked)
		state.SetFailed(s.failed)
		state.AddNodes(nil, []*enode.Node{dead, helper})
		for n := state.QueryTarget(); n != nil; n = state.QueryTarget() {
			if n.ID() == dead.ID() {
				deadAsked++
				state.HandleErrorResponse(n, errTimeout)
			} else {
				state.AddReply(n, nil, 0, 0)
			}
		}
		if !state.IsDone() {
			t.Fatal("pass not done with every node asked")
		}
		s.nextPass(state)
		got = append(got, s.activeBucket)
	}
	if want := []int{10, 11, 12, 13, 14, 15}; !slices.Equal(got, want) {
		t.Fatalf("passes went to %v, want %v", got, want)
	}
	if deadAsked != 1 {
		t.Fatalf("dead node asked %d times, want once", deadAsked)
	}
}

// TestTopicSearchNextPassPlacedFailures checks that a bucket placed by density
// samples is kept even when every query to it failed: it is no retry.
func TestTopicSearchNextPassPlacedFailures(t *testing.T) {
	topic := topicindex.TopicID{1}
	dead := nodeAtDistance(enode.ID(topic), 256-10, net.IP{203, 0, 113, 1})
	helpers := []*enode.Node{
		nodeAtDistance(enode.ID(topic), 256-11, net.IP{198, 51, 100, 1}),
		nodeAtDistance(enode.ID(topic), 256-11, net.IP{192, 0, 2, 1}),
	}
	s := &topicSearch{activeBucket: 10, asked: topicindex.NewAskedSet(topicindex.Config{})}
	var got []int
	for range 2 {
		state := topicindex.NewSearch(topic, topicindex.Config{})
		state.SetActiveBucket(s.activeBucket)
		state.SetAsked(s.asked)
		state.AddNodes(nil, append([]*enode.Node{dead}, helpers...))
		for n := state.QueryTarget(); n != nil; n = state.QueryTarget() {
			if n.ID() == dead.ID() {
				state.HandleErrorResponse(n, errTimeout)
			} else {
				state.AddReply(n, nil, topicindex.TopicNodesLimit, topicindex.TopicNodesLimit)
			}
		}
		if !state.IsDone() {
			t.Fatal("pass not done with every node asked")
		}
		if s.nextPass(state) {
			t.Fatal("a pass placed at its own bucket counted as a move")
		}
		got = append(got, s.activeBucket)
	}
	if want := []int{10, 10}; !slices.Equal(got, want) {
		t.Fatalf("passes went to %v, want %v", got, want)
	}
}

// TestTopicSearchRunPartialReply drives one pass of the run loop by hand:
// replies that carry ads but end in an error reach the state as partial
// replies, so their ads count and, below the floor, they are no density
// sample. Two of them would move the active bucket as samples.
func TestTopicSearchRunPartialReply(t *testing.T) {
	node := startLocalhostV5(t, Config{})
	defer node.Close()
	sys := node.topicSys
	s := &topicSearch{
		topic:        testTopic1,
		config:       sys.config,
		quit:         make(chan struct{}),
		resultCh:     make(chan *enode.Node, 200),
		resultFilter: topicindex.NewSearchFilter(sys.config),
		asked:        topicindex.NewAskedSet(sys.config),
		queryCh:      make(chan topicQueryJob),
		queryRespCh:  make(chan topicQueryResult),
		readCh:       make(chan struct{}, 1),
		newNodesCh:   make(chan *enode.Node, 100),
	}
	state := topicindex.NewSearch(testTopic1, sys.config)
	active := state.ActiveBucket()
	// Two registrars in the active bucket.
	regs := map[enode.ID]*enode.Node{}
	for port := 45000; len(regs) < 2; port++ {
		if n := capableRecord(t, port); enode.LogDist(enode.ID(testTopic1), n.ID()) == 256-active {
			regs[n.ID()] = n
		}
	}
	for _, n := range regs {
		state.AddNodes(nil, []*enode.Node{n})
	}

	// A consumer that reads everything, as the iterator does: the pass
	// cannot end while a result waits unread.
	var read atomic.Int32
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-s.resultCh:
				read.Add(1)
				select {
				case s.readCh <- struct{}{}:
				default:
				}
			case <-stop:
				return
			}
		}
	}()
	defer close(stop)

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.run(sys, state)
	}()
	var nads int
	for i := range len(regs) {
		var job topicQueryJob
		select {
		case job = <-s.queryCh:
			if regs[job.dst.ID()] == nil {
				t.Fatalf("query to %v, want a registrar", job.dst.ID())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no query sent")
		}
		ads := []*enode.Node{capableRecord(t, 46000+2*i), capableRecord(t, 46001+2*i)}
		nads += len(ads)
		s.queryRespCh <- topicQueryResult{src: job.dst, topicNodes: ads, err: errTimeout}
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(s.quit)
		t.Fatal("the pass did not end after the partial replies")
	}
	if got := state.AdsSeen(); got != nads {
		t.Fatalf("ads seen %d, want %d: partial replies' ads count", got, nads)
	}
	if state.ActiveBucket() != active {
		t.Fatalf("active bucket moved to %d on partial replies below the floor", state.ActiveBucket())
	}
	if got := int(read.Load()); got != nads {
		t.Fatalf("%d results handed out, want %d: the pass end releases the partial replies' ads", got, nads)
	}
}

// TestTopicSearchRunUnsupportedAds checks that records the searcher filters
// out still count toward the density sample, as the registrar sent them, but
// not as ads seen: a pass that found nothing usable is idle.
func TestTopicSearchRunUnsupportedAds(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"complete", nil},
		{"partial", errTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := startLocalhostV5(t, Config{})
			defer node.Close()
			sys := node.topicSys
			s := &topicSearch{
				topic:        testTopic1,
				config:       sys.config,
				quit:         make(chan struct{}),
				resultCh:     make(chan *enode.Node, 200),
				resultFilter: topicindex.NewSearchFilter(sys.config),
				asked:        topicindex.NewAskedSet(sys.config),
				queryCh:      make(chan topicQueryJob),
				queryRespCh:  make(chan topicQueryResult),
				readCh:       make(chan struct{}, 1),
				newNodesCh:   make(chan *enode.Node, 100),
				activeBucket: 1,
			}
			state := topicindex.NewSearch(testTopic1, sys.config)
			state.SetActiveBucket(s.activeBucket)
			regs := map[enode.ID]*enode.Node{}
			for port := 45000; len(regs) < 2; port++ {
				if n := capableRecord(t, port); enode.LogDist(enode.ID(testTopic1), n.ID()) == 256-s.activeBucket {
					regs[n.ID()] = n
				}
			}
			for _, n := range regs {
				state.AddNodes(nil, []*enode.Node{n})
			}

			done := make(chan int)
			go func() {
				_, nresults := s.run(sys, state)
				done <- nresults
			}()
			for range len(regs) {
				var job topicQueryJob
				select {
				case job = <-s.queryCh:
				case <-time.After(5 * time.Second):
					t.Fatal("no query sent")
				}
				// Full replies of records without the topic discovery entry.
				unsupported := nodesAtDistance(enode.ID(testTopic1), 200, topicindex.TopicNodesLimit)
				s.queryRespCh <- topicQueryResult{src: job.dst, topicNodes: unsupported, err: tc.err}
			}
			var nresults int
			select {
			case nresults = <-done:
			case <-time.After(5 * time.Second):
				close(s.quit)
				t.Fatal("the pass did not end after the replies")
			}
			if got := state.AdsSeen(); got != 0 {
				t.Fatalf("ads seen %d, want 0: unsupported records are no ads", got)
			}
			if got := state.ActiveBucket(); got != 0 {
				t.Fatalf("active bucket %d, want 0: full replies as sent move the search out", got)
			}
			moved := s.nextPass(state)
			if moved {
				t.Fatal("a pass without usable ads counted as a move")
			}
			s.passGap(nresults, moved)
			if s.idlePasses != 1 {
				t.Fatalf("idle passes %d, want 1: a pass without usable ads is idle", s.idlePasses)
			}
		})
	}
}
