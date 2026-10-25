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

// TestTopicRegTimeoutBans checks that a REGTOPIC timeout bans the registrar
// from topic registration for an ad lifetime. The node stays in the routing
// table and its ad stays in the ad cache: a single timeout is only counted
// toward the DHT's consecutive wire-failure tally.
func TestTopicRegTimeoutBans(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t)
	defer test.close()
	const lifetime = 15 * time.Minute
	clock := new(mclock.Simulated)
	setBanClock(test.udp.topicSys, clock, lifetime)

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

	waitForCond(t, "regtopic timeout bans the node", func() bool {
		return test.udp.topicSys.isBanned(n.ID())
	})
	waitForCond(t, "regtopic timeout counted toward wire-failure tally", func() bool {
		return test.db.FindFails(n.ID(), n.IPAddr()) >= 1
	})
	if test.table.getNode(n.ID()) == nil {
		t.Fatal("banned node removed from the routing table")
	}
	if got := test.udp.LocalTopicNodes(topic); len(got) != 1 || got[0].ID() != n.ID() {
		t.Fatalf("ad evicted on first timeout; eviction should defer to the DHT threshold: %v", got)
	}
	// The registration loop re-seeds from the table, but skips the banned node.
	time.Sleep(regloopMinTime + 500*time.Millisecond)
	if q := pipeQueued(test); q != 0 {
		t.Fatalf("%d packets sent to a banned node", q)
	}

	// Once the ban expires, the node is seeded again from the table.
	clock.Run(lifetime + time.Second)
	waitQueued(t, test, 10*time.Second)
	test.waitPacketOut(func(p *v5wire.Regtopic, _ netip.AddrPort, _ v5wire.Nonce) {})
}

// TestTopicSearchTimeoutBans checks that a TOPICQUERY timeout bans the node
// from topic search until the ban expires.
func TestTopicSearchTimeoutBans(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t)
	defer test.close()
	const lifetime = 15 * time.Minute
	clock := new(mclock.Simulated)
	setBanClock(test.udp.topicSys, clock, lifetime)

	ln := test.getNode(newkey(), netip.MustParseAddrPort("10.0.2.56:30303"))
	ln.Set(topicindex.TopicDiscoveryVersion)
	n := ln.Node()
	test.table.addFoundNode(n, true)

	it := test.udp.TopicSearch(testTopic1, 0)
	defer it.Close()
	test.waitPacketOut(func(p *v5wire.TopicQuery, _ netip.AddrPort, _ v5wire.Nonce) {})
	waitForCond(t, "topicquery timeout bans the node", func() bool {
		return test.udp.topicSys.isBanned(n.ID())
	})
	// The next pass starts after a gap of 2*regloopMinTime and skips the node.
	time.Sleep(2*regloopMinTime + 500*time.Millisecond)
	if q := pipeQueued(test); q != 0 {
		t.Fatalf("%d packets sent to a banned node", q)
	}

	clock.Run(lifetime + time.Second)
	waitQueued(t, test, 10*time.Second)
	test.waitPacketOut(func(p *v5wire.TopicQuery, _ netip.AddrPort, _ v5wire.Nonce) {})
}

// TestTopicBannedAuxNodeFiltered checks that a banned node arriving as an aux
// node is not queried, and that a reply from a banned node lifts its ban.
func TestTopicBannedAuxNodeFiltered(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t)
	defer test.close()

	key := newkey()
	ln := test.getNode(key, netip.MustParseAddrPort("10.0.2.57:30303"))
	ln.Set(topicindex.TopicDiscoveryVersion)
	n := ln.Node()
	test.table.addFoundNode(n, true)
	auxln := test.getNode(newkey(), netip.MustParseAddrPort("10.0.2.58:30303"))
	auxln.Set(topicindex.TopicDiscoveryVersion)
	aux := auxln.Node()
	test.udp.topicSys.ban(aux.ID())

	it := test.udp.TopicSearch(testTopic1, 0)
	defer it.Close()
	test.waitPacketOut(func(p *v5wire.TopicQuery, a netip.AddrPort, _ v5wire.Nonce) {
		// Banned while the query is in flight: the reply lifts the ban.
		test.udp.topicSys.ban(n.ID())
		test.packetInFrom(key, a, &v5wire.Nodes{ReqID: p.ReqID, RespCount: 1, Nodes: []*enr.Record{aux.Record()}})
	})
	waitForCond(t, "reply lifts the ban", func() bool {
		return !test.udp.topicSys.isBanned(n.ID())
	})
	time.Sleep(time.Second)
	if q := pipeQueued(test); q != 0 {
		t.Fatalf("%d packets sent after the reply, want none to the banned aux node", q)
	}
	if !test.udp.topicSys.isBanned(aux.ID()) {
		t.Fatal("aux node no longer banned")
	}
}

// TestTopicRegDispatchSkipsBanned checks that a registration attempt whose
// node was banned after it entered the table is dropped when it comes due.
func TestTopicRegDispatchSkipsBanned(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t)
	defer test.close()
	sys := test.udp.topicSys

	topic := topicindex.TopicID{8, 8, 8}
	key := newkey()
	ln := test.getNode(key, netip.MustParseAddrPort("10.0.2.59:30303"))
	ln.Set(topicindex.TopicDiscoveryVersion)
	n := ln.Node()
	test.table.addFoundNode(n, true)
	sys.register(topic, 1)

	// A ticket schedules the next attempt; the node is banned before it is due.
	// The reply itself lifts a ban, which shows when it has been handled.
	const wait = time.Second
	test.waitPacketOut(func(p *v5wire.Regtopic, a netip.AddrPort, _ v5wire.Nonce) {
		sys.ban(n.ID())
		test.packetInFrom(key, a, &v5wire.Regconfirmation{
			ReqID:    p.ReqID,
			Ticket:   []byte{1},
			WaitTime: uint(wait / time.Millisecond),
		})
	})
	waitForCond(t, "reply lifts the ban", func() bool { return !sys.isBanned(n.ID()) })
	sys.ban(n.ID())
	time.Sleep(wait + time.Second)
	if q := pipeQueued(test); q != 0 {
		t.Fatalf("%d packets sent to a banned node", q)
	}

	sys.mu.Lock()
	reg := sys.reg[topic]
	sys.mu.Unlock()
	sys.stopRegister(topic)
	if c := reg.state.NodeCount(); c != 0 {
		t.Fatalf("registration table holds %d nodes, want the banned attempt gone", c)
	}
}

// TestTopicSearchDispatchSkipsBanned checks that a search does not query a
// node that was banned after the pass picked it up. The banned node is the
// last target of the pass, which must still roll over.
func TestTopicSearchDispatchSkipsBanned(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t)
	defer test.close()
	sys := test.udp.topicSys
	const lifetime = 15 * time.Minute
	clock := new(mclock.Simulated)
	setBanClock(sys, clock, lifetime)

	keys := map[netip.AddrPort]*ecdsa.PrivateKey{}
	ids := map[netip.AddrPort]enode.ID{}
	for _, a := range []string{"10.0.4.1:30303", "10.0.5.1:30303"} {
		addr := netip.MustParseAddrPort(a)
		keys[addr] = newkey()
		ln := test.getNode(keys[addr], addr)
		ln.Set(topicindex.TopicDiscoveryVersion)
		test.table.addFoundNode(ln.Node(), true)
		ids[addr] = ln.Node().ID()
	}

	it := test.udp.TopicSearch(testTopic1, 0)
	defer it.Close()
	// Ban the other node while the first query is in flight, then answer.
	var banned netip.AddrPort
	test.waitPacketOut(func(p *v5wire.TopicQuery, a netip.AddrPort, _ v5wire.Nonce) {
		for addr, id := range ids {
			if addr != a {
				banned = addr
				sys.ban(id)
			}
		}
		test.packetInFrom(keys[a], a, &v5wire.Nodes{ReqID: p.ReqID, RespCount: 1})
	})
	time.Sleep(time.Second)
	if q := pipeQueued(test); q != 0 {
		t.Fatalf("%d packets sent after the reply, want none to the banned node", q)
	}

	// The pass rolls over: the next one queries the other node again.
	waitQueued(t, test, 10*time.Second)
	test.waitPacketOut(func(p *v5wire.TopicQuery, a netip.AddrPort, _ v5wire.Nonce) {
		if a == banned {
			t.Fatal("banned node queried")
		}
		clock.Run(lifetime + time.Second)
		test.packetInFrom(keys[a], a, &v5wire.Nodes{ReqID: p.ReqID, RespCount: 1})
	})
	// Once the ban expires, the node is queried again.
	for deadline := time.Now().Add(30 * time.Second); ; {
		if time.Now().After(deadline) {
			t.Fatal("ban expired, but the node was not queried again")
		}
		waitQueued(t, test, 20*time.Second)
		var to netip.AddrPort
		test.waitPacketOut(func(p *v5wire.TopicQuery, a netip.AddrPort, _ v5wire.Nonce) {
			if to = a; a != banned {
				test.packetInFrom(keys[a], a, &v5wire.Nodes{ReqID: p.ReqID, RespCount: 1})
			}
		})
		if to == banned {
			return
		}
	}
}

// TestTopicRegBannedBeforeSend checks that an attempt banned while it waits
// for the busy request worker is dropped without sending REGTOPIC.
func TestTopicRegBannedBeforeSend(t *testing.T) {
	t.Parallel()
	test := newUDPV5Test(t)
	defer test.close()
	sys := test.udp.topicSys

	topic := topicindex.TopicID{6, 6, 6}
	keys := map[netip.AddrPort]*ecdsa.PrivateKey{}
	ids := map[netip.AddrPort]enode.ID{}
	for _, a := range []string{"10.0.6.1:30303", "10.0.7.1:30303"} {
		addr := netip.MustParseAddrPort(a)
		keys[addr] = newkey()
		ln := test.getNode(keys[addr], addr)
		ln.Set(topicindex.TopicDiscoveryVersion)
		test.table.addFoundNode(ln.Node(), true)
		ids[addr] = ln.Node().ID()
	}
	sys.register(topic, 1)

	// The worker is busy with the first REGTOPIC while the loop selects the
	// other node's attempt. Ban that node, then release the worker.
	test.waitPacketOut(func(p *v5wire.Regtopic, a netip.AddrPort, _ v5wire.Nonce) {
		time.Sleep(200 * time.Millisecond)
		for addr, id := range ids {
			if addr != a {
				sys.ban(id)
			}
		}
		test.packetInFrom(keys[a], a, &v5wire.Regconfirmation{ReqID: p.ReqID, WaitTime: 900000})
	})
	time.Sleep(time.Second)
	if q := pipeQueued(test); q != 0 {
		t.Fatalf("%d packets sent after the worker was released, want none to the banned node", q)
	}

	sys.mu.Lock()
	reg := sys.reg[topic]
	sys.mu.Unlock()
	sys.stopRegister(topic)
	if c := reg.state.NodeCount(); c != 1 {
		t.Fatalf("registration table holds %d nodes, want only the registered one", c)
	}
}

// setBanClock makes the topic system's bans expire on clock.
func setBanClock(sys *topicSystem, clock mclock.Clock, lifetime time.Duration) {
	sys.banMu.Lock()
	defer sys.banMu.Unlock()
	sys.banned = topicindex.NewAskedSet(topicindex.Config{AdLifetime: lifetime, Clock: clock})
}

func pipeQueued(test *udpV5Test) int {
	test.pipe.mu.Lock()
	defer test.pipe.mu.Unlock()
	return len(test.pipe.queue)
}

// waitQueued waits up to d for a packet to be sent.
func waitQueued(t *testing.T, test *udpV5Test, d time.Duration) {
	t.Helper()
	for deadline := time.Now().Add(d); pipeQueued(test) == 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("no packet sent")
		}
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
	// The timeout banned n; lift the ban so the registration retries it.
	test.udp.topicSys.unban(n.ID())

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
// never answers is retried by one pass, not more: the search then walks on.
func TestTopicSearchNextPassDeadFrontier(t *testing.T) {
	topic := topicindex.TopicID{1}
	dead := nodeAtDistance(enode.ID(topic), 256-10, net.IP{203, 0, 113, 1})
	helper := nodeAtDistance(enode.ID(topic), 256-3, net.IP{198, 51, 100, 1})
	s := &topicSearch{activeBucket: 10, asked: topicindex.NewAskedSet(topicindex.Config{})}
	var got []int
	for range 6 {
		state := topicindex.NewSearch(topic, topicindex.Config{})
		state.SetActiveBucket(s.activeBucket)
		state.SetAsked(s.asked)
		state.AddNodes(nil, []*enode.Node{dead, helper})
		for n := state.QueryTarget(); n != nil; n = state.QueryTarget() {
			if n.ID() == dead.ID() {
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
