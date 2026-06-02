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

package topicindex

import (
	"net"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// TestBlacklist checks ban, lookup and TTL-based expiry.
func TestBlacklist(t *testing.T) {
	var clock mclock.Simulated
	bl := NewBlacklist(10*time.Minute, &clock)

	id := nodeAtDistance(enode.ID(topic1), 100, intIP(1)).ID()
	if bl.Contains(id) {
		t.Fatal("id banned before Ban")
	}
	bl.Ban(id)
	if !bl.Contains(id) {
		t.Fatal("id not banned after Ban")
	}

	// Still banned just before the TTL.
	clock.Run(9 * time.Minute)
	if !bl.Contains(id) {
		t.Fatal("ban expired too early")
	}

	// Expired after the TTL.
	clock.Run(2 * time.Minute)
	if bl.Contains(id) {
		t.Fatal("ban did not expire after TTL")
	}
}

// TestBlacklistDisabled checks that a zero/negative TTL disables the blacklist,
// and that a nil *Blacklist is safe to use.
func TestBlacklistDisabled(t *testing.T) {
	id := nodeAtDistance(enode.ID(topic1), 100, intIP(1)).ID()

	disabled := NewBlacklist(0, nil)
	disabled.Ban(id)
	if disabled.Contains(id) {
		t.Fatal("disabled blacklist reports a ban")
	}

	var nilBL *Blacklist
	nilBL.Ban(id) // must not panic
	if nilBL.Contains(id) {
		t.Fatal("nil blacklist reports a ban")
	}
}

func blacklistConfig(t *testing.T, bl *Blacklist) Config {
	cfg := testConfig(t)
	cfg.Blacklist = bl
	return cfg
}

// TestRegistrationRemoveNode checks eviction and blacklist gating in the
// registration table.
func TestRegistrationRemoveNode(t *testing.T) {
	var clock mclock.Simulated
	bl := NewBlacklist(time.Hour, &clock)
	r := NewRegistration(topic1, blacklistConfig(t, bl))

	n := nodeAtDistance(enode.ID(topic1), 200, net.IP{192, 0, 2, 1})
	r.AddNodes(nil, []*enode.Node{n})
	if r.NodeCount() != 1 {
		t.Fatalf("node not added, NodeCount=%d", r.NodeCount())
	}

	// RemoveNode drops it from the table.
	r.RemoveNode(n.ID())
	if r.NodeCount() != 0 {
		t.Fatalf("node not removed, NodeCount=%d", r.NodeCount())
	}

	// Once banned, it cannot be re-added.
	bl.Ban(n.ID())
	r.AddNodes(nil, []*enode.Node{n})
	if r.NodeCount() != 0 {
		t.Fatal("blacklisted node was re-added")
	}

	// After the ban expires, it can be added again.
	clock.Run(2 * time.Hour)
	r.AddNodes(nil, []*enode.Node{n})
	if r.NodeCount() != 1 {
		t.Fatal("node not added after ban expiry")
	}
}

// TestSearchRemoveNode checks eviction and blacklist gating in the search table.
func TestSearchRemoveNode(t *testing.T) {
	var clock mclock.Simulated
	bl := NewBlacklist(time.Hour, &clock)
	s := NewSearch(topic1, blacklistConfig(t, bl))

	n := nodeAtDistance(enode.ID(topic1), 200, net.IP{192, 0, 2, 1})
	s.AddNodes(nil, []*enode.Node{n})
	if !s.bucket(n.ID()).contains(n.ID()) {
		t.Fatal("node not added to search bucket")
	}

	s.RemoveNode(n.ID())
	if s.bucket(n.ID()).contains(n.ID()) {
		t.Fatal("node not removed from search bucket")
	}

	bl.Ban(n.ID())
	s.AddNodes(nil, []*enode.Node{n})
	if s.bucket(n.ID()).contains(n.ID()) {
		t.Fatal("blacklisted node was re-added to search bucket")
	}
}

// TestTopicTableRemoveNode checks that RemoveNode evicts a node's ads across all
// topics.
func TestTopicTableRemoveNode(t *testing.T) {
	tab := NewTopicTable(testConfig(t))
	topic2 := TopicID{9, 9, 9}

	n := nodeAtDistance(enode.ID(topic1), 200, net.IP{192, 0, 2, 1})
	other := nodeAtDistance(enode.ID(topic1), 201, net.IP{192, 0, 2, 2})
	tab.add(n, topic1)
	tab.add(n, topic2)
	tab.add(other, topic1)

	tab.RemoveNode(n.ID())

	if got := len(tab.Nodes(topic1)); got != 1 {
		t.Fatalf("topic1 has %d nodes after removal, want 1", got)
	}
	if got := len(tab.Nodes(topic2)); got != 0 {
		t.Fatalf("topic2 has %d nodes after removal, want 0", got)
	}
	if tab.all.Len() != 1 {
		t.Fatalf("ad cache has %d entries after removal, want 1", tab.all.Len())
	}
}
