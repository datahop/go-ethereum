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

package topicindex

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

func TestSearchFilterExpiry(t *testing.T) {
	clock := new(mclock.Simulated)
	f := NewSearchFilter(Config{AdLifetime: time.Minute, Clock: clock})
	n := newNode()

	if f.Seen(n) {
		t.Fatal("node seen before Add")
	}
	f.Add(n)
	clock.Run(time.Minute - 1)
	if !f.Seen(n) {
		t.Fatal("node not seen before expiry")
	}
	clock.Run(1)
	if f.Seen(n) {
		t.Fatal("node seen after expiry")
	}
	if len(f.seen) != 0 || f.order.Len() != 0 {
		t.Fatalf("filter not empty after expiry: %d entries, %d queued", len(f.seen), f.order.Len())
	}
}

func TestSearchFilterNewerRecord(t *testing.T) {
	clock := new(mclock.Simulated)
	f := NewSearchFilter(Config{AdLifetime: time.Minute, Clock: clock})
	id := newNode().ID()
	old, updated := nodeWithSeq(id, intIP(1), 1), nodeWithSeq(id, intIP(1), 2)

	f.Add(old)
	if f.Seen(updated) {
		t.Fatal("newer record filtered")
	}
	clock.Run(30 * time.Second)
	f.Add(updated)
	if !f.Seen(old) || !f.Seen(updated) {
		t.Fatal("record not seen after re-add")
	}
	// The first expiry must not drop the entry refreshed by the re-add.
	clock.Run(30 * time.Second)
	if !f.Seen(updated) {
		t.Fatal("re-added record expired with the original entry")
	}
	clock.Run(30 * time.Second)
	if f.Seen(updated) {
		t.Fatal("re-added record seen after expiry")
	}
}

func TestSearchFilterLimit(t *testing.T) {
	f := NewSearchFilter(Config{Clock: new(mclock.Simulated)})
	f.limit = 3
	nodes := []*enode.Node{newNode(), newNode(), newNode(), newNode()}
	for _, n := range nodes {
		f.Add(n)
	}
	if len(f.seen) != f.limit {
		t.Fatalf("filter holds %d entries, want %d", len(f.seen), f.limit)
	}
	if f.Seen(nodes[0]) {
		t.Fatal("oldest entry not evicted")
	}
	for _, n := range nodes[1:] {
		if !f.Seen(n) {
			t.Fatalf("node %v evicted", n.ID())
		}
	}
}
