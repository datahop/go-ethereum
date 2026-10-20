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

func newNodes(n int) []*enode.Node {
	nodes := make([]*enode.Node, n)
	for i := range nodes {
		nodes[i] = newNode()
	}
	return nodes
}

// Results are handed out one registrar at a time, once searchMixRegistrars
// registrars have results waiting.
func TestSearchFilterMix(t *testing.T) {
	f := NewSearchFilter(Config{Clock: new(mclock.Simulated)})
	regs := newNodes(searchMixRegistrars)
	ads := make([][]*enode.Node, len(regs))
	for i := range regs {
		ads[i] = newNodes(16)
	}
	for i, reg := range regs[:len(regs)-1] {
		if got := f.Take(reg.ID(), ads[i]); len(got) != 0 {
			t.Fatalf("reply %d gave %d results, want 0", i, len(got))
		}
	}
	last := len(regs) - 1
	got := f.Take(regs[last].ID(), ads[last])
	if len(got) != 16*len(regs) {
		t.Fatalf("got %d results, want %d", len(got), 16*len(regs))
	}
	for i, n := range got {
		if want := ads[i%len(regs)][i/len(regs)]; n != want {
			t.Fatalf("result %d is not from registrar %d", i, i%len(regs))
		}
	}
	if got := f.Release(); len(got) != 0 {
		t.Fatalf("released %d results, want 0", len(got))
	}
}

// Results of fewer registrars wait until Release. Once enough registrars have
// replied, everything waiting is handed out one registrar at a time.
func TestSearchFilterMixRelease(t *testing.T) {
	f := NewSearchFilter(Config{Clock: new(mclock.Simulated)})
	f.mixRegistrars = 3
	regA, regB, regC := newNode().ID(), newNode().ID(), newNode().ID()
	adsA, adsB, adsC := newNodes(3), newNodes(3), newNodes(1)

	if got := f.Take(regA, adsA); len(got) != 0 {
		t.Fatalf("first reply gave %d results, want 0", len(got))
	}
	if got := f.Take(regB, adsB); len(got) != 0 {
		t.Fatalf("second reply gave %d results, want 0", len(got))
	}
	got := f.Take(regC, adsC)
	want := []*enode.Node{adsA[0], adsB[0], adsC[0], adsA[1], adsB[1], adsA[2], adsB[2]}
	if len(got) != len(want) {
		t.Fatalf("third reply gave %d results, want %d, one registrar at a time", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("result %d out of order", i)
		}
	}
	if got := f.Release(); len(got) != 0 {
		t.Fatalf("release gave %d results, want 0", len(got))
	}
	// A lone registrar's results wait for the pass to end.
	if got := f.Take(regA, newNodes(2)); len(got) != 0 {
		t.Fatalf("lone reply gave %d results, want 0", len(got))
	}
	if got := f.Release(); len(got) != 2 {
		t.Fatalf("release gave %d results, want 2", len(got))
	}
}

// A result that two registrars return is handed out once. Results returned
// recently are dropped.
func TestSearchFilterMixShared(t *testing.T) {
	f := NewSearchFilter(Config{Clock: new(mclock.Simulated)})
	regA, regB := newNode().ID(), newNode().ID()
	ads := newNodes(3)

	f.Add(ads[0])
	f.Take(regA, []*enode.Node{ads[0], ads[1]})
	f.Take(regB, []*enode.Node{ads[1], ads[2]})
	got := f.Release()
	if len(got) != 2 || got[0] != ads[1] || got[1] != ads[2] {
		t.Fatalf("released %d results, want the two that were not returned before", len(got))
	}
}

// A newer record of a node that already waits in a queue replaces the waiting one.
func TestSearchFilterMixNewerRecord(t *testing.T) {
	f := NewSearchFilter(Config{Clock: new(mclock.Simulated)})
	regA, regB := newNode().ID(), newNode().ID()
	id := newNode().ID()
	old, updated := nodeWithSeq(id, intIP(1), 1), nodeWithSeq(id, intIP(1), 2)

	f.Take(regA, []*enode.Node{old})
	f.Take(regB, []*enode.Node{updated})
	got := f.Release()
	if len(got) != 1 || got[0].Seq() != 2 {
		t.Fatalf("released %v, want the record with seq 2", got)
	}
	f.Take(regA, []*enode.Node{updated})
	f.Take(regB, []*enode.Node{old})
	if got := f.Release(); len(got) != 1 || got[0].Seq() != 2 {
		t.Fatalf("an older record replaced the waiting one: %v", got)
	}
}

// Registrars holding the same ads count toward the mix: the first ones to
// reply queue the ads, the later ones confirm them, and the results flow
// once enough registrars have replied, instead of waiting for the pass.
func TestSearchFilterMixOverlap(t *testing.T) {
	f := NewSearchFilter(Config{Clock: new(mclock.Simulated)})
	regs := newNodes(searchMixRegistrars)
	ads := newNodes(16)
	for i, reg := range regs[:len(regs)-1] {
		if got := f.Take(reg.ID(), ads); len(got) != 0 {
			t.Fatalf("reply %d gave %d results, want 0", i, len(got))
		}
	}
	got := f.Take(regs[len(regs)-1].ID(), ads)
	if len(got) != len(ads) {
		t.Fatalf("got %d results after %d registrars replied with the same ads, want %d", len(got), len(regs), len(ads))
	}
	if got := f.Release(); len(got) != 0 {
		t.Fatalf("released %d results, want 0", len(got))
	}
}

// Results do not wait longer than searchMixWait for more registrars.
func TestSearchFilterMixWait(t *testing.T) {
	clock := new(mclock.Simulated)
	f := NewSearchFilter(Config{Clock: clock})
	regA, regB := newNode().ID(), newNode().ID()
	adsA, adsB := newNodes(2), newNodes(2)
	if got := f.Take(regA, adsA); len(got) != 0 {
		t.Fatalf("first reply gave %d results, want 0", len(got))
	}
	clock.Run(searchMixWait - time.Second)
	if got := f.Take(regB, adsB); len(got) != 0 {
		t.Fatalf("second reply before the wait gave %d results, want 0", len(got))
	}
	clock.Run(time.Second)
	got := f.Take(newNode().ID(), nil) // a reply without ads, or a failed query
	want := []*enode.Node{adsA[0], adsB[0], adsA[1], adsB[1]}
	if len(got) != len(want) {
		t.Fatalf("got %d results after the wait, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("result %d out of order", i)
		}
	}
}

// The local node and nodes already handed out in the pass are dropped, and
// do not count as a registrar's results.
func TestSearchFilterMixDrops(t *testing.T) {
	self := newNode()
	f := NewSearchFilter(Config{Clock: new(mclock.Simulated), Self: self.ID()})
	f.mixRegistrars = 2
	regA, regB, regC := newNode().ID(), newNode().ID(), newNode().ID()
	ads := newNodes(2)
	if got := f.Take(regA, []*enode.Node{self}); len(got) != 0 || len(f.queues) != 0 {
		t.Fatal("the local node was queued")
	}
	f.Take(regA, []*enode.Node{ads[0]})
	got := f.Take(regB, []*enode.Node{ads[1]})
	if len(got) != 2 {
		t.Fatalf("got %d results, want 2", len(got))
	}
	// ads[0] was handed out and waits for the consumer: a third registrar
	// returning it must not queue it again.
	if got := f.Take(regC, []*enode.Node{ads[0]}); len(got) != 0 {
		t.Fatalf("a handed-out node was handed out again: %d results", len(got))
	}
	if got := f.Release(); len(got) != 0 {
		t.Fatalf("released %d results, want 0", len(got))
	}
}

// A newer record of a returned node is returned once more within the ttl,
// and not again however often its record is bumped.
func TestSearchFilterNewerRecordOnce(t *testing.T) {
	clock := new(mclock.Simulated)
	f := NewSearchFilter(Config{AdLifetime: time.Minute, Clock: clock})
	id := newNode().ID()
	r1, r2, r3 := nodeWithSeq(id, intIP(1), 1), nodeWithSeq(id, intIP(1), 2), nodeWithSeq(id, intIP(1), 3)
	f.Add(r1)
	if f.Seen(r2) {
		t.Fatal("newer record filtered")
	}
	f.Add(r2)
	if !f.Seen(r3) {
		t.Fatal("a second newer record was not filtered")
	}
	clock.Run(2 * time.Minute)
	if f.Seen(r3) {
		t.Fatal("record seen after expiry")
	}
}
