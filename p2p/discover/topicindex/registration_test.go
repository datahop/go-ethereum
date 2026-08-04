// Copyright 2022 The go-ethereum Authors
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
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

// randomID returns a random node ID such that enode.LogDist(a, b) == n.
func randomID(a enode.ID, n int) (b enode.ID) {
	if n == 0 {
		return a
	}
	b = a
	pos := len(a) - n/8 - 1
	bit := byte(0x01) << (byte(n%8) - 1)
	if bit == 0 {
		pos++
		bit = 0x80
	}
	b[pos] = a[pos]&^bit | ^a[pos]&bit
	for i := pos + 1; i < len(a); i++ {
		b[i] = byte(rand.Intn(255))
	}
	return b
}

// This test checks basic assignment of nodes into registration buckets.
func TestRegistrationBuckets(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)

	var (
		far256  = nodesAtDistanceFrom(enode.ID(topic1), 256, 3, 1)
		far255  = nodesAtDistanceFrom(enode.ID(topic1), 255, 3, 10)
		close5  = nodesAtDistanceFrom(enode.ID(topic1), 5, 1, 20)
		close20 = nodesAtDistanceFrom(enode.ID(topic1), 20, 1, 30)
	)
	r.AddNodes(nil, far256)
	r.AddNodes(nil, far255)
	r.AddNodes(nil, close5)
	r.AddNodes(nil, close20)

	last := len(r.buckets) - 1
	if !rbContainsAll(r.buckets[0], far256) {
		t.Fatalf("far256 nodes missing in bucket[%d]", 0)
	}
	if !rbContainsAll(r.buckets[1], far255) {
		t.Fatalf("far255 nodes missing in bucket[%d]", 1)
	}
	if !rbContainsAll(r.buckets[last], close5) {
		t.Fatalf("close5 nodes missing in bucket[%d]", last)
	}
	if !rbContainsAll(r.buckets[last], close20) {
		t.Fatalf("close20 nodes missing in bucket[%d]", last-20)
	}
}

func rbContainsAll(b regBucket, nodes []*enode.Node) bool {
	for _, n := range nodes {
		if _, ok := b.att[n.ID()]; !ok {
			return false
		}
	}
	return true
}

// This checks that the one-per-source-per-bucket rule is applied within a single
// AddNodes call. The source must be a registrar in the table (the rule is
// tracked on its attempt), which mirrors production.
func TestRegistrationOnePerBucketCheck(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)
	src := nodeAtDistance(enode.ID(topic1), 255, intIP(1))
	r.AddNodes(nil, []*enode.Node{src})

	// Attempt to insert multiple nodes from src into the same bucket.
	// Only one of them should actually be added.
	bi := r.bucketIndex(nodeAtDistance(enode.ID(topic1), 200, intIP(1)).ID())
	nodes := nodesAtDistance(enode.ID(topic1), 200, 10)
	r.AddNodes(src, nodes)
	got := 0
	for _, c := range r.buckets[bi].count {
		got += c
	}
	if got != 1 {
		t.Fatalf("expected 1 node in target bucket under one-per-source-per-bucket, got %d", got)
	}
}

// TestRegistrationSourcePersistentCap verifies that a single source cannot
// contribute more than one node to a given bucket across multiple AddNodes
// calls. The per-source accounting lives on the source registrar's own attempt,
// so the sources must themselves be registrars in the table — which mirrors
// production, where the source of nodes is always a registrar we queried. Once
// src has contributed an entry to a bucket, subsequent calls from src to that
// bucket are rejected, while different sources and bootstrap (src == nil) calls
// remain admissible.
func TestRegistrationSourcePersistentCap(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)

	// The sources must be registrars in the table for the cap to be tracked on
	// their attempts. They land in their own (distinct) buckets.
	src1 := nodeAtDistance(enode.ID(topic1), 255, intIP(1))
	src2 := nodeAtDistance(enode.ID(topic1), 254, intIP(2))
	r.AddNodes(nil, []*enode.Node{src1, src2})

	// All contributed nodes share one bucket (distance 200); count entries in
	// just that bucket so the source registrars above don't skew the totals.
	bi := r.bucketIndex(nodeAtDistance(enode.ID(topic1), 200, intIP(3)).ID())
	bucketCount := func() int {
		n := 0
		for _, c := range r.buckets[bi].count {
			n += c
		}
		return n
	}

	// First call from src1 admits one node into the bucket.
	node1 := nodeAtDistance(enode.ID(topic1), 200, intIP(3))
	r.AddNodes(src1, []*enode.Node{node1})
	if bucketCount() != 1 {
		t.Fatalf("after 1st call: expected 1 node, got %d", bucketCount())
	}

	// Second call from the same src1 to the same bucket is rejected.
	node2 := nodeAtDistance(enode.ID(topic1), 200, intIP(4))
	r.AddNodes(src1, []*enode.Node{node2})
	if bucketCount() != 1 {
		t.Fatalf("after 2nd call from src1 (cross-RPC cap): expected 1 node, got %d", bucketCount())
	}

	// A different src2 is allowed to contribute to the same bucket.
	node3 := nodeAtDistance(enode.ID(topic1), 200, intIP(5))
	r.AddNodes(src2, []*enode.Node{node3})
	if bucketCount() != 2 {
		t.Fatalf("after 3rd call from src2: expected 2 nodes, got %d", bucketCount())
	}

	// Bootstrap path (src == nil) is not subject to the cap.
	node4 := nodeAtDistance(enode.ID(topic1), 200, intIP(6))
	r.AddNodes(nil, []*enode.Node{node4})
	if bucketCount() != 3 {
		t.Fatalf("after bootstrap (src=nil): expected 3 nodes, got %d", bucketCount())
	}
}

// TestRegistrationSourceCapFreedOnEviction verifies that the per-source bucket
// accounting is released when the source registrar is removed, so a re-added
// registrar gets a fresh budget (the property the per-attempt design buys over
// a never-cleared per-bucket map).
func TestRegistrationSourceCapFreedOnEviction(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)

	src := nodeAtDistance(enode.ID(topic1), 255, intIP(1))
	r.AddNodes(nil, []*enode.Node{src})

	bi := r.bucketIndex(nodeAtDistance(enode.ID(topic1), 200, intIP(3)).ID())
	bucketCount := func() int {
		n := 0
		for _, c := range r.buckets[bi].count {
			n += c
		}
		return n
	}

	// src fills the bucket once; a second entry is capped.
	r.AddNodes(src, []*enode.Node{nodeAtDistance(enode.ID(topic1), 200, intIP(3))})
	r.AddNodes(src, []*enode.Node{nodeAtDistance(enode.ID(topic1), 200, intIP(4))})
	if bucketCount() != 1 {
		t.Fatalf("expected cap to hold: want 1, got %d", bucketCount())
	}

	// Evict the source registrar. Its filledBuckets set goes with it.
	srcBucket := &r.buckets[r.bucketIndex(src.ID())]
	r.removeAttempt(srcBucket.att[src.ID()], "test")

	// Re-add the source and let it contribute again — the cap budget is fresh.
	r.AddNodes(nil, []*enode.Node{src})
	r.AddNodes(src, []*enode.Node{nodeAtDistance(enode.ID(topic1), 200, intIP(7))})
	if bucketCount() != 2 {
		t.Fatalf("expected fresh budget after re-add: want 2, got %d", bucketCount())
	}
}

// This checks that the per-bucket IP limit is applied in AddNodes.
func TestRegistrationIPCheck(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)
	src1 := nodeAtDistance(enode.ID(topic1), 255, intIP(1))
	src2 := nodeAtDistance(enode.ID(topic1), 255, intIP(1))

	// Attempt to insert nodes with same IP in one bucket.
	// Only one of them should actually be added.
	// This needs to be done across multiple AddNodes calls to avoid
	// the one-per-bucket check.

	node1 := nodeAtDistance(enode.ID(topic1), 200, net.IP{192, 0, 2, 1})
	node2 := nodeAtDistance(enode.ID(topic1), 200, net.IP{192, 0, 2, 2})
	r.AddNodes(src1, []*enode.Node{node1})
	r.AddNodes(src2, []*enode.Node{node2})

	if r.NodeCount() > 1 {
		t.Fatal("too many nodes added")
	}
}

// TestRegistrationIPCheckOnRecordUpdate checks that when an already-known node
// presents a newer record with a different endpoint, the per-bucket IP-limit
// tracker is kept consistent, so the update can't be used to slip a second
// address from the same subnet into the bucket.
func TestRegistrationIPCheckOnRecordUpdate(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)

	// node1 registers with an address in subnet A.
	node1 := nodeAtDistance(enode.ID(topic1), 200, net.IP{192, 0, 2, 1})
	r.AddNodes(nil, []*enode.Node{node1})
	if r.NodeCount() != 1 {
		t.Fatalf("expected 1 node after initial add, got %d", r.NodeCount())
	}

	// node1 presents a newer record that moves it to subnet B. The tracker must
	// release subnet A and now count subnet B.
	node1b := nodeWithSeq(node1.ID(), net.IP{198, 51, 100, 1}, node1.Seq()+1)
	r.AddNodes(nil, []*enode.Node{node1b})

	// A different node in subnet B must now be rejected: subnet B already holds
	// node1's slot. Before the fix the tracker still counted subnet A, so this
	// second subnet-B node was wrongly admitted.
	node2 := nodeAtDistance(enode.ID(topic1), 200, net.IP{198, 51, 100, 2})
	r.AddNodes(nil, []*enode.Node{node2})

	if r.NodeCount() != 1 {
		t.Fatalf("subnet limit bypassed on record update: got %d nodes, want 1", r.NodeCount())
	}

	// Subnet A must have been released by the move: a node in subnet A is now
	// admitted. Before the fix its slot stayed counted forever (the leak).
	node3 := nodeAtDistance(enode.ID(topic1), 200, net.IP{192, 0, 2, 2})
	r.AddNodes(nil, []*enode.Node{node3})
	if r.NodeCount() != 2 {
		t.Fatalf("old subnet not released on record update: got %d nodes, want 2", r.NodeCount())
	}
}

// This test checks that registration attempts are created for found nodes.
func TestRegistrationRequests(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)

	if req := r.Update(); req != nil {
		t.Fatal("request spawned on fresh Registration")
	}

	// Deliver some nodes.
	target := enode.ID(r.Topic())
	for i := 50; i < 256; i++ {
		nodes := nodesAtDistance(target, i, 5)
		r.AddNodes(nil, nodes)
	}

	req := r.Update()
	if req == nil {
		t.Fatal("no request scheduled")
	}
	if r.Update() != req {
		t.Fatal("top request changed")
	}

	r.StartRequest(req)
	if r.Update() == req {
		t.Fatal("top request not removed")
	}
	r.HandleRegistered(req, cfg.AdLifetime)
}

// This test checks that registration attempts expire after the lifetime
// of the ad runs out.
func TestRegistrationExpiry(t *testing.T) {
	simclock := new(mclock.Simulated)

	cfg := testConfig(t)
	cfg.Clock = simclock
	cfg.AdLifetime = 20
	r := NewRegistration(topic1, cfg)

	// Deliver some nodes.
	node := nodesAtDistance(enode.ID(r.Topic()), 30, 1)
	r.AddNodes(nil, node)

	// A registration attempt should be created.
	att := r.Update()
	if att == nil {
		t.Fatal("no request scheduled")
	}
	if att.State != Waiting {
		t.Fatal("attempt should be in state", Waiting, "but has state", att.State)
	}

	// Mark registration to successful.
	r.StartRequest(att)
	r.HandleRegistered(att, cfg.AdLifetime)

	// NextUpdateTime should now return the expiry time of the ad.
	now := simclock.Now()
	if next := r.NextUpdateTime(); next != now.Add(cfg.AdLifetime) {
		t.Fatal("wrong next update time:", next)
	}

	// The attempt should be removed when the ad expires.
	simclock.Run(cfg.AdLifetime)
	if a := r.Update(); a != nil {
		t.Log(spew.Sdump(a))
		t.Fatal("Update returned an attempt, but nothing to do.")
	}
	if r.heap.Len() > 0 {
		t.Fatal("attempt not removed")
	}

	// Re-add the node.
	simclock.Run(1 * time.Second)
	r.AddNodes(nil, node)

	// It should get scheduled for registration again.
	att = r.Update()
	if att == nil {
		t.Fatal("no request scheduled")
	}
	if att.State != Waiting {
		t.Fatal("attempt should be in state", Waiting, "but has state", att.State)
	}
}

// TestRegistrationHandleTicketResponseDropAboveBudget verifies that a
// registrar quoting a wait time above RegAttemptTimeout causes the attempt
// to be dropped, freeing the bucket slot for another registrar. Without
// this, a single packet could park the slot for the quoted duration.
func TestRegistrationHandleTicketResponseDropAboveBudget(t *testing.T) {
	simclock := new(mclock.Simulated)
	cfg := testConfig(t)
	cfg.Clock = simclock
	cfg.AdLifetime = 15 * time.Minute
	cfg.RegAttemptTimeout = 22*time.Minute + 30*time.Second
	r := NewRegistration(topic1, cfg)

	node := nodesAtDistance(enode.ID(r.Topic()), 30, 1)
	r.AddNodes(nil, node)
	att := r.Update()
	if att == nil {
		t.Fatal("no request scheduled")
	}
	r.StartRequest(att)

	// Registrar quotes 49 days (well above RegAttemptTimeout). Expect the
	// attempt to be removed from the bucket.
	r.HandleTicketResponse(att, []byte("t"), 49*24*time.Hour)
	if att.bucket.att[node[0].ID()] != nil {
		t.Fatal("attempt not removed after wait time above RegAttemptTimeout")
	}
}

// nodesAtDistance creates n nodes for which enode.LogDist(base, node.ID()) == ld.
func nodesAtDistance(base enode.ID, ld int, n int) []*enode.Node {
	return nodesAtDistanceFrom(base, ld, n, 1)
}

// nodesAtDistanceFrom creates n nodes starting IP offset at 'start'.
func nodesAtDistanceFrom(base enode.ID, ld int, n int, start int) []*enode.Node {
	results := make([]*enode.Node, n)
	for i := range results {
		results[i] = nodeAtDistance(base, ld, intIP(start+i))
	}
	return results
}

// nodeAtDistance creates a node for which enode.LogDist(base, n.id) == ld.
func nodeAtDistance(base enode.ID, ld int, ip net.IP) *enode.Node {
	var r enr.Record
	r.Set(enr.IP(ip))
	return enode.SignNull(&r, randomID(base, ld))
}

// nodeWithSeq creates a node with an explicit ID, IP and sequence number. It is
// used to simulate a node re-advertising itself with a newer record.
func nodeWithSeq(id enode.ID, ip net.IP, seq uint64) *enode.Node {
	var r enr.Record
	r.Set(enr.IP(ip))
	r.SetSeq(seq)
	return enode.SignNull(&r, id)
}

func intIP(i int) net.IP {
	return net.IP{byte(i), 0, 2, byte(i)}
}

// TestRegistrationRecordUpdateIPLimit checks that when an already-scheduled node
// advances its ENR to an IP in a subnet that is already full for the bucket, the
// node is dropped rather than seated in violation of the per-/24 limit.
func TestRegistrationRecordUpdateIPLimit(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)
	base := enode.ID(topic1)

	// Two nodes in the same bucket, in different /24s.
	a := nodeAtDistance(base, 200, net.IP{3, 0, 2, 1})
	b := nodeAtDistance(base, 200, net.IP{4, 0, 2, 1})
	r.AddNodes(nil, []*enode.Node{a, b})

	bi := r.bucketIndex(a.ID())
	bucket := &r.buckets[bi]
	if _, ok := bucket.att[a.ID()]; !ok {
		t.Fatal("setup: node a not admitted")
	}
	if _, ok := bucket.att[b.ID()]; !ok {
		t.Fatal("setup: node b not admitted")
	}

	// a advertises a newer ENR moving into b's (full) /24.
	var rec enr.Record
	rec.Set(enr.IP(net.IP{4, 0, 2, 99})) // same /24 as b, different host
	rec.SetSeq(a.Seq() + 1)
	aMoved := enode.SignNull(&rec, a.ID())
	r.AddNodes(nil, []*enode.Node{aMoved})

	// a must be dropped: it can neither keep its stale IP nor put a second node
	// in b's /24.
	if _, ok := bucket.att[a.ID()]; ok {
		t.Fatal("node a should have been dropped after moving into a full /24")
	}
	if _, ok := bucket.att[b.ID()]; !ok {
		t.Fatal("node b should still be present")
	}

	// The /24 limit must still hold: a fresh node in that /24 is rejected.
	c := nodeAtDistance(base, 200, net.IP{4, 0, 2, 200})
	r.AddNodes(nil, []*enode.Node{c})
	if _, ok := bucket.att[c.ID()]; ok {
		t.Fatal("third node in the full /24 should have been rejected")
	}
}

// TestRegistrationRecordUpdateIPLimitAboveOne checks the record-update handling
// stays correct when the per-bucket /24 limit is greater than 1: dropping a node
// that moved into a full subnet must not release a co-tenant's slot in the node's
// old subnet. (The constant is 1 today; this guards a future change.)
func TestRegistrationRecordUpdateIPLimitAboveOne(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)
	base := enode.ID(topic1)

	bi := r.bucketIndex(nodeAtDistance(base, 200, net.IP{9, 0, 2, 9}).ID())
	r.buckets[bi].ips.Limit = 2 // raise the per-bucket /24 limit
	bucket := &r.buckets[bi]

	// /24-X: a, c. /24-Y (full): d, e.
	a := nodeAtDistance(base, 200, net.IP{3, 0, 2, 1})
	c := nodeAtDistance(base, 200, net.IP{3, 0, 2, 2})
	d := nodeAtDistance(base, 200, net.IP{4, 0, 2, 1})
	e := nodeAtDistance(base, 200, net.IP{4, 0, 2, 2})
	r.AddNodes(nil, []*enode.Node{a, c, d, e})
	for _, n := range []*enode.Node{a, c, d, e} {
		if _, ok := bucket.att[n.ID()]; !ok {
			t.Fatal("setup: node not admitted")
		}
	}

	// a moves into /24-Y (full) → a dropped, c must keep its /24-X slot.
	var rec enr.Record
	rec.Set(enr.IP(net.IP{4, 0, 2, 99}))
	rec.SetSeq(a.Seq() + 1)
	r.AddNodes(nil, []*enode.Node{enode.SignNull(&rec, a.ID())})
	if _, ok := bucket.att[a.ID()]; ok {
		t.Fatal("node a should have been dropped")
	}

	// /24-X now holds exactly 1 (c): exactly one more fits, a second is rejected.
	f := nodeAtDistance(base, 200, net.IP{3, 0, 2, 3})
	g := nodeAtDistance(base, 200, net.IP{3, 0, 2, 4})
	r.AddNodes(nil, []*enode.Node{f, g})
	if _, ok := bucket.att[f.ID()]; !ok {
		t.Fatal("one more node should fit in /24-X (limit 2)")
	}
	if _, ok := bucket.att[g.ID()]; ok {
		t.Fatal("second extra node exceeds /24 limit — c's slot was undercounted")
	}
}

// TestRegistrationRecordUpdateSameSubnet checks that a node refreshing its ENR
// to a different host in the same /24 is kept (not evicted), even at IP limit 1.
func TestRegistrationRecordUpdateSameSubnet(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)
	base := enode.ID(topic1)

	a := nodeAtDistance(base, 200, net.IP{3, 0, 2, 1})
	r.AddNodes(nil, []*enode.Node{a})
	bi := r.bucketIndex(a.ID())
	bucket := &r.buckets[bi]

	var rec enr.Record
	rec.Set(enr.IP(net.IP{3, 0, 2, 250})) // same /24, different host
	rec.SetSeq(a.Seq() + 1)
	r.AddNodes(nil, []*enode.Node{enode.SignNull(&rec, a.ID())})

	att, ok := bucket.att[a.ID()]
	if !ok {
		t.Fatal("node moving within its own /24 should be kept")
	}
	if got := att.Node.IP(); !got.Equal(net.IP{3, 0, 2, 250}) {
		t.Fatalf("record not updated: IP=%v", got)
	}
}
