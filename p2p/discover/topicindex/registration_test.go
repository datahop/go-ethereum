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

// TestRegistrationRemoveNode checks that RemoveNode drops a parked attempt but
// leaves an in-flight one for its pending response.
func TestRegistrationRemoveNode(t *testing.T) {
	cfg := testConfig(t)
	r := NewRegistration(topic1, cfg)

	// Waiting attempt: removed.
	waiting := nodesAtDistance(enode.ID(r.Topic()), 30, 1)
	r.AddNodes(nil, waiting)
	if att := r.Update(); att == nil || att.State != Waiting {
		t.Fatal("no waiting attempt scheduled")
	}
	r.RemoveNode(waiting[0].ID())
	if r.buckets[r.bucketIndex(waiting[0].ID())].att[waiting[0].ID()] != nil {
		t.Fatal("waiting attempt not removed")
	}

	// Registered attempt (ad not yet expired): removed before expiry.
	registered := nodesAtDistance(enode.ID(r.Topic()), 40, 1)
	r.AddNodes(nil, registered)
	att := r.Update()
	if att == nil {
		t.Fatal("no attempt scheduled")
	}
	r.StartRequest(att)
	r.HandleRegistered(att, cfg.AdLifetime)
	r.RemoveNode(registered[0].ID())
	if r.buckets[r.bucketIndex(registered[0].ID())].att[registered[0].ID()] != nil {
		t.Fatal("registered attempt not removed before ad expiry")
	}

	// Unknown node: no-op.
	r.RemoveNode(enode.ID{42})

	// In-flight attempt: left in place, the pending response handles it.
	inflight := nodesAtDistance(enode.ID(r.Topic()), 50, 1)
	r.AddNodes(nil, inflight)
	att = r.Update()
	if att == nil {
		t.Fatal("no attempt scheduled")
	}
	r.StartRequest(att)
	r.RemoveNode(inflight[0].ID())
	if r.buckets[r.bucketIndex(inflight[0].ID())].att[inflight[0].ID()] == nil {
		t.Fatal("in-flight attempt removed; must be left for response handling")
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

// TestRegistrationRecordUpdate checks how an already-scheduled node's newer ENR
// is handled: a non-newer seq is ignored, and an endpoint change keeps the
// per-/24 IP tracker consistent — a move within the same /24 is kept, a move to
// a free /24 seats the new subnet and releases the old, a move into a full /24
// is dropped, and a dropped node never releases a co-tenant's slot.
func TestRegistrationRecordUpdate(t *testing.T) {
	type probe struct {
		ip    net.IP
		admit bool
	}
	cases := []struct {
		name     string
		limit    int
		setup    []net.IP // admitted up front; index 0's record is updated
		moveIP   net.IP   // index 0's new endpoint
		sameSeq  bool     // offer the update with a non-newer seq (must be ignored)
		wantKept bool     // whether index 0 survives the update
		probes   []probe  // follow-up admissions checking the tracker count
	}{
		{
			name:     "stale-seq-ignored",
			limit:    1,
			setup:    []net.IP{{3, 0, 2, 1}},
			moveIP:   net.IP{4, 0, 2, 1}, // offered with a non-newer seq
			sameSeq:  true,
			wantKept: true,
			// Update ignored: /24-3 still held (rejected), /24-4 never counted (admitted).
			probes: []probe{{net.IP{3, 0, 2, 9}, false}, {net.IP{4, 0, 2, 9}, true}},
		},
		{
			name:     "into-empty-subnet",
			limit:    1,
			setup:    []net.IP{{3, 0, 2, 1}},
			moveIP:   net.IP{6, 0, 2, 1}, // different /24, has room
			wantKept: true,
			// Old /24-3 slot is freed (a fresh node fits); new /24-6 is now counted.
			probes: []probe{{net.IP{3, 0, 2, 9}, true}, {net.IP{6, 0, 2, 9}, false}},
		},
		{
			name:     "into-full-subnet-limit-2",
			limit:    2,
			setup:    []net.IP{{3, 0, 2, 1}, {3, 0, 2, 2}, {4, 0, 2, 1}, {4, 0, 2, 2}},
			moveIP:   net.IP{4, 0, 2, 99}, // /24-4 full (indexes 2,3)
			wantKept: false,
			// /24-3 must keep its co-tenant: exactly one more fits, a second does not.
			probes: []probe{{net.IP{3, 0, 2, 3}, true}, {net.IP{3, 0, 2, 4}, false}},
		},
		{
			name:     "within-same-subnet",
			limit:    1,
			setup:    []net.IP{{3, 0, 2, 1}},
			moveIP:   net.IP{3, 0, 2, 250}, // same /24, different host
			wantKept: true,
		},
	}
	base := enode.ID(topic1)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := NewRegistration(topic1, testConfig(t))
			bi := r.bucketIndex(nodeAtDistance(base, 200, net.IP{9, 0, 2, 9}).ID())
			r.buckets[bi].ips.Limit = uint(c.limit)
			bucket := &r.buckets[bi]

			nodes := make([]*enode.Node, len(c.setup))
			for i, ip := range c.setup {
				nodes[i] = nodeAtDistance(base, 200, ip)
			}
			r.AddNodes(nil, nodes)
			for i, n := range nodes {
				if _, ok := bucket.att[n.ID()]; !ok {
					t.Fatalf("setup: node %d (%v) not admitted", i, c.setup[i])
				}
			}

			// Offer node 0 a new record. A non-newer seq must be ignored.
			seq := nodes[0].Seq() + 1
			if c.sameSeq {
				seq = nodes[0].Seq()
			}
			r.AddNodes(nil, []*enode.Node{nodeWithSeq(nodes[0].ID(), c.moveIP, seq)})

			att, ok := bucket.att[nodes[0].ID()]
			if ok != c.wantKept {
				t.Fatalf("after update: kept=%v, want %v", ok, c.wantKept)
			}
			if c.wantKept {
				// A stale update is ignored, so the endpoint stays as it was.
				wantIP := c.moveIP
				if c.sameSeq {
					wantIP = c.setup[0]
				}
				if !att.Node.IP().Equal(wantIP) {
					t.Fatalf("endpoint after update: got %v, want %v", att.Node.IP(), wantIP)
				}
			}
			// The other setup nodes are untouched by node 0's update and must remain.
			for i := 1; i < len(nodes); i++ {
				if _, ok := bucket.att[nodes[i].ID()]; !ok {
					t.Fatalf("node %d (%v) should still be present", i, c.setup[i])
				}
			}

			for _, p := range c.probes {
				pn := nodeAtDistance(base, 200, p.ip)
				r.AddNodes(nil, []*enode.Node{pn})
				if _, ok := bucket.att[pn.ID()]; ok != p.admit {
					t.Fatalf("probe %v admitted=%v, want %v", p.ip, ok, p.admit)
				}
			}
		})
	}
}
