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
	"net"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

func newTestAdmittedSet() (*AdmittedSet, *mclock.Simulated) {
	clock := new(mclock.Simulated)
	return NewAdmittedSet(Config{AdLifetime: time.Minute, Clock: clock}), clock
}

// subnetNodes creates n nodes in 203.0.113.0/24.
func subnetNodes(n int) []*enode.Node {
	nodes := make([]*enode.Node, n)
	for i := range nodes {
		nodes[i] = nodeWithIP(net.IP{203, 0, 113, byte(i + 1)})
	}
	return nodes
}

// A /24 gets searchSubnetLimit distinct nodes admitted; after that only those
// are allowed again. Checking a node does not count it.
func TestAdmittedSetLimit(t *testing.T) {
	a, _ := newTestAdmittedSet()
	nodes := subnetNodes(searchSubnetLimit + 1)
	for i, n := range nodes[:searchSubnetLimit] {
		if !a.Allows(n) {
			t.Fatalf("node %d refused below the limit", i)
		}
		a.Add(n)
	}
	if a.Allows(nodes[searchSubnetLimit]) {
		t.Fatal("node over the subnet limit allowed")
	}
	if !a.Allows(nodes[0]) {
		t.Fatal("counted node refused re-admission")
	}
	if !a.Allows(nodeWithIP(net.IP{203, 0, 114, 1})) {
		t.Fatal("node from another subnet refused")
	}

	a, _ = newTestAdmittedSet()
	nodes = subnetNodes(searchSubnetLimit + 1)
	for _, n := range nodes[:searchSubnetLimit-1] {
		a.Add(n)
	}
	x, y := nodes[searchSubnetLimit-1], nodes[searchSubnetLimit]
	if !a.Allows(x) {
		t.Fatal("node refused with one slot left")
	}
	a.Add(y)
	if a.Allows(x) {
		t.Fatal("checking a node reserved its slot")
	}
	if !a.Allows(y) {
		t.Fatal("counted node refused")
	}
}

// LAN and address-less nodes are never counted.
func TestAdmittedSetExempt(t *testing.T) {
	a, _ := newTestAdmittedSet()
	var nodes []*enode.Node
	for i := 0; i <= searchSubnetLimit; i++ {
		nodes = append(nodes,
			nodeWithIP(net.IP{10, 0, 0, byte(i + 1)}),
			nodeWithIP(net.IP{127, 0, 0, 1}),
			newNode(),
		)
	}
	for i, n := range nodes {
		if !a.Allows(n) {
			t.Fatalf("exempt node %d refused", i)
		}
		a.Add(n)
	}
	if len(a.subnets) != 0 {
		t.Fatalf("exempt nodes counted in %d subnets", len(a.subnets))
	}
}

// A node's slot expires an ad lifetime after its first admission, however
// often it is admitted again meanwhile.
func TestAdmittedSetKeepsFirstExpiry(t *testing.T) {
	a, clock := newTestAdmittedSet()
	nodes := subnetNodes(searchSubnetLimit + 1)
	for _, n := range nodes[:searchSubnetLimit] {
		a.Add(n)
	}
	clock.Run(40 * time.Second)
	a.Add(nodes[0]) // re-admitted in a later pass
	if a.Allows(nodes[searchSubnetLimit]) {
		t.Fatal("node over the subnet limit allowed before expiry")
	}
	clock.Run(21 * time.Second)
	if !a.Allows(nodes[searchSubnetLimit]) {
		t.Fatal("re-admission extended the subnet's expiry")
	}
}
