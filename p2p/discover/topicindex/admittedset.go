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
	"net/netip"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/netutil"
)

// AdmittedSet remembers the nodes a topic search admitted to its table, by
// /24, for an ad lifetime from the first admission: a subnet with
// searchSubnetLimit distinct IDs counted can only re-admit those. LAN and
// address-less nodes are not counted.
type AdmittedSet struct {
	clock   mclock.Clock
	ttl     time.Duration
	subnets map[netip.Prefix]map[enode.ID]mclock.AbsTime // first admission
	expired mclock.AbsTime                               // when expire last ran
}

// NewAdmittedSet creates an empty set.
func NewAdmittedSet(cfg Config) *AdmittedSet {
	cfg = cfg.withDefaults()
	return &AdmittedSet{clock: cfg.Clock, ttl: cfg.AdLifetime, subnets: make(map[netip.Prefix]map[enode.ID]mclock.AbsTime)}
}

// Allows reports whether n may be admitted. It does not count n.
func (a *AdmittedSet) Allows(n *enode.Node) bool {
	key, ok := a.key(n)
	if !ok {
		return true
	}
	a.expire()
	ids := a.subnets[key]
	if _, ok := ids[n.ID()]; ok {
		return true
	}
	return len(ids) < searchSubnetLimit
}

// Add counts n against its subnet. Re-admitting a counted node keeps its
// expiry.
func (a *AdmittedSet) Add(n *enode.Node) {
	key, ok := a.key(n)
	if !ok {
		return
	}
	a.expire()
	ids := a.subnets[key]
	if ids == nil {
		ids = make(map[enode.ID]mclock.AbsTime)
		a.subnets[key] = ids
	}
	if _, ok := ids[n.ID()]; !ok {
		ids[n.ID()] = a.clock.Now()
	}
}

func (a *AdmittedSet) key(n *enode.Node) (netip.Prefix, bool) {
	ip := n.IP()
	if ip == nil || netutil.IsLAN(ip) {
		return netip.Prefix{}, false
	}
	p, err := netutil.IPToAddr(ip).Prefix(searchSubnet)
	return p, err == nil
}

// expire drops the entries past the ttl, at most once a second.
func (a *AdmittedSet) expire() {
	now := a.clock.Now()
	if now.Sub(a.expired) < time.Second && len(a.subnets) > 0 {
		return
	}
	a.expired = now
	for key, ids := range a.subnets {
		for id, first := range ids {
			if now.Sub(first) >= a.ttl {
				delete(ids, id)
			}
		}
		if len(ids) == 0 {
			delete(a.subnets, key)
		}
	}
}
