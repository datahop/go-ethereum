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
	mrand "math/rand"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
)

// Lower-bound state must stay within the cache contents (discv5-theory.md,
// "Waiting-Time Lower Bound"): topic bounds only for topics in the cache, IP
// bounds only on existing tree vertices.
func TestBoundedLowerBoundState(t *testing.T) {
	r := mrand.New(mrand.NewSource(5))
	clock := new(mclock.Simulated)
	tab := NewTopicTable(Config{AdCacheSize: 200, AdLifetime: time.Minute, Clock: clock})
	topic := func() TopicID {
		var t TopicID
		r.Read(t[:4]) // high topic churn: mostly distinct topics
		return t
	}
	for step := 0; step < 20000; step++ {
		before := tab.Stats()
		n := nodeWithIP(randomPublicIPv4(r))
		w := tab.Register(n, topic(), 0)
		after := tab.Stats()
		if w > 0 && (after.IPv4Nodes != before.IPv4Nodes || after.IPv6Nodes != before.IPv6Nodes) {
			t.Fatalf("step %d: a wait ticket changed the IP tree (%d -> %d vertices)", step, before.IPv4Nodes, after.IPv4Nodes)
		}
		if after.TopicBounds > after.OccupiedTopics {
			t.Fatalf("step %d: %d topic bounds for %d occupied topics", step, after.TopicBounds, after.OccupiedTopics)
		}
		clock.Run(time.Duration(r.Intn(500)) * time.Millisecond)
		tab.Expire()
	}
	clock.Run(time.Hour)
	tab.Expire()
	if s := tab.Stats(); s.Occupancy != 0 || s.TopicBounds != 0 || s.IPv4Nodes != 1 {
		t.Fatalf("state left after the cache emptied: %+v", s)
	}
}

// TestFullCacheWait records what Register returns when the cache is exactly
// full. The occupancy factor (1-c/C)^-10 is +Inf there; converting +Inf to a
// Duration is implementation-defined in Go and differs between CPU
// architectures, so the quoted wait and lower-bound bookkeeping at c = C are
// platform-dependent.
func TestFullCacheWait(t *testing.T) {
	r := mrand.New(mrand.NewSource(6))
	clock := new(mclock.Simulated)
	tab := NewTopicTable(Config{AdCacheSize: 10, AdLifetime: time.Minute, Clock: clock})
	var topic TopicID
	for i := 0; i < 10; i++ {
		tab.add(nodeWithIP(randomPublicIPv4(r)), topic, tab.config.Clock.Now())
		clock.Run(time.Second)
	}
	n := nodeWithIP(randomPublicIPv4(r))
	required, comps := tab.waitTime(n, topic, clock.Now())
	w := tab.Register(n, topic, 0)
	t.Logf("full cache: service component %v s, waitTime() = %v, Register() = %v, topic bounds = %d", comps.serviceSecs, required, w, tab.Stats().TopicBounds)
	if w <= 0 || tab.Occupancy() != 10 {
		t.Fatalf("full cache admitted or returned non-positive wait %v", w)
	}
}
