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
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// Blacklist is a set of temporarily-banned node IDs. Nodes are added when they
// fail to respond to discv5 RPCs too many times in a row; the ban expires after
// a configurable TTL so a recovered (or wrongly-banned) peer can be used again.
//
// Blacklist is safe for concurrent use: it is updated by the topic system's
// failure-tracking goroutine and consulted by the registration and search state
// machines on their own goroutines. A nil *Blacklist is valid and behaves as an
// empty, immutable set (this keeps the topicindex package usable in tests that
// don't wire up a blacklist).
type Blacklist struct {
	clock mclock.Clock
	ttl   time.Duration

	mu     sync.Mutex
	banned map[enode.ID]mclock.AbsTime
}

// NewBlacklist creates a blacklist with the given ban duration. If ttl <= 0 the
// blacklist is disabled: Ban is a no-op and Contains always returns false.
func NewBlacklist(ttl time.Duration, clock mclock.Clock) *Blacklist {
	if clock == nil {
		clock = mclock.System{}
	}
	return &Blacklist{
		clock:  clock,
		ttl:    ttl,
		banned: make(map[enode.ID]mclock.AbsTime),
	}
}

// Ban adds id to the blacklist until now+ttl.
func (b *Blacklist) Ban(id enode.ID) {
	if b == nil || b.ttl <= 0 {
		return
	}
	b.mu.Lock()
	b.banned[id] = b.clock.Now().Add(b.ttl)
	b.mu.Unlock()
}

// Contains reports whether id is currently banned. Expired entries are dropped
// lazily on lookup.
func (b *Blacklist) Contains(id enode.ID) bool {
	if b == nil || b.ttl <= 0 {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	exp, ok := b.banned[id]
	if !ok {
		return false
	}
	if b.clock.Now() >= exp {
		delete(b.banned, id)
		return false
	}
	return true
}

// Len returns the number of currently-banned (non-expired) nodes. Intended for
// metrics/observability.
func (b *Blacklist) Len() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clock.Now()
	n := 0
	for _, exp := range b.banned {
		if now < exp {
			n++
		}
	}
	return n
}
