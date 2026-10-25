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
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// AskedSet remembers the nodes a topic search asked, for an ad lifetime from
// the first ask: a spare re-asked within that time keeps its expiry, and the
// time of its last ask orders the spares.
type AskedSet struct {
	clock   mclock.Clock
	ttl     time.Duration
	at      map[enode.ID]*asked
	expired mclock.AbsTime // when expire last ran
}

type asked struct {
	first, last mclock.AbsTime
}

// NewAskedSet creates an empty set.
func NewAskedSet(cfg Config) *AskedSet {
	cfg = cfg.withDefaults()
	return &AskedSet{clock: cfg.Clock, ttl: cfg.AdLifetime, at: make(map[enode.ID]*asked)}
}

// Has reports whether id was asked within the ttl.
func (a *AskedSet) Has(id enode.ID) bool {
	a.expire()
	_, ok := a.at[id]
	return ok
}

// Add records that id was asked. The first ask sets the expiry; a later one
// only moves the node behind the other spares.
func (a *AskedSet) Add(id enode.ID) {
	a.expire()
	now := a.clock.Now()
	if e, ok := a.at[id]; ok {
		e.last = now
		return
	}
	a.at[id] = &asked{first: now, last: now}
}

// Remove drops id from the set.
func (a *AskedSet) Remove(id enode.ID) { delete(a.at, id) }

// LastAsked returns when id was last asked, or zero when it is not in the set.
func (a *AskedSet) LastAsked(id enode.ID) mclock.AbsTime {
	a.expire()
	if e, ok := a.at[id]; ok {
		return e.last
	}
	return 0
}

// expire drops the entries past the ttl, at most once a second.
func (a *AskedSet) expire() {
	now := a.clock.Now()
	if now.Sub(a.expired) < time.Second && len(a.at) > 0 {
		return
	}
	a.expired = now
	for id, e := range a.at {
		if now.Sub(e.first) >= a.ttl {
			delete(a.at, id)
		}
	}
}
