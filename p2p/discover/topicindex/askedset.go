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
// the first ask: a spare re-asked within that time keeps its expiry.
type AskedSet struct {
	clock   mclock.Clock
	ttl     time.Duration
	at      map[enode.ID]mclock.AbsTime
	expired mclock.AbsTime // when expire last ran
}

// NewAskedSet creates an empty set.
func NewAskedSet(cfg Config) *AskedSet {
	cfg = cfg.withDefaults()
	return &AskedSet{clock: cfg.Clock, ttl: cfg.AdLifetime, at: make(map[enode.ID]mclock.AbsTime)}
}

// Has reports whether id was asked within the ttl.
func (a *AskedSet) Has(id enode.ID) bool {
	a.expire()
	_, ok := a.at[id]
	return ok
}

// Add records that id was asked, unless it already is in the set.
func (a *AskedSet) Add(id enode.ID) {
	a.expire()
	if _, ok := a.at[id]; !ok {
		a.at[id] = a.clock.Now()
	}
}

// AskedAt returns when id was first asked, or zero when it is not in the set.
func (a *AskedSet) AskedAt(id enode.ID) mclock.AbsTime {
	a.expire()
	return a.at[id]
}

// expire drops the entries past the ttl, at most once a second.
func (a *AskedSet) expire() {
	now := a.clock.Now()
	if now.Sub(a.expired) < time.Second && len(a.at) > 0 {
		return
	}
	a.expired = now
	for id, t := range a.at {
		if now.Sub(t) >= a.ttl {
			delete(a.at, id)
		}
	}
}
