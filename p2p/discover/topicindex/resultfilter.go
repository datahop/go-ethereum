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

const resultFilterLimit = 5000

// ResultFilter remembers the nodes returned by a topic search, so that later
// search passes don't return them again until AdLifetime has passed or the
// node record has been updated.
type ResultFilter struct {
	clock mclock.Clock
	ttl   time.Duration
	limit int
	seen  map[enode.ID]resultFilterEntry
	queue []resultFilterItem // in insertion order
}

type resultFilterEntry struct {
	seq    uint64
	expiry mclock.AbsTime
}

type resultFilterItem struct {
	id     enode.ID
	expiry mclock.AbsTime
}

// NewResultFilter creates an empty filter.
func NewResultFilter(cfg Config) *ResultFilter {
	cfg = cfg.withDefaults()
	return &ResultFilter{
		clock: cfg.Clock,
		ttl:   cfg.AdLifetime,
		limit: resultFilterLimit,
		seen:  make(map[enode.ID]resultFilterEntry),
	}
}

// Seen reports whether n was returned recently with the same or a newer record.
func (f *ResultFilter) Seen(n *enode.Node) bool {
	f.expire()
	e, ok := f.seen[n.ID()]
	return ok && n.Seq() <= e.seq
}

// Add records that n was returned.
func (f *ResultFilter) Add(n *enode.Node) {
	f.expire()
	id := n.ID()
	if _, ok := f.seen[id]; !ok {
		for len(f.seen) >= f.limit && len(f.queue) > 0 {
			f.pop()
		}
	}
	expiry := f.clock.Now().Add(f.ttl)
	f.seen[id] = resultFilterEntry{seq: n.Seq(), expiry: expiry}
	f.queue = append(f.queue, resultFilterItem{id: id, expiry: expiry})
}

func (f *ResultFilter) expire() {
	now := f.clock.Now()
	for len(f.queue) > 0 && f.queue[0].expiry <= now {
		f.pop()
	}
}

// pop removes the oldest queue item. The entry is kept if it was re-added later.
func (f *ResultFilter) pop() {
	it := f.queue[0]
	f.queue = f.queue[1:]
	if e, ok := f.seen[it.id]; ok && e.expiry == it.expiry {
		delete(f.seen, it.id)
	}
}
