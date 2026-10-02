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
	"container/list"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

const (
	searchFilterLimit = 50000

	// searchRegistrarLimit is the number of results a search takes from one
	// registrar per ad lifetime.
	searchRegistrarLimit = 5
)

// SearchFilter remembers the nodes returned by a topic search, so that later
// search passes don't return them again until AdLifetime has passed or the
// node record has been updated. It also limits the results one registrar
// contributes, so that the results of a search come from several registrars.
type SearchFilter struct {
	clock mclock.Clock
	ttl   time.Duration
	limit int
	seen  map[enode.ID]*list.Element
	order *list.List // of *searchFilterEntry, oldest expiry first

	registrarLimit int
	registrars     map[enode.ID]*registrarCount
	held           []*enode.Node // results above the limit, in arrival order
	heldSet        map[enode.ID]struct{}
}

// registrarCount is the number of results taken from one registrar.
type registrarCount struct {
	n      int
	expiry mclock.AbsTime
}

type searchFilterEntry struct {
	id     enode.ID
	seq    uint64
	expiry mclock.AbsTime
}

// NewSearchFilter creates an empty filter.
func NewSearchFilter(cfg Config) *SearchFilter {
	cfg = cfg.withDefaults()
	return &SearchFilter{
		clock: cfg.Clock,
		ttl:   cfg.AdLifetime,
		limit: searchFilterLimit,
		seen:  make(map[enode.ID]*list.Element),
		order: list.New(),

		registrarLimit: searchRegistrarLimit,
		registrars:     make(map[enode.ID]*registrarCount),
		heldSet:        make(map[enode.ID]struct{}),
	}
}

// Take returns the results of a registrar's reply that can be handed out now.
// Results returned recently are dropped. A registrar contributes at most
// searchRegistrarLimit results within an ad lifetime; the rest are held until
// Release. A held result that another registrar returns is taken from that
// registrar instead.
func (f *SearchFilter) Take(registrar enode.ID, results []*enode.Node) []*enode.Node {
	now := f.clock.Now()
	rc := f.registrars[registrar]
	if rc == nil || rc.expiry <= now {
		rc = &registrarCount{expiry: now.Add(f.ttl)}
		f.registrars[registrar] = rc
	}
	take := results[:0:0]
	for _, n := range results {
		if f.Seen(n) {
			continue
		}
		if rc.n >= f.registrarLimit {
			if _, ok := f.heldSet[n.ID()]; !ok {
				f.heldSet[n.ID()] = struct{}{}
				f.held = append(f.held, n)
			}
			continue
		}
		rc.n++
		delete(f.heldSet, n.ID())
		take = append(take, n)
	}
	return take
}

// Release returns the results held back by Take. It is called at the end of a
// search pass.
func (f *SearchFilter) Release() []*enode.Node {
	var out []*enode.Node
	for _, n := range f.held {
		if _, ok := f.heldSet[n.ID()]; ok && !f.Seen(n) {
			out = append(out, n)
		}
	}
	f.held, f.heldSet = nil, make(map[enode.ID]struct{})
	now := f.clock.Now()
	for id, rc := range f.registrars {
		if rc.expiry <= now {
			delete(f.registrars, id)
		}
	}
	return out
}

// Seen reports whether n was returned recently with the same or a newer record.
func (f *SearchFilter) Seen(n *enode.Node) bool {
	f.expire()
	el, ok := f.seen[n.ID()]
	return ok && n.Seq() <= el.Value.(*searchFilterEntry).seq
}

// Add records that n was returned. Every ID keeps a single entry, so the
// filter never holds more than limit entries.
func (f *SearchFilter) Add(n *enode.Node) {
	f.expire()
	expiry := f.clock.Now().Add(f.ttl)
	if el, ok := f.seen[n.ID()]; ok {
		e := el.Value.(*searchFilterEntry)
		e.seq, e.expiry = n.Seq(), expiry
		f.order.MoveToBack(el)
		return
	}
	for len(f.seen) >= f.limit {
		f.remove(f.order.Front())
	}
	f.seen[n.ID()] = f.order.PushBack(&searchFilterEntry{id: n.ID(), seq: n.Seq(), expiry: expiry})
}

func (f *SearchFilter) expire() {
	now := f.clock.Now()
	for el := f.order.Front(); el != nil && el.Value.(*searchFilterEntry).expiry <= now; el = f.order.Front() {
		f.remove(el)
	}
}

func (f *SearchFilter) remove(el *list.Element) {
	delete(f.seen, el.Value.(*searchFilterEntry).id)
	f.order.Remove(el)
}
