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

	// Results are handed out once this many registrars replied with ads in
	// the pass, or once the first result has waited this long.
	searchMixRegistrars = 5
	searchMixWait       = 5 * time.Second
)

// SearchFilter remembers the nodes returned by a topic search, so that later
// search passes don't return them again until AdLifetime has passed; a node
// with a newer record is returned once more, and only once. It also mixes the
// results of the registrars, so that the results of a search come from
// several of them.
type SearchFilter struct {
	self  enode.ID
	clock mclock.Clock
	ttl   time.Duration
	limit int
	seen  map[enode.ID]*list.Element
	order *list.List // of *searchFilterEntry, oldest expiry first

	mixRegistrars int
	mixWait       time.Duration
	queues        []*registrarQueue // in arrival order of the registrars that replied with ads
	registrars    map[enode.ID]*registrarQueue
	queued        map[enode.ID]*registrarQueue // waiting nodes, by the queue that holds them
	handed        map[enode.ID]struct{}        // handed out in this pass
	firstQueued   mclock.AbsTime
	waiting       bool
}

// registrarQueue holds the results of one registrar that wait to be handed out.
type registrarQueue struct {
	nodes []*enode.Node
}

type searchFilterEntry struct {
	id       enode.ID
	seq      uint64
	expiry   mclock.AbsTime
	returned bool // a newer record was returned again
}

// NewSearchFilter creates an empty filter.
func NewSearchFilter(cfg Config) *SearchFilter {
	cfg = cfg.withDefaults()
	f := &SearchFilter{
		self:  cfg.Self,
		clock: cfg.Clock,
		ttl:   cfg.AdLifetime,
		limit: searchFilterLimit,
		seen:  make(map[enode.ID]*list.Element),
		order: list.New(),

		mixRegistrars: searchMixRegistrars,
		mixWait:       searchMixWait,
	}
	f.reset()
	return f
}

func (f *SearchFilter) reset() {
	f.queues = nil
	f.registrars = make(map[enode.ID]*registrarQueue)
	f.queued = make(map[enode.ID]*registrarQueue)
	f.handed = make(map[enode.ID]struct{})
	f.waiting = false
}

// Take queues the results of a registrar's reply and returns the results that
// can be handed out now. A registrar counts toward the mix when its reply
// held ads, even if every one of them already waits in another queue.
func (f *SearchFilter) Take(registrar enode.ID, results []*enode.Node) []*enode.Node {
	q := f.registrars[registrar]
	for _, n := range results {
		if n.ID() == f.self || f.Seen(n) {
			continue
		}
		if _, ok := f.handed[n.ID()]; ok {
			continue
		}
		if q == nil {
			q = new(registrarQueue)
			f.registrars[registrar] = q
			f.queues = append(f.queues, q)
		}
		if other, ok := f.queued[n.ID()]; ok {
			other.replace(n)
			continue
		}
		if !f.waiting {
			f.firstQueued, f.waiting = f.clock.Now(), true
		}
		f.queued[n.ID()] = q
		q.nodes = append(q.nodes, n)
	}
	return f.mix(false)
}

// replace swaps a waiting record of the same node for a newer one.
func (q *registrarQueue) replace(n *enode.Node) {
	for i, old := range q.nodes {
		if old.ID() == n.ID() {
			if n.Seq() > old.Seq() {
				q.nodes[i] = n
			}
			return
		}
	}
}

// Release returns the results that still wait, one registrar at a time. It is
// called at the end of a search pass.
func (f *SearchFilter) Release() []*enode.Node {
	out := f.mix(true)
	f.reset()
	return out
}

// mix takes one result from every registrar that has results waiting, round
// after round, once enough registrars have replied or the results have waited
// long enough. With all set, it takes every waiting result.
func (f *SearchFilter) mix(all bool) []*enode.Node {
	if !all && len(f.queues) < f.mixRegistrars && f.waiting && f.clock.Now().Sub(f.firstQueued) >= f.mixWait {
		all = true
	}
	var out []*enode.Node
	for {
		waiting := 0
		for _, q := range f.queues {
			if len(q.nodes) > 0 {
				waiting++
			}
		}
		if waiting == 0 || (len(f.queues) < f.mixRegistrars && !all) {
			return out
		}
		for _, q := range f.queues {
			if len(q.nodes) == 0 {
				continue
			}
			n := q.nodes[0]
			q.nodes = q.nodes[1:]
			delete(f.queued, n.ID())
			f.handed[n.ID()] = struct{}{}
			out = append(out, n)
		}
	}
}

// Seen reports whether n was returned recently, with the same or a newer
// record, or whether its one re-delivery with a newer record was already used.
func (f *SearchFilter) Seen(n *enode.Node) bool {
	f.expire()
	el, ok := f.seen[n.ID()]
	if !ok {
		return false
	}
	e := el.Value.(*searchFilterEntry)
	return n.Seq() <= e.seq || e.returned
}

// Add records that n was returned. Every ID keeps a single entry, so the
// filter never holds more than limit entries.
func (f *SearchFilter) Add(n *enode.Node) {
	f.expire()
	expiry := f.clock.Now().Add(f.ttl)
	if el, ok := f.seen[n.ID()]; ok {
		e := el.Value.(*searchFilterEntry)
		if n.Seq() > e.seq {
			e.returned = true
		}
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
