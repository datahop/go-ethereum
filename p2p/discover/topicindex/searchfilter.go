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
// results of the registrars for the whole pass, one from each in turn, so
// that no registrar's reply reaches the consumer in a burst.
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
	newest        map[enode.ID]*enode.Node // the newest record of every waiting node
	handed        map[enode.ID]struct{}    // handed out in this pass
	firstQueued   mclock.AbsTime
	waiting       bool
	dropped       int // results Take dropped as recently returned
}

// registrarQueue holds the results of one registrar that wait to be handed
// out. A node several registrars return waits in each of their queues and is
// handed out once.
type registrarQueue struct {
	nodes    []*enode.Node
	queuedAt mclock.AbsTime // when the queue last went from empty to waiting
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
	f.newest = make(map[enode.ID]*enode.Node)
	f.handed = make(map[enode.ID]struct{})
	f.waiting = false
}

// Take queues the results of a registrar's reply and returns the results that
// can be handed out now. A registrar counts toward the mix when its reply
// held ads, even if every one of them waits in other queues as well.
func (f *SearchFilter) Take(registrar enode.ID, results []*enode.Node) []*enode.Node {
	q := f.registrars[registrar]
	now := f.clock.Now()
	for _, n := range results {
		if n.ID() == f.self {
			continue
		}
		if f.Seen(n) {
			f.dropped++
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
		if cur, ok := f.newest[n.ID()]; !ok || n.Seq() > cur.Seq() {
			f.newest[n.ID()] = n
		}
		if len(q.nodes) == 0 {
			q.queuedAt = now
		}
		if !f.waiting {
			f.firstQueued, f.waiting = now, true
		}
		q.nodes = append(q.nodes, n)
	}
	return f.mix(false)
}

// Dropped returns how many results Take has dropped as recently returned.
func (f *SearchFilter) Dropped() int {
	return f.dropped
}

// Release returns the results that still wait, one registrar at a time. It is
// called at the end of a search pass.
func (f *SearchFilter) Release() []*enode.Node {
	out := f.mix(true)
	f.reset()
	return out
}

// mix hands out results in rounds of one per registrar with results waiting,
// once enough registrars have replied or the first result has waited long
// enough. A round with a single registrar waits as well, so that a late
// reply is interleaved with whatever comes after it, unless it has waited
// long enough on its own. With all set, every waiting result goes out.
func (f *SearchFilter) mix(all bool) []*enode.Node {
	now := f.clock.Now()
	waited := func(since mclock.AbsTime) bool { return now.Sub(since) >= f.mixWait }
	if !all && len(f.queues) < f.mixRegistrars && !(f.waiting && waited(f.firstQueued)) {
		return nil
	}
	var out []*enode.Node
	for {
		var round []*registrarQueue
		for _, q := range f.queues {
			if len(q.nodes) > 0 {
				round = append(round, q)
			}
		}
		if len(round) == 0 || (!all && len(round) == 1 && !waited(round[0].queuedAt)) {
			return out
		}
		for _, q := range round {
			n := q.nodes[0]
			q.nodes = q.nodes[1:]
			if _, ok := f.handed[n.ID()]; ok {
				continue
			}
			f.handed[n.ID()] = struct{}{}
			out = append(out, f.newest[n.ID()])
			delete(f.newest, n.ID())
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
