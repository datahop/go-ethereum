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

	// searchMixRegistrars is the number of registrars a search mixes results
	// from. Results are handed out while this many registrars have some waiting.
	searchMixRegistrars = 5
)

// SearchFilter remembers the nodes returned by a topic search, so that later
// search passes don't return them again until AdLifetime has passed or the
// node record has been updated. It also mixes the results of the registrars,
// so that the results of a search come from several of them.
type SearchFilter struct {
	clock mclock.Clock
	ttl   time.Duration
	limit int
	seen  map[enode.ID]*list.Element
	order *list.List // of *searchFilterEntry, oldest expiry first

	mixRegistrars int
	queues        []*registrarQueue // in arrival order of the registrars
	registrars    map[enode.ID]*registrarQueue
	queued        map[enode.ID]struct{} // nodes waiting in a queue
}

// registrarQueue holds the results of one registrar that wait to be handed out.
type registrarQueue struct {
	nodes []*enode.Node
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

		mixRegistrars: searchMixRegistrars,
		registrars:    make(map[enode.ID]*registrarQueue),
		queued:        make(map[enode.ID]struct{}),
	}
}

// Take queues the results of a registrar's reply and returns the results that
// can be handed out now. Results returned recently are dropped. Results are
// handed out one registrar at a time, and only while searchMixRegistrars
// registrars have results waiting. The rest wait until Release.
func (f *SearchFilter) Take(registrar enode.ID, results []*enode.Node) []*enode.Node {
	q := f.registrars[registrar]
	for _, n := range results {
		if f.Seen(n) {
			continue
		}
		if _, ok := f.queued[n.ID()]; ok {
			continue
		}
		if q == nil {
			q = new(registrarQueue)
			f.registrars[registrar] = q
			f.queues = append(f.queues, q)
		}
		f.queued[n.ID()] = struct{}{}
		q.nodes = append(q.nodes, n)
	}
	return f.mix(false)
}

// Release returns the results that still wait, one registrar at a time. It is
// called at the end of a search pass.
func (f *SearchFilter) Release() []*enode.Node {
	out := f.mix(true)
	f.queues, f.registrars = nil, make(map[enode.ID]*registrarQueue)
	return out
}

// mix takes one result from every registrar that has results waiting, and
// repeats this while enough registrars have some. With all set, it takes
// every waiting result.
func (f *SearchFilter) mix(all bool) []*enode.Node {
	var out []*enode.Node
	for {
		ready := 0
		for _, q := range f.queues {
			if len(q.nodes) > 0 {
				ready++
			}
		}
		if ready == 0 || (ready < f.mixRegistrars && !all) {
			return out
		}
		for _, q := range f.queues {
			if len(q.nodes) == 0 {
				continue
			}
			n := q.nodes[0]
			q.nodes = q.nodes[1:]
			delete(f.queued, n.ID())
			out = append(out, n)
		}
	}
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
