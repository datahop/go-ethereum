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

package discover

import (
	"bytes"
	"sync"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/discover/v5wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// Per-transport, per-message-type wire counters. Off by default; the testbed
// enables them to attribute traffic to message types, which the encrypted
// packets on the conn layer cannot provide.

var wireStatsOn bool

// EnableWireStats turns on per-message-type packet counting for transports
// created afterwards.
func EnableWireStats() { wireStatsOn = true }

// WireCounter holds cumulative wire traffic for one message type.
type WireCounter struct {
	TxMsgs  int64 `json:"txMsgs"`
	TxBytes int64 `json:"txBytes"`
	RxMsgs  int64 `json:"rxMsgs"`
	RxBytes int64 `json:"rxBytes"`
}

// renewalRegtopicName is the counter key for REGTOPIC requests to a registrar
// that already admitted the advertiser once; first registrations keep
// "REGTOPIC/v5".
const renewalRegtopicName = "REGTOPIC(renewal)/v5"

// OpKey identifies a topic operation: the request message name ("REGTOPIC/v5"
// or "TOPICQUERY/v5") and the operation ID given to RegisterTopic or TopicSearch.
type OpKey struct {
	Msg  string
	OpID uint64
}

// OpCounter holds the traffic of one topic operation: the requests it sent, the
// responses and handshake challenges they received, and the distinct nodes asked.
type OpCounter struct {
	WireCounter
	Nodes int `json:"nodes"`
}

// TopicLoad holds the topic requests a node received for one topic, and the
// responses it sent to them.
type TopicLoad struct {
	Regtopic   WireCounter `json:"regtopic"`
	TopicQuery WireCounter `json:"topicQuery"`
}

type opCounter struct {
	WireCounter
	nodes map[enode.ID]struct{}
}

type wireStats struct {
	mu       sync.Mutex
	m        map[string]*WireCounter
	renewals map[v5wire.Packet]struct{}
	ops      map[OpKey]*opCounter
	topics   map[topicindex.TopicID]*TopicLoad
	handling *WireCounter // topic request being handled on the dispatch goroutine
}

func newWireStats() *wireStats {
	if !wireStatsOn {
		return nil
	}
	return &wireStats{
		m:        make(map[string]*WireCounter),
		renewals: make(map[v5wire.Packet]struct{}),
		ops:      make(map[OpKey]*opCounter),
		topics:   make(map[topicindex.TopicID]*TopicLoad),
	}
}

func opKey(p v5wire.Packet) (OpKey, bool) {
	switch p := p.(type) {
	case *v5wire.Regtopic:
		return OpKey{p.Name(), p.OpID}, true
	case *v5wire.TopicQuery:
		return OpKey{p.Name(), p.OpID}, true
	}
	return OpKey{}, false
}

func (ws *wireStats) op(key OpKey) *opCounter {
	c := ws.ops[key]
	if c == nil {
		c = &opCounter{nodes: make(map[enode.ID]struct{})}
		ws.ops[key] = c
	}
	return c
}

// countOpTx counts a sent packet toward its topic operation, or toward the
// topic request being handled when it is a response.
func (ws *wireStats) countOpTx(toID enode.ID, p v5wire.Packet, bytes int) {
	if ws == nil {
		return
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if key, ok := opKey(p); ok {
		c := ws.op(key)
		c.TxMsgs++
		c.TxBytes += int64(bytes)
		c.nodes[toID] = struct{}{}
	} else if ws.handling != nil {
		ws.handling.TxMsgs++
		ws.handling.TxBytes += int64(bytes)
	}
}

// countOpRx counts a received packet toward the topic operation whose request
// it answers. req is the request of the matching call, or nil.
func (ws *wireStats) countOpRx(req v5wire.Packet, bytes int) {
	key, ok := opKey(req)
	if ws == nil || !ok {
		return
	}
	ws.mu.Lock()
	c := ws.op(key)
	c.RxMsgs++
	c.RxBytes += int64(bytes)
	ws.mu.Unlock()
}

// beginRequest counts a received REGTOPIC or TOPICQUERY toward its topic, and
// attributes the packets sent until endRequest to it.
func (ws *wireStats) beginRequest(p v5wire.Packet, bytes int) {
	if ws == nil {
		return
	}
	var (
		topic topicindex.TopicID
		reg   bool
	)
	switch p := p.(type) {
	case *v5wire.Regtopic:
		topic, reg = p.Topic, true
	case *v5wire.TopicQuery:
		topic = p.Topic
	default:
		return
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	l := ws.topics[topic]
	if l == nil {
		l = new(TopicLoad)
		ws.topics[topic] = l
	}
	ws.handling = &l.TopicQuery
	if reg {
		ws.handling = &l.Regtopic
	}
	ws.handling.RxMsgs++
	ws.handling.RxBytes += int64(bytes)
}

func (ws *wireStats) endRequest() {
	if ws == nil {
		return
	}
	ws.mu.Lock()
	ws.handling = nil
	ws.mu.Unlock()
}

func (ws *wireStats) markRenewal(p v5wire.Packet, on bool) {
	if ws == nil {
		return
	}
	ws.mu.Lock()
	if on {
		ws.renewals[p] = struct{}{}
	} else {
		delete(ws.renewals, p)
	}
	ws.mu.Unlock()
}

func (ws *wireStats) txName(p v5wire.Packet) string {
	if ws == nil {
		return ""
	}
	ws.mu.Lock()
	_, ok := ws.renewals[p]
	ws.mu.Unlock()
	if ok {
		return renewalRegtopicName
	}
	return p.Name()
}

func (ws *wireStats) counter(name string) *WireCounter {
	c := ws.m[name]
	if c == nil {
		c = new(WireCounter)
		ws.m[name] = c
	}
	return c
}

func (ws *wireStats) countTx(name string, bytes int) {
	if ws == nil {
		return
	}
	ws.mu.Lock()
	c := ws.counter(name)
	c.TxMsgs++
	c.TxBytes += int64(bytes)
	ws.mu.Unlock()
}

func (ws *wireStats) countRx(name string, bytes int) {
	if ws == nil {
		return
	}
	ws.mu.Lock()
	c := ws.counter(name)
	c.RxMsgs++
	c.RxBytes += int64(bytes)
	ws.mu.Unlock()
}

// WireStats returns a snapshot of the per-message-type counters, or nil when
// counting is not enabled. The message-type key is the v5wire packet name
// (e.g. "REGTOPIC/v5", "TOPICQUERY/v5"); sizes are encrypted on-wire bytes.
func (t *UDPv5) WireStats() map[string]WireCounter {
	if t.wireStats == nil {
		return nil
	}
	t.wireStats.mu.Lock()
	defer t.wireStats.mu.Unlock()
	out := make(map[string]WireCounter, len(t.wireStats.m))
	for k, v := range t.wireStats.m {
		out[k] = *v
	}
	return out
}

// OpStats returns the traffic of each topic operation, or nil when counting is
// not enabled. Renewals count as REGTOPIC/v5.
func (t *UDPv5) OpStats() map[OpKey]OpCounter {
	if t.wireStats == nil {
		return nil
	}
	t.wireStats.mu.Lock()
	defer t.wireStats.mu.Unlock()
	out := make(map[OpKey]OpCounter, len(t.wireStats.ops))
	for k, v := range t.wireStats.ops {
		out[k] = OpCounter{WireCounter: v.WireCounter, Nodes: len(v.nodes)}
	}
	return out
}

// TopicLoadStats returns the topic requests received per topic and the responses
// sent to them, or nil when counting is not enabled.
func (t *UDPv5) TopicLoadStats() map[topicindex.TopicID]TopicLoad {
	if t.wireStats == nil {
		return nil
	}
	t.wireStats.mu.Lock()
	defer t.wireStats.mu.Unlock()
	out := make(map[topicindex.TopicID]TopicLoad, len(t.wireStats.topics))
	for k, v := range t.wireStats.topics {
		out[k] = *v
	}
	return out
}

// callRequest returns the request of the active call that the received packet
// answers, or nil.
func (t *UDPv5) callRequest(fromID enode.ID, p v5wire.Packet) v5wire.Packet {
	var c *callV5
	switch p := p.(type) {
	case *v5wire.Whoareyou:
		c = t.activeCallByAuth[p.Nonce]
	case *v5wire.Nodes, *v5wire.TopicNodes, *v5wire.Regconfirmation:
		if ac := t.activeCallByNode[fromID]; ac != nil && bytes.Equal(p.RequestID(), ac.reqid) {
			c = ac
		}
	}
	if c == nil {
		return nil
	}
	return c.packet
}

// wireStatsName is the counter key for a received packet.
//
// A packet that cannot be decrypted decodes to v5wire.Unknown, whose name is
// "UNKNOWN/v5". That is accurate on the wire but misleading in a cost
// breakdown: these are not unknown *messages*, they are ordinary messages from
// a peer this node has no session with yet. The receiver answers each one with
// a WHOAREYOU and the sender repeats the message inside a handshake, so the
// bytes are session-establishment overhead, and they only ever appear on the
// receive side.
func wireStatsName(p v5wire.Packet) string {
	if p.Kind() == v5wire.UnknownPacket {
		return "SESSION-SETUP(undecryptable)"
	}
	return p.Name()
}
