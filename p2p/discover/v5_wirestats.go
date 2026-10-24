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
	"sync"

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

	Timeouts         int64 `json:"timeouts"`
	GhostTimeouts    int64 `json:"ghostTimeouts"`    // calls to records carrying TestbedGhost
	OwnGhostTimeouts int64 `json:"ownGhostTimeouts"` // of which to a ghost pinned in this table
}

// TestbedGhost is the "ghost" ENR entry the testbed sets on unreachable records
// it pins into tables. Only the wire counters read it.
type TestbedGhost uint

// TestbedGhostMark is the value of the entry on testbed ghost records.
const TestbedGhostMark TestbedGhost = 1

func (TestbedGhost) ENRKey() string { return "ghost" }

// IsTestbedGhost reports whether n is a testbed ghost record.
func IsTestbedGhost(n *enode.Node) bool {
	var v TestbedGhost
	return n.Record().Load(&v) == nil && v == TestbedGhostMark
}

type wireStats struct {
	mu sync.Mutex
	m  map[string]*WireCounter
}

func newWireStats() *wireStats {
	if !wireStatsOn {
		return nil
	}
	return &wireStats{m: make(map[string]*WireCounter)}
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

func (ws *wireStats) countTimeout(name string, ghost, own bool) {
	if ws == nil {
		return
	}
	ws.mu.Lock()
	c := ws.counter(name)
	c.Timeouts++
	if ghost {
		c.GhostTimeouts++
	}
	if own {
		c.OwnGhostTimeouts++
	}
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
// (e.g. "FINDNODE/v5", "NODES/v5"); sizes are encrypted on-wire bytes.
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
