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
	"net/netip"
	"testing"

	"github.com/ethereum/go-ethereum/p2p/discover/v5wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

func TestWireStatsDisabled(t *testing.T) {
	defer func(on bool) { wireStatsOn = on }(wireStatsOn)
	wireStatsOn = false

	node := startLocalhostV5(t, Config{})
	defer node.Close()
	if node.WireStats() != nil {
		t.Fatal("stats returned while counting is disabled")
	}
}

func TestWireStats_findnode(t *testing.T) {
	defer func(on bool) { wireStatsOn = on }(wireStatsOn)
	EnableWireStats()
	test := newUDPV5Test(t)
	defer test.close()

	remote := test.getNode(test.remotekey, test.remoteaddr).Node()
	done := make(chan error, 1)
	go func() {
		_, err := test.udp.Findnode(remote, []uint{230})
		done <- err
	}()
	test.waitPacketOut(func(p *v5wire.Findnode, _ netip.AddrPort, _ v5wire.Nonce) {
		test.packetIn(&v5wire.Nodes{ReqID: p.ReqID, RespCount: 1})
	})
	if err := <-done; err != nil {
		t.Fatalf("findnode: %v", err)
	}
	stats := test.udp.WireStats()
	if c := stats["FINDNODE/v5"]; c.TxMsgs != 1 || c.TxBytes == 0 {
		t.Fatalf("FINDNODE not counted: %+v", c)
	}
	if c := stats["NODES/v5"]; c.RxMsgs != 1 || c.RxBytes == 0 {
		t.Fatalf("NODES not counted: %+v", c)
	}
}

func TestWireStats_timeouts(t *testing.T) {
	defer func(on bool) { wireStatsOn = on }(wireStatsOn)
	EnableWireStats()
	test := newUDPV5Test(t)
	defer test.close()

	ghost := func(addr netip.AddrPort) *enode.Node {
		ln := test.getNode(newkey(), addr)
		ln.Set(TestbedGhostMark)
		return ln.Node()
	}
	foreign := ghost(netip.MustParseAddrPort("10.0.1.98:20001"))
	own := ghost(netip.MustParseAddrPort("10.0.1.97:20002"))
	pinNode(test.table, own)
	if !IsTestbedGhost(own) || IsTestbedGhost(test.udp.Self()) {
		t.Fatal("IsTestbedGhost misreports the mark")
	}

	for _, n := range []*enode.Node{foreign, own} {
		done := make(chan error, 1)
		go func() {
			_, err := test.udp.Findnode(n, []uint{250})
			done <- err
		}()
		test.waitPacketOut(func(*v5wire.Findnode, netip.AddrPort, v5wire.Nonce) {})
		if err := <-done; err != errTimeout {
			t.Fatalf("findnode to %v: got %v, want timeout", n.ID(), err)
		}
	}
	c := test.udp.WireStats()["FINDNODE/v5"]
	if c.Timeouts != 2 || c.GhostTimeouts != 2 || c.OwnGhostTimeouts != 1 {
		t.Fatalf("wrong timeout counters: %+v", c)
	}
}
