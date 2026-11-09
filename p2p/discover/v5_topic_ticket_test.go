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
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/discover/v5wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

// TestTicketNotBoundToAdvertiser checks that a ticket issued to one advertiser
// is not credited to another (F4 of the registrar overhead study, fixed by #160:
// tickets carry the advertiser's node ID). The second advertiser is treated as
// a new attempt and must not be admitted on the first one's waiting time.
func TestTicketNotBoundToAdvertiser(t *testing.T) {
	clock := new(mclock.Simulated)
	clock.Run(time.Hour)
	test := newUDPV5TestClock(t, clock)
	defer test.close()

	// Occupy the topic so that a fresh request gets a wait ticket. Done before
	// the first packet, which orders it before the dispatch goroutine's access.
	for i := 0; i < 50; i++ {
		var r enr.Record
		r.Set(enr.IP(net.IP{20, byte(i), 0, 1}))
		test.udp.topicTable.Add(enode.SignNull(&r, enode.ID{byte(i), 1}), testTopic1)
	}

	var (
		keyA, addrA = newkey(), netip.MustParseAddrPort("10.0.1.1:30303")
		keyB, addrB = newkey(), netip.MustParseAddrPort("10.0.2.2:30303")
		ticket      []byte
		wait        time.Duration
	)
	test.packetInFrom(keyA, addrA, &v5wire.Regtopic{
		ReqID: []byte{1}, Topic: testTopic1,
		ENR: signedRecord(t, keyA, addrA.Addr(), netip.Addr{}, addrA.Port()),
	})
	test.waitPacketOut(func(p *v5wire.Regconfirmation, _ netip.AddrPort, _ v5wire.Nonce) {
		ticket, wait = p.Ticket, time.Duration(p.WaitTime)*time.Millisecond
	})
	if len(ticket) == 0 || wait <= 0 {
		t.Fatalf("advertiser A was not given a wait ticket (wait %v)", wait)
	}
	clock.Run(wait)

	// Advertiser B presents A's ticket with its own record.
	test.packetInFrom(keyB, addrB, &v5wire.Regtopic{
		ReqID: []byte{2}, Topic: testTopic1, Ticket: ticket,
		ENR: signedRecord(t, keyB, addrB.Addr(), netip.Addr{}, addrB.Port()),
	})
	var confirmed bool
	test.waitPacketOut(func(p *v5wire.Regconfirmation, _ netip.AddrPort, _ v5wire.Nonce) {
		confirmed = len(p.Ticket) == 0 // registered successfully
	})
	admitted := false
	for _, n := range test.udp.LocalTopicNodes(testTopic1) {
		if n.ID() == enode.PubkeyToIDV4(&keyB.PublicKey) {
			admitted = true
		}
	}
	t.Logf("ticket issued to A (wait %v), presented by B after the wait: confirmed=%v, B in cache=%v", wait, confirmed, admitted)
	if admitted || confirmed {
		t.Error("advertiser B was admitted with a ticket issued to advertiser A")
	}
}
