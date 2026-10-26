// Copyright 2022 The go-ethereum Authors
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
	"encoding/binary"
	"reflect"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// This test checks basic ticket encoding/decoding.
func TestTicketSealerSimple(t *testing.T) {
	clock := new(mclock.Simulated)
	ts := NewTicketSealer(clock)

	ticket := &Ticket{
		Topic:          topic1,
		Node:           enode.ID{1, 2, 3},
		FirstIssued:    clock.Now(),
		WaitTimeIssued: 1 * time.Minute,
		LastUsed:       clock.Now(),
	}
	enc := ts.Pack(ticket)
	if len(enc) != encTicketFullSize {
		t.Fatal("encoded ticket has wrong size")
	}

	clock.Run(1 * time.Minute)

	ticket2, err := ts.Unpack(ticket.Topic, enc)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ticket, ticket2) {
		t.Fatal("decoded ticket not equal")
	}

	// The node is covered by the MAC.
	enc[ticketMacSize+2+len(TopicID{})] ^= 0xff
	if _, err := ts.Unpack(ticket.Topic, enc); err != errBadTicketMAC {
		t.Fatal("ticket with altered node not rejected:", err)
	}
}

func TestTicketSealerInvalid(t *testing.T) {
	clock := new(mclock.Simulated)
	ts := NewTicketSealer(clock)

	enc := ts.Pack(&Ticket{
		Topic:          topic1,
		FirstIssued:    clock.Now(),
		WaitTimeIssued: 5 * time.Minute,
		LastUsed:       clock.Now(),
	})
	if _, err := ts.Unpack(topic2, enc); err != errWrongTicketTopic {
		t.Fatal("unpacking with wrong topic did not raise correct error")
	}
	ticket, err := ts.Unpack(topic1, enc)
	if err != nil {
		t.Fatal("unpacking early ticket failed:", err)
	}
	if timing := ticket.TimingAt(clock.Now()); timing != TicketEarly {
		t.Fatal("wrong timing for early ticket:", timing)
	}
}

func TestTicketTiming(t *testing.T) {
	var (
		issued = mclock.AbsTime(1000)
		wait   = time.Minute
		start  = issued.Add(wait)
		ticket = Ticket{LastUsed: issued, WaitTimeIssued: wait}
	)
	if ticket.WindowStart() != start {
		t.Fatalf("WindowStart = %v, want %v", ticket.WindowStart(), start)
	}
	tests := []struct {
		at   mclock.AbsTime
		want TicketTiming
	}{
		{start - 1, TicketEarly},
		{start, TicketOnTime},
		{start.Add(ticketValidityWindow) - 1, TicketOnTime},
		{start.Add(ticketValidityWindow), TicketLate},
		{start.Add(ticketValidityWindow+ticketLateGrace) - 1, TicketLate},
		{start.Add(ticketValidityWindow + ticketLateGrace), TicketExpired},
	}
	for _, test := range tests {
		if got := ticket.TimingAt(test.at); got != test.want {
			t.Errorf("TimingAt(start%+d) = %v, want %v", test.at-start, got, test.want)
		}
	}
}

// This test checks that tickets sealed with an older key
// are accepted after a key rotation happened.
// TestTicketSealerKeyReuse verifies that consecutive Pack calls reuse the
// same signing key until ticketRekeyInterval is reached.
func TestTicketSealerKeyReuse(t *testing.T) {
	prevRekeyInterval := ticketRekeyInterval
	defer func() { ticketRekeyInterval = prevRekeyInterval }()
	ticketRekeyInterval = 10

	clock := new(mclock.Simulated)
	ts := NewTicketSealer(clock)

	// Pack 25 tickets. With rekey interval 10, this should allocate
	// exactly 3 keys: one for tickets 1..10, one for 11..20, one for 21..25.
	for i := 0; i < 25; i++ {
		ts.Pack(&Ticket{
			Topic:          topic1,
			FirstIssued:    clock.Now(),
			WaitTimeIssued: ticketKeyLifetime - 3,
			LastUsed:       clock.Now(),
		})
	}

	if got, want := len(ts.keys), 3; got != want {
		t.Fatalf("wrong number of signing keys allocated: got %d, want %d", got, want)
	}
}

func TestTicketSealerKeyRotation(t *testing.T) {
	prevRekeyInterval := ticketRekeyInterval
	defer func() { ticketRekeyInterval = prevRekeyInterval }()

	ticketRekeyInterval = 10

	clock := new(mclock.Simulated)
	ts := NewTicketSealer(clock)

	var encTickets [][]byte
	for i := 0; i < ticketRekeyInterval*2; i++ {
		ticket := &Ticket{
			Topic:          topic1,
			FirstIssued:    clock.Now(),
			WaitTimeIssued: ticketKeyLifetime - 3,
			LastUsed:       clock.Now(),
		}
		encTickets = append(encTickets, ts.Pack(ticket))
	}

	clock.Run(ticketKeyLifetime - 1)
	for i, enc := range encTickets {
		if _, err := ts.Unpack(topic1, enc); err != nil {
			t.Fatalf("can't unpack ticket %d: %v", i, err)
		}
	}

	clock.Run(1)
	for i, enc := range encTickets {
		if _, err := ts.Unpack(topic1, enc); err != errNoTicketKey {
			t.Fatalf("Unpack returned wrong error for ticket %d with expired key: %v", i, err)
		}
	}
}

func TestUsedTickets(t *testing.T) {
	clock := new(mclock.Simulated)
	u := NewUsedTickets(clock)
	mac := func(i int) []byte {
		enc := make([]byte, encTicketFullSize)
		binary.BigEndian.PutUint32(enc, uint32(i))
		return enc
	}

	if u.Use(mac(0)) {
		t.Fatal("first use reported as replay")
	}
	if !u.Use(mac(0)) {
		t.Fatal("second use not reported as replay")
	}
	clock.Run(usedTicketTTL - 1)
	if !u.Use(mac(0)) {
		t.Fatal("ticket forgotten before its TTL")
	}
	clock.Run(1)
	if u.Use(mac(0)) {
		t.Fatal("ticket remembered past its TTL")
	}

	u = NewUsedTickets(clock)
	for i := 0; i <= usedTicketLimit; i++ {
		if u.Use(mac(i)) {
			t.Fatalf("ticket %d reported as replay", i)
		}
	}
	if u.Len() != usedTicketLimit {
		t.Fatalf("Len = %d, want %d", u.Len(), usedTicketLimit)
	}
	if u.Use(mac(0)) {
		t.Fatal("oldest ticket not dropped past the limit")
	}
}
