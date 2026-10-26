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
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
)

// This is the maximum number of times a ticketKey can be used for encoding.
// When reached, TicketSealer creates a new key.
var ticketRekeyInterval = 100000

const (
	ticketKeyLifetime    = 6 * time.Hour
	ticketValidityWindow = 5 * time.Second
	ticketMacSize        = 32

	// ticketLateGrace is how long past its window a ticket is still credited its
	// scheduled wait. It is short against the ad lifetime, so a ticket cannot be
	// replayed once the ad it earned has expired.
	ticketLateGrace = 10 * time.Second
)

// Ticket defines the content of generated tickets.
type Ticket struct {
	KeyID          uint16
	Topic          TopicID
	WaitTimeIssued time.Duration  // wait time returned in last REGCONFIRMATION
	LastUsed       mclock.AbsTime // last request in this waiting session
	FirstIssued    mclock.AbsTime // creation time of first ticket in this waiting session
}

// WindowStart is the earliest time the ticket may be used.
func (t Ticket) WindowStart() mclock.AbsTime {
	return t.LastUsed.Add(t.WaitTimeIssued)
}

// TicketTiming says where a time falls relative to a ticket's validity window.
type TicketTiming int

const (
	TicketOnTime TicketTiming = iota
	TicketEarly
	TicketLate
	TicketExpired
)

// TimingAt reports whether now is before the ticket's window, inside it, after
// it within the grace, or past the grace.
func (t Ticket) TimingAt(now mclock.AbsTime) TicketTiming {
	start := t.WindowStart()
	switch {
	case now < start:
		return TicketEarly
	case now >= start.Add(ticketValidityWindow+ticketLateGrace):
		return TicketExpired
	case now >= start.Add(ticketValidityWindow):
		return TicketLate
	default:
		return TicketOnTime
	}
}

// TicketSealer can encode/decode tickets.
type TicketSealer struct {
	keys      []*ticketKey
	clock     mclock.Clock
	idCounter uint16
}

var (
	encTicketSize     = binary.Size(Ticket{})
	encTicketFullSize = ticketMacSize + encTicketSize
)

// ticketKey is a MAC key held by TicketSealer.
type ticketKey struct {
	keyID     uint16
	hmac      hash.Hash
	macBuffer []byte

	uses     int
	lastUsed mclock.AbsTime
}

func NewTicketSealer(clock mclock.Clock) *TicketSealer {
	return &TicketSealer{clock: clock}
}

var (
	errBadTicketSize    = errors.New("encoded ticket has wrong size")
	errBadTicketMAC     = errors.New("invalid ticket MAC")
	errNoTicketKey      = errors.New("unknown ticket MAC keyID")
	errWrongTicketTopic = errors.New("ticket used with wrong topic")
)

// Unpack decrypts, authenticates and deserializes a ticket. It does not check the
// ticket's timing, see Ticket.TimingAt.
func (ts *TicketSealer) Unpack(topic TopicID, input []byte) (*Ticket, error) {
	// Empty input means the sender has no ticket.
	if len(input) == 0 {
		return &Ticket{Topic: topic}, nil
	}
	// If non-empty, the ticket size must match exactly.
	if len(input) != encTicketFullSize {
		return nil, errBadTicketSize
	}

	// Read the ticket data.
	// Not great that this happens before checking MAC,
	// but KeyID from Ticket is needed to find the right key.
	enc := input[ticketMacSize:]
	var ticket Ticket
	r := bytes.NewReader(enc)
	if err := binary.Read(r, binary.BigEndian, &ticket); err != nil {
		return nil, err
	}

	// Find the matching MAC key.
	ts.gcKeys()
	mac := input[:ticketMacSize]
	for _, key := range ts.keys {
		if key.keyID != ticket.KeyID {
			continue
		}
		computed := key.computeMAC(enc)
		if !hmac.Equal(mac, computed) {
			return nil, errBadTicketMAC
		}
		if ticket.Topic != topic {
			return nil, errWrongTicketTopic
		}
		return &ticket, nil
	}
	return nil, errNoTicketKey
}

// Pack encodes a ticket and seals it.
// Note: this modifies KeyID of ticket.
func (ts *TicketSealer) Pack(ticket *Ticket) []byte {
	ts.gcKeys()
	key := ts.getValidKey()
	ticket.KeyID = key.keyID

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, ticket); err != nil {
		panic(err)
	}

	mac := key.computeMAC(buf.Bytes())
	output := make([]byte, len(mac)+buf.Len())
	copy(output, mac)
	copy(output[len(mac):], buf.Bytes())
	return output
}

// gcKeys removes old keys.
func (ts *TicketSealer) gcKeys() {
	deadline := ts.clock.Now().Add(-ticketKeyLifetime)
	cut := 0
	for _, key := range ts.keys {
		if key.lastUsed > deadline {
			break
		}
		cut++
	}
	if cut > 0 {
		copy(ts.keys, ts.keys[cut:])
		ts.keys = ts.keys[:len(ts.keys)-cut]
	}
}

// getValidKey ensures there is at least one valid key and returns it.
func (ts *TicketSealer) getValidKey() *ticketKey {
	if len(ts.keys) > 0 && ts.keys[len(ts.keys)-1].uses < ticketRekeyInterval {
		// Current latest key is still valid.
		key := ts.keys[len(ts.keys)-1]
		key.uses++
		key.lastUsed = ts.clock.Now()
		return key
	}

	// Generate key ID.
	id := ts.idCounter
	ts.idCounter++

	// Generate key.
	keybytes := make([]byte, 32)
	if _, err := rand.Read(keybytes); err != nil {
		panic("can't get random data")
	}
	mac := hmac.New(sha256.New, keybytes)
	key := &ticketKey{
		hmac:     mac,
		keyID:    id,
		uses:     1,
		lastUsed: ts.clock.Now(),
	}
	if ticketMacSize != key.hmac.Size() {
		panic("ticketMacSize is wrong: " + strconv.Itoa(key.hmac.Size()))
	}

	// Store the key.
	ts.keys = append(ts.keys, key)
	return key
}

func (key *ticketKey) computeMAC(data []byte) []byte {
	key.hmac.Reset()
	key.hmac.Write(data)
	key.macBuffer = key.hmac.Sum(key.macBuffer[:0])
	return key.macBuffer
}

const (
	// usedTicketTTL is how long a credited ticket is remembered: the span in
	// which it could still be credited.
	usedTicketTTL = ticketValidityWindow + ticketLateGrace
	// usedTicketLimit caps the memory. Past it the oldest entry is dropped: only a
	// flood of credited tickets reaches it, and the flooder's entries go first.
	usedTicketLimit = 1 << 14
)

// UsedTickets remembers credited tickets by their MAC, so that a ticket is
// credited once.
type UsedTickets struct {
	clock mclock.Clock
	set   map[[ticketMacSize]byte]struct{}
	queue []usedTicket // in insertion order
}

type usedTicket struct {
	mac [ticketMacSize]byte
	exp mclock.AbsTime
}

func NewUsedTickets(clock mclock.Clock) *UsedTickets {
	return &UsedTickets{clock: clock, set: make(map[[ticketMacSize]byte]struct{})}
}

// Use records a ticket as credited and reports whether it was already. The
// ticket must be one that TicketSealer.Unpack accepted.
func (u *UsedTickets) Use(enc []byte) (replay bool) {
	now := u.clock.Now()
	for len(u.queue) > 0 && u.queue[0].exp <= now {
		u.dropOldest()
	}
	mac := [ticketMacSize]byte(enc[:ticketMacSize])
	if _, ok := u.set[mac]; ok {
		return true
	}
	if len(u.queue) >= usedTicketLimit {
		u.dropOldest()
	}
	u.set[mac] = struct{}{}
	u.queue = append(u.queue, usedTicket{mac: mac, exp: now.Add(usedTicketTTL)})
	return false
}

// Len returns the number of remembered tickets.
func (u *UsedTickets) Len() int {
	return len(u.queue)
}

func (u *UsedTickets) dropOldest() {
	delete(u.set, u.queue[0].mac)
	u.queue = u.queue[1:]
}
