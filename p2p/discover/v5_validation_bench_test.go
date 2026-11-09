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

// Benchmarks of the per-request validation work that handleRegtopic does
// besides the registrar core (TopicTable.RegisterAt): decoding and verifying
// the advertiser's record, the source-address check, and ticket handling.

import (
	"crypto/sha256"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/rlp"
)

// validationRecord returns an RLP-encoded, signed record like an advertiser's:
// IPv4, UDP and TCP ports and the topic discovery entry. maxSize pads it to
// the 300-byte limit.
func validationRecord(b *testing.B, i int, maxSize bool) ([]byte, netip.Addr) {
	b.Helper()
	digest := sha256.Sum256([]byte(fmt.Sprintf("validation/%d", i)))
	key, err := crypto.ToECDSA(digest[:])
	if err != nil {
		b.Fatal(err)
	}
	ip := netip.AddrFrom4([4]byte{52, byte(i >> 8), byte(i), 7})
	build := func(pad int) ([]byte, error) {
		var r enr.Record
		r.Set(enr.IP(net.IP(ip.AsSlice())))
		r.Set(enr.UDP(30303))
		r.Set(enr.TCP(30303))
		r.Set(topicindex.TopicDiscoveryVersion)
		if pad > 0 {
			r.Set(enr.WithEntry("zz", make([]byte, pad)))
		}
		if err := enode.SignV4(&r, key); err != nil {
			return nil, err
		}
		return rlp.EncodeToBytes(&r)
	}
	enc, err := build(0)
	if err != nil {
		b.Fatal(err)
	}
	// For maxSize, grow an opaque entry while the record stays within the limit.
	for pad := 1; maxSize; pad++ {
		next, err := build(pad)
		if err != nil || len(next) > enr.SizeLimit {
			break
		}
		enc = next
	}
	if len(enc) > enr.SizeLimit {
		b.Fatalf("record is %d bytes, limit %d", len(enc), enr.SizeLimit)
	}
	return enc, ip
}

// BenchmarkValidationENR measures what handleRegtopic does with the record of
// every REGTOPIC: RLP decoding (done by the packet decoder) and enode.New, which
// verifies the signature.
func BenchmarkValidationENR(b *testing.B) {
	for _, size := range []struct {
		name string
		max  bool
	}{{"typical", false}, {"max", true}} {
		b.Run("size="+size.name, func(b *testing.B) {
			const n = 256
			encs := make([][]byte, n)
			for i := range encs {
				encs[i], _ = validationRecord(b, i, size.max)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var r enr.Record
				if err := rlp.DecodeBytes(encs[i%n], &r); err != nil {
					b.Fatal(err)
				}
				if _, err := enode.New(enode.ValidSchemes, &r); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(encs[0])), "record-bytes")
		})
	}
}

// BenchmarkValidationEndpoint measures checkRegtopicRecordIP, the comparison of
// the record's address with the packet source.
func BenchmarkValidationEndpoint(b *testing.B) {
	enc, ip := validationRecord(b, 1, false)
	var r enr.Record
	if err := rlp.DecodeBytes(enc, &r); err != nil {
		b.Fatal(err)
	}
	n, err := enode.New(enode.ValidSchemes, &r)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := checkRegtopicRecordIP(n, ip); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkValidationTicket measures ticket sealing (encode and MAC) for a wait
// response, unsealing (MAC check and decode) of a retry's ticket, and the
// replay check (UsedTickets.Use) of a credited ticket.
func BenchmarkValidationTicket(b *testing.B) {
	clock := new(mclock.Simulated)
	clock.Run(time.Hour)
	var topic topicindex.TopicID
	topic[0] = 1
	ticket := func(i int) *topicindex.Ticket {
		return &topicindex.Ticket{Topic: topic, Node: enode.ID{byte(i), byte(i >> 8)}, WaitTimeIssued: time.Minute, LastUsed: clock.Now(), FirstIssued: clock.Now()}
	}
	b.Run("op=pack", func(b *testing.B) {
		sealer := topicindex.NewTicketSealer(clock)
		t := ticket(1)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sealer.Pack(t)
		}
	})
	b.Run("op=unpack", func(b *testing.B) {
		sealer := topicindex.NewTicketSealer(clock)
		enc := sealer.Pack(ticket(1))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := sealer.Unpack(topic, enc); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("op=replay-check", func(b *testing.B) {
		sealer := topicindex.NewTicketSealer(clock)
		const n = 4096
		encs := make([][]byte, n)
		for i := range encs {
			encs[i] = append([]byte(nil), sealer.Pack(ticket(i))...)
		}
		used := topicindex.NewUsedTickets(clock)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if i%n == 0 && i > 0 {
				b.StopTimer()
				clock.Run(time.Minute) // expire the remembered tickets
				b.StartTimer()
			}
			used.Use(encs[i%n])
		}
	})
}
