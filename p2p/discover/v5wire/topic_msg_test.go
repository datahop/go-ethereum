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

package v5wire

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/rlp"
)

// TestTopicMessageRLP checks that each topic-discovery message type survives an
// RLP encode → decode → re-encode round-trip losslessly.
func TestTopicMessageRLP(t *testing.T) {
	rec := signedTestRecord(t)
	topic := [32]byte{1, 2, 3, 4, 5}

	msgs := []Packet{
		&Regtopic{ReqID: []byte{1, 2, 3}, Topic: topic, Ticket: []byte{9, 9}, ENR: rec, Buckets: []uint{250, 251}},
		&Regconfirmation{ReqID: []byte{4}, RespCount: 2, Ticket: []byte{7, 7}, WaitTime: 1500},
		&TopicQuery{ReqID: []byte{5}, Topic: topic, Buckets: []uint{248}},
		&TopicNodes{ReqID: []byte{6}, RespCount: 3, Nodes: []*enr.Record{rec}},
	}
	for _, msg := range msgs {
		enc, err := rlp.EncodeToBytes(msg)
		if err != nil {
			t.Fatalf("%s encode: %v", msg.Name(), err)
		}
		dec, err := DecodeMessage(msg.Kind(), enc)
		if err != nil {
			t.Fatalf("%s decode: %v", msg.Name(), err)
		}
		if dec.Kind() != msg.Kind() {
			t.Errorf("%s decoded to kind %d, want %d", msg.Name(), dec.Kind(), msg.Kind())
		}
		reenc, err := rlp.EncodeToBytes(dec)
		if err != nil {
			t.Fatalf("%s re-encode: %v", msg.Name(), err)
		}
		if !bytes.Equal(enc, reenc) {
			t.Errorf("%s: RLP not stable across round-trip\n enc=%x\nreenc=%x", msg.Name(), enc, reenc)
		}
	}
}

// raw mirror types let the test place malformed values where the real message
// types (fixed-size arrays, uint8, *enr.Record) would otherwise prevent them.
type rawTopicQuery struct {
	ReqID, Topic []byte
	Buckets      []uint
}

type rawTopicQueryExtra struct {
	ReqID, Topic []byte
	Buckets      []uint
	Extra        []byte
}

type rawRegtopic struct {
	ReqID, Topic, Ticket []byte
	ENR                  rlp.RawValue
	Buckets              []uint
}

type rawRegconfirmation struct {
	ReqID     []byte
	RespCount uint
	Ticket    []byte
	WaitTime  uint
}

type rawTopicNodes struct {
	ReqID     []byte
	RespCount uint
	Nodes     []rlp.RawValue
}

// TestTopicMessageDecodeErrors checks that DecodeMessage rejects malformed topic
// messages — unknown type, empty/non-list bodies, missing/extra fields, wrong
// fixed-array sizes, integer overflow, malformed embedded ENRs, trailing data,
// truncated bodies, and oversized request ids — with an error and never a panic.
func TestTopicMessageDecodeErrors(t *testing.T) {
	rec := signedTestRecord(t)
	var topic [32]byte

	tooFew := [][]byte{{0x01}} // one-element list: fewer fields than any topic message
	trailing := func(b []byte) []byte { return append(append([]byte{}, b...), 0x00) }
	truncated := func(b []byte) []byte { return b[:len(b)-1] } // drop the last byte of a valid encoding

	cases := []struct {
		name string
		kind byte
		body []byte
		want error // if non-nil, the exact error expected
	}{
		{"unknown-type", 0xEE, rlpMust(tooFew), nil},

		// empty / not-a-list bodies
		{"regtopic-empty", RegtopicMsg, nil, nil},
		{"regtopic-not-list", RegtopicMsg, []byte{0x01}, nil},
		{"topicquery-not-list", TopicQueryMsg, []byte{0x01}, nil},

		// missing fields (too few list elements)
		{"regtopic-too-few", RegtopicMsg, rlpMust(tooFew), nil},
		{"regconfirmation-too-few", RegconfirmationMsg, rlpMust(tooFew), nil},
		{"topicquery-too-few", TopicQueryMsg, rlpMust(tooFew), nil},
		{"topicnodes-too-few", TopicNodesMsg, rlpMust(tooFew), nil},

		// extra trailing data after a valid message
		{"topicquery-trailing", TopicQueryMsg, trailing(rlpMust(&TopicQuery{ReqID: []byte{1}, Topic: topic, Buckets: []uint{}})), nil},
		{"topicnodes-trailing", TopicNodesMsg, trailing(rlpMust(&TopicNodes{ReqID: []byte{1}, RespCount: 1, Nodes: []*enr.Record{rec}})), nil},

		// truncated valid message (list header promises more bytes than are present)
		{"regtopic-truncated", RegtopicMsg, truncated(rlpMust(&Regtopic{ReqID: []byte{1}, Topic: topic, Ticket: []byte{2}, ENR: rec, Buckets: []uint{}})), nil},
		{"topicquery-truncated", TopicQueryMsg, truncated(rlpMust(&TopicQuery{ReqID: []byte{1}, Topic: topic, Buckets: []uint{}})), nil},
		{"topicnodes-truncated", TopicNodesMsg, truncated(rlpMust(&TopicNodes{ReqID: []byte{1}, RespCount: 1, Nodes: []*enr.Record{rec}})), nil},

		// oversized request id (> 8 bytes)
		{"regtopic-bigreqid", RegtopicMsg, rlpMust(&Regtopic{ReqID: make([]byte, 9), Topic: topic, ENR: rec, Buckets: []uint{}}), ErrInvalidReqID},
		{"regconfirmation-bigreqid", RegconfirmationMsg, rlpMust(&Regconfirmation{ReqID: make([]byte, 9), RespCount: 1}), ErrInvalidReqID},
		{"topicquery-bigreqid", TopicQueryMsg, rlpMust(&TopicQuery{ReqID: make([]byte, 9), Topic: topic, Buckets: []uint{}}), ErrInvalidReqID},
		{"topicnodes-bigreqid", TopicNodesMsg, rlpMust(&TopicNodes{ReqID: make([]byte, 9), RespCount: 1}), ErrInvalidReqID},

		// too many list elements (distinct from trailing bytes)
		{"topicquery-too-many", TopicQueryMsg, rlpMust(&rawTopicQueryExtra{ReqID: []byte{1}, Topic: make([]byte, 32), Buckets: []uint{}, Extra: []byte{2}}), nil},

		// fixed-size Topic field with wrong length
		{"topicquery-topic-short", TopicQueryMsg, rlpMust(&rawTopicQuery{ReqID: []byte{1}, Topic: make([]byte, 31), Buckets: []uint{}}), nil},
		{"topicquery-topic-long", TopicQueryMsg, rlpMust(&rawTopicQuery{ReqID: []byte{1}, Topic: make([]byte, 33), Buckets: []uint{}}), nil},

		// integer overflow into uint8 RespCount
		{"regconfirmation-respcount-overflow", RegconfirmationMsg, rlpMust(&rawRegconfirmation{ReqID: []byte{1}, RespCount: 256}), nil},
		{"topicnodes-respcount-overflow", TopicNodesMsg, rlpMust(&rawTopicNodes{ReqID: []byte{1}, RespCount: 256}), nil},

		// malformed embedded ENR
		{"regtopic-bad-enr", RegtopicMsg, rlpMust(&rawRegtopic{ReqID: []byte{1}, Topic: make([]byte, 32), ENR: rlp.RawValue{0x01}, Buckets: []uint{}}), nil},
		{"topicnodes-bad-enr", TopicNodesMsg, rlpMust(&rawTopicNodes{ReqID: []byte{1}, RespCount: 1, Nodes: []rlp.RawValue{{0x01}}}), nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := DecodeMessage(c.kind, c.body)
			if err == nil {
				t.Fatal("expected decode error, got nil")
			}
			if c.want != nil && err != c.want {
				t.Fatalf("got error %q, want %q", err, c.want)
			}
		})
	}
}

// TestTopicMessageReqIDBoundary checks that request ids up to the 8-byte limit
// (and empty) are accepted — the rejection only kicks in above 8 bytes.
func TestTopicMessageReqIDBoundary(t *testing.T) {
	for _, n := range []int{0, 1, 8} {
		body, err := rlp.EncodeToBytes(&TopicQuery{ReqID: make([]byte, n), Buckets: []uint{}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeMessage(TopicQueryMsg, body); err != nil {
			t.Errorf("request id length %d: unexpected error %v", n, err)
		}
	}
}

// rlpMust encodes v to RLP, panicking on error (for seed corpus / table setup).
func rlpMust(v interface{}) []byte {
	b, err := rlp.EncodeToBytes(v)
	if err != nil {
		panic(err)
	}
	return b
}

// signedTestRecord returns a minimal signed ENR for use in topic messages.
func signedTestRecord(tb testing.TB) *enr.Record {
	tb.Helper()
	var rec enr.Record
	rec.Set(enr.IPv4{127, 0, 0, 1})
	rec.Set(enr.UDP(30303))
	if err := enode.SignV4(&rec, testKeyA); err != nil {
		tb.Fatal(err)
	}
	return &rec
}

// TestTopicMessageOversizedFields checks that DecodeMessage bounds the
// variable-length fields of the topic messages (Buckets, Nodes, Ticket) rather
// than relying solely on the packet MTU and downstream handlers.
func TestTopicMessageOversizedFields(t *testing.T) {
	rec := signedTestRecord(t)
	var topic [32]byte
	manyRecords := func(n int) []*enr.Record {
		rs := make([]*enr.Record, n)
		for i := range rs {
			rs[i] = rec
		}
		return rs
	}
	cases := []struct {
		name string
		kind byte
		body []byte
	}{
		{"regtopic-many-buckets", RegtopicMsg, rlpMust(&Regtopic{ReqID: []byte{1}, Topic: topic, Ticket: []byte{1}, ENR: rec, Buckets: make([]uint, 100000)})},
		{"topicquery-many-buckets", TopicQueryMsg, rlpMust(&TopicQuery{ReqID: []byte{1}, Topic: topic, Buckets: make([]uint, 100000)})},
		{"topicnodes-many-nodes", TopicNodesMsg, rlpMust(&TopicNodes{ReqID: []byte{1}, RespCount: 1, Nodes: manyRecords(5000)})},
		{"regtopic-huge-ticket", RegtopicMsg, rlpMust(&Regtopic{ReqID: []byte{1}, Topic: topic, Ticket: make([]byte, 1<<20), ENR: rec, Buckets: []uint{}})},
		{"regconfirmation-huge-ticket", RegconfirmationMsg, rlpMust(&Regconfirmation{ReqID: []byte{1}, RespCount: 1, Ticket: make([]byte, 1<<20)})},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := DecodeMessage(c.kind, c.body); err == nil {
				t.Fatal("decoded oversized message without error")
			}
		})
	}
}

// TestTopicMessageFieldsAtCap checks that a variable-length field sized exactly
// at its decode limit is accepted — the complement of TestTopicMessageOversizedFields,
// which only exercises the reject-when-over side.
func TestTopicMessageFieldsAtCap(t *testing.T) {
	rec := signedTestRecord(t)
	var topic [32]byte
	records := func(n int) []*enr.Record {
		rs := make([]*enr.Record, n)
		for i := range rs {
			rs[i] = rec
		}
		return rs
	}
	cases := []struct {
		name string
		kind byte
		body []byte
	}{
		{"regtopic-buckets-at-cap", RegtopicMsg, rlpMust(&Regtopic{ReqID: []byte{1}, Topic: topic, Ticket: []byte{1}, ENR: rec, Buckets: make([]uint, maxTopicDistances)})},
		{"regtopic-ticket-at-cap", RegtopicMsg, rlpMust(&Regtopic{ReqID: []byte{1}, Topic: topic, Ticket: make([]byte, maxTicketSizeBytes), ENR: rec, Buckets: []uint{}})},
		{"regconfirmation-ticket-at-cap", RegconfirmationMsg, rlpMust(&Regconfirmation{ReqID: []byte{1}, RespCount: 1, Ticket: make([]byte, maxTicketSizeBytes)})},
		{"topicquery-buckets-at-cap", TopicQueryMsg, rlpMust(&TopicQuery{ReqID: []byte{1}, Topic: topic, Buckets: make([]uint, maxTopicDistances)})},
		{"topicnodes-nodes-at-cap", TopicNodesMsg, rlpMust(&TopicNodes{ReqID: []byte{1}, RespCount: 1, Nodes: records(maxTopicNodeCount)})},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := DecodeMessage(c.kind, c.body); err != nil {
				t.Fatalf("rejected message with field at cap: %v", err)
			}
		})
	}
}

// FuzzDecodeMessage checks that DecodeMessage never panics on arbitrary input,
// and that any message it accepts re-encodes without error. Seeded from the
// valid round-trip corpus so the fuzzer starts from well-formed messages.
func FuzzDecodeMessage(f *testing.F) {
	rec := signedTestRecord(f)
	var topic [32]byte
	seeds := []Packet{
		&Regtopic{ReqID: []byte{1, 2, 3}, Topic: topic, Ticket: []byte{9, 9}, ENR: rec, Buckets: []uint{250, 251}},
		&Regconfirmation{ReqID: []byte{4}, RespCount: 2, Ticket: []byte{7, 7}, WaitTime: 1500},
		&TopicQuery{ReqID: []byte{5}, Topic: topic, Buckets: []uint{248}},
		&TopicNodes{ReqID: []byte{6}, RespCount: 3, Nodes: []*enr.Record{rec}},
	}
	for _, m := range seeds {
		f.Add(byte(m.Kind()), rlpMust(m))
	}
	f.Fuzz(func(t *testing.T, kind byte, body []byte) {
		msg, err := DecodeMessage(kind, body)
		if err != nil {
			return
		}
		if _, err := rlp.EncodeToBytes(msg); err != nil {
			t.Fatalf("re-encode of decoded %T failed: %v", msg, err)
		}
	})
}
