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
	var rec enr.Record
	rec.Set(enr.IPv4{127, 0, 0, 1})
	rec.Set(enr.UDP(30303))
	if err := enode.SignV4(&rec, testKeyA); err != nil {
		t.Fatal(err)
	}
	topic := [32]byte{1, 2, 3, 4, 5}

	msgs := []Packet{
		&Regtopic{ReqID: []byte{1, 2, 3}, Topic: topic, Ticket: []byte{9, 9}, ENR: &rec, Buckets: []uint{250, 251}},
		&Regconfirmation{ReqID: []byte{4}, RespCount: 2, Ticket: []byte{7, 7}, WaitTime: 1500},
		&TopicQuery{ReqID: []byte{5}, Topic: topic, Buckets: []uint{248}},
		&TopicNodes{ReqID: []byte{6}, RespCount: 3, Nodes: []*enr.Record{&rec}},
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

// TestTopicMessageDecodeErrors checks that DecodeMessage rejects malformed topic
// messages — unknown type, empty/non-list bodies, missing fields, extra trailing
// data, and oversized request ids — with an error and never a panic.
func TestTopicMessageDecodeErrors(t *testing.T) {
	var rec enr.Record
	rec.Set(enr.IPv4{127, 0, 0, 1})
	rec.Set(enr.UDP(30303))
	if err := enode.SignV4(&rec, testKeyA); err != nil {
		t.Fatal(err)
	}
	var topic [32]byte

	enc := func(v interface{}) []byte {
		b, err := rlp.EncodeToBytes(v)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		return b
	}
	tooFew := [][]byte{{0x01}} // one-element list: fewer fields than any topic message
	trailing := func(b []byte) []byte { return append(append([]byte{}, b...), 0x00) }

	cases := []struct {
		name string
		kind byte
		body []byte
		want error // if non-nil, the exact error expected
	}{
		{"unknown-type", 0xEE, enc(tooFew), nil},

		// empty / not-a-list bodies
		{"regtopic-empty", RegtopicMsg, nil, nil},
		{"regtopic-not-list", RegtopicMsg, []byte{0x01}, nil},
		{"topicquery-not-list", TopicQueryMsg, []byte{0x01}, nil},

		// missing fields (too few list elements)
		{"regtopic-too-few", RegtopicMsg, enc(tooFew), nil},
		{"regconfirmation-too-few", RegconfirmationMsg, enc(tooFew), nil},
		{"topicquery-too-few", TopicQueryMsg, enc(tooFew), nil},
		{"topicnodes-too-few", TopicNodesMsg, enc(tooFew), nil},

		// extra trailing data after a valid message
		{"topicquery-trailing", TopicQueryMsg, trailing(enc(&TopicQuery{ReqID: []byte{1}, Topic: topic, Buckets: []uint{}})), nil},
		{"topicnodes-trailing", TopicNodesMsg, trailing(enc(&TopicNodes{ReqID: []byte{1}, RespCount: 1, Nodes: []*enr.Record{&rec}})), nil},

		// oversized request id (> 8 bytes)
		{"regtopic-bigreqid", RegtopicMsg, enc(&Regtopic{ReqID: make([]byte, 9), Topic: topic, ENR: &rec, Buckets: []uint{}}), ErrInvalidReqID},
		{"regconfirmation-bigreqid", RegconfirmationMsg, enc(&Regconfirmation{ReqID: make([]byte, 9), RespCount: 1}), ErrInvalidReqID},
		{"topicquery-bigreqid", TopicQueryMsg, enc(&TopicQuery{ReqID: make([]byte, 9), Topic: topic, Buckets: []uint{}}), ErrInvalidReqID},
		{"topicnodes-bigreqid", TopicNodesMsg, enc(&TopicNodes{ReqID: make([]byte, 9), RespCount: 1}), ErrInvalidReqID},
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
