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
	"crypto/sha256"
	"encoding/hex"
	"net"
	"reflect"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/rlp"
)

// TestVectorsTopicMessages pins the TopDisc message encodings of the discv5
// wire test vectors in the devp2p specs: each message encodes to the expected
// plaintext, and the plaintext decodes back to the message.
func TestVectorsTopicMessages(t *testing.T) {
	var rec enr.Record
	rec.SetSeq(1)
	rec.Set(enr.IP(net.IP{127, 0, 0, 1}))
	rec.Set(enr.UDP(30303))
	rec.Set(topicindex.TopicDiscoveryVersion)
	if err := enode.SignV4(&rec, testKeyA); err != nil {
		t.Fatal(err)
	}
	wantENR := vectorBytes(`
		f888b840f36f7d2609019afe961991302e5269e47d9401f33faf6e56ba1b81f9
		a827de5778c20038bcf71e98ed832aec61ff7ed9454b1893ad41ff20d3bf9d63
		ca65c04901826964827634826970847f00000189736563703235366b31a10313
		d14211e0287b2361a1615890a9b5212080546d0a257ae4cff96cf534992cb982
		7464018375647082765f`)
	if enc, _ := rlp.EncodeToBytes(&rec); !bytes.Equal(enc, wantENR) {
		t.Fatalf("wrong ENR encoding:\n got %x\nwant %x", enc, wantENR)
	}

	topic := sha256.Sum256([]byte("example-service"))
	ticket := vectorBytes("0102030405060708090a0b0c0d0e0f10")

	tests := []struct {
		name string
		msg  Packet
		want string
	}{
		{
			name: "REGTOPIC first request",
			msg:  &Regtopic{ReqID: []byte{0, 0, 0, 1}, Topic: topic, Ticket: []byte{}, ENR: &rec, Buckets: []uint{256, 255, 254}},
			want: `
				07f8b98400000001a0acac0a3cfc659b9392c42a643065965e71cae5229afead
				e460788cca53315f8980f888b840f36f7d2609019afe961991302e5269e47d94
				01f33faf6e56ba1b81f9a827de5778c20038bcf71e98ed832aec61ff7ed9454b
				1893ad41ff20d3bf9d63ca65c04901826964827634826970847f000001897365
				63703235366b31a10313d14211e0287b2361a1615890a9b5212080546d0a257a
				e4cff96cf534992cb9827464018375647082765fc782010081ff81fe`,
		},
		{
			name: "REGTOPIC retry with ticket",
			msg:  &Regtopic{ReqID: []byte{0, 0, 0, 2}, Topic: topic, Ticket: ticket, ENR: &rec, Buckets: []uint{256, 255, 254}},
			want: `
				07f8c98400000002a0acac0a3cfc659b9392c42a643065965e71cae5229afead
				e460788cca53315f89900102030405060708090a0b0c0d0e0f10f888b840f36f
				7d2609019afe961991302e5269e47d9401f33faf6e56ba1b81f9a827de5778c2
				0038bcf71e98ed832aec61ff7ed9454b1893ad41ff20d3bf9d63ca65c0490182
				6964827634826970847f00000189736563703235366b31a10313d14211e0287b
				2361a1615890a9b5212080546d0a257ae4cff96cf534992cb982746401837564
				7082765fc782010081ff81fe`,
		},
		{
			name: "REGCONFIRMATION admitted",
			msg:  &Regconfirmation{ReqID: []byte{0, 0, 0, 1}, RespCount: 1, Ticket: []byte{}, WaitTime: 900000},
			want: "08cb84000000010180830dbba0",
		},
		{
			name: "REGCONFIRMATION ticket issued",
			msg:  &Regconfirmation{ReqID: []byte{0, 0, 0, 1}, RespCount: 2, Ticket: ticket, WaitTime: 12345},
			want: "08da840000000102900102030405060708090a0b0c0d0e0f10823039",
		},
		{
			name: "REGCONFIRMATION rejected",
			msg:  &Regconfirmation{ReqID: []byte{0, 0, 0, 1}, RespCount: 1, Ticket: []byte{}, WaitTime: 0},
			want: "08c88400000001018080",
		},
		{
			name: "TOPICQUERY",
			msg:  &TopicQuery{ReqID: []byte{0, 0, 0, 3}, Topic: topic, Buckets: []uint{256, 255}},
			want: `
				09ec8400000003a0acac0a3cfc659b9392c42a643065965e71cae5229afeade4
				60788cca53315f89c582010081ff`,
		},
		{
			name: "TOPICNODES one advertisement",
			msg:  &TopicNodes{ReqID: []byte{0, 0, 0, 3}, RespCount: 1, Nodes: []*enr.Record{&rec}},
			want: `
				0af892840000000301f88af888b840f36f7d2609019afe961991302e5269e47d
				9401f33faf6e56ba1b81f9a827de5778c20038bcf71e98ed832aec61ff7ed945
				4b1893ad41ff20d3bf9d63ca65c04901826964827634826970847f0000018973
				6563703235366b31a10313d14211e0287b2361a1615890a9b5212080546d0a25
				7ae4cff96cf534992cb9827464018375647082765f`,
		},
		{
			name: "TOPICNODES empty",
			msg:  &TopicNodes{ReqID: []byte{0, 0, 0, 3}, RespCount: 1, Nodes: []*enr.Record{}},
			want: "0ac7840000000301c0",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := vectorBytes(test.want)
			body, err := rlp.EncodeToBytes(test.msg)
			if err != nil {
				t.Fatal(err)
			}
			if enc := append([]byte{test.msg.Kind()}, body...); !bytes.Equal(enc, want) {
				t.Errorf("wrong encoding:\n got %x\nwant %x", enc, want)
			}
			dec, err := DecodeMessage(want[0], want[1:])
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(dec, test.msg) {
				t.Errorf("wrong decoded message:\n got %+v\nwant %+v", dec, test.msg)
			}
		})
	}
}

func vectorBytes(s string) []byte {
	b, err := hex.DecodeString(strings.Join(strings.Fields(s), ""))
	if err != nil {
		panic(err)
	}
	return b
}
