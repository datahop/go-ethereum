// Copyright 2019 The go-ethereum Authors
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
	"fmt"
	"net"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/rlp"
)

// Packet is implemented by all message types.
type Packet interface {
	Name() string        // Name returns a string corresponding to the message type.
	Kind() byte          // Kind returns the message type.
	RequestID() []byte   // Returns the request ID.
	SetRequestID([]byte) // Sets the request ID.

	// AppendLogInfo returns its argument 'ctx' with additional fields
	// appended for logging purposes.
	AppendLogInfo(ctx []interface{}) []interface{}
}

// Message types.
const (
	PingMsg byte = iota + 1
	PongMsg
	FindnodeMsg
	NodesMsg
	TalkRequestMsg
	TalkResponseMsg
	RequestTicketMsg
	TicketMsg
	RegtopicMsg
	RegconfirmationMsg
	TopicQueryMsg
	TopicNodesMsg

	UnknownPacket   = byte(255) // any non-decryptable packet
	WhoareyouPacket = byte(254) // the WHOAREYOU packet
)

// Protocol messages.
type (
	// Unknown represents any packet that can't be decrypted.
	Unknown struct {
		Nonce Nonce
	}

	// WHOAREYOU contains the handshake challenge.
	Whoareyou struct {
		ChallengeData []byte   // Encoded challenge
		Nonce         Nonce    // Nonce of request packet
		IDNonce       [16]byte // Identity proof data
		RecordSeq     uint64   // ENR sequence number of recipient

		// Node is the locally known node record of recipient.
		// This must be set by the caller of Encode.
		Node *enode.Node

		sent mclock.AbsTime // for handshake GC.
	}

	// PING is sent during liveness checks.
	Ping struct {
		ReqID  []byte
		ENRSeq uint64
	}

	// PONG is the reply to PING.
	Pong struct {
		ReqID  []byte
		ENRSeq uint64
		ToIP   net.IP // These fields should mirror the UDP envelope address of the ping
		ToPort uint16 // packet, which provides a way to discover the external address (after NAT).
	}

	// FINDNODE is a query for nodes in the given bucket.
	Findnode struct {
		ReqID     []byte
		Distances []uint

		// OpID is for debugging purposes and is not part of the packet encoding.
		// It identifies the 'operation' on behalf of which the request was sent.
		OpID uint64 `rlp:"-"`
	}

	// NODES is a response to FINDNODE, REGTOPIC or TOPICQUERY.
	Nodes struct {
		ReqID     []byte
		RespCount uint8 // total number of responses to the request
		Nodes     []*enr.Record
	}

	// TALKREQ is an application-level request.
	TalkRequest struct {
		ReqID    []byte
		Protocol string
		Message  []byte
	}

	// TALKRESP is the reply to TALKREQ.
	TalkResponse struct {
		ReqID   []byte
		Message []byte
	}

	// REGTOPIC registers the sender in a topic queue using a ticket.
	Regtopic struct {
		ReqID  []byte
		Topic  [32]byte
		Ticket []byte
		ENR    *enr.Record

		// Buckets contains the distances from the topic where the
		// requesting node still has space in its search table.
		Buckets []uint

		// OpID is for debugging purposes and is not part of the packet encoding.
		// It identifies the 'operation' on behalf of which the request was sent.
		OpID uint64 `rlp:"-"`
	}

	// REGCONFIRMATION is one of the responses to REGTOPIC.
	Regconfirmation struct {
		ReqID     []byte
		RespCount uint8  // total number of responses to the request
		Ticket    []byte // registered successfully if length zero
		WaitTime  uint   // how to wait until sending next REGTOPIC (in ms)
		// Note: when len(Ticket) == 0, registration is successful and
		// WaitTime is the registration lifetime.

		CumulativeWaitTime *uint `rlp:"-"` // for logs
	}

	// TOPICQUERY asks for nodes with the given topic.
	TopicQuery struct {
		ReqID []byte
		Topic [32]byte

		// Buckets contains the distances from the topic where the
		// requesting node still has space in its search table.
		Buckets []uint

		// OpID is for debugging purposes and is not part of the packet encoding.
		// It identifies the 'operation' on behalf of which the request was sent.
		OpID uint64 `rlp:"-"`
	}

	// TOPICNODES is one of the responses to TOPICQUERY.
	TopicNodes struct {
		ReqID     []byte
		RespCount uint8         // total number of responses to the request
		Nodes     []*enr.Record // nodes matching the requested topic
	}
)

// DecodeMessage decodes the message body of a packet.
func DecodeMessage(ptype byte, body []byte) (Packet, error) {
	var dec Packet
	switch ptype {
	case PingMsg:
		dec = new(Ping)
	case PongMsg:
		dec = new(Pong)
	case FindnodeMsg:
		dec = new(Findnode)
	case NodesMsg:
		dec = new(Nodes)
	case TalkRequestMsg:
		dec = new(TalkRequest)
	case TalkResponseMsg:
		dec = new(TalkResponse)
	case RegtopicMsg:
		dec = new(Regtopic)
	case RegconfirmationMsg:
		dec = new(Regconfirmation)
	case TopicQueryMsg:
		dec = new(TopicQuery)
	case TopicNodesMsg:
		dec = new(TopicNodes)
	default:
		return nil, fmt.Errorf("unknown packet type %d", ptype)
	}
	if err := rlp.DecodeBytes(body, dec); err != nil {
		return nil, err
	}
	if dec.RequestID() != nil && len(dec.RequestID()) > 8 {
		return nil, ErrInvalidReqID
	}
	return dec, nil
}

func (*Whoareyou) Name() string        { return "WHOAREYOU/v5" }
func (*Whoareyou) Kind() byte          { return WhoareyouPacket }
func (*Whoareyou) RequestID() []byte   { return nil }
func (*Whoareyou) SetRequestID([]byte) {}

func (*Whoareyou) AppendLogInfo(ctx []interface{}) []interface{} {
	return ctx
}

func (*Unknown) Name() string        { return "UNKNOWN/v5" }
func (*Unknown) Kind() byte          { return UnknownPacket }
func (*Unknown) RequestID() []byte   { return nil }
func (*Unknown) SetRequestID([]byte) {}

func (*Unknown) AppendLogInfo(ctx []interface{}) []interface{} {
	return ctx
}

func (*Ping) Name() string             { return "PING/v5" }
func (*Ping) Kind() byte               { return PingMsg }
func (p *Ping) RequestID() []byte      { return p.ReqID }
func (p *Ping) SetRequestID(id []byte) { p.ReqID = id }

func (p *Ping) AppendLogInfo(ctx []interface{}) []interface{} {
	return append(ctx, "req", hexutil.Bytes(p.ReqID), "enrseq", p.ENRSeq)
}

func (*Pong) Name() string             { return "PONG/v5" }
func (*Pong) Kind() byte               { return PongMsg }
func (p *Pong) RequestID() []byte      { return p.ReqID }
func (p *Pong) SetRequestID(id []byte) { p.ReqID = id }

func (p *Pong) AppendLogInfo(ctx []interface{}) []interface{} {
	return append(ctx, "req", hexutil.Bytes(p.ReqID), "enrseq", p.ENRSeq)
}

func (p *Findnode) Name() string           { return "FINDNODE/v5" }
func (p *Findnode) Kind() byte             { return FindnodeMsg }
func (p *Findnode) RequestID() []byte      { return p.ReqID }
func (p *Findnode) SetRequestID(id []byte) { p.ReqID = id }

func (p *Findnode) AppendLogInfo(ctx []interface{}) []interface{} {
	ctx = append(ctx, "req", hexutil.Bytes(p.ReqID))
	if p.OpID != 0 {
		ctx = append(ctx, "opid", p.OpID)
	}
	return ctx
}

func (*Nodes) Name() string             { return "NODES/v5" }
func (*Nodes) Kind() byte               { return NodesMsg }
func (p *Nodes) RequestID() []byte      { return p.ReqID }
func (p *Nodes) SetRequestID(id []byte) { p.ReqID = id }

func (p *Nodes) AppendLogInfo(ctx []interface{}) []interface{} {
	return append(ctx,
		"req", hexutil.Bytes(p.ReqID),
		"tot", p.RespCount,
		"n", len(p.Nodes),
	)
}

func (*TalkRequest) Name() string             { return "TALKREQ/v5" }
func (*TalkRequest) Kind() byte               { return TalkRequestMsg }
func (p *TalkRequest) RequestID() []byte      { return p.ReqID }
func (p *TalkRequest) SetRequestID(id []byte) { p.ReqID = id }

func (p *TalkRequest) AppendLogInfo(ctx []interface{}) []interface{} {
	return append(ctx, "proto", p.Protocol, "reqid", hexutil.Bytes(p.ReqID), "len", len(p.Message))
}

func (*TalkResponse) Name() string             { return "TALKRESP/v5" }
func (*TalkResponse) Kind() byte               { return TalkResponseMsg }
func (p *TalkResponse) RequestID() []byte      { return p.ReqID }
func (p *TalkResponse) SetRequestID(id []byte) { p.ReqID = id }

func (p *TalkResponse) AppendLogInfo(ctx []interface{}) []interface{} {
	return append(ctx, "req", p.ReqID, "len", len(p.Message))
}

func (*Regtopic) Name() string             { return "REGTOPIC/v5" }
func (*Regtopic) Kind() byte               { return RegtopicMsg }
func (p *Regtopic) RequestID() []byte      { return p.ReqID }
func (p *Regtopic) SetRequestID(id []byte) { p.ReqID = id }

func (p *Regtopic) AppendLogInfo(ctx []interface{}) []interface{} {
	ctx = append(ctx, "req", hexutil.Bytes(p.ReqID), "topic", (*enode.ID)(&p.Topic))
	if p.OpID != 0 {
		ctx = append(ctx, "opid", p.OpID)
	}
	return ctx
}

func (*Regconfirmation) Name() string             { return "REGCONFIRMATION/v5" }
func (*Regconfirmation) Kind() byte               { return RegconfirmationMsg }
func (p *Regconfirmation) RequestID() []byte      { return p.ReqID }
func (p *Regconfirmation) SetRequestID(id []byte) { p.ReqID = id }

func (p *Regconfirmation) AppendLogInfo(ctx []interface{}) []interface{} {
	ok := len(p.Ticket) == 0
	ctx = append(ctx, "req", hexutil.Bytes(p.ReqID), "ok", ok)
	if ok {
		ctx = append(ctx, "adlifetime", p.WaitTime)
	} else {
		ctx = append(ctx, "wtime", p.WaitTime)
	}
	if p.CumulativeWaitTime != nil {
		ctx = append(ctx, "total-wtime", *p.CumulativeWaitTime)
	}
	return ctx
}

func (*TopicQuery) Name() string             { return "TOPICQUERY/v5" }
func (*TopicQuery) Kind() byte               { return TopicQueryMsg }
func (p *TopicQuery) RequestID() []byte      { return p.ReqID }
func (p *TopicQuery) SetRequestID(id []byte) { p.ReqID = id }

func (p *TopicQuery) AppendLogInfo(ctx []interface{}) []interface{} {
	ctx = append(ctx, "req", hexutil.Bytes(p.ReqID), "topic", (*enode.ID)(&p.Topic))
	if p.OpID != 0 {
		ctx = append(ctx, "opid", p.OpID)
	}
	return ctx
}

func (*TopicNodes) Name() string             { return "TOPICNODES/v5" }
func (*TopicNodes) Kind() byte               { return TopicNodesMsg }
func (p *TopicNodes) RequestID() []byte      { return p.ReqID }
func (p *TopicNodes) SetRequestID(id []byte) { p.ReqID = id }

func (p *TopicNodes) AppendLogInfo(ctx []interface{}) []interface{} {
	return append(ctx,
		"req", hexutil.Bytes(p.ReqID),
		"tot", p.RespCount,
		"n", len(p.Nodes),
	)
}
