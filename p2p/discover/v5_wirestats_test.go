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
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
)

func TestWireStatsTopicOps(t *testing.T) {
	defer func(on bool) { wireStatsOn = on }(wireStatsOn)
	EnableWireStats()

	registrar := startLocalhostV5(t, Config{})
	advertiser := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{registrar.Self()}})
	searcher := startLocalhostV5(t, Config{Bootnodes: []*enode.Node{registrar.Self()}})
	defer func() {
		for _, n := range []*UDPv5{registrar, advertiser, searcher} {
			n.Close()
		}
	}()
	seedTopicTable(t, registrar, testTopic1, advertiser.Self())

	it := searcher.TopicSearch(testTopic1, 7)
	defer it.Close()
	timeout := time.AfterFunc(30*time.Second, it.Close)
	defer timeout.Stop()
	if !it.Next() {
		t.Fatal("search ended without results")
	}
	op := searcher.OpStats()[OpKey{"TOPICQUERY/v5", 7}]
	if op.TxMsgs == 0 || op.RxMsgs == 0 || op.Nodes == 0 {
		t.Fatalf("search not counted: %+v", op)
	}
	if n := len(searcher.OpStats()); n != 1 {
		t.Fatalf("searcher has %d operations, want 1", n)
	}
	load := registrar.TopicLoadStats()[testTopic1].TopicQuery
	if load.RxMsgs == 0 || load.TxMsgs == 0 {
		t.Fatalf("registrar TOPICQUERY load not counted: %+v", load)
	}

	advertiser.RegisterTopic(testTopic1, 9)
	deadline := time.Now().Add(20 * time.Second)
	for {
		op := advertiser.OpStats()[OpKey{"REGTOPIC/v5", 9}]
		load := registrar.TopicLoadStats()[testTopic1].Regtopic
		if op.TxMsgs > 0 && op.RxMsgs > 0 && op.Nodes > 0 && load.RxMsgs > 0 && load.TxMsgs > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("registration not counted: advertiser %+v, registrar %+v", op, load)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestWireStatsDisabled(t *testing.T) {
	defer func(on bool) { wireStatsOn = on }(wireStatsOn)
	wireStatsOn = false

	node := startLocalhostV5(t, Config{})
	defer node.Close()
	if node.OpStats() != nil || node.TopicLoadStats() != nil {
		t.Fatal("stats returned while counting is disabled")
	}
}
