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

package eth

import (
	"math/big"
	"slices"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/forkid"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/eth/protocols/eth"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
)

type testDiscv5 struct {
	random, topic        []*enode.Node
	registered, searched []topicindex.TopicID
}

func (d *testDiscv5) RandomNodes() enode.Iterator { return enode.IterNodes(d.random) }

func (d *testDiscv5) RegisterTopic(topic topicindex.TopicID, opid uint64) {
	d.registered = append(d.registered, topic)
}

func (d *testDiscv5) TopicSearch(topic topicindex.TopicID, opid uint64) enode.Iterator {
	d.searched = append(d.searched, topic)
	return enode.IterNodes(d.topic)
}

type testEthEntry struct {
	ForkID forkid.ID
	Rest   []rlp.RawValue `rlp:"tail"`
}

func (testEthEntry) ENRKey() string { return "eth" }

func testEthNode(id byte, fid forkid.ID) *enode.Node {
	var r enr.Record
	r.Set(testEthEntry{ForkID: fid})
	return enode.SignNull(&r, enode.ID{id})
}

func TestDiscv5Sources(t *testing.T) {
	gspec := &core.Genesis{Config: params.TestChainConfig}
	chain, err := core.NewBlockChain(rawdb.NewMemoryDatabase(), gspec, ethash.NewFaker(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer chain.Stop()
	genesis := chain.Genesis()
	fid := forkid.NewID(chain.Config(), genesis, 0, genesis.Time())

	var (
		wrongFork = forkid.ID{Hash: [4]byte{1, 2, 3, 4}}
		randomBad = testEthNode(4, wrongFork)
		random    = testEthNode(1, fid)
		topicBad  = testEthNode(2, wrongFork)
		topicOK   = testEthNode(3, fid)
		topic     = eth.DiscoveryTopic(chain.Config().ChainID, genesis.Hash())
	)
	for _, enabled := range []bool{false, true} {
		disc := &testDiscv5{random: []*enode.Node{randomBad, random}, topic: []*enode.Node{topicBad, topicOK}}
		mix := enode.NewFairMix(discmixTimeout)
		addDiscv5Sources(mix, disc, enabled, topic, eth.NewNodeFilter(chain))

		want := map[enode.ID]string{random.ID(): ""}
		var wantTopics []topicindex.TopicID
		if enabled {
			want[topicOK.ID()] = "discv5-topic"
			wantTopics = []topicindex.TopicID{topic}
		}
		if !slices.Equal(disc.registered, wantTopics) {
			t.Errorf("topic=%v: registered %v, want %v", enabled, disc.registered, wantTopics)
		}
		if !slices.Equal(disc.searched, wantTopics) {
			t.Errorf("topic=%v: searched %v, want %v", enabled, disc.searched, wantTopics)
		}

		// The wrong-fork nodes precede the good ones in their sources, so they
		// have been filtered once the good ones arrive.
		got := make(map[enode.ID]string)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for len(got) < len(want) && mix.Next() {
				got[mix.Node().ID()] = mix.NodeSource()
			}
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("topic=%v: timeout", enabled)
		}
		mix.Close()
		<-done
		if len(got) != len(want) {
			t.Errorf("topic=%v: got %d nodes, want %d", enabled, len(got), len(want))
		}
		for id, source := range got {
			if wsource, ok := want[id]; !ok || wsource != source {
				t.Errorf("topic=%v: unexpected node %v from source %q", enabled, id, source)
			}
		}
	}
}

func TestDiscv5Topic(t *testing.T) {
	var (
		genesis = params.MainnetGenesisHash
		chainID = params.MainnetChainConfig.ChainID
		want    = eth.DiscoveryTopic(chainID, genesis)
	)
	tests := []struct {
		name    string
		enabled bool
		chainID *big.Int
		ok      bool
	}{
		{name: "disabled", chainID: chainID},
		{name: "enabled", enabled: true, chainID: chainID, ok: true},
		{name: "enabled, no chain id", enabled: true},
	}
	for _, test := range tests {
		config := &ethconfig.Config{DiscoveryTopic: test.enabled}
		topic, ok := discv5Topic(config, &params.ChainConfig{ChainID: test.chainID}, genesis)
		if ok != test.ok {
			t.Errorf("%s: ok %v, want %v", test.name, ok, test.ok)
		}
		if ok && topic != want {
			t.Errorf("%s: topic %v, want %v", test.name, topic, want)
		}
	}
}
