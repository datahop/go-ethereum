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
	"testing"

	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/eth/protocols/eth"
	"github.com/ethereum/go-ethereum/params"
)

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
