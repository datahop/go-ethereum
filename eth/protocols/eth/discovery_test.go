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
	"crypto/sha256"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
)

func TestDiscoveryTopic(t *testing.T) {
	want := common.HexToHash("0xad42f8e7258646d1800f22a878197efcbce43e797136fa817f410b4fd983ae42")
	got := DiscoveryTopic(params.MainnetChainConfig.ChainID, params.MainnetGenesisHash)
	if common.Hash(got) != want {
		t.Fatalf("got %x, want %x", got, want)
	}
	if byName := sha256.Sum256([]byte("eth/1/0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3")); got != byName {
		t.Fatalf("got %x, want SHA-256 of the topic name %x", got, byName)
	}
}
