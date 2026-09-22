package main

import (
	"fmt"
	"math/big"
	"reflect"
	"sort"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/forkid"
	"github.com/ethereum/go-ethereum/params"
)

// Prints the fork hash at every fork boundary of the networks go-ethereum
// knows, to name the fork ids seen in a discv5 crawl.
func main() {
	nets := []struct {
		name    string
		config  *params.ChainConfig
		genesis *core.Genesis
	}{
		{"mainnet", params.MainnetChainConfig, core.DefaultGenesisBlock()},
		{"sepolia", params.SepoliaChainConfig, core.DefaultSepoliaGenesisBlock()},
		{"holesky", params.HoleskyChainConfig, core.DefaultHoleskyGenesisBlock()},
		{"hoodi", params.HoodiChainConfig, core.DefaultHoodiGenesisBlock()},
	}
	for _, n := range nets {
		gblock := n.genesis.ToBlock()
		var blocks []uint64
		var times []uint64
		v := reflect.ValueOf(*n.config)
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			switch t.Field(i).Type {
			case reflect.TypeOf((*big.Int)(nil)):
				if !f.IsNil() && f.Interface().(*big.Int).IsUint64() && t.Field(i).Name != "ChainID" {
					blocks = append(blocks, f.Interface().(*big.Int).Uint64())
				}
			case reflect.TypeOf((*uint64)(nil)):
				if !f.IsNil() {
					times = append(times, *f.Interface().(*uint64))
				}
			}
		}
		sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		seen := map[string]bool{}
		fmt.Printf("== %s\n", n.name)
		try := func(label string, block, time uint64) {
			id := forkid.NewID(n.config, gblock, block, time)
			h := fmt.Sprintf("%x", id.Hash)
			if !seen[h] {
				seen[h] = true
				fmt.Printf("  %-18s %s\n", label, h)
			}
		}
		try("genesis", 0, n.genesis.Timestamp)
		for _, b := range blocks {
			try(fmt.Sprintf("block %d", b), b, n.genesis.Timestamp)
		}
		last := uint64(1 << 62)
		for _, tm := range times {
			try(fmt.Sprintf("time %d", tm), last, tm)
		}
	}
}
