// Copyright 2026 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"crypto/sha256"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/eth/protocols/eth"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/urfave/cli/v2"
)

const (
	exampleTopicName = "example-service"
	exampleTopicHash = "0xacac0a3cfc659b9392c42a643065965e71cae5229afeade460788cca53315f89"
)

func TestParseTopic(t *testing.T) {
	hashOf := func(s string) topicindex.TopicID { return sha256.Sum256([]byte(s)) }
	literal := func(s string) topicindex.TopicID { return topicindex.TopicID(common.HexToHash(s)) }
	other := "0x" + strings.Repeat("01", 32)
	tests := []struct {
		in   string
		want topicindex.TopicID
	}{
		{in: exampleTopicName, want: literal(exampleTopicHash)},
		{in: exampleTopicHash, want: literal(exampleTopicHash)},
		{in: strings.ToUpper(exampleTopicHash[2:]), want: hashOf(strings.ToUpper(exampleTopicHash[2:]))},
		{in: "0x" + strings.ToUpper(exampleTopicHash[2:]), want: literal(exampleTopicHash)},
		{in: other, want: literal(other)},
		{in: exampleTopicHash[:65], want: hashOf(exampleTopicHash[:65])},
		{in: exampleTopicHash + "0", want: hashOf(exampleTopicHash + "0")},
		{in: "0x" + strings.Repeat("g", 64), want: hashOf("0x" + strings.Repeat("g", 64))},
	}
	for _, test := range tests {
		got, err := parseTopic(test.in)
		if err != nil {
			t.Fatalf("%q: %v", test.in, err)
		}
		if got != test.want {
			t.Errorf("%q: got %x, want %x", test.in, got, test.want)
		}
	}
	if _, err := parseTopic(""); err == nil {
		t.Error("empty topic accepted")
	}
}

func TestParseEthTopic(t *testing.T) {
	mainnetRaw := "1/" + params.MainnetGenesisHash.Hex()
	mainnet := eth.DiscoveryTopic(params.MainnetChainConfig.ChainID, params.MainnetGenesisHash)
	byName, err := parseTopic("eth/1/0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3")
	if err != nil || byName != mainnet {
		t.Fatalf("topic name parse: got %x (%v), want %x", byName, err, mainnet)
	}
	tests := []struct {
		in   string
		want topicindex.TopicID
	}{
		{in: "mainnet", want: mainnet},
		{in: mainnetRaw, want: mainnet},
		{in: "1/0x" + strings.ToUpper(params.MainnetGenesisHash.Hex()[2:]), want: mainnet},
		{in: "sepolia", want: eth.DiscoveryTopic(params.SepoliaChainConfig.ChainID, params.SepoliaGenesisHash)},
		{in: "hoodi", want: eth.DiscoveryTopic(params.HoodiChainConfig.ChainID, params.HoodiGenesisHash)},
		{in: "560048/" + params.HoodiGenesisHash.Hex(), want: eth.DiscoveryTopic(params.HoodiChainConfig.ChainID, params.HoodiGenesisHash)},
	}
	for _, test := range tests {
		var got topicindex.TopicID
		cmd := *discv5ListenCommand
		cmd.Action = func(ctx *cli.Context) (err error) {
			got, err = parseEthTopic(ctx.String(ethTopicFlag.Name))
			return err
		}
		app := &cli.App{Commands: []*cli.Command{&cmd}}
		if err := app.Run([]string{"devp2p", cmd.Name, "--eth-topic", test.in}); err != nil {
			t.Fatalf("%q: %v", test.in, err)
		}
		if got != test.want {
			t.Errorf("%q: got %x, want %x", test.in, got, test.want)
		}
	}
	invalid := []string{
		"", "Mainnet", "holesky",
		params.MainnetGenesisHash.Hex(),
		"1/" + params.MainnetGenesisHash.Hex()[2:],
		mainnetRaw + "0",
		"1/0x" + strings.Repeat("g", 64),
		"/" + params.MainnetGenesisHash.Hex(),
		"01/" + params.MainnetGenesisHash.Hex(),
		"+1/" + params.MainnetGenesisHash.Hex(),
		"-1/" + params.MainnetGenesisHash.Hex(),
		"0x1/" + params.MainnetGenesisHash.Hex(),
		"eth/" + mainnetRaw,
		mainnetRaw + "/",
	}
	for _, in := range invalid {
		if _, err := parseEthTopic(in); err == nil {
			t.Errorf("%q accepted", in)
		}
	}
}

func TestTopicFlags(t *testing.T) {
	tests := []struct {
		cmd          *cli.Command
		args         []string
		wantRegister []string
		wantSearch   []string
	}{
		{cmd: discv5ListenCommand, args: nil},
		{cmd: discv5ListenCommand, args: []string{"--topic-register", "a,b", "--topic-register", "c"}, wantRegister: []string{"a", "b", "c"}},
		{cmd: discv5ListenCommand, args: []string{"--topic-search", "a,b", "--topic-search", exampleTopicHash}, wantSearch: []string{"a", "b", exampleTopicHash}},
		{cmd: discv5PingCommand, args: nil},
	}
	for _, test := range tests {
		var register, search []string
		cmd := *test.cmd
		cmd.Action = func(ctx *cli.Context) error {
			if commandHasFlag(ctx, topicRegisterFlag) {
				register = ctx.StringSlice(topicRegisterFlag.Name)
				search = ctx.StringSlice(topicSearchFlag.Name)
			}
			return nil
		}
		app := &cli.App{Commands: []*cli.Command{&cmd}}
		if err := app.Run(append([]string{"devp2p", cmd.Name}, test.args...)); err != nil {
			t.Fatalf("%s %v: %v", cmd.Name, test.args, err)
		}
		if !slices.Equal(register, test.wantRegister) {
			t.Errorf("%s %v: register %v, want %v", cmd.Name, test.args, register, test.wantRegister)
		}
		if !slices.Equal(search, test.wantSearch) {
			t.Errorf("%s %v: search %v, want %v", cmd.Name, test.args, search, test.wantSearch)
		}
	}
}

func TestListenTopics(t *testing.T) {
	mainnet, _ := parseEthTopic("mainnet")
	a, _ := parseTopic("a")
	b, _ := parseTopic("b")
	tests := []struct {
		args         []string
		wantRegister []topicindex.TopicID
		wantSearch   []topicindex.TopicID
		wantNames    []string
	}{
		{args: nil},
		{args: []string{"--eth-topic", "mainnet"}, wantRegister: []topicindex.TopicID{mainnet}, wantSearch: []topicindex.TopicID{mainnet}, wantNames: []string{"mainnet"}},
		{
			args:         []string{"--topic-register", "a", "--topic-search", "b", "--eth-topic", "mainnet"},
			wantRegister: []topicindex.TopicID{a, mainnet},
			wantSearch:   []topicindex.TopicID{b, mainnet},
			wantNames:    []string{"b", "mainnet"},
		},
	}
	for _, test := range tests {
		var register, search []topicindex.TopicID
		var names []string
		cmd := *discv5ListenCommand
		cmd.Action = func(ctx *cli.Context) (err error) {
			register, search, names, err = listenTopics(ctx)
			return err
		}
		app := &cli.App{Commands: []*cli.Command{&cmd}}
		if err := app.Run(append([]string{"devp2p", cmd.Name}, test.args...)); err != nil {
			t.Fatalf("%v: %v", test.args, err)
		}
		if !slices.Equal(register, test.wantRegister) {
			t.Errorf("%v: register %v, want %v", test.args, register, test.wantRegister)
		}
		if !slices.Equal(search, test.wantSearch) {
			t.Errorf("%v: search %v, want %v", test.args, search, test.wantSearch)
		}
		if !slices.Equal(names, test.wantNames) {
			t.Errorf("%v: names %v, want %v", test.args, names, test.wantNames)
		}
	}

	cmd := *discv5ListenCommand
	cmd.Action = func(ctx *cli.Context) error {
		_, _, _, err := listenTopics(ctx)
		return err
	}
	app := &cli.App{Commands: []*cli.Command{&cmd}}
	if err := app.Run([]string{"devp2p", cmd.Name, "--eth-topic", "1/0xbad"}); err == nil {
		t.Error("invalid --eth-topic accepted")
	}
}

// TestTopicRPC registers a topic through the RPC API by name at one registrar
// and by hash at another, and looks each registration up by the other form.
func TestTopicRPC(t *testing.T) {
	tests := []struct{ register, lookup string }{
		{register: exampleTopicName, lookup: exampleTopicHash},
		{register: exampleTopicHash, lookup: exampleTopicName},
	}
	for _, test := range tests {
		t.Run("register="+test.register, func(t *testing.T) {
			t.Parallel()
			registrar := startTestV5(t)
			adv := startTestV5(t, registrar.Self())
			if err := rpcClient(t, adv).Call(nil, "discv5_registerTopic", test.register); err != nil {
				t.Fatal(err)
			}
			if err := rpcClient(t, adv).Call(nil, "discv5_registerTopic", ""); err == nil {
				t.Fatal("empty topic accepted")
			}
			client := rpcClient(t, registrar)
			deadline := time.Now().Add(15 * time.Second)
			for {
				var nodes []*enode.Node
				if err := client.Call(&nodes, "discv5_topicNodes", test.lookup); err != nil {
					t.Fatal(err)
				}
				if len(nodes) == 1 && nodes[0].ID() == adv.Self().ID() {
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("got %d nodes for %s, want the advertiser", len(nodes), test.lookup)
				}
				time.Sleep(200 * time.Millisecond)
			}
		})
	}
}

func startTestV5(t *testing.T, bootnodes ...*enode.Node) *discover.UDPv5 {
	t.Helper()
	key, _ := crypto.GenerateKey()
	db, _ := enode.OpenDB("")
	t.Cleanup(db.Close)
	ln := enode.NewLocalNode(db, key)
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IP{127, 0, 0, 1}})
	if err != nil {
		t.Fatal(err)
	}
	ln.SetStaticIP(net.IP{127, 0, 0, 1})
	ln.SetFallbackUDP(conn.LocalAddr().(*net.UDPAddr).Port)
	cfg := discover.Config{PrivateKey: key, Bootnodes: bootnodes}
	disc, err := discover.ListenV5(conn, ln, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(disc.Close)
	return disc
}

func rpcClient(t *testing.T, disc *discover.UDPv5) *rpc.Client {
	t.Helper()
	srv := rpc.NewServer()
	if err := srv.RegisterName("discv5", &discAPI{discv5API: &discv5API{disc}, searchTimeout: defaultSearchTimeout}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	client := rpc.DialInProc(srv)
	t.Cleanup(client.Close)
	return client
}
