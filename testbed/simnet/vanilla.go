package main

import (
	"crypto/ecdsa"
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/marcopolo/simnet"

	// Stock upstream geth v1.17.3 discv5 stack (renamed module). These are
	// distinct Go types from the fork's enode/discover; the two stacks
	// interoperate only over the wire (packets + ENR strings).
	vcrypto "github.com/ethereum/go-ethereum-vanilla/crypto"
	vdiscover "github.com/ethereum/go-ethereum-vanilla/p2p/discover"
	venode "github.com/ethereum/go-ethereum-vanilla/p2p/enode"
)

// vanillaRec is a node running the stock upstream discv5 stack. It provides DHT
// routing substrate but does not speak topic discovery.
type vanillaRec struct {
	idx  int
	key  *ecdsa.PrivateKey
	ln   *venode.LocalNode
	disc *vdiscover.UDPv5
}

func (v vanillaRec) url() string { return v.ln.Node().String() }
func (v vanillaRec) id() string  { return v.ln.ID().String() }

// spawnVanillaNode creates and starts a single stock-geth-v1.17.3 discv5 node on
// the simnet transport. bootURLs are ENR strings parsed by the vanilla stack's
// own parser (cross-stack bootstrap happens purely via the ENR string).
func spawnVanillaNode(sim *simnet.Simnet, settings simnet.NodeBiDiLinkSettings, idx int, bootURLs []string, refreshInterval time.Duration) vanillaRec {
	key, err := vcrypto.GenerateKey()
	if err != nil {
		fatalf("vanilla generate key %d: %v", idx, err)
	}
	addr := &net.UDPAddr{IP: net.IP{33, byte(idx / 256), byte(idx % 256), 1}, Port: 30303}
	conn := &simUDPConn{SimConn: sim.NewEndpoint(addr, settings)}

	db, err := venode.OpenDB("")
	if err != nil {
		fatalf("vanilla open db %d: %v", idx, err)
	}
	ln := venode.NewLocalNode(db, key)
	ln.SetStaticIP(addr.IP)
	ln.SetFallbackUDP(addr.Port)

	var boot []*venode.Node
	for _, u := range bootURLs {
		n, err := venode.Parse(venode.ValidSchemes, u)
		if err != nil {
			continue // skip URLs this stack can't parse rather than aborting
		}
		boot = append(boot, n)
	}
	cfg := vdiscover.Config{PrivateKey: key, Bootnodes: boot}
	if refreshInterval > 0 {
		cfg.RefreshInterval = refreshInterval
	}
	disc, err := vdiscover.ListenV5(conn, ln, cfg)
	if err != nil {
		fatalf("vanilla listen v5 on node %d: %v", idx, err)
	}
	return vanillaRec{idx: idx, key: key, ln: ln, disc: disc}
}

// reportInterop measures whether the two stacks actually merged into one DHT:
// how many vanilla nodes appear in the (sampled) fork nodes' routing tables and
// vice versa. Healthy cross-population is the signal that TopDisc and stock geth
// interoperate at the wire level. Hosts are sampled to bound cost at scale.
func reportInterop(forks []nodeRec, vanillas []vanillaRec, maxHosts int) {
	forkIDs := make(map[string]bool, len(forks))
	for _, f := range forks {
		forkIDs[f.ln.ID().String()] = true
	}
	vanillaIDs := make(map[string]bool, len(vanillas))
	for _, v := range vanillas {
		vanillaIDs[v.id()] = true
	}

	sampleFork := forks
	if maxHosts > 0 && len(sampleFork) > maxHosts {
		sampleFork = sampleFork[:maxHosts]
	}
	sampleVan := vanillas
	if maxHosts > 0 && len(sampleVan) > maxHosts {
		sampleVan = sampleVan[:maxHosts]
	}

	// Fork tables: count vanilla nodes seen, per host.
	fSeesV := make([]int, 0, len(sampleFork))
	for _, f := range sampleFork {
		c := 0
		for _, n := range f.disc.AllNodes() {
			if vanillaIDs[n.ID().String()] {
				c++
			}
		}
		fSeesV = append(fSeesV, c)
	}
	// Vanilla tables: count fork nodes seen, per host.
	vSeesF := make([]int, 0, len(sampleVan))
	for _, v := range sampleVan {
		c := 0
		for _, n := range v.disc.AllNodes() {
			if forkIDs[n.ID().String()] {
				c++
			}
		}
		vSeesF = append(vSeesF, c)
	}

	stat := func(xs []int) (min, med, max int, anyZero int) {
		if len(xs) == 0 {
			return 0, 0, 0, 0
		}
		s := append([]int(nil), xs...)
		sort.Ints(s)
		for _, x := range s {
			if x == 0 {
				anyZero++
			}
		}
		return s[0], s[len(s)/2], s[len(s)-1], anyZero
	}
	fmin, fmed, fmax, fzero := stat(fSeesV)
	vmin, vmed, vmax, vzero := stat(vSeesF)

	fmt.Println()
	fmt.Println("=== cross-stack interop (DHT merge) ===")
	fmt.Printf("  fork hosts: %d TopDisc, %d vanilla in network\n", len(forks), len(vanillas))
	fmt.Printf("  vanilla nodes seen in fork tables (sampled %d):    min=%d med=%d max=%d  hostsWithNone=%d\n",
		len(sampleFork), fmin, fmed, fmax, fzero)
	fmt.Printf("  fork nodes seen in vanilla tables (sampled %d):    min=%d med=%d max=%d  hostsWithNone=%d\n",
		len(sampleVan), vmin, vmed, vmax, vzero)
}
