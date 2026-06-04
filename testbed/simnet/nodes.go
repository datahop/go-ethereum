package main

import (
	"crypto/ecdsa"
	"math/rand"
	"net"
	"time"

	"github.com/marcopolo/simnet"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// defaultMaxBootnodes caps how many of the previously-spawned nodes a
// fresh node uses as its bootstrap set. Without a cap, node N tries to
// PING all N-1 predecessors at startup, producing O(N^2) bootstrap
// traffic that stalls at scale (n=2000 stalls indefinitely). Mirrors the
// Python testbed's select_bootnodes helper which uses ~20. Configurable
// via -max-bootnodes — smaller values reduce startup traffic at the cost
// of slower routing-table convergence.
const defaultMaxBootnodes = 20

// testTopic is the legacy single-topic ID used when -topics is unset or 1.
// Identical across runs so behaviour is reproducible.
var testTopic = topicindex.TopicID{0x55, 0x49, 0x43, 0x4e, 0x47, 0x54, 0x45, 0x53, 0x54}

// makeTopic returns a deterministic 32-byte topic ID for index i, used when
// -topics > 1.
func makeTopic(i int) topicindex.TopicID {
	var t topicindex.TopicID
	t[0] = 0x44 // 'D'
	t[1] = 0x4e // 'N'
	t[2] = 0x47 // 'G'
	t[3] = byte(i >> 24)
	t[4] = byte(i >> 16)
	t[5] = byte(i >> 8)
	t[6] = byte(i)
	return t
}

type nodeRec struct {
	idx    int
	key    *ecdsa.PrivateKey
	ln     *enode.LocalNode
	disc   *discover.UDPv5
	legacy bool // true if the DiscNG ENR flag was stripped — used by the validation workload
}

// pickLegacySet deterministically chooses which node indices will have the
// DISC-NG ENR flag stripped, simulating legacy Discv5 peers in a mixed
// population.
func pickLegacySet(total int, frac float64, seed int64) map[int]bool {
	count := int(float64(total) * frac)
	if count <= 0 {
		return nil
	}
	s := seed
	if s == 0 {
		s = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(s))
	set := make(map[int]bool, count)
	for _, i := range rng.Perm(total)[:count] {
		set[i] = true
	}
	return set
}

func spawnNodes(sim *simnet.Simnet, settings simnet.NodeBiDiLinkSettings, count int, legacySet map[int]bool, maxBootnodes int, spawnDelay time.Duration, refreshInterval time.Duration) []nodeRec {
	if maxBootnodes <= 0 {
		maxBootnodes = defaultMaxBootnodes
	}
	all := make([]nodeRec, 0, count)
	for i := 0; i < count; i++ {
		if i > 0 && spawnDelay > 0 {
			time.Sleep(spawnDelay)
		}
		key, err := crypto.GenerateKey()
		if err != nil {
			fatalf("generate key %d: %v", i, err)
		}

		// Use 4-byte IP form so endpoint addresses match what the discv5 ->
		// adapter -> WriteTo path produces; 16-byte forms surface as router
		// "unknown destination" drops.
		//
		// Use a non-LAN public-style range (33.x.x.x). RFC1918 ranges like
		// 10.0.0.0/8 are treated as LAN by go-ethereum's netutil.IsLAN,
		// which silently disables the IP-bucket cap (regBucketSubnet=24)
		// and the IP-similarity score in §6 eq. 1. Spreading nodes across
		// distinct /24s (byte 33.N1.N2.1 with N1*256+N2 = node index) makes
		// the cap actually fire, exercising the full DISC-NG IP-defence
		// pathway.
		addr := &net.UDPAddr{
			IP:   net.IP{33, byte(i / 256), byte(i % 256), 1},
			Port: 30303,
		}
		conn := &simUDPConn{SimConn: sim.NewEndpoint(addr, settings)}

		db, err := enode.OpenDB("")
		if err != nil {
			fatalf("open enode db %d: %v", i, err)
		}
		ln := enode.NewLocalNode(db, key)
		ln.SetStaticIP(addr.IP)
		ln.SetFallbackUDP(addr.Port)

		cfg := discover.Config{PrivateKey: key}
		if refreshInterval > 0 {
			cfg.RefreshInterval = refreshInterval
		}
		if len(all) > 0 {
			cfg.Bootnodes = append(cfg.Bootnodes, all[0].ln.Node())
			pool := all[1:]
			n := maxBootnodes - 1
			if n > len(pool) {
				n = len(pool)
			}
			for _, idx := range rand.Perm(len(pool))[:n] {
				cfg.Bootnodes = append(cfg.Bootnodes, pool[idx].ln.Node())
			}
		}

		disc, err := discover.ListenV5(conn, ln, cfg)
		if err != nil {
			fatalf("listen v5 on node %d: %v", i, err)
		}

		// Strip the DISC-NG capability flag from the LocalNode's record
		// before sim.Start() so this node looks like a stock-Discv5 peer
		// on the wire. ListenV5 sets the flag inside newUDPv5; we delete
		// here, before any packet is sent, so peers only ever see the
		// legacy version of this node's ENR.
		legacy := legacySet[i]
		if legacy {
			ln.Delete(new(topicindex.TopicDiscovery))
		}

		all = append(all, nodeRec{idx: i, key: key, ln: ln, disc: disc, legacy: legacy})
	}
	return all
}
