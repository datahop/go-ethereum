package main

import (
	"crypto/ecdsa"
	"encoding/binary"
	"math"
	"math/rand"
	"net"
	"time"

	"github.com/marcopolo/simnet"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// defaultMaxBootnodes caps how many predecessors a fresh node bootstraps from.
const defaultMaxBootnodes = 20

// testTopic is the legacy single-topic ID used when -topics is unset or 1.
var testTopic = topicindex.TopicID{0x55, 0x49, 0x43, 0x4e, 0x47, 0x54, 0x45, 0x53, 0x54}

// node* vars override topicindex.Config defaults when set; wired from flags.
var (
	nodeAdLifetime           time.Duration
	nodeSearchBucketSize     int
	nodeRegAttemptTimeout    time.Duration
	nodeRemoveOnExpiry       bool
	nodeNodesPerSourceBucket int
)

// makeTopic returns a deterministic 32-byte topic ID for index i.
func makeTopic(i int) topicindex.TopicID {
	// Spread topic ids uniformly across the keyspace with a golden-ratio (Weyl)
	// sequence on the high 64 bits, so any number of topics is maximally
	// separated (no accidental clustering). Low bits are hash-derived so ids are
	// still full-width and realistic.
	const phi = 0.6180339887498949 // (sqrt(5) - 1) / 2
	frac := math.Mod(float64(i+1)*phi, 1.0)
	h := crypto.Keccak256([]byte{0x74, 0x6f, 0x70, byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)})
	var t topicindex.TopicID
	copy(t[:], h)
	binary.BigEndian.PutUint64(t[:8], uint64(frac*math.Ldexp(1, 63))<<1)
	return t
}

type nodeRec struct {
	idx    int
	key    *ecdsa.PrivateKey
	ln     *enode.LocalNode
	disc   *discover.UDPv5
	legacy bool
}

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

// sampleBootnodes picks up to max bootnodes from pool: the first node plus a
// random sample of the rest.
func sampleBootnodes(pool []nodeRec, max int, rng *rand.Rand) []*enode.Node {
	if len(pool) == 0 {
		return nil
	}
	if max <= 0 {
		max = defaultMaxBootnodes
	}
	boot := []*enode.Node{pool[0].ln.Node()}
	rest := pool[1:]
	n := max - 1
	if n > len(rest) {
		n = len(rest)
	}
	for _, idx := range rng.Perm(len(rest))[:n] {
		boot = append(boot, rest[idx].ln.Node())
	}
	return boot
}

// spawnNode creates one discv5 node with the given bootnodes and the flag-driven
// topic config overrides.
func spawnNode(sim *simnet.Simnet, settings simnet.NodeBiDiLinkSettings, idx int, legacy bool, boot []*enode.Node, refreshInterval time.Duration) nodeRec {
	key, err := crypto.GenerateKey()
	if err != nil {
		fatalf("generate key %d: %v", idx, err)
	}
	// Non-LAN public-style /24 per node so the IP-diversity defences fire.
	addr := &net.UDPAddr{
		IP:   net.IP{33, byte(idx / 256), byte(idx % 256), 1},
		Port: 30303,
	}
	conn := &simUDPConn{SimConn: sim.NewEndpoint(addr, settings), idx: idx}
	registerConn(conn)

	db, err := enode.OpenDB("")
	if err != nil {
		fatalf("open enode db %d: %v", idx, err)
	}
	ln := enode.NewLocalNode(db, key)
	ln.SetStaticIP(addr.IP)
	ln.SetFallbackUDP(addr.Port)

	cfg := discover.Config{PrivateKey: key}
	if refreshInterval > 0 {
		cfg.RefreshInterval = refreshInterval
	}
	cfg.Bootnodes = boot
	if nodeAdLifetime > 0 {
		cfg.Topic.AdLifetime = nodeAdLifetime
	}
	if nodeSearchBucketSize > 0 {
		cfg.Topic.SearchBucketSize = nodeSearchBucketSize
	}
	if nodeRegAttemptTimeout > 0 {
		cfg.Topic.RegAttemptTimeout = nodeRegAttemptTimeout
	}
	if nodeNodesPerSourceBucket > 0 {
		cfg.Topic.MaxNodesPerSourcePerBucket = nodeNodesPerSourceBucket
	}
	cfg.Topic.RemoveOnExpiry = nodeRemoveOnExpiry

	disc, err := discover.ListenV5(conn, ln, cfg)
	if err != nil {
		fatalf("listen v5 on node %d: %v", idx, err)
	}
	if legacy {
		ln.Delete(new(topicindex.TopicDiscovery))
	}
	return nodeRec{idx: idx, key: key, ln: ln, disc: disc, legacy: legacy}
}

func spawnNodes(sim *simnet.Simnet, settings simnet.NodeBiDiLinkSettings, count int, legacySet map[int]bool, maxBootnodes int, spawnDelay time.Duration, refreshInterval time.Duration, adLifetime time.Duration) []nodeRec {
	nodeAdLifetime = adLifetime
	if maxBootnodes <= 0 {
		maxBootnodes = defaultMaxBootnodes
	}
	rng := rand.New(rand.NewSource(1))
	all := make([]nodeRec, 0, count)
	for i := 0; i < count; i++ {
		if i > 0 && spawnDelay > 0 {
			time.Sleep(spawnDelay)
		}
		boot := sampleBootnodes(all, maxBootnodes, rng)
		rec := spawnNode(sim, settings, i, legacySet[i], boot, refreshInterval)
		all = append(all, rec)
	}
	return all
}
