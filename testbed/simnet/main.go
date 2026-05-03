package main

import (
	"crypto/ecdsa"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/marcopolo/simnet"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

func main() {
	nodes := flag.Int("nodes", 5, "number of discv5 nodes to spawn")
	latencyMs := flag.Int("latency", 30, "static per-pair latency in milliseconds")
	bandwidthMibps := flag.Int("bandwidth-mibps", 100, "per-direction bandwidth (Mibps)")
	runFor := flag.Duration("duration", 5*time.Second, "how long to run before tearing down")
	flag.Parse()

	fmt.Printf("simnet-testbed: spawning %d nodes (latency=%dms, bw=%dMibps)\n",
		*nodes, *latencyMs, *bandwidthMibps)

	sim := &simnet.Simnet{
		LatencyFunc: simnet.StaticLatency(time.Duration(*latencyMs) * time.Millisecond),
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
	settings := simnet.NodeBiDiLinkSettings{
		Downlink: simnet.LinkSettings{BitsPerSecond: *bandwidthMibps * simnet.Mibps},
		Uplink:   simnet.LinkSettings{BitsPerSecond: *bandwidthMibps * simnet.Mibps},
	}

	type nodeRec struct {
		key  *ecdsa.PrivateKey
		ln   *enode.LocalNode
		disc *discover.UDPv5
	}
	all := make([]nodeRec, 0, *nodes)

	for i := 0; i < *nodes; i++ {
		key, err := crypto.GenerateKey()
		if err != nil {
			fatalf("generate key %d: %v", i, err)
		}

		// Use 4-byte IP form so endpoint addresses match what the discv5 ->
		// adapter -> WriteTo path produces (net.UDPAddrFromAddrPort yields
		// 4-byte IPs for IPv4). simnet's router keys addresses strictly, so
		// 4-byte vs 16-byte mismatches surface as "unknown destination" drops.
		addr := &net.UDPAddr{
			IP:   net.IP{10, 100, byte(i / 256), byte(i%256 + 1)},
			Port: 30303,
		}
		conn := &simUDPConn{SimConn: sim.NewEndpoint(addr, settings)}

		// Build a local node record. discover wants an enode.DB; in-memory is fine.
		db, err := enode.OpenDB("")
		if err != nil {
			fatalf("open enode db %d: %v", i, err)
		}
		ln := enode.NewLocalNode(db, key)
		ln.SetStaticIP(addr.IP)
		ln.SetFallbackUDP(addr.Port)

		cfg := discover.Config{PrivateKey: key}
		// Boot from the previously-spawned nodes so the DHT can form.
		for _, prev := range all {
			cfg.Bootnodes = append(cfg.Bootnodes, prev.ln.Node())
		}

		disc, err := discover.ListenV5(conn, ln, cfg)
		if err != nil {
			fatalf("listen v5 on node %d: %v", i, err)
		}

		all = append(all, nodeRec{key: key, ln: ln, disc: disc})
		fmt.Printf("  node %d: %s @ %s\n", i, ln.ID().TerminalString(), addr)
	}

	sim.Start()
	defer sim.Close()

	fmt.Printf("simnet up; running for %s\n", *runFor)
	time.Sleep(*runFor)

	for _, n := range all {
		n.disc.Close()
	}
	fmt.Println("teardown complete")
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "fatal: "+format+"\n", args...)
	os.Exit(1)
}
