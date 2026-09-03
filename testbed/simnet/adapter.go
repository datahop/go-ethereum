// Command simnet-testbed runs an in-process discv5 / DISC-NG testbed using
// github.com/marcopolo/simnet for simulated UDP transport.
package main

import (
	"encoding/json"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"

	"github.com/marcopolo/simnet"

	"github.com/ethereum/go-ethereum/p2p/discover"
)

// simUDPConn adapts simnet.SimConn (net.PacketConn over net.UDPAddr) to
// discv5's UDPConn interface (netip.AddrPort). It also counts per-node
// sent/received packets and bytes for overhead reporting.
type simUDPConn struct {
	*simnet.SimConn
	idx     int
	txPkts  atomic.Int64
	txBytes atomic.Int64
	rxPkts  atomic.Int64
	rxBytes atomic.Int64
}

var (
	connRegistry   []*simUDPConn
	connRegistryMu sync.Mutex
)

func registerConn(c *simUDPConn) {
	connRegistryMu.Lock()
	connRegistry = append(connRegistry, c)
	connRegistryMu.Unlock()
}

// dumpOverhead writes per-node traffic counters to path: total sent/received
// packets and bytes from the connection layer, plus the per-message-type
// breakdown from the codec layer when wire stats are enabled. Node IDs are
// included so consumers can place each node in the ID space.
func dumpOverhead(path string, tqByIdx map[int]int64, idByIdx map[int]string) {
	type rec struct {
		Idx     int                             `json:"idx"`
		ID      string                          `json:"id"`
		TxPkts  int64                           `json:"txPkts"`
		TxBytes int64                           `json:"txBytes"`
		RxPkts  int64                           `json:"rxPkts"`
		RxBytes int64                           `json:"rxBytes"`
		TQRcv   int64                           `json:"tqRcv"`
		ByType  map[string]discover.WireCounter `json:"byType,omitempty"`
	}
	wire := make(map[int]map[string]discover.WireCounter)
	for _, nr := range liveNodeRecs() {
		if nr.disc == nil {
			continue
		}
		if ws := nr.disc.WireStats(); len(ws) > 0 {
			wire[nr.idx] = ws
		}
	}
	connRegistryMu.Lock()
	recs := make([]rec, 0, len(connRegistry))
	for _, c := range connRegistry {
		recs = append(recs, rec{
			Idx: c.idx, ID: idByIdx[c.idx],
			TxPkts: c.txPkts.Load(), TxBytes: c.txBytes.Load(),
			RxPkts: c.rxPkts.Load(), RxBytes: c.rxBytes.Load(),
			TQRcv: tqByIdx[c.idx], ByType: wire[c.idx],
		})
	}
	connRegistryMu.Unlock()
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	json.NewEncoder(f).Encode(recs)
}

func (c *simUDPConn) ReadFromUDPAddrPort(b []byte) (int, netip.AddrPort, error) {
	n, addr, err := c.SimConn.ReadFrom(b)
	if err != nil {
		return 0, netip.AddrPort{}, err
	}
	c.rxPkts.Add(1)
	c.rxBytes.Add(int64(n))
	return n, addr.(*net.UDPAddr).AddrPort(), nil
}

func (c *simUDPConn) WriteToUDPAddrPort(b []byte, addr netip.AddrPort) (int, error) {
	n, err := c.SimConn.WriteTo(b, net.UDPAddrFromAddrPort(addr))
	if err == nil {
		c.txPkts.Add(1)
		c.txBytes.Add(int64(n))
	}
	return n, err
}
