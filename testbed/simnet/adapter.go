// Command simnet-testbed runs an in-process discv5 / DISC-NG testbed using
// github.com/marcopolo/simnet for simulated UDP transport.
package main

import (
	"net"
	"net/netip"

	"github.com/marcopolo/simnet"
)

// simUDPConn adapts simnet.SimConn (net.PacketConn over net.UDPAddr) to
// discv5's UDPConn interface (netip.AddrPort).
type simUDPConn struct {
	*simnet.SimConn
}

func (c *simUDPConn) ReadFromUDPAddrPort(b []byte) (int, netip.AddrPort, error) {
	n, addr, err := c.SimConn.ReadFrom(b)
	if err != nil {
		return 0, netip.AddrPort{}, err
	}
	return n, addr.(*net.UDPAddr).AddrPort(), nil
}

func (c *simUDPConn) WriteToUDPAddrPort(b []byte, addr netip.AddrPort) (int, error) {
	return c.SimConn.WriteTo(b, net.UDPAddrFromAddrPort(addr))
}
