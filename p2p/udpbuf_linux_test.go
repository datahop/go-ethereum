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

//go:build linux

package p2p

import (
	"net"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/internal/testlog"
	"github.com/ethereum/go-ethereum/log"
)

func TestSetUDPReadBuffer(t *testing.T) {
	data, err := os.ReadFile("/proc/sys/net/core/rmem_max")
	if err != nil {
		t.Skip("can't read rmem_max:", err)
	}
	rmemMax, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatal("invalid rmem_max:", err)
	}
	logger := testlog.Logger(t, log.LvlDebug)

	t.Run("request", func(t *testing.T) {
		conn := listenUDPTest(t)
		setUDPReadBuffer(conn, discoveryReadBuffer, logger)
		size, err := udpReadBufferSize(conn)
		if err != nil {
			t.Fatal("read buffer size:", err)
		}
		// Unprivileged requests are clamped to rmem_max.
		if want := min(discoveryReadBuffer, rmemMax); size < want {
			t.Fatalf("read buffer size %d, want >= %d", size, want)
		}
	})
	t.Run("keep-larger", func(t *testing.T) {
		conn := listenUDPTest(t)
		if err := conn.SetReadBuffer(min(discoveryReadBuffer, rmemMax)); err != nil {
			t.Fatal(err)
		}
		before, err := udpReadBufferSize(conn)
		if err != nil {
			t.Fatal("read buffer size:", err)
		}
		setUDPReadBuffer(conn, before/2, logger)
		after, err := udpReadBufferSize(conn)
		if err != nil {
			t.Fatal("read buffer size:", err)
		}
		if after != before {
			t.Fatalf("read buffer size changed from %d to %d", before, after)
		}
	})
}

func listenUDPTest(t *testing.T) *net.UDPConn {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}
