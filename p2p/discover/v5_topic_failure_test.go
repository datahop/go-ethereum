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

package discover

import (
	"net"
	"net/netip"
	"testing"
	"time"
)

// waitForCond polls cond until it returns true or the deadline passes.
func waitForCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// TestTopicFailureBlacklist checks that the topic system blacklists a node after
// MaxNodeFailures consecutive failed RPCs, and not before.
func TestTopicFailureBlacklist(t *testing.T) {
	test := newUDPV5Test(t)
	defer test.close()

	sys := test.udp.topicSys
	bl := sys.config.Blacklist
	n := nodeAtDistance(test.table.self().ID(), 128, net.IP{203, 0, 113, 7})
	id := n.ID()
	ip := netip.AddrFrom4([4]byte{203, 0, 113, 7})
	N := sys.config.MaxNodeFailures

	fail := func() { test.udp.callOutcomeCh <- callOutcome{id: id, ip: ip, success: false} }

	// N-1 failures: counter rises but no ban yet.
	for i := 0; i < N-1; i++ {
		fail()
	}
	waitForCond(t, "failure counter reaches N-1", func() bool {
		return test.db.FindFailsV5(id, ip) == N-1
	})
	if bl.Contains(id) {
		t.Fatal("node blacklisted before reaching MaxNodeFailures")
	}

	// One more failure crosses the threshold: node is banned and the counter is
	// reset so a future ban needs a fresh budget.
	fail()
	waitForCond(t, "node blacklisted", func() bool { return bl.Contains(id) })
	if got := test.db.FindFailsV5(id, ip); got != 0 {
		t.Fatalf("failure counter not reset after ban, got %d", got)
	}
}

// TestTopicFailureSuccessResets checks that a successful RPC resets the
// consecutive-failure counter, preventing a ban.
func TestTopicFailureSuccessResets(t *testing.T) {
	test := newUDPV5Test(t)
	defer test.close()

	sys := test.udp.topicSys
	bl := sys.config.Blacklist
	n := nodeAtDistance(test.table.self().ID(), 129, net.IP{203, 0, 113, 8})
	id := n.ID()
	ip := netip.AddrFrom4([4]byte{203, 0, 113, 8})
	N := sys.config.MaxNodeFailures
	if N < 2 {
		t.Skip("test needs MaxNodeFailures >= 2")
	}

	fail := func() { test.udp.callOutcomeCh <- callOutcome{id: id, ip: ip, success: false} }
	ok := func() { test.udp.callOutcomeCh <- callOutcome{id: id, ip: ip, success: true} }

	// N-1 failures, then a success resets the counter.
	for i := 0; i < N-1; i++ {
		fail()
	}
	waitForCond(t, "counter reaches N-1", func() bool { return test.db.FindFailsV5(id, ip) == N-1 })
	ok()
	waitForCond(t, "counter resets to 0", func() bool { return test.db.FindFailsV5(id, ip) == 0 })

	// N-1 more failures must still not ban (proves the success reset the count).
	for i := 0; i < N-1; i++ {
		fail()
	}
	waitForCond(t, "counter reaches N-1 again", func() bool { return test.db.FindFailsV5(id, ip) == N-1 })
	if bl.Contains(id) {
		t.Fatal("node blacklisted despite an intervening success")
	}
}
