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

// Regression tests for the waiting-time overflow (finding F1 of the registrar
// overhead study), fixed by #155 (cap at maxWaitTime) and #157 (saturating wire
// decode, sub-floor clamp). They follow a waiting time from TopicTable.Register through the
// REGCONFIRMATION encoding (waitTimeToMs), the advertiser's decoding
// (v5_topic.go: time.Duration(ms) * time.Millisecond) and ticket validation
// (TicketSealer.Unpack) at high cache occupancy, where the occupancy factor
// (1 - N/C)^-10 makes the float waiting time exceed the int64 range of
// time.Duration. Converting such a float to an integer is implementation-
// defined in Go: amd64 yields math.MinInt64, arm64 saturates to MaxInt64
// (and NaN converts to MinInt64 on amd64, 0 on arm64).

import (
	"fmt"
	"math"
	mrand "math/rand"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

const overflowCapacity = 1000

type overflowCase struct {
	name    string
	share   float64 // fraction of cached ads in the requested topic
	sameNet bool    // cached ads and requester share a /16 (high IP score)
}

var overflowCases = []overflowCase{
	{"lowshare-lowip", 0, false},
	{"lowshare-highip", 0, true},
	{"highshare-lowip", 0.5, false},
	{"highshare-highip", 0.5, true},
}

func overflowNode(r *mrand.Rand, sameNet bool) *enode.Node {
	var rec enr.Record
	ip := net.IP{byte(1 + r.Intn(223)), byte(r.Intn(256)), byte(r.Intn(256)), byte(1 + r.Intn(254))}
	for ip[0] == 10 || ip[0] == 100 || ip[0] == 127 || ip[0] == 169 || ip[0] == 172 || ip[0] == 192 || ip[0] == 198 {
		ip[0] = byte(1 + r.Intn(223)) // avoid LAN and special-purpose blocks
	}
	if sameNet {
		ip[0], ip[1] = 52, 7
	}
	rec.Set(enr.IP(ip))
	var id enode.ID
	r.Read(id[:])
	return enode.SignNull(&rec, id)
}

// overflowTable fills a table with n ads; max(1, share*n) of them for topic.
func overflowTable(n int, c overflowCase) (*topicindex.TopicTable, *mclock.Simulated, topicindex.TopicID, *enode.Node) {
	r := mrand.New(mrand.NewSource(int64(n)))
	clock := new(mclock.Simulated)
	clock.Run(time.Hour) // keep AbsTime arithmetic away from zero
	tab := topicindex.NewTopicTable(topicindex.Config{AdCacheSize: overflowCapacity, Clock: clock})
	var topic, other topicindex.TopicID
	topic[0], other[0] = 1, 2
	inTopic := max(1, int(c.share*float64(n)))
	for i := 0; i < n; i++ {
		t := other
		if i < inTopic {
			t = topic
		}
		if !tab.Add(overflowNode(r, c.sameNet), t) {
			panic("table full")
		}
	}
	return tab, clock, topic, overflowNode(r, c.sameNet)
}

type overflowResult struct {
	required   time.Duration // TopicTable.WaitTime
	wait       time.Duration // Register return value (0 = admitted)
	admitted   bool
	wireMs     uint          // REGCONFIRMATION.WaitTime
	clientWait time.Duration // as decoded by the advertiser
	ticketOK   string        // ticket validity when the advertiser retries as told
	bounds     int
}

// overflowProbe registers a fresh ad without credit and follows the response.
func overflowProbe(n int, c overflowCase) overflowResult {
	tab, clock, topic, node := overflowTable(n, c)
	var res overflowResult
	res.required = tab.WaitTime(node, topic)
	before := tab.Occupancy()
	res.wait = tab.Register(node, topic, 0)
	res.admitted = tab.Occupancy() > before
	res.bounds = tab.Stats().TopicBounds
	if res.wait <= 0 {
		return res
	}
	// handleRegtopic: ticket and REGCONFIRMATION for a wait response.
	sealer := topicindex.NewTicketSealer(clock)
	ticket := &topicindex.Ticket{Topic: topic, WaitTimeIssued: res.wait, LastUsed: clock.Now(), FirstIssued: clock.Now()}
	enc := sealer.Pack(ticket)
	res.wireMs = waitTimeToMs(res.wait)
	// v5_topic.go: advertiser decoding of the wire value.
	res.clientWait = waitTimeFromMs(res.wireMs)
	switch {
	case res.clientWait <= 0:
		// The advertiser schedules its retry at now+clientWait, i.e. immediately.
		_, err := sealer.Unpack(topic, enc)
		res.ticketOK = fmt.Sprintf("immediate retry: %v", errString(err))
	case res.clientWait > 100*365*24*time.Hour:
		start := ticket.LastUsed.Add(ticket.WaitTimeIssued)
		res.ticketOK = fmt.Sprintf("retry beyond 100y; window start wraps=%v", start < ticket.LastUsed)
	default:
		clock.Run(res.clientWait)
		_, err := sealer.Unpack(topic, enc)
		res.ticketOK = errString(err)
	}
	return res
}

func errString(err error) string {
	if err == nil {
		return "valid"
	}
	return err.Error()
}

// TestRegisterHighOccupancy records production behaviour at 80-100% occupancy.
func TestRegisterHighOccupancy(t *testing.T) {
	t.Logf("GOARCH=%s, C=%d, E=15m", runtime.GOARCH, overflowCapacity)
	for _, pct := range []int{80, 85, 90, 95, 99, 100} {
		for _, c := range overflowCases {
			n := pct * overflowCapacity / 100
			r := overflowProbe(n, c)
			t.Logf("%3d%% %-17s WaitTime=%-22v Register=%-22v admitted=%-5v wire=%dms client=%v ticket=%s topicBounds=%d",
				pct, c.name, r.required, r.wait, r.admitted, r.wireMs, r.clientWait, r.ticketOK, r.bounds)
		}
	}
}

// TestRegisterOverflowThreshold checks that no occupancy makes the float
// waiting time overflow a time.Duration (F1, fixed by #155). It logs the first
// overflowing occupancy, -1 if none.
func TestRegisterOverflowThreshold(t *testing.T) {
	for _, c := range overflowCases {
		first := -1
		for n := 700; n < overflowCapacity; n++ {
			tab, _, topic, node := overflowTable(n, c)
			if w := tab.WaitTime(node, topic); w <= 0 || w == math.MaxInt64 {
				first = n
				break
			}
		}
		t.Logf("%s: GOARCH=%s first overflowing occupancy N=%d of C=%d", c.name, runtime.GOARCH, first, overflowCapacity)
		if first >= 0 {
			t.Errorf("%s: waiting time overflows at N=%d", c.name, first)
		}
	}
}

// TestRegisterHighOccupancyInvariants: at any occupancy >= 80% the required
// wait exceeds the 1s admission floor by orders of magnitude, so a fresh ad
// without credit must not be admitted, the quote must decode to a positive wait
// no longer than the cap, and an advertiser that waits as told must be able to
// retry. Failed before #155/#157 (F1).
func TestRegisterHighOccupancyInvariants(t *testing.T) {
	for _, pct := range []int{80, 85, 90, 95, 99, 100} {
		for _, c := range overflowCases {
			n := pct * overflowCapacity / 100
			r := overflowProbe(n, c)
			switch {
			case r.admitted:
				t.Errorf("%d%% %s: fresh ad admitted without credit (WaitTime=%v)", pct, c.name, r.required)
			case r.wait > 0 && (r.clientWait <= 0 || r.clientWait > 24*time.Hour):
				t.Errorf("%d%% %s: wait %v decodes to %v at the advertiser", pct, c.name, r.wait, r.clientWait)
			case r.wait > 0 && r.ticketOK != "valid" && r.clientWait <= 6*time.Hour:
				// Longer waits outlive the ticket key (ticketKeyLifetime); that is
				// a separate issue, see TestTicketOutlivesKey.
				t.Errorf("%d%% %s: ticket not valid after waiting as told: %s", pct, c.name, r.ticketOK)
			}
		}
	}
}

// TestRegisterFullCacheNaN covers N == C with an empty topic and a requester
// without an address: both components are 0 * +Inf = NaN.
func TestRegisterFullCacheNaN(t *testing.T) {
	tab, _, _, _ := overflowTable(overflowCapacity, overflowCases[0])
	var empty topicindex.TopicID
	empty[0] = 9
	var rec enr.Record
	node := enode.SignNull(&rec, enode.ID{9})
	before := tab.Occupancy()
	w := tab.Register(node, empty, 0)
	t.Logf("GOARCH=%s full cache, empty topic, no IP: WaitTime=%v Register=%v admitted=%v", runtime.GOARCH, tab.WaitTime(node, empty), w, tab.Occupancy() > before)
	if tab.Occupancy() > before {
		t.Fatal("admitted into a full cache")
	}
}

// TestTicketOutlivesKey records that a ticket whose wait exceeds the 6h ticket
// key lifetime is rejected when the key has not been used since it was issued
// (F3, still open: the registrar quotes up to maxWaitTime = 24h).
func TestTicketOutlivesKey(t *testing.T) {
	clock := new(mclock.Simulated)
	sealer := topicindex.NewTicketSealer(clock)
	var topic topicindex.TopicID
	ticket := &topicindex.Ticket{Topic: topic, WaitTimeIssued: 7 * time.Hour, LastUsed: clock.Now(), FirstIssued: clock.Now()}
	enc := sealer.Pack(ticket)
	clock.Run(7 * time.Hour)
	_, err := sealer.Unpack(topic, enc)
	t.Logf("ticket with a 7h wait, retried after 7h on an otherwise idle registrar: %v", errString(err))
	if err == nil {
		t.Fatal("expected the ticket key to have expired")
	}
}
