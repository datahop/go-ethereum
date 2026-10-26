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

package topicindex

// Per-operation cost of the registrar core on controlled cache states. The
// global occupancy N and the size T of the requested topic are varied
// independently. Sub-benchmark names carry the parameters for benchstat and
// for the complexity fits in docs/registrar-overhead.

import (
	"fmt"
	mrand "math/rand"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/p2p/netutil"
)

const benchCapacity = 20000

var (
	benchSizesN = []int{0, 100, 1000, 2500, 5000, 10000}
	benchSizesT = []int{1, 16, 64, 256, 1024, 4096}
	benchFixedN = 5000
	benchFixedT = 16
)

func benchNode(r *mrand.Rand, concentrated bool) *enode.Node {
	var rec enr.Record
	ip := randomPublicIPv4(r)
	if concentrated {
		ip = net.IP{ip[0] & 0x0f, 1, byte(r.Intn(256)), byte(1 + r.Intn(254))}
		ip[0] |= 0x40 // 64.x - 79.x: public
	}
	rec.Set(enr.IP(ip))
	var id enode.ID
	r.Read(id[:])
	return enode.SignNull(&rec, id)
}

// benchTable returns a table with n ads, t of them for 'topic' and the rest
// spread over 299 other topics. All entries expire at E from the clock's
// current time.
func benchTable(n, t int, concentrated bool) (*TopicTable, *mclock.Simulated, TopicID, []*enode.Node) {
	if t > n {
		panic("t > n")
	}
	r := mrand.New(mrand.NewSource(int64(n*131 + t)))
	clock := new(mclock.Simulated)
	tab := NewTopicTable(Config{AdCacheSize: benchCapacity, Clock: clock})
	var topic TopicID
	topic[0] = 0xaa
	others := make([]TopicID, 299)
	for i := range others {
		others[i][0], others[i][1] = 0xbb, byte(i)
		others[i][2] = byte(i >> 8)
	}
	nodes := make([]*enode.Node, 0, n)
	for i := 0; i < n; i++ {
		nd := benchNode(r, concentrated)
		if i < t {
			tab.add(nd, topic)
		} else {
			tab.add(nd, others[i%len(others)])
		}
		nodes = append(nodes, nd)
	}
	return tab, clock, topic, nodes
}

func freshNodes(k int) []*enode.Node {
	r := mrand.New(mrand.NewSource(99))
	out := make([]*enode.Node, k)
	for i := range out {
		out[i] = benchNode(r, false)
	}
	return out
}

// Register for a fresh ad without credit. Most requests receive a wait
// ticket (state unchanged apart from lower bounds); an ad admitted under the 1s
// floor is removed again outside the timed region.
func benchRegisterWait(b *testing.B, n, t int) {
	tab, _, topic, _ := benchTable(n, t, false)
	pool := freshNodes(1024)
	admitted := 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if tab.Register(pool[i%len(pool)], topic, 0) == 0 {
			b.StopTimer()
			admitted++
			reg := tab.reg[topic].Front().Value.(*topicTableEntry)
			tab.remove(reg)
			tab.wt.removeReg(reg)
			b.StartTimer()
		}
	}
	b.ReportMetric(float64(admitted)/float64(b.N), "admit/op")
}

func BenchmarkRegisterFresh(b *testing.B) {
	for _, n := range benchSizesN {
		if n < benchFixedT || n == benchFixedN {
			continue // N=benchFixedN is part of the T sweep
		}
		b.Run(fmt.Sprintf("N=%d/T=%d", n, benchFixedT), func(b *testing.B) { benchRegisterWait(b, n, benchFixedT) })
	}
	for _, t := range benchSizesT {
		b.Run(fmt.Sprintf("N=%d/T=%d", benchFixedN, t), func(b *testing.B) { benchRegisterWait(b, benchFixedN, t) })
	}
}

// Register for an ad that is already in the cache: the duplicate check scans
// the topic list. The oldest entry is at the back of the list (worst case).
func BenchmarkRegisterDuplicate(b *testing.B) {
	for _, t := range benchSizesT {
		b.Run(fmt.Sprintf("N=%d/T=%d", benchFixedN, t), func(b *testing.B) {
			tab, _, topic, nodes := benchTable(benchFixedN, t, false)
			dup := nodes[0]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if tab.Register(dup, topic, 0) != 0 {
					b.Fatal("duplicate not detected")
				}
			}
		})
	}
}

// Register for a retry with enough credit: the ad is admitted. The new entry
// is removed again outside the timed region.
func benchRegisterAdmit(b *testing.B, n, t int) {
	tab, _, topic, _ := benchTable(n, t, false)
	pool := freshNodes(1024)
	credit := 1000 * time.Hour
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if tab.Register(pool[i%len(pool)], topic, credit) != 0 {
			b.Fatal("not admitted")
		}
		b.StopTimer()
		reg := tab.reg[topic].Front().Value.(*topicTableEntry)
		tab.remove(reg)
		tab.wt.removeReg(reg)
		b.StartTimer()
	}
}

func BenchmarkRegisterRetryAdmit(b *testing.B) {
	for _, n := range benchSizesN {
		if n < benchFixedT || n == benchFixedN {
			continue // N=benchFixedN is part of the T sweep
		}
		b.Run(fmt.Sprintf("N=%d/T=%d", n, benchFixedT), func(b *testing.B) { benchRegisterAdmit(b, n, benchFixedT) })
	}
	for _, t := range benchSizesT {
		b.Run(fmt.Sprintf("N=%d/T=%d", benchFixedN, t), func(b *testing.B) { benchRegisterAdmit(b, benchFixedN, t) })
	}
}

// RandomNodes with the production TOPICQUERY callback (v5_udp.go:handleTopicQuery).
func BenchmarkRandomNodes(b *testing.B) {
	requester := netip.MustParseAddr("93.184.216.34")
	check := func(n *enode.Node) bool { return netutil.CheckRelayAddr(requester, n.IPAddr()) == nil }
	run := func(b *testing.B, n, t int) {
		tab, _, topic, _ := benchTable(n, t, false)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if got := tab.RandomNodes(topic, 16, check); len(got) != min(t, 16) {
				b.Fatal(len(got))
			}
		}
	}
	for _, t := range append([]int{0}, benchSizesT...) {
		b.Run(fmt.Sprintf("N=%d/T=%d", benchFixedN, t), func(b *testing.B) { run(b, benchFixedN, t) })
	}
	for _, n := range benchSizesN {
		if n < benchFixedT || n == benchFixedN {
			continue // N=benchFixedN is part of the T sweep
		}
		b.Run(fmt.Sprintf("N=%d/T=%d", n, benchFixedT), func(b *testing.B) { run(b, n, benchFixedT) })
	}
}

// Expire in steady state: the table holds N=benchFixedN ads in cohorts of k
// inserted one second apart. Each iteration inserts a new cohort (untimed),
// advances the clock by one second and expires the oldest cohort (timed).
// ns/entry is reported as a custom metric.
func BenchmarkExpire(b *testing.B) {
	for _, k := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("N=%d/k=%d", benchFixedN, k), func(b *testing.B) {
			cohorts := benchFixedN / k
			r := mrand.New(mrand.NewSource(int64(k)))
			clock := new(mclock.Simulated)
			tab := NewTopicTable(Config{AdCacheSize: benchCapacity, AdLifetime: time.Duration(cohorts) * time.Second, Clock: clock})
			var topics [300]TopicID
			for j := range topics {
				topics[j][0], topics[j][1] = byte(j), byte(j>>8)
			}
			ring := make([]*enode.Node, benchFixedN+k)
			for j := range ring {
				ring[j] = benchNode(r, false)
			}
			next := 0
			addCohort := func() {
				for j := 0; j < k; j++ {
					tab.add(ring[next%len(ring)], topics[next%len(topics)])
					next++
				}
			}
			// Cohort c is added at c seconds and expires at c+cohorts seconds.
			for c := 0; c < cohorts; c++ {
				if c > 0 {
					clock.Run(time.Second)
				}
				addCohort()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				clock.Run(time.Second) // the oldest cohort is now due
				addCohort()
				b.StartTimer()
				tab.Expire()
			}
			b.StopTimer()
			if tab.Occupancy() != benchFixedN {
				b.Fatal("occupancy drifted", tab.Occupancy())
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(k), "ns/entry")
		})
	}
}

// IP-similarity scoring against trees of n addresses.
func BenchmarkIPScore(b *testing.B) {
	for _, shape := range []string{"diverse", "concentrated"} {
		for _, n := range []int{10, 100, 1000, 10000, 100000} {
			b.Run(fmt.Sprintf("shape=%s/N=%d", shape, n), func(b *testing.B) {
				r := mrand.New(mrand.NewSource(int64(n)))
				tree := newIPTree(32)
				var ip4 enr.IPv4
				for i := 0; i < n; i++ {
					nd := benchNode(r, shape == "concentrated")
					nd.Load(&ip4)
					tree.insert(net.IP(ip4[:]))
				}
				probe := make([]net.IP, 256)
				for i := range probe {
					benchNode(r, shape == "concentrated").Load(&ip4)
					probe[i] = append(net.IP{}, ip4[:]...)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tree.scoreNode(probe[i%len(probe)])
				}
			})
		}
	}
}

// RemoveNode for an ID that has no ads: a full scan of the cache.
func BenchmarkRemoveNode(b *testing.B) {
	for _, n := range benchSizesN {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			tab, _, _, _ := benchTable(n, 0, false)
			var absent enode.ID
			absent[0] = 0xff
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tab.RemoveNode(absent)
			}
			if tab.Occupancy() != n {
				b.Fatal("removed")
			}
		})
	}
}
