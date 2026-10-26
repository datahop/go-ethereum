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

// These tests compare the registrar against reference implementations written
// directly from discv5-theory.md ("Waiting-Time Function", "IP Similarity Tree").
// They characterise the known deviations: each test passes with the current code
// and fails if either the code or the list of documented deviations changes.

import (
	"encoding/binary"
	"math"
	mrand "math/rand"
	"net"
	"net/netip"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/netutil"
)

const (
	specPocc = 10
	specG    = 1e-7
)

// specIPScore is score(x) from the spec. Prefix lengths k with 2^k <= N are
// considered; includeZero selects whether k = 0 counts towards D (it can never
// contribute a point since c_0 = N).
func specIPScore(ips []net.IP, x net.IP, includeZero bool) float64 {
	n := len(ips)
	if n == 0 {
		return 0
	}
	xv := binary.BigEndian.Uint32(x.To4())
	var d, points int
	start := 1
	if includeZero {
		start = 0
	}
	for k := start; k <= 32 && 1<<k <= n; k++ {
		d++
		mask := uint32(0)
		if k > 0 {
			mask = ^uint32(0) << (32 - k)
		}
		ck := 0
		for _, ip := range ips {
			if binary.BigEndian.Uint32(ip.To4())&mask == xv&mask {
				ck++
			}
		}
		if float64(ck) > float64(n)/math.Pow(2, float64(k)) {
			points++
		}
	}
	if d == 0 {
		return 0
	}
	return float64(points) / float64(d)
}

// specWait is w(ad) from the spec, in seconds.
func specWait(e float64, capacity, c, cs int, score float64) float64 {
	service := 0.0
	if c > 0 {
		service = float64(cs) / float64(c)
	}
	return e / math.Pow(1-float64(c)/float64(capacity), specPocc) * (service + score + specG)
}

// implementedWait is specWait with the deviations documented in
// docs/registrar-overhead applied: base scaled by 0.1, service term T/(N+1)
// instead of c(s)/c, and no G term. Lower bounds are not included.
func implementedWait(e float64, capacity, c, cs int, score float64) float64 {
	base := waitTimeBaseModifier * e / math.Pow(1-float64(c)/float64(capacity), occupancyExp)
	return base*float64(cs)/float64(c+1) + base*score
}

func randomPublicIPv4(r *mrand.Rand) net.IP {
	for {
		ip := make(net.IP, 4)
		binary.BigEndian.PutUint32(ip, r.Uint32())
		if !netutil.IsLAN(ip) && !netutil.AddrIsSpecialNetwork(netip.AddrFrom4([4]byte(ip))) {
			return ip
		}
	}
}

// concentratedIPv4 draws addresses from a few /16 prefixes.
func concentratedIPv4(r *mrand.Rand, prefixes []net.IP) net.IP {
	p := prefixes[r.Intn(len(prefixes))]
	return net.IP{p[0], p[1], byte(r.Intn(256)), byte(1 + r.Intn(254))}
}

// ipTreeTruth compares every tree counter with the true number of present
// addresses under its prefix and returns the number of mismatching vertices.
func ipTreeTruth(tree *ipTree, ips []net.IP) (vertices, mismatched int) {
	var walk func(n *ipTreeNode, prefix uint32, depth int)
	walk = func(n *ipTreeNode, prefix uint32, depth int) {
		if n == nil {
			return
		}
		vertices++
		mask := uint32(0)
		if depth > 0 {
			mask = ^uint32(0) << (32 - depth)
		}
		count := 0
		for _, ip := range ips {
			if binary.BigEndian.Uint32(ip.To4())&mask == prefix {
				count++
			}
		}
		if count != n.counter {
			mismatched++
		}
		if depth < 32 {
			walk(n.left, prefix|1<<(31-depth), depth+1) // ipBit set -> left
			walk(n.right, prefix, depth+1)
		}
	}
	walk(tree.root, 0, 0)
	return vertices, mismatched
}

// ipScoreDeviation drives a tree through growth to n addresses followed by
// FIFO churn, then compares scores against the spec reference.
func ipScoreDeviation(r *mrand.Rand, n, churn int, gen func() net.IP) (scoreDiff, queries, vertices, mismatched int, meanAbs float64) {
	tree := newIPTree(32)
	var ips []net.IP
	for i := 0; i < n+churn; i++ {
		if len(ips) == n {
			tree.remove(ips[0])
			ips = ips[1:]
		}
		ip := gen()
		tree.insert(ip)
		ips = append(ips, ip)
	}
	for q := 0; q < 200; q++ {
		x := gen()
		if q%2 == 0 {
			x = ips[r.Intn(len(ips))]
		}
		got, want := tree.score(x), specIPScore(ips, x, false)
		queries++
		if !almostEQ(got, want) {
			scoreDiff++
			meanAbs += math.Abs(got - want)
		}
	}
	if queries > 0 {
		meanAbs /= float64(queries)
	}
	vertices, mismatched = ipTreeTruth(tree, ips)
	return
}

// TestSpecIPScoreDeviation measures how far production IP scores are from the
// spec. ipTree.insert stops descending at depth floor(log2(N)) for the N at
// insertion time, so addresses inserted while the tree was small are missing
// from deeper counters; remove walks the full existing path and decrements
// vertices the address never incremented. Counters therefore depend on
// insertion history and drift from the cache contents under churn.
func TestSpecIPScoreDeviation(t *testing.T) {
	r := mrand.New(mrand.NewSource(1))
	prefixes := []net.IP{randomPublicIPv4(r), randomPublicIPv4(r), randomPublicIPv4(r)}
	diverse := func() net.IP { return randomPublicIPv4(r) }
	concentrated := func() net.IP {
		if r.Intn(4) == 0 {
			return randomPublicIPv4(r)
		}
		return concentratedIPv4(r, prefixes)
	}
	for _, tc := range []struct {
		name     string
		n, churn int
		gen      func() net.IP
	}{
		{"diverse/growth-1000", 1000, 0, diverse},
		{"diverse/churn-1000x10", 1000, 10000, diverse},
		{"concentrated/growth-1000", 1000, 0, concentrated},
		{"concentrated/churn-1000x10", 1000, 10000, concentrated},
	} {
		d, q, v, m, mean := ipScoreDeviation(r, tc.n, tc.churn, tc.gen)
		t.Logf("%-28s scores differing from spec: %3d/%d (mean |diff| %.3f); tree vertices with wrong counter: %d/%d", tc.name, d, q, mean, m, v)
	}
}

// TestIPTreeCountersMatchContents is the strict form of the check above. It
// fails with the current ipTree and is skipped unless TOPDISC_SPEC_STRICT=1.
func TestIPTreeCountersMatchContents(t *testing.T) {
	if os.Getenv("TOPDISC_SPEC_STRICT") == "" {
		t.Skip("known deviation, see TestSpecIPScoreDeviation; set TOPDISC_SPEC_STRICT=1 to run")
	}
	r := mrand.New(mrand.NewSource(1))
	d, q, v, m, _ := ipScoreDeviation(r, 1000, 10000, func() net.IP { return randomPublicIPv4(r) })
	if d != 0 || m != 0 {
		t.Fatalf("%d/%d scores differ from spec, %d/%d tree counters wrong", d, q, m, v)
	}
}

// TestSpecWaitTimeDeviations checks that the un-floored waiting-time components
// equal the spec formula with exactly the documented deviations applied.
func TestSpecWaitTimeDeviations(t *testing.T) {
	r := mrand.New(mrand.NewSource(2))
	const capacity = 1000
	e := (15 * time.Minute).Seconds()
	var ratios []float64
	var specAboveE, implAboveE, implUnderFloor, n int
	for trial := 0; trial < 200; trial++ {
		clock := new(mclock.Simulated)
		tab := NewTopicTable(Config{AdCacheSize: capacity, Clock: clock})
		topics := make([]TopicID, 1+r.Intn(50))
		for i := range topics {
			r.Read(topics[i][:])
		}
		c := r.Intn(capacity * 9 / 10)
		var ips []net.IP
		for i := 0; i < c; i++ {
			ip := randomPublicIPv4(r)
			tab.add(nodeWithIP(ip), topics[r.Intn(len(topics))])
			ips = append(ips, ip)
		}
		for q := 0; q < 10; q++ {
			ip := randomPublicIPv4(r)
			if q%2 == 0 && c > 0 {
				ip = ips[r.Intn(c)]
			}
			topic := topics[r.Intn(len(topics))]
			_, comps := tab.waitTime(nodeWithIP(ip), topic, clock.Now())
			got := comps.serviceSecs + comps.ipSecs
			// The formula is checked with the tree's own score; the IP score
			// deviation is measured separately in TestSpecIPScoreDeviation.
			score := tab.wt.ipv4.score(ip)
			want := implementedWait(e, capacity, c, tab.topicSize(topic), score)
			if math.Abs(got-want) > 1e-9*math.Max(1, want) {
				t.Fatalf("c=%d T=%d: implemented wait %v, expected %v", c, tab.topicSize(topic), got, want)
			}
			spec := specWait(e, capacity, c, tab.topicSize(topic), specIPScore(ips, ip, false))
			ratios = append(ratios, got/spec)
			n++
			if spec > e {
				specAboveE++
			}
			if got > e {
				implAboveE++
			}
			if got <= topicTableWaitTimeFloor.Seconds() {
				implUnderFloor++
			}
		}
	}
	sort.Float64s(ratios)
	t.Logf("implemented/spec wait ratio: min %.4f median %.4f max %.4f", ratios[0], ratios[len(ratios)/2], ratios[len(ratios)-1])
	t.Logf("of %d quotes: spec > E in %d, implemented > E in %d, implemented <= 1s floor in %d", n, specAboveE, implAboveE, implUnderFloor)
}

// TestSpecNoGTerm: a diverse ad into an empty cache waits zero (spec: E*G > 0).
func TestSpecNoGTerm(t *testing.T) {
	clock := new(mclock.Simulated)
	tab := NewTopicTable(Config{AdCacheSize: 1000, Clock: clock})
	var topic TopicID
	if w := tab.WaitTime(newNode(), topic); w != 0 {
		t.Fatalf("empty-cache wait = %v, want 0", w)
	}
	if spec := specWait(900, 1000, 0, 0, 0); spec <= 0 {
		t.Fatalf("spec wait = %v, want > 0", spec)
	}
}

// TestSpecAdmissionFloor: required waits up to topicTableWaitTimeFloor are
// admitted without a ticket. The spec admits only when the remaining wait is <= 0.
func TestSpecAdmissionFloor(t *testing.T) {
	clock := new(mclock.Simulated)
	tab := NewTopicTable(Config{AdCacheSize: 1000000, Clock: clock})
	var topic, other TopicID
	other[0] = 1
	for i := 0; i < 199; i++ {
		tab.add(newNode(), other)
	}
	tab.add(newNode(), topic)
	n := newNode() // no IP: IP score 0
	w := tab.WaitTime(n, topic)
	if w <= 0 || w > topicTableWaitTimeFloor {
		t.Fatalf("setup: required wait %v not in (0, %v]", w, topicTableWaitTimeFloor)
	}
	if got := tab.Register(n, topic, 0); got != 0 {
		t.Fatalf("Register returned %v, want immediate admission", got)
	}
}

// TestSpecNoWaitCap: the registrar quotes single waits far above E.
func TestSpecNoWaitCap(t *testing.T) {
	clock := new(mclock.Simulated)
	tab := NewTopicTable(Config{AdCacheSize: 1000, Clock: clock})
	r := mrand.New(mrand.NewSource(3))
	var topic TopicID
	for i := 0; i < 320; i++ {
		tab.add(nodeWithIP(randomPublicIPv4(r)), topic)
	}
	w := tab.Register(nodeWithIP(randomPublicIPv4(r)), topic, 0)
	if w <= tab.AdLifetime() {
		t.Fatalf("wait %v at 32%% occupancy, expected > E = %v", w, tab.AdLifetime())
	}
	t.Logf("single quoted wait at 32%% occupancy, single topic: %v (E = %v)", w, tab.AdLifetime())
}

// TestSpecLANAddressesInTree: LAN addresses are not scored but are inserted into
// the IP tree, so they increase N for scoring public addresses.
func TestSpecLANAddressesInTree(t *testing.T) {
	tab := NewTopicTable(Config{AdCacheSize: 1000, Clock: new(mclock.Simulated)})
	var topic TopicID
	tab.add(nodeWithIP(net.IP{192, 168, 0, 1}), topic)
	if got := tab.wt.ipv4.count(); got != 1 {
		t.Fatalf("IPv4 tree count = %d, want 1 (LAN address inserted)", got)
	}
}

func TestSpecDefaultCapacity(t *testing.T) {
	if got := (Config{}).withDefaults().AdCacheSize; got != 5000 {
		t.Fatalf("default AdCacheSize = %d; spec default C is 1000", got)
	}
}
