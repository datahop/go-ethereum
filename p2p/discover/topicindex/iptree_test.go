package topicindex

import (
	"crypto/rand"
	"fmt"
	"math"
	mrand "math/rand"
	"net"
	"testing"
)

type treeAddOp struct {
	ip    string
	score float64
}

var ipv4TestOps = []treeAddOp{
	{"1.2.3.4", 0.0},
	{"1.2.3.5", 0.0},
	{"1.2.3.6", 0.0},
	{"1.2.3.4", 0.0},
	{"1.2.3.4", 0.0},
	{"1.2.3.4", 0.5},
	{"1.1.1.1", 1.0},
	{"8.8.8.8", 1.0},
}

var ipv6TestOps = []treeAddOp{
	{"fe80::716b:dafc:774e:a826", 0.0},
	{"fe80::716b:dafc:774e:a827", 0.0},
	{"fe80::716b:dafc:774e:a828", 0.0},
	{"abc2:2345:7a7b:774e:a828:f184:ee25:4545", 0.0},
}

func TestIPTreeInsert(t *testing.T) {
	t.Run("IPv4", func(t *testing.T) {
		it := newIPTree(32)
		for i, add := range ipv4TestOps {
			sc, _ := it.insert(net.ParseIP(add.ip))
			if !almostEQ(sc, add.score) {
				t.Errorf("score %g not equal to expected score %g when adding IP %s", sc, add.score, add.ip)
			}
			if c := it.count(); c != i+1 {
				t.Errorf("wrong count %d at op %d", c, i)
			}
		}
	})
	t.Run("IPv6", func(t *testing.T) {
		it := newIPTree(128)
		for i, add := range ipv6TestOps {
			sc, _ := it.insert(net.ParseIP(add.ip))
			if !almostEQ(sc, add.score) {
				t.Errorf("score %g not equal to expected score %g when adding IP %s", sc, add.score, add.ip)
			}
			if c := it.count(); c != i+1 {
				t.Errorf("wrong count %d at op %d", c, i)
			}
		}
	})
}

func TestIPTreeScore(t *testing.T) {
	randomAdd := func(t *testing.T, it *ipTree) {
		ip := make(net.IP, it.bits/8)
		rand.Read(ip)
		sc1 := it.score(ip)
		sc2, _ := it.insert(ip)
		if sc1 != sc2 {
			t.Fatalf("score() result %f not equal insert() result %f, IP: %v", sc1, sc2, ip)
		}
	}

	t.Run("IPv4", func(t *testing.T) {
		it := newIPTree(32)
		for i := 0; i < 1000; i++ {
			randomAdd(t, it)
		}
	})
	t.Run("IPv6", func(t *testing.T) {
		it := newIPTree(128)
		for i := 0; i < 1000; i++ {
			randomAdd(t, it)
		}
	})
}

func TestIPTreeRemove(t *testing.T) {
	it := newIPTree(32)
	levels := make([]byte, len(ipv4TestOps))
	for i, op := range ipv4TestOps {
		_, levels[i] = it.insert(net.ParseIP(op.ip))
	}

	// Remove the IPs backwards, and check that score() matches after removing.
	for i := len(ipv4TestOps) - 1; i >= 0; i-- {
		op := ipv4TestOps[i]
		ip := net.ParseIP(op.ip)

		it.remove(ip, levels[i])
		if sc := it.score(ip); !almostEQ(sc, op.score) {
			t.Errorf("score %g does not match expected score %g after removing %v", sc, op.score, ip)
		}
		if c := it.count(); c != i {
			t.Errorf("wrong count %d at op %d", c, i)
		}
	}
}

// TestIPTreeRemoveShallowInsert checks that removing an IP inserted while the
// tree was shallow leaves the levels it never incremented untouched.
func TestIPTreeRemoveShallowInsert(t *testing.T) {
	it := newIPTree(32)
	var entries []ipTreeEntry
	add := func(ip string) ipTreeEntry {
		e := ipTreeEntry{ip: net.ParseIP(ip)}
		_, e.levels = it.insert(e.ip)
		entries = append(entries, e)
		return e
	}
	for i := 1; i <= 4; i++ {
		add(fmt.Sprintf("200.0.0.%d", i))
	}
	a := add("10.0.0.1")
	for i := 1; i <= 60; i++ {
		add(fmt.Sprintf("200.0.1.%d", i))
	}
	b := add("10.0.0.2")
	if b.levels <= a.levels {
		t.Fatalf("setup failed: B levels %d not deeper than A levels %d", b.levels, a.levels)
	}

	// Snapshot the nodes on B's path below A's insert depth.
	var deep []*ipTreeNode
	var before []int
	node := it.root
	for depth := byte(0); depth < b.levels; depth++ {
		if ipBit(b.ip.To4(), depth) {
			node = node.left
		} else {
			node = node.right
		}
		if depth >= a.levels {
			deep = append(deep, node)
			before = append(before, node.counter)
		}
	}

	it.remove(a.ip, a.levels)
	entries = append(entries[:4], entries[5:]...)

	for i, n := range deep {
		if n.counter != before[i] {
			t.Errorf("level %d counter changed from %d to %d", int(a.levels)+i+1, before[i], n.counter)
		}
	}
	checkIPTreeCounts(t, it, entries)
}

// TestIPTreeRandomChurn checks that the tree counters stay exact under a random
// sequence of inserts and removes, and that the tree is empty at the end.
func TestIPTreeRandomChurn(t *testing.T) {
	for _, bits := range []byte{32, 128} {
		t.Run(fmt.Sprintf("bits=%d", bits), func(t *testing.T) {
			rng := mrand.New(mrand.NewSource(1))
			it := newIPTree(bits)
			// Draw IPs from a few clustered prefixes so insert depths vary
			// within the same branch.
			randomIP := func() net.IP {
				ip := make(net.IP, bits/8)
				rng.Read(ip)
				ip[0] = byte(rng.Intn(4))
				if rng.Intn(2) == 0 {
					ip[1] = 0
				}
				return ip
			}
			var entries []ipTreeEntry
			for i := 0; i < 3000; i++ {
				insertProb := 60
				if len(entries) > 300 {
					insertProb = 40
				}
				if len(entries) == 0 || rng.Intn(100) < insertProb {
					e := ipTreeEntry{ip: randomIP()}
					_, e.levels = it.insert(e.ip)
					entries = append(entries, e)
				} else {
					j := rng.Intn(len(entries))
					it.remove(entries[j].ip, entries[j].levels)
					entries = append(entries[:j], entries[j+1:]...)
				}
				checkIPTreeCounts(t, it, entries)
			}
			for len(entries) > 0 {
				j := rng.Intn(len(entries))
				it.remove(entries[j].ip, entries[j].levels)
				entries = append(entries[:j], entries[j+1:]...)
				checkIPTreeCounts(t, it, entries)
			}
			if it.count() != 0 || it.root.left != nil || it.root.right != nil {
				t.Fatalf("tree not empty: count %d", it.count())
			}
		})
	}
}

type ipTreeEntry struct {
	ip     net.IP
	levels byte
}

// checkIPTreeCounts verifies that every node's counter equals the number of
// inserted IPs under its prefix whose insert reached the node's level, and
// that no other nodes exist.
func checkIPTreeCounts(t *testing.T, it *ipTree, entries []ipTreeEntry) {
	t.Helper()
	want := make(map[string]int)
	for _, e := range entries {
		ip := it.normIP(e.ip)
		prefix := make([]byte, 0, e.levels)
		for depth := byte(0); depth < e.levels; depth++ {
			if ipBit(ip, depth) {
				prefix = append(prefix, '1')
			} else {
				prefix = append(prefix, '0')
			}
			want[string(prefix)]++
		}
	}
	have := make(map[string]int)
	var walk func(n *ipTreeNode, prefix string)
	walk = func(n *ipTreeNode, prefix string) {
		if n == nil {
			return
		}
		if prefix != "" {
			have[prefix] = n.counter
		}
		walk(n.left, prefix+"1")
		walk(n.right, prefix+"0")
	}
	walk(it.root, "")

	if it.count() != len(entries) {
		t.Fatalf("root counter %d, want %d", it.count(), len(entries))
	}
	for prefix, w := range want {
		if h, ok := have[prefix]; !ok {
			t.Fatalf("node /%d %s missing, want counter %d", len(prefix), prefix, w)
		} else if h != w {
			t.Fatalf("node /%d %s counter %d, want %d", len(prefix), prefix, h, w)
		}
	}
	for prefix, h := range have {
		if _, ok := want[prefix]; !ok {
			t.Fatalf("unexpected node /%d %s with counter %d", len(prefix), prefix, h)
		}
	}
}

func almostEQ(a, b float64) bool {
	return math.Abs(a-b) < 1e-08
}
