package topicindex

import (
	"fmt"
	"math"
	"net"
)

type ipTree struct {
	root *ipTreeNode
	bits byte
}

type ipTreeNode struct {
	left    *ipTreeNode
	right   *ipTreeNode
	counter int
}

func newIPTree(bits byte) *ipTree {
	if bits != 32 && bits != 128 {
		panic(fmt.Errorf("invalid ipTree bits %d", bits))
	}
	return &ipTree{new(ipTreeNode), bits}
}

func (it *ipTree) normIP(ip net.IP) net.IP {
	switch it.bits {
	case 32:
		ipv4 := ip.To4()
		if ipv4 == nil {
			panic("ipTree(bits=32) operation on invalid address")
		}
		return ipv4
	case 128:
		ipv6 := ip.To16()
		if ipv6 == nil {
			panic("ipTree(bits=128) operation on invalid address")
		}
		return ipv6
	default:
		panic(fmt.Errorf("invalid ipTree bits %d", it.bits))
	}
}

// insert adds an IP address to the tree and returns the similarity score.
func (it *ipTree) insert(ip net.IP) float64 {
	ip = it.normIP(ip)
	sum := 0
	node := &it.root
	rootCounter := float64(it.root.counter)
	it.root.counter++
	effectiveDepth := byte(0)
	for depth := byte(0); depth < it.bits; depth++ {
		// Stop once the expected bucket occupancy drops below 1: beyond
		// this point the tree has no statistical power to flag a bucket
		// as overloaded, so further depth shouldn't dilute the score.
		balanced := rootCounter / math.Pow(2, float64(depth+1))
		if balanced < 1 {
			break
		}
		effectiveDepth++

		if ipBit(ip, depth) {
			node = &(*node).left
		} else {
			node = &(*node).right
		}
		if *node == nil {
			*node = new(ipTreeNode)
		}
		n := *node
		if float64(n.counter) > balanced {
			sum++
		}
		n.counter++
	}
	return it.computeScore(sum, effectiveDepth)
}

// score computes the score that the addition of an IP would return.
func (it *ipTree) score(ip net.IP) float64 {
	ip = it.normIP(ip)
	sum := 0
	node := &it.root
	rootCounter := float64(it.root.counter)
	effectiveDepth := byte(0)
	hitNil := false
	for depth := byte(0); depth < it.bits; depth++ {
		balanced := rootCounter / math.Pow(2, float64(depth+1))
		if balanced < 1 {
			break
		}
		// effectiveDepth is driven solely by `balanced`, exactly like
		// insert(). It must NOT be gated on node existence: insert()
		// creates missing nodes and keeps walking as long as balanced
		// stays >= 1, so score() must count those same depths as
		// effective even though it can't find an overloaded counter
		// there (an unvisited branch has counter 0, never > balanced
		// anyway, so `sum` is unaffected — only the depth bookkeeping
		// needs to match).
		effectiveDepth++

		if hitNil {
			// Already past the unexplored frontier: there is no data
			// to inspect, but we still must advance effectiveDepth to
			// mirror insert(). Skip node traversal, sum can't grow.
			continue
		}

		if ipBit(ip, depth) {
			node = &(*node).left
		} else {
			node = &(*node).right
		}
		if *node == nil {
			// Frontier of explored tree. insert() would create a node
			// here and continue; we have no node to inspect, so just
			// remember this and keep advancing effectiveDepth above.
			hitNil = true
			continue
		}
		n := *node
		if float64(n.counter) > balanced {
			sum++
		}
	}
	return it.computeScore(sum, effectiveDepth)
}

// remove removes an IP from the tree.
func (it *ipTree) remove(ip net.IP) {
	ip = it.normIP(ip)
	node := &it.root
	it.root.counter--
	for depth := byte(0); depth < it.bits; depth++ {
		if ipBit(ip, depth) {
			node = &(*node).left
		} else {
			node = &(*node).right
		}
		if *node == nil {
			return
		}
		n := *node
		n.counter--
		// If this was the last IP in this node, remove the branch.
		if n.counter == 0 {
			*node = nil
			return
		}
	}
}

// count returns the total number of IP addresses in tree.
func (it *ipTree) count() int {
	if it.root == nil {
		return 0
	}
	return it.root.counter
}

// computeScore normalizes sum by the number of tree levels that were
// actually evaluated (effectiveDepth), rather than the tree's fixed bit
// width. This keeps IPv4 (32-bit) and IPv6 (128-bit) scores on a
// comparable scale for the same population size and avoids diluting the
// score with unpopulated, statistically meaningless depth.
func (it *ipTree) computeScore(sum int, effectiveDepth byte) float64 {
	c := it.count()
	if c == 0 {
		return 0
	}
	if effectiveDepth == 0 {
		return 0
	}
	sc := float64(sum) / float64(effectiveDepth)
	return sc
}

func ipBit(ip net.IP, i byte) bool {
	return ip[i/8]&(1<<(7-i%8)) != 0
}