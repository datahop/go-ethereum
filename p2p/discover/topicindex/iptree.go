package topicindex

import (
	"fmt"
	"math"
	"net"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
)

type ipTree struct {
	root *ipTreeNode
	bits byte
}

type ipTreeNode struct {
	left    *ipTreeNode
	right   *ipTreeNode
	counter int
	bound   lowerBound
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
		//beyond this point the tree has no statistical power to
		// flag a bucket as overloaded
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
	sc, _ := it.scoreNode(ip)
	return sc
}

// computes the score that the addition of an IP would return, and also
// returns the longest-prefix-match node: the deepest already-existing node on the
// IP's path. The returned node is where the IP-component lower bound for ip is stored and read
func (it *ipTree) scoreNode(ip net.IP) (float64, *ipTreeNode) {
	ip = it.normIP(ip)
	sum := 0
	node := &it.root
	lpm := it.root
	rootCounter := float64(it.root.counter)
	effectiveDepth := byte(0)
	hitNil := false
	for depth := byte(0); depth < it.bits; depth++ {
		balanced := rootCounter / math.Pow(2, float64(depth+1))
		if balanced < 1 {
			break
		}

		effectiveDepth++
		// mimic the behaviour of insert() that creates new nodes when
		// adding an address and keeps increasing effectiveDepth.
		if hitNil {
			continue
		}

		if ipBit(ip, depth) {
			node = &(*node).left
		} else {
			node = &(*node).right
		}
		if *node == nil {
			// insert() would create a node here and continue, so we do
			// the same to make the score match.
			hitNil = true
			continue
		}
		n := *node
		lpm = n
		if float64(n.counter) > balanced {
			sum++
		}
	}
	return it.computeScore(sum, effectiveDepth), lpm
}

// pathFloor returns the largest still-effective lower bound stored on any
// existing node along the IP's path. A bound on an ancestor covers every
// address under its prefix, and later inserts can create nodes deeper than the
// one a bound was recorded on, so the whole path is walked rather than just
// checking the longest-prefix-match node.
func (it *ipTree) pathFloor(ip net.IP, now mclock.AbsTime) time.Duration {
	ip = it.normIP(ip)
	var floor time.Duration
	node := it.root
	for depth := byte(0); depth < it.bits; depth++ {
		if ipBit(ip, depth) {
			node = node.left
		} else {
			node = node.right
		}
		if node == nil {
			break
		}
		if rem := node.bound.remaining(now); rem > floor {
			floor = rem
		}
	}
	return floor
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
		// If this was the last IP in this node, remove the branch. This is also
		// what garbage-collects the lower bounds stored on the branch's nodes.
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
