package main

import (
	"bufio"
	"container/heap"
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/p2p/netutil"
)

type dataset struct {
	AssignmentMode   string `json:"ip_assignment_mode"`
	Source           string `json:"source"`
	SHA256           string `json:"sha256"`
	Rows             int    `json:"rows"`
	Distinct         int    `json:"distinct_ips"`
	AssignedDistinct int    `json:"assigned_distinct_ips"`
}

func readIPs(path, column string) ([]netip.Addr, dataset, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, dataset{}, err
	}
	meta := dataset{Source: path, SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
	var ips []netip.Addr
	appendIP := func(s string, line int) error {
		ip, err := netip.ParseAddr(strings.TrimSpace(s))
		if err != nil || !ip.Unmap().Is4() {
			return fmt.Errorf("%s: row %d: invalid IPv4 %q", path, line, s)
		}
		ips = append(ips, ip.Unmap())
		return nil
	}
	if strings.EqualFold(filepath.Ext(path), ".csv") {
		r := csv.NewReader(strings.NewReader(string(data)))
		header, err := r.Read()
		if err != nil {
			return nil, meta, err
		}
		index := -1
		for i, h := range header {
			if strings.TrimSpace(h) == column {
				index = i
			}
		}
		if index < 0 {
			return nil, meta, fmt.Errorf("CSV column %q missing", column)
		}
		for line := 2; ; line++ {
			row, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, meta, err
			}
			if err := appendIP(row[index], line); err != nil {
				return nil, meta, err
			}
		}
	} else {
		s := bufio.NewScanner(strings.NewReader(string(data)))
		for line := 1; s.Scan(); line++ {
			text := strings.TrimSpace(s.Text())
			if text == "" || strings.HasPrefix(text, "#") {
				continue
			}
			if err := appendIP(text, line); err != nil {
				return nil, meta, err
			}
		}
		if err := s.Err(); err != nil {
			return nil, meta, err
		}
	}
	distinct := make(map[netip.Addr]bool)
	for _, ip := range ips {
		distinct[ip] = true
	}
	meta.Rows, meta.Distinct = len(ips), len(distinct)
	return ips, meta, nil
}

type zipf struct{ cdf []float64 }

func newZipf(n int, alpha float64) zipf {
	z := zipf{make([]float64, n)}
	total := 0.0
	for i := range z.cdf {
		total += math.Pow(float64(i+1), -alpha)
		z.cdf[i] = total
	}
	for i := range z.cdf {
		z.cdf[i] /= total
	}
	z.cdf[n-1] = 1
	return z
}
func (z zipf) sample(r *rand.Rand) int { u := r.Float64(); return sort.SearchFloat64s(z.cdf, u) }

type advertiser struct {
	node    *enode.Node
	topic   int
	waiting bool
	first   mclock.AbsTime
	readyAt mclock.AbsTime
}
type wake struct {
	at mclock.AbsTime
	id int
}
type wakes []wake

func (h wakes) Len() int { return len(h) }
func (h wakes) Less(i, j int) bool {
	if h[i].at == h[j].at {
		return h[i].id < h[j].id
	}
	return h[i].at < h[j].at
}
func (h wakes) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *wakes) Push(v any)   { *h = append(*h, v.(wake)) }
func (h *wakes) Pop() any     { old := *h; v := old[len(old)-1]; *h = old[:len(old)-1]; return v }

type workload struct {
	ads        []advertiser
	topics     []topicindex.TopicID
	population []int
	ready      []int
	head       int
	active     wakes
	retries    wakes
	z          zipf
	rng        *rand.Rand
}

func population(c config) (*workload, dataset, error) {
	var ips []netip.Addr
	meta := dataset{Source: "synthetic", AssignmentMode: "synthetic_unique_ipv4"}
	var err error
	if c.IPFile != "" {
		ips, meta, err = readIPs(c.IPFile, c.IPColumn)
		if err != nil {
			return nil, meta, err
		}
		if len(ips) == 0 {
			return nil, meta, fmt.Errorf("%s: zero usable IPv4 rows", c.IPFile)
		}
		meta.AssignmentMode = "empirical_rows_with_replacement"

	} else {
		r := rand.New(rand.NewSource(c.Seed + 2))
		seen := make(map[netip.Addr]bool)
		for len(ips) < c.Advertisers {
			v := r.Uint32()
			ip := netip.AddrFrom4([4]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
			if seen[ip] || !ip.IsGlobalUnicast() || netutil.AddrIsLAN(ip) || netutil.AddrIsSpecialNetwork(ip) {
				continue
			}
			seen[ip] = true
			ips = append(ips, ip)
		}
		meta.Rows, meta.Distinct = len(ips), len(ips)
		hash := sha256.New()
		for _, ip := range ips {
			fmt.Fprintln(hash, ip)
		}
		meta.SHA256 = fmt.Sprintf("%x", hash.Sum(nil))
	}
	w := &workload{ads: make([]advertiser, c.Advertisers), topics: make([]topicindex.TopicID, c.Topics), population: make([]int, c.Topics), ready: make([]int, 0, c.Advertisers), active: make(wakes, 0, c.Advertisers), retries: make(wakes, 0, c.Advertisers), z: newZipf(c.Topics, c.Alpha), rng: rand.New(rand.NewSource(c.Seed + 3))}
	assigned := make(map[netip.Addr]bool)
	// Independent from topic assignment and runtime query randomness. Sample rows
	// in their original order so repeated rows retain their empirical weight.
	ipRNG := rand.New(rand.NewSource(c.Seed + 2))
	r := rand.New(rand.NewSource(c.Seed + 1))
	for i := range w.topics {
		w.topics[i] = sha256.Sum256([]byte(fmt.Sprintf("registrarbench/topic/%d", i)))
	}
	for i := range w.ads {
		index := i
		if c.IPFile != "" {
			index = ipRNG.Intn(len(ips))
		}
		ip := ips[index]
		var record enr.Record
		record.Set(enr.IPv4(ip.AsSlice()))
		record.Set(enr.UDP(30303))
		digest := sha256.Sum256([]byte(fmt.Sprintf("registrarbench/key/%d/%d", c.Seed, i)))
		key, err := crypto.ToECDSA(digest[:])
		if err != nil {
			return nil, meta, err
		}
		if err := enode.SignV4(&record, key); err != nil {
			return nil, meta, err
		}
		node, err := enode.New(enode.ValidSchemes, &record)
		if err != nil {
			return nil, meta, err
		}
		topic := w.z.sample(r)
		w.ads[i] = advertiser{node: node, topic: topic}
		w.population[topic]++
		w.ready = append(w.ready, i)
		assigned[ip] = true
	}
	r.Shuffle(len(w.ready), func(i, j int) { w.ready[i], w.ready[j] = w.ready[j], w.ready[i] })
	meta.AssignedDistinct = len(assigned)
	return w, meta, nil
}

// Active entries are released only after TopicTable.Expire has run. For realtime,
// release is conservatively scheduled at method completion + lifetime, no earlier
// than the table's actual insertion expiry. Simulated start/completion are equal.
func (w *workload) release(now mclock.AbsTime) {
	for len(w.active) > 0 && w.active[0].at <= now {
		item := heap.Pop(&w.active).(wake)
		w.enqueueFresh(item.id, item.at)
	}
}
func (w *workload) enqueueFresh(id int, at mclock.AbsTime) {
	a := &w.ads[id]
	a.waiting = false
	a.first = 0
	a.readyAt = at
	if len(w.ready) == cap(w.ready) {
		copy(w.ready, w.ready[w.head:])
		w.ready = w.ready[:len(w.ready)-w.head]
		w.head = 0
	}
	w.ready = append(w.ready, id)
}
func (w *workload) next() (int, bool) {
	if w.head == len(w.ready) {
		return 0, false
	}
	id := w.ready[w.head]
	w.head++
	return id, true
}
func (w *workload) finish(id int, issued, completed mclock.AbsTime, wait, lifetime time.Duration) error {
	a := &w.ads[id]
	if wait > 0 {
		if issued.Add(wait) < issued {
			return fmt.Errorf("retry due-time overflow for advertiser %d: %s", id, wait)
		}
		if !a.waiting {
			a.first = issued
			a.waiting = true
		}
		heap.Push(&w.retries, wake{at: issued.Add(wait), id: id})
	} else {
		a.waiting = false
		heap.Push(&w.active, wake{at: completed.Add(lifetime), id: id})
	}
	return nil
}
func arrival(index uint64, rate float64) time.Duration {
	if rate == 0 {
		return time.Duration(math.MaxInt64)
	}
	ns := float64(index) * float64(time.Second) / rate
	if ns >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(ns)
}
func ipv4(n *enode.Node) bool { return n.IPAddr().Is4() }
