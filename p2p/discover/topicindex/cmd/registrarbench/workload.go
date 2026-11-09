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
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/p2p/netutil"
	"github.com/ethereum/go-ethereum/rlp"
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

// advertiser is one fixed identity of the population. Its client-side attempt
// state mirrors topicindex.Registration: 'first' is the registrar-local time of
// the first ticket (the registrar credits now-first) and 'quoteSum' is the
// client's RegAttempt.totalWaitTime, the sum of the waits it was quoted.
type advertiser struct {
	node     *enode.Node
	enc      []byte // RLP-encoded record, decoded afresh per request when ENR copies are enabled
	topic    int
	waiting  bool
	first    mclock.AbsTime
	quoteSum time.Duration
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

// workload holds the fixed advertiser population. An advertiser is in exactly
// one of three places: the ready FIFO (eligible for a fresh arrival), the
// retry heap (waiting with a ticket) or the active heap (admitted, until its
// ad has expired from the table).
type workload struct {
	ads        []advertiser
	topics     []topicindex.TopicID
	population []int
	ready      []int
	head       int
	active     wakes
	retries    wakes
	z          zipf
	rng        *rand.Rand // query topic selection
}

const (
	ipDistEmpirical    = "empirical"
	ipDistDiverse      = "diverse"
	ipDistConcentrated = "concentrated"
)

func publicIPv4(r *rand.Rand) netip.Addr {
	for {
		v := r.Uint32()
		ip := netip.AddrFrom4([4]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
		if ip.IsGlobalUnicast() && !netutil.AddrIsLAN(ip) && !netutil.AddrIsSpecialNetwork(ip) {
			return ip
		}
	}
}

// addresses returns one IPv4 per advertiser for the configured distribution.
func addresses(c config) ([]netip.Addr, dataset, error) {
	r := rand.New(rand.NewSource(c.Seed + 2))
	out := make([]netip.Addr, c.Advertisers)
	assigned := make(map[netip.Addr]bool)
	var meta dataset
	switch c.ipDist() {
	case ipDistEmpirical:
		ips, m, err := readIPs(c.IPFile, c.IPColumn)
		if err != nil {
			return nil, m, err
		}
		if len(ips) == 0 {
			return nil, m, fmt.Errorf("%s: zero usable IPv4 rows", c.IPFile)
		}
		meta = m
		meta.AssignmentMode = "empirical_rows_with_replacement"
		// Rows are sampled in file order so repeated rows keep their weight.
		for i := range out {
			out[i] = ips[r.Intn(len(ips))]
		}
	case ipDistDiverse:
		meta = dataset{Source: "synthetic", AssignmentMode: "synthetic_unique_ipv4"}
		seen := make(map[netip.Addr]bool)
		for i := range out {
			ip := publicIPv4(r)
			for seen[ip] {
				ip = publicIPv4(r)
			}
			seen[ip] = true
			out[i] = ip
		}
	case ipDistConcentrated:
		meta = dataset{Source: "synthetic", AssignmentMode: fmt.Sprintf("synthetic_%d_slash16", c.IPPrefixes)}
		prefixes := make([]netip.Addr, c.IPPrefixes)
		for i := range prefixes {
			prefixes[i] = publicIPv4(r)
		}
		for i := range out {
			p := prefixes[r.Intn(len(prefixes))].As4()
			out[i] = netip.AddrFrom4([4]byte{p[0], p[1], byte(r.Intn(256)), byte(1 + r.Intn(254))})
		}
	default:
		return nil, meta, fmt.Errorf("unknown IP distribution %q", c.IPDist)
	}
	if meta.Source == "synthetic" {
		hash := sha256.New()
		for _, ip := range out {
			fmt.Fprintln(hash, ip)
		}
		meta.SHA256 = fmt.Sprintf("%x", hash.Sum(nil))
		meta.Rows = len(out)
	}
	for _, ip := range out {
		assigned[ip] = true
	}
	if meta.Source == "synthetic" {
		meta.Distinct = len(assigned)
	}
	meta.AssignedDistinct = len(assigned)
	return out, meta, nil
}

func population(c config) (*workload, dataset, error) {
	ips, meta, err := addresses(c)
	if err != nil {
		return nil, meta, err
	}
	w := &workload{ads: make([]advertiser, c.Advertisers), topics: make([]topicindex.TopicID, c.Topics), population: make([]int, c.Topics), ready: make([]int, 0, c.Advertisers), active: make(wakes, 0, c.Advertisers), retries: make(wakes, 0, c.Advertisers), z: newZipf(c.Topics, c.Alpha), rng: rand.New(rand.NewSource(c.Seed + 3))}
	r := rand.New(rand.NewSource(c.Seed + 1))
	for i := range w.topics {
		w.topics[i] = sha256.Sum256([]byte(fmt.Sprintf("registrarbench/topic/%d", i)))
	}
	// Topics are drawn sequentially; signing is deterministic per index and
	// runs in parallel.
	for i := range w.ads {
		topic := w.z.sample(r)
		w.ads[i].topic = topic
		w.population[topic]++
		w.ready = append(w.ready, i)
	}
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
		workers  = runtime.GOMAXPROCS(0)
	)
	for k := 0; k < workers; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			for i := k; i < len(w.ads); i += workers {
				node, enc, err := signedNode(c.Seed, i, ips[i], c.ENRPad)
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
				w.ads[i].node, w.ads[i].enc = node, enc
			}
		}(k)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, meta, firstErr
	}
	r.Shuffle(len(w.ready), func(i, j int) { w.ready[i], w.ready[j] = w.ready[j], w.ready[i] })
	return w, meta, nil
}

// signedNode creates advertiser i's deterministic, validly signed record.
// A positive pad adds an opaque entry of that many bytes, used to approach
// the 300-byte record size limit.
func signedNode(seed int64, i int, ip netip.Addr, pad int) (*enode.Node, []byte, error) {
	var record enr.Record
	record.Set(enr.IPv4(ip.AsSlice()))
	record.Set(enr.UDP(30303))
	if pad > 0 {
		record.Set(enr.WithEntry("zz", make([]byte, pad)))
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("registrarbench/key/%d/%d", seed, i)))
	key, err := crypto.ToECDSA(digest[:])
	if err != nil {
		return nil, nil, err
	}
	if err := enode.SignV4(&record, key); err != nil {
		return nil, nil, err
	}
	node, err := enode.New(enode.ValidSchemes, &record)
	if err != nil {
		return nil, nil, err
	}
	enc, err := rlp.EncodeToBytes(&record)
	return node, enc, err
}

// decodeNode returns an independent copy of the advertiser's record, as the
// production REGTOPIC handler decodes one from every request.
func decodeNode(enc []byte) (*enode.Node, error) {
	var r enr.Record
	if err := rlp.DecodeBytes(enc, &r); err != nil {
		return nil, err
	}
	return enode.New(enode.ValidSchemes, &r)
}

// release returns advertisers whose ads have expired to the ready FIFO.
func (w *workload) release(now mclock.AbsTime) {
	for len(w.active) > 0 && w.active[0].at <= now {
		item := heap.Pop(&w.active).(wake)
		w.enqueueFresh(item.id)
	}
}

// enqueueFresh ends any attempt state and makes the advertiser eligible for a
// later fresh arrival.
func (w *workload) enqueueFresh(id int) {
	a := &w.ads[id]
	a.waiting, a.first, a.quoteSum = false, 0, 0
	// Compact only when the consumed prefix is at least half the slice, so
	// each element is moved O(1) times on average.
	if len(w.ready) == cap(w.ready) && w.head >= len(w.ready)/2 {
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
func (w *workload) readyCount() int { return len(w.ready) - w.head }

// Abandonment policies for an advertiser that has been quoted a waiting time.
const (
	abandonProduction = "production" // topicindex.Registration.HandleTicketResponse
	abandonSpec       = "spec"       // a single waiting interval SHOULD NOT exceed E
	abandonNone       = "none"       // retry after any wait (the original harness)
)

// clientWait is the waiting time as received by the client: REGCONFIRMATION
// carries it in milliseconds, rounded up (v5_udp.go:waitTimeToMs), and the
// client converts it back with waitTimeFromMs, which saturates. A wait below
// the registrar's 1 s floor is raised to the floor
// (Registration.HandleTicketResponse).
func clientWait(d time.Duration) time.Duration {
	ms := math.Ceil(d.Seconds() * 1000)
	if ms >= float64(math.MaxInt64/int64(time.Millisecond)) {
		return math.MaxInt64
	}
	return max(time.Duration(ms)*time.Millisecond, clientWaitFloor)
}

// clientWaitFloor mirrors topicindex.topicTableWaitTimeFloor.
const clientWaitFloor = time.Second

// quoted applies the client's response to a ticket with waiting time wait,
// issued at registrar time 'issued'. It returns "" if the advertiser will
// retry, otherwise the abandonment reason ("single", "cumulative" or
// "wrapped").
//
// The production policy mirrors HandleTicketResponse exactly: drop when the
// single quote exceeds RegAttemptTimeout, otherwise add it to the sum of
// quotes and drop when that sum exceeds RegAttemptTimeout.
func (w *workload) quoted(id int, issued mclock.AbsTime, wait time.Duration, policy string, timeout, lifetime time.Duration) (string, error) {
	a := &w.ads[id]
	cw := clientWait(wait)
	if cw <= 0 || issued.Add(cw) < issued {
		// The decoded wait wrapped around (registrar quoted ~MaxInt64 at
		// occupancy >= ~84%, see v5_topic_overflow_test.go) or the retry time
		// overflows. The production client then retries at once with a ticket
		// the registrar rejects without reply, and drops the attempt on timeout.
		return "wrapped", nil
	}
	switch policy {
	case abandonProduction:
		if cw > timeout {
			return "single", nil
		}
		if a.quoteSum+cw > timeout {
			return "cumulative", nil
		}
	case abandonSpec:
		if cw > lifetime {
			return "single", nil
		}
	}
	if !a.waiting {
		a.first = issued
		a.waiting = true
	}
	a.quoteSum += cw
	heap.Push(&w.retries, wake{at: issued.Add(cw), id: id})
	return "", nil
}

// cooldown takes an advertiser that abandoned this registrar out of the
// population until at, when it becomes eligible for a fresh arrival again.
func (w *workload) cooldown(id int, at mclock.AbsTime) {
	a := &w.ads[id]
	a.waiting, a.first, a.quoteSum = false, 0, 0
	heap.Push(&w.active, wake{at: at, id: id})
}

// admitted records an admission; the advertiser becomes eligible again once
// its ad has expired.
func (w *workload) admitted(id int, releaseAt mclock.AbsTime) {
	a := &w.ads[id]
	a.waiting, a.first, a.quoteSum = false, 0, 0
	heap.Push(&w.active, wake{at: releaseAt, id: id})
}

// arrivals is a fixed-rate arrival process, either deterministic (i/rate) or
// Poisson with exponential inter-arrival times from its own PRNG.
type arrivals struct {
	rate    float64
	poisson bool
	rng     *rand.Rand
	index   uint64
	next    time.Duration
}

func newArrivals(rate float64, poisson bool, seed int64) *arrivals {
	a := &arrivals{rate: rate, poisson: poisson, rng: rand.New(rand.NewSource(seed))}
	a.next = a.at(0)
	if poisson && rate > 0 {
		a.next = a.gap()
	}
	return a
}
func (a *arrivals) gap() time.Duration {
	return time.Duration(a.rng.ExpFloat64() / a.rate * float64(time.Second))
}
func (a *arrivals) at(index uint64) time.Duration {
	if a.rate == 0 {
		return time.Duration(math.MaxInt64)
	}
	ns := float64(index) * float64(time.Second) / a.rate
	if ns >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(ns)
}

// skipTo drops all arrivals before t.
func (a *arrivals) skipTo(t time.Duration) {
	if a.rate == 0 || a.next >= t {
		return
	}
	if a.poisson {
		a.next = t + a.gap()
		return
	}
	a.index = uint64(math.Ceil(t.Seconds() * a.rate))
	a.next = a.at(a.index)
}

// consume advances to the next arrival.
func (a *arrivals) consume() {
	a.index++
	if a.rate == 0 {
		return
	}
	if a.poisson {
		a.next += a.gap()
	} else {
		a.next = a.at(a.index)
	}
}

// pendingBefore counts arrivals scheduled before end that have not been consumed.
func (a *arrivals) pendingBefore(end time.Duration) uint64 {
	if a.rate == 0 || a.next >= end {
		return 0
	}
	if !a.poisson {
		return uint64(math.Ceil(end.Seconds()*a.rate)) - a.index
	}
	// Poisson: estimate the backlog from its duration.
	return uint64(math.Ceil((end - a.next).Seconds() * a.rate))
}

func relayCheck(requester netip.Addr) func(*enode.Node) bool {
	return func(n *enode.Node) bool { return netutil.CheckRelayAddr(requester, n.IPAddr()) == nil }
}
