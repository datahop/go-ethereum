package main

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// Bucketed overhead time series. Per-node counters are cumulative, so rates
// over time need periodic sampling; sampling every node individually would be
// far too large at 10k nodes, so samples are aggregated into ID-space buckets
// (position = top 64 bits of the node ID) and, for the codec-level counters,
// by message type.

const overheadBuckets = 50

// cacheSampleLimit bounds the population whose ad caches are polled per sample;
// each poll round-trips through that node's dispatch goroutine.
const cacheSampleLimit = 20000

// idPosition maps a node ID to its position in [0,1) in ID space.
func idPosition(id enode.ID) float64 {
	return float64(binary.BigEndian.Uint64(id[:8])) / float64(1<<64)
}

func idBucket(id enode.ID) int {
	b := int(idPosition(id) * overheadBuckets)
	if b >= overheadBuckets {
		b = overheadBuckets - 1
	}
	return b
}

// overheadSample is one sampling tick: cumulative totals per ID-space bucket,
// both at the packet level (all traffic, encrypted) and per message type.
type overheadSample struct {
	TSec    float64                        `json:"tSec"`
	TxBytes []int64                        `json:"txBytes"` // by ID-space bucket
	RxBytes []int64                        `json:"rxBytes"`
	TxMsgs  []int64                        `json:"txMsgs"`
	RxMsgs  []int64                        `json:"rxMsgs"`
	ByType  map[string]*bucketedWireSample `json:"byType"` // msg type -> per-bucket totals
	// Nodes counts live nodes per bucket, so a bucket total can be turned into
	// a per-node figure. Buckets are not equally populated and churn changes
	// the counts during a run, so this cannot be inferred from the node total.
	Nodes []int `json:"nodes"`
	// Cache occupancy across the sampled nodes: ads held, configured capacity,
	// and the per-topic totals. Figure 1 ("cache utilisation over time") reads
	// these; the waiting-time function is driven by the fill ratio, so this is
	// the state behind the quoted waits.
	CacheHeld    int64            `json:"cacheHeld"`
	CacheCap     int64            `json:"cacheCap"`
	CacheByTopic map[string]int64 `json:"cacheByTopic"`
	numNodes     int
}

type bucketedWireSample struct {
	TxBytes []int64 `json:"txBytes"`
	RxBytes []int64 `json:"rxBytes"`
	TxMsgs  []int64 `json:"txMsgs"`
	RxMsgs  []int64 `json:"rxMsgs"`
}

func newBucketedWireSample() *bucketedWireSample {
	return &bucketedWireSample{
		TxBytes: make([]int64, overheadBuckets),
		RxBytes: make([]int64, overheadBuckets),
		TxMsgs:  make([]int64, overheadBuckets),
		RxMsgs:  make([]int64, overheadBuckets),
	}
}

// nodeRegistry tracks every node created by spawnNode, including mid-run churn
// joiners and the TopDisc side of a mixed-binary run, so the overhead sampler
// sees the live population rather than the initial spawn list.
var (
	nodeRegistry   []nodeRec
	nodeRegistryMu sync.Mutex
)

func registerNodeRec(rec nodeRec) {
	nodeRegistryMu.Lock()
	nodeRegistry = append(nodeRegistry, rec)
	nodeRegistryMu.Unlock()
}

func liveNodeRecs() []nodeRec {
	nodeRegistryMu.Lock()
	defer nodeRegistryMu.Unlock()
	return append([]nodeRec(nil), nodeRegistry...)
}

type overheadSeries struct {
	mu      sync.Mutex
	samples []overheadSample
	stop    chan struct{}
	done    chan struct{}
	// The normal teardown path and the absolute watchdog can both reach dump
	// concurrently, so stopping the sampler has to be idempotent.
	stopOnce sync.Once
}

// startOverheadSeries samples cumulative traffic counters every period until
// stopped. Sampling walks every live node, so the period should be tens of
// seconds at 10k scale.
func startOverheadSeries(period time.Duration, start time.Time) *overheadSeries {
	os := &overheadSeries{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(os.done)
		tick := time.NewTicker(period)
		defer tick.Stop()
		for {
			select {
			case <-os.stop:
				os.sample(start) // final sample
				return
			case <-tick.C:
				os.sample(start)
			}
		}
	}()
	return os
}

func (o *overheadSeries) sample(start time.Time) {
	nodes := liveNodeRecs()
	// Occupancy is read on each node's dispatch goroutine, so it is far more
	// expensive than the atomic counters. Sample a bounded subset and scale.
	cacheSample := len(nodes) <= cacheSampleLimit
	s := overheadSample{
		TSec:         time.Since(start).Seconds(),
		TxBytes:      make([]int64, overheadBuckets),
		RxBytes:      make([]int64, overheadBuckets),
		TxMsgs:       make([]int64, overheadBuckets),
		RxMsgs:       make([]int64, overheadBuckets),
		ByType:       make(map[string]*bucketedWireSample),
		Nodes:        make([]int, overheadBuckets),
		CacheByTopic: make(map[string]int64),
	}
	connRegistryMu.Lock()
	byIdx := make(map[int]*simUDPConn, len(connRegistry))
	for _, c := range connRegistry {
		byIdx[c.idx] = c
	}
	connRegistryMu.Unlock()

	for _, nr := range nodes {
		if nr.disc == nil {
			continue
		}
		b := idBucket(nr.ln.ID())
		s.numNodes++
		s.Nodes[b]++
		if c := byIdx[nr.idx]; c != nil {
			s.TxBytes[b] += c.txBytes.Load()
			s.RxBytes[b] += c.rxBytes.Load()
			s.TxMsgs[b] += c.txPkts.Load()
			s.RxMsgs[b] += c.rxPkts.Load()
		}
		if cacheSample {
			held, capacity, byTopic := nr.disc.TopicCacheOccupancy()
			s.CacheHeld += int64(held)
			s.CacheCap += int64(capacity)
			for t, n := range byTopic {
				s.CacheByTopic[t.String()] += int64(n)
			}
		}
		for name, wc := range nr.disc.WireStats() {
			bw := s.ByType[name]
			if bw == nil {
				bw = newBucketedWireSample()
				s.ByType[name] = bw
			}
			bw.TxBytes[b] += wc.TxBytes
			bw.RxBytes[b] += wc.RxBytes
			bw.TxMsgs[b] += wc.TxMsgs
			bw.RxMsgs[b] += wc.RxMsgs
		}
	}
	o.mu.Lock()
	o.samples = append(o.samples, s)
	o.mu.Unlock()
}

// dump stops sampling and writes the series, plus the final per-topic
// wait-time samples, to path.
func (o *overheadSeries) dump(path string) {
	o.stopOnce.Do(func() { close(o.stop) })
	<-o.done
	o.mu.Lock()
	defer o.mu.Unlock()

	type waitOut struct {
		Topic      string  `json:"topic"`
		Admitted   int64   `json:"admitted"`
		Quoted     int64   `json:"quoted"`
		QuotedMs   []int64 `json:"quotedMs"`
		AdmittedMs []int64 `json:"admittedMs"`
	}
	var waits []waitOut
	for topic, st := range discover.WaitTimeStatsSnapshot() {
		waits = append(waits, waitOut{topic.String(), st.Admitted, st.Quoted, st.QuotedMs, st.AdmittedMs})
	}

	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	json.NewEncoder(f).Encode(struct {
		Buckets  int              `json:"buckets"`
		Samples  []overheadSample `json:"samples"`
		WaitTime []waitOut        `json:"waitTime"`
	}{overheadBuckets, o.samples, waits})
}
