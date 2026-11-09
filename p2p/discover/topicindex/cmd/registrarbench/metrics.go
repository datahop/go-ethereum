package main

import (
	"fmt"
	"math"
	"math/bits"
	"os"
	"os/exec"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"syscall"
)

func cpuSeconds() (float64, error) {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		return 0, err
	}
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6, nil
}
func rssBytes() (uint64, error) {
	if runtime.GOOS == "linux" {
		data, err := os.ReadFile("/proc/self/statm")
		if err != nil {
			return 0, err
		}
		fields := strings.Fields(string(data))
		if len(fields) < 2 {
			return 0, fmt.Errorf("invalid statm")
		}
		pages, err := strconv.ParseUint(fields[1], 10, 64)
		return pages * uint64(os.Getpagesize()), err
	}
	if runtime.GOOS == "darwin" {
		data, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
		if err != nil {
			return 0, err
		}
		kb, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		return kb * 1024, err
	}
	return 0, fmt.Errorf("RSS unsupported on %s", runtime.GOOS)
}

type memory struct {
	Live        uint64 `json:"live_heap_last_gc_bytes"`
	Incremental int64  `json:"incremental_live_heap_bytes"`
	Allocated   uint64 `json:"heap_alloc_bytes"`
	TotalAlloc  uint64 `json:"total_alloc_bytes"`
	Mallocs     uint64 `json:"mallocs"`
	GC          uint32 `json:"gc_cycles"`
	RSS         uint64 `json:"rss_bytes"`
	RSSError    string `json:"rss_error,omitempty"`
}

func memorySample(baseline uint64) memory {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	samples := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(samples)
	live := samples[0].Value.Uint64()
	m := memory{Live: live, Incremental: int64(live) - int64(baseline), Allocated: ms.HeapAlloc, TotalAlloc: ms.TotalAlloc, Mallocs: ms.Mallocs, GC: ms.NumGC}
	rss, err := rssBytes()
	m.RSS = rss
	if err != nil {
		m.RSSError = err.Error()
	}
	return m
}

// Fixed log2 histogram: percentile upper bounds have <3.125% bucket width
// above 32 ns. Raw operation latencies are emitted for exact recomputation.
type histogram struct {
	bins  [2048]uint64
	Count uint64
	Sum   float64 // float64: wait times in ns overflow uint64 sums
}

func (h *histogram) add(ns uint64) {
	shift := max(0, bits.Len64(ns)-6)
	index := shift*32 + int(ns>>shift)
	if index >= len(h.bins) {
		index = len(h.bins) - 1
	}
	h.bins[index]++
	h.Count++
	h.Sum += float64(ns)
}
func (h *histogram) quantile(q float64) uint64 {
	if h.Count == 0 {
		return 0
	}
	target := uint64(math.Ceil(float64(h.Count) * q))
	var count uint64
	for i, n := range h.bins {
		count += n
		if count >= target {
			if i < 64 {
				return uint64(i)
			}
			shift := (i - 32) / 32
			value := i - shift*32
			return (uint64(value+1) << shift) - 1
		}
	}
	return 0
}
func (h *histogram) summary() map[string]any {
	mean := 0.0
	if h.Count > 0 {
		mean = h.Sum / float64(h.Count)
	}
	return map[string]any{"count": h.Count, "mean_ns": mean, "median_ns_upper": h.quantile(.5), "p95_ns_upper": h.quantile(.95), "p99_ns_upper": h.quantile(.99)}
}

type counters struct {
	FreshArrivals       uint64    `json:"fresh_arrivals"`
	PopulationScarcity  uint64    `json:"population_scarcity_events"`
	Registrations       uint64    `json:"registrations_processed"`
	Fresh               uint64    `json:"fresh_attempts"`
	Retries             uint64    `json:"retry_attempts"`
	Admissions          uint64    `json:"new_admissions"`
	Immediate           uint64    `json:"immediate_admissions"`
	Eventual            uint64    `json:"admissions_after_wait"`
	Waits               uint64    `json:"wait_responses"`
	Abandoned           uint64    `json:"abandoned_attempts"`
	AbandonedSingle     uint64    `json:"abandoned_single_quote"`
	AbandonedCumulative uint64    `json:"abandoned_cumulative_quotes"`
	AbandonedWrapped    uint64    `json:"abandoned_wrapped_wait"`
	ExpiredRetries      uint64    `json:"expired_retries"`
	LateRetries         uint64    `json:"late_retries"`
	Queries             uint64    `json:"queries_processed"`
	Returned            uint64    `json:"advertisements_returned"`
	Expiries            uint64    `json:"expiry_calls"`
	Expired             uint64    `json:"expired_entries"`
	RegLatency          histogram `json:"-"`
	QueryLatency        histogram `json:"-"`
	ExpiryLatency       histogram `json:"-"`
	QueueDelay          histogram `json:"-"`
	QuotedWait          histogram `json:"-"` // registrar wait quotes (ns)
	AdmittedWait        histogram `json:"-"` // registrar-credited wait at admission, 0 if immediate
	EventualWait        histogram `json:"-"` // as AdmittedWait, admissions after waiting only
	AbandonedAfter      histogram `json:"-"` // credited wait when the attempt was abandoned
	RegBytes            uint64    `json:"registration_alloc_bytes"`
	RegAllocs           uint64    `json:"registration_allocations"`
	QueryBytes          uint64    `json:"query_alloc_bytes"`
	QueryAllocs         uint64    `json:"query_allocations"`
	ExpiryBytes         uint64    `json:"expiry_alloc_bytes"`
	ExpiryAllocs        uint64    `json:"expiry_allocations"`
}

// sub returns the difference of the scalar counters c - o.
func (c counters) sub(o counters) counters {
	return counters{FreshArrivals: c.FreshArrivals - o.FreshArrivals, PopulationScarcity: c.PopulationScarcity - o.PopulationScarcity,
		Registrations: c.Registrations - o.Registrations, Fresh: c.Fresh - o.Fresh, Retries: c.Retries - o.Retries,
		Admissions: c.Admissions - o.Admissions, Immediate: c.Immediate - o.Immediate, Eventual: c.Eventual - o.Eventual,
		Waits: c.Waits - o.Waits, Abandoned: c.Abandoned - o.Abandoned, AbandonedSingle: c.AbandonedSingle - o.AbandonedSingle,
		AbandonedCumulative: c.AbandonedCumulative - o.AbandonedCumulative, AbandonedWrapped: c.AbandonedWrapped - o.AbandonedWrapped, ExpiredRetries: c.ExpiredRetries - o.ExpiredRetries, LateRetries: c.LateRetries - o.LateRetries,
		Queries: c.Queries - o.Queries, Returned: c.Returned - o.Returned, Expiries: c.Expiries - o.Expiries, Expired: c.Expired - o.Expired}
}

func ratio(a, b uint64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}
