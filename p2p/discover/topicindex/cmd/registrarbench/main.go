// registrarbench measures TopicTable core operations without discovery transport.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"time"
)

type config struct {
	Mode            string
	Warmup          *time.Duration
	Saturation      bool
	Advertisers     int
	Topics          int
	Alpha           float64
	Seed            int64
	RegRate         float64
	QueryRate       float64
	Duration        time.Duration
	WarmupFraction  float64
	CacheSize       int
	Lifetime        time.Duration
	IPFile          string
	IPColumn        string
	SampleInterval  time.Duration
	Output          string
	AllocationAudit bool
	CPUProfile      string
}

func flags() config {
	c := config{}
	flag.StringVar(&c.Mode, "mode", "realtime", "realtime or simulated")
	flag.BoolVar(&c.Saturation, "saturation", false, "realtime load-test marker; population remains fixed")
	flag.Func("warmup", "warm-up duration (overrides fraction)", func(v string) error { d, err := time.ParseDuration(v); c.Warmup = &d; return err })
	flag.IntVar(&c.Advertisers, "advertisers", 10000, "exact fixed advertiser population")
	flag.IntVar(&c.Topics, "topics", 300, "topic count")
	flag.Float64Var(&c.Alpha, "zipf-alpha", 1, "Zipf exponent")
	flag.Int64Var(&c.Seed, "seed", 1, "deterministic seed")
	flag.Float64Var(&c.RegRate, "reg-rate", 100, "scheduled total registration opportunities/second, retries first")
	flag.Float64Var(&c.QueryRate, "query-rate", 100, "offered queries/second")
	flag.DurationVar(&c.Duration, "duration", time.Hour, "total workload duration (logical time in simulated mode)")
	flag.Float64Var(&c.WarmupFraction, "warmup-fraction", .5, "fraction of duration for warm-up")
	flag.IntVar(&c.CacheSize, "cache-size", 0, "cache capacity; 0 uses production default")
	flag.DurationVar(&c.Lifetime, "ad-lifetime", 0, "advertisement lifetime; 0 uses production default")
	flag.StringVar(&c.IPFile, "ip-file", "results/data/dnslist-ipv4.txt", "IPv4 text or .csv dataset; empty uses synthetic IPs")
	flag.StringVar(&c.IPColumn, "ip-column", "ip", "CSV IPv4 column")
	flag.DurationVar(&c.SampleInterval, "sample-interval", time.Second, "resource sampling interval")
	flag.StringVar(&c.Output, "output", "registrarbench.jsonl", "JSONL output path, or - for stdout")
	flag.BoolVar(&c.AllocationAudit, "allocation-audit", false, "instrument allocation counters per method; invalidates performance timing")
	flag.StringVar(&c.CPUProfile, "cpu-profile", "", "optional measurement-phase CPU profile path")
	flag.Parse()
	return c
}
func (c config) warmup() time.Duration {
	if c.Warmup != nil {
		return *c.Warmup
	}
	return time.Duration(float64(c.Duration) * c.WarmupFraction)
}
func (c config) validate() error {
	if c.Mode != "" && c.Mode != "realtime" && c.Mode != "simulated" {
		return fmt.Errorf("unknown mode %q", c.Mode)
	}
	if c.warmup() < 0 || c.warmup() >= c.Duration {
		return fmt.Errorf("warmup must be in [0,duration)")
	}
	if c.Saturation && c.Mode == "simulated" {
		return fmt.Errorf("saturation is a realtime load-test marker")
	}
	finite := func(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
	if c.Advertisers <= 0 || c.Topics <= 0 || !finite(c.Alpha) || c.Alpha < 0 || !finite(c.RegRate) || c.RegRate < 0 || c.RegRate > 1e9 || !finite(c.QueryRate) || c.QueryRate < 0 || c.QueryRate > 1e9 || c.Duration <= 0 || c.SampleInterval <= 0 || c.CacheSize < 0 || c.Lifetime < 0 || !finite(c.WarmupFraction) || c.WarmupFraction < 0 || c.WarmupFraction >= 1 {
		return fmt.Errorf("invalid configuration: positive sizes/durations, finite nonnegative rates <=1e9, alpha >=0 and warmup fraction in [0,1) required")
	}
	return nil
}

type operation struct {
	RetryDue       int64   `json:"retry_due_ns,omitempty"`
	Mode           string  `json:"mode"`
	LogicalElapsed int64   `json:"logical_elapsed_ns"`
	WallElapsed    int64   `json:"wall_elapsed_ns"`
	Removed        int     `json:"removed,omitempty"`
	Type           string  `json:"type"`
	Kind           string  `json:"operation"`
	Scheduled      int64   `json:"scheduled_ns"`
	Started        int64   `json:"started_ns"`
	Latency        int64   `json:"latency_ns"`
	Queue          *int64  `json:"queue_delay_ns,omitempty"`
	Fresh          bool    `json:"fresh"`
	Advertiser     int     `json:"advertiser"`
	Rank           int     `json:"topic_rank"`
	Population     int     `json:"topic_population"`
	TopicSize      int     `json:"topic_cache_size"`
	Occupancy      int     `json:"occupancy"`
	Utilisation    float64 `json:"utilisation"`
	Outcome        string  `json:"outcome,omitempty"`
	Wait           int64   `json:"wait_ns,omitempty"`
	Credited       int64   `json:"credited_wait_ns,omitempty"`
	Returned       int     `json:"returned"`
	AllocBytes     uint64  `json:"alloc_bytes,omitempty"`
	Allocs         uint64  `json:"allocations,omitempty"`
}

func revision() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" {
				return s.Value
			}
		}
	}
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err == nil {
		return strings.TrimSpace(string(out))
	}
	return "unknown"
}
func phase(offset, warmup time.Duration) bool { return offset >= warmup }
func offeredBefore(d time.Duration, rate float64) uint64 {
	if rate == 0 || d <= 0 {
		return 0
	}
	return uint64(math.Ceil(d.Seconds() * rate))
}
func auditBefore(enabled bool) runtime.MemStats {
	var m runtime.MemStats
	if enabled {
		runtime.ReadMemStats(&m)
	}
	return m
}
func auditAfter(enabled bool, b runtime.MemStats) (uint64, uint64) {
	if !enabled {
		return 0, 0
	}
	var a runtime.MemStats
	runtime.ReadMemStats(&a)
	return a.TotalAlloc - b.TotalAlloc, a.Mallocs - b.Mallocs
}

func run(c config, out io.Writer) error {
	if c.Mode == "" {
		c.Mode = "realtime"
	}
	if err := c.validate(); err != nil {
		return err
	}
	if err := os.Setenv("GODEBUG", os.Getenv("GODEBUG")+",randseednop=0"); err != nil {
		return err
	}
	rand.Seed(c.Seed + 4)
	requestedAdvertisers := c.Advertisers
	w, data, err := population(c)
	if err != nil {
		return err
	}
	e := newEngine(c, w)
	tab := e.table
	c.CacheSize = tab.Stats().Capacity
	c.Lifetime = tab.AdLifetime()
	e.cfg = c
	encoder := json.NewEncoder(out)
	topicCounts := make([]int, c.Topics)
	runtime.GC()
	baseline := memorySample(0)
	baseline.Incremental = 0
	if err := encoder.Encode(map[string]any{"type": "metadata", "schema_version": 5, "population_model": "fixed-reusable-v2", "mode": c.Mode, "registration_semantics": "total-attempt-scheduled-arrivals-v1", "config": c, "dataset": data, "go_version": runtime.Version(), "revision": revision(), "os": runtime.GOOS, "arch": runtime.GOARCH, "gomaxprocs": runtime.GOMAXPROCS(0), "godebug": os.Getenv("GODEBUG"), "gogc_env": os.Getenv("GOGC"), "gomemlimit_env": os.Getenv("GOMEMLIMIT"), "baseline": baseline, "topic_populations": w.population, "requested_advertisers": requestedAdvertisers, "pregenerated_advertisers": c.Advertisers, "actual_advertisers": c.Advertisers, "population_auto_sized": false, "result_limit": 16, "allocation_audit": c.AllocationAudit, "warmup_ns": int64(c.warmup()), "retry_validity_ns": int64(retryValidityWindow), "live_heap": "last completed GC; incremental subtracts post-population baseline; includes harness", "rss": "whole process, supplementary", "percentiles": "histogram upper bounds; raw method latency permits exact recomputation"}); err != nil {
		return err
	}
	var startMem runtime.MemStats
	var startCPU float64
	var measureLogical time.Duration
	var measureWall time.Time
	var profile *os.File
	defer func() {
		if profile != nil {
			pprof.StopCPUProfile()
			profile.Close()
		}
	}()
	begin := func() error {
		if c.CPUProfile != "" {
			f, err := os.Create(c.CPUProfile)
			if err != nil {
				return err
			}
			if err = pprof.StartCPUProfile(f); err != nil {
				f.Close()
				return err
			}
			profile = f
		}
		runtime.ReadMemStats(&startMem)
		if c.Mode == "realtime" {
			var err error
			startCPU, err = cpuSeconds()
			if err != nil {
				return err
			}
		}
		measureLogical = e.scheduler.elapsed()
		measureWall = time.Now()
		e.measuring = true
		return nil
	}
	sample := func() error {
		stats := tab.Stats()
		for i, t := range w.topics {
			topicCounts[i] = tab.TopicSize(t)
		}
		rec := map[string]any{"type": "sample", "mode": c.Mode, "elapsed_ns": int64(e.scheduler.elapsed()), "logical_elapsed_ns": int64(e.scheduler.elapsed()), "wall_elapsed_ns": int64(time.Since(e.scheduler.wallStart)), "measurement": e.measuring, "state": stats, "utilisation": float64(stats.Occupancy) / float64(c.CacheSize), "ads_per_topic": topicCounts, "memory": memorySample(baseline.Live), "counts": e.total, "retry_queue_length": len(e.queued), "population_limited": e.total.PopulationScarcity > 0, "scheduled_registration_arrivals": offeredBefore(min(e.scheduler.elapsed(), c.Duration), c.RegRate), "dispatched_registration_arrivals": e.pacer.index}
		if c.Mode == "realtime" {
			cpu, err := cpuSeconds()
			if err != nil {
				return err
			}
			rec["process_cpu_seconds"] = cpu
		}
		return encoder.Encode(rec)
	}
	e.scheduler.start()
	nextSample := time.Duration(0)
	for {
		now := e.scheduler.elapsed()
		e.checkSupply(min(now, c.Duration))
		if now >= c.Duration {
			break
		}
		if !e.measuring && now >= c.warmup() {
			if err := begin(); err != nil {
				return err
			}
		}
		if e.nextExpiry() <= now {
			op := e.expire()
			if e.measuring {
				if err := encoder.Encode(op); err != nil {
					return err
				}
			}
			continue
		}
		w.release(e.scheduler.clock.Now())
		if now >= nextSample {
			if err := sample(); err != nil {
				return err
			}
			nextSample = (now/c.SampleInterval + 1) * c.SampleInterval
			continue
		}
		event := e.nextWork()
		next := min(event.due, e.nextExpiry(), e.nextRelease(), nextSample, c.Duration)
		if !e.measuring {
			next = min(next, c.warmup())
		}
		if now < next {
			e.scheduler.advance(next)
			continue
		}
		if event.due <= now {
			if event.kind == "wake" {
				continue
			}
			op, err := e.execute(event)
			if err != nil {
				return err
			}
			if e.measuring {
				if err := encoder.Encode(op); err != nil {
					return err
				}
			}
		}
	}
	if !e.measuring {
		return fmt.Errorf("measurement phase never started")
	}
	elapsed := e.scheduler.elapsed()
	wallElapsed := time.Since(e.scheduler.wallStart)
	seconds := (elapsed - measureLogical).Seconds()
	wall := time.Since(measureWall).Seconds()
	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)
	summary := map[string]any{"type": "summary", "mode": c.Mode, "allocation_audit": c.AllocationAudit, "performance_valid": !c.AllocationAudit, "realtime_metrics_valid": c.Mode == "realtime" && !c.AllocationAudit, "measurement_seconds": seconds, "measurement_logical_seconds": seconds, "measurement_wall_seconds": wall, "logical_elapsed_ns": int64(elapsed), "wall_elapsed_ns": int64(wallElapsed), "measurement_start_ns": int64(measureLogical), "configured_measurement_seconds": (c.Duration - c.warmup()).Seconds(), "counts": e.measured, "total_counts": e.total, "configured_reg_rate": c.RegRate, "target_registration_rps": c.RegRate, "configured_query_rate": c.QueryRate, "total_query_arrivals_offered": offeredBefore(c.Duration, c.QueryRate), "unserved_queries": offeredBefore(c.Duration, c.QueryRate) - e.queryIndex, "pending_retries": len(w.retries), "retry_queue_length": len(e.queued), "population_limited": e.total.PopulationScarcity > 0, "registration_latency": e.measured.RegLatency.summary(), "query_latency": e.measured.QueryLatency.summary(), "expiry_latency": e.measured.ExpiryLatency.summary(), "immediate_admission_fraction_of_fresh": ratio(e.measured.Immediate, e.measured.Fresh), "wait_fraction_of_requests": ratio(e.measured.Waits, e.measured.Registrations)}
	summary["actual_advertisers"] = c.Advertisers
	summary["population_scarcity_events"] = e.total.PopulationScarcity
	summary["missed_registration_opportunities_due_to_scarcity"] = e.total.PopulationScarcity
	summary["population_scarcity_fraction"] = ratio(e.total.PopulationScarcity, offeredBefore(c.Duration, c.RegRate))
	summary["population_scarcity_interval_ns"] = e.total.ScarcityIntervalNS
	summary["measured_population_scarcity_events"] = e.measured.PopulationScarcity
	summary["measured_population_limited"] = e.measured.PopulationScarcity > 0
	summary["population_limited_any"] = e.total.PopulationScarcity > 0
	summary["population_limited_measurement"] = e.measured.PopulationScarcity > 0
	summary["measured_population_scarcity_fraction_of_handled_opportunities"] = ratio(e.measured.PopulationScarcity, e.measured.Registrations+e.measured.PopulationScarcity)
	summary["handled_registration_opportunities"] = e.pacer.index

	summary["requested_advertisers"] = requestedAdvertisers
	summary["pregenerated_advertisers"] = c.Advertisers
	summary["population_auto_sized"] = false
	summary["scheduled_registration_arrivals"] = offeredBefore(c.Duration, c.RegRate)
	summary["measured_scheduled_registration_arrivals"] = offeredBefore(c.Duration, c.RegRate) - offeredBefore(min(measureLogical, c.Duration), c.RegRate)
	summary["dispatched_registration_arrivals"] = e.pacer.index
	summary["unserved_registration_arrivals"] = offeredBefore(c.Duration, c.RegRate) - e.pacer.index

	overdueRetries := 0
	for _, retry := range append(append([]wake(nil), w.retries...), e.queued...) {
		if retry.at < e.scheduler.origin.Add(c.Duration) {
			overdueRetries++
		}
	}
	summary["overdue_retries"] = overdueRetries
	summary["ready_advertisers"] = len(w.ready) - w.head
	summary["deadline_overrun_ns"] = max(int64(0), int64(elapsed-c.Duration))
	summary["rates_time_basis"] = "logical"
	if c.Mode == "realtime" {
		summary["rates_time_basis"] = "wall"
	}
	for key, n := range map[string]uint64{"registration_rps": e.measured.Registrations, "actual_total_registration_rps": e.measured.Registrations, "fresh_registration_rps": e.measured.Fresh, "retry_rps": e.measured.Retries, "retry_demand_rps": e.measured.RetryDemand, "duplicate_rps": e.measured.Duplicates, "admission_rps": e.measured.Admissions, "wait_rps": e.measured.Waits, "retries_queued_rps": e.measured.RetriesQueued, "expired_retry_rps": e.measured.ExpiredRetries, "query_rps": e.measured.Queries} {
		summary[key] = float64(n) / seconds
	}
	if c.Mode == "realtime" {
		endCPU, err := cpuSeconds()
		if err != nil {
			return err
		}
		summary["cpu_seconds"] = endCPU - startCPU
		summary["cpu_percent_one_core"] = 100 * (endCPU - startCPU) / wall
		summary["allocation_bytes_per_second"] = float64(endMem.TotalAlloc-startMem.TotalAlloc) / wall
		summary["allocations_per_second"] = float64(endMem.Mallocs-startMem.Mallocs) / wall
	} else {
		summary["allocation_bytes_per_logical_second"] = float64(endMem.TotalAlloc-startMem.TotalAlloc) / seconds
	}
	if profile != nil {
		pprof.StopCPUProfile()
		profile.Close()
		profile = nil
	}
	if err := sample(); err != nil {
		return err
	}
	runtime.GC()
	summary["post_measurement_forced_gc_memory"] = memorySample(baseline.Live)
	summary["final_state"] = tab.Stats()
	if c.AllocationAudit {
		m := &e.measured
		summary["audit_per_operation"] = map[string]float64{"registration_bytes": ratio(m.RegBytes, m.Registrations), "registration_allocations": ratio(m.RegAllocs, m.Registrations), "query_bytes": ratio(m.QueryBytes, m.Queries), "query_allocations": ratio(m.QueryAllocs, m.Queries), "expiry_bytes": ratio(m.ExpiryBytes, m.Expiries), "expiry_allocations": ratio(m.ExpiryAllocs, m.Expiries)}
	}
	err = encoder.Encode(summary)
	runtime.KeepAlive(w)
	runtime.KeepAlive(tab)
	return err
}
func main() {
	c := flags()
	var out io.Writer = os.Stdout
	var file *os.File
	if c.Output != "-" {
		var err error
		file, err = os.Create(c.Output)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		out = file
	}
	buf := bufio.NewWriterSize(out, 256*1024)
	err := run(c, buf)
	flushErr := buf.Flush()
	if file != nil {
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
	}
	if err == nil {
		err = flushErr
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
