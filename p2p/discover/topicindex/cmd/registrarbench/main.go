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
	"runtime"
	"runtime/pprof"
	"time"
)

type config struct {
	Mode            string
	Warmup          *time.Duration
	Advertisers     int
	Topics          int
	Alpha           float64
	Seed            int64
	FreshRate       float64
	QueryRate       float64
	Arrivals        string
	Abandon         string
	AttemptTimeout  time.Duration
	Duration        time.Duration
	WarmupFraction  float64
	CacheSize       int
	Lifetime        time.Duration
	IPFile          string
	IPColumn        string
	IPDist          string
	IPPrefixes      int
	IndependentENR  bool
	RawOps          bool
	Pacing          string
	AllowTranslated bool
	SampleInterval  time.Duration
	Output          string
	AllocationAudit bool
	CPUProfile      string
	HeapProfile     string
	MemfillLevels   string
	MemfillReps     int
	ENRPad          int
}

func flags() config {
	c := config{}
	flag.StringVar(&c.Mode, "mode", "realtime", "realtime, simulated, hybrid (simulated warm-up, realtime measurement) or memfill")
	flag.Func("warmup", "warm-up duration (overrides fraction)", func(v string) error { d, err := time.ParseDuration(v); c.Warmup = &d; return err })
	flag.IntVar(&c.Advertisers, "advertisers", 10000, "exact fixed advertiser population")
	flag.IntVar(&c.Topics, "topics", 300, "topic count")
	flag.Float64Var(&c.Alpha, "zipf-alpha", 1, "Zipf exponent")
	flag.Int64Var(&c.Seed, "seed", 1, "deterministic seed")
	flag.Float64Var(&c.FreshRate, "fresh-rate", 1, "fresh registration-attempt arrivals/second; retries arrive separately at their due time")
	flag.Float64Var(&c.QueryRate, "query-rate", 20, "query arrivals/second")
	flag.StringVar(&c.Arrivals, "arrivals", "deterministic", "arrival process for fresh attempts and queries: deterministic or poisson")
	flag.StringVar(&c.Abandon, "abandon", abandonProduction, "advertiser policy for long waits: production (RegAttemptTimeout on single and summed quotes), spec (single quote > E) or none")
	flag.DurationVar(&c.AttemptTimeout, "attempt-timeout", 0, "RegAttemptTimeout for --abandon=production; 0 uses the production default 1.5*lifetime")
	flag.DurationVar(&c.Duration, "duration", time.Hour, "total workload duration (logical time in simulated mode)")
	flag.Float64Var(&c.WarmupFraction, "warmup-fraction", .5, "fraction of duration for warm-up")
	flag.IntVar(&c.CacheSize, "cache-size", 0, "cache capacity; 0 uses production default")
	flag.DurationVar(&c.Lifetime, "ad-lifetime", 0, "advertisement lifetime; 0 uses production default")
	flag.StringVar(&c.IPFile, "ip-file", "results/data/dnslist-ipv4.txt", "IPv4 text or .csv dataset for --ip-dist=empirical")
	flag.StringVar(&c.IPColumn, "ip-column", "ip", "CSV IPv4 column")
	flag.StringVar(&c.IPDist, "ip-dist", "", "empirical, diverse (unique random public IPv4) or concentrated (--ip-prefixes /16s); default empirical if --ip-file is set")
	flag.IntVar(&c.IPPrefixes, "ip-prefixes", 8, "number of /16 prefixes for --ip-dist=concentrated")
	flag.BoolVar(&c.IndependentENR, "independent-enr", true, "decode a fresh ENR copy for every REGTOPIC, as production does")
	flag.BoolVar(&c.RawOps, "raw-ops", true, "write one JSONL record per operation")
	flag.StringVar(&c.Pacing, "pacing", "sleep", "realtime idle behaviour between arrivals: sleep or spin")
	flag.BoolVar(&c.AllowTranslated, "allow-translated", false, "allow performance_valid for a Rosetta-translated process")
	flag.DurationVar(&c.SampleInterval, "sample-interval", time.Second, "state sampling interval")
	flag.StringVar(&c.Output, "output", "registrarbench.jsonl", "JSONL output path, or - for stdout")
	flag.BoolVar(&c.AllocationAudit, "allocation-audit", false, "instrument allocation counters per method; invalidates performance timing")
	flag.StringVar(&c.CPUProfile, "cpu-profile", "", "optional measurement-phase CPU profile path")
	flag.StringVar(&c.HeapProfile, "heap-profile", "", "optional end-of-measurement heap profile path")
	flag.StringVar(&c.MemfillLevels, "memfill-levels", "0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1", "memfill: occupancy levels as fractions of capacity")
	flag.IntVar(&c.MemfillReps, "memfill-reps", 5, "memfill: repetitions")
	flag.IntVar(&c.ENRPad, "enr-pad", 0, "add an opaque ENR entry of this many bytes (records must stay within 300 bytes)")
	flag.Parse()
	return c
}
func (c config) warmup() time.Duration {
	if c.Warmup != nil {
		return *c.Warmup
	}
	return time.Duration(float64(c.Duration) * c.WarmupFraction)
}
func (c config) ipDist() string {
	if c.IPDist != "" {
		return c.IPDist
	}
	if c.IPFile != "" {
		return ipDistEmpirical
	}
	return ipDistDiverse
}
func (c config) validate() error {
	switch c.Mode {
	case "realtime", "simulated", "hybrid", "memfill":
	default:
		return fmt.Errorf("unknown mode %q", c.Mode)
	}
	if c.Mode != "memfill" && (c.warmup() < 0 || c.warmup() >= c.Duration) {
		return fmt.Errorf("warmup must be in [0,duration)")
	}
	if c.Arrivals != "deterministic" && c.Arrivals != "poisson" {
		return fmt.Errorf("unknown arrival process %q", c.Arrivals)
	}
	if c.Abandon != abandonProduction && c.Abandon != abandonSpec && c.Abandon != abandonNone {
		return fmt.Errorf("unknown abandonment policy %q", c.Abandon)
	}
	if c.Pacing != "sleep" && c.Pacing != "spin" {
		return fmt.Errorf("unknown pacing %q", c.Pacing)
	}
	if c.ipDist() == ipDistEmpirical && c.IPFile == "" {
		return fmt.Errorf("--ip-dist=empirical needs --ip-file")
	}
	if c.ipDist() == ipDistConcentrated && (c.IPPrefixes <= 0 || c.IPPrefixes > 65536) {
		return fmt.Errorf("--ip-prefixes must be in [1,65536]")
	}
	finite := func(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
	if c.Advertisers <= 0 || c.Topics <= 0 || !finite(c.Alpha) || c.Alpha < 0 || !finite(c.FreshRate) || c.FreshRate < 0 || c.FreshRate > 1e9 || !finite(c.QueryRate) || c.QueryRate < 0 || c.QueryRate > 1e9 || c.Duration <= 0 || c.SampleInterval <= 0 || c.CacheSize < 0 || c.Lifetime < 0 || c.AttemptTimeout < 0 || !finite(c.WarmupFraction) || c.WarmupFraction < 0 || c.WarmupFraction >= 1 {
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

// div returns a/b, or nil (JSON null) when the result is not finite.
func div(a, b float64) any {
	if v := a / b; !math.IsNaN(v) && !math.IsInf(v, 0) {
		return v
	}
	return nil
}

// seconds converts a nanosecond histogram summary to seconds.
func secondsSummary(h *histogram) map[string]any {
	return map[string]any{"count": h.Count, "mean_s": h.Sum / float64(max(h.Count, 1)) / 1e9,
		"median_s_upper": float64(h.quantile(.5)) / 1e9, "p95_s_upper": float64(h.quantile(.95)) / 1e9, "p99_s_upper": float64(h.quantile(.99)) / 1e9}
}

// rates returns per-second rates of a counter window.
func rates(c counters, seconds float64) map[string]float64 {
	r := func(n uint64) float64 { return float64(n) / seconds }
	return map[string]float64{"fresh_arrival_rps": r(c.FreshArrivals), "registration_rps": r(c.Registrations), "fresh_attempt_rps": r(c.Fresh),
		"retry_rps": r(c.Retries), "admission_rps": r(c.Admissions), "wait_rps": r(c.Waits), "abandon_rps": r(c.Abandoned),
		"expired_retry_rps": r(c.ExpiredRetries), "scarcity_rps": r(c.PopulationScarcity), "query_rps": r(c.Queries)}
}

func windowStats(c counters, seconds, occupancySeconds float64) map[string]any {
	return map[string]any{"seconds": seconds, "time_avg_occupancy": div(occupancySeconds, seconds), "rates": rates(c, seconds),
		"wait_fraction_of_requests": ratio(c.Waits, c.Registrations), "abandon_fraction_of_fresh": ratio(c.Abandoned, c.Fresh)}
}

func run(c config, out io.Writer) error {
	if c.Mode == "" {
		c.Mode = "realtime"
	}
	if c.Arrivals == "" {
		c.Arrivals = "deterministic"
	}
	if c.Abandon == "" {
		c.Abandon = abandonProduction
	}
	if c.Pacing == "" {
		c.Pacing = "sleep"
	}
	if err := c.validate(); err != nil {
		return err
	}
	if err := os.Setenv("GODEBUG", os.Getenv("GODEBUG")+",randseednop=0"); err != nil {
		return err
	}
	// Production reservoir sampling uses the global math/rand source.
	rand.Seed(c.Seed + 4)
	plat := detectPlatform()
	encoder := json.NewEncoder(out)
	if c.Mode == "memfill" {
		return runMemfill(c, plat, encoder)
	}
	w, data, err := population(c)
	if err != nil {
		return err
	}
	e := newEngine(c, w)
	tab := e.table
	c.CacheSize = tab.Stats().Capacity
	c.Lifetime = tab.AdLifetime()
	if c.AttemptTimeout == 0 {
		// topicindex.Config.withDefaults: RegAttemptTimeout = E + E/2.
		c.AttemptTimeout = c.Lifetime + c.Lifetime/2
	}
	e.cfg = c
	perfValid, perfReasons := performanceValid(c, plat)
	topicCounts := make([]int, c.Topics)
	runtime.GC()
	baseline := memorySample(0)
	baseline.Incremental = 0
	if err := encoder.Encode(map[string]any{"type": "metadata", "schema_version": 6, "population_model": "fixed-reusable-v3",
		"mode": c.Mode, "registration_semantics": "independent-fresh-arrivals-v1", "config": c, "ip_dist": c.ipDist(), "dataset": data, "platform": plat,
		"go_version": plat.GoVersion, "revision": plat.Revision, "dirty": plat.Dirty, "os": plat.GOOS, "arch": plat.GOARCH,
		"godebug": os.Getenv("GODEBUG"), "gogc_env": os.Getenv("GOGC"), "gomemlimit_env": os.Getenv("GOMEMLIMIT"), "baseline": baseline,
		"topic_populations": w.population, "result_limit": 16, "query_requester": queryRequester.String(), "warmup_ns": int64(c.warmup()),
		"retry_validity_ns": int64(retryValidityWindow), "attempt_timeout_ns": int64(c.AttemptTimeout)}); err != nil {
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
	e.occupancy = occupancyIntegral{start: c.warmup(), end: c.Duration, mid: c.warmup() + (c.Duration-c.warmup())/2}
	begin := func() error {
		if e.scheduler.hybrid != nil {
			e.scheduler.hybrid.switchToReal()
		}
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
		var err error
		if startCPU, err = cpuSeconds(); err != nil {
			return err
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
		rec := map[string]any{"type": "sample", "mode": c.Mode, "elapsed_ns": int64(e.scheduler.elapsed()), "wall_elapsed_ns": int64(time.Since(e.scheduler.wallStart)),
			"measurement": e.measuring, "state": stats, "utilisation": float64(stats.Occupancy) / float64(c.CacheSize), "ads_per_topic": topicCounts,
			"memory": memorySample(baseline.Live), "counts": e.total, "pending_retries": len(w.retries), "ready_advertisers": w.readyCount()}
		cpu, err := cpuSeconds()
		if err != nil {
			return err
		}
		rec["process_cpu_seconds"] = cpu
		return encoder.Encode(rec)
	}
	emit := func(op operation) error {
		if e.measuring && c.RawOps {
			return encoder.Encode(op)
		}
		return nil
	}
	e.scheduler.start()
	nextSample := time.Duration(0)
	for {
		now := e.scheduler.elapsed()
		e.occupancy.add(now, tab.Occupancy())
		if now >= c.Duration {
			break
		}
		if !e.measuring && now >= c.warmup() {
			if err := begin(); err != nil {
				return err
			}
			now = e.scheduler.elapsed()
		}
		if e.measuring && !e.halfTaken && now >= e.occupancy.mid {
			e.halfStart, e.halfTaken = e.measured, true
		}
		if e.nextExpiry() <= now {
			if err := emit(e.expire()); err != nil {
				return err
			}
			continue
		}
		w.release(e.scheduler.clock.Now())
		if now >= nextSample {
			if err := sample(); err != nil {
				return err
			}
			nextSample = now - now%c.SampleInterval + c.SampleInterval
			continue
		}
		event := e.nextWork()
		next := min(event.due, e.nextExpiry(), e.nextRelease(), nextSample, c.Duration)
		if !e.measuring {
			next = min(next, c.warmup())
		} else if !e.halfTaken {
			next = min(next, e.occupancy.mid)
		}
		if now < next {
			e.scheduler.advance(next)
			continue
		}
		if event.due <= now {
			op, err := e.execute(event)
			if err != nil {
				return err
			}
			if err := emit(op); err != nil {
				return err
			}
		}
	}
	if !e.measuring {
		return fmt.Errorf("measurement phase never started")
	}
	if !e.halfTaken {
		e.halfStart = e.measured
	}
	elapsed := e.scheduler.elapsed()
	seconds := (min(elapsed, c.Duration) - measureLogical).Seconds()
	wall := time.Since(measureWall).Seconds()
	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)
	endCPU, err := cpuSeconds()
	if err != nil {
		return err
	}
	if profile != nil {
		pprof.StopCPUProfile()
		profile.Close()
		profile = nil
	}
	m := &e.measured
	half := (c.Duration - measureLogical).Seconds() / 2
	timeAvg := (e.occupancy.halves[0] + e.occupancy.halves[1]) / max(seconds, 1e-9)
	admissionRate := float64(m.Admissions) / seconds
	realtime := e.scheduler.realtime()
	summary := map[string]any{"type": "summary", "mode": c.Mode, "allocation_audit": c.AllocationAudit,
		"performance_valid": perfValid && realtime, "performance_invalid_reasons": perfReasons, "realtime_metrics_valid": perfValid && realtime,
		"load_valid": m.PopulationScarcity == 0, "population_scarcity_any": e.total.PopulationScarcity > 0,
		"measurement_seconds": seconds, "measurement_wall_seconds": wall, "measurement_start_ns": int64(measureLogical),
		"logical_elapsed_ns": int64(elapsed), "wall_elapsed_ns": int64(time.Since(e.scheduler.wallStart)),
		"counts": e.measured, "total_counts": e.total, "rates": rates(*m, seconds),
		"configured_fresh_rate": c.FreshRate, "configured_query_rate": c.QueryRate,
		"time_avg_occupancy": div(timeAvg, 1), "time_avg_utilisation": div(timeAvg, float64(c.CacheSize)),
		"littles_law_expected_occupancy": admissionRate * c.Lifetime.Seconds(),
		"littles_law_ratio":              div(timeAvg, admissionRate*c.Lifetime.Seconds()),
		"halves":                         []map[string]any{windowStats(e.halfStart, half, e.occupancy.halves[0]), windowStats(m.sub(e.halfStart), half, e.occupancy.halves[1])},
		"wait_fraction_of_requests":      ratio(m.Waits, m.Registrations), "immediate_admission_fraction_of_fresh": ratio(m.Immediate, m.Fresh),
		"abandon_fraction_of_fresh": ratio(m.Abandoned, m.Fresh), "retries_per_admission": ratio(m.Retries, m.Admissions),
		"registration_latency": m.RegLatency.summary(), "query_latency": m.QueryLatency.summary(), "expiry_latency": m.ExpiryLatency.summary(),
		"queue_delay": m.QueueDelay.summary(), "quoted_wait": secondsSummary(&m.QuotedWait), "cumulative_wait_at_admission": secondsSummary(&m.AdmittedWait),
		"cumulative_wait_at_admission_after_wait": secondsSummary(&m.EventualWait), "credited_wait_at_abandonment": secondsSummary(&m.AbandonedAfter),
		"pending_retries": len(w.retries), "ready_advertisers": w.readyCount(),
		"unserved_fresh_arrivals": e.fresh.pendingBefore(c.Duration), "unserved_queries": e.query.pendingBefore(c.Duration),
		"cpu_seconds": endCPU - startCPU, "allocation_bytes": endMem.TotalAlloc - startMem.TotalAlloc, "allocations": endMem.Mallocs - startMem.Mallocs}
	overdue := 0
	for _, r := range w.retries {
		if r.at < e.scheduler.origin.Add(c.Duration) {
			overdue++
		}
	}
	summary["overdue_retries"] = overdue
	if realtime {
		summary["cpu_percent_one_core"] = 100 * (endCPU - startCPU) / wall
		ops := m.Registrations + m.Queries + m.Expiries
		summary["cpu_us_per_operation"] = 1e6 * (endCPU - startCPU) / float64(max(ops, 1))
	}
	if err := sample(); err != nil {
		return err
	}
	runtime.GC()
	summary["post_measurement_forced_gc_memory"] = memorySample(baseline.Live)
	summary["final_state"] = tab.Stats()
	if c.HeapProfile != "" {
		f, err := os.Create(c.HeapProfile)
		if err != nil {
			return err
		}
		err = pprof.WriteHeapProfile(f)
		f.Close()
		if err != nil {
			return err
		}
	}
	if c.AllocationAudit {
		summary["audit_per_operation"] = map[string]float64{"registration_bytes": ratio(m.RegBytes, m.Registrations), "registration_allocations": ratio(m.RegAllocs, m.Registrations), "query_bytes": ratio(m.QueryBytes, m.Queries), "query_allocations": ratio(m.QueryAllocs, m.Queries), "expiry_bytes": ratio(m.ExpiryBytes, m.Expiries), "expiry_allocations": ratio(m.ExpiryAllocs, m.Expiries)}
	}
	for k, v := range summary {
		if _, err := json.Marshal(v); err != nil {
			return fmt.Errorf("summary field %s: %w", k, err)
		}
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
