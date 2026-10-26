package main

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
)

func testEngine(t *testing.T) *engine {
	t.Helper()
	c := testConfig()
	c.Mode = "simulated"
	c.Lifetime = time.Minute
	c.RegRate = 10
	c.QueryRate = 0
	w, _, err := population(c)
	if err != nil {
		t.Fatal(err)
	}
	e := newEngine(c, w)
	e.scheduler.start()
	return e
}

// Distinct topics make these scheduling fixtures admit each selected identity
// without additional waits, isolating the preloaded retry demand.
func cappedEngine(t *testing.T, retries int) *engine {
	t.Helper()
	c := testConfig()
	c.Mode = "simulated"
	c.Advertisers = 100
	c.Topics = 100
	c.RegRate = 20
	c.QueryRate = 0
	c.Lifetime = time.Hour
	w, _, err := population(c)
	if err != nil {
		t.Fatal(err)
	}
	for i := range w.ads {
		w.ads[i].topic = i
		w.population[i] = 1
	}
	e := newEngine(c, w)
	e.scheduler.start()
	for i := 0; i < retries; i++ {
		id, _ := w.next()
		if err := w.finish(id, 0, 0, time.Second, c.Lifetime); err != nil {
			t.Fatal(err)
		}
	}
	if retries > 0 {
		e.scheduler.advance(time.Second)
		for i := 0; i < 20; i++ {
			e.pacer.consume()
		}
	}
	return e
}
func driveUntil(t *testing.T, e *engine, end time.Duration) []operation {
	t.Helper()
	var ops []operation
	for e.scheduler.elapsed() < end {
		event := e.nextWork()
		if event.due >= end {
			e.scheduler.advance(end)
			break
		}
		if event.due > e.scheduler.elapsed() {
			e.scheduler.advance(event.due)
			continue
		}
		if event.kind == "wake" {
			continue
		}
		op, err := e.execute(event)
		if err != nil {
			t.Fatal(err)
		}
		ops = append(ops, op)
	}
	return ops
}
func TestFreshFillsCappedCapacity(t *testing.T) {
	e := cappedEngine(t, 5)
	ops := driveUntil(t, e, 2*time.Second)
	if e.total.Retries != 5 || e.total.Fresh != 15 || len(ops) != 20 {
		t.Fatal("expected 5 retry + 15 fresh", e.total.Retries, e.total.Fresh)
	}
	for i, op := range ops {
		if op.Fresh != (i >= 5) {
			t.Fatal("retry priority", i, op)
		}
	}
}
func TestRetryOverloadQueues(t *testing.T) {
	e := cappedEngine(t, 27)
	ops := driveUntil(t, e, 2*time.Second)
	if len(ops) != 20 || e.total.Retries != 20 || e.total.Fresh != 0 || len(e.queued) != 7 || e.total.MaxRetryQueue != 27 || e.total.RetriesQueued != 27 {
		t.Fatal("retry cap/queue", len(ops), len(e.queued))
	}
	for i := 1; i < len(ops); i++ {
		if ops[i].Started-ops[i-1].Started < int64(50*time.Millisecond) {
			t.Fatal("burst")
		}
	}
}
func TestFixedRegistrationArrivals(t *testing.T) {
	for _, delay := range []time.Duration{time.Millisecond, 80 * time.Millisecond} {
		e := cappedEngine(t, 0)
		e.cfg.Mode = "realtime" // injected logical clock models serial worker time deterministically
		var previousLag int64
		for i := 0; i < 8; i++ {
			event := e.nextWork()
			e.scheduler.advance(event.due)
			event = e.nextWork()
			op, err := e.execute(event)
			if err != nil {
				t.Fatal(err)
			}
			if op.Scheduled != int64(i)*int64(50*time.Millisecond) {
				t.Fatal("completion shifted schedule", op)
			}
			if op.Queue == nil || *op.Queue != op.Started-op.Scheduled {
				t.Fatal("original arrival lost", op)
			}
			if delay > 50*time.Millisecond && i > 0 && *op.Queue <= previousLag {
				t.Fatal("overload lag did not grow")
			}
			previousLag = *op.Queue
			// Stand-in for additional serial operation execution, without a real sleep.
			e.scheduler.simulated.Run(delay)
		}
	}
}
func TestScarcityConsumesScheduledOpportunity(t *testing.T) {
	e := cappedEngine(t, 0)
	e.workload.ready = e.workload.ready[:8]
	driveUntil(t, e, 400*time.Millisecond)
	op, err := e.execute(e.nextWork())
	if err != nil {
		t.Fatal(err)
	}
	if op.Type != "population_scarcity" || e.total.PopulationScarcity != 1 || e.total.Registrations != 8 || e.pacer.next != 450*time.Millisecond {
		t.Fatal("scarcity changed schedule or called Register", op)
	}
}
func TestQueryArrivalIndependentOfCompletion(t *testing.T) {
	e := cappedEngine(t, 0)
	e.cfg.RegRate = 0
	e.pacer = newPacer(0)
	e.cfg.QueryRate = 20
	e.cfg.Mode = "realtime"
	for i := 0; i < 5; i++ {
		event := e.nextWork()
		op, err := e.execute(event)
		if err != nil {
			t.Fatal(err)
		}
		if op.Kind != "query" || op.Scheduled != int64(i)*int64(50*time.Millisecond) || *op.Queue != int64(i)*int64(30*time.Millisecond) {
			t.Fatal(op)
		}
		e.scheduler.simulated.Run(80 * time.Millisecond)
	}
}
func TestQueuedRetryExpiryWithoutRestart(t *testing.T) {
	e := cappedEngine(t, 7)
	e.cfg.RegRate = 1
	e.pacer = newPacer(1)
	e.pacer.consume()
	w := e.workload
	w.ready = w.ready[:w.head]
	ops := driveUntil(t, e, 6*time.Second)
	for i := 0; i < 2; i++ {
		op, err := e.execute(e.nextWork())
		if err != nil {
			t.Fatal(err)
		}
		ops = append(ops, op)
	}
	if len(ops) != 7 || e.total.Retries != 5 || e.total.ExpiredRetries != 2 || e.table.Occupancy() != 5 || len(w.ready)-w.head != 2 || len(e.queued) != 0 {
		t.Fatal("expiry sent/restarted retry", e.total.Retries, e.total.ExpiredRetries)
	}
	// Expiry leaves the ordinary arrival at 6s untouched. Only dispatching that
	// opportunity can issue a fresh call, with all previous waiting credit reset.
	index := e.pacer.index
	for i := 0; i < 3; i++ {
		if e.nextWork().kind != "fresh" {
			t.Fatal("expired entry survived in retry queue")
		}
	}
	if len(w.ready)-w.head != 2 || e.total.Registrations != 5 || e.pacer.index != index {
		t.Fatal("expiry duplicated eligibility or issued a call")
	}
	op, err := e.execute(e.nextWork())
	if err != nil {
		t.Fatal(err)
	}
	if !op.Fresh || op.Credited != 0 || op.Scheduled != int64(6*time.Second) || e.total.Registrations != 6 || len(w.ready)-w.head != 1 {
		t.Fatal("recycled advertiser did not use ordinary fresh opportunity", op)
	}
	for _, op := range ops {
		if op.Type == "retry_expired" && op.Started != int64(6*time.Second) {
			t.Fatal("late expiration", op)
		}
	}
}
func TestProductionRetryValidity(t *testing.T) {
	for _, offset := range []time.Duration{0, 5*time.Second - time.Nanosecond, 5 * time.Second} {
		clock := new(mclock.Simulated)
		sealer := topicindex.NewTicketSealer(clock)
		ticket := topicindex.Ticket{WaitTimeIssued: time.Second}
		encoded := sealer.Pack(&ticket)
		clock.Run(time.Second + offset)
		_, err := sealer.Unpack(ticket.Topic, encoded)
		if (err != nil) != retryExpired(mclock.AbsTime(time.Second), clock.Now()) {
			t.Fatal("production grace boundary differs", offset, err)
		}
	}
}

func TestExpiredRealtimeRetryRecycled(t *testing.T) {
	e := testEngine(t)
	e.cfg.Mode = "realtime" // deterministic overloaded realtime dispatch using injected clock
	w := e.workload
	id, _ := w.next()
	if err := w.finish(id, 0, 0, time.Second, e.cfg.Lifetime); err != nil {
		t.Fatal(err)
	}
	e.scheduler.advance(6 * time.Second)
	op, err := e.execute(e.nextWork())
	if err != nil {
		t.Fatal(err)
	}
	if op.Type != "retry_expired" || e.total.ExpiredRetries != 1 || e.total.Registrations != 0 || e.table.Occupancy() != 0 || w.ads[id].waiting || w.ads[id].first != 0 {
		t.Fatal("expired credit retained", op)
	}
	occurrences := 0
	for _, readyID := range w.ready[w.head:] {
		if readyID == id {
			occurrences++
		}
	}
	if occurrences != 1 || len(e.queued) != 0 || e.pacer.index != 0 {
		t.Fatal("realtime expiry did not recycle exactly once")
	}
	if op.RetryDue != int64(time.Second) || op.Started-op.RetryDue != int64(5*time.Second) {
		t.Fatal("queue delay", op.Queue)
	}
}

func TestSharedLifecycle(t *testing.T) {
	for _, mode := range []string{"simulated", "realtime"} {
		e := testEngine(t)
		e.cfg.Mode = mode
		event := e.nextWork()
		op, err := e.execute(event)
		if err != nil {
			t.Fatal(err)
		}
		if op.Outcome != "admitted" || len(e.workload.active) != 1 {
			t.Fatal(op)
		}
		id := event.id
		e.scheduler.advance(e.cfg.Lifetime)
		e.expire()
		e.workload.release(e.scheduler.clock.Now())
		if e.table.Occupancy() != 0 || e.workload.ads[id].waiting {
			t.Fatal("expiry lifecycle", mode)
		}
		// Isolate the newly released advertiser to verify a fresh admission, not renewal.
		e.workload.ready = []int{id}
		e.workload.head = 0
		op, err = e.execute(e.nextWork())
		if err != nil || !op.Fresh || op.Outcome != "admitted" {
			t.Fatal(op, err)
		}
	}
}

func TestSimulatedMinutesAndWarmup(t *testing.T) {
	c := testConfig()
	c.Mode = "simulated"
	c.Duration = 3 * time.Minute
	c.SampleInterval = 30 * time.Second
	c.Lifetime = time.Minute
	c.RegRate = 5
	c.QueryRate = 7
	var out bytes.Buffer
	start := time.Now()
	if err := run(c, &out); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatal("simulation waited for wall time")
	}
	decoder := json.NewDecoder(&out)
	var summary map[string]any
	hadWarmState := false
	retries := 0
	for decoder.More() {
		var r map[string]any
		if err := decoder.Decode(&r); err != nil {
			t.Fatal(err)
		}
		switch r["type"] {
		case "sample":
			if r["measurement"] == false && r["utilisation"].(float64) > 0 {
				hadWarmState = true
			}
		case "operation":
			if _, ok := r["queue_delay_ns"]; ok {
				t.Fatal("simulated queue delay")
			}
			if r["started_ns"].(float64) < float64(c.warmup()) {
				t.Fatal("warmup counted")
			}
			if r["operation"] == "registration" && r["fresh"] == false {
				retries++
				if delay := r["started_ns"].(float64) - r["retry_due_ns"].(float64); delay < 0 || delay >= float64(retryValidityWindow) {
					t.Fatal("retry outside validity window")
				}
			}
		case "summary":
			summary = r
		}
	}
	counts := summary["counts"].(map[string]any)
	total := summary["total_counts"].(map[string]any)
	if total["registrations_processed"].(float64)+total["population_scarcity_events"].(float64) != 900 || summary["pregenerated_advertisers"] != float64(c.Advertisers) || summary["population_auto_sized"] != false || summary["population_limited"] != true {
		t.Fatal("target demand/population not sustained", summary)
	}
	if summary["logical_elapsed_ns"] != float64(c.Duration) || counts["queries_processed"] != float64(90*7) || !hadWarmState || retries == 0 {
		t.Fatal("logical time/warmup/query/retry mismatch", summary)
	}
	if _, ok := summary["cpu_percent_one_core"]; ok {
		t.Fatal("simulated CPU utilisation")
	}
	if counts["registrations_processed"] != counts["fresh_attempts"].(float64)+counts["retry_attempts"].(float64) {
		t.Fatal("composition")
	}
}

func runSummary(t *testing.T, c config) map[string]any {
	t.Helper()
	var out bytes.Buffer
	if err := run(c, &out); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(&out)
	var summary map[string]any
	for decoder.More() {
		var r map[string]any
		if err := decoder.Decode(&r); err != nil {
			t.Fatal(err)
		}
		if r["type"] == "metadata" {
			if r["actual_advertisers"] != float64(c.Advertisers) || r["population_auto_sized"] != false {
				t.Fatal("population expanded", r)
			}
		}
		if r["type"] == "summary" {
			summary = r
		}
	}
	if summary == nil {
		t.Fatal("missing summary")
	}
	return summary
}
func TestFixedPopulationAndScarcitySummary(t *testing.T) {
	c := testConfig()
	c.Mode = "simulated"
	c.Advertisers = 100
	c.Duration = 3 * time.Second
	c.RegRate = 100
	c.Lifetime = time.Hour
	s := runSummary(t, c)
	if s["requested_advertisers"] != float64(100) || s["actual_advertisers"] != float64(100) {
		t.Fatal("fixed population")
	}
	c.Advertisers = 1
	c.Topics = 1
	c.Duration = 2 * time.Second
	c.RegRate = 20
	s = runSummary(t, c)
	if s["population_limited_any"] != true || s["population_limited_measurement"] != true || s["population_limited"] != true || s["population_scarcity_events"] != float64(39) || s["population_scarcity_fraction"] != 39.0/40 {
		t.Fatal("scarcity accounting", s)
	}
	if s["total_counts"].(map[string]any)["registrations_processed"] != float64(1) {
		t.Fatal("active advertiser reused")
	}
	c.Advertisers = 100
	s = runSummary(t, c)
	if s["population_limited_any"] != false || s["population_limited_measurement"] != false || s["population_limited"] != false || s["population_scarcity_events"] != float64(0) || s["total_counts"].(map[string]any)["registrations_processed"] != float64(40) {
		t.Fatal("adequate population not serving load", s)
	}
}
func TestLongSimulationReusesFixedIdentity(t *testing.T) {
	c := testConfig()
	c.Mode = "simulated"
	c.Advertisers = 1
	c.Topics = 1
	c.RegRate = 2
	c.QueryRate = 0
	c.Duration = 3 * time.Minute
	c.Lifetime = time.Second
	c.SampleInterval = 30 * time.Second
	start := time.Now()
	s := runSummary(t, c)
	total := s["total_counts"].(map[string]any)
	if time.Since(start) > 10*time.Second || total["new_admissions"] != float64(180) || total["population_scarcity_events"] != float64(180) || total["duplicate_results"] != float64(0) {
		t.Fatal("fixed identity not reused correctly", total)
	}
}
func TestWaitingAdvertiserNotSelectedFresh(t *testing.T) {
	e := testEngine(t)
	w := e.workload
	id, _ := w.next()
	w.ready = w.ready[:w.head]
	if err := w.finish(id, 0, 0, time.Second, e.cfg.Lifetime); err != nil {
		t.Fatal(err)
	}
	op, err := e.execute(e.nextWork())
	if err != nil {
		t.Fatal(err)
	}
	if op.Type != "population_scarcity" || e.total.Registrations != 0 || len(w.retries) != 1 {
		t.Fatal("waiting identity selected fresh", op)
	}
}
func TestWaitingHeapIndependentOfFreshQueue(t *testing.T) {
	e := testEngine(t)
	w := e.workload
	id, _ := w.next()
	if err := w.finish(id, 0, 0, time.Second, time.Minute); err != nil {
		t.Fatal(err)
	}
	item := heap.Pop(&w.retries).(wake)
	if item.id != id || item.at != mclock.AbsTime(time.Second) {
		t.Fatal(item)
	}
}

func TestSimulatedDeterminism(t *testing.T) {
	c := testConfig()
	c.Mode = "simulated"
	c.Duration = 30 * time.Second
	c.Lifetime = 5 * time.Second
	c.SampleInterval = 10 * time.Second
	c.RegRate = 10
	c.QueryRate = 5
	semantic := func() string {
		var out bytes.Buffer
		if err := run(c, &out); err != nil {
			t.Fatal(err)
		}
		decoder := json.NewDecoder(&out)
		var selected []map[string]any
		for decoder.More() {
			var r map[string]any
			if err := decoder.Decode(&r); err != nil {
				t.Fatal(err)
			}
			if r["type"] == "operation" {
				delete(r, "latency_ns")
				delete(r, "wall_elapsed_ns")
				selected = append(selected, r)
			}
		}
		data, err := json.Marshal(selected)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	if semantic() != semantic() {
		t.Fatal("seeded simulated lifecycle is not reproducible")
	}
}

func TestSupplyCountersRespectMeasurementBoundary(t *testing.T) {
	e := testEngine(t)
	e.workload.head = len(e.workload.ready)
	e.checkSupply(time.Second)
	if e.measured.FreshSupplyLimitedNS != 0 {
		t.Fatal("warmup scarcity counted")
	}
	e.measuring = true
	e.checkSupply(2 * time.Second)
	if e.total.FreshSupplyLimitedNS != int64(2*time.Second) || e.measured.FreshSupplyLimitedNS != int64(time.Second) {
		t.Fatal("scarcity boundary")
	}
}

func TestRealtimeScheduledStarts(t *testing.T) {
	c := testConfig()
	c.Mode = "realtime"
	c.RegRate = 20
	c.Duration = 600 * time.Millisecond
	c.WarmupFraction = 0
	c.Lifetime = time.Second
	var out bytes.Buffer
	if err := run(c, &out); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(&out)
	count := 0
	for decoder.More() {
		var r map[string]any
		if err := decoder.Decode(&r); err != nil {
			t.Fatal(err)
		}
		if r["type"] == "operation" && r["operation"] == "registration" {
			scheduled := int64(r["scheduled_ns"].(float64))
			if scheduled != int64(count)*int64(50*time.Millisecond) {
				t.Fatal("shifted realtime arrivals")
			}
			if r["queue_delay_ns"].(float64) != r["started_ns"].(float64)-r["scheduled_ns"].(float64) {
				t.Fatal("queue lag")
			}

			count++
		}
	}
	if count == 0 || count > 12 {
		t.Fatal("realtime cap", count)
	}
}

func TestZeroRateStillExpiresRetries(t *testing.T) {
	e := cappedEngine(t, 2)
	e.cfg.RegRate = 0
	e.pacer = newPacer(0)
	ops := driveUntil(t, e, 7*time.Second)
	if len(ops) != 2 || e.total.Registrations != 0 || e.total.ExpiredRetries != 2 {
		t.Fatal("zero-rate expiry")
	}
}

func TestExplicitIPFileRejectsEmptyOrInvalidBeforeTiming(t *testing.T) {
	for _, input := range []string{"", "# no addresses\n\n", "invalid\n", "2001:db8::1\n", "8.8.8.8\ninvalid\n"} {
		c := testConfig()
		c.IPFile = filepath.Join(t.TempDir(), "ips.txt")
		if err := os.WriteFile(c.IPFile, []byte(input), 0600); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := run(c, &out); err == nil || out.Len() != 0 {
			t.Fatal("invalid empirical input did not fail before timing", input, err)
		}
	}
}

func TestWarmupScarcityMarksWholeRun(t *testing.T) {
	c := testConfig()
	c.Mode = "simulated"
	c.Advertisers = 1
	c.Topics = 1
	c.RegRate = 1
	c.QueryRate = 0
	c.Duration = 2500 * time.Millisecond
	c.Lifetime = 1500 * time.Millisecond
	warmup := 1750 * time.Millisecond
	c.Warmup = &warmup
	s := runSummary(t, c)
	if s["population_limited_any"] != true || s["population_limited_measurement"] != false || s["population_limited"] != true || s["population_scarcity_events"] != float64(1) || s["measured_population_limited"] != false || s["measured_population_scarcity_events"] != float64(0) {
		t.Fatal("warmup scarcity silently excluded", s)
	}
}

// Every identity must belong to exactly one lifecycle container after each event.
// This detects both duplicate fresh entries and population loss on retry expiry.
func assertPopulationConserved(t *testing.T, e *engine) {
	t.Helper()
	w := e.workload
	seen := make([]int, len(w.ads))
	for _, id := range w.ready[w.head:] {
		seen[id]++
		if w.ads[id].waiting {
			t.Fatal("waiting identity in fresh queue", id)
		}
	}
	for _, item := range w.active {
		seen[item.id]++
		if w.ads[item.id].waiting {
			t.Fatal("active identity waiting", item.id)
		}
	}
	for _, queue := range []wakes{w.retries, e.queued} {
		for _, item := range queue {
			seen[item.id]++
			if !w.ads[item.id].waiting {
				t.Fatal("retry identity not waiting", item.id)
			}
		}
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("advertiser %d belongs to %d lifecycle containers", id, n)
		}
	}
}

func TestRepeatedExpiredRetryRecycling(t *testing.T) {
	c := testConfig()
	c.Mode = "simulated"
	c.Advertisers = 2
	c.Topics = 1
	c.RegRate = .1
	c.QueryRate = 0
	c.CacheSize = 50
	c.Lifetime = 30 * time.Second
	c.Duration = 3 * time.Minute
	w, _, err := population(c)
	if err != nil {
		t.Fatal(err)
	}
	e := newEngine(c, w)
	e.scheduler.start()
	expired := make(map[int]int)
	recycled := make(map[int]int)
	for e.scheduler.elapsed() < c.Duration {
		now := e.scheduler.elapsed()
		if e.nextExpiry() <= now {
			e.expire()
		}
		w.release(e.scheduler.clock.Now())
		event := e.nextWork()
		assertPopulationConserved(t, e)
		next := min(event.due, e.nextExpiry(), e.nextRelease(), c.Duration)
		if next > now {
			e.scheduler.advance(next)
			continue
		}
		if event.kind == "wake" {
			continue
		}
		calls, arrivalIndex, occupancy := e.total.Registrations, e.pacer.index, e.table.Occupancy()
		op, err := e.execute(event)
		if err != nil {
			t.Fatal(err)
		}
		if op.Type == "retry_expired" {
			if e.total.Registrations != calls || e.pacer.index != arrivalIndex || e.table.Occupancy() != occupancy {
				t.Fatal("expiry made a registrar call or consumed a normal opportunity")
			}
			expired[op.Advertiser]++
		}
		if op.Type == "operation" && op.Kind == "registration" && op.Fresh && expired[op.Advertiser] > 0 {
			if op.Credited != 0 || op.Scheduled != int64(arrival(arrivalIndex, c.RegRate)) {
				t.Fatal("recycling bypassed normal fresh schedule", op)
			}
			recycled[op.Advertiser]++
		}
		assertPopulationConserved(t, e)
	}
	repeated := 0
	for id, n := range recycled {
		if n >= 2 && expired[id] >= 2 {
			repeated++
		}
	}
	if e.total.ExpiredRetries == 0 || repeated == 0 {
		t.Fatalf("fixture must expire/recycle repeatedly: expired=%d recycled=%d repeated=%d", e.total.ExpiredRetries, len(recycled), repeated)
	}
	t.Logf("expired retries=%d, recycled identities=%d, repeatedly recycled=%d", e.total.ExpiredRetries, len(recycled), repeated)
}
