package main

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"math"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

func simConfig() config {
	c := testConfig()
	c.Mode = "simulated"
	return c
}

func testEngine(t *testing.T, c config) *engine {
	t.Helper()
	w, _, err := population(c)
	if err != nil {
		t.Fatal(err)
	}
	if c.AttemptTimeout == 0 {
		c.AttemptTimeout = c.Lifetime + c.Lifetime/2
	}
	e := newEngine(c, w)
	e.scheduler.start()
	return e
}

// drive executes arrivals until end, expiring and releasing as the main loop does.
func drive(t *testing.T, e *engine, end time.Duration) []operation {
	t.Helper()
	var ops []operation
	for {
		now := e.scheduler.elapsed()
		if e.nextExpiry() <= now {
			e.expire()
			continue
		}
		e.workload.release(e.scheduler.clock.Now())
		event := e.nextWork()
		next := min(event.due, e.nextExpiry(), e.nextRelease())
		if next >= end {
			e.scheduler.advance(end)
			return ops
		}
		if next > now {
			e.scheduler.advance(next)
			continue
		}
		if event.due <= now {
			op, err := e.execute(event)
			if err != nil {
				t.Fatal(err)
			}
			ops = append(ops, op)
		}
	}
}

func readRecords(t *testing.T, out *bytes.Buffer) []map[string]any {
	t.Helper()
	var recs []map[string]any
	d := json.NewDecoder(out)
	for d.More() {
		var r map[string]any
		if err := d.Decode(&r); err != nil {
			t.Fatal(err)
		}
		recs = append(recs, r)
	}
	return recs
}

func summaryOf(t *testing.T, recs []map[string]any) map[string]any {
	t.Helper()
	for _, r := range recs {
		if r["type"] == "summary" {
			return r
		}
	}
	t.Fatal("missing summary")
	return nil
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

// productionClient drives topicindex.Registration, the production advertiser
// state machine, through one attempt and reports whether it was dropped.
func productionClient(t *testing.T, quotes []time.Duration, timeout time.Duration) bool {
	t.Helper()
	clock := new(mclock.Simulated)
	var topic topicindex.TopicID
	cfg := topicindex.Config{Clock: clock, AdLifetime: time.Minute, RegAttemptTimeout: timeout, RegBucketSize: 1}
	r := topicindex.NewRegistration(topic, cfg)
	var rec enr.Record
	rec.Set(topicindex.TopicDiscoveryVersion)
	rec.Set(enr.IPv4{1, 2, 3, 4})
	var id enode.ID
	id[0] = 0xff
	r.AddNodes(nil, []*enode.Node{enode.SignNull(&rec, id)})
	for _, q := range quotes {
		att := r.Update()
		if att == nil {
			return true
		}
		r.StartRequest(att)
		r.HandleTicketResponse(att, []byte{1}, q)
		clock.Run(q)
	}
	return r.Update() == nil
}

func TestAbandonmentMatchesProductionClient(t *testing.T) {
	const timeout = 90 * time.Second
	cases := [][]time.Duration{
		{timeout},
		{timeout + time.Millisecond},
		{60 * time.Second, 30 * time.Second},
		{60 * time.Second, 30*time.Second + time.Millisecond},
		{10 * time.Second, 10 * time.Second, 10 * time.Second},
	}
	// The 1.5E limit applies to each single quote and to the sum of quotes;
	// both comparisons are strict.
	expected := []bool{false, true, false, true, false}
	for i, quotes := range cases {
		want := productionClient(t, quotes, timeout)
		if want != expected[i] {
			t.Fatalf("quotes %v: production client dropped=%v, test expects %v", quotes, want, expected[i])
		}
		w := &workload{ads: make([]advertiser, 1)}
		dropped := false
		for i, q := range quotes {
			reason, err := w.quoted(0, mclock.AbsTime(i), q, abandonProduction, timeout, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if reason != "" {
				dropped = true
				break
			}
		}
		if dropped != want {
			t.Fatalf("quotes %v: harness dropped=%v, production client dropped=%v", quotes, dropped, want)
		}
	}
}

func TestAbandonPolicies(t *testing.T) {
	const e = time.Minute
	for _, tc := range []struct {
		policy string
		quotes []time.Duration
		want   string
	}{
		{abandonSpec, []time.Duration{e}, ""},
		{abandonSpec, []time.Duration{e + time.Millisecond}, "single"},
		{abandonSpec, []time.Duration{e, e, e}, ""},
		{abandonNone, []time.Duration{100 * e}, ""},
		{abandonProduction, []time.Duration{e, e/2 + time.Millisecond}, "cumulative"},
		{abandonProduction, []time.Duration{2 * e}, "single"},
	} {
		w := &workload{ads: make([]advertiser, 1)}
		got := ""
		for _, q := range tc.quotes {
			reason, _ := w.quoted(0, 0, q, tc.policy, e+e/2, e)
			if reason != "" {
				got = reason
				break
			}
		}
		if got != tc.want {
			t.Errorf("%s %v: got %q, want %q", tc.policy, tc.quotes, got, tc.want)
		}
	}
}

func TestClientWaitRoundsUpToMilliseconds(t *testing.T) {
	w := &workload{ads: make([]advertiser, 1)}
	if _, err := w.quoted(0, 0, 1500*time.Microsecond+1, abandonNone, time.Hour, time.Hour); err != nil {
		t.Fatal(err)
	}
	if got := w.retries[0].at; got != mclock.AbsTime(2*time.Millisecond) {
		t.Fatalf("retry at %v, want 2ms", got)
	}
	if w.ads[0].quoteSum != 2*time.Millisecond {
		t.Fatalf("quote sum %v", w.ads[0].quoteSum)
	}
}

// An abandoned advertiser is not retried; it can only come back through a
// fresh arrival.
func TestAbandonedReturnsOnlyViaFreshArrival(t *testing.T) {
	c := simConfig()
	c.Advertisers, c.Topics, c.CacheSize = 50, 1, 1000
	c.Lifetime = time.Hour
	c.FreshRate, c.QueryRate = 10, 0
	c.AttemptTimeout = time.Millisecond // every wait is abandoned
	e := testEngine(t, c)
	ops := drive(t, e, 2*time.Second)
	abandoned := 0
	for _, op := range ops {
		if op.Kind == "registration" && !op.Fresh {
			t.Fatal("retry issued after abandonment")
		}
		if op.Outcome == "abandoned_single" {
			abandoned++
		}
	}
	if abandoned == 0 || len(e.workload.retries) != 0 {
		t.Fatalf("abandoned=%d pending retries=%d", abandoned, len(e.workload.retries))
	}
	if e.total.Abandoned != uint64(abandoned) || e.workload.readyCount() != c.Advertisers-int(e.total.Admissions)-0 {
		t.Fatalf("counters %+v ready %d", e.total, e.workload.readyCount())
	}
}

// Retries arrive at their own due time even when no fresh arrivals occur.
func TestRetriesIndependentOfFreshArrivals(t *testing.T) {
	c := simConfig()
	c.Advertisers, c.Topics, c.CacheSize = 50, 1, 1000
	c.Lifetime = time.Hour
	c.FreshRate, c.QueryRate = 0, 0
	c.Abandon = abandonNone
	e := testEngine(t, c)
	w := e.workload
	for i := 0; i < 5; i++ {
		id, _ := w.next()
		if _, err := w.quoted(id, e.scheduler.clock.Now(), time.Duration(i+1)*time.Second, abandonNone, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	ops := drive(t, e, 10*time.Second)
	if len(ops) < 5 {
		t.Fatalf("%d operations, want >= 5 retries", len(ops))
	}
	for i, op := range ops[:5] {
		if op.Fresh || op.Started != op.Scheduled || op.Scheduled != int64(time.Duration(i+1)*time.Second) {
			t.Fatalf("retry %d: %+v", i, op)
		}
	}
}

func TestArrivalProcesses(t *testing.T) {
	d := newArrivals(2, false, 1)
	for i := 0; i < 4; i++ {
		if d.next != time.Duration(i)*500*time.Millisecond {
			t.Fatalf("deterministic arrival %d at %v", i, d.next)
		}
		d.consume()
	}
	if d.pendingBefore(3*time.Second) != 2 {
		t.Fatal("deterministic backlog", d.pendingBefore(3*time.Second))
	}
	p, q := newArrivals(100, true, 7), newArrivals(100, true, 7)
	var gaps []time.Duration
	last := time.Duration(0)
	for i := 0; i < 100000; i++ {
		if p.next != q.next || p.next < last {
			t.Fatal("poisson arrivals not deterministic or not monotone")
		}
		gaps = append(gaps, p.next-last)
		last = p.next
		p.consume()
		q.consume()
	}
	rate := float64(len(gaps)) / last.Seconds()
	var sq float64
	mean := last.Seconds() / float64(len(gaps))
	for _, g := range gaps {
		sq += (g.Seconds() - mean) * (g.Seconds() - mean)
	}
	cv := math.Sqrt(sq/float64(len(gaps))) / mean
	if math.Abs(rate-100) > 2 || math.Abs(cv-1) > .02 {
		t.Fatalf("poisson rate %.2f cv %.3f", rate, cv)
	}
	if z := newArrivals(0, true, 1); z.next != time.Duration(math.MaxInt64) {
		t.Fatal("zero rate")
	}
}

// Scarcity loses the arrival; it is never banked and the run is load-invalid.
func TestScarcityIsNotBanked(t *testing.T) {
	c := simConfig()
	c.Advertisers, c.Topics = 2, 2
	c.Lifetime = time.Hour
	c.FreshRate, c.QueryRate = 100, 0
	c.Duration, c.WarmupFraction = 2*time.Second, .5
	c.SampleInterval = time.Second
	var out bytes.Buffer
	if err := run(c, &out); err != nil {
		t.Fatal(err)
	}
	s := summaryOf(t, readRecords(t, &out))
	counts := s["counts"].(map[string]any)
	if s["load_valid"] != false || counts["population_scarcity_events"].(float64) == 0 {
		t.Fatal("scarcity not flagged", s["load_valid"], counts)
	}
	if counts["fresh_attempts"].(float64)+counts["population_scarcity_events"].(float64) != counts["fresh_arrivals"].(float64) {
		t.Fatal("arrival accounting", counts)
	}
}

func TestIndependentENRCopies(t *testing.T) {
	c := simConfig()
	c.IndependentENR = true
	e := testEngine(t, c)
	a := &e.workload.ads[0]
	n, err := e.requestNode(a)
	if err != nil {
		t.Fatal(err)
	}
	if n == a.node || n.ID() != a.node.ID() || n.Seq() != a.node.Seq() || n.IPAddr() != a.node.IPAddr() {
		t.Fatal("decoded node is not an independent equal copy")
	}
	e.cfg.IndependentENR = false
	if n, _ := e.requestNode(a); n != a.node {
		t.Fatal("shared mode must pass the population node")
	}
}

func TestQueryCallbackMatchesProduction(t *testing.T) {
	accept := relayCheck(queryRequester)
	mk := func(ip net.IP) *enode.Node {
		var r enr.Record
		r.Set(enr.IP(ip))
		return enode.SignNull(&r, enode.ID{1})
	}
	if !accept(mk(net.IP{8, 8, 8, 8})) || accept(mk(net.IP{192, 168, 1, 1})) || accept(mk(net.IP{127, 0, 0, 1})) {
		t.Fatal("relay check")
	}
}

func TestOccupancyIntegral(t *testing.T) {
	o := occupancyIntegral{start: 10 * time.Second, mid: 20 * time.Second, end: 30 * time.Second}
	o.add(5*time.Second, 100) // warm-up, ignored
	o.add(15*time.Second, 2)  // [10,15) at 2
	o.add(25*time.Second, 4)  // [15,20) and [20,25) at 4
	o.add(40*time.Second, 6)  // [25,30) at 6, rest clipped
	if o.halves != [2]float64{2*5 + 4*5, 4*5 + 6*5} {
		t.Fatal(o.halves)
	}
}

func TestSimulatedDeterminism(t *testing.T) {
	c := simConfig()
	c.Duration = 30 * time.Second
	c.Lifetime = 5 * time.Second
	c.SampleInterval = 10 * time.Second
	c.FreshRate = 10
	c.QueryRate = 5
	semantic := func() string {
		var out bytes.Buffer
		if err := run(c, &out); err != nil {
			t.Fatal(err)
		}
		var selected []map[string]any
		for _, r := range readRecords(t, &out) {
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

// In simulated mode every ad lives exactly E, so time-averaged occupancy must
// equal admission rate * E up to boundary effects.
func TestLittlesLawSimulated(t *testing.T) {
	c := simConfig()
	c.Advertisers, c.Topics, c.CacheSize = 2000, 50, 1000
	c.Lifetime = time.Minute
	c.FreshRate, c.QueryRate = 5, 1
	c.Duration, c.Warmup = 40*time.Minute, ptr(10*time.Minute)
	c.SampleInterval = time.Minute
	c.RawOps = false
	var out bytes.Buffer
	if err := run(c, &out); err != nil {
		t.Fatal(err)
	}
	s := summaryOf(t, readRecords(t, &out))
	if r := s["littles_law_ratio"].(float64); math.Abs(r-1) > .01 {
		t.Fatalf("Little's law ratio %v", r)
	}
	if len(s["halves"].([]any)) != 2 {
		t.Fatal("halves")
	}
}

func ptr[T any](v T) *T { return &v }

// The hybrid clock must keep TopicTable expiry and lower bounds identical to a
// purely simulated run across the switch to real time.
func TestHybridTableEquivalence(t *testing.T) {
	sim := new(mclock.Simulated)
	wall := new(mclock.Simulated) // stands in for the system clock
	wall.Run(12345 * time.Hour)
	hyb := newHybridClock(wall)
	cfg := func(c mclock.Clock) topicindex.Config {
		return topicindex.Config{AdCacheSize: 50, AdLifetime: time.Minute, Clock: c}
	}
	a, b := topicindex.NewTopicTable(cfg(sim)), topicindex.NewTopicTable(cfg(hyb))
	c := simConfig()
	c.Advertisers, c.Topics = 60, 3
	w, _, err := population(c)
	if err != nil {
		t.Fatal(err)
	}
	step := func(d time.Duration) {
		sim.Run(d)
		if hyb.switched {
			wall.Run(d)
		} else {
			hyb.Sleep(d)
		}
	}
	check := func(i int) {
		t.Helper()
		if sim.Now() != hyb.Now() {
			t.Fatalf("step %d: clocks diverged %v vs %v", i, sim.Now(), hyb.Now())
		}
		a.Expire()
		b.Expire()
		if !reflect.DeepEqual(a.Stats(), b.Stats()) || a.NextExpiryTime() != b.NextExpiryTime() {
			t.Fatalf("step %d: state diverged\n%+v\n%+v", i, a.Stats(), b.Stats())
		}
	}
	credit := make([]time.Duration, len(w.ads))
	for i := 0; i < 400; i++ {
		if i == 150 {
			hyb.switchToReal()
			check(i)
		}
		ad := &w.ads[i%len(w.ads)]
		topic := w.topics[ad.topic]
		wa := a.Register(ad.node, topic, credit[i%len(w.ads)])
		wb := b.Register(ad.node, topic, credit[i%len(w.ads)])
		if wa != wb {
			t.Fatalf("step %d: wait %v vs %v", i, wa, wb)
		}
		if wa > 0 {
			credit[i%len(w.ads)] += wa
		} else {
			credit[i%len(w.ads)] = 0
		}
		step(time.Duration(1+i%7) * time.Second)
		check(i)
	}
	if a.Stats().TopicBounds == 0 || a.Stats().EffectiveIPBounds == 0 {
		t.Fatal("test did not exercise lower bounds", a.Stats())
	}
}

func TestHybridClockContinuity(t *testing.T) {
	wall := new(mclock.Simulated)
	wall.Run(time.Hour)
	h := newHybridClock(wall)
	h.Sleep(10 * time.Minute)
	before := h.Now()
	wall.Run(time.Hour) // real time passing before the switch is invisible
	if h.Now() != before {
		t.Fatal("real time leaked into simulated phase")
	}
	h.switchToReal()
	if h.Now() != before {
		t.Fatal("discontinuity at switch")
	}
	wall.Run(3 * time.Second)
	if h.Now() != before.Add(3*time.Second) {
		t.Fatal("not following real time after switch")
	}
}

func TestPerformanceValidity(t *testing.T) {
	p := detectPlatform()
	if p.GOARCH == "" || p.GoVersion == "" || p.TimerResolution <= 0 || p.Revision == "" {
		t.Fatalf("platform %+v", p)
	}
	c := testConfig()
	p.Translated, p.Revision = true, "x"
	if ok, _ := performanceValid(c, p); ok {
		t.Fatal("translated run marked valid")
	}
	c.AllowTranslated = true
	if ok, _ := performanceValid(c, p); !ok {
		t.Fatal("override ignored")
	}
	c.AllocationAudit = true
	if ok, _ := performanceValid(c, p); ok {
		t.Fatal("audit run marked valid")
	}
}

func TestMemfill(t *testing.T) {
	c := testConfig()
	c.Mode = "memfill"
	c.Lifetime = 0 // production E, so probe waits exceed the 1s admission floor
	c.CacheSize = 2000
	c.Topics = 20
	c.MemfillLevels = "0,0.5,1"
	c.MemfillReps = 1
	var out bytes.Buffer
	if err := run(c, &out); err != nil {
		t.Fatal(err)
	}
	heapAt := map[string][]float64{}
	bounds := 0
	for _, r := range readRecords(t, &out) {
		switch r["type"] {
		case "memfill":
			heapAt[r["variant"].(string)] = append(heapAt[r["variant"].(string)], r["heap_bytes"].(float64))
		case "memfill_bounds":
			if r["topic_bounds_added"].(float64) <= 0 {
				t.Fatal("no bounds recorded", r)
			}
			bounds++
		}
	}
	sh, in := heapAt["shared"], heapAt["independent"]
	if len(sh) != 3 || len(in) != 3 || bounds != 2 {
		t.Fatal("records", heapAt, bounds)
	}
	if !(sh[2] > sh[0] && in[2]-in[0] > sh[2]-sh[0]) {
		t.Fatalf("independent copies should cost more per ad: shared %v independent %v", sh, in)
	}
}

func TestRetryHeapOrder(t *testing.T) {
	var h wakes
	heap.Push(&h, wake{at: 3, id: 1})
	heap.Push(&h, wake{at: 1, id: 2})
	heap.Push(&h, wake{at: 1, id: 0})
	if heap.Pop(&h).(wake).id != 0 || heap.Pop(&h).(wake).id != 2 {
		t.Fatal("heap order")
	}
}

// A quote near the int64 limit wraps when the client decodes it; the attempt
// is dropped regardless of the abandonment policy.
func TestWrappedWaitAbandons(t *testing.T) {
	for _, policy := range []string{abandonProduction, abandonSpec, abandonNone} {
		w := &workload{ads: make([]advertiser, 1)}
		reason, err := w.quoted(0, mclock.AbsTime(time.Hour), time.Duration(math.MaxInt64), policy, time.Hour, time.Hour)
		if err != nil || reason != "wrapped" || len(w.retries) != 0 {
			t.Fatalf("%s: reason %q err %v retries %d", policy, reason, err, len(w.retries))
		}
	}
}
