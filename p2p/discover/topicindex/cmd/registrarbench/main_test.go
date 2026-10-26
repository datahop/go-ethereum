package main

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
)

func testConfig() config {
	return config{Advertisers: 20, Topics: 5, Alpha: 1, Seed: 7, RegRate: 100, QueryRate: 100, Duration: 300 * time.Millisecond, WarmupFraction: .5, CacheSize: 100, Lifetime: 40 * time.Millisecond, SampleInterval: 50 * time.Millisecond, IPColumn: "ip"}
}
func TestZipf(t *testing.T) {
	z := newZipf(300, 1)
	r := rand.New(rand.NewSource(1))
	counts := make([]int, 300)
	for i := 0; i < 100000; i++ {
		counts[z.sample(r)]++
	}
	if counts[0] < 15000 || counts[0] > 17000 || counts[1] < 7000 || counts[1] > 9000 {
		t.Fatal(counts[:3])
	}
	uniform := newZipf(4, 0)
	if !reflect.DeepEqual(uniform.cdf, []float64{.25, .5, .75, 1}) {
		t.Fatal(uniform)
	}
}
func TestIPFiles(t *testing.T) {
	for _, tc := range []struct {
		name, text     string
		valid          bool
		rows, distinct int
	}{
		{"ips.txt", "# comment\n8.8.8.8\n8.8.8.8\n::ffff:1.1.1.1\n", true, 3, 2},
		{"ips.csv", "ip,label\n8.8.8.8,a\n8.8.8.8,b\n", true, 2, 1},
		{"bad.txt", "8.8.8.8\nbad\n", false, 0, 0},
		{"v6.txt", "2001:4860::1\n", false, 0, 0},
		{"bad.csv", "address\n8.8.8.8\n", false, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), tc.name)
			os.WriteFile(path, []byte(tc.text), 0600)
			ips, m, err := readIPs(path, "ip")
			if (err == nil) != tc.valid {
				t.Fatal(err)
			}
			if tc.valid && (len(ips) != tc.rows || m.Distinct != tc.distinct || len(m.SHA256) != 64) {
				t.Fatal(m)
			}
		})
	}
}
func TestPopulation(t *testing.T) {
	c := testConfig()
	a, ma, err := population(c)
	if err != nil {
		t.Fatal(err)
	}
	b, mb, err := population(c)
	if err != nil {
		t.Fatal(err)
	}
	if ma != mb || !reflect.DeepEqual(a.population, b.population) || !reflect.DeepEqual(a.ready, b.ready) {
		t.Fatal("not deterministic")
	}
	ids := make(map[string]bool)
	for i := range a.ads {
		n := a.ads[i].node
		if n.String() != b.ads[i].node.String() || !n.IPAddr().Is4() || ids[n.ID().String()] {
			t.Fatal("invalid identity")
		}
		ids[n.ID().String()] = true
	}
}
func TestLifecycle(t *testing.T) {
	c := testConfig()
	c.Advertisers = 1
	w, _, err := population(c)
	if err != nil {
		t.Fatal(err)
	}
	clock := new(mclock.Simulated)
	tab := topicindex.NewTopicTable(topicindex.Config{Clock: clock, AdLifetime: time.Second})
	id, ok := w.next()
	if !ok {
		t.Fatal("not ready")
	}
	a := &w.ads[id]
	wait := tab.Register(a.node, w.topics[a.topic], 0)
	if wait != 0 || tab.Occupancy() != 1 {
		t.Fatal(wait)
	}
	w.finish(id, clock.Now(), clock.Now(), wait, time.Second)
	if _, ok := w.next(); ok {
		t.Fatal("active advertiser ready")
	}
	clock.Run(time.Second)
	tab.Expire()
	w.release(clock.Now())
	if tab.Occupancy() != 0 {
		t.Fatal("not expired")
	}
	id, ok = w.next()
	if !ok {
		t.Fatal("not released")
	}
	if wait := tab.Register(a.node, w.topics[a.topic], 0); wait != 0 {
		t.Fatal(wait)
	}
	// Exercise accumulated retry credit independent of cache admission.
	w.finish(id, clock.Now(), clock.Now(), 2*time.Second, time.Second)
	first := a.first
	clock.Run(time.Second)
	w.release(clock.Now())
	if _, ok = w.next(); ok {
		t.Fatal("retry early")
	}
	clock.Run(time.Second)
	w.release(clock.Now())
	item := heap.Pop(&w.retries).(wake)
	id = item.id
	if item.at != clock.Now() || !a.waiting || clock.Now().Sub(a.first) != 2*time.Second {
		t.Fatal("retry state")
	}
	w.finish(id, clock.Now(), clock.Now(), time.Second, time.Second)
	if a.first != first {
		t.Fatal("credit reset")
	}
}
func TestWaitClassification(t *testing.T) {
	c := testConfig()
	c.Topics = 1
	w, _, err := population(c)
	if err != nil {
		t.Fatal(err)
	}
	clock := new(mclock.Simulated)
	tab := topicindex.NewTopicTable(topicindex.Config{Clock: clock, AdLifetime: time.Minute, AdCacheSize: 100})
	topic := w.topics[0]
	if wait := tab.Register(w.ads[0].node, topic, 0); wait != 0 {
		t.Fatal(wait)
	}
	if wait := tab.Register(w.ads[1].node, topic, 0); wait <= 0 || tab.Occupancy() != 1 {
		t.Fatal("expected wait", wait)
	}
}
func TestScheduleAndPhase(t *testing.T) {
	if arrival(3, 2) != 1500*time.Millisecond || offeredBefore(time.Second, 2) != 2 || offeredBefore(time.Second, 0) != 0 {
		t.Fatal("arrival schedule")
	}
	if phase(499*time.Millisecond, 500*time.Millisecond) || !phase(500*time.Millisecond, 500*time.Millisecond) {
		t.Fatal("phase")
	}
	for _, rate := range []float64{.5, 3, 1000} {
		for i := uint64(0); i < 100; i++ {
			if arrival(i+1, rate) <= arrival(i, rate) {
				t.Fatal("non-increasing schedule")
			}
		}
	}
}
func TestRunOutput(t *testing.T) {
	for _, audit := range []bool{false, true} {
		c := testConfig()
		c.AllocationAudit = audit
		var out bytes.Buffer
		if err := run(c, &out); err != nil {
			t.Fatal(err)
		}
		d := json.NewDecoder(&out)
		operations := 0
		samples := 0
		summary := false
		for d.More() {
			var r map[string]any
			if err := d.Decode(&r); err != nil {
				t.Fatal(err)
			}
			switch r["type"] {
			case "operation":
				operations++
				if r["started_ns"].(float64) < float64(c.Duration)*c.WarmupFraction {
					t.Fatal("warmup operation")
				}
				if r["queue_delay_ns"].(float64) < 0 {
					t.Fatal("negative delay")
				}
			case "sample":
				samples++
			case "summary":
				summary = true
				if r["performance_valid"] != !audit {
					t.Fatal("audit label")
				}
				counts := r["counts"].(map[string]any)
				if counts["registrations_processed"].(float64) != counts["new_admissions"].(float64)+counts["wait_responses"].(float64) {
					t.Fatal("classification")
				}
			}
		}
		if !summary || samples < 2 || operations == 0 {
			t.Fatal("missing records", summary, samples, operations)
		}
	}
}
func TestHistogram(t *testing.T) {
	var h histogram
	for _, v := range []uint64{1, 31, 32, 63, 64, 1000, 1000000} {
		h.add(v)
	}
	if h.quantile(.5) != 63 || h.quantile(.99) < 1000 || h.Count != 7 {
		t.Fatal(h.summary())
	}
}

type slowOutput struct{ bytes.Buffer }

func (s *slowOutput) Write(p []byte) (int, error) {
	time.Sleep(15 * time.Millisecond)
	return s.Buffer.Write(p)
}
func TestOverloadDoesNotReduceOfferedRate(t *testing.T) {
	c := testConfig()
	c.WarmupFraction = 0
	c.Duration = 150 * time.Millisecond
	c.RegRate = 0
	c.QueryRate = 10000
	c.SampleInterval = time.Second
	var out slowOutput
	if err := run(c, &out); err != nil {
		t.Fatal(err)
	}
	d := json.NewDecoder(&out.Buffer)
	for d.More() {
		var r map[string]any
		if err := d.Decode(&r); err != nil {
			t.Fatal(err)
		}
		if r["type"] == "summary" {
			if r["total_query_arrivals_offered"].(float64) != 1500 || r["unserved_queries"].(float64) <= 0 {
				t.Fatal(r)
			}
			return
		}
	}
	t.Fatal("missing summary")
}

func TestEmpiricalIPRowsWithReplacement(t *testing.T) {
	c := testConfig()
	c.Advertisers = 100
	c.IPFile = filepath.Join(t.TempDir(), "ips.txt")
	if err := os.WriteFile(c.IPFile, []byte("8.8.8.8\n8.8.8.8\n1.1.1.1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	first, meta, err := population(c)
	if err != nil {
		t.Fatal(err)
	}
	again, _, err := population(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.ads) != 100 || meta.Rows != 3 || meta.Distinct != 2 || meta.AssignedDistinct != 2 || meta.AssignmentMode != "empirical_rows_with_replacement" || len(meta.SHA256) != 64 {
		t.Fatal("population or metadata", meta)
	}
	for i := range first.ads {
		if first.ads[i].node.String() != again.ads[i].node.String() {
			t.Fatal("mapping/ENR not deterministic", i)
		}
	}
	otherConfig := c
	otherConfig.Seed++
	other, _, err := population(otherConfig)
	if err != nil {
		t.Fatal(err)
	}
	different := false
	for i := range first.ads {
		if first.ads[i].node.IPAddr() != other.ads[i].node.IPAddr() {
			different = true
		}
	}
	if !different {
		t.Fatal("seed did not change IP assignments")
	}
	// A larger population must preserve its mapping/topic prefix and query stream.
	c.Advertisers = 2000
	large, _, err := population(c)
	if err != nil {
		t.Fatal(err)
	}
	aCount := 0
	for i, a := range large.ads {
		switch a.node.IPAddr().String() {
		case "8.8.8.8":
			aCount++
		case "1.1.1.1":
		default:
			t.Fatal("non-empirical IP generated")
		}
		if i < len(first.ads) && (a.node.IPAddr() != first.ads[i].node.IPAddr() || a.topic != first.ads[i].topic) {
			t.Fatal("population size perturbed mapping/topic prefix", i)
		}
	}
	if aCount < 1200 || aCount > 1467 {
		t.Fatal("duplicate row weight not approximately 2:1", aCount)
	}
	if first.rng.Int63() != large.rng.Int63() {
		t.Fatal("IP assignment perturbed query stream")
	}
	t.Logf("A,A,B input: A assigned to %d of 2000 advertisers", aCount)
}
