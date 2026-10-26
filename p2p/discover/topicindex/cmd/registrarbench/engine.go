package main

import (
	"container/heap"
	"fmt"
	"math"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
)

// Mirrors topicindex/ticket.go's [due, due+5s) interval. A focused test compares
// these boundaries against production TicketSealer.Unpack. No ticket crypto is
// used by the benchmark engine.
const retryValidityWindow = 5 * time.Second

// scheduler supplies the SAME clock to the workload and TopicTable. Wall timing
// of method execution always uses time.Now/Since, independent of this clock.
type scheduler struct {
	clock     mclock.Clock
	simulated *mclock.Simulated
	origin    mclock.AbsTime
	wallStart time.Time
}

func newScheduler(mode string) *scheduler {
	s := &scheduler{clock: mclock.System{}}
	if mode == "simulated" {
		s.simulated = new(mclock.Simulated)
		s.clock = s.simulated
	}
	return s
}
func (s *scheduler) start()                 { s.origin = s.clock.Now(); s.wallStart = time.Now() }
func (s *scheduler) elapsed() time.Duration { return s.clock.Now().Sub(s.origin) }
func (s *scheduler) advance(to time.Duration) {
	d := to - s.elapsed()
	if d <= 0 {
		return
	}
	if s.simulated != nil {
		s.simulated.Run(d)
	} else {
		s.clock.Sleep(d)
	}
}

// pacer represents fixed arrivals at index/rate from the experiment origin.
// Completion time never shifts this schedule. Unprocessed arrivals are backlog,
// not saved idle credit. A scarcity opportunity is consumed without a core call.
type pacer struct {
	rate  float64
	index uint64
	next  time.Duration
}

func newPacer(rate float64) pacer               { return pacer{rate: rate, next: arrival(0, rate)} }
func (p *pacer) consume()                       { p.index++; p.next = arrival(p.index, p.rate) }
func retryExpired(due, now mclock.AbsTime) bool { return now.Sub(due) >= retryValidityWindow }

type workEvent struct {
	kind string
	due  time.Duration
	id   int
}
type engine struct {
	cfg             config
	scheduler       *scheduler
	table           *topicindex.TopicTable
	workload        *workload
	pacer           pacer
	queued          wakes
	queryIndex      uint64
	total           counters
	measured        counters
	measuring       bool
	lastSupplyCheck time.Duration
}

func newEngine(c config, w *workload) *engine {
	s := newScheduler(c.Mode)
	return &engine{cfg: c, scheduler: s, workload: w, pacer: newPacer(c.RegRate), queued: make(wakes, 0, c.Advertisers),
		table: topicindex.NewTopicTable(topicindex.Config{AdCacheSize: c.CacheSize, AdLifetime: c.Lifetime, Clock: s.clock})}
}
func (e *engine) eachCount(fn func(*counters)) {
	fn(&e.total)
	if e.measuring {
		fn(&e.measured)
	}
}
func (e *engine) checkSupply(now time.Duration) {
	w := e.workload
	if w.head == len(w.ready) && len(e.queued) == 0 && e.cfg.RegRate > 0 {
		// Time with neither a fresh advertiser nor a due retry available.
		from := e.lastSupplyCheck
		if now > from {
			e.total.FreshSupplyLimitedNS += int64(now - from)
			if e.measuring {
				e.measured.FreshSupplyLimitedNS += int64(now - from)
			}
		}
	}
	e.lastSupplyCheck = now
}

// Pending retries enter the due queue at their deadline; earliest due wins.
// Expiration is independent of registration capacity, including at rate zero.
func (e *engine) nextWork() workEvent {
	now := e.scheduler.elapsed()
	w := e.workload
	for len(w.retries) > 0 && w.retries[0].at.Sub(e.scheduler.origin) <= now {
		heap.Push(&e.queued, heap.Pop(&w.retries).(wake))
		e.eachCount(func(c *counters) {
			c.RetryDemand++
			c.RetriesQueued++
			c.MaxRetryQueue = max(c.MaxRetryQueue, uint64(len(e.queued)))
		})
	}
	e.eachCount(func(c *counters) { c.MaxRetryQueue = max(c.MaxRetryQueue, uint64(len(e.queued))) })
	next := workEvent{kind: "query", due: arrival(e.queryIndex, e.cfg.QueryRate)}
	if len(w.retries) > 0 {
		due := w.retries[0].at.Sub(e.scheduler.origin)
		if due < next.due {
			next = workEvent{kind: "wake", due: due}
		}
	}
	if len(e.queued) > 0 {
		expiry := e.queued[0].at.Sub(e.scheduler.origin) + retryValidityWindow
		if expiry <= now {
			return workEvent{kind: "retry_expired", due: expiry, id: e.queued[0].id}
		}
		if expiry < next.due {
			next = workEvent{kind: "retry_expired", due: expiry, id: e.queued[0].id}
		}
	}
	if e.cfg.RegRate > 0 && e.pacer.next <= next.due {
		event := workEvent{kind: "fresh", due: e.pacer.next, id: -1}
		if len(e.queued) > 0 {
			event.kind = "retry"
			event.id = e.queued[0].id
		} else if w.head < len(w.ready) {
			event.id = w.ready[w.head]
		}
		next = event
	}

	return next
}
func (e *engine) nextExpiry() time.Duration {
	if exp := e.table.NextExpiryTime(); exp != topicindex.Never {
		return exp.Sub(e.scheduler.origin)
	}
	return time.Duration(math.MaxInt64)
}
func (e *engine) nextRelease() time.Duration {
	if len(e.workload.active) > 0 {
		return e.workload.active[0].at.Sub(e.scheduler.origin)
	}
	return time.Duration(math.MaxInt64)
}
func (e *engine) newOperation(kind string, due time.Duration, topic int) operation {
	op := operation{Type: "operation", Kind: kind, Mode: e.cfg.Mode, Scheduled: int64(due),
		Occupancy: e.table.Occupancy()}
	op.Utilisation = float64(op.Occupancy) / float64(e.cfg.CacheSize)
	if topic >= 0 {
		op.Rank = topic + 1
		op.Population = e.workload.population[topic]
		op.TopicSize = e.table.TopicSize(e.workload.topics[topic])
	}
	return op
}
func (e *engine) stamp(op *operation) mclock.AbsTime {
	now := e.scheduler.clock.Now()
	op.Started = int64(now.Sub(e.scheduler.origin))
	op.LogicalElapsed = op.Started
	op.WallElapsed = int64(time.Since(e.scheduler.wallStart))
	if e.cfg.Mode == "realtime" {
		delay := max(int64(0), op.Started-op.Scheduled)
		op.Queue = &delay
	}
	return now
}
func (e *engine) expire() operation {
	op := e.newOperation("expiry", e.nextExpiry(), -1)
	e.stamp(&op)
	before := auditBefore(e.cfg.AllocationAudit)
	start := time.Now()
	e.table.Expire()
	op.Latency = int64(time.Since(start))
	op.AllocBytes, op.Allocs = auditAfter(e.cfg.AllocationAudit, before)
	op.Removed = op.Occupancy - e.table.Occupancy()
	e.eachCount(func(c *counters) {
		c.Expiries++
		c.ExpiryLatency.add(uint64(op.Latency))
		c.ExpiryBytes += op.AllocBytes
		c.ExpiryAllocs += op.Allocs
	})
	return op
}
func (e *engine) execute(event workEvent) (operation, error) {
	w := e.workload
	if event.kind == "query" {
		e.queryIndex++
		topic := w.z.sample(w.rng)
		op := e.newOperation("query", event.due, topic)
		e.stamp(&op)
		before := auditBefore(e.cfg.AllocationAudit)
		start := time.Now()
		nodes := e.table.RandomNodes(w.topics[topic], 16, ipv4)
		op.Latency = int64(time.Since(start))
		op.Returned = len(nodes)
		op.AllocBytes, op.Allocs = auditAfter(e.cfg.AllocationAudit, before)
		e.eachCount(func(c *counters) {
			c.Queries++
			c.Returned += uint64(len(nodes))
			c.QueryLatency.add(uint64(op.Latency))
			c.QueryBytes += op.AllocBytes
			c.QueryAllocs += op.Allocs
		})
		return op, nil
	}
	retry := event.kind == "retry" || event.kind == "retry_expired"
	var retryDue time.Duration
	if retry {
		retryDue = heap.Pop(&e.queued).(wake).at.Sub(e.scheduler.origin)
	} else {
		id, ok := w.next()
		if !ok {
			op := e.newOperation("registration", event.due, -1)
			op.Type = "population_scarcity"
			op.Outcome = "no_eligible_advertiser"
			op.Advertiser = -1
			e.stamp(&op)
			if time.Duration(op.Started) < e.pacer.next {
				return op, fmt.Errorf("scarcity dispatched before scheduled opportunity")
			}
			e.pacer.consume()
			interval := max(time.Duration(0), min(e.pacer.next, e.cfg.Duration)-event.due)
			e.eachCount(func(c *counters) { c.PopulationScarcity++; c.ScarcityIntervalNS += int64(interval) })
			return op, nil
		}
		if id != event.id {
			return operation{}, fmt.Errorf("fresh queue invariant violated: got %d, want %d", id, event.id)
		}
	}
	a := &w.ads[event.id]
	op := e.newOperation("registration", event.due, a.topic)
	op.Fresh = !retry
	op.Advertiser = event.id
	now := e.stamp(&op)
	if retry {
		op.RetryDue = int64(retryDue)
		if retryExpired(e.scheduler.origin.Add(retryDue), now) {
			// The sole queued entry was popped above. End this lifecycle and
			// restore fresh eligibility, without a call or a new scheduled event.
			op.Type = "retry_expired"
			op.Outcome = "expired_retry"
			e.eachCount(func(c *counters) { c.ExpiredRetries++ })
			w.enqueueFresh(event.id, now)
			return op, nil
		}
		op.Credited = int64(now.Sub(a.first))
	}
	if time.Duration(op.Started) < e.pacer.next {
		return op, fmt.Errorf("registration dispatched before pacing deadline")
	}
	before := auditBefore(e.cfg.AllocationAudit)
	start := time.Now()
	wait := e.table.Register(a.node, w.topics[a.topic], time.Duration(op.Credited))
	op.Latency = int64(time.Since(start))
	completed := e.scheduler.clock.Now()
	e.pacer.consume()
	op.Wait = int64(wait)
	op.AllocBytes, op.Allocs = auditAfter(e.cfg.AllocationAudit, before)
	delta := e.table.Occupancy() - op.Occupancy
	if wait < 0 || (wait == 0 && delta != 1) || (wait > 0 && delta != 0) {
		if wait == 0 && delta == 0 {
			e.eachCount(func(c *counters) { c.Duplicates++ })
		}
		return op, fmt.Errorf("inconsistent Register result: advertiser=%d topic=%d occupancy=%d capacity=%d delta=%d wait=%s", event.id, a.topic+1, op.Occupancy, e.cfg.CacheSize, delta, wait)
	}
	if err := w.finish(event.id, now, completed, wait, e.cfg.Lifetime); err != nil {
		return op, err
	}
	op.Outcome = "admitted"
	if wait > 0 {
		op.Outcome = "wait"
	}
	e.eachCount(func(c *counters) {
		c.Registrations++
		c.RegLatency.add(uint64(op.Latency))
		c.RegBytes += op.AllocBytes
		c.RegAllocs += op.Allocs
		if retry {
			c.Retries++
		} else {
			c.Fresh++
		}
		if wait > 0 {
			c.Waits++
		} else {
			c.Admissions++
			if retry {
				c.Eventual++
			} else {
				c.Immediate++
			}
		}
	})
	return op, nil
}
