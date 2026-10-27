package main

import (
	"container/heap"
	"fmt"
	"math"
	"net/netip"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// Mirrors topicindex/ticket.go's [due, due+5s) interval. A focused test compares
// these boundaries against production TicketSealer.Unpack. No ticket crypto is
// used by the benchmark engine.
const retryValidityWindow = 5 * time.Second

func retryExpired(due, now mclock.AbsTime) bool { return now.Sub(due) >= retryValidityWindow }

// queryRequester is the source address of benchmark TOPICQUERY requests. It is
// a public address, so the production relay check accepts any public ad.
var queryRequester = netip.MustParseAddr("93.184.216.34")

// scheduler supplies the SAME clock to the workload and TopicTable. Wall timing
// of method execution always uses time.Now/Since, independent of this clock.
type scheduler struct {
	clock     mclock.Clock
	simulated *mclock.Simulated
	hybrid    *hybridClock
	spin      bool
	origin    mclock.AbsTime
	wallStart time.Time
}

func newScheduler(mode, pacing string) *scheduler {
	s := &scheduler{clock: mclock.System{}, spin: pacing == "spin"}
	switch mode {
	case "simulated":
		s.simulated = new(mclock.Simulated)
		s.clock = s.simulated
	case "hybrid":
		s.hybrid = newHybridClock(mclock.System{})
		s.clock = s.hybrid
	}
	return s
}
func (s *scheduler) start()                 { s.origin = s.clock.Now(); s.wallStart = time.Now() }
func (s *scheduler) elapsed() time.Duration { return s.clock.Now().Sub(s.origin) }

// realtime reports whether the clock currently follows wall time.
func (s *scheduler) realtime() bool {
	return s.simulated == nil && (s.hybrid == nil || s.hybrid.switched)
}

func (s *scheduler) advance(to time.Duration) {
	d := to - s.elapsed()
	if d <= 0 {
		return
	}
	switch {
	case s.simulated != nil:
		s.simulated.Run(d)
	case s.spin && s.realtime():
		// Busy-wait so the worker's core does not idle between requests.
		for s.elapsed() < to {
		}
	default:
		s.clock.Sleep(d)
	}
}

type workEvent struct {
	kind string // "retry", "fresh" or "query"
	due  time.Duration
}

// occupancyIntegral accumulates occupancy·time over each half of the
// measurement window.
type occupancyIntegral struct {
	start, mid, end time.Duration
	last            time.Duration
	halves          [2]float64 // occupancy·seconds
}

func (o *occupancyIntegral) add(now time.Duration, occupancy int) {
	from := max(o.last, o.start)
	to := min(now, o.end)
	if to > from {
		if a, b := from, min(to, o.mid); b > a {
			o.halves[0] += float64(occupancy) * (b - a).Seconds()
		}
		if a, b := max(from, o.mid), to; b > a {
			o.halves[1] += float64(occupancy) * (b - a).Seconds()
		}
	}
	o.last = max(o.last, now)
}

type engine struct {
	cfg       config
	scheduler *scheduler
	table     *topicindex.TopicTable
	workload  *workload
	fresh     *arrivals
	query     *arrivals
	accept    func(*enode.Node) bool
	total     counters
	measured  counters
	halfStart counters // measured counters at the measurement midpoint
	halfTaken bool
	measuring bool
	occupancy occupancyIntegral
}

func newEngine(c config, w *workload) *engine {
	s := newScheduler(c.Mode, c.Pacing)
	poisson := c.Arrivals == "poisson"
	e := &engine{cfg: c, scheduler: s, workload: w,
		fresh:  newArrivals(c.FreshRate, poisson, c.Seed+5),
		query:  newArrivals(c.QueryRate, poisson, c.Seed+6),
		accept: relayCheck(queryRequester),
		table:  topicindex.NewTopicTable(topicindex.Config{AdCacheSize: c.CacheSize, AdLifetime: c.Lifetime, Clock: s.clock})}
	if c.Mode == "hybrid" {
		// Queries do not change registrar state; simulating them during the
		// warm-up only costs time (hours at high query rates).
		e.query.skipTo(c.warmup())
	}
	return e
}
func (e *engine) eachCount(fn func(*counters)) {
	fn(&e.total)
	if e.measuring {
		fn(&e.measured)
	}
}

// nextWork returns the earliest pending arrival. Retries arrive at their own
// due time, independently of fresh and query arrivals. Ties go to retries,
// then fresh arrivals, then queries.
func (e *engine) nextWork() workEvent {
	next := workEvent{kind: "query", due: e.query.next}
	if e.fresh.next <= next.due {
		next = workEvent{kind: "fresh", due: e.fresh.next}
	}
	if w := e.workload; len(w.retries) > 0 {
		if due := w.retries[0].at.Sub(e.scheduler.origin); due <= next.due {
			next = workEvent{kind: "retry", due: due}
		}
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
	if e.scheduler.realtime() {
		delay := max(int64(0), op.Started-op.Scheduled)
		op.Queue = &delay
		e.eachCount(func(c *counters) { c.QueueDelay.add(uint64(delay)) })
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
		c.Expired += uint64(op.Removed)
		c.ExpiryLatency.add(uint64(op.Latency))
		c.ExpiryBytes += op.AllocBytes
		c.ExpiryAllocs += op.Allocs
	})
	return op
}

// requestNode returns the record the registrar receives with a request.
func (e *engine) requestNode(a *advertiser) (*enode.Node, error) {
	if !e.cfg.IndependentENR {
		return a.node, nil
	}
	return decodeNode(a.enc)
}

func (e *engine) execute(event workEvent) (operation, error) {
	w := e.workload
	switch event.kind {
	case "query":
		e.query.consume()
		topic := w.z.sample(w.rng)
		op := e.newOperation("query", event.due, topic)
		e.stamp(&op)
		before := auditBefore(e.cfg.AllocationAudit)
		start := time.Now()
		nodes := e.table.RandomNodes(w.topics[topic], 16, e.accept)
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
	case "fresh":
		e.fresh.consume()
		e.eachCount(func(c *counters) { c.FreshArrivals++ })
		id, ok := w.next()
		if !ok {
			// No idle advertiser: the arrival is lost, never banked as credit.
			op := e.newOperation("registration", event.due, -1)
			op.Type, op.Outcome, op.Advertiser = "population_scarcity", "no_eligible_advertiser", -1
			e.stamp(&op)
			e.eachCount(func(c *counters) { c.PopulationScarcity++ })
			return op, nil
		}
		return e.register(event, id, false)
	case "retry":
		item := heap.Pop(&w.retries).(wake)
		return e.register(event, item.id, true)
	}
	return operation{}, fmt.Errorf("unknown event %q", event.kind)
}

func (e *engine) register(event workEvent, id int, retry bool) (operation, error) {
	w := e.workload
	a := &w.ads[id]
	op := e.newOperation("registration", event.due, a.topic)
	op.Fresh = !retry
	op.Advertiser = id
	now := e.stamp(&op)
	if retry {
		op.RetryDue = int64(event.due)
		if retryExpired(e.scheduler.origin.Add(event.due), now) {
			// Served too late (realtime backlog): production drops the request
			// on ticket validation and the client's attempt fails.
			op.Type, op.Outcome = "retry_expired", "expired_retry"
			e.eachCount(func(c *counters) { c.ExpiredRetries++ })
			w.enqueueFresh(id)
			return op, nil
		}
		op.Credited = int64(now.Sub(a.first))
	}
	node, err := e.requestNode(a)
	if err != nil {
		return op, err
	}
	before := auditBefore(e.cfg.AllocationAudit)
	start := time.Now()
	wait := e.table.Register(node, w.topics[a.topic], time.Duration(op.Credited))
	op.Latency = int64(time.Since(start))
	completed := e.scheduler.clock.Now()
	op.Wait = int64(wait)
	op.AllocBytes, op.Allocs = auditAfter(e.cfg.AllocationAudit, before)
	delta := e.table.Occupancy() - op.Occupancy
	if wait < 0 || (wait == 0 && delta != 1) || (wait > 0 && delta != 0) {
		return op, fmt.Errorf("inconsistent Register result: advertiser=%d topic=%d occupancy=%d capacity=%d delta=%d wait=%s", id, a.topic+1, op.Occupancy, e.cfg.CacheSize, delta, wait)
	}
	abandoned := ""
	if wait > 0 {
		op.Outcome = "wait"
		abandoned, err = w.quoted(id, now, wait, e.cfg.Abandon, e.cfg.AttemptTimeout, e.cfg.Lifetime)
		if err != nil {
			return op, err
		}
		if abandoned != "" {
			op.Outcome = "abandoned_" + abandoned
			op.Credited = 0
			if a.waiting {
				op.Credited = int64(now.Sub(a.first))
			}
			// The advertiser leaves this registrar and can only return
			// through a later fresh arrival.
			w.enqueueFresh(id)
		}
	} else {
		op.Outcome = "admitted"
		w.admitted(id, completed.Add(e.cfg.Lifetime))
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
		switch {
		case wait == 0:
			c.Admissions++
			c.AdmittedWait.add(uint64(op.Credited))
			if retry {
				c.Eventual++
				c.EventualWait.add(uint64(op.Credited))
			} else {
				c.Immediate++
			}
		default:
			c.Waits++
			c.QuotedWait.add(uint64(wait))
			switch abandoned {
			case "single":
				c.Abandoned++
				c.AbandonedSingle++
				c.AbandonedAfter.add(uint64(op.Credited))
			case "cumulative":
				c.Abandoned++
				c.AbandonedCumulative++
				c.AbandonedAfter.add(uint64(op.Credited))
			case "wrapped":
				c.Abandoned++
				c.AbandonedWrapped++
				c.AbandonedAfter.add(uint64(op.Credited))
			}
		}
	})
	return op, nil
}
