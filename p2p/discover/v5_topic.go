// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package discover

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/discover/v5wire"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// topicSystem manages the resources required for registering and searching
// in topics.
type topicSystem struct {
	transport *UDPv5
	config    topicindex.Config

	mu  sync.Mutex
	reg map[topicindex.TopicID]*topicReg

	// banned holds the nodes whose topic RPC timed out. They are kept out of
	// the registration and search tables for an ad lifetime.
	banMu  sync.Mutex
	banned *topicindex.AskedSet

	wg   sync.WaitGroup
	quit chan struct{}
}

// EnableReach turns on topicindex reach sampling (testbed bottleneck analysis).
func EnableReach() { topicindex.EnableReach() }

func newTopicSystem(transport *UDPv5, config topicindex.Config) *topicSystem {
	sys := &topicSystem{
		transport: transport,
		config:    config,
		reg:       make(map[topicindex.TopicID]*topicReg),
		banned:    topicindex.NewAskedSet(config),
		quit:      make(chan struct{}),
	}
	sys.wg.Add(1)
	go sys.evictRemovedNodes()
	return sys
}

// evictRemovedNodes evicts nodes dropped by the DHT routing table from the ad
// cache and registration tables.
func (sys *topicSystem) evictRemovedNodes() {
	defer sys.wg.Done()
	removed := make(chan enode.ID, bucketSize*nBuckets)
	sub := sys.transport.tab.subscribeRemovedNodes(removed)
	defer sub.Unsubscribe()

	for {
		select {
		case <-sys.quit:
			return
		case id := <-removed:
			sys.evictNode(id)
		}
	}
}

// evictNode removes a dead node's ads and its reg attempts in every reg table.
func (sys *topicSystem) evictNode(id enode.ID) {
	sys.transport.evictTopicTableNode(id)
	sys.mu.Lock()
	regs := make([]*topicReg, 0, len(sys.reg))
	for _, r := range sys.reg {
		regs = append(regs, r)
	}
	sys.mu.Unlock()
	for _, r := range regs {
		r.evict(id)
	}
}

// ban keeps id out of topic search and registration for an ad lifetime. The
// loops skip a banned node when they pick it as a target.
func (sys *topicSystem) ban(id enode.ID) {
	sys.banMu.Lock()
	defer sys.banMu.Unlock()
	sys.banned.Add(id)
}

func (sys *topicSystem) unban(id enode.ID) {
	sys.banMu.Lock()
	defer sys.banMu.Unlock()
	sys.banned.Remove(id)
}

func (sys *topicSystem) isBanned(id enode.ID) bool {
	sys.banMu.Lock()
	defer sys.banMu.Unlock()
	return sys.banned.Has(id)
}

// filterNodes returns the topic-discovery capable nodes that are not banned.
func (sys *topicSystem) filterNodes(nodes []*enode.Node) []*enode.Node {
	nodes = filterTopicDiscovery(nodes)
	sys.banMu.Lock()
	defer sys.banMu.Unlock()
	filtered := nodes[:0]
	for _, n := range nodes {
		if !sys.banned.Has(n.ID()) {
			filtered = append(filtered, n)
		}
	}
	return filtered
}

func (sys *topicSystem) register(topic topicindex.TopicID, opid uint64) {
	sys.mu.Lock()
	defer sys.mu.Unlock()

	if _, ok := sys.reg[topic]; ok {
		return
	}
	sys.reg[topic] = newTopicReg(sys, topic, opid)
}

func (sys *topicSystem) stopRegister(topic topicindex.TopicID) {
	sys.mu.Lock()
	defer sys.mu.Unlock()

	if reg := sys.reg[topic]; reg != nil {
		reg.stop()
		delete(sys.reg, topic)
	}
}

func (sys *topicSystem) stop() {
	close(sys.quit)
	sys.wg.Wait()

	sys.mu.Lock()
	defer sys.mu.Unlock()

	for topic, reg := range sys.reg {
		reg.stop()
		delete(sys.reg, topic)
	}
}

func (sys *topicSystem) newSearchIterator(topic topicindex.TopicID, opid uint64) enode.Iterator {
	sys.mu.Lock()
	defer sys.mu.Unlock()

	resultCh := make(chan *enode.Node, 200)
	s := newTopicSearch(sys, topic, resultCh, opid)
	return newTopicSearchIterator(sys, s, resultCh)
}

// topicReg handles registering for a single topic.
type topicReg struct {
	state *topicindex.Registration
	clock mclock.Clock
	opid  uint64

	wg   sync.WaitGroup
	quit chan struct{}

	regRequest  chan topicRegJob
	regResponse chan topicRegResult

	// evictCh delivers IDs of dead nodes (DHT routing-table removals and search
	// query timeouts) to be removed from the registration table.
	evictCh chan enode.ID

	// nodes subscription
	newNodesCh  chan *enode.Node
	newNodesSub event.Subscription

	statsCh chan chan []topicindex.BucketStats
}

// evict requests removal of a node from the registration table. It is called
// from the topic system's eviction path (evictNode), fed by DHT removals and
// search query timeouts.
func (reg *topicReg) evict(id enode.ID) {
	select {
	case reg.evictCh <- id:
	case <-reg.quit:
	}
}

func newTopicReg(sys *topicSystem, topic topicindex.TopicID, opid uint64) *topicReg {
	reg := &topicReg{
		state:       topicindex.NewRegistration(topic, sys.config),
		clock:       sys.config.Clock,
		opid:        opid,
		quit:        make(chan struct{}),
		regRequest:  make(chan topicRegJob),
		regResponse: make(chan topicRegResult),
		evictCh:     make(chan enode.ID, 64),
		statsCh:     make(chan chan []topicindex.BucketStats),
	}

	// Set up the subscription for new main table nodes.
	reg.newNodesCh = make(chan *enode.Node, 100)
	reg.newNodesSub = sys.transport.tab.subscribeNodes(reg.newNodesCh)

	reg.wg.Add(2)
	go reg.registrationLoop(sys)
	go reg.sendRequestsLoop(sys)
	return reg
}

func (reg *topicReg) stop() {
	close(reg.quit)
	reg.wg.Wait()
}

func (reg *topicReg) registrationLoop(sys *topicSystem) {
	defer reg.wg.Done()
	defer reg.newNodesSub.Unsubscribe()
	defer close(reg.regRequest)

	time := mclock.AbsTime(-1)
	for {
		if time >= 0 {
			if exit := reg.pause(time); exit {
				return
			}
		}
		time = reg.clock.Now()

		// Initialize the registration state with DISC-NG capable nodes only.
		nodes := sys.filterNodes(sys.transport.tab.allNodes())
		if len(nodes) == 0 {
			continue // No DISC-NG capable nodes, retry later.
		}
		shuffleNodes(nodes)
		reg.state.AddNodes(nil, nodes)

		// Perform registration.
		if exit := reg.runRegistration(sys); exit {
			return
		}
	}
}

func shuffleNodes(nodes []*enode.Node) {
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
}

const regloopMinTime = 2 * time.Second

// pause ensures that top-level registration loop iterations take at least regLoopMinTime.
// This prevents the loop from running too hot when the local node table is very empty.
func (reg *topicReg) pause(lastTime mclock.AbsTime) bool {
	d := reg.clock.Now().Sub(lastTime)
	if d < regloopMinTime {
		sleep := reg.clock.NewTimer(regloopMinTime - d)
		defer sleep.Stop()
		for {
			select {
			case <-sleep.C():
				return false
			case <-reg.newNodesCh:
				// Drain the channel to avoid blocking the Table's feed sender.
			case id := <-reg.evictCh:
				reg.state.RemoveNode(id)
			case ch := <-reg.statsCh:
				ch <- reg.state.BucketStats()
			case <-reg.quit:
				return true
			}
		}
	}
	return false
}

func (reg *topicReg) runRegistration(sys *topicSystem) (exit bool) {
	var (
		updateEv      = mclock.NewAlarm(reg.clock)
		nextAttempt   topicRegJob
		sendAttemptCh chan<- topicRegJob
	)

	for {
		if reg.state.NodeCount() == 0 {
			return false
		}

		var updateCh <-chan struct{}
		if sendAttemptCh == nil {
			next := reg.state.NextUpdateTime()
			if next != topicindex.Never {
				updateEv.Schedule(next)
				updateCh = updateEv.C()
			}
		}

		select {
		case <-reg.quit:
			return true

		case ch := <-reg.statsCh:
			ch <- reg.state.BucketStats()

		case n := <-reg.newNodesCh:
			if topicindex.SupportsTopicDiscovery(n) && !sys.isBanned(n.ID()) {
				reg.state.AddNodes(nil, []*enode.Node{n})
			}

		case id := <-reg.evictCh:
			reg.state.RemoveNode(id)
			// If the evicted node was the attempt selected for the next
			// request, cancel the pending send: its attempt has been removed,
			// so StartRequest would operate on a stale (removed) attempt.
			if sendAttemptCh != nil && nextAttempt.node.ID() == id {
				sendAttemptCh = nil
			}

		case <-updateCh:
			attempt := reg.state.Update()
			for attempt != nil && sys.isBanned(attempt.Node.ID()) {
				reg.state.RemoveNode(attempt.Node.ID())
				attempt = reg.state.Update()
			}
			if attempt != nil {
				sendAttemptCh = reg.regRequest
				nextAttempt = topicRegJob{
					attempt: attempt,
					node:    attempt.Node,
					ticket:  attempt.Ticket,
					renewal: reg.state.WasRegistered(attempt.Node.ID()),
				}
				nextAttempt.buckets = reg.state.BucketsWithFreeSpace(nextAttempt.buckets[:0])
			}

		case sendAttemptCh <- nextAttempt:
			reg.state.StartRequest(nextAttempt.attempt)
			sendAttemptCh = nil

		case resp := <-reg.regResponse:
			if resp.err == errClosed {
				continue // shutdown cancel: liveness unknown, not a failure
			}
			if len(resp.nodes) > 0 {
				reg.state.AddNodes(resp.att.Node, sys.filterNodes(resp.nodes))
			}
			if resp.err != nil {
				reg.state.HandleErrorResponse(resp.att, resp.err)
				if resp.err == errTimeout {
					// Count the timeout toward the global eviction counter
					sys.transport.trackTopicRequest(resp.att.Node, false)
					sys.ban(resp.att.Node.ID())
				}
				continue
			}
			// The node responded: reset its global counter
			sys.transport.trackTopicRequest(resp.att.Node, true)
			sys.unban(resp.att.Node.ID())
			wt := time.Duration(resp.msg.WaitTime) * time.Millisecond
			if len(resp.msg.Ticket) > 0 {
				reg.state.HandleTicketResponse(resp.att, resp.msg.Ticket, wt)
			} else {
				reg.state.HandleRegistered(resp.att, wt)
			}
		}
	}
}

// topicRegJob is a dispatch job handed from the event loop to the request
// worker. It carries a value snapshot of the attempt's node and ticket, so
// the worker does not share *RegAttempt state with the event loop goroutine.
// The attempt field is only used for response correlation on the way back.
type topicRegJob struct {
	attempt *topicindex.RegAttempt
	node    *enode.Node
	ticket  []byte
	buckets []uint
	renewal bool
}

type topicRegResult struct {
	msg   *v5wire.Regconfirmation
	nodes []*enode.Node
	err   error

	att *topicindex.RegAttempt
}

// sendRequestsLoop performs topic registration requests.
func (reg *topicReg) sendRequestsLoop(sys *topicSystem) {
	defer reg.wg.Done()

	for job := range reg.regRequest {
		var resp topicRegResult
		if sys.isBanned(job.node.ID()) {
			// Banned while the job waited for the worker.
			resp.err = errBanned
		} else {
			resp = sys.transport.regtopic(reg.quit, job.node, reg.state.Topic(), job.ticket, job.buckets, reg.opid, job.renewal)
		}
		resp.att = job.attempt

		select {
		case reg.regResponse <- resp:
		case <-reg.quit:
			return
		}
	}
}

var errBanned = errors.New("banned")

const (
	// searchPassBackoff caps the gap between passes that change nothing
	// (the default of Config.SearchPassBackoff).
	searchPassBackoff = time.Minute
	// searchUnreadLimit is the number of results that may wait unread before
	// the search stops querying. It resumes when the consumer reads again.
	// The search uses Config.TopicNodesLimit, of which this is the default.
	searchUnreadLimit = topicindex.TopicNodesLimit
)

// topicSearch handles searching in a single topic.
type topicSearch struct {
	topic  topicindex.TopicID
	opid   uint64
	config topicindex.Config

	wg   sync.WaitGroup
	quit chan struct{}

	queryCh      chan topicQueryJob
	queryRespCh  chan topicQueryResult
	resultCh     chan *enode.Node
	readCh       chan struct{} // the consumer took a result
	resultFilter *topicindex.SearchFilter
	activeBucket int
	retried      bool          // the last pass kept its bucket because every query to it failed
	idlePasses   int           // passes in a row that neither returned anything new nor moved the active bucket
	queries      atomic.Uint64 // TOPICQUERY requests sent, read by tests
	asked        *topicindex.AskedSet

	newNodesCh  chan *enode.Node
	newNodesSub event.Subscription

	// contacted counts distinct nodes queried; stats the rest of the
	// search's progress.
	contactsMu sync.Mutex
	contacted  map[enode.ID]struct{}
	stats      TopicSearchStats
	started    mclock.AbsTime
}

// TopicSearchStats counts a topic search's work since it started.
type TopicSearchStats struct {
	Passes    int `json:"passes"`    // passes started over the search table
	Queries   int `json:"queries"`   // TOPICQUERY requests sent
	Contacted int `json:"contacted"` // distinct nodes queried
	Received  int `json:"received"`  // registrants in TOPICNODES replies
	Duplicate int `json:"duplicate"` // received results already seen in the same pass
	Filtered  int `json:"filtered"`  // results dropped as recently returned
	Yielded   int `json:"yielded"`   // results handed to the iterator

	MaxTopicPerReply int `json:"maxTopicPerReply"` // most registrants in one TOPICNODES reply
	MaxAuxPerReply   int `json:"maxAuxPerReply"`   // most neighbours in one NODES reply to a query

	// QueriesByBucket counts TOPICQUERY requests by the search-table bucket of
	// the queried node, index 0 the farthest from the topic. ActiveTrace records
	// every move of the search's active bucket, and the bucket each pass
	// starts in, in milliseconds since the search began.
	QueriesByBucket []int        `json:"queriesByBucket,omitempty"`
	ActiveTrace     []BucketStep `json:"activeTrace,omitempty"`
}

// BucketStep is one point of TopicSearchStats.ActiveTrace.
type BucketStep struct {
	AtMs   int64 `json:"atMs"`
	Bucket int   `json:"bucket"`
}

func newTopicSearch(sys *topicSystem, topic topicindex.TopicID, out chan *enode.Node, opid uint64) *topicSearch {
	s := &topicSearch{
		topic:        topic,
		config:       sys.config,
		opid:         opid,
		quit:         make(chan struct{}),
		resultCh:     out,
		resultFilter: topicindex.NewSearchFilter(sys.config),
		asked:        topicindex.NewAskedSet(sys.config),
		started:      sys.config.Clock.Now(),

		queryCh:     make(chan topicQueryJob),
		queryRespCh: make(chan topicQueryResult),
		readCh:      make(chan struct{}, 1),
		contacted:   make(map[enode.ID]struct{}),
	}

	s.newNodesCh = make(chan *enode.Node, 100)
	s.newNodesSub = sys.transport.tab.subscribeNodes(s.newNodesCh)

	s.wg.Add(2)
	go s.runLoop(sys)
	go s.runRequests(sys)
	return s
}

func (s *topicSearch) stop() {
	close(s.quit)
	s.wg.Wait()
}

func (s *topicSearch) runLoop(sys *topicSystem) {
	defer s.wg.Done()
	defer s.newNodesSub.Unsubscribe()
	defer s.closeDown()

	time := mclock.AbsTime(-1)
	gap := regloopMinTime
	cycle := 0
	for {
		if time >= 0 {
			if exit := s.pause(time, gap); exit {
				return
			}
		}
		time = s.config.Clock.Now()

		state := topicindex.NewSearch(s.topic, s.config)
		state.SetCycle(cycle)
		cycle++
		state.SetActiveBucket(s.activeBucket)
		state.SetAsked(s.asked)
		nodes := sys.filterNodes(sys.transport.tab.allNodes())
		if len(nodes) == 0 {
			continue
		}
		shuffleNodes(nodes)
		state.AddNodes(nil, nodes)
		s.contactsMu.Lock()
		s.stats.Passes++
		s.recordActive(state.ActiveBucket())
		s.contactsMu.Unlock()

		exit, nresults := s.run(sys, state)
		moved := s.nextPass(state)
		if exit {
			return
		}
		gap = s.passGap(nresults, moved)
	}
}

// nextPass sets where the next pass starts from the state of the one that
// ended, and reports whether it moved. A pass that saw no ads counts as idle
// wherever it lands, so that the back-off grows on an empty topic. A bucket
// whose queries all failed is kept for one retry pass only, so that a dead
// node cannot pin the search.
func (s *topicSearch) nextPass(state *topicindex.Search) (moved bool) {
	next := state.NextPassBucket()
	retry := next == s.activeBucket && state.ActiveFailed()
	if retry && s.retried {
		state.SetActiveBucket(next + 1)
		next, retry = state.ActiveBucket(), false
	}
	s.retried = retry
	moved = next != s.activeBucket && state.AdsSeen() > 0
	s.activeBucket = next
	return moved
}

// passGap is the minimum length of the next pass: regloopMinTime, doubling
// after every pass that neither returned anything new nor moved the active
// bucket, up to searchPassBackoff.
func (s *topicSearch) passGap(nresults int, moved bool) time.Duration {
	backoff := s.config.PassBackoff()
	if nresults > 0 || moved || backoff <= 0 {
		s.idlePasses = 0
		return regloopMinTime
	}
	s.idlePasses++
	if s.idlePasses >= 30 {
		return backoff
	}
	return min(backoff, regloopMinTime<<s.idlePasses)
}

// pause ensures that top-level search loop iterations take at least gap.
// This prevents the loop from running too hot when the local node table is very empty.
func (s *topicSearch) pause(lastTime mclock.AbsTime, gap time.Duration) bool {
	d := s.config.Clock.Now().Sub(lastTime)
	if d < gap {
		sleep := s.config.Clock.NewTimer(gap - d)
		defer sleep.Stop()
		for {
			select {
			case <-sleep.C():
				return false
			case <-s.newNodesCh:
				// Drain the channel to avoid blocking the Table's feed sender.
			case <-s.quit:
				return true
			}
		}
	}
	return false
}

type topicQueryJob struct {
	dst     *enode.Node
	buckets []uint
}

func (s *topicSearch) run(sys *topicSystem, state *topicindex.Search) (exit bool, nresults int) {
	var (
		queryCh   chan<- topicQueryJob
		nextQuery topicQueryJob
		resultCh  chan<- *enode.Node
		result    *enode.Node
		released  bool // the pass is over and its waiting results are handed out
	)
	lastActive := state.ActiveBucket()

	// Results the consumer has not read wait in the search's buffer or in the
	// result channel. The search sends no query while TopicNodesLimit of them
	// wait, and ends no pass while any waits.
	unreadLimit, _ := s.config.ResponseLimits()
	unread := func() int { return state.Buffered() + len(s.resultCh) }
	for {
		if queryCh == nil && unread() == 0 && state.IsDone() {
			// The pass is over: hand out the results that still wait in the filter.
			if held := s.resultFilter.Release(); len(held) > 0 {
				added := state.AddResults(held)
				s.contactsMu.Lock()
				s.stats.Duplicate += len(held) - added
				s.contactsMu.Unlock()
				released = true
				continue
			}
			s.config.Log.Debug("Topic search rollover", "topic", s.topic, "nres", nresults)
			return false, nresults
		}
		if queryCh == nil && !released && unread() < unreadLimit {
			target, skipped := state.QueryTarget(), false
			for target != nil && sys.isBanned(target.ID()) {
				state.HandleErrorResponse(target, errBanned)
				target, skipped = state.QueryTarget(), true
			}
			if skipped && target == nil {
				continue // the skips may have ended the pass
			}
			if target != nil {
				queryCh = s.queryCh
				nextQuery = topicQueryJob{dst: target}
				nextQuery.buckets = state.BucketsWithFreeSpace(nextQuery.buckets[:0])
			}
		}
		if n := state.PeekResult(); n != nil {
			result = n
			resultCh = s.resultCh
		}

		select {
		case <-s.quit:
			return true, nresults

		case <-s.newNodesCh:
			// Drained here as in pause: the table's feed sends under its
			// mutex and would stall every lookup once the channel is full.
		case <-s.readCh:

		case queryCh <- nextQuery:
			s.queries.Add(1)
			s.contactsMu.Lock()
			if s.contacted == nil { // tests build topicSearch by hand
				s.contacted = make(map[enode.ID]struct{})
			}
			s.contacted[nextQuery.dst.ID()] = struct{}{}
			s.countQueryBucket(state.BucketIndex(nextQuery.dst.ID()), state.NumBuckets())
			s.contactsMu.Unlock()
		case resp := <-s.queryRespCh:
			switch {
			case resp.err == errClosed:
				// shutdown cancel: liveness unknown, not a failure.
			case resp.err != nil && len(resp.topicNodes)+len(resp.auxNodes) == 0:
				// No nodes at all: drop the node from this search. A timeout counts
				// toward its global wire eviction
				state.HandleErrorResponse(resp.src, resp.err)
				if resp.err == errTimeout {
					sys.transport.trackTopicRequest(resp.src, false)
					sys.ban(resp.src.ID())
				}
				// Results waiting in the filter may be due by now.
				state.AddResults(s.resultFilter.Take(resp.src.ID(), nil))
			default:
				if resp.err != nil {
					// Partial response: nodes arrived before the error, so the
					// node counts as responsive and is kept. Still log the error.
					s.config.Log.Debug("TOPICQUERY/v5 failed", "topic", s.topic, "id", resp.src.ID(), "err", resp.err)
				}
				// The node responded: reset its global counter
				sys.transport.trackTopicRequest(resp.src, true)
				sys.unban(resp.src.ID())
				// The density sample is the reply's size as the registrar sent
				// it, before the searcher's own filtering drops records.
				ads := filterTopicDiscovery(resp.topicNodes)
				nads, sent := len(ads), len(resp.topicNodes)
				state.RecordReach(resp.src, ads)
				dropped := s.resultFilter.Dropped()
				taken := s.resultFilter.Take(resp.src.ID(), ads)
				dropped = s.resultFilter.Dropped() - dropped
				var added int
				if resp.err != nil {
					added = state.AddPartialReply(resp.src, taken, nads, sent)
				} else {
					added = state.AddReply(resp.src, taken, nads, sent)
				}
				state.AddNodes(resp.src, sys.filterNodes(resp.auxNodes))
				s.contactsMu.Lock()
				s.stats.Received += nads
				s.stats.Duplicate += len(taken) - added
				s.stats.Filtered += dropped
				s.stats.MaxTopicPerReply = max(s.stats.MaxTopicPerReply, len(resp.topicNodes))
				s.stats.MaxAuxPerReply = max(s.stats.MaxAuxPerReply, len(resp.auxNodes))
				if a := state.ActiveBucket(); a != lastActive {
					lastActive = a
					s.recordActive(a)
				}
				s.contactsMu.Unlock()
			}
			queryCh = nil

		case resultCh <- result:
			nresults++
			s.contactsMu.Lock()
			s.stats.Yielded++
			s.contactsMu.Unlock()
			s.resultFilter.Add(result)
			state.PopResult()
			result, resultCh = nil, nil
		}
	}
}

// countQueryBucket and recordActive are called with contactsMu held.
func (s *topicSearch) countQueryBucket(bi, n int) {
	if bi < 0 {
		return
	}
	for len(s.stats.QueriesByBucket) < n {
		s.stats.QueriesByBucket = append(s.stats.QueriesByBucket, 0)
	}
	s.stats.QueriesByBucket[bi]++
}

func (s *topicSearch) recordActive(bi int) {
	at := int64(s.config.Clock.Now().Sub(s.started) / time.Millisecond)
	s.stats.ActiveTrace = append(s.stats.ActiveTrace, BucketStep{AtMs: at, Bucket: bi})
}

func (s *topicSearch) closeDown() {
	close(s.queryCh)
	close(s.resultCh)
	// Drain the result channel. This guarantees that, when the iterator's
	// Close returns, Next will always return false.
	for range s.resultCh {
	}
}

type topicQueryResult struct {
	src *enode.Node

	topicNodes []*enode.Node
	auxNodes   []*enode.Node
	err        error
}

func (s *topicSearch) runRequests(sys *topicSystem) {
	defer s.wg.Done()

	for job := range s.queryCh {
		var result topicQueryResult
		if sys.isBanned(job.dst.ID()) {
			result.err = errBanned
		} else {
			result = sys.transport.topicQuery(s.quit, job.dst, s.topic, job.buckets, s.opid)
		}
		result.src = job.dst

		select {
		case s.queryRespCh <- result:
		case <-s.quit:
			return
		}
	}
}

// topicSearchIterator implements enode.Iterator.
type topicSearchIterator struct {
	sys     *topicSystem
	search  *topicSearch
	ch      <-chan *enode.Node
	closing sync.Once
	cur     *enode.Node
}

func newTopicSearchIterator(sys *topicSystem, search *topicSearch, ch <-chan *enode.Node) *topicSearchIterator {
	return &topicSearchIterator{sys: sys, search: search, ch: ch}
}

func (tsi *topicSearchIterator) Next() bool {
	n, ok := <-tsi.ch
	tsi.cur = n
	select {
	case tsi.search.readCh <- struct{}{}:
	default:
	}
	return ok
}

func (tsi *topicSearchIterator) Node() *enode.Node {
	return tsi.cur
}

// Contacts reports how many TOPICQUERY requests the search has sent and to how
// many distinct nodes.
func (tsi *topicSearchIterator) Contacts() (queries, nodes int) {
	tsi.search.contactsMu.Lock()
	defer tsi.search.contactsMu.Unlock()
	return int(tsi.search.queries.Load()), len(tsi.search.contacted)
}

// Stats reports the search's progress counters.
func (tsi *topicSearchIterator) Stats() TopicSearchStats {
	tsi.search.contactsMu.Lock()
	defer tsi.search.contactsMu.Unlock()
	st := tsi.search.stats
	st.Queries, st.Contacted = int(tsi.search.queries.Load()), len(tsi.search.contacted)
	st.QueriesByBucket = append([]int(nil), st.QueriesByBucket...)
	st.ActiveTrace = append([]BucketStep(nil), st.ActiveTrace...)
	return st
}

func (tsi *topicSearchIterator) Close() {
	tsi.closing.Do(tsi.search.stop)
}

// filterTopicDiscovery returns only the nodes that advertise a supported
// version of the topic-discovery capability in their ENR.
func filterTopicDiscovery(nodes []*enode.Node) []*enode.Node {
	filtered := make([]*enode.Node, 0, len(nodes))
	for _, n := range nodes {
		if topicindex.SupportsTopicDiscovery(n) {
			filtered = append(filtered, n)
		}
	}
	return filtered
}
