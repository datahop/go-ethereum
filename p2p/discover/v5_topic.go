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
	"math/rand"
	"sync"
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

	wg   sync.WaitGroup
	quit chan struct{}
}

func newTopicSystem(transport *UDPv5, config topicindex.Config) *topicSystem {
	sys := &topicSystem{
		transport: transport,
		config:    config,
		reg:       make(map[topicindex.TopicID]*topicReg),
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
		nodes := filterTopicDiscovery(sys.transport.tab.allNodes())
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

		case n := <-reg.newNodesCh:
			if topicindex.SupportsTopicDiscovery(n) {
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
			if attempt != nil {
				sendAttemptCh = reg.regRequest
				nextAttempt = topicRegJob{
					attempt: attempt,
					node:    attempt.Node,
					ticket:  attempt.Ticket,
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
				reg.state.AddNodes(resp.att.Node, filterTopicDiscovery(resp.nodes))
			}
			if resp.err != nil {
				reg.state.HandleErrorResponse(resp.att, resp.err)
				if resp.err == errTimeout {
					sys.transport.evictTopicTableNode(resp.att.Node.ID())
				}
				continue
			}
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
		topic := reg.state.Topic()
		resp := sys.transport.regtopic(reg.quit, job.node, topic, job.ticket, job.buckets, reg.opid)
		resp.att = job.attempt

		select {
		case reg.regResponse <- resp:
		case <-reg.quit:
			return
		}
	}
}

// topicSearch handles searching in a single topic.
type topicSearch struct {
	topic  topicindex.TopicID
	opid   uint64
	config topicindex.Config

	wg   sync.WaitGroup
	quit chan struct{}

	queryCh     chan topicQueryJob
	queryRespCh chan topicQueryResult
	resultCh    chan *enode.Node

	newNodesCh  chan *enode.Node
	newNodesSub event.Subscription
}

func newTopicSearch(sys *topicSystem, topic topicindex.TopicID, out chan *enode.Node, opid uint64) *topicSearch {
	s := &topicSearch{
		topic:    topic,
		config:   sys.config,
		opid:     opid,
		quit:     make(chan struct{}),
		resultCh: out,

		queryCh:     make(chan topicQueryJob),
		queryRespCh: make(chan topicQueryResult),
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
	for {
		if time >= 0 {
			if exit := s.pause(time); exit {
				return
			}
		}
		time = s.config.Clock.Now()

		state := topicindex.NewSearch(s.topic, s.config)
		nodes := filterTopicDiscovery(sys.transport.tab.allNodes())
		if len(nodes) == 0 {
			continue
		}
		shuffleNodes(nodes)
		state.AddNodes(nil, nodes)

		if exit := s.run(sys, state); exit {
			return
		}
	}
}

// pause ensures that top-level search loop iterations take at least regLoopMinTime.
// This prevents the loop from running too hot when the local node table is very empty.
func (s *topicSearch) pause(lastTime mclock.AbsTime) bool {
	d := s.config.Clock.Now().Sub(lastTime)
	if d < regloopMinTime {
		sleep := s.config.Clock.NewTimer(regloopMinTime - d)
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

func (s *topicSearch) run(sys *topicSystem, state *topicindex.Search) (exit bool) {
	var (
		queryCh   chan<- topicQueryJob
		nextQuery topicQueryJob
		resultCh  chan<- *enode.Node
		result    *enode.Node
		nresults  int
	)

	for {
		if state.IsDone() {
			s.config.Log.Debug("Topic search rollover", "topic", s.topic, "nres", nresults)
			return false
		}
		if queryCh == nil {
			target := state.QueryTarget()
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
			return true

		case queryCh <- nextQuery:
		case resp := <-s.queryRespCh:
			switch {
			case resp.err == errClosed:
				// shutdown cancel: liveness unknown, not a failure.
			case resp.err != nil && len(resp.topicNodes)+len(resp.auxNodes) == 0:
				// No nodes at all: drop the node. A partial response (some nodes
				// before an error) is treated as a response by the default case.
				state.HandleErrorResponse(resp.src, resp.err)
				// A timeout means the node is dead globally: evict its ads and
				// its parked attempts in every reg table, as a DHT removal does.
				if resp.err == errTimeout {
					sys.evictNode(resp.src.ID())
				}
			default:
				if resp.err != nil {
					// Partial response: nodes arrived before the error, so the
					// node counts as responsive and is kept. Still log the error.
					s.config.Log.Debug("TOPICQUERY/v5 failed", "topic", s.topic, "id", resp.src.ID(), "err", resp.err)
				}
				state.AddNodes(resp.src, filterTopicDiscovery(resp.auxNodes))
				state.AddQueryResults(resp.src, filterTopicDiscovery(resp.topicNodes))
			}
			queryCh = nil

		case resultCh <- result:
			nresults++
			state.PopResult()
			result, resultCh = nil, nil
		}
	}
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
		result := sys.transport.topicQuery(s.quit, job.dst, s.topic, job.buckets, s.opid)
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
	return ok
}

func (tsi *topicSearchIterator) Node() *enode.Node {
	return tsi.cur
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
