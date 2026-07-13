package main

import (
	"sync"

	"github.com/ethereum/go-ethereum/p2p/enode"
)

// topicMembership tracks which registrant registered which topic. It is safe
// for concurrent use: searchers read membership on the hot path (lock-free via
// sync.Map) while the steady-state churn loop adds joiners as they register.
type topicMembership struct {
	topicOf sync.Map // enode.ID -> int (topic index)

	mu      sync.Mutex
	byTopic map[int]map[enode.ID]struct{}
}

func newTopicMembership(numTopics int) *topicMembership {
	m := &topicMembership{byTopic: make(map[int]map[enode.ID]struct{}, numTopics)}
	for t := 0; t < numTopics; t++ {
		m.byTopic[t] = make(map[enode.ID]struct{})
	}
	return m
}

// add records that id is a registrant for the given topic.
func (m *topicMembership) add(id enode.ID, topic int) {
	m.topicOf.Store(id, topic)
	m.mu.Lock()
	if m.byTopic[topic] == nil {
		m.byTopic[topic] = make(map[enode.ID]struct{})
	}
	m.byTopic[topic][id] = struct{}{}
	m.mu.Unlock()
}

// has reports whether id registered the given topic (searcher hot path).
func (m *topicMembership) has(id enode.ID, topic int) bool {
	v, ok := m.topicOf.Load(id)
	return ok && v.(int) == topic
}

// topicFor returns the topic id registered, if any.
func (m *topicMembership) topicFor(id enode.ID) (int, bool) {
	v, ok := m.topicOf.Load(id)
	if !ok {
		return 0, false
	}
	return v.(int), true
}

// countTopic returns the current number of registrants for a topic.
func (m *topicMembership) countTopic(topic int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.byTopic[topic])
}

// snapshot returns a deep copy of the per-topic registrant sets, for reporting
// against a stable view while churn keeps mutating the live sets.
func (m *topicMembership) snapshot() map[int]map[enode.ID]struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[int]map[enode.ID]struct{}, len(m.byTopic))
	for t, set := range m.byTopic {
		c := make(map[enode.ID]struct{}, len(set))
		for id := range set {
			c[id] = struct{}{}
		}
		out[t] = c
	}
	return out
}
