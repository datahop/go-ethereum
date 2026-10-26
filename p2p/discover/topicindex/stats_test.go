package topicindex

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
)

func TestStatsReadOnly(t *testing.T) {
	clock := new(mclock.Simulated)
	tab := NewTopicTable(Config{Clock: clock, AdLifetime: time.Second, AdCacheSize: 20})
	n := newNode()
	topic := TopicID{1}
	if !tab.Add(n, topic) {
		t.Fatal("add")
	}
	a, b := tab.Stats(), tab.Stats()
	if a != b || a.Occupancy != 1 || a.OccupiedTopics != 1 || tab.TopicSize(topic) != 1 {
		t.Fatal(a, b)
	}
	clock.Run(time.Second)
	if tab.Stats().Occupancy != 1 {
		t.Fatal("Stats expired state")
	}
	tab.Expire()
	if s := tab.Stats(); s.Occupancy != 0 || s.OccupiedTopics != 0 || s.TopicBounds != 0 {
		t.Fatal(s)
	}
}

func TestStatsAdmissionStructures(t *testing.T) {
	clock := new(mclock.Simulated)
	tab := NewTopicTable(Config{Clock: clock, AdLifetime: time.Minute})
	first := nodeWithIP([]byte{8, 8, 8, 8})
	second := nodeWithIP([]byte{1, 1, 1, 1})
	topic := TopicID{9}
	if tab.Register(first, topic, 0) != 0 {
		t.Fatal("initial registration")
	}
	if tab.Register(second, topic, 0) <= 0 {
		t.Fatal("expected wait")
	}
	a := tab.Stats()
	if a.IPv4Entries != 1 || a.IPv4Nodes < 1 || a.TopicBounds != 1 || a.EffectiveTopicBounds != 1 {
		t.Fatal(a)
	}
	tab.RemoveNode(first.ID())
	b := tab.Stats()
	if b.Occupancy != 0 || b.IPv4Entries != 0 || b.TopicBounds != 0 {
		t.Fatal(b)
	}
}
