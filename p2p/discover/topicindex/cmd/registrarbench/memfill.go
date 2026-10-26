package main

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// heapAfterGC returns live heap bytes after forcing collection.
func heapAfterGC() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func parseLevels(s string) ([]float64, error) {
	var out []float64
	for _, f := range strings.Split(s, ",") {
		v, err := strconv.ParseFloat(strings.TrimSpace(f), 64)
		if err != nil || v < 0 || v > 1 {
			return nil, fmt.Errorf("invalid memfill level %q", f)
		}
		out = append(out, v)
	}
	return out, nil
}

// runMemfill fills a table to controlled occupancy levels with TopicTable.Add
// (no admission control, no expiry) and records the post-GC live heap at each
// level. The slope of heap over occupancy is the marginal cost of one cached
// ad. Variant "shared" stores the population's own *enode.Node (ENR storage is
// excluded, as in the original harness); "independent" stores a freshly
// decoded copy per ad, as the production REGTOPIC handler does.
func runMemfill(c config, plat platform, enc *json.Encoder) error {
	levels, err := parseLevels(c.MemfillLevels)
	if err != nil {
		return err
	}
	if c.CacheSize == 0 {
		c.CacheSize = topicindex.NewTopicTable(topicindex.Config{}).Stats().Capacity
	}
	c.Advertisers = c.CacheSize
	w, data, err := population(c)
	if err != nil {
		return err
	}
	encSize := 0
	for _, a := range w.ads {
		encSize += len(a.enc)
	}
	if err := enc.Encode(map[string]any{"type": "metadata", "schema_version": 6, "mode": "memfill", "config": c, "ip_dist": c.ipDist(), "dataset": data, "platform": plat,
		"mean_encoded_enr_bytes": float64(encSize) / float64(len(w.ads)), "go_version": plat.GoVersion, "revision": plat.Revision, "dirty": plat.Dirty, "arch": plat.GOARCH}); err != nil {
		return err
	}
	for rep := 0; rep < c.MemfillReps; rep++ {
		for _, variant := range []string{"shared", "independent"} {
			clock := new(mclock.Simulated)
			tab := topicindex.NewTopicTable(topicindex.Config{AdCacheSize: c.CacheSize, AdLifetime: c.Lifetime, Clock: clock})
			order := w.ready[w.head:]
			filled := 0
			for _, level := range levels {
				target := int(level * float64(c.CacheSize))
				for ; filled < target; filled++ {
					a := &w.ads[order[filled%len(order)]]
					node := a.node
					if variant == "independent" {
						if node, err = decodeNode(a.enc); err != nil {
							return err
						}
					}
					if !tab.Add(node, w.topics[a.topic]) {
						return fmt.Errorf("memfill: table full at %d", filled)
					}
				}
				heap := heapAfterGC()
				if err := enc.Encode(map[string]any{"type": "memfill", "rep": rep, "variant": variant, "level": level, "occupancy": tab.Occupancy(), "heap_bytes": heap, "state": tab.Stats()}); err != nil {
					return err
				}
			}
			runtime.KeepAlive(tab)
			// Lower-bound state, on a separate table at 50% occupancy: one
			// ticket per occupied topic records a topic bound. IP bounds are
			// stored inside existing tree vertices and need no allocation.
			tab = topicindex.NewTopicTable(topicindex.Config{AdCacheSize: c.CacheSize, AdLifetime: c.Lifetime, Clock: clock})
			for i := 0; i < c.CacheSize/2; i++ {
				a := &w.ads[order[i%len(order)]]
				node := a.node
				if variant == "independent" {
					if node, err = decodeNode(a.enc); err != nil {
						return err
					}
				}
				tab.Add(node, w.topics[a.topic])
			}
			var probe []*enode.Node
			for t := range w.topics {
				if tab.TopicSize(w.topics[t]) > 0 {
					n, err := decodeNode(w.ads[order[(c.CacheSize/2+t)%len(order)]].enc)
					if err != nil {
						return err
					}
					probe = append(probe, n)
				}
			}
			before, beforeBounds, occ := heapAfterGC(), tab.Stats().TopicBounds, tab.Occupancy()
			i := 0
			for t := range w.topics {
				if tab.TopicSize(w.topics[t]) > 0 {
					tab.Register(probe[i], w.topics[t], 0)
					i++
				}
			}
			after, afterBounds := heapAfterGC(), tab.Stats().TopicBounds
			if tab.Occupancy() != occ {
				return fmt.Errorf("memfill: bound probe admitted %d ads", tab.Occupancy()-occ)
			}
			if err := enc.Encode(map[string]any{"type": "memfill_bounds", "rep": rep, "variant": variant, "occupancy": occ, "topic_bounds_added": afterBounds - beforeBounds, "heap_delta_bytes": int64(after) - int64(before), "state": tab.Stats()}); err != nil {
				return err
			}
			runtime.KeepAlive(tab)
			runtime.KeepAlive(probe)
		}
	}
	runtime.KeepAlive(w)
	return nil
}
