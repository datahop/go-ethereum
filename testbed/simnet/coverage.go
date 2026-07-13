package main

import (
	"fmt"
	"sort"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// registrationCoverage holds the post-register-wait snapshot:
//
//	byRegistrant[reg_id]   = how many distinct nodes have reg_id in their topic table
//	byHost[host_id]        = how many distinct registrants this host knows
type registrationCoverage struct {
	ByRegistrant map[string]int `json:"byRegistrant"`
	ByHost       map[string]int `json:"byHost"`
}

// multiTopicCoverage holds per-topic registration coverage post-register-wait.
type multiTopicCoverage struct {
	ByTopic map[int]registrationCoverage `json:"byTopic"`
}

func snapshotRegistrationCoverage(all []nodeRec, registrantIDs map[enode.ID]struct{}) registrationCoverage {
	cov := registrationCoverage{
		ByRegistrant: make(map[string]int),
		ByHost:       make(map[string]int),
	}
	for _, host := range all {
		visible := host.disc.LocalTopicNodes(testTopic)
		hostID := host.ln.ID()
		seen := make(map[enode.ID]struct{})
		for _, n := range visible {
			id := n.ID()
			if id == hostID {
				continue
			}
			if _, ok := registrantIDs[id]; !ok {
				continue
			}
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			cov.ByRegistrant[id.String()]++
		}
		cov.ByHost[hostID.String()] = len(seen)
	}
	return cov
}

func snapshotMultiTopicCoverage(all []nodeRec, registrantsByTopic map[int]map[enode.ID]struct{}, topics []topicindex.TopicID) multiTopicCoverage {
	out := multiTopicCoverage{ByTopic: make(map[int]registrationCoverage)}
	for t, regSet := range registrantsByTopic {
		cov := registrationCoverage{
			ByRegistrant: make(map[string]int),
			ByHost:       make(map[string]int),
		}
		for _, host := range all {
			visible := host.disc.LocalTopicNodes(topics[t])
			hostID := host.ln.ID()
			seen := make(map[enode.ID]struct{})
			for _, n := range visible {
				id := n.ID()
				if id == hostID {
					continue
				}
				if _, ok := regSet[id]; !ok {
					continue
				}
				if _, dup := seen[id]; dup {
					continue
				}
				seen[id] = struct{}{}
				cov.ByRegistrant[id.String()]++
			}
			cov.ByHost[hostID.String()] = len(seen)
		}
		out.ByTopic[t] = cov
	}
	return out
}

func printRegistrationCoverage(cov registrationCoverage, numRegistrants, numHosts int) {
	regCounts := make([]int, 0, numRegistrants)
	for _, c := range cov.ByRegistrant {
		regCounts = append(regCounts, c)
	}
	sort.Ints(regCounts)

	hostCounts := make([]int, 0, numHosts)
	for _, c := range cov.ByHost {
		hostCounts = append(hostCounts, c)
	}
	sort.Ints(hostCounts)

	fmt.Println()
	fmt.Println("=== post-register-wait coverage ===")
	if len(regCounts) == 0 {
		fmt.Println("no registrants visible on any remote node")
	} else {
		fmt.Printf("per-registrant fan-out (hosts that know each registrant):\n")
		fmt.Printf("  registrants visible somewhere: %d / %d\n", len(regCounts), numRegistrants)
		fmt.Printf("  fan-out                        min=%d  med=%d  max=%d  (cap = %d hosts)\n",
			regCounts[0], regCounts[len(regCounts)/2], regCounts[len(regCounts)-1], numHosts)
	}
	if len(hostCounts) > 0 {
		fmt.Printf("per-host registrant view (registrants known by each host):\n")
		fmt.Printf("  view size                      min=%d  med=%d  max=%d  (cap = %d registrants)\n",
			hostCounts[0], hostCounts[len(hostCounts)/2], hostCounts[len(hostCounts)-1], numRegistrants)
	}
}

func printMultiTopicCoverage(all multiTopicCoverage, registrantsByTopic map[int]map[enode.ID]struct{}, numHosts int) {
	fmt.Println()
	fmt.Println("=== post-register-wait coverage (per topic) ===")
	topics := make([]int, 0, len(all.ByTopic))
	for t := range all.ByTopic {
		topics = append(topics, t)
	}
	sort.Ints(topics)
	for _, t := range topics {
		cov := all.ByTopic[t]
		regSet := registrantsByTopic[t]
		fan := make([]int, 0, len(cov.ByRegistrant))
		for _, c := range cov.ByRegistrant {
			fan = append(fan, c)
		}
		sort.Ints(fan)
		fmt.Printf("  topic %d (%d registrants): visible=%d  fan-out min=%d med=%d max=%d (cap=%d)\n",
			t, len(regSet), len(fan),
			minOrZero(fan), medOrZero(fan), maxOrZero(fan), numHosts-1)
	}
}
