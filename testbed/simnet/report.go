package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

func report(results []searchResult, target int, metricsOut string, regCoverage registrationCoverage) {
	if len(results) == 0 {
		fmt.Println("no searchers")
		return
	}

	var (
		latenciesFirst    []time.Duration
		latenciesComplete []time.Duration
		recallSum         float64
		hitTimeout        int
		fullRecall        int
	)
	for _, r := range results {
		if r.TimeToFirst > 0 {
			latenciesFirst = append(latenciesFirst, r.TimeToFirst)
		}
		latenciesComplete = append(latenciesComplete, r.TimeToCompletion)
		recallSum += float64(r.FoundRegistrant) / float64(target)
		if r.HitTimeoutBefore {
			hitTimeout++
		}
		if r.FoundRegistrant >= target {
			fullRecall++
		}
	}

	fmt.Println()
	fmt.Println("=== workload summary ===")
	fmt.Printf("searchers:                 %d\n", len(results))
	fmt.Printf("registrants (target):      %d\n", target)
	fmt.Printf("full recall (found all):   %d / %d\n", fullRecall, len(results))
	fmt.Printf("mean recall:               %.2f\n", recallSum/float64(len(results)))
	fmt.Printf("hit timeout:               %d / %d\n", hitTimeout, len(results))
	if len(latenciesFirst) > 0 {
		fmt.Printf("time to first result:      median=%s p95=%s\n",
			percentile(latenciesFirst, 50), percentile(latenciesFirst, 95))
	}
	fmt.Printf("time to completion:        median=%s p95=%s\n",
		percentile(latenciesComplete, 50), percentile(latenciesComplete, 95))

	if metricsOut != "" {
		writeMetrics(metricsOut, results, target, regCoverage)
		fmt.Printf("metrics written to: %s\n", metricsOut)
	}
}

func writeMetrics(path string, results []searchResult, target int, regCoverage registrationCoverage) {
	out := map[string]any{
		"target":               target,
		"numSearchers":         len(results),
		"results":              results,
		"registrationCoverage": regCoverage,
	}
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "metrics: open %s: %v\n", path, err)
		return
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintf(os.Stderr, "metrics: encode: %v\n", err)
	}
}

func reportMultiTopic(results []searchResult, registrantsByTopic map[int]map[enode.ID]struct{}, topics []topicindex.TopicID, regTimingNs map[string]map[string]int64, metricsOut string, cov multiTopicCoverage) {
	type topicReport struct {
		Topic        int     `json:"topic"`
		NumSearchers int     `json:"numSearchers"`
		Target       int     `json:"target"`
		FullRecall   int     `json:"fullRecall"`
		MeanRecall   float64 `json:"meanRecall"`
		HitTimeout   int     `json:"hitTimeout"`
	}
	byTopic := make(map[int][]searchResult)
	for _, r := range results {
		byTopic[r.Topic] = append(byTopic[r.Topic], r)
	}
	topicIdxs := make([]int, 0, len(byTopic))
	for t := range byTopic {
		topicIdxs = append(topicIdxs, t)
	}
	sort.Ints(topicIdxs)

	reports := make([]topicReport, 0, len(topicIdxs))
	fmt.Println()
	fmt.Println("=== per-topic search summary ===")
	fmt.Printf("%-6s %12s %8s %12s %10s %10s\n", "topic", "searchers", "target", "fullRecall", "meanRec", "timeout")
	for _, t := range topicIdxs {
		rs := byTopic[t]
		var (
			full, timeout int
			recallSum     float64
			counted       int
		)
		for _, r := range rs {
			if r.Target > 0 {
				recallSum += float64(r.FoundRegistrant) / float64(r.Target)
				counted++
				if r.FoundRegistrant >= r.Target {
					full++
				}
			}
			if r.HitTimeoutBefore {
				timeout++
			}
		}
		mean := 0.0
		if counted > 0 {
			mean = recallSum / float64(counted)
		}
		target := 0
		if rs[0].Target > 0 {
			target = rs[0].Target
		}
		fmt.Printf("%-6d %12d %8d %12s %10.4f %10d\n", t, len(rs), target,
			fmt.Sprintf("%d/%d", full, len(rs)), mean, timeout)
		reports = append(reports, topicReport{
			Topic: t, NumSearchers: len(rs), Target: target,
			FullRecall: full, MeanRecall: mean, HitTimeout: timeout,
		})
	}

	if len(regTimingNs) > 0 {
		fmt.Println()
		fmt.Println("=== registration timing (time to first remote admission, ms) ===")
		fmt.Printf("%-6s %12s %12s %12s\n", "topic", "n", "mean", "std")
		for _, t := range topicIdxs {
			th := topics[t].String()
			vals := regTimingNs[th]
			if len(vals) == 0 {
				continue
			}
			var sum, sumSq float64
			for _, ns := range vals {
				ms := float64(ns) / 1e6
				sum += ms
				sumSq += ms * ms
			}
			n := float64(len(vals))
			mean := sum / n
			variance := sumSq/n - mean*mean
			if variance < 0 {
				variance = 0
			}
			fmt.Printf("%-6d %12d %12.1f %12.1f\n", t, len(vals), mean, variance)
		}
	}

	if metricsOut != "" {
		out := map[string]any{
			"perTopic":             reports,
			"results":              results,
			"registrationCoverage": cov,
			"registrationTimingNs": regTimingNs,
		}
		f, err := os.Create(metricsOut)
		if err != nil {
			fmt.Fprintf(os.Stderr, "metrics: open %s: %v\n", metricsOut, err)
			return
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			fmt.Fprintf(os.Stderr, "metrics: encode: %v\n", err)
		}
		fmt.Printf("metrics written to: %s\n", metricsOut)
	}
}
