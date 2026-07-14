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
		uniqRecallSum     float64
		dupRecallSum      float64
		uniqCountSum      float64
		hitTimeout        int
		fullRecall        int
	)
	for _, r := range results {
		if r.TimeToFirst > 0 {
			latenciesFirst = append(latenciesFirst, r.TimeToFirst)
		}
		latenciesComplete = append(latenciesComplete, r.TimeToCompletion)
		uniqRecallSum += float64(r.UniqueRegistrant) / float64(target)
		dupRecallSum += float64(r.FoundRegistrant) / float64(target)
		uniqCountSum += float64(r.UniqueRegistrant)
		if r.HitTimeoutBefore {
			hitTimeout++
		}
		if r.UniqueRegistrant >= target {
			fullRecall++
		}
	}

	fmt.Println()
	fmt.Println("=== workload summary ===")
	fmt.Printf("searchers:                       %d\n", len(results))
	fmt.Printf("registrants (target):            %d\n", target)
	fmt.Printf("full recall (uniq >= target):    %d / %d\n", fullRecall, len(results))
	fmt.Printf("mean recall (uniq/target):       %.4f\n", uniqRecallSum/float64(len(results)))
	fmt.Printf("mean unique registrants seen:    %.1f\n", uniqCountSum/float64(len(results)))
	fmt.Printf("mean duplicate-count ratio:      %.4f\n", dupRecallSum/float64(len(results)))
	fmt.Printf("hit timeout:                     %d / %d\n", hitTimeout, len(results))
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
	// Compact (no indent): the 10k per-searcher found sets make this file
	// multi-GB; pretty-printing ~doubles size and write time.
	enc := json.NewEncoder(f)
	if err := enc.Encode(out); err != nil {
		fmt.Fprintf(os.Stderr, "metrics: encode: %v\n", err)
	}
}

func reportMultiTopic(results []searchResult, registrantsByTopic map[int]map[enode.ID]struct{}, topics []topicindex.TopicID, regTimingNs map[string]map[string]int64, metricsOut string, cov multiTopicCoverage) {
	type topicReport struct {
		Topic              int     `json:"topic"`
		NumSearchers       int     `json:"numSearchers"`
		Target             int     `json:"target"`
		FullRecall         int     `json:"fullRecall"`         // count of searchers whose unique-registrants >= target
		MeanRecall         float64 `json:"meanRecall"`         // mean(uniqueRegistrants / target) per searcher
		MeanFoundDup       float64 `json:"meanFoundDup"`       // mean(foundRegistrant / target) — duplicate-counting (kept for back-compat)
		MeanUniqueCount    float64 `json:"meanUniqueCount"`    // mean count of distinct registrant IDs each searcher saw
		HitTimeout         int     `json:"hitTimeout"`
	}
	byTopic := make(map[int][]searchResult)
	for _, r := range results {
		// Skip empty slots from legacy nodes — their goroutine returns
		// before writing a result, leaving zero-value entries that
		// would otherwise pollute topic 0.
		if r.NodeID == "" {
			continue
		}
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
	fmt.Printf("%-6s %12s %8s %12s %10s %10s %10s %10s\n",
		"topic", "searchers", "target", "fullRecall", "meanUniq", "meanRec", "meanDup", "timeout")
	for _, t := range topicIdxs {
		rs := byTopic[t]
		var (
			full, timeout                    int
			uniqRecallSum, dupRecallSum      float64
			uniqCountSum                     float64
			counted                          int
		)
		for _, r := range rs {
			if r.Target > 0 {
				uniqRecallSum += float64(r.UniqueRegistrant) / float64(r.Target)
				dupRecallSum += float64(r.FoundRegistrant) / float64(r.Target)
				uniqCountSum += float64(r.UniqueRegistrant)
				counted++
				if r.UniqueRegistrant >= r.Target {
					full++
				}
			}
			if r.HitTimeoutBefore {
				timeout++
			}
		}
		uniqMean, dupMean, uniqCountMean := 0.0, 0.0, 0.0
		if counted > 0 {
			uniqMean = uniqRecallSum / float64(counted)
			dupMean = dupRecallSum / float64(counted)
			uniqCountMean = uniqCountSum / float64(counted)
		}
		target := 0
		if rs[0].Target > 0 {
			target = rs[0].Target
		}
		fmt.Printf("%-6d %12d %8d %12s %10.4f %10.1f %10.4f %10d\n",
			t, len(rs), target,
			fmt.Sprintf("%d/%d", full, len(rs)),
			uniqMean, uniqCountMean, dupMean, timeout)
		reports = append(reports, topicReport{
			Topic: t, NumSearchers: len(rs), Target: target,
			FullRecall:      full,
			MeanRecall:      uniqMean,
			MeanFoundDup:    dupMean,
			MeanUniqueCount: uniqCountMean,
			HitTimeout:      timeout,
		})
	}

	// Per-registrant find-count distribution. For each topic, for each
	// registrant, count how many searchers' FoundRegistrantIDs included
	// that registrant. If the protocol finds registrants uniformly, the
	// distribution should be tight around some mean. Skew here indicates
	// some registrants are hard / impossible to find.
	fmt.Println()
	fmt.Println("=== per-registrant find-count distribution (how many searchers found each registrant) ===")
	fmt.Printf("%-6s %12s %10s %8s %8s %8s %8s %8s %8s %10s\n",
		"topic", "registrants", "neverFound", "min", "p5", "p25", "p50", "p75", "p95", "max")
	type findCountStats struct {
		Topic       int     `json:"topic"`
		Registrants int     `json:"registrants"`
		NeverFound  int     `json:"neverFound"`
		Min         int     `json:"min"`
		P5          int     `json:"p5"`
		P25         int     `json:"p25"`
		P50         int     `json:"p50"`
		P75         int     `json:"p75"`
		P95         int     `json:"p95"`
		Max         int     `json:"max"`
		Mean        float64 `json:"mean"`
		Counts      []int   `json:"counts"` // per-registrant find counts (for plotting)
	}
	var findStats []findCountStats
	for _, t := range topicIdxs {
		regSet := registrantsByTopic[t]
		counts := make(map[string]int, len(regSet))
		for id := range regSet {
			counts[id.TerminalString()] = 0
		}
		for _, r := range byTopic[t] {
			for _, regID := range r.FoundRegistrantIDs {
				if _, ok := counts[regID]; ok {
					counts[regID]++
				}
			}
		}
		values := make([]int, 0, len(counts))
		var sum, never int
		for _, c := range counts {
			values = append(values, c)
			sum += c
			if c == 0 {
				never++
			}
		}
		sort.Ints(values)
		pct := func(p int) int {
			if len(values) == 0 {
				return 0
			}
			i := (p * (len(values) - 1)) / 100
			return values[i]
		}
		mean := 0.0
		if len(values) > 0 {
			mean = float64(sum) / float64(len(values))
		}
		s := findCountStats{
			Topic:       t,
			Registrants: len(values),
			NeverFound:  never,
			Min:         pct(0),
			P5:          pct(5),
			P25:         pct(25),
			P50:         pct(50),
			P75:         pct(75),
			P95:         pct(95),
			Max:         pct(100),
			Mean:        mean,
			Counts:      values,
		}
		findStats = append(findStats, s)
		fmt.Printf("%-6d %12d %10d %8d %8d %8d %8d %8d %8d %10d   mean=%.1f\n",
			t, s.Registrants, s.NeverFound, s.Min, s.P5, s.P25, s.P50, s.P75, s.P95, s.Max, s.Mean)
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
			"findCountByTopic":     findStats,
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
