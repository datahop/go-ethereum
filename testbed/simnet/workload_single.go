package main

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
)

func runSingleTopicWorkload(all []nodeRec, registerWait, searchTimeout time.Duration, registerFrac float64, metricsOut string) {
	_ = registerFrac
	registrants := all
	searchers := all

	registrantIDs := make(map[enode.ID]struct{}, len(registrants))
	for _, n := range registrants {
		registrantIDs[n.ln.ID()] = struct{}{}
	}

	fmt.Printf("workload: %d nodes (each registers AND searches), single topic=%x\n",
		len(all), testTopic[:])

	for _, n := range registrants {
		n.disc.RegisterTopic(testTopic, uint64(n.idx))
	}
	fmt.Printf("registrations started; register-wait=%s\n", registerWait)
	time.Sleep(registerWait)

	regCoverage := snapshotRegistrationCoverage(all, registrantIDs)
	printRegistrationCoverage(regCoverage, len(registrants), len(all))

	results := runSearches(searchers, registrantIDs, len(registrants)-1, searchTimeout)
	report(results, len(registrants), metricsOut, regCoverage)
}
