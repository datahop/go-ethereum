package main

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
)

// runDiscNGValidationWorkload exercises the incremental-deployment design
// from issue #6: in a mixed population, flagged nodes register and search a
// topic while legacy nodes (no DISC-NG ENR flag) stay passive. The expected
// outcome is that filterDiscNG keeps REGTOPIC/TOPICQUERY off the legacy
// peers — so legacy hosts must end up with zero entries in their topic
// tables for the test topic, and flagged hosts should see the full
// flagged-registrant set.
func runDiscNGValidationWorkload(all []nodeRec, registerWait, searchTimeout time.Duration, metricsOut string) {
	var flagged, legacy []nodeRec
	flaggedIDs := make(map[enode.ID]struct{})
	for _, n := range all {
		if n.legacy {
			legacy = append(legacy, n)
		} else {
			flagged = append(flagged, n)
			flaggedIDs[n.ln.ID()] = struct{}{}
		}
	}
	fmt.Printf("DISC-NG validation: %d flagged (register+search), %d legacy (passive), topic=%x\n",
		len(flagged), len(legacy), testTopic[:])

	for _, n := range flagged {
		n.disc.RegisterTopic(testTopic, uint64(n.idx))
	}
	fmt.Printf("registrations started; register-wait=%s\n", registerWait)
	time.Sleep(registerWait)

	cov := snapshotRegistrationCoverage(all, flaggedIDs)
	printRegistrationCoverage(cov, len(flagged), len(all))

	var flaggedSum, legacySum int
	for _, n := range flagged {
		flaggedSum += cov.ByHost[n.ln.ID().String()]
	}
	for _, n := range legacy {
		legacySum += cov.ByHost[n.ln.ID().String()]
	}
	fmt.Println()
	fmt.Println("=== DISC-NG filter validation (issue #6) ===")
	fmt.Printf("  flagged hosts (%d): %d flagged-registrant entries summed across hosts\n", len(flagged), flaggedSum)
	fmt.Printf("  legacy hosts  (%d): %d flagged-registrant entries summed across hosts (expected 0)\n", len(legacy), legacySum)
	if legacySum == 0 && flaggedSum > 0 {
		fmt.Println("  RESULT: PASS — no REGTOPIC leaked to legacy peers")
	} else {
		fmt.Println("  RESULT: FAIL — filterDiscNG did not isolate the populations")
	}

	results := runSearches(flagged, flaggedIDs, len(flagged)-1, searchTimeout)
	report(results, len(flagged)-1, metricsOut, cov)
}
