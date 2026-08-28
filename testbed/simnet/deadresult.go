package main

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
)

// deadResultTracker measures how often topic searches return registrants that
// were already dead (killed by churn) at the moment they were returned, and how
// long those nodes had been dead — i.e. the staleness of dead search results.
type deadResultTracker struct {
	killedAt sync.Map // enode.ID -> time.Time (kill wall-clock time)

	mu    sync.Mutex
	total int             // distinct (searcher, registrant) finds
	dead  int             // of those, ones already dead when found
	ages  []time.Duration // dead-age of each dead find (foundAt - killedAt)
}

func newDeadResultTracker() *deadResultTracker { return &deadResultTracker{} }

// markKilled records that a node was killed at time t.
func (d *deadResultTracker) markKilled(id enode.ID, t time.Time) {
	d.killedAt.Store(id, t)
}

// deadAge returns how long id had been dead at time 'at', and whether it was
// dead at all. Not dead (or killed after 'at') returns (0, false).
func (d *deadResultTracker) deadAge(id enode.ID, at time.Time) (time.Duration, bool) {
	v, ok := d.killedAt.Load(id)
	if !ok {
		return 0, false
	}
	kt := v.(time.Time)
	if at.Before(kt) {
		return 0, false
	}
	return at.Sub(kt), true
}

// merge folds one searcher's local counts into the shared tracker.
func (d *deadResultTracker) merge(total, dead int, ages []time.Duration) {
	d.mu.Lock()
	d.total += total
	d.dead += dead
	d.ages = append(d.ages, ages...)
	d.mu.Unlock()
}

// report prints the dead-result rate and the dead-age distribution.
func (d *deadResultTracker) report() {
	d.mu.Lock()
	total, dead := d.total, d.dead
	ages := append([]time.Duration(nil), d.ages...)
	d.mu.Unlock()

	pct := 0.0
	if total > 0 {
		pct = float64(dead) * 100 / float64(total)
	}
	fmt.Println()
	fmt.Println("=== dead results in searches ===")
	fmt.Printf("  registrant finds: %d   dead-when-returned: %d (%.2f%%)\n", total, dead, pct)
	if len(ages) == 0 {
		fmt.Println("  no dead results returned")
		return
	}
	sort.Slice(ages, func(i, j int) bool { return ages[i] < ages[j] })
	var sum time.Duration
	for _, a := range ages {
		sum += a
	}
	pctl := func(p int) time.Duration {
		idx := p * len(ages) / 100
		if idx >= len(ages) {
			idx = len(ages) - 1
		}
		return ages[idx]
	}
	sec := func(dur time.Duration) string { return fmt.Sprintf("%.0fs", dur.Seconds()) }
	fmt.Printf("  dead-age (how long dead when returned): min=%s p50=%s p90=%s p99=%s max=%s mean=%s\n",
		sec(ages[0]), sec(pctl(50)), sec(pctl(90)), sec(pctl(99)), sec(ages[len(ages)-1]),
		sec(sum/time.Duration(len(ages))))
}

// recordFinds checks a batch of (registrant, foundAt) finds against the kill set
// and returns local counts for merging. Used per-searcher to avoid lock
// contention on the hot path.
type deadLocal struct {
	total int
	dead  int
	ages  []time.Duration
}

func (l *deadLocal) record(d *deadResultTracker, id enode.ID, at time.Time) {
	l.total++
	if age, ok := d.deadAge(id, at); ok {
		l.dead++
		l.ages = append(l.ages, age)
	}
}
