// Copyright 2026 The go-ethereum Authors
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
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/p2p/discover/topicindex"
)

// Registrar-side wait-time sampling. Off by default; the testbed enables it to
// observe registration pressure (what registrars actually quote) at scale.

var (
	waitStatsOn bool
	waitStatsMu sync.Mutex
	waitStats   = make(map[topicindex.TopicID]*WaitTimeStats)
)

// EnableWaitStats turns on registrar-side wait-time sampling.
func EnableWaitStats() { waitStatsOn = true }

// WaitTimeStats holds quoted wait times for one topic, as seen by all local
// registrars. Quotes are the wait a registrar told a registrant to observe;
// Admitted counts requests admitted straight away (quote of zero).
type WaitTimeStats struct {
	Admitted int64 `json:"admitted"`
	Quoted   int64 `json:"quoted"`
	// QuotedMs is every wait a registrar quoted. A registrant retries until it
	// is admitted, so this holds several samples per registration and describes
	// the pricing signal rather than what any one registration ended up paying.
	QuotedMs []int64 `json:"quotedMs"`
	// AdmittedMs is the cumulative wait each *successful* registration had
	// accrued when it was finally admitted: one sample per registration, and
	// the figure to read for "how long did registering actually take".
	AdmittedMs []int64 `json:"admittedMs"`
	sampleCap  int
}

// recordWaitQuote samples the outcome of one registration attempt.
func recordWaitQuote(topic topicindex.TopicID, quote, cumulative time.Duration) {
	if !waitStatsOn {
		return
	}
	waitStatsMu.Lock()
	defer waitStatsMu.Unlock()
	st := waitStats[topic]
	if st == nil {
		st = &WaitTimeStats{sampleCap: 200000}
		waitStats[topic] = st
	}
	if quote <= 0 {
		st.Admitted++
		if cumulative > 0 && len(st.AdmittedMs) < st.sampleCap {
			st.AdmittedMs = append(st.AdmittedMs, cumulative.Milliseconds())
		}
		return
	}
	st.Quoted++
	// Keep the raw samples bounded: the distribution is the deliverable, and a
	// 10k-node run issues far more quotes than a plot needs.
	if len(st.QuotedMs) < st.sampleCap {
		st.QuotedMs = append(st.QuotedMs, quote.Milliseconds())
	}
}

// WaitTimeStatsSnapshot returns the per-topic quoted-wait samples collected so
// far, or nil when sampling is not enabled.
func WaitTimeStatsSnapshot() map[topicindex.TopicID]WaitTimeStats {
	if !waitStatsOn {
		return nil
	}
	waitStatsMu.Lock()
	defer waitStatsMu.Unlock()
	out := make(map[topicindex.TopicID]WaitTimeStats, len(waitStats))
	for k, v := range waitStats {
		c := WaitTimeStats{Admitted: v.Admitted, Quoted: v.Quoted}
		c.QuotedMs = append([]int64(nil), v.QuotedMs...)
		c.AdmittedMs = append([]int64(nil), v.AdmittedMs...)
		out[k] = c
	}
	return out
}
