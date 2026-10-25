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

package topicindex

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
)

// An asked node expires an ad lifetime after its first ask, however often it
// is asked again as a spare meanwhile.
func TestAskedSetKeepsFirstExpiry(t *testing.T) {
	clock := new(mclock.Simulated)
	a := NewAskedSet(Config{AdLifetime: time.Minute, Clock: clock})
	id := newNode().ID()
	a.Add(id)
	clock.Run(40 * time.Second)
	a.Add(id) // re-asked as a spare
	if !a.Has(id) {
		t.Fatal("asked node not in the set")
	}
	if a.LastAsked(id) != mclock.AbsTime(40*time.Second) {
		t.Fatal("the re-ask did not move the last-asked time")
	}
	clock.Run(21 * time.Second)
	if a.Has(id) {
		t.Fatal("re-asking a spare extended its expiry")
	}
}
