package main

import (
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
)

// hybridClock runs on simulated time until switchToReal is called, then keeps
// running in real time from the simulated time base:
//
//	Now() = simNow(switch) + (real.Now() - real.Now(switch))
//
// The clock is continuous and monotone across the switch, so absolute times
// stored by TopicTable before the switch (ad expiry, lower-bound timestamps,
// ticket issue times) keep their meaning: an ad admitted at simulated time t
// still expires at t+E, now reached in real time.
type hybridClock struct {
	sim      *mclock.Simulated
	real     mclock.Clock
	switched bool
	base     mclock.AbsTime // simulated time at the switch
	realBase mclock.AbsTime // real time at the switch
}

func newHybridClock(real mclock.Clock) *hybridClock {
	return &hybridClock{sim: new(mclock.Simulated), real: real}
}

// switchToReal freezes simulated time and continues in real time.
func (c *hybridClock) switchToReal() {
	if c.switched {
		return
	}
	c.base, c.realBase, c.switched = c.sim.Now(), c.real.Now(), true
}

func (c *hybridClock) Now() mclock.AbsTime {
	if !c.switched {
		return c.sim.Now()
	}
	return c.base + (c.real.Now() - c.realBase)
}

// Sleep advances simulated time before the switch and sleeps afterwards.
func (c *hybridClock) Sleep(d time.Duration) {
	if !c.switched {
		c.sim.Run(d)
		return
	}
	c.real.Sleep(d)
}

func (c *hybridClock) current() mclock.Clock {
	if c.switched {
		return c.real
	}
	return c.sim
}

func (c *hybridClock) NewTimer(d time.Duration) mclock.ChanTimer { return c.current().NewTimer(d) }
func (c *hybridClock) After(d time.Duration) <-chan mclock.AbsTime {
	return c.current().After(d)
}
func (c *hybridClock) AfterFunc(d time.Duration, f func()) mclock.Timer {
	return c.current().AfterFunc(d, f)
}
