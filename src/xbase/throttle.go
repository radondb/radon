/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package xbase

import (
	"sync"
	"time"

	"xbase/sync2"

	"github.com/beefsack/go-rate"
)

// Throttle tuple.
type Throttle struct {
	limit sync2.AtomicInt32
	rate  *rate.RateLimiter
	mu    sync.Mutex
}

// NewThrottle creates the new throttle.
func NewThrottle(l int) *Throttle {
	return &Throttle{
		limit: sync2.NewAtomicInt32(int32(l)),
		rate:  rate.New(l, time.Second),
	}
}

// Acquire used to acquire the lock of throttle.
func (throttle *Throttle) Acquire() {
	if throttle.limit.Get() <= 0 {
		return
	}

	throttle.mu.Lock()
	defer throttle.mu.Unlock()
	throttle.rate.Wait()
}

// Release used to do nothing.
func (throttle *Throttle) Release() {
}

// Set used to set the quota for the throttle.
func (throttle *Throttle) Set(l int) {
	throttle.mu.Lock()
	defer throttle.mu.Unlock()

	throttle.limit.Set(int32(l))
	throttle.rate = rate.New(l, time.Second)
}

// Limits returns the limits of the throttle.
func (throttle *Throttle) Limits() int {
	throttle.mu.Lock()
	defer throttle.mu.Unlock()
	return int(throttle.limit.Get())
}
