/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package xbase

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestThrottleNoLimits(t *testing.T) {
	throttle := NewThrottle(100)
	for i := 0; i < 10; i++ {
		go func() {
			throttle.Acquire()
			time.Sleep(1000)
			defer throttle.Release()
		}()
	}
	time.Sleep(time.Second * 2)

	for i := 0; i < 10; i++ {
		go func() {
			throttle.Acquire()
			time.Sleep(1000)
			defer throttle.Release()
		}()
	}
	throttle.Set(0)
	time.Sleep(time.Second * 2)
	assert.True(t, throttle.limit.Get() == 0)
}
