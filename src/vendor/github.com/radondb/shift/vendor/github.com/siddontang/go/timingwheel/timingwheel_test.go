package timingwheel

import (
	"testing"
	"time"
)

func TestTimingWheel(t *testing.T) {
	w := NewTimingWheel(100*time.Millisecond, 10)

	for {
		select {
		case <-w.After(200 * time.Millisecond):
			return
		}
	}
}
