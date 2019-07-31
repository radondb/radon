package time2

import (
	"time"
)

type Ticker struct {
	C <-chan time.Time
	r *timer
}

func NewTicker(d time.Duration) *Ticker {
	return defaultWheel.NewTicker(d)
}

func TickFunc(d time.Duration, f func()) *Ticker {
	return defaultWheel.TickFunc(d, f)
}

func Tick(d time.Duration) <-chan time.Time {
	return defaultWheel.Tick(d)
}

func (t *Ticker) Stop() {
	t.r.w.delTimer(t.r)
}

func (t *Ticker) Reset(d time.Duration) {
	t.r.w.resetTimer(t.r, d, d)
}
