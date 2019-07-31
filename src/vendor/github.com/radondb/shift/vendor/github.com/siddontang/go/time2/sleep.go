package time2

import (
	"time"
)

type Timer struct {
	C <-chan time.Time
	r *timer
}

func After(d time.Duration) <-chan time.Time {
	return defaultWheel.After(d)
}

func Sleep(d time.Duration) {
	defaultWheel.Sleep(d)
}

func AfterFunc(d time.Duration, f func()) *Timer {
	return defaultWheel.AfterFunc(d, f)
}

func NewTimer(d time.Duration) *Timer {
	return defaultWheel.NewTimer(d)
}

func (t *Timer) Reset(d time.Duration) {
	t.r.w.resetTimer(t.r, d, 0)
}

func (t *Timer) Stop() {
	t.r.w.delTimer(t.r)
}
