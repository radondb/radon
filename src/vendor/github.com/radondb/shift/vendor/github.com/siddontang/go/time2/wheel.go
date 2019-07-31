package time2

import (
	"sync"
	"time"
)

const (
	tvn_bits uint64 = 6
	tvr_bits uint64 = 8
	tvn_size uint64 = 64  //1 << tvn_bits
	tvr_size uint64 = 256 //1 << tvr_bits

	tvn_mask uint64 = 63  //tvn_size - 1
	tvr_mask uint64 = 255 //tvr_size -1
)

const (
	defaultTimerSize = 128
)

type timer struct {
	expires uint64
	period  uint64

	f   func(time.Time, interface{})
	arg interface{}

	w *Wheel

	vec   []*timer
	index int
}

type Wheel struct {
	sync.Mutex

	jiffies uint64

	tv1 [][]*timer
	tv2 [][]*timer
	tv3 [][]*timer
	tv4 [][]*timer
	tv5 [][]*timer

	tick time.Duration

	quit chan struct{}
}

//tick is the time for a jiffies
func NewWheel(tick time.Duration) *Wheel {
	w := new(Wheel)

	w.quit = make(chan struct{})

	f := func(size int) [][]*timer {
		tv := make([][]*timer, size)
		for i := range tv {
			tv[i] = make([]*timer, 0, defaultTimerSize)
		}

		return tv
	}

	w.tv1 = f(int(tvr_size))
	w.tv2 = f(int(tvn_size))
	w.tv3 = f(int(tvn_size))
	w.tv4 = f(int(tvn_size))
	w.tv5 = f(int(tvn_size))

	w.jiffies = 0
	w.tick = tick

	go w.run()
	return w
}

func (w *Wheel) addTimerInternal(t *timer) {
	expires := t.expires
	idx := t.expires - w.jiffies

	var tv [][]*timer
	var i uint64

	if idx < tvr_size {
		i = expires & tvr_mask
		tv = w.tv1
	} else if idx < (1 << (tvr_bits + tvn_bits)) {
		i = (expires >> tvr_bits) & tvn_mask
		tv = w.tv2
	} else if idx < (1 << (tvr_bits + 2*tvn_bits)) {
		i = (expires >> (tvr_bits + tvn_bits)) & tvn_mask
		tv = w.tv3
	} else if idx < (1 << (tvr_bits + 3*tvn_bits)) {
		i = (expires >> (tvr_bits + 2*tvn_bits)) & tvn_mask
		tv = w.tv4
	} else if int64(idx) < 0 {
		i = w.jiffies & tvr_mask
		tv = w.tv1
	} else {
		if idx > 0x00000000ffffffff {
			idx = 0x00000000ffffffff

			expires = idx + w.jiffies
		}

		i = (expires >> (tvr_bits + 3*tvn_bits)) & tvn_mask
		tv = w.tv5
	}

	tv[i] = append(tv[i], t)

	t.vec = tv[i]
	t.index = len(tv[i]) - 1
}

func (w *Wheel) cascade(tv [][]*timer, index int) int {
	vec := tv[index]
	tv[index] = vec[0:0:defaultTimerSize]

	for _, t := range vec {
		w.addTimerInternal(t)
	}

	return index
}

func (w *Wheel) getIndex(n int) int {
	return int((w.jiffies >> (tvr_bits + uint64(n)*tvn_bits)) & tvn_mask)
}

func (w *Wheel) onTick() {
	w.Lock()

	index := int(w.jiffies & tvr_mask)

	if index == 0 && (w.cascade(w.tv2, w.getIndex(0))) == 0 &&
		(w.cascade(w.tv3, w.getIndex(1))) == 0 &&
		(w.cascade(w.tv4, w.getIndex(2))) == 0 &&
		(w.cascade(w.tv5, w.getIndex(3)) == 0) {

	}

	w.jiffies++

	vec := w.tv1[index]
	w.tv1[index] = vec[0:0:defaultTimerSize]

	w.Unlock()

	f := func(vec []*timer) {
		now := time.Now()
		for _, t := range vec {
			if t == nil {
				continue
			}

			t.f(now, t.arg)

			if t.period > 0 {
				t.expires = t.period + w.jiffies
				w.addTimer(t)
			}
		}
	}

	if len(vec) > 0 {
		go f(vec)
	}
}

func (w *Wheel) addTimer(t *timer) {
	w.Lock()
	w.addTimerInternal(t)
	w.Unlock()
}

func (w *Wheel) delTimer(t *timer) {
	w.Lock()
	vec := t.vec
	index := t.index

	if len(vec) > index && vec[index] == t {
		vec[index] = nil
	}

	w.Unlock()
}

func (w *Wheel) resetTimer(t *timer, when time.Duration, period time.Duration) {
	w.delTimer(t)

	t.expires = w.jiffies + uint64(when/w.tick)
	t.period = uint64(period / w.tick)

	w.addTimer(t)
}

func (w *Wheel) newTimer(when time.Duration, period time.Duration,
	f func(time.Time, interface{}), arg interface{}) *timer {
	t := new(timer)

	t.expires = w.jiffies + uint64(when/w.tick)
	t.period = uint64(period / w.tick)

	t.f = f
	t.arg = arg

	t.w = w

	return t
}

func (w *Wheel) run() {
	ticker := time.NewTicker(w.tick)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.onTick()
		case <-w.quit:
			return
		}
	}
}

func (w *Wheel) Stop() {
	close(w.quit)
}

func sendTime(t time.Time, arg interface{}) {
	select {
	case arg.(chan time.Time) <- t:
	default:
	}
}

func goFunc(t time.Time, arg interface{}) {
	go arg.(func())()
}

func dummyFunc(t time.Time, arg interface{}) {

}

func (w *Wheel) After(d time.Duration) <-chan time.Time {
	return w.NewTimer(d).C
}

func (w *Wheel) Sleep(d time.Duration) {
	<-w.NewTimer(d).C
}

func (w *Wheel) Tick(d time.Duration) <-chan time.Time {
	return w.NewTicker(d).C
}

func (w *Wheel) TickFunc(d time.Duration, f func()) *Ticker {
	t := &Ticker{
		r: w.newTimer(d, d, goFunc, f),
	}

	w.addTimer(t.r)

	return t

}

func (w *Wheel) AfterFunc(d time.Duration, f func()) *Timer {
	t := &Timer{
		r: w.newTimer(d, 0, goFunc, f),
	}

	w.addTimer(t.r)

	return t
}

func (w *Wheel) NewTimer(d time.Duration) *Timer {
	c := make(chan time.Time, 1)
	t := &Timer{
		C: c,
		r: w.newTimer(d, 0, sendTime, c),
	}

	w.addTimer(t.r)

	return t
}

func (w *Wheel) NewTicker(d time.Duration) *Ticker {
	c := make(chan time.Time, 1)
	t := &Ticker{
		C: c,
		r: w.newTimer(d, d, sendTime, c),
	}

	w.addTimer(t.r)

	return t
}

var defaultWheel *Wheel

func init() {
	defaultWheel = NewWheel(500 * time.Millisecond)
}
