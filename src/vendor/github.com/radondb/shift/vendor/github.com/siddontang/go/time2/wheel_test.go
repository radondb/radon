package time2

import (
	"testing"
	"time"
)

var testWheel = NewWheel(1 * time.Millisecond)

func TestTimer(t *testing.T) {
	t1 := testWheel.NewTimer(500 * time.Millisecond)

	before := time.Now()
	<-t1.C

	after := time.Now()

	println(after.Sub(before).String())
}

func TestTicker(t *testing.T) {
	wait := make(chan struct{}, 100)
	i := 0
	f := func() {
		println(time.Now().Unix())
		i++
		if i >= 10 {
			wait <- struct{}{}
		}
	}
	before := time.Now()

	t1 := testWheel.TickFunc(1000*time.Millisecond, f)

	<-wait

	t1.Stop()

	after := time.Now()

	println(after.Sub(before).String())
}
