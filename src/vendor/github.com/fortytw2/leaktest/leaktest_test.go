package leaktest

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type testReporter struct {
	failed bool
	msg    string
}

func (tr *testReporter) Errorf(format string, args ...interface{}) {
	tr.failed = true
	tr.msg = fmt.Sprintf(format, args)
}

var leakyFuncs = []func(){
	// Infinite for loop
	func() {
		for {
			time.Sleep(time.Second)
		}
	},
	// Select on a channel not referenced by other goroutines.
	func() {
		c := make(chan struct{}, 0)
		select {
		case <-c:
		}
	},
	// Blocked select on channels not referenced by other goroutines.
	func() {
		c := make(chan struct{}, 0)
		c2 := make(chan struct{}, 0)
		select {
		case <-c:
		case c2 <- struct{}{}:
		}
	},
	// Blocking wait on sync.Mutex that isn't referenced by other goroutines.
	func() {
		var mu sync.Mutex
		mu.Lock()
		mu.Lock()
	},
	// Blocking wait on sync.RWMutex that isn't referenced by other goroutines.
	func() {
		var mu sync.RWMutex
		mu.RLock()
		mu.Lock()
	},
	func() {
		var mu sync.Mutex
		mu.Lock()
		c := sync.NewCond(&mu)
		c.Wait()
	},
}

func TestCheck(t *testing.T) {

	// this works because the running goroutine is left running at the
	// start of the next test case - so the previous leaks don't affect the
	// check for the next one
	for i, fn := range leakyFuncs {
		checker := &testReporter{}
		snapshot := Check(checker)
		go fn()

		snapshot()
		if !checker.failed {
			t.Errorf("didn't catch sleeping goroutine, test #%d", i)
		}
	}
}

func TestEmptyLeak(t *testing.T) {
	defer Check(t)()
	time.Sleep(time.Second)
}
