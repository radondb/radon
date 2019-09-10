/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package engine

import "sync"

// calcPool used to the merge join calc.
type calcPool struct {
	queue chan int
	wg    *sync.WaitGroup
}

func newCalcPool(size int) *calcPool {
	if size <= 0 {
		size = 1
	}
	return &calcPool{
		queue: make(chan int, size),
		wg:    &sync.WaitGroup{},
	}
}

func (p *calcPool) add(delta int) {
	for i := 0; i < delta; i++ {
		p.queue <- 1
	}
	for i := 0; i > delta; i-- {
		<-p.queue
	}
	p.wg.Add(delta)
}

func (p *calcPool) done() {
	<-p.queue
	p.wg.Done()
}

func (p *calcPool) wait() {
	p.wg.Wait()
}
