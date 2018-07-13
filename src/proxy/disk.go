/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"sync"
	"time"

	"xbase"
	"xbase/sync2"

	"github.com/xelabs/go-mysqlstack/xlog"
)

// DiskCheck tuple.
type DiskCheck struct {
	log       *xlog.Log
	dir       string
	done      chan bool
	ticker    *time.Ticker
	wg        sync.WaitGroup
	highwater sync2.AtomicBool
}

// NewDiskCheck creates the DiskCheck tuple.
func NewDiskCheck(log *xlog.Log, dir string) *DiskCheck {
	return &DiskCheck{
		log:    log,
		dir:    dir,
		done:   make(chan bool),
		ticker: time.NewTicker(time.Duration(time.Second * 5)), // 5 seconds.
	}
}

// HighWater returns the highwater mark.
// If true there is no spance left on device.
func (dc *DiskCheck) HighWater() bool {
	return dc.highwater.Get()
}

// Init used to init disk check goroutine.
func (dc *DiskCheck) Init() error {
	log := dc.log

	dc.wg.Add(1)
	go func(dc *DiskCheck) {
		defer dc.wg.Done()
		dc.check()
	}(dc)
	log.Info("disk.check.init.done")
	return nil
}

// Close used to close the disk check goroutine.
func (dc *DiskCheck) Close() {
	close(dc.done)
	dc.wg.Wait()
}

func (dc *DiskCheck) check() {
	defer dc.ticker.Stop()
	for {
		select {
		case <-dc.ticker.C:
			dc.doCheck()
		case <-dc.done:
			return
		}
	}
}

func (dc *DiskCheck) doCheck() {
	log := dc.log
	ds, err := xbase.DiskUsage(dc.dir)
	if err != nil {
		log.Error("disk.check[%v].error:%v", dc.dir, err)
		return
	}
	used := float64(ds.Used) / float64(ds.All)
	switch {
	case used >= 0.90:
		log.Warning("disk.check.got.high.water:%+v, used.perc[%.2f].more.than.95percent!!!", ds, used)
		dc.highwater.Set(true)
	case used >= 0.80:
		log.Warning("disk.check.got.water.mark:%+v, used.perc[%.2f].more.than.80percent", ds, used)
		dc.highwater.Set(false)
	default:
		dc.highwater.Set(false)
	}
}
