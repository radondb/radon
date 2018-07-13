/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package binlog

import (
	"os"
	"sync"
	"time"

	"config"
	"xbase"

	"github.com/xelabs/go-mysqlstack/common"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	binlogInfoFile = "bin-log.info"
)

// IOWorker tuple.
type IOWorker struct {
	log     *xlog.Log
	info    *Info
	rfile   xbase.RotateFile
	binDir  string
	maxSize int
	queue   chan *Event
	wg      sync.WaitGroup
}

// NewIOWorker creates the new IOWorker.
func NewIOWorker(log *xlog.Log, conf *config.BinlogConfig) *IOWorker {
	return &IOWorker{
		log:     log,
		binDir:  conf.LogDir,
		maxSize: conf.MaxSize,
		queue:   make(chan *Event, 1),
		info:    NewInfo(log, conf, binlogInfoFile),
		rfile:   xbase.NewRotateFile(conf.LogDir, prefix, extension, conf.MaxSize),
	}
}

// GTID returns the last event Timestamp.
func (io *IOWorker) GTID() int64 {
	log := io.log
	ts, err := io.info.ReadTs()
	if err != nil {
		log.Error("ioworker.bin.log.info.read.ts.error:%v", err)
		return 0
	}
	return ts
}

// Init used to create the log dir.
func (io *IOWorker) Init() error {
	log := io.log
	log.Info("binlog.ioworker.init.bindir[%v]", io.binDir)
	if err := os.MkdirAll(io.binDir, 0744); err != nil {
		return err
	}
	if err := io.info.Init(); err != nil {
		return err
	}

	io.wg.Add(1)
	go func(io *IOWorker) {
		defer io.wg.Done()
		io.eventConsumer()
	}(io)
	log.Info("binlog.ioworker.init.done")
	return nil
}

func (io *IOWorker) eventConsumer() {
	for e := range io.queue {
		io.writeEvent(e)
	}
}

func (io *IOWorker) writeEvent(e *Event) {
	log := io.log
	datas := packEventv1(e)

	buf := common.NewBuffer(256)
	buf.WriteU32(uint32(len(datas)))
	buf.WriteBytes(datas)
	if _, err := io.rfile.Write(buf.Datas()); err != nil {
		log.Panic("binlog.ioworker.write.event[query:%v].error:%v", e.Query, err)
	}
	io.info.Sync(io.rfile.Name(), int64(e.Timestamp))
}

// LogEvent used to write the query to binary log.
func (io *IOWorker) LogEvent(typ string, schema string, query string) {
	io.queue <- &Event{
		Type:      typ,
		Schema:    schema,
		Query:     query,
		Timestamp: uint64(time.Now().UTC().UnixNano()),
	}
}

// Close used to close the io worker.
func (io *IOWorker) Close() {
	io.log.Info("ioworker.prepare.to.close")
	close(io.queue)
	io.wg.Wait()
	io.rfile.Sync()
	io.rfile.Close()
	io.info.Close()
	io.log.Info("ioworker.closed")
}
