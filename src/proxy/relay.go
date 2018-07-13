/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"binlog"
	"config"
	"fmt"
	"strings"
	"sync"
	"time"

	"xbase"
	"xbase/stats"
	"xbase/sync2"

	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	relayInfoFile = "relay-log.info"
	parallelSame  = 1
	parallelAll   = 2
)

// BackupRelay tuple.
type BackupRelay struct {
	log            *xlog.Log
	conf           *config.BinlogConfig
	spanner        *Spanner
	relayRates     *stats.Rates
	relayTimings   *stats.Timings
	relayInfo      *binlog.Info
	stateWg        sync.WaitGroup
	backupWorkerWg sync.WaitGroup
	relayWorkerWg  sync.WaitGroup
	sqlworker      *binlog.SQLWorker
	eventQueue     chan *binlog.Event
	stop           sync2.AtomicBool
	stopRelay      sync2.AtomicBool
	relayBinlog    sync2.AtomicString
	relayGTID      sync2.AtomicInt64
	initGTID       sync2.AtomicInt64 // Init GTID.
	resetGTID      sync2.AtomicInt64 // Reset GTID.
	state          sync2.AtomicString
	limits         sync2.AtomicInt32
	paralles       sync2.AtomicInt32
	counts         sync2.AtomicInt64
	parallelType   sync2.AtomicInt32
}

// NewBackupRelay creates new BackupRelay tuple.
func NewBackupRelay(log *xlog.Log, conf *config.BinlogConfig, spanner *Spanner) *BackupRelay {
	return &BackupRelay{
		log:     log,
		conf:    conf,
		spanner: spanner,
	}
}

// Init used to init all the workers.
func (br *BackupRelay) Init() error {
	log := br.log
	conf := br.conf
	br.parallelType.Set(int32(conf.ParallelType))
	br.limits.Set(int32(conf.RelayWorkers))
	br.relayTimings = stats.NewTimings("Relay")
	br.relayRates = stats.NewRates("RelayRates", br.relayTimings, 1, time.Second)

	// relay info.
	br.relayInfo = binlog.NewInfo(log, conf, relayInfoFile)
	if err := br.relayInfo.Init(); err != nil {
		return err
	}

	// Init the SQLWorker.
	br.initSQLWorker()
	// Init the backup workers.
	br.initBackupWorkers()

	if conf.EnableRelay {
		// Start the backup relay worker.
		br.relayWorkerWg.Add(1)
		go func(br *BackupRelay) {
			defer br.relayWorkerWg.Done()
			br.relayToEventQueue()
		}(br)
	}
	return nil
}

func (br *BackupRelay) initSQLWorker() {
	log := br.log
	spanner := br.spanner
	binlog := spanner.binlog
	relayInfo := br.relayInfo

	ts, err := relayInfo.ReadTs()
	if err != nil {
		log.Panic("backup.relay.read.ts.error:%v", err)
	}
	br.initGTID.Set(ts)
	br.resetGTID.Set(ts)

	// Get the ts from the relay.info file.
	sqlworker, err := binlog.NewSQLWorker(ts)
	if err != nil {
		log.Panic("backup.relay.to.backup.new.sqlworker.error:%v", err)
	}
	br.sqlworker = sqlworker
	log.Info("backup.relay[binlog:%v, pos:%v].sqlworker.init.from[%v]", sqlworker.RelayName(), sqlworker.RelayPosition(), ts)
}

func (br *BackupRelay) closeSQLWorker() {
	if br.sqlworker != nil {
		binlog := br.spanner.binlog
		defer binlog.CloseSQLWorker(br.sqlworker)
	}
}

func (br *BackupRelay) initBackupWorkers() {
	log := br.log
	workers := br.conf.RelayWorkers
	br.eventQueue = make(chan *binlog.Event, workers*2)
	br.backupWorkerWg.Add(1)
	go func(br *BackupRelay) {
		defer br.backupWorkerWg.Done()
		br.backupWorker(0)
	}(br)
	log.Info("backup.relay.workers[nums:%d].start...", workers)
}

func (br *BackupRelay) waitForBackupWorkerDone() {
	log := br.log

	log.Info("backup.relay.wait.relay.workers.done...")
	i := 0
	for len(br.eventQueue) > 0 {
		log.Info("backup.relay.wait.for.relay.worker.done.live.events:[%d].wait.seconds:%d", len(br.eventQueue), i)
		time.Sleep(time.Second)
		i++
	}
	close(br.eventQueue)
	br.backupWorkerWg.Wait()
	log.Info("backup.relay.workers.all.done...")
}

// RelayBinlog returns the current relay binlog name.
func (br *BackupRelay) RelayBinlog() string {
	return br.relayBinlog.Get()
}

// RelayGTID returns the current relay GTID.
func (br *BackupRelay) RelayGTID() int64 {
	return br.relayGTID.Get()
}

// RelayRates returns the relay rates.
func (br *BackupRelay) RelayRates() string {
	return br.relayRates.String()
}

// RelayStatus returns the stop status.
func (br *BackupRelay) RelayStatus() bool {
	return !br.stopRelay.Get()
}

// RelayCounts returns the counts have relayed.
func (br *BackupRelay) RelayCounts() int64 {
	return br.counts.Get()
}

// MaxWorkers returns the max parallel worker numbers.
func (br *BackupRelay) MaxWorkers() int32 {
	return br.limits.Get()
}

// SetMaxWorkers used to set the limits number.
func (br *BackupRelay) SetMaxWorkers(n int32) {
	if n > 0 {
		br.limits.Set(n)
	}
}

// ParallelWorkers returns the number of the parallel workers.
func (br *BackupRelay) ParallelWorkers() int32 {
	return br.paralles.Get()
}

// SetParallelType used to set the parallel type.
func (br *BackupRelay) SetParallelType(n int32) {
	br.parallelType.Set(n)
}

// ParallelType returns the type of parallel.
func (br *BackupRelay) ParallelType() int32 {
	return br.parallelType.Get()
}

// StopRelayWorker used to stop the relay worker.
func (br *BackupRelay) StopRelayWorker() {
	br.stopRelay.Set(true)
}

// StartRelayWorker used to restart the relay worker.
func (br *BackupRelay) StartRelayWorker() {
	br.stopRelay.Set(false)
}

// RestartGTID returns the restart GTID of next relay.
func (br *BackupRelay) RestartGTID() int64 {
	return br.resetGTID.Get()
}

// ResetRelayWorker used to reset the relay gtid.
// Then the relay worker should relay from the new gtid point.
func (br *BackupRelay) ResetRelayWorker(gtid int64) {
	br.log.Info("backup.relay.reset.relay.worker.from[%v].to[%v]", br.resetGTID.Get(), gtid)
	br.resetGTID.Set(gtid)
}

func (br *BackupRelay) checkRelayWorkerRestart() {
	if br.initGTID.Get() < br.resetGTID.Get() {
		log := br.log
		spanner := br.spanner
		binlog := spanner.binlog

		oldGTID := br.initGTID.Get()
		newGTID := br.resetGTID.Get()
		old := br.sqlworker
		new, err := binlog.NewSQLWorker(newGTID)
		if err != nil {
			log.Panic("backup.relay.restart.new.sqlworker.error:%v", err)
		}

		log.Info("backup.relay.restart.sqlworker.old[bin:%v, pos:%v].new[bin:%v, pos:%v].GTID.from[%v].to[%v]", old.RelayName(), old.RelayPosition(), new.RelayName(), new.RelayPosition(), oldGTID, newGTID)
		binlog.CloseSQLWorker(old)
		br.sqlworker = new
		br.initGTID.Set(newGTID)
	}
}

func (br *BackupRelay) relayToEventQueue() {
	log := br.log
	relayInfo := br.relayInfo
	spanner := br.spanner
	scatter := spanner.scatter

	for !br.stop.Get() {
		// Check relay stop.
		if br.stopRelay.Get() {
			log.Warning("backup.relay.is.stopped,please.check...")
			time.Sleep(time.Millisecond * time.Duration(br.conf.RelayWaitMs))
			continue
		}

		// Check backup ready.
		if !scatter.HasBackup() {
			log.Warning("backup.relay.but.we.don't.have.backup...")
			time.Sleep(time.Millisecond * time.Duration(br.conf.RelayWaitMs))
			continue
		}

		// Check to see the sqlworker need restarting after the GTID reset.
		br.checkRelayWorkerRestart()

		sqlworker := br.sqlworker
		event, err := sqlworker.NextEvent()
		if err != nil {
			log.Error("backup.relay.read.next.event[binlog:%v, pos:%v].error.degrade.to.readonly.err:%v", sqlworker.RelayName(), sqlworker.RelayPosition(), err)
			spanner.SetReadOnly(true)
			br.StopRelayWorker()
			time.Sleep(time.Second)
			continue
		}

		// We have dry run all the events.
		if event == nil {
			time.Sleep(time.Millisecond * 500)
			continue
		}

		// Sync the relay info to file.
		if err := relayInfo.Sync(event.LogName, int64(event.Timestamp)); err != nil {
			log.Panic("backup.sync.relay.info[%+v].error:%v", event, err)
		}

		// Write to queue.
		br.eventQueue <- event
		br.relayGTID.Set(int64(event.Timestamp))
		br.relayBinlog.Set(event.LogName)
	}
	log.Warning("backup.relay[binlog:%v, pos:%v].normal.exit", br.sqlworker.RelayName(), br.sqlworker.RelayPosition())
}

func (br *BackupRelay) backupExecuteDDL(event *binlog.Event) {
	log := br.log
	spanner := br.spanner

	br.counts.Add(1)
	if _, err := spanner.handleBackupDDL(event.Schema, event.Query); err != nil {
		log.Error("backup.relay.worker.execute.the.event[%+v].error.degrade.to.readonly.err:%v", event, err)
		spanner.SetReadOnly(true)
		br.StopRelayWorker()
	}
}

func (br *BackupRelay) backupExecuteDML(event *binlog.Event) {
	log := br.log
	spanner := br.spanner

	br.counts.Add(1)
	t0 := time.Now()
	if _, err := spanner.handleBackupWrite(event.Schema, event.Query); err != nil {
		log.Error("backup.relay.worker.execute.the.event[%+v].error.degrade.to.readonly.err:%v", event, err)
		spanner.SetReadOnly(true)
		br.StopRelayWorker()
	}
	br.relayTimings.Add(fmt.Sprintf("relay.%s.rates", strings.ToLower(event.Type)), time.Since(t0))
}

func (br *BackupRelay) backupWorker(n int) {
	log := br.log

	ddlWorker := func(br *BackupRelay, event *binlog.Event) {
		defer br.stateWg.Done()
		defer br.paralles.Add(-1)
		br.backupExecuteDDL(event)
		br.state.Set(event.Type)
	}

	dmlWorker := func(br *BackupRelay, event *binlog.Event) {
		defer br.stateWg.Done()
		defer br.paralles.Add(-1)
		br.backupExecuteDML(event)
		br.state.Set(event.Type)
	}

	for event := range br.eventQueue {
		switch event.Type {
		case xbase.DDL:
			br.stateWg.Wait()
			br.paralles.Add(1)
			br.stateWg.Add(1)
			ddlWorker(br, event)
		case xbase.INSERT, xbase.DELETE, xbase.UPDATE, xbase.REPLACE:
			switch br.parallelType.Get() {
			case int32(parallelSame):
				if br.state.Get() != event.Type {
					// Wait the prev State done.
					br.stateWg.Wait()
				}

				for {
					// Check the parallel worker number.
					if br.paralles.Get() < br.limits.Get() {
						br.paralles.Add(1)
						br.stateWg.Add(1)
						go dmlWorker(br, event)
						break
					}
					time.Sleep(50 * time.Nanosecond)
				}
			case int32(parallelAll):
				for {
					// Check the parallel worker number.
					if br.paralles.Get() < br.limits.Get() {
						br.paralles.Add(1)
						br.stateWg.Add(1)
						go dmlWorker(br, event)
						break
					}
					time.Sleep(50 * time.Nanosecond)
				}
			default:
				br.stateWg.Wait()
				br.paralles.Add(1)
				br.stateWg.Add(1)
				dmlWorker(br, event)
			}
		default:
			log.Error("backup.worker.relay.unsupport.event[%+v]", event)
			br.spanner.SetReadOnly(true)
			br.StopRelayWorker()
		}
	}
	br.stateWg.Wait()
}

// Close used to close all the backgroud workers.
func (br *BackupRelay) Close() {
	// Wait for relay worker done.
	br.stopRelay.Set(true)
	br.stop.Set(true)
	br.relayWorkerWg.Wait()
	br.closeSQLWorker()
	br.relayInfo.Close()

	// Wait for backup workers done.
	br.waitForBackupWorkerDone()
	br.relayRates.Close()
}
