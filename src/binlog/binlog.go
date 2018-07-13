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
	"path"
	"path/filepath"
	"sync"
	"time"

	"config"
	"xbase"
	"xbase/sync2"

	"github.com/xelabs/go-mysqlstack/xlog"
)

const (
	prefix    = "radon-"
	extension = ".binlog"
)

// Binlog tuple.
type Binlog struct {
	log         *xlog.Log
	mu          sync.RWMutex
	wg          sync.WaitGroup
	id          sync2.AtomicInt64
	done        chan bool
	conf        *config.BinlogConfig
	rfile       xbase.RotateFile
	binDir      string
	ioworker    *IOWorker
	sqlworkers  map[int64]*SQLWorker
	purgeTicker *time.Ticker
}

// NewBinlog creates the new binlog tuple.
func NewBinlog(log *xlog.Log, conf *config.BinlogConfig) *Binlog {
	return &Binlog{
		log:         log,
		conf:        conf,
		done:        make(chan bool),
		binDir:      conf.LogDir,
		ioworker:    NewIOWorker(log, conf),
		sqlworkers:  make(map[int64]*SQLWorker, 64),
		purgeTicker: time.NewTicker(time.Duration(time.Second * 300)), // 5 minutes.
		rfile:       xbase.NewRotateFile(conf.LogDir, prefix, extension, conf.MaxSize),
	}
}

// Init used to init the ioworker.
func (bin *Binlog) Init() error {
	log := bin.log

	log.Info("binlog.init.conf:%+v", bin.conf)
	defer log.Info("binlog.init.done")

	// Purge worker.
	bin.wg.Add(1)
	go func(bin *Binlog) {
		defer bin.wg.Done()
		bin.purge()
	}(bin)

	// IO Worker.
	return bin.ioworker.Init()
}

func (bin *Binlog) addSQLWork(sqlworker *SQLWorker) {
	bin.mu.Lock()
	defer bin.mu.Unlock()
	bin.id.Add(1)
	id := bin.id.Get()
	sqlworker.setID(id)
	bin.sqlworkers[id] = sqlworker
}

func (bin *Binlog) removeSQLWork(sqlworker *SQLWorker) {
	bin.mu.Lock()
	defer bin.mu.Unlock()
	delete(bin.sqlworkers, sqlworker.id)
}

// NewSQLWorker creates the new sql worker.
func (bin *Binlog) NewSQLWorker(ts int64) (*SQLWorker, error) {
	sqlworker := NewSQLWorker(bin.log, bin.conf, ts)
	if err := sqlworker.Init(); err != nil {
		return nil, err
	}
	bin.addSQLWork(sqlworker)
	return sqlworker, nil
}

// CloseSQLWorker used to close the sqlworker.
func (bin *Binlog) CloseSQLWorker(sqlworker *SQLWorker) {
	bin.removeSQLWork(sqlworker)
	sqlworker.close()
}

// LogEvent used to write the event to the bin.
func (bin *Binlog) LogEvent(typ string, schema string, sql string) {
	bin.ioworker.LogEvent(typ, schema, sql)
}

// Close used to close the bin.
func (bin *Binlog) Close() {
	close(bin.done)
	bin.wg.Wait()
	bin.ioworker.Close()
}

func (bin *Binlog) purge() {
	defer bin.purgeTicker.Stop()
	for {
		select {
		case <-bin.purgeTicker.C:
			bin.doPurge()
		case <-bin.done:
			return
		}
	}
}

func (bin *Binlog) doPurge() {
	minName := ""
	bin.mu.RLock()
	for _, sqlworker := range bin.sqlworkers {
		if minName == "" {
			minName = sqlworker.RelayName()
		} else {
			if sqlworker.RelayName() < minName {
				minName = sqlworker.RelayName()
			}
		}
	}
	bin.mu.RUnlock()

	if minName != "" {
		bin.purgebinTo(minName)
	}
}

func (bin *Binlog) purgebinTo(name string) {
	log := bin.log

	// name is empty
	name = path.Base(name)
	if name == "." {
		return
	}

	oldLogs, err := bin.rfile.GetOldLogInfos()
	if err != nil {
		log.Error("bin.purge.bin.to[%s].get.old.loginfos.error:%v", name, err)
		return
	}
	for _, old := range oldLogs {
		if old.Name < name {
			os.Remove(filepath.Join(bin.binDir, old.Name))
		}
	}
}

// LastGTID returns the last event GTID.
func (bin *Binlog) LastGTID() int64 {
	return bin.ioworker.GTID()
}

// RelayInfo represents the relay sqlworker status.
type RelayInfo struct {
	ID            int64
	StartGTID     int64
	RelayGTID     int64
	LastWriteGTID int64
	Relaybin      string
	RelayPosition int64
	SecondBehinds int64
}

// RelayInfos returns all the sqlworker status.
func (bin *Binlog) RelayInfos() []RelayInfo {
	bin.mu.RLock()
	defer bin.mu.RUnlock()
	lastGTID := bin.ioworker.GTID()
	relayInfos := make([]RelayInfo, 0, 8)
	for id, sqlworker := range bin.sqlworkers {
		relayInfo := RelayInfo{
			ID:            id,
			StartGTID:     sqlworker.SeekGTID(),
			RelayGTID:     sqlworker.RelayGTID(),
			LastWriteGTID: lastGTID,
			Relaybin:      sqlworker.RelayName(),
			RelayPosition: sqlworker.RelayPosition(),
			SecondBehinds: (lastGTID - sqlworker.RelayGTID()) / int64(time.Second),
		}
		relayInfos = append(relayInfos, relayInfo)
	}
	return relayInfos
}
