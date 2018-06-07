/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package binlog

import (
	"config"
	"io"
	"os"
	"path"
	"time"
	"xbase"
	"xbase/sync2"

	"github.com/xelabs/go-mysqlstack/common"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// SQLWorker tuple.
type SQLWorker struct {
	log   *xlog.Log
	rfile xbase.RotateFile

	// gtid is timestamp,
	// (time.Now().UTC().UnixNano(), format as '1514254947594569594'
	binDir        string
	currFile      *os.File
	id            int64
	seekTimestamp int64
	stop          sync2.AtomicBool
	currPos       sync2.AtomicInt64
	currTimestamp sync2.AtomicInt64
	currBinName   sync2.AtomicString
}

// NewSQLWorker creates the new SQLWorker.
func NewSQLWorker(log *xlog.Log, conf *config.BinlogConfig, ts int64) *SQLWorker {
	return &SQLWorker{
		log:           log,
		binDir:        conf.LogDir,
		seekTimestamp: ts,
		rfile:         xbase.NewRotateFile(conf.LogDir, prefix, extension, conf.MaxSize),
	}
}

func (sql *SQLWorker) close() {
	sql.stop.Set(true)
	if sql.currFile != nil {
		sql.currFile.Close()
	}
	sql.rfile.Close()
	sql.currPos.Set(0)
	sql.currBinName.Set("")
	sql.log.Info("sqlworker.closed")
}

// RelayName returns the current binlog which are read.
func (sql *SQLWorker) RelayName() string {
	return sql.currBinName.Get()
}

// RelayPosition returns the current binlog position which are read.
func (sql *SQLWorker) RelayPosition() int64 {
	return sql.currPos.Get()
}

// RelayGTID returns the last event timestamp have read.
func (sql *SQLWorker) RelayGTID() int64 {
	return sql.currTimestamp.Get()
}

// SeekGTID returns the timestamp which we started.
func (sql *SQLWorker) SeekGTID() int64 {
	return sql.seekTimestamp
}

func (sql *SQLWorker) setID(id int64) {
	sql.id = id
}

func (sql *SQLWorker) readOneEvent() (*Event, error) {
	// No any binlog files in binlog dir, we return the EOF error.
	if sql.currBinName.Get() == "" {
		return nil, io.EOF
	}

	pos := sql.currPos.Get()
	// 1. Read the event length datas, 4bytes.
	lenDatas := make([]byte, 4)
	_, err := sql.currFile.ReadAt(lenDatas, sql.currPos.Get())
	if err != nil {
		return nil, err
	}

	buf := common.ReadBuffer(lenDatas)
	len, err := buf.ReadU32()
	if err != nil {
		return nil, err
	}

	// 2. Read the event datas.
	datas := make([]byte, len)
	_, err = sql.currFile.ReadAt(datas, sql.currPos.Get()+4)
	if err != nil {
		return nil, err
	}

	// 3. Unpack the event.
	event, err := unpackEvent(datas)
	if err != nil {
		return nil, err
	}

	// Set the position at last.
	sql.currPos.Add(4)
	sql.currPos.Add(int64(len))
	endLogPos := sql.currPos.Get()

	event.Pos = pos
	event.LogName = sql.currBinName.Get()
	event.EndLogPos = endLogPos
	return event, nil
}

func (sql *SQLWorker) seekToEvent(ts int64) error {
	prevPos := sql.currPos.Get()
	for !sql.stop.Get() {
		event, err := sql.readOneEvent()
		if err != nil {
			// We have got the end of the current binlog.
			if err == io.EOF {
				return nil
			}
			return err
		}
		// Find the first larger event, we should stop.
		if event.Timestamp > uint64(ts) {
			// Reset the position to the previous.
			sql.currPos.Set(prevPos)
			return nil
		}
		// Reset the position.
		prevPos = sql.currPos.Get()
	}
	return nil
}

// Init used to init the sql current position and seek to the right event.
func (sql *SQLWorker) Init() error {
	log := sql.log
	if sql.currBinName.Get() == "" {
		currLogInfo, err := sql.rfile.GetCurrLogInfo(sql.seekTimestamp)
		if err != nil {
			log.Error("binlog.sql.init.get.current[seekts:%v].loginfo.error:%v", sql.seekTimestamp, err)
			return err
		}

		if currLogInfo.Name != "" {
			log.Info("sqlworker.init.currlog[%v].seekts[%v, %v]", currLogInfo.Name, sql.seekTimestamp, time.Unix(0, sql.seekTimestamp))
			file, err := os.Open(path.Join(sql.binDir, currLogInfo.Name))
			if err != nil {
				return err
			}
			sql.currPos.Set(0)
			sql.currFile = file
			sql.currBinName.Set(currLogInfo.Name)
			return sql.seekToEvent(sql.seekTimestamp)
		}
	}
	return nil
}

func (sql *SQLWorker) checkNextFileExists() bool {
	log := sql.log
	logInfo, err := sql.rfile.GetNextLogInfo(sql.currBinName.Get())
	if err != nil {
		log.Error("binlog.sql.get.next.log.curr[%s].error:%v", sql.currBinName.Get(), err)
		return false
	}

	if logInfo.Name == "" {
		return false
	}

	if logInfo.Name != sql.currBinName.Get() {
		// Here, we make sure the next file is exists, but we check that whether a new write(stale) to the sql.currBinName binlog file.
		if sql.currBinName.Get() != "" {
			fileInfo, err := os.Lstat(path.Join(sql.binDir, sql.currBinName.Get()))
			if err != nil {
				log.Error("binlog.sql.check.next.file.stat[%s].error:%v", sql.currBinName.Get(), err)
				return false
			}
			size := fileInfo.Size()
			if sql.currPos.Get() < size {
				log.Warning("binlog.sql.found.stale.write.size[%v].currpos[%v]", size, sql.currPos.Get())
				return false
			}
		}

		// Rotate to the next binlog file.
		file, err := os.Open(path.Join(sql.binDir, logInfo.Name))
		if err != nil {
			log.Error("binlog.sql.check.next.file.error:%v", err)
			return false
		}
		sql.currFile.Close()
		sql.currFile = file
		sql.currPos.Set(0)
		sql.currBinName.Set(logInfo.Name)
		return true
	}
	// We don't have next the binlog file.
	return false
}

// NextEvent used to read the next event.
// If we get the end of the current binlog file and don't have next binlog, just returns (nil,nil).
func (sql *SQLWorker) NextEvent() (*Event, error) {
	event, err := sql.readOneEvent()
	if err != nil {
		if err == io.EOF {
			if !sql.checkNextFileExists() {
				return nil, nil
			}
			// Changed to the next binlog file, read next event from the new file.
			return sql.NextEvent()
		}
		return nil, err
	}
	sql.currTimestamp.Set(int64(event.Timestamp))
	return event, nil
}
