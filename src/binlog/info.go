/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package binlog

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"

	"config"
	"xbase/sync2"

	"github.com/xelabs/go-mysqlstack/xlog"
)

type info struct {
	Binlog    string `json:"binlog"`
	Timestamp int64  `json:"gtid"`
}

// Info tuple.
type Info struct {
	log      *xlog.Log
	file     *os.File
	binDir   string
	infoFile string
	currTs   sync2.AtomicInt64
	currBin  sync2.AtomicString
}

// NewInfo returns info tuple.
func NewInfo(log *xlog.Log, conf *config.BinlogConfig, fileName string) *Info {
	return &Info{
		log:      log,
		binDir:   conf.LogDir,
		infoFile: filepath.Join(conf.LogDir, fileName),
	}
}

// Init used to init the relay.
func (inf *Info) Init() error {
	f, err := os.OpenFile(inf.infoFile, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	inf.file = f
	return nil
}

// Sync used to sync the ts to the relay file.
func (inf *Info) Sync(binlog string, ts int64) error {
	info := &info{Binlog: binlog, Timestamp: ts}
	jsons, err := info.MarshalJSON()
	if err != nil {
		return err
	}
	inf.file.Truncate(0)
	_, err = inf.file.WriteAt(jsons, 0)
	inf.currTs.Set(ts)
	inf.currBin.Set(binlog)
	return err
}

// ReadTs used to get the ts from the relay file.
func (inf *Info) ReadTs() (int64, error) {
	info := &info{}
	buf, err := ioutil.ReadFile(inf.infoFile)
	if err != nil {
		return 0, err
	}

	if len(buf) > 0 {
		err = json.Unmarshal(buf, info)
		if err != nil {
			return 0, err
		}
	}
	return info.Timestamp, nil
}

// Close used to close the file of relay.
func (inf *Info) Close() {
	log := inf.log
	inf.file.Sync()
	inf.file.Close()
	log.Info("info.close.last[binlog:%v, ts:%v]", inf.currBin.Get(), inf.currTs.Get())
}
