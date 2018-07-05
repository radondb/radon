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
	"testing"
	"time"

	"config"
	"fakedb"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestBinlog(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_binlog_", log)
	defer leaktest.Check(t)()
	defer os.RemoveAll(tmpDir)

	conf := &config.BinlogConfig{
		MaxSize: 102400,
		LogDir:  tmpDir,
	}

	binlog := NewBinlog(log, conf)
	err := binlog.Init()
	assert.Nil(t, err)
	defer binlog.Close()

	ts := time.Now().UnixNano()
	n := 10
	schema := "radon"
	for i := 0; i < n; i++ {
		query := "select a,b,cd from table1 where a=b and c=d and e=d group by id order\n by desc"
		binlog.LogEvent("SELECT", schema, query)
	}
	time.Sleep(time.Second)

	sqlworker, err := binlog.NewSQLWorker(ts)
	assert.Nil(t, err)
	defer binlog.CloseSQLWorker(sqlworker)

	relayInfos := binlog.RelayInfos()
	assert.True(t, len(relayInfos) > 0)
}

func TestBinlogPurge(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_binlog_", log)
	defer os.RemoveAll(tmpDir)
	defer leaktest.Check(t)()

	conf := &config.BinlogConfig{
		MaxSize: 102400,
		LogDir:  tmpDir,
	}

	binlog := NewBinlog(log, conf)
	err := binlog.Init()
	assert.Nil(t, err)
	defer binlog.Close()

	n := 10000
	schema := "radon"
	for i := 0; i < n; i++ {
		query := "select a,b,cd from table1 where a=b and c=d and e=d group by id order\n by desc"
		binlog.LogEvent("SELECT", schema, query)
	}
	time.Sleep(time.Second)

	logs1, _ := binlog.rfile.GetOldLogInfos()
	file0 := logs1[len(logs1)/2-1]
	file1 := logs1[len(logs1)/2]
	file2 := logs1[len(logs1)/2+1]

	// sqlwoker1.
	{
		sqlworker1, err := binlog.NewSQLWorker(file1.Ts)
		assert.Nil(t, err)
		defer binlog.CloseSQLWorker(sqlworker1)
		assert.Equal(t, file1.Name, sqlworker1.RelayName())
	}

	// sqlwoker2.
	{
		sqlworker2, err := binlog.NewSQLWorker(file2.Ts)
		assert.Nil(t, err)
		defer binlog.CloseSQLWorker(sqlworker2)
		assert.Equal(t, file2.Name, sqlworker2.RelayName())
	}

	{
		// Purge.
		binlog.doPurge()

		// Check old binlogs.
		logs1, _ = binlog.rfile.GetOldLogInfos()
		for _, logInfo := range logs1 {
			assert.True(t, logInfo.Name > file0.Name)
		}
		binlog.LastGTID()
	}
}
