/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package binlog

import (
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"config"
	"fakedb"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestSQLWorker(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_binlog_", log)
	defer os.RemoveAll(tmpDir)
	defer leaktest.Check(t)()

	conf := &config.BinlogConfig{
		MaxSize: 102400,
		LogDir:  tmpDir,
	}

	ts := time.Now().UnixNano()
	ioworker := NewIOWorker(log, conf)
	err := ioworker.Init()
	assert.Nil(t, err)
	defer ioworker.Close()

	n := 10000
	schema := "radon"
	for i := 0; i < n; i++ {
		query := "select a,b,cd from table1 where a=b and c=d and e=d group by id order\n by desc"
		ioworker.LogEvent("SELECT", schema, query)
	}
	time.Sleep(time.Second)

	sqlworker := NewSQLWorker(log, conf, ts)
	err = sqlworker.Init()
	assert.Nil(t, err)
	defer sqlworker.close()

	got := 0
	for {
		event, err := sqlworker.NextEvent()
		if err != nil {
			break
		}
		if event == nil {
			break
		}
		got++
	}
	assert.Equal(t, n, got)
}

func TestSQLWorkerInitError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_binlog_", log)
	defer os.RemoveAll(tmpDir)
	defer leaktest.Check(t)()

	conf := &config.BinlogConfig{
		MaxSize: 102400,
		LogDir:  tmpDir,
	}

	sqlworker := NewSQLWorker(log, conf, 0)
	sqlworker.rfile = &mockRotateFile{}
	err := sqlworker.Init()
	assert.NotNil(t, err)
	defer sqlworker.close()

	// For mock.go code coverage.
	{
		sqlworker.rfile.Write([]byte{0x00})
		sqlworker.rfile.Sync()
		sqlworker.rfile.GetOldLogInfos()
		sqlworker.rfile.GetNextLogInfo("")
	}
}

func TestSQLWorkerNoAnyBinlogFiles(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_binlog_", log)
	defer os.RemoveAll(tmpDir)
	defer leaktest.Check(t)()

	conf := &config.BinlogConfig{
		MaxSize: 102400,
		LogDir:  tmpDir,
	}

	ts := time.Now().UnixNano()
	ioworker := NewIOWorker(log, conf)
	err := ioworker.Init()
	assert.Nil(t, err)
	defer ioworker.Close()

	sqlworker := NewSQLWorker(log, conf, ts)
	err = sqlworker.Init()
	assert.Nil(t, err)
	defer sqlworker.close()

	// Writes events.
	n := 100
	schema := "radon"
	for i := 0; i < n; i++ {
		query := "select a,b,cd from table1 where a=b and c=d and e=d group by id order\n by desc"
		ioworker.LogEvent("SELECT", schema, query)
	}
	time.Sleep(500 * time.Millisecond)

	// Reads events.
	got := 0
	for {
		event, err := sqlworker.NextEvent()
		if err != nil {
			break
		}
		if event == nil {
			break
		}
		got++
	}
	assert.Equal(t, n, got)
}

func TestSQLWorkerSeekEvent(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_binlog_", log)
	defer os.RemoveAll(tmpDir)
	defer leaktest.Check(t)()

	conf := &config.BinlogConfig{
		MaxSize: 102400,
		LogDir:  tmpDir,
	}
	os.RemoveAll(conf.LogDir)

	ioworker := NewIOWorker(log, conf)
	err := ioworker.Init()
	assert.Nil(t, err)

	n := 10
	schema := "radon"
	for i := 0; i < n; i++ {
		query := "select a,b,cd from table1 where a=b and c=d and e=d group by id order\n by desc"
		ioworker.LogEvent("SELECT", schema, query)
	}
	ts := time.Now().UnixNano()
	ioworker.Close()
	time.Sleep(time.Second)

	{
		sqlworker := NewSQLWorker(log, conf, ts)
		err = sqlworker.Init()
		assert.Nil(t, err)
		defer sqlworker.close()
	}

	{
		ts = time.Now().UnixNano()
		sqlworker := NewSQLWorker(log, conf, ts)
		err = sqlworker.Init()
		assert.Nil(t, err)
		defer sqlworker.close()
	}
}

func TestSQLWorkerAndIOWorkerAsync(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_binlog_", log)
	defer os.RemoveAll(tmpDir)
	defer leaktest.Check(t)()

	conf := &config.BinlogConfig{
		MaxSize: 1024 * 1024,
		LogDir:  tmpDir,
	}

	var wg sync.WaitGroup
	writeDone := make(chan bool)
	os.RemoveAll(conf.LogDir)
	ts := time.Now().UnixNano()

	wg.Add(1)
	writes := 100000
	go func() {
		defer wg.Done()
		ioworker := NewIOWorker(log, conf)
		err := ioworker.Init()
		assert.Nil(t, err)
		defer ioworker.Close()

		schema := "radon"
		sleepParts := writes - writes/10
		for i := 0; i < writes; i++ {
			query := "select a,b,cd from table1 where a=b and c=d and e=d group by id order\n by desc"
			ioworker.LogEvent("SELECT", schema, query)
			if i > sleepParts {
				time.Sleep(time.Duration(rand.Intn(500)) * time.Microsecond)
			}
		}
		writeDone <- true
	}()
	time.Sleep(time.Second)

	wg.Add(1)
	reads := 0
	go func() {
		defer wg.Done()
		sqlworker := NewSQLWorker(log, conf, ts)
		err := sqlworker.Init()
		assert.Nil(t, err)
		defer sqlworker.close()

		done := false
		for !done {
			select {
			case <-writeDone:
				done = true
			default:
			}
			event, err := sqlworker.NextEvent()
			assert.Nil(t, err)
			if event == nil {
				time.Sleep(time.Duration(rand.Intn(100)) * time.Microsecond)
			} else {
				reads++
			}
			//log.Info("read.event:%+v", event)
		}
	}()
	wg.Wait()
	assert.Equal(t, writes, reads)
}

func TestSQLWorkerSeekFromSecond(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_binlog_", log)
	defer os.RemoveAll(tmpDir)
	defer leaktest.Check(t)()

	conf := &config.BinlogConfig{
		MaxSize: 1024 * 1024,
		LogDir:  tmpDir,
	}

	os.RemoveAll(conf.LogDir)
	ts := time.Now().UnixNano()
	writes := 100
	{
		ioworker := NewIOWorker(log, conf)
		err := ioworker.Init()
		assert.Nil(t, err)
		defer ioworker.Close()

		schema := "radon"
		for i := 0; i < writes; i++ {
			query := "select a,b,cd from table1 where a=b and c=d and e=d group by id order\n by desc"
			ioworker.LogEvent("SELECT", schema, query)
			if i == 0 {
				ts = time.Now().UnixNano()
			}
		}
	}

	reads := 0
	{
		sqlworker := NewSQLWorker(log, conf, ts)
		err := sqlworker.Init()
		assert.Nil(t, err)
		defer sqlworker.close()

		for {
			event, err := sqlworker.NextEvent()
			assert.Nil(t, err)
			if event == nil {
				break
			} else {
				reads++
			}
		}
	}
	assert.True(t, reads <= (writes-1))
}

func TestSQLWorkerStaleWrite(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_binlog_", log)
	defer os.RemoveAll(tmpDir)
	defer leaktest.Check(t)()

	conf := &config.BinlogConfig{
		MaxSize: 1024,
		LogDir:  tmpDir,
	}

	ts := time.Now().UnixNano()
	ioworker := NewIOWorker(log, conf)
	err := ioworker.Init()
	assert.Nil(t, err)
	defer ioworker.Close()

	n := 256
	schema := "radon"
	for i := 0; i < n; i++ {
		query := "select a,b,cd from table1 where a=b and c=d and e=d group by id order\n by desc"
		ioworker.LogEvent("SELECT", schema, query)
	}
	time.Sleep(time.Second)

	sqlworker := NewSQLWorker(log, conf, ts)
	err = sqlworker.Init()
	assert.Nil(t, err)
	defer sqlworker.close()

	_, err = sqlworker.NextEvent()
	assert.Nil(t, err)
	exists := sqlworker.checkNextFileExists()
	assert.False(t, exists)
}
