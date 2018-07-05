/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package binlog

import (
	"fmt"
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

func TestIOWorker(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_binlog_", log)
	defer leaktest.Check(t)()
	defer os.RemoveAll(tmpDir)

	conf := &config.BinlogConfig{
		MaxSize: 102400,
		LogDir:  tmpDir,
	}

	os.RemoveAll(conf.LogDir)
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
}

func TestIOWorkerMultiThread(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_binlog_", log)
	defer os.RemoveAll(tmpDir)
	defer leaktest.Check(t)()

	conf := &config.BinlogConfig{
		MaxSize: 1024 * 1024,
		LogDir:  tmpDir,
	}
	os.RemoveAll(conf.LogDir)
	ioworker := NewIOWorker(log, conf)
	err := ioworker.Init()
	assert.Nil(t, err)
	defer ioworker.Close()

	schema := "radon"
	var wait sync.WaitGroup
	for k := 0; k < 10; k++ {
		wait.Add(1)
		go func(ioworker *IOWorker) {
			n := 10000
			for i := 0; i < n; i++ {
				query := "select a,b,cd from table1 where a=b and c=d and e=d group by id order\n by desc"
				ioworker.LogEvent("SELECT", schema, query)
			}
			wait.Done()
		}(ioworker)
	}
	wait.Wait()
}

func TestIOWorkerBench(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_binlog_", log)
	defer os.RemoveAll(tmpDir)

	conf := &config.BinlogConfig{
		MaxSize: 1024 * 1024 * 100,
		LogDir:  tmpDir,
	}
	os.RemoveAll(conf.LogDir)
	ioworker := NewIOWorker(log, conf)
	err := ioworker.Init()
	assert.Nil(t, err)
	defer ioworker.Close()

	{
		N := 100000
		schema := "radon"
		now := time.Now()
		for i := 0; i < N; i++ {
			query := "select a,b,cd from table1 where a=b and c=d and e=d group by id order\n by desc"
			ioworker.LogEvent("SELECT", schema, query)
		}
		took := time.Since(now)
		fmt.Printf(" LOOP\t%v COST %v, avg:%v/s\n", N, took, (int64(N)/(took.Nanoseconds()/1e6))*1000)
	}
}
