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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestInfo(t *testing.T) {
	defer leaktest.Check(t)()
	os.RemoveAll(mockDir)
	os.MkdirAll(mockDir, os.ModePerm)

	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	conf := &config.BinlogConfig{
		MaxSize: 102400,
		LogDir:  mockDir,
	}

	ts := time.Now().UnixNano()
	{
		relay := NewInfo(log, conf, "relay-log.info")
		err := relay.Init()
		assert.Nil(t, err)
		defer relay.Close()

		n := 100000
		now := time.Now()
		for i := 0; i < n; i++ {
			ts = time.Now().UnixNano()
			relay.Sync("xx", ts)
		}
		took := time.Since(now)
		fmt.Printf(" LOOP\t%v COST %v, avg:%v/s\n", n, took, (int64(n)/(took.Nanoseconds()/1e6))*1000)

		ts1, _ := relay.ReadTs()
		assert.Equal(t, ts, ts1)
	}

	//
	{
		relay := NewInfo(log, conf, "relay-log.info")
		err := relay.Init()
		assert.Nil(t, err)
		defer relay.Close()
		ts1, _ := relay.ReadTs()
		assert.Equal(t, ts1, ts)
	}
}
