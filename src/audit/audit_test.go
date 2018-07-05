/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package audit

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"config"
	"fakedb"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestAudit(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_audit_", log)
	defer os.RemoveAll(tmpDir)
	conf := &config.AuditConfig{
		Mode:        ALL,
		MaxSize:     102400,
		ExpireHours: 1,
		LogDir:      tmpDir,
	}

	audit := NewAudit(log, conf)
	err := audit.Init()
	assert.Nil(t, err)
	defer audit.Close()

	n := 10000
	for i := 0; i < n; i++ {
		typ := "SELECT"
		user := "BohuTANG>>>>"
		host := "127.0.0.1:8899"
		threadID := uint32(i)
		query := "select a,b,cd from table1 where a=b and c=d and e=d group by id order\n by desc"
		if i%2 == 0 {
			audit.LogWriteEvent(typ, user, host, threadID, query, 0, time.Now())
		} else {
			audit.LogReadEvent(typ, user, host, threadID, query, 0, time.Now())
		}
	}
}

func TestAuditMultiThread(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_audit_", log)
	defer os.RemoveAll(tmpDir)
	conf := &config.AuditConfig{
		Mode:        ALL,
		MaxSize:     1024 * 1024,
		ExpireHours: 1,
		LogDir:      tmpDir,
	}

	audit := NewAudit(log, conf)
	err := audit.Init()
	assert.Nil(t, err)
	defer audit.Close()

	var wait sync.WaitGroup
	for k := 0; k < 10; k++ {
		wait.Add(1)
		go func(a *Audit) {
			n := 10000
			for i := 0; i < n; i++ {
				typ := "SELECT"
				user := "BohuTANG>>>>"
				host := "127.0.0.1:8899"
				threadID := uint32(i)
				query := "select a,b,cd from table1 where a=b and c=d and e=d group by id order\n by desc"
				if i%2 == 0 {
					a.LogWriteEvent(typ, user, host, threadID, query, 0, time.Now())
				} else {
					a.LogReadEvent(typ, user, host, threadID, query, 0, time.Now())
				}
			}
			wait.Done()
		}(audit)
	}
	wait.Wait()
}

func TestPurge(t *testing.T) {
	fileFormat := "20060102150405.000"
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_audit_", log)
	defer os.RemoveAll(tmpDir)
	conf := &config.AuditConfig{
		Mode:        ALL,
		MaxSize:     102400,
		ExpireHours: 1,
		LogDir:      tmpDir,
	}

	audit := NewAudit(log, conf)
	err := audit.Init()
	assert.Nil(t, err)
	defer audit.Close()

	n := 10000
	for i := 0; i < n; i++ {
		typ := "SELECT"
		user := "BohuTANG>>>>"
		host := "127.0.0.1:8899"
		threadID := uint32(i)
		query := "select a,b,cd from table1 where a=b and c=d and e=d group by id order\n by desc"
		if i%2 == 0 {
			audit.LogWriteEvent(typ, user, host, threadID, query, 0, time.Now())
		} else {
			audit.LogReadEvent(typ, user, host, threadID, query, 0, time.Now())
		}
	}
	// first the close the audit to stop the event writing.

	logs, _ := audit.rfile.GetOldLogInfos()
	// purge the old log.
	l0 := logs[0]
	ts := time.Unix(0, l0.Ts).UTC().Add(time.Duration(time.Hour * time.Duration(-2)))
	timestamp := ts.Format(fileFormat)
	newName := filepath.Join(conf.LogDir, fmt.Sprintf("%s%s%s", prefix, timestamp, extension))
	os.Rename(filepath.Join(conf.LogDir, l0.Name), newName)
	audit.doPurge()

	logs1, _ := audit.rfile.GetOldLogInfos()
	want := len(logs)
	got := len(logs1)
	assert.Equal(t, want-1, got)
}

func TestAuditBench(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))
	tmpDir := fakedb.GetTmpDir("", "radon_audit_", log)
	defer os.RemoveAll(tmpDir)
	conf := &config.AuditConfig{
		Mode:        ALL,
		MaxSize:     1024 * 1024 * 100,
		ExpireHours: 1,
		LogDir:      tmpDir,
	}

	audit := NewAudit(log, conf)
	err := audit.Init()
	assert.Nil(t, err)
	defer audit.Close()

	{
		N := 100000
		now := time.Now()
		for i := 0; i < N; i++ {
			typ := "SELECT"
			user := "BohuTANG>>>>"
			host := "127.0.0.1:8899"
			threadID := uint32(i)
			query := "select a,b,cd from table1 where a=b and c=d and e=d group by id order\n by desc"
			audit.LogWriteEvent(typ, user, host, threadID, query, 0, time.Now())
		}
		took := time.Since(now)
		fmt.Printf(" LOOP\t%v COST %v, avg:%v/s\n", N, took, (int64(N)/(took.Nanoseconds()/1e6))*1000)
	}
}
