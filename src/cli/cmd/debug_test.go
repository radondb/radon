/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package cmd

import (
	"testing"
	"time"

	"ctl"
	"proxy"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestCmdDebugConfigz(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	admin := ctl.NewAdmin(log, proxy)
	admin.Start()
	defer admin.Stop()
	time.Sleep(100 * time.Nanosecond)

	{
		cmd := NewDebugCommand()
		_, err := executeCommand(cmd, "configz")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "configz", "--radon-host", "127.0.0.1")
		assert.Nil(t, err)
	}
}

func TestCmdDebugBackendz(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	admin := ctl.NewAdmin(log, proxy)
	admin.Start()
	defer admin.Stop()
	time.Sleep(100 * time.Nanosecond)

	{
		cmd := NewDebugCommand()
		_, err := executeCommand(cmd, "backendz")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "backendz", "--radon-host", "127.0.0.1")
		assert.Nil(t, err)
	}
}

func TestCmdDebugSchemaz(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	admin := ctl.NewAdmin(log, proxy)
	admin.Start()
	defer admin.Stop()
	time.Sleep(100 * time.Nanosecond)

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		cmd := NewDebugCommand()
		_, err := executeCommand(cmd, "schemaz")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "schemaz", "--radon-host", "127.0.0.1")
		assert.Nil(t, err)
	}
}
