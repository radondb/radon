/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestProxyUseDatabase(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("use test", &sqltypes.Result{})
	}

	// connection without database.
	{
		_, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
	}

	// use db.
	{
		_, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		//// 'use db' In MySQL client use COM_INIT_DB, but the client.FetchAll use COM_QUERY, so comment the below.
		//query := "use test"
		//_, err = client.FetchAll(query, -1)
		//assert.Nil(t, err)
	}
}

func TestProxyUseDatabasePrivilegeNotSuper(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxyPrivilegeNotSuper(log, MockDefaultConfig())
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("use test1", &sqltypes.Result{})
	}

	// connection without database.
	{
		_, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
	}

	// use db.
	{
		_, err := driver.NewConn("mock", "mock", address, "test1", "utf8")
		assert.Nil(t, err)
	}
}

func TestProxyUseDatabasePrivilegeDB(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxyPrivilegeN(log, MockDefaultConfig())
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("use test1", &sqltypes.Result{})
	}

	// connection without database.
	{
		_, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
	}

	// use db.
	{
		_, err := driver.NewConn("mock", "mock", address, "test1", "utf8")
		assert.NotNil(t, err)
	}
}
