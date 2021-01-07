/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
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

func TestProxyOptimizeTable(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("optimize .*", &sqltypes.Result{})
	}

	// create database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create database test"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// create test table.
	{
		querys := []string{
			"create table test.t1(id int, b int) partition by hash(id)",
			"create table test.t2(id int, b int) partition by hash(id)",
		}
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		for _, query := range querys {
			_, err = client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
	}

	// optimize with table exist.
	{
		querys := []string{
			"optimize /*test option*/ local table t1",
			"optimize /*test option*/ no_write_to_binlog table t1",
			"optimize /*test multi tables*/ table t1, t2",
		}
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		for _, query := range querys {
			_, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
	}

	// optimize with table not exist.
	{
		querys := []string{
			"optimize /*test option*/ local table t3",
			"optimize /*test multi tables*/ table t1, t2, t3",
		}
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		for _, query := range querys {
			_, err := client.FetchAll(query, -1)
			assert.NotNil(t, err)
		}
	}

	// optimize with db not exist.
	{
		query := "optimize /*test option*/ local table xx.t1"
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}
}
