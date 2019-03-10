/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestProxySessionWaitForShutdown(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select * .*", &sqltypes.Result{})
		fakedbs.AddQueryDelay("select * from test.t1_0002", &sqltypes.Result{}, 30000)
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
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create table t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
		client.Quit()
	}

	var wg sync.WaitGroup
	{
		wg.Add(1)
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		go func(c driver.Conn) {
			defer wg.Done()
			query := "select * from t1"
			_, err = client.FetchAll(query, -1)
		}(client)
	}
	time.Sleep(time.Second)
	cleanup()
	wg.Wait()
}

func TestProxySessionTxnBeginCommit(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()
	proxy.SetTwoPC(true)

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select * .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert * .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("begin", &sqltypes.Result{})
		fakedbs.AddQueryPattern("XA .*", result1)
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
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create table t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
		client.Quit()
	}

	client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
	assert.Nil(t, err)
	query := "begin"
	_, err = client.FetchAll(query, -1)
	assert.Nil(t, err)

	query = "insert into test.t1(id, b) values(1, 1), (2, 2);"
	_, err = client.FetchAll(query, -1)
	assert.Nil(t, err)

	query = "select * from test.t1" //TODO
	_, err = client.FetchAll(query, -1)
	assert.Nil(t, err)

	query = "commit"
	_, err = client.FetchAll(query, -1)
	assert.Nil(t, err)
}
