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

func TestProxySet(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
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
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// set.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		{
			query := "set @@SESSION.radon_streaming_fetch='ON'"
			_, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
		{
			query := "set @@SESSION.radon_streaming_fetch='OFF'"
			_, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
		{
			query := "set @@SESSION.radon_streaming_fetch=true"
			_, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
		{
			query := "set @@SESSION.radon_streaming_fetch=false"
			_, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
		{
			query := "set @@SESSION.radon_streaming_fetch=123"
			_, err := client.FetchAll(query, -1)
			assert.NotNil(t, err)
		}
	}
}

func TestProxySetAutocommit(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		proxy.conf.Proxy.TwopcEnable = true
		proxy.conf.Proxy.AutocommitFalseIsTxn = true
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("xa .*", &sqltypes.Result{})
	}

	// set.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		{
			query := "set autocommit=0"
			_, err := client.FetchAll(query, -1)
			assert.Nil(t, err)

			query = "select 1"
			_, err = client.FetchAll(query, -1)
			assert.Nil(t, err)

			query = "commit"
			_, err = client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
		{
			query := "set autocommit=1"
			_, err := client.FetchAll(query, -1)
			assert.Nil(t, err)

			query = "commit"
			_, err = client.FetchAll(query, -1)
			assert.NotNil(t, err)
		}
	}

	proxy.conf.Proxy.AutocommitFalseIsTxn = false
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		{
			query := "set autocommit=0"
			_, err := client.FetchAll(query, -1)
			assert.Nil(t, err)

			query = "select 1"
			_, err = client.FetchAll(query, -1)
			assert.Nil(t, err)

			query = "commit"
			_, err = client.FetchAll(query, -1)
			assert.NotNil(t, err)
		}
		{
			query := "set autocommit=1"
			_, err := client.FetchAll(query, -1)
			assert.Nil(t, err)

			query = "commit"
			_, err = client.FetchAll(query, -1)
			assert.NotNil(t, err)
		}
	}
}
