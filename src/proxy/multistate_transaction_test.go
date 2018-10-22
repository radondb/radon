/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"errors"
	"testing"

	"fakedb"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestProxyHandleMultiStateTransaction(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryError("select * from backup", errors.New("mock.backup.select.error"))
	}

	// begin.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "begin;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// start transaction.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "start transaction;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// rollback.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "rollback;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// commit.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "commit;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
}
