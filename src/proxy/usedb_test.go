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

func TestProxyUseDB(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()

	// Client.
	client, err := driver.NewConn("mock", "mock", proxy.Address(), "", "utf8")
	assert.Nil(t, err)

	// Use test.
	{
		query := "use test"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err := client.FetchAll(query, -1)
		assert.Nil(t, err)

		want := 1
		got := fakedbs.GetQueryCalledNum(query)
		assert.Equal(t, want, got)
	}

	// Use mysql.
	{
		query := "use mysql"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err := client.FetchAll(query, -1)
		want := "Access denied; lacking privileges for database mysql (errno 1227) (sqlstate 42000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// Use test error.
	{
		query := "use test"
		fakedbs.AddQueryError(query, errors.New("mock use test error"))
		_, err := client.FetchAll(query, -1)
		want := "mock use test error (errno 1105) (sqlstate HY000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// test db not exists.
	{
		_, err := driver.NewConn("mock", "mock", proxy.Address(), "xx", "utf8")
		assert.NotNil(t, err)
	}
}
