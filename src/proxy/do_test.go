/*
 * Radon
 *
 * Copyright 2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestDoStmt(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("do 1 *.*", &sqltypes.Result{})
		fakedbs.AddQueryErrorPattern("do a", errors.New("ERROR 1054 (42S22): Unknown column 'a' in 'field list'"))
	}

	// Normal do stmt.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		querys := []string{
			"do 1",
			"do 1,1+1,1&1",
			"do 1 != 3, not 1, null is null, not null, 1 or 2",
		}
		for _, query := range querys {
			_, err = client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
	}
	// Error do stmt.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "do a"
		_, actual := client.FetchAll(query, -1)
		expected := "ERROR 1054 (42S22): Unknown column 'a' in 'field list' (errno 1105) (sqlstate HY000)"
		assert.EqualValues(t, expected, actual.Error())
	}
}
