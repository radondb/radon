/*
 * Radon
 *
 * Copyright 2018-2020 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// +--------------+----------+----------+-------------------------------------------------------------------+
// | Table        | Op       | Msg_type | Msg_text                                                          |
// +--------------+----------+----------+-------------------------------------------------------------------+
// | test.t       | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
// | test.t       | optimize | status   | OK                                                                |
// | test.t1_0001 | optimize | status   | OK                                                                |
// | test.t1_0001 | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
// +--------------+----------+----------+-------------------------------------------------------------------+
var (
	qrResult = &sqltypes.Result{
		RowsAffected: 1,
		Fields: []*querypb.Field{
			{
				Name: "Table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Op",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Msg_type",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Msg_text",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test.t")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("optimize/check")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("note")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("Table does not support optimize/check, doing recreate + analyze instead")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test.t")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("optimize/check")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("status")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("OK")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test1.t1_0001")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("optimize/check")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("status")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("OK")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test1.t1_0001")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("optimize/check")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("note")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("Table does not support optimize/check, doing recreate + analyze instead")),
			},
		},
	}
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
		fakedbs.AddQueryPattern("optimize .*", qrResult)
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
			"optimize local table t3",
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

func TestProxyCheckTable(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("check .*", qrResult)
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

	// check with table exist.
	{
		querys := []string{
			"check /*test option*/ table t1 for upgrade quick fast medium extended changed",
			"check /*test multi tables*/ table t1, t2",
		}
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		for _, query := range querys {
			_, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
	}

	// check with table not exist.
	{
		querys := []string{
			"check table t3",
			"check /*test multi tables*/ table t1, t2, t3",
		}
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		for _, query := range querys {
			_, err := client.FetchAll(query, -1)
			assert.NotNil(t, err)
		}
	}

	// check with db not exist.
	{
		query := "check /*test db not exist*/ local table xx.t1"
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}
}
