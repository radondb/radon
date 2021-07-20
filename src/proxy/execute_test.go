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
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestProxyExecute(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("xa .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("desc .*", &sqltypes.Result{})
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

	// Insert.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "insert into test.t1 (id, b) values(1,2),(3,4)"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// Insert with 2PC.
	{
		proxy.conf.Proxy.TwopcEnable = true
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "insert into test.t1 (id, b) values(1,2),(3,4)"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// select with 2PC.
	{
		proxy.conf.Proxy.TwopcEnable = true
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "select * from test.t1"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
}

func TestProxyExecute2PCError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("xa .*", &sqltypes.Result{})
		fakedbs.AddQueryError("insert into test.t1_0008(id, b) values (1, 2)", errors.New("xx"))
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

	// Insert with 2PC but execute error.
	{
		proxy.conf.Proxy.TwopcEnable = true
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "insert into test.t1 (id, b) values(1,2),(3,4)"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// Insert with 2PC but execute error, RollbackPhaseOne error.
	{
		fakedbs.AddQueryErrorPattern("XA END .*", sqldb.NewSQLError1(1397, "XAE04", "XAER_NOTA: Unknown XID"))
		proxy.conf.Proxy.TwopcEnable = true
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "insert into test.t1 (id, b) values(1,2),(3,4)"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}
}

func TestProxyExecute2PCCommitError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("xa start .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("xa end .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("xa rollback .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("xa commit .*", &sqltypes.Result{})
		fakedbs.AddQueryErrorPattern("xa prepare.*", errors.New("mock.xa.prepare.error"))
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

	// Insert with 2PC but prepare error in the commit phase.
	{
		proxy.conf.Proxy.TwopcEnable = true
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "insert into test.t1 (id, b) values(1,2),(3,4)"
		_, err = client.FetchAll(query, -1)
		want := "mock.xa.prepare.error (errno 1105) (sqlstate HY000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// Insert with 2PC but rollback error in the commit phase.
	{
		fakedbs.ResetPatternErrors()
		fakedbs.AddQueryErrorPattern("XA ROLLBACK .*", sqldb.NewSQLError1(1397, "XAE04", "XAER_NOTA: Unknown XID"))
		fakedbs.AddQueryPattern("xa prepare .*", &sqltypes.Result{})

		proxy.conf.Proxy.TwopcEnable = true
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "insert into test.t1 (id, b) values(1,2),(3,4)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
}

func TestProxyExecuteSelectError(t *testing.T) {
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
		querys := []string{
			"create table test.t1(id int, b int) partition by hash(id)",
			"create table test.t2(id int, b int) partition by hash(id)",
		}
		for _, query := range querys {
			_, err = client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
	}

	// select.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		querys := []string{
			"select t1.a,t2.b from test.t1, test.t2",
		}
		wants := []string{
			"ExecuteStreamFetch.unsupport.cross-shard.join (errno 1105) (sqlstate HY000)",
		}
		for i, query := range querys {
			sql := "set @@SESSION.radon_streaming_fetch='ON'"
			_, err := client.FetchAll(sql, -1)
			assert.Nil(t, err)

			fakedbs.AddQuery(query, fakedb.Result3)
			_, err = client.FetchAll(query, -1)

			got := err.Error()
			assert.Equal(t, wants[i], got)
		}
	}
}

func TestProxyExecuteReadonly(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("desc .*", &sqltypes.Result{})
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

	// Insert.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "insert into test.t1 (id, b) values(1,2),(3,4)"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// Set readonly.
	{
		proxy.SetReadOnly(true)
	}

	// select.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "select * from test.t1"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// Insert.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "insert into test.t1 (id, b) values(1,2),(3,4)"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		want := "The MySQL server is running with the --read-only option so it cannot execute this statement (errno 1290) (sqlstate 42000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// Radon Admin.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "radon attach('attach1', '127.0.0.1:6000', 'root', '123456', 'xxxx')"
		_, err = client.FetchAll(query, -1)
		want := "The MySQL server is running with the --read-only option so it cannot execute this statement (errno 1290) (sqlstate 42000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestProxyExecuteStreamFetch(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("desc .*", &sqltypes.Result{})
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

	// select with stream.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "select * from test.t1"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
}

func TestProxyExecuteMultiStmtTxnDDLError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("begin", &sqltypes.Result{})
		fakedbs.AddQueryPattern("rollback", &sqltypes.Result{})
		fakedbs.AddQueryPattern("xa .*", &sqltypes.Result{})
		fakedbs.AddQueryError("insert into test.t1_0008(id, b) values (1, 2)", errors.New("xx"))
	}

	// create database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create database test"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	//begin && create test table.
	{
		proxy.conf.Proxy.TwopcEnable = true
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "begin;"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)

		query1 := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query1, -1)
		assert.NotNil(t, err)

		query2 := "rollback;"
		_, err = client.FetchAll(query2, -1)
		assert.Nil(t, err)
	}
}

func TestProxyExecuteMultiStmtTxnDMLError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("begin", &sqltypes.Result{})
		fakedbs.AddQueryPattern("commit", &sqltypes.Result{})
		fakedbs.AddQueryPattern("xa .*", &sqltypes.Result{})
	}

	// create database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create database test"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	//begin && create test table.
	{
		proxy.conf.Proxy.TwopcEnable = true
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query1 := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query1, -1)
		assert.Nil(t, err)

		{
			query := "begin;"
			_, err = client.FetchAll(query, -1)
			assert.Nil(t, err)

			querys := []string{
				"insert into test.t1 (id, b) values(1,2),(3,4)",
				"update test.t1 set b=1 where id=3",
				"delete from test.t1 where 1=1",
			}
			for _, query := range querys {
				_, err = client.FetchAll(query, -1)
				assert.NotNil(t, err)
			}
		}

		query := "commit;"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
}

func TestProxyExecutPrivilegeN(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxyPrivilegeN(log, MockDefaultConfig())
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("xa .*", &sqltypes.Result{})
	}

	// create database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create database test"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// select.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "select * from test.t1"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
		want := "Access denied for user 'mock'@'%' to database 'test' (errno 1045) (sqlstate 28000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestProxyExecuteLoadBalance(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()
	proxy.SetLoadBalance(1)

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("desc .*", &sqltypes.Result{})
	}

	// create database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "create database test"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// select with non-2pc.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "select * from test.t1"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// select /*+ loadbalance */ with non-2pc.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "select /*+ loadbalance=0 */ * from test.t1"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// select /*+ loadbalance */ with 2PC.
	{
		proxy.conf.Proxy.TwopcEnable = true
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "select /*+ loadbalance=1 */ * from test.t1"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
}
