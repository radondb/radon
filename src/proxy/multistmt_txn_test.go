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

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestProxyHandleMStmtTxnBegin(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()
	proxy.SetTwoPC(true)

	{
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("select @@autocommit", autocommitResult1)
	}

	client, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)

	{
		query := "begin;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	client.Close()
}

func TestProxyHandleMStmtTxnRollback(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup() // if the session is not closed, cost 1s
	address := proxy.Address()
	proxy.SetTwoPC(true)

	{
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("select @@autocommit", autocommitResult1)
	}

	client, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)

	{
		query := "rollback;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	client.Close()
}

func TestProxyHandleMStmtTxnCommit(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup() // if the session is not closed, cost 1s
	address := proxy.Address()
	proxy.SetTwoPC(true)

	{
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("select @@autocommit", autocommitResult1)
	}

	client, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)

	{
		query := "commit;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	client.Close()
}

func TestProxyHandleMStmtTxnBeginRollback(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup() // if the session is not closed, cost 1s
	address := proxy.Address()
	proxy.SetTwoPC(true)

	{
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("select @@autocommit", autocommitResult1)
	}

	client, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)

	{
		query := "begin;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "rollback;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	client.Close()
}

func TestProxyHandleMStmtTxnBeginCommit(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup() // if the session is not closed, cost 1s
	address := proxy.Address()
	proxy.SetTwoPC(true)

	// fakedbs.
	{
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("select @@autocommit", autocommitResult1)
	}

	client, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)

	{
		query := "begin;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "commit;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	client.Close()
}

func TestProxyHandleMStmtTxnDoubleBegin(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup() // if the session is not closed, cost 1s
	address := proxy.Address()
	proxy.SetTwoPC(true)

	{
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("select @@autocommit", autocommitResult1)
	}

	client, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)

	{
		query := "begin;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "start transaction;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}
}

func TestProxyHandleMStmtTxnBeginFailed(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup() // if the session is not closed, cost 1s
	address := proxy.Address()
	proxy.SetTwoPC(true)

	{
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("select @@autocommit", autocommitResult1)
		fakedbs.AddQueryErrorPattern("XA START .*", errors.New("mock.xa.start.error"))
	}

	client, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)

	{
		query := "begin;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	client.Close()
}

func TestProxyHandleMStmtTxnBeginRollbackFailed(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup() // if the session is not closed, cost 1s
	address := proxy.Address()
	proxy.SetTwoPC(true)

	{
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("select @@autocommit", autocommitResult1)
		fakedbs.AddQueryErrorPattern("XA END .*", errors.New("mock.xa.end.error"))
	}

	client, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)

	{
		query := "begin;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "rollback;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	client.Close()
}

func TestProxyHandleMStmtTxnBeginCommitFailed(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup() // if the session is not closed, cost 1s
	address := proxy.Address()
	proxy.SetTwoPC(true)

	{
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("select @@autocommit", autocommitResult1)
		fakedbs.AddQueryErrorPattern("XA END .*", errors.New("mock.xa.end.error"))
	}

	client, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)

	{
		query := "begin;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "commit;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	client.Close()
}

func TestProxyHandleMStmtTxnTwoPCFalse(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup() // if the session is not closed, cost 1s
	address := proxy.Address()
	assert.Equal(t, false, proxy.conf.Proxy.TwopcEnable)

	{
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("select @@autocommit", autocommitResult1)
	}

	client, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)

	{
		query := "begin;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	{
		query := "commit;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	{
		query := "rollback;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	client.Close()
}

func TestProxyHandleMStmtTxnBeginUpdatesCommitFailed(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert .*", &sqltypes.Result{})
		fakedbs.AddQueryErrorPattern("XA END .*", errors.New("mock.xa.end.error"))
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	proxy.SetTwoPC(true)
	client1, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)
	{
		query := "begin;"
		_, err = client1.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "insert into test.t1(id, b) values(1, 1), (2, 2);"
		_, err := client1.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "commit;"
		_, err = client1.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	client1.Close()
}

func TestProxyHandleMStmtTxnBeginUpdatesRollbackFailed(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert .*", &sqltypes.Result{})
		fakedbs.AddQueryErrorPattern("XA END .*", errors.New("mock.xa.end.error"))
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	proxy.SetTwoPC(true)
	client1, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)
	{
		query := "begin;"
		_, err = client1.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "insert into test.t1(id, b) values(1, 1), (2, 2);"
		_, err := client1.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "rollback;"
		_, err = client1.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	client1.Close()
}

func TestProxyHandleMStmtTxnBeginUpdateRollbackFailed(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert .*", &sqltypes.Result{})
		fakedbs.AddQueryErrorPattern("XA END .*", errors.New("mock.xa.end.error"))
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	proxy.SetTwoPC(true)
	client1, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)
	{
		query := "begin;"
		_, err = client1.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "insert into test.t1(id, b) values(1, 1);" // one backend
		_, err := client1.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "rollback;"
		_, err = client1.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	client1.Close()
}

// use DATABASE
func TestProxyHandleMStmtTxnUseDB(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	proxy.SetTwoPC(true)
	client1, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)

	{
		query := "use test;"
		_, err = client1.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "begin;"
		_, err = client1.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "insert into t1(id, b) values(1, 1);" // one backend
		_, err := client1.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "select * from t1;"
		_, err := client1.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "commit;"
		_, err = client1.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	client1.Close()
}

func TestProxyHandleMStmtTxnNOUseDB(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert .*", &sqltypes.Result{})
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	proxy.SetTwoPC(true)
	client1, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)

	{
		query := "begin;"
		_, err = client1.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "insert into test.t1(id, b) values(1, 1);"
		_, err := client1.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		query := "select * from t1;"
		_, err := client1.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	{
		query := "commit;"
		_, err = client1.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	client1.Close()
}

/*
	autoCommit := "select @@autocommit"
	results, err := spanner.ExecuteScatter(autoCommit)
	if err != nil {
		return nil, err
	}

	for _, row := range results.Rows {
		value := row[0].ToNative()
		if value.(int64) == 0 {
			log.Error("spanner.execute.multistate.begin.autocommit0.unsupported.")
			return nil, errors.New("ExecuteMultiStatBegin.autocommit0.unsupported")
		}
	}

func TestProxyHandleMultiStateAutoCommit0(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup() // if the session is not closed, cost 1s
	address := proxy.Address()
	proxy.SetTwoPC(true)

	{
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("select @@autocommit", autocommitResult0)
	}

	client, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)

	{
		query := "begin;"
		fakedbs.AddQuery(query, fakedb.Result3)
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}
}

*/
