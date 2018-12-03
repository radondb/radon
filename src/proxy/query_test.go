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

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestProxyQueryTxn(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()
	querys := []string{
		"start transaction",
		"commit",
		"SET autocommit=0",
	}

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		for _, query := range querys {
			fakedbs.AddQueryPattern(query, &sqltypes.Result{})
		}
	}

	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()

		for _, query := range querys {
			_, err = client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
	}
}

func TestProxyQuerySet(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()
	querys := []string{
		"SET autocommit=0",
		"SET SESSION wait_timeout = 2147483",
		"SET NAMES utf8",
	}

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		for _, query := range querys {
			fakedbs.AddQueryPattern(query, &sqltypes.Result{})
		}
	}

	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()

		// Support.
		for _, query := range querys {
			_, err = client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
	}
}

func TestProxyQueryComments(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()
	querys := []string{
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/",
		"select a /*xx*/ from t1",
	}

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		for _, query := range querys {
			fakedbs.AddQuery(query, &sqltypes.Result{})
		}
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()

		// Support.
		for _, query := range querys {
			_, err = client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
	}
}

// Proxy with no backup
func TestProxyQueryStream(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	result11 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: make([][]sqltypes.Value, 0, 256)}

	for i := 0; i < 2017; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("11")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("1nice name")),
		}
		result11.Rows = append(result11.Rows, row)
	}

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select .*", result11)
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// select.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		{
			query := "select /*backup*/ * from test.t1"
			qr, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
			want := 60510
			got := int(qr.RowsAffected)
			assert.Equal(t, want, got)
		}
		{ // select * from test.t1 t1 as ...;
			query := "select * from test.t1 as aliaseTable"
			qr, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
			want := 60510
			got := int(qr.RowsAffected)
			assert.Equal(t, want, got)
		}
		{ // select id from t1 as ...;
			query := "select /*backup*/ * from test.t1 as aliaseTable"
			qr, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
			want := 60510
			got := int(qr.RowsAffected)
			assert.Equal(t, want, got)
		}
		{ // select 1 from dual
			query := "select 1 from dual"
			qr, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
			want := 2017
			got := int(qr.RowsAffected)
			assert.Equal(t, want, got)
		}
		{ // select 1
			query := "select 1"
			qr, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
			want := 2017
			got := int(qr.RowsAffected)
			assert.Equal(t, want, got)
		}
		{ // select @@version_comment limit 1 [from] [dual]
			query := "select @@version_comment limit 1"
			qr, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
			want := 2017
			got := int(qr.RowsAffected)
			assert.Equal(t, want, got)
		}
	}

	//select from `subquery` error
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "select id from (select * from test.t1) as aliaseTable"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
		want := "unsupported: subqueries.in.select (errno 1105) (sqlstate HY000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
	// select .* from dual  error
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		fakedbs.AddQueryErrorPattern("select .*", errors.New("mock.mysql.select.from.dual.error"))
		{ // ERROR 1054 (42S22): Unknown column 'a' in 'field list'
			query := "select a from dual"
			_, err := client.FetchAll(query, -1)
			want := "mock.mysql.select.from.dual.error (errno 1105) (sqlstate HY000)"
			got := err.Error()
			assert.Equal(t, want, got)
		}
		{
			query := "select /*backup*/ a from test.dual"
			_, err := client.FetchAll(query, -1)
			want := "Table 'dual' doesn't exist (errno 1146) (sqlstate 42S02)"
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}

// Proxy with backup stream fetch.
func TestProxyQueryStreamWithBackup(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	result11 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: make([][]sqltypes.Value, 0, 256)}

	for i := 0; i < 2017; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("11")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("1nice name")),
		}
		result11.Rows = append(result11.Rows, row)
	}

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select .*", result11)
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// select.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		{
			query := "select /*backup*/ * from test.t1"
			qr, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
			want := 60510
			got := int(qr.RowsAffected)
			assert.Equal(t, want, got)
		}
		{ // select * from test.t1 t1 as ...;
			query := "select * from test.t1 as aliaseTable"
			qr, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
			want := 60510
			got := int(qr.RowsAffected)
			assert.Equal(t, want, got)
		}
		{ // select id from t1 as ...;
			query := "select /*backup*/ * from test.t1 as aliaseTable"
			qr, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
			want := 60510
			got := int(qr.RowsAffected)
			assert.Equal(t, want, got)
		}
		{ // select 1 from dual
			query := "select 1 from dual"
			qr, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
			want := 2017
			got := int(qr.RowsAffected)
			assert.Equal(t, want, got)
		}
		{ // select 1
			query := "select 1"
			qr, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
			want := 2017
			got := int(qr.RowsAffected)
			assert.Equal(t, want, got)
		}
		{ // select @@version_comment limit 1 [from] [dual]
			query := "select @@version_comment limit 1"
			qr, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
			want := 2017
			got := int(qr.RowsAffected)
			assert.Equal(t, want, got)
		}
	}

	// select .*  from dual error
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		fakedbs.AddQueryErrorPattern("select .*", errors.New("mock.mysql.select.from.dual.error"))
		{ // ERROR 1054 (42S22): Unknown column 'a' in 'field list'
			query := "select a from dual"
			_, err := client.FetchAll(query, -1)
			want := "mock.mysql.select.from.dual.error (errno 1105) (sqlstate HY000)"
			got := err.Error()
			assert.Equal(t, want, got)
		}
		{
			query := "select a from dual as aliasTable"
			_, err := client.FetchAll(query, -1)
			want := "mock.mysql.select.from.dual.error (errno 1105) (sqlstate HY000)"
			got := err.Error()
			assert.Equal(t, want, got)
		}
		{
			query := "select a from test.t1 as aliasTable"
			_, err := client.FetchAll(query, -1)
			want := "mock.mysql.select.from.dual.error (errno 1105) (sqlstate HY000)"
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}

// Test with long query time
func TestLongQuery(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()

	// set longQueryTime = 0s
	proxy.SetLongQueryTime(0)
	address := proxy.Address()
	client, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)
	defer client.Close()

	querys := []string{
		"select 1 from dual",
	}
	querysError := []string{
		"select a a from dual",
	}

	// fakedbs: add a query and returns the expected result without no delay
	{
		fakedbs.AddQueryPattern("select 1 from dual", &sqltypes.Result{})
	}

	{
		// long query success
		{
			for _, query := range querys {
				_, err = client.FetchAll(query, -1)
				assert.Nil(t, err)
			}
		}
		// long query failed
		{
			for _, query := range querysError {
				_, err = client.FetchAll(query, -1)
				assert.NotNil(t, err)
			}
		}
	}
}

// Test with long query time
func TestLongQuery2(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()

	// set longQueryTime = 5s
	proxy.SetLongQueryTime(5)
	address := proxy.Address()
	client, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)
	defer client.Close()

	querys := []string{
		"select 1 from dual",
	}
	querysError := []string{
		"select a a from dual",
	}
	// fakedbs: add a query and returns the expected result returned by delay 6s
	{
		fakedbs.AddQueryDelay("select 1 from dual", &sqltypes.Result{}, 6*1000)
	}

	{
		// long query success
		{
			for _, query := range querys {
				_, err = client.FetchAll(query, -1)
				assert.Nil(t, err)
			}
		}
		// long query failed
		{
			for _, query := range querysError {
				_, err = client.FetchAll(query, -1)
				assert.NotNil(t, err)
			}
		}
	}
}
