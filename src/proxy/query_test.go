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

// Proxy with backup
func TestProxyQueryStreamWithBackup(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	fakedbs, proxy, cleanup := MockProxyWithBackup(log)
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
			want := 64544
			got := int(qr.RowsAffected)
			assert.Equal(t, want, got)
		}
		{ // select * from test.t1 t1 as ...;
			query := "select * from test.t1 as aliaseTable"
			qr, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
			want := 64544
			got := int(qr.RowsAffected)
			assert.Equal(t, want, got)
		}
		{ // select id from t1 as ...;
			query := "select /*backup*/ * from test.t1 as aliaseTable"
			qr, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
			want := 64544
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

/*
func TestProxyQueryBench(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.ERROR))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
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

	// insert.
	{
		var wg sync.WaitGroup

		l := 1000
		threads := 64
		now := time.Now()
		for k := 0; k < threads; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				client, err := driver.NewConn("mock", "mock", address, "", "utf8")
				assert.Nil(t, err)
				for i := 0; i < l; i++ {
					query := "insert into test.t1(id, b) values(1,1)"
					_, err := client.FetchAll(query, -1)
					assert.Nil(t, err)
				}
			}()

		}
		wg.Wait()
		n := l * threads
		took := time.Since(now)
		fmt.Printf(" LOOP\t%v COST %v, avg:%v/s\n", n, took, (int64(n)/(took.Nanoseconds()/1e6))*1000)
	}
}
*/
