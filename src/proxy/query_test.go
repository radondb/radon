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
		query := "select /*backup*/ * from test.t1"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := 60510
		got := int(qr.RowsAffected)
		assert.Equal(t, want, got)
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
