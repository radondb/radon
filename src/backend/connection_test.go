/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package backend

import (
	"errors"
	"testing"

	"fakedb"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"

	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestConnection(t *testing.T) {
	//defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedb := fakedb.New(log, 1)
	defer fakedb.Close()
	addr := fakedb.Addrs()[0]

	// Connection
	conn, cleanup := MockClient(log, addr)
	defer cleanup()

	// connection ID
	{
		want := uint32(1)
		got := conn.ID()
		assert.Equal(t, want, got)
	}

	// usedb
	{
		fakedb.AddQuery("USE MOCKDB", result2)
		err := conn.UseDB("MOCKDB")
		assert.Nil(t, err)
	}

	sqlErr := sqldb.NewSQLError(sqldb.ER_UNKNOWN_ERROR, "query.error")
	// usedb error
	{
		fakedb.AddQueryError("USE USEDBERROR", sqlErr)
		err := conn.UseDB("USEDBERROR")
		want := "query.error (errno 1105) (sqlstate HY000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
	// again
	{
		fakedb.AddQueryError("USE USEDBERROR", sqlErr)
		err := conn.UseDB("USEDBERROR")
		want := "query.error (errno 1105) (sqlstate HY000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// execute
	{
		fakedb.AddQuery("SELECT1", result1)
		r, err := conn.Execute("SELECT1")
		assert.Nil(t, err)
		assert.Equal(t, result1, r)
	}
}

func TestConnectionRecyle(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	// MySQL Server starts...
	fakedb := fakedb.New(log, 1)
	defer fakedb.Close()
	addr := fakedb.Addrs()[0]

	// Connection
	conn, cleanup := MockClient(log, addr)
	defer cleanup()

	// usedb
	{
		fakedb.AddQuery("USE MOCKDB", result2)
		err := conn.UseDB("MOCKDB")
		assert.Nil(t, err)
		conn.Recycle()
	}
}

func TestConnectionKill(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	// MySQL Server starts...
	fakedb := fakedb.New(log, 1)
	defer fakedb.Close()
	addr := fakedb.Addrs()[0]

	// Connection
	conn, cleanup := MockClient(log, addr)
	defer cleanup()

	// kill
	{
		err := conn.Kill("kill.you")
		assert.Nil(t, err)
	}

	// check
	{
		fakedb.AddQuery("USE MOCKDB", result2)
		err := conn.UseDB("MOCKDB")
		assert.NotNil(t, err)
	}
}

func TestConnectionKillError(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	// MySQL Server starts...
	fakedb := fakedb.New(log, 1)
	defer fakedb.Close()
	addr := fakedb.Addrs()[0]

	// Connection
	conn, cleanup := MockClient(log, addr)
	defer cleanup()

	// kill
	{
		query := "kill 1"
		fakedb.AddQueryError(query, errors.New("mock.kill.error"))
		err := conn.Kill("kill.you")
		want := "mock.kill.error (errno 1105) (sqlstate HY000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestConnectionExecuteTimeout(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	// MySQL Server starts...
	fakedb := fakedb.New(log, 1)
	defer fakedb.Close()

	config := fakedb.BackendConfs()[0]
	conn, cleanup := MockClientWithConfig(log, config)
	defer cleanup()
	// execute timeout
	{
		fakedb.AddQueryDelay("SELECT2", result2, 1000)
		_, err := conn.ExecuteWithLimits("SELECT2", 100, 100)
		assert.NotNil(t, err)
	}
}

func TestConnectionMemoryCheck(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	// MySQL Server starts...
	fakedb := fakedb.New(log, 1)
	defer fakedb.Close()

	config := fakedb.BackendConfs()[0]
	conn, cleanup := MockClientWithConfig(log, config)
	defer cleanup()
	{
		fakedb.AddQuery("SELECT2", result2)
		_, err := conn.ExecuteWithLimits("SELECT2", 0, 5)
		want := "Query execution was interrupted, max memory usage[5 bytes] exceeded"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestConnectionClosed(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	// MySQL Server starts...
	fakedb := fakedb.New(log, 1)
	defer fakedb.Close()
	addr := fakedb.Addrs()[0]

	// Connection
	conn, cleanup := MockClient(log, addr)
	defer cleanup()

	// Close the connection.
	{
		conn.Close()
		assert.True(t, conn.Closed())
	}

	// Execute querys on a closed connection.
	{
		_, err := conn.Execute("SELECT2")
		// error: write tcp 127.0.0.1:33686->127.0.0.1:5917: use of closed network connection
		assert.NotNil(t, err)
	}
}

/*
func TestConnectionRealServer(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	conf := &config.BackendConfig{
		Name:           "node1",
		Address:        "127.0.0.1:3304",
		User:           "root",
		Password:       "",
		DBName:         "",
		Charset:        "utf8",
		MaxConnections: 1024,
		MaxMemoryUsage: 1024 * 1024 * 1024,
		QueryTimeout:   20000,
	}

	pool := NewPool(log, mysqlStats, conf)
	conn := NewConnection(log, pool)
	if err := conn.Dial(); err == nil {
		defer conn.Close()

		// usedb
		{
			err := conn.UseDB("mysql")
			assert.Nil(t, err)
		}

		// create database
		{
			_, err := conn.Execute("create database if not exists test")
			assert.Nil(t, err)
		}

		// create table
		{
			_, err := conn.Execute("create table if not exists test.t1(a int)")
			assert.Nil(t, err)
		}

		// insert
		{
			r, err := conn.Execute("insert into test.t1 values(1),(2),(3)")
			assert.Nil(t, err)
			log.Debug("query:%+v", r)
		}

		// selset
		{
			N := 10000
			now := time.Now()
			for i := 0; i < N; i++ {
				conn.Execute("select * from test.t1")
			}
			took := time.Since(now)
			log.Debug(" LOOP\t%v COST %v, avg:%v/s", N, took, (int64(N)/(took.Nanoseconds()/1e6))*1000)
		}

		// selset
		{
			N := 10000
			now := time.Now()
			for i := 0; i < N; i++ {
				conn.Execute("select * from test.t1")
			}
			took := time.Since(now)
			log.Debug(" LOOP\t%v COST %v, avg:%v/s", N, took, (int64(N)/(took.Nanoseconds()/1e6))*1000)
		}

		// usedb
		{
			err := conn.UseDB("test")
			assert.Nil(t, err)
		}

		// selset
		{
			N := 10000
			now := time.Now()
			for i := 0; i < N; i++ {
				conn.Execute("select * from t1")
			}
			took := time.Since(now)
			log.Debug(" LOOP\t%v COST %v, avg:%v/s", N, took, (int64(N)/(took.Nanoseconds()/1e6))*1000)
		}
		log.Debug("--status:%s", mysqlStats.String())

		// mysql.user
		{
			_, err := conn.Execute("select * from mysql.user")
			assert.Nil(t, err)
		}

		// drop database
		{
			_, err := conn.Execute("drop database test")
			assert.Nil(t, err)
		}

		// kill
		{
			err := conn.Kill("killme")
			assert.Nil(t, err)
		}
	}
}
*/
