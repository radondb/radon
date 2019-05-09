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
	"sync"
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

func TestConnectionExecuteThreadSafe(t *testing.T) {
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

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := conn.ExecuteWithLimits("SELECT2", 100, 100)
			assert.NotNil(t, err)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := conn.ExecuteWithLimits("SELECT2", 100, 100)
			assert.NotNil(t, err)
		}()
		wg.Wait()
	}
}

func TestTruncateQueryLog(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))
	var querys []string
	var queryLogMaxLen = 30
	querys = []string{
		"select * from a where i>1 and jjjjjjjjjjjjjjjjjjjjjjjj>1",
		"select * from a where i>1",
		"",
		"\n\n\t",
	}

	for _, query := range querys {
		if len(query) > queryLogMaxLen {
			query = query[:queryLogMaxLen]
			assert.EqualValues(t, queryLogMaxLen, len(query))
		}
		log.Debug("execute[%s].len[%d]", query, len(query))
	}
}
