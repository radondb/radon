/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/xlog"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func TestServer(t *testing.T) {
	result1 := &sqltypes.Result{
		RowsAffected: 3,
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "extra",
				Type: querypb.Type_NULL_TYPE,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("10")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name")),
				sqltypes.NULL,
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("20")),
				sqltypes.NULL,
				sqltypes.NULL,
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("30")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("")),
				sqltypes.NULL,
			},
		},
	}
	result2 := &sqltypes.Result{}

	log := xlog.NewStdLog(xlog.Level(xlog.ERROR))
	th := NewTestHandler(log)
	svr, err := MockMysqlServer(log, th)
	assert.Nil(t, err)
	defer svr.Close()
	address := svr.Addr()

	// query
	{
		client, err := NewConn("mock", "mock", address, "test", "")
		assert.Nil(t, err)
		defer client.Close()

		th.AddQuery("SELECT1", result1)
		_, err = client.Query("SELECT1")
		assert.Nil(t, err)
	}

	// query1
	{
		client, err := NewConn("mock", "mock", address, "test", "")
		assert.Nil(t, err)

		th.AddQuery("SELECT2", result2)
		_, err = client.Query("SELECT2")
		assert.Nil(t, err)
		client.Close()
	}

	// exec
	{
		client, err := NewConn("mock", "mock", address, "test", "")
		assert.Nil(t, err)
		defer client.Close()

		th.AddQuery("SELECT1", result1)
		err = client.Exec("SELECT1")
		assert.Nil(t, err)
	}

	// fetch all
	{
		client, err := NewConn("mock", "mock", address, "test", "")
		assert.Nil(t, err)
		defer client.Close()

		th.AddQuery("SELECT1", result1)
		r, err := client.FetchAll("SELECT1", -1)
		assert.Nil(t, err)
		want := result1.Copy()
		got := r
		assert.Equal(t, want.Rows, got.Rows)
	}

	// fetch one
	{
		client, err := NewConn("mock", "mock", address, "test", "")
		assert.Nil(t, err)

		th.AddQuery("SELECT1", result1)
		r, err := client.FetchAll("SELECT1", 1)
		assert.Nil(t, err)
		defer client.Close()

		want := 1
		got := len(r.Rows)
		assert.Equal(t, want, got)
	}

	// error
	{
		client, err := NewConn("mock", "mock", address, "test", "")
		assert.Nil(t, err)
		defer client.Close()

		sqlErr := sqldb.NewSQLError(sqldb.ER_UNKNOWN_ERROR, "query.error")
		th.AddQueryError("ERROR1", sqlErr)
		err = client.Exec("ERROR1")
		assert.NotNil(t, err)
		want := "query.error (errno 1105) (sqlstate HY000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// panic
	{
		client, err := NewConn("mock", "mock", address, "test", "")
		assert.Nil(t, err)
		defer client.Close()

		th.AddQueryPanic("PANIC")
		client.Exec("PANIC")

		want := true
		got := client.Closed()
		assert.Equal(t, want, got)
	}

	// ping
	{
		client, err := NewConn("mock", "mock", address, "test", "")
		assert.Nil(t, err)
		err = client.Ping()
		assert.Nil(t, err)
	}

	// init db
	{
		client, err := NewConn("mock", "mock", address, "test", "")
		assert.Nil(t, err)
		err = client.InitDB("test")
		assert.Nil(t, err)
	}

	// auth denied
	{
		_, err := NewConn("mockx", "mock", address, "test", "")
		want := "Access denied for user 'mockx' (errno 1045) (sqlstate 28000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestServerSessionClose(t *testing.T) {
	result2 := &sqltypes.Result{}

	log := xlog.NewStdLog(xlog.Level(xlog.ERROR))
	th := NewTestHandler(log)
	svr, err := MockMysqlServer(log, th)
	assert.Nil(t, err)
	address := svr.Addr()

	{
		// create session 1
		client1, err := NewConn("mock", "mock", address, "test", "")
		assert.Nil(t, err)

		th.AddQuery("SELECT2", result2)
		r, err := client1.FetchAll("SELECT2", -1)
		assert.Nil(t, err)
		assert.Equal(t, result2, r)

		// kill session 1
		client2, err := NewConn("mock", "mock", address, "test", "")
		assert.Nil(t, err)
		_, err = client2.Query("KILL 1")
		assert.Nil(t, err)
	}
}

func TestServerComInitDB(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	th := NewTestHandler(log)
	svr, err := MockMysqlServer(log, th)
	assert.Nil(t, err)
	defer svr.Close()
	address := svr.Addr()

	// query
	{
		_, err := NewConn("mock", "mock", address, "xxtest", "")
		want := "mock.cominit.db.error: unkonw database[xxtest] (errno 1105) (sqlstate HY000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestServerUnsupportedCommand(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.ERROR))
	th := NewTestHandler(log)
	svr, err := MockMysqlServer(log, th)
	assert.Nil(t, err)
	defer svr.Close()
	address := svr.Addr()

	// query
	{
		client, err := NewConn("mock", "mock", address, "", "")
		assert.Nil(t, err)
		defer client.Close()
		err = client.Command(sqldb.COM_SLEEP)
		want := "command handling not implemented yet: COM_SLEEP (errno 1105) (sqlstate HY000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}
