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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestClient(t *testing.T) {
	result2 := &sqltypes.Result{
		RowsAffected: 123,
		InsertID:     123456789,
	}

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

		// connection ID
		assert.Equal(t, uint32(1), client.ConnectionID())

		th.AddQuery("SELECT2", result2)
		rows, err := client.Query("SELECT2")
		assert.Nil(t, err)

		assert.Equal(t, uint64(123), rows.RowsAffected())
		assert.Equal(t, uint64(123456789), rows.LastInsertID())
	}
}

func TestClientClosed(t *testing.T) {
	result2 := &sqltypes.Result{}

	log := xlog.NewStdLog(xlog.Level(xlog.ERROR))
	th := NewTestHandler(log)
	svr, err := MockMysqlServer(log, th)
	assert.Nil(t, err)
	defer svr.Close()
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

		// check client1 connection
		err = client1.Ping()
		assert.NotNil(t, err)
		want := true
		got := client1.Closed()
		assert.Equal(t, want, got)
	}
}

func TestClientFetchAllWithFunc(t *testing.T) {
	result1 := &sqltypes.Result{
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
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("10")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("20")),
				sqltypes.NULL,
			},
		},
	}

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

		th.AddQuery("SELECT2", result1)
		checkFunc := func(rows Rows) error {
			if rows.Bytes() > 2 {
				return errors.New("client.checkFunc.error")
			}
			return nil
		}
		_, err = client.FetchAllWithFunc("SELECT2", -1, checkFunc)
		want := "client.checkFunc.error"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestClientStream(t *testing.T) {
	want := &sqltypes.Result{
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
		want.Rows = append(want.Rows, row)
	}

	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))
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

		th.AddQueryStream("SELECT2", want)
		rows, err := client.Query("SELECT2")
		assert.Nil(t, err)

		got := &sqltypes.Result{
			Fields: rows.Fields(),
			Rows:   make([][]sqltypes.Value, 0, 256)}

		for rows.Next() {
			row, err := rows.RowValues()
			assert.Nil(t, err)
			got.Rows = append(got.Rows, row)
		}
		assert.Equal(t, want, got)
	}
}

func TestMock(t *testing.T) {
	result1 := &sqltypes.Result{
		RowsAffected: 123,
		InsertID:     123456789,
	}
	result2 := &sqltypes.Result{
		RowsAffected: 123,
		InsertID:     123456789,
	}

	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))
	th := NewTestHandler(log)
	svr, err := MockMysqlServer(log, th)
	assert.Nil(t, err)
	defer svr.Close()
	address := svr.Addr()

	{
		th.AddQuery("SELECT2", result2)

		client, err := NewConn("mock", "mock", address, "test", "")
		assert.Nil(t, err)
		defer client.Close()

		// connection ID
		assert.Equal(t, uint32(1), client.ConnectionID())

		rows, err := client.Query("SELECT2")
		assert.Nil(t, err)

		assert.Equal(t, uint64(123), rows.RowsAffected())
		assert.Equal(t, uint64(123456789), rows.LastInsertID())
	}

	{
		th.AddQueryPattern("SELECT3 .*", result2)

		client, err := NewConn("mock", "mock", address, "test", "")
		assert.Nil(t, err)
		defer client.Close()

		_, err = client.Query("SELECT3 * from t1")
		assert.Nil(t, err)
	}

	{
		th.AddQueryErrorPattern("SELECT4 .*", errors.New("select4.mock.error"))

		client, err := NewConn("mock", "mock", address, "test", "")
		assert.Nil(t, err)
		defer client.Close()

		_, err = client.Query("SELECT4 * from t1")
		assert.NotNil(t, err)
	}

	{
		th.AddQueryDelay("SELECT5", result2, 10)

		client, err := NewConn("mock", "mock", address, "test", "")
		assert.Nil(t, err)
		defer client.Close()

		_, err = client.Query("SELECT5")
		assert.Nil(t, err)
	}

	{
		th.AddQuerys("s6", result1, result2)

		client, err := NewConn("mock", "mock", address, "test", "")
		assert.Nil(t, err)
		defer client.Close()

		_, err = client.Query("s6")
		assert.Nil(t, err)
	}

	// Query num.
	{
		got := th.GetQueryCalledNum("SELECT2")
		want := 1
		assert.Equal(t, want, got)
	}

	th.ResetPatternErrors()
	th.ResetErrors()
	th.ResetAll()
}
