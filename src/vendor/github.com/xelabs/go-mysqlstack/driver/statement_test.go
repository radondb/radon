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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func TestStatement(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))
	th := NewTestHandler(log)
	svr, err := MockMysqlServer(log, th)
	assert.Nil(t, err)
	defer svr.Close()
	address := svr.Addr()

	result1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "a",
				Type: sqltypes.Int32,
			},
			{
				Name: "b",
				Type: sqltypes.VarChar,
			},
			{
				Name: "c",
				Type: sqltypes.Datetime,
			},
			{
				Name: "d",
				Type: sqltypes.Time,
			},
			{
				Name: "e",
				Type: sqltypes.VarChar,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.Int32, []byte("10")),
				sqltypes.MakeTrusted(sqltypes.VarChar, []byte("xx10xx")),
				sqltypes.MakeTrusted(sqltypes.Datetime, []byte(time.Now().Format("2006-01-02 15:04:05"))),
				sqltypes.MakeTrusted(sqltypes.Time, []byte("15:04:05")),
				sqltypes.MakeTrusted(sqltypes.VarChar, nil),
			},
		},
	}
	result2 := &sqltypes.Result{}
	th.AddQueryPattern("drop table if .*", result2)
	th.AddQueryPattern("create table if .*", result2)
	th.AddQueryPattern("insert .*", result2)
	th.AddQueryPattern("select .*", result1)

	// query
	{
		client, err := NewConn("mock", "mock", address, "test", "")
		//client, err := NewConn("root", "", "127.0.0.1:3307", "test", "")
		assert.Nil(t, err)
		defer client.Close()

		query := "drop table if exists t1"
		err = client.Exec(query)
		assert.Nil(t, err)

		query = "create table if not exists t1 (a int, b varchar(20), c datetime, d time, e varchar(20))"
		err = client.Exec(query)
		assert.Nil(t, err)

		// Prepare Insert.
		{
			query = "insert into t1(a, b, c, d, e) values(?,?,?,?,?)"
			stmt, err := client.ComStatementPrepare(query)
			assert.Nil(t, err)
			log.Debug("stmt:%+v", stmt)

			params := []sqltypes.Value{
				sqltypes.NewInt32(11),
				sqltypes.NewVarChar("xx10xx"),
				sqltypes.MakeTrusted(sqltypes.Datetime, []byte(time.Now().Format("2006-01-02 15:04:05"))),
				sqltypes.MakeTrusted(sqltypes.Time, []byte("15:04:05")),
				sqltypes.MakeTrusted(sqltypes.VarChar, nil),
			}
			err = stmt.ComStatementExecute(params)
			assert.Nil(t, err)
			stmt.ComStatementClose()
		}

		// Normal Select int.
		{
			query = "select * from t1 where a=10"
			qr, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
			log.Debug("normal:%+v", qr)
		}

		{
			query = "select * from t1 where a=10"
			qr, err := client.FetchAll(query, -1)
			assert.Nil(t, err)
			log.Debug("normal:%+v", qr)
		}

		// Prepare Select int.
		{
			query = "select * from t1 where a=?"
			stmt, err := client.ComStatementPrepare(query)
			assert.Nil(t, err)
			assert.NotNil(t, stmt)
			log.Debug("stmt:%+v", stmt)

			params := []sqltypes.Value{
				sqltypes.NewInt32(11),
			}
			qr, err := stmt.ComStatementQuery(params)
			assert.Nil(t, err)
			log.Debug("%+v", qr)
			stmt.ComStatementClose()
		}

		// Prepare Select int.
		{
			query = "select * from t1 where a=?"
			stmt, err := client.ComStatementPrepare(query)
			assert.Nil(t, err)
			log.Debug("stmt:%+v", stmt)

			params := []sqltypes.Value{
				sqltypes.NewInt32(11),
			}
			qr, err := stmt.ComStatementQuery(params)
			assert.Nil(t, err)
			log.Debug("%+v", qr)
			stmt.ComStatementClose()
		}

		// Prepare Select time.
		{
			query = "select a,b,c,d,e from t1 where c=?"
			stmt, err := client.ComStatementPrepare(query)
			assert.Nil(t, err)
			log.Debug("stmt:%+v", stmt)

			params := []sqltypes.Value{
				sqltypes.MakeTrusted(sqltypes.Datetime, []byte(time.Now().Format("2006-01-02 15:04:05"))),
			}
			qr, err := stmt.ComStatementQuery(params)
			assert.Nil(t, err)
			log.Debug("%+v", qr)
			stmt.ComStatementReset()
			stmt.ComStatementClose()
		}
	}
}
