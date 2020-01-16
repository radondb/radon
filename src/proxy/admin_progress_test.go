/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"testing"

	"github.com/radondb/shift/shift"
	shiftLog "github.com/radondb/shift/xlog"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestRadonProgress(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	router := proxy.Router()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("drop table .*", &sqltypes.Result{})
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
			"create table test.a(i int primary key) single",
		}
		for _, query := range querys {
			_, err = client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
	}

	// radon progress failed.
	{
		query := "radon progress test.a"
		_, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		// xx.xx not exist
		progress := NewProgress(log, router, "xx", "xx")
		_, err = progress.GetShiftProgressInfo()
		assert.NotNil(t, err)
	}

	// radon progress successfull.
	{
		expected := &sqltypes.Result{}
		expected.Fields = []*querypb.Field{
			{Name: "DumpProgressRate", Type: querypb.Type_VARCHAR},
			{Name: "DumpRemainTime", Type: querypb.Type_VARCHAR},
			{Name: "PositionBehinds", Type: querypb.Type_VARCHAR},
			{Name: "SynGTID", Type: querypb.Type_VARCHAR},
			{Name: "MasterGTID", Type: querypb.Type_VARCHAR},
			{Name: "MigrateStatus", Type: querypb.Type_VARCHAR},
		}
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("not start yet!")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("0")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("not start yet!")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("")),
		}
		expected.Rows = append(expected.Rows, row)

		// init progress file
		slog := shiftLog.NewStdLog(shiftLog.Level(shiftLog.ERROR))
		cfg := &shift.Config{
			FromDatabase: "test",
			FromTable:    "a",
		}
		shift := shift.NewShift(slog, cfg)
		shift.WriteShiftProgress()

		query := "radon progress test.a"
		_, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		progress := NewProgress(log, router, "test", "a")
		actual, err := progress.GetShiftProgressInfo()
		assert.Nil(t, err)
		assert.True(t, assert.ObjectsAreEqualValues(expected, actual))
	}
}
