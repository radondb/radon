/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	showDatabasesResult = &sqltypes.Result{
		RowsAffected: 2,
		Fields: []*querypb.Field{
			{
				Name: "Database",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test1")),
			},
		},
	}

	showTableStatusResult1 = &sqltypes.Result{
		RowsAffected: 13,
		Fields: []*querypb.Field{
			{
				Name: "Name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Engine",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Version",
				Type: querypb.Type_UINT64,
			},
			{
				Name: "Row_format",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Rows",
				Type: querypb.Type_UINT64,
			},
			{
				Name: "Avg_row_length",
				Type: querypb.Type_UINT64,
			},
			{
				Name: "Data_length",
				Type: querypb.Type_UINT64,
			},
			{
				Name: "Max_data_length",
				Type: querypb.Type_UINT64,
			},
			{
				Name: "Index_length",
				Type: querypb.Type_UINT64,
			},
			{
				Name: "Data_free",
				Type: querypb.Type_UINT64,
			},
			{
				Name: "Auto_increment",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Create_time",
				Type: querypb.Type_DATETIME,
			},
			{
				Name: "Update_time",
				Type: querypb.Type_DATETIME,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("a_0000")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("InnoDB")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("10")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("Dynamic")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("2")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("8192")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("16384")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("0")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("2")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("0")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("NULL")),
				sqltypes.MakeTrusted(querypb.Type_DATETIME, []byte("2019-01-22 08:31:47")),
				sqltypes.MakeTrusted(querypb.Type_DATETIME, []byte("2019-01-22 10:33:47")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("a_0001")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("InnoDB")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("10")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("Dynamic")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("3")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("16384")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("16384")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("0")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("2")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("0")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("NULL")),
				sqltypes.MakeTrusted(querypb.Type_DATETIME, []byte("2019-01-23 08:31:47")),
				sqltypes.MakeTrusted(querypb.Type_DATETIME, []byte("2019-01-23 10:33:47")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("c")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("InnoDB")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("10")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("Dynamic")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("2")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("16384")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("16384")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("0")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("2")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("0")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("NULL")),
				sqltypes.MakeTrusted(querypb.Type_DATETIME, []byte("2019-01-22 08:31:47")),
				sqltypes.MakeTrusted(querypb.Type_DATETIME, []byte("2019-01-22 10:33:47")),
			},
		},
	}

	showTableStatusResult2 = &sqltypes.Result{
		RowsAffected: 2,
		Fields: []*querypb.Field{
			{
				Name: "Name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Engine",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Version",
				Type: querypb.Type_UINT64,
			},
			{
				Name: "Row_format",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Rows",
				Type: querypb.Type_UINT64,
			},
			{
				Name: "Avg_row_length",
				Type: querypb.Type_UINT64,
			},
			{
				Name: "Data_length",
				Type: querypb.Type_UINT64,
			},
			{
				Name: "Max_data_length",
				Type: querypb.Type_UINT64,
			},
			{
				Name: "Index_length",
				Type: querypb.Type_UINT64,
			},
			{
				Name: "Data_free",
				Type: querypb.Type_UINT64,
			},
			{
				Name: "Auto_increment",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Create_time",
				Type: querypb.Type_DATETIME,
			},
			{
				Name: "Update_time",
				Type: querypb.Type_DATETIME,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("a")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("InnoDB")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("10")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("Dynamic")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("25")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("16384")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("163840")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("0")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("20")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("0")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("NULL")),
				sqltypes.MakeTrusted(querypb.Type_DATETIME, []byte("2019-01-22 08:31:47")),
				sqltypes.MakeTrusted(querypb.Type_DATETIME, []byte("2019-01-23 10:33:47")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("c")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("InnoDB")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("10")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("Dynamic")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("2")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("16384")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("16384")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("0")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("2")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("0")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("NULL")),
				sqltypes.MakeTrusted(querypb.Type_DATETIME, []byte("2019-01-22 08:31:47")),
				sqltypes.MakeTrusted(querypb.Type_DATETIME, []byte("2019-01-22 10:33:47")),
			},
		},
	}
)

func TestProxyShowDatabases(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show databases", showDatabasesResult)
	}

	// show databases.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show databases"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		// the user with super privilege can see all databases.
		assert.EqualValues(t, 2, len(qr.Rows))
	}
}

func TestProxyShowDatabasesPrivilege(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxyPrivilegeNotSuper(log, MockDefaultConfig())
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show databases", showDatabasesResult)
	}

	// show databases.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show databases"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		assert.EqualValues(t, 2, len(qr.Rows))
	}
}

func TestProxyShowDatabasesPrivilegeDB(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxyPrivilegeN(log, MockDefaultConfig())
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show databases", showDatabasesResult)
	}

	// show databases.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show databases"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		assert.EqualValues(t, 1, len(qr.Rows))
	}
}

func TestProxyShowEngines(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show engines", &sqltypes.Result{})
	}

	// show databases.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show engines"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
}

func TestProxyShowCreateDatabase(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show create database xx", &sqltypes.Result{})
	}

	// show databases.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show create database xx"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
}

func TestProxyShowTables(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show .*", &sqltypes.Result{})
	}

	// show tables.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show tables from test"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// show tables like.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show tables from test like '%user%'"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// show tables error with null database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show tables"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// show tables error with sys database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show tables from MYSQL"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}
}

func TestProxyShowTableStatus(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show table status .*", showTableStatusResult1)
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
	}
	// create database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create database test"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// show tables.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show table status"
		got, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := showTableStatusResult2
		assert.Equal(t, want.Rows, got.Rows)
		assert.Equal(t, want.RowsAffected, got.RowsAffected)
	}

	// show tables error with null database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show table status"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// show tables error with sys database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show table status from MYSQL"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}
}

func TestProxyShowCreateTable(t *testing.T) {
	r1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "create table",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1_0000")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("create table t1_0000")),
			},
		},
	}

	lr := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "create table",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("l_0000")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("create table l_0000")),
			},
		},
	}

	r2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "create table",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("g_t1")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("create table g_t1")),
			},
		},
	}

	r3 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "create table",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("s_t1")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("create table s_t1")),
			},
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()
	backends := fakedbs.BackendConfs()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQuerys("show create table test.t1_0000", r1)
		fakedbs.AddQuerys("show create table test.l_0000", lr)
		fakedbs.AddQuerys("show create table test.t1", r1)
		fakedbs.AddQuerys("show create table t1", r1)
		fakedbs.AddQuerys("show create table MYSQL.t1", r1)
		fakedbs.AddQuerys("show create table xxx.t1", r1)
		fakedbs.AddQuerys("show create table test.g_t1", r2)
		fakedbs.AddQuerys("show create table test.s_t1", r3)
	}

	// create database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create database test"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// create test table with hash.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create table t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
		client.Quit()
	}

	// show create table which shardType is hash.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show create table test.t1"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "[t1 create table t1\n/*!50100 PARTITION BY HASH(id) */]"
		got := fmt.Sprintf("%+v", qr.Rows[0])
		assert.Equal(t, want, got)
	}

	// create test table with list.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		b1 := backends[0].Name
		query := fmt.Sprintf("create table l(id int, b int) partition by list(id)(partition %s values in (1,2));", b1)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
		client.Quit()
	}

	// show create table which shardType is list.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show create table test.l"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "[l create table l\n/*!50100 PARTITION BY LIST(id) */]"
		got := fmt.Sprintf("%+v", qr.Rows[0])
		assert.Equal(t, want, got)
	}

	// create test table with global.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create table g_t1(id int, b int) global"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
		client.Quit()
	}

	// show create table which shardType is global.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show create table test.g_t1"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "[g_t1 create table g_t1\n/*!GLOBAL*/]"
		got := fmt.Sprintf("%+v", qr.Rows[0])
		assert.Equal(t, want, got)
	}

	// create test table with single.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create table s_t1(id int, b int) single"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
		client.Quit()
	}

	// show create table which shardType is single.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show create table test.s_t1"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "[s_t1 create table s_t1\n/*!SINGLE*/]"
		got := fmt.Sprintf("%+v", qr.Rows[0])
		assert.Equal(t, want, got)
	}

	// show create table err(no database).
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show create table t1"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// show create table err(system database).
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show create table MYSQL.t1"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// show create table err(database not exist).
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show create table xxx.t1"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}
}

func TestProxyShowColumns(t *testing.T) {
	r1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "create table",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1_0000")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("create table t1_0000")),
			},
		},
	}

	r2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Field",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Type",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Null",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Key",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Default",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Extra",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("col_a")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("int(11)")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("YES")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("NULL")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("NULL")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("NULL")),
			},
		},
	}

	r3 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Field",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Type",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Null",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Key",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Default",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Extra",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Privileges",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Comment",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("col_a")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("int(11)")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("YES")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("PRI")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("NULL")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("NULL")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("select,insert,update,references")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("NULL")),
			},
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show create .*", r1)
		fakedbs.AddQueryPattern("show columns .*", r2)
		fakedbs.AddQueryPattern("show full columns .*", r3)
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
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create table t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
		client.Quit()
	}

	// show create table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show create table test.t1"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "[t1 create table t1\n/*!50100 PARTITION BY HASH(id) */]"
		got := fmt.Sprintf("%+v", qr.Rows[0])
		assert.Equal(t, want, got)
	}

	// show columns from table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show columns from test.t1"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "[col_a int(11) YES NULL NULL NULL]"
		got := fmt.Sprintf("%+v", qr.Rows[0])
		assert.Equal(t, want, got)
	}

	// show columns from table where.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show columns from test.t1 where `Null` = 'YES'"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "[col_a int(11) YES NULL NULL NULL]"
		got := fmt.Sprintf("%+v", qr.Rows[0])
		assert.Equal(t, want, got)
	}

	// show full columns from table(use database).
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show full columns from t1"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "[col_a int(11) YES PRI NULL NULL select,insert,update,references NULL]"
		got := fmt.Sprintf("%+v", qr.Rows[0])
		assert.Equal(t, want, got)
	}

	// show full columns from table where.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show full columns from test.t1 where `Key` = 'PRI'"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "[col_a int(11) YES PRI NULL NULL select,insert,update,references NULL]"
		got := fmt.Sprintf("%+v", qr.Rows[0])
		assert.Equal(t, want, got)
	}

	// show full fields from table like.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show full fields from test.t1 like '%a'"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "[col_a int(11) YES PRI NULL NULL select,insert,update,references NULL]"
		got := fmt.Sprintf("%+v", qr.Rows[0])
		assert.Equal(t, want, got)
	}

	// show columns from table err(database is empty).
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show columns from t1"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// show columns from table err(sys database:MYSQL).
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show columns from MYSQL.t1"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// show columns from table err(database not exist).
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show columns from xxx.t1"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}
}

func TestProxyShowIndex(t *testing.T) {
	r := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Non_unique",
				Type: querypb.Type_INT64,
			},
			{
				Name: "Key_name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Seq_in_index",
				Type: querypb.Type_INT64,
			},
			{
				Name: "Column_name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Collation",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Cardinality",
				Type: querypb.Type_INT64,
			},
			{
				Name: "Sub_part",
				Type: querypb.Type_INT64,
			},
			{
				Name: "Packed",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Null",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Index_type",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Comment",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Index_comment",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1_0000")),
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("0")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("PRIMARY")),
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("1")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("a")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("A")),
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("0")),
				sqltypes.MakeTrusted(querypb.Type_NULL_TYPE, nil),
				sqltypes.MakeTrusted(querypb.Type_NULL_TYPE, nil),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("BTREE")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("")),
			},
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show index .*", r)
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
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create table t1(id int primary key, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
		client.Quit()
	}

	// show index from table.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show index from t1"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "[t1 0 PRIMARY 1 a A 0    BTREE  ]"
		got := fmt.Sprintf("%+v", qr.Rows[0])
		assert.Equal(t, want, got)
	}

	// show indexes from table from database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show indexes from t1 from test"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "[t1 0 PRIMARY 1 a A 0    BTREE  ]"
		got := fmt.Sprintf("%+v", qr.Rows[0])
		assert.Equal(t, want, got)
	}

	// show keys from table where.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show keys from test.t1 where Key_name = 'PRIMARY'"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "[t1 0 PRIMARY 1 a A 0    BTREE  ]"
		got := fmt.Sprintf("%+v", qr.Rows[0])
		assert.Equal(t, want, got)
	}

	// show index from table err(database is empty).
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show index from t1"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// show index from table err(sys database:MYSQL).
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show index from user from mysql"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// show index from table err(database not exist).
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show index from t1 from xxx"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}
}

func TestProxyShowProcesslist(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, scleanup := MockProxy(log)
	defer scleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select * .*", &sqltypes.Result{})
		fakedbs.AddQueryDelay("select * from test.t1_0002", &sqltypes.Result{}, 3000)
		fakedbs.AddQueryDelay("select * from test.t1_0004", &sqltypes.Result{}, 3000)
		fakedbs.AddQueryPattern("XA * .*", &sqltypes.Result{})
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
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create table t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
		client.Quit()
	}

	var wg sync.WaitGroup
	var clients []driver.Conn
	nums := 10
	// long query.
	{
		for i := 0; i < nums; i++ {
			client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
			assert.Nil(t, err)
			wg.Add(1)
			go func(c driver.Conn) {
				defer wg.Done()
				query := "select * from t1"
				_, err = client.FetchAll(query, -1)
			}(client)
			clients = append(clients, client)
		}

		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		clients = append(clients, client)
		_ = clients
	}

	// show processlist.
	{
		time.Sleep(time.Second)
		show, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		_, err = show.FetchAll("show processlist", -1)
		assert.Nil(t, err)
	}

	// show processlist about the process in transaction.
	{
		proxy.SetTwoPC(true)
		clientTxn, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		_, err = clientTxn.FetchAll("begin", -1)
		assert.Nil(t, err)
		clients = append(clients, clientTxn)

		show, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		info, err := show.FetchAll("show processlist", -1)
		assert.Nil(t, err)
		// ios, the value is sometimes not equal.
		// assert.Equal(t, len(clients)+2, int(info.RowsAffected))
		log.Debug("%+v", info.Rows)

		_, err = clientTxn.FetchAll("commit", -1)
		assert.Nil(t, err)
		proxy.SetTwoPC(false)
	}

	// show queryz.
	{
		show, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		qr, err := show.FetchAll("show queryz", -1)
		assert.Nil(t, err)
		log.Info("%+v", qr.Rows)
	}

	// show txnz.
	{
		show, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		qr, err := show.FetchAll("show txnz", -1)
		assert.Nil(t, err)
		log.Info("%+v", qr.Rows)
	}
	wg.Wait()
}

func TestProxyShowProcesslistPrivilege(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxyPrivilegeUsers(log, MockDefaultConfig())
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select * .*", &sqltypes.Result{})
		fakedbs.AddQueryDelay("select * from test.t1_0002", &sqltypes.Result{}, 3000)
		fakedbs.AddQueryDelay("select * from test.t1_0004", &sqltypes.Result{}, 3000)
	}

	var wg sync.WaitGroup
	var clients []driver.Conn
	nums := 2
	// long query.
	{
		for i := 0; i < nums; i++ {
			client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
			assert.Nil(t, err)
			wg.Add(1)
			go func(c driver.Conn) {
				defer wg.Done()
				query := "select * from t1"
				_, err = client.FetchAll(query, -1)
			}(client)
			clients = append(clients, client)
		}
	}

	// show processlist.
	{
		time.Sleep(time.Second)
		show, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		_, err = show.FetchAll("show processlist", -1)
		assert.Nil(t, err)
		// Temporarily comment out, because of the ci environment, `assert` often fails.
		// assert.Equal(t, nums+1, int(qr.RowsAffected))
	}

	// show processlist.
	{
		time.Sleep(time.Second)
		show, err := driver.NewConn("mock1", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		qr, err := show.FetchAll("show processlist", -1)
		assert.Nil(t, err)
		assert.Equal(t, 1, int(qr.RowsAffected))
	}

	wg.Wait()
}

func TestProxyShowStatus(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select * .*", &sqltypes.Result{})
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
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create table t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
		client.Quit()
	}

	// show status.
	{
		show, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		qr, err := show.FetchAll("show status", -1)
		assert.Nil(t, err)
		want := `{"max-connections":1024,"max-result-size":1073741824,"max-join-rows":32768,"ddl-timeout":36000000,"query-timeout":300000,"twopc-enable":false,"allow-ip":null,"audit-log-mode":"N","readonly":false,"throttle":0}`
		got := string(qr.Rows[1][1].Raw())
		assert.Equal(t, want, got)
	}
}

func TestProxyShowStatusPrivilege(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxyPrivilegeN(log, MockDefaultConfig())
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
	}

	// show status.
	{
		show, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		_, err = show.FetchAll("show status", -1)
		assert.NotNil(t, err)
		want := fmt.Sprintf("Access denied; lacking super privilege for the operation (errno 1227) (sqlstate 42000)")
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestProxyShowVersions(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
	}

	// show versions.
	{
		show, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		qr, err := show.FetchAll("show versions", -1)
		assert.Nil(t, err)
		got := string(qr.Rows[0][0].Raw())
		assert.True(t, strings.Contains(got, "GoVersion"))
	}
}

func TestProxyShowWarnings(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	querys := []string{"show warnings", "show variables"}
	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		for _, query := range querys {
			fakedbs.AddQuery(query, &sqltypes.Result{})
		}
	}

	// show versions.
	{
		for _, query := range querys {
			show, err := driver.NewConn("mock", "mock", address, "test", "utf8")
			assert.Nil(t, err)
			qr, err := show.FetchAll(query, -1)
			assert.Nil(t, err)

			want := &sqltypes.Result{}
			assert.Equal(t, want, qr)
		}
	}
}

func TestProxyShowUnsupports(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
	}
	querys := []string{
		"show test",
	}

	// show test.
	{
		show, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		for _, query := range querys {
			_, err = show.FetchAll(query, -1)
			assert.NotNil(t, err)
			want := fmt.Sprintf("unsupported.query:%s (errno 1105) (sqlstate HY000)", query)
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}

func TestProxyShowQueryzPrivilege(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxyPrivilegeN(log, MockDefaultConfig())
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
	}

	// show queryz.
	{
		show, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		_, err = show.FetchAll("show queryz", -1)
		assert.NotNil(t, err)
		want := fmt.Sprintf("Access denied; lacking super privilege for the operation (errno 1227) (sqlstate 42000)")
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestProxyShowTxnzPrivilege(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxyPrivilegeN(log, MockDefaultConfig())
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
	}

	// show txnz.
	{
		show, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		_, err = show.FetchAll("show txnz", -1)
		assert.NotNil(t, err)
		want := fmt.Sprintf("Access denied; lacking super privilege for the operation (errno 1227) (sqlstate 42000)")
		got := err.Error()
		assert.Equal(t, want, got)
	}
}
