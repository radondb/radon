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

func TestProxyShowDatabases(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show databases", &sqltypes.Result{})
	}

	// show databases.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show databases"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
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

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQuerys("show create table test.t1_0000", r1)
		fakedbs.AddQuerys("show create table test.t1", r1)
		fakedbs.AddQuerys("show create table t1", r1)
		fakedbs.AddQuerys("show create table MYSQL.t1", r1)
		fakedbs.AddQuerys("show create table xxx.t1", r1)
		fakedbs.AddQuerys("show create table test.g_t1", r2)
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
		want := "[t1 create table t1\n/*!50100 PARTITION BY HASH (id) */]"
		got := fmt.Sprintf("%+v", qr.Rows[0])
		assert.Equal(t, want, got)
	}

	// create test table with global.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create table g_t1(id int, b int)"
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
		want := "[g_t1 create table g_t1]"
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

	log := xlog.NewStdLog(xlog.Level(xlog.ERROR))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show create .*", r1)
		fakedbs.AddQueryPattern("show columns .*", r2)
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
		want := "[t1 create table t1\n/*!50100 PARTITION BY HASH (id) */]"
		got := fmt.Sprintf("%+v", qr.Rows[0])
		assert.Equal(t, want, got)
	}

	// show columns from table
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

	// show columns from table err(database is empty)
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show columns from t1"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// show columns from table err(sys database:MYSQL)
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show columns from MYSQL.t1"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// show columns from table err(database not exist)
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "show columns from xxx.t1"
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
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select * .*", &sqltypes.Result{})
		fakedbs.AddQueryDelay("select * from test.t1_0002", &sqltypes.Result{}, 3000)
		fakedbs.AddQueryDelay("select * from test.t1_0004", &sqltypes.Result{}, 3000)
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

func TestProxyShowStatus(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select * .*", &sqltypes.Result{})
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
		want := `{"max-connections":1024,"max-result-size":1073741824,"ddl-timeout":36000000,"query-timeout":300000,"twopc-enable":false,"allow-ip":null,"audit-log-mode":"N","readonly":false,"throttle":0}`
		got := string(qr.Rows[1][1].Raw())
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
