/*
 * go-mydumper
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package common

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestDumper(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	fakedbs := driver.NewTestHandler(log)
	server, err := driver.MockMysqlServer(log, fakedbs)
	assert.Nil(t, err)
	defer server.Close()
	address := server.Addr()

	selectResult := &sqltypes.Result{
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
				Name: "namei1",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "null",
				Type: querypb.Type_NULL_TYPE,
			},
			{
				Name: "decimal",
				Type: querypb.Type_DECIMAL,
			},
			{
				Name: "datetime",
				Type: querypb.Type_DATETIME,
			},
		},
		Rows: make([][]sqltypes.Value, 0, 256)}

	for i := 0; i < 201710; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("11")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("11\"xx\"")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("")),
			sqltypes.MakeTrusted(querypb.Type_NULL_TYPE, nil),
			sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("210.01")),
			sqltypes.NULL,
		}
		selectResult.Rows = append(selectResult.Rows, row)
	}

	schemaResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Create Table",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("CREATE TABLE `t1` (`a` int(11) DEFAULT NULL,`b` varchar(100) DEFAULT NULL) ENGINE=InnoDB")),
			},
		}}

	tablesResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Tables_in_test",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t2")),
			},
		}}

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show create table .*", schemaResult)
		fakedbs.AddQueryPattern("show tables from .*", tablesResult)
		fakedbs.AddQueryPattern("select .*", selectResult)
	}

	args := &Args{
		Database:      "test",
		Outdir:        "/tmp/dumpertest",
		User:          "mock",
		Password:      "mock",
		Address:       address,
		ChunksizeInMB: 1,
		Threads:       16,
		StmtSize:      10000,
		IntervalMs:    500,
	}

	os.RemoveAll(args.Outdir)
	if _, err := os.Stat(args.Outdir); os.IsNotExist(err) {
		x := os.MkdirAll(args.Outdir, 0777)
		AssertNil(x)
	}

	// Dumper.
	{
		Dumper(log, args)
	}
	dat, err := ioutil.ReadFile(args.Outdir + "/test.t1.00001.sql")
	assert.Nil(t, err)
	want := strings.Contains(string(dat), `(11,"11\"xx\"","",NULL,210.01,NULL)`)
	assert.True(t, want)
}
