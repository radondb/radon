/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"fakedb"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mydumper/src/common"
	"github.com/xelabs/go-mysqlstack/driver"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestDumperWithProxy(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_proxy_", log)
	defer os.RemoveAll(tmpDir)

	fakedbs, server, cleanup := MockProxy(log)
	defer cleanup()
	address := server.Address()

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
		},
		Rows: make([][]sqltypes.Value, 0, 256)}

	for i := 0; i < 2017; i++ {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("11")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("1nice name")),
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
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show create table .*", schemaResult)
		fakedbs.AddQuery("show tables from test", tablesResult)
		fakedbs.AddQueryPattern("select .*", selectResult)
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b varchar(100)) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)

		query = "create table test.t2(id int, b varchar(100)) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	args := &common.Args{
		Database:      "test",
		Outdir:        tmpDir,
		User:          "mock",
		Password:      "mock",
		Address:       address,
		ChunksizeInMB: 1,
		Threads:       16,
		StmtSize:      10000,
		IntervalMs:    1000,
	}

	os.RemoveAll(args.Outdir)
	x := os.MkdirAll(args.Outdir, 0777)
	common.AssertNil(x)
	common.Dumper(log, args)
}

func TestLoaderWithProxy(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_proxy_", log)
	defer os.RemoveAll(tmpDir)

	fakedbs, server, cleanup := MockProxy(log)
	defer cleanup()
	address := server.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
	}

	args := &common.Args{
		Database:      "test",
		Outdir:        tmpDir,
		User:          "mock",
		Password:      "mock",
		Address:       address,
		ChunksizeInMB: 1,
		Threads:       16,
		StmtSize:      10000,
		IntervalMs:    1000,
	}

	// Rewrite schema.
	{
		schema1 := "CREATE TABLE `t1` (`id` int(11) DEFAULT NULL,`b` varchar(100) DEFAULT NULL) ENGINE=InnoDB PARTITION BY HASH(id);"
		common.WriteFile(path.Join(args.Outdir, "test.t1-schema.sql"), schema1)

		schema2 := "CREATE TABLE `t2` (`id` int(11) DEFAULT NULL,`b` varchar(100) DEFAULT NULL) ENGINE=InnoDB PARTITION BY HASH(id);"
		common.WriteFile(path.Join(args.Outdir, "test.t2-schema.sql"), schema2)
	}

	// Loader.
	common.Loader(log, args)
}
