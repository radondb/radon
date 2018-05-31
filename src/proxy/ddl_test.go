/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"errors"
	"fakedb"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestProxyDDLDB(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxyWithBackup(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern(".* database .*", &sqltypes.Result{})
	}

	// create database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create database test"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// drop database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "drop database if exists test"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// ACL database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create database mysql"
		_, err = client.FetchAll(query, -1)
		want := "Access denied; lacking privileges for database mysql (errno 1227) (sqlstate 42000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestProxyDDLTable(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show tables from .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("alter table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("drop table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("truncate table .*", &sqltypes.Result{})
	}

	// create table error.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table t1(a int, b int)"
		_, err = client.FetchAll(query, -1)
		want := "create table must end with 'PARTITION BY HASH(shard-key)' (errno 1105) (sqlstate HY000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// create table(ACL).
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create table mysql.t2(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		want := "Access denied; lacking privileges for database mysql (errno 1227) (sqlstate 42000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create table t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// create sbtest table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table sbtest.sbt1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// alter test table engine.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "alter table t1 engine=tokudb"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// truncate table.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "truncate table t1"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// create sbtest table mysql internal error.
	{
		fakedbs.AddQueryErrorPattern("create table .*", errors.New("mock.mysql.create.table.error"))

		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table sbtest.sberror2(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		want := "mock.mysql.create.table.error (errno 1105) (sqlstate HY000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// check sbtest.tables.
	{
		client, err := driver.NewConn("mock", "mock", address, "sbtest", "utf8")
		assert.Nil(t, err)
		query := "show tables"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "[[sbt1]]"
		got := fmt.Sprintf("%+v", qr.Rows)
		assert.Equal(t, want, got)
	}

	// drop sbtest table error.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "drop table sbtest.t1"
		_, err = client.FetchAll(query, -1)
		want := "Table 't1' doesn't exist (errno 1146) (sqlstate 42S02)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// drop sbtest1 table error.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "drop table sbtest1.t1"
		_, err = client.FetchAll(query, -1)
		want := "Unknown database 'sbtest1' (errno 1049) (sqlstate 42000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// drop sbtest table.
	{
		client, err := driver.NewConn("mock", "mock", address, "sbtest", "utf8")
		assert.Nil(t, err)
		query := "drop table sbt1"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// check sbtest.tables.
	{
		client, err := driver.NewConn("mock", "mock", address, "sbtest", "utf8")
		assert.Nil(t, err)
		query := "show tables"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "[]"
		got := fmt.Sprintf("%+v", qr.Rows)
		assert.Equal(t, want, got)
	}

	// create sbtest table.
	{
		fakedbs.ResetPatternErrors()
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table sbtest.sbt1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// drop sbtest table internal error.
	{
		fakedbs.AddQueryErrorPattern("drop table .*", errors.New("mock.mysql.drop.table.error"))
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "drop table sbtest.sbt1"
		_, err = client.FetchAll(query, -1)
		want := "mock.mysql.drop.table.error (errno 1105) (sqlstate HY000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestProxyDDLIndex(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show tables from .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show create table .*", fakedb.Result1)
		fakedbs.AddQueryPattern("drop table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create index.*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("drop index.*", &sqltypes.Result{})
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create table t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// show create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "show create table t1"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// create index.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create index index1 on t1(a,b)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// create index error.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create index index1 on xx.t1(a,b)"
		_, err = client.FetchAll(query, -1)
		want := "Unknown database 'xx' (errno 1049) (sqlstate 42000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// create index.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create index index1 on t1(a,b)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// create index error.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create index index1 on xx.t1(a,b)"
		_, err = client.FetchAll(query, -1)
		want := "Unknown database 'xx' (errno 1049) (sqlstate 42000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// drop index.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "drop index index1 on t1"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
}

func TestProxyDDLColumn(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("alter table .*", &sqltypes.Result{})
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create table t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// add column.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "alter table t1 add column(c1 int, c2 varchar(100))"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// drop column.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "alter table t1 drop column c2"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// drop column error(drop the shardkey).
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "alter table t1 drop column id"
		_, err = client.FetchAll(query, -1)
		want := "unsupported: cannot.drop.the.column.on.shard.key (errno 1105) (sqlstate HY000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// modify column.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "alter table t1 modify column c2 varchar(1)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// modify column error(drop the shardkey).
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "alter table t1 modify column id bigint"
		_, err = client.FetchAll(query, -1)
		want := "unsupported: cannot.modify.the.column.on.shard.key (errno 1105) (sqlstate HY000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestProxyDDLUnsupported(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("rename .*", &sqltypes.Result{})
	}

	// rename test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "rename table t1 to t2"
		_, err = client.FetchAll(query, -1)
		want := "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use, syntax error at position 7 near 'rename' (errno 1149) (sqlstate 42000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestProxyDDLCreateTable(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
	}

	querys := []string{
		"create table t1(a int, b int) partition by hash(a)",
		"create table t2(a int, b int) PARTITION BY hash(a)",
		"create table t3(a int, b int)   PARTITION  BY hash(a)  ",
		"create table t4(a int, b int)engine=tokudb PARTITION  BY hash(a)  ",
		"create table t5(a int, b int) default charset=utf8  PARTITION  BY hash(a)  ",
		"create table t6(a int, b int)engine=tokudb default charset=utf8  PARTITION  BY hash(a)  ",
	}

	for _, query := range querys {
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
}

func TestProxyDDLCreateTableError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
	}

	querys := []string{
		"create table t1(a int, b int)",
		"create table t2(a int, partition int) PARTiITION BY hash(a)",
		"create table dual(a int) partition by hash(a)",
	}
	results := []string{
		"create table must end with 'PARTITION BY HASH(shard-key)' (errno 1105) (sqlstate HY000)",
		"You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use, syntax error at position 33 near 'partition' (errno 1149) (sqlstate 42000)",
		"spanner.ddl.check.create.table[dual].error:not surpport (errno 1105) (sqlstate HY000)",
	}

	for i, query := range querys {
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		_, err = client.FetchAll(query, -1)
		want := results[i]
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestProxyMyLoaderImport(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show create database .*", &sqltypes.Result{})
		fakedbs.AddQuery("/*show create database sbtest*/", &sqltypes.Result{})
	}

	querys := []string{
		"create table t1(a int, b int) partition by hash(a)",
		"show create database sbtest",
		"/*show create database sbtest*/",
		"SET autocommit=0",
		"SET SESSION wait_timeout = 2147483",
	}

	for _, query := range querys {
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
}

func TestProxyDDLConstraint(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
	}

	querys := []string{
		"CREATE TABLE t1(a int primary key,b int ) PARTITION BY HASH(a);",
		"CREATE TABLE t2(a int unique,b int ) PARTITION BY HASH(a);",
		"CREATE TABLE t2(a int ,b int primary key) PARTITION BY HASH(a);",
		"CREATE TABLE t3(a int primary key,b int unique) PARTITION BY HASH(a);",
	}

	results := []string{
		"",
		"",
		"The unique/primary constraint only be defined on the sharding key column[a] not [b] (errno 1105) (sqlstate HY000)",
		"The unique/primary constraint only be defined on the sharding key column[a] not [b] (errno 1105) (sqlstate HY000)",
	}

	for i, query := range querys {
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		_, err = client.FetchAll(query, -1)
		if err != nil {
			want := results[i]
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}

func TestProxyDDLShardKeyCheck(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
	}

	querys := []string{
		"CREATE TABLE t1(a int primary key,b int ) PARTITION BY HASH(`a`);",
		"CREATE TABLE t1(a int,b int ) PARTITION BY HASH(c);",
	}

	results := []string{
		"",
		"Sharding Key column 'c' doesn't exist in table (errno 1105) (sqlstate HY000)",
	}

	for i, query := range querys {
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		_, err = client.FetchAll(query, -1)
		if err != nil {
			want := results[i]
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}

func TestProxyDDLAlterCharset(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show tables from .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("alter table .*", &sqltypes.Result{})
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create table t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// alter test table charset.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "alter table t1 convert to character set utf8mb"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
}
