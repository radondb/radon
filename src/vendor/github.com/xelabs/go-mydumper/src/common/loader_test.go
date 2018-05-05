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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestLoader(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))
	fakedbs := driver.NewTestHandler(log)
	server, err := driver.MockMysqlServer(log, fakedbs)
	assert.Nil(t, err)
	defer server.Close()
	address := server.Addr()

	// fakedbs.
	{
		fakedbs.AddQuery("create database if not exists `test`", &sqltypes.Result{})
		fakedbs.AddQuery("create table `t1` (`a` int(11) default null,`b` varchar(100) default null) engine=innodb", &sqltypes.Result{})
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert into .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("drop table .*", &sqltypes.Result{})
	}

	args := &Args{
		Outdir:          "/tmp/dumpertest",
		User:            "mock",
		Password:        "mock",
		Threads:         16,
		Address:         address,
		IntervalMs:      500,
		OverwriteTables: true,
	}
	// Loader.
	{
		Loader(log, args)
	}
}
