/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package executor

import (
	"testing"

	"backend"
	"fakedb"
	"planner"
	"router"
	"xcontext"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestDDLExecutor(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	fakedbs.AddQueryPattern("create table `sbtest`.`A.*", fakedb.Result3)
	fakedbs.AddQueryPattern("create database.*", fakedb.Result3)

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)

	// create table
	{
		query := "create table A(a int)"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		plan := planner.NewDDLPlan(log, database, query, node.(*sqlparser.DDL), route)
		err = plan.Build()
		assert.Nil(t, err)

		txn, err := scatter.CreateTransaction()
		assert.Nil(t, err)
		defer txn.Finish()
		executor := NewDDLExecutor(log, plan, txn)
		{
			ctx := xcontext.NewResultContext()
			err := executor.Execute(ctx)
			assert.Nil(t, err)
		}
	}

	// create database
	{
		query := "create database sbtest"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		plan := planner.NewDDLPlan(log, database, query, node.(*sqlparser.DDL), route)
		err = plan.Build()
		assert.Nil(t, err)

		txn, err := scatter.CreateTransaction()
		assert.Nil(t, err)
		defer txn.Finish()
		executor := NewDDLExecutor(log, plan, txn)
		{
			ctx := xcontext.NewResultContext()
			err := executor.Execute(ctx)
			assert.Nil(t, err)
		}
	}
}
