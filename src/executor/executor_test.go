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

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestExecutor1(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	fakedbs.AddQueryPattern("create table sbtest.A.*", fakedb.Result3)
	fakedbs.AddQueryPattern("create database.*", fakedb.Result3)

	database := "sbtest"
	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)

	planTree := planner.NewPlanTree()

	// DDL
	{
		query := "create table A(a int)"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		plan := planner.NewDDLPlan(log, database, query, node.(*sqlparser.DDL), route)
		err = planTree.Add(plan)
		assert.Nil(t, err)
	}

	// insert
	{
		query := "insert into A(a) values(1)"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		plan := planner.NewInsertPlan(log, database, query, node.(*sqlparser.Insert), route)
		err = planTree.Add(plan)
		assert.Nil(t, err)
	}

	// delete
	{
		query := "delete from A where a=2"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		plan := planner.NewDeletePlan(log, database, query, node.(*sqlparser.Delete), route)
		err = planTree.Add(plan)
		assert.Nil(t, err)
	}

	// update
	{
		query := "update A set a=3 where a=2"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		plan := planner.NewUpdatePlan(log, database, query, node.(*sqlparser.Update), route)
		err = planTree.Add(plan)
		assert.Nil(t, err)
	}

	// update
	{
		query := "select * from A  where a=2"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		plan := planner.NewSelectPlan(log, database, query, node.(*sqlparser.Select), route)
		err = planTree.Add(plan)
		assert.Nil(t, err)
	}

	// Execute.
	txn, err := scatter.CreateTransaction()
	assert.Nil(t, err)
	defer txn.Finish()
	executorTree := NewTree(log, planTree, txn)
	qr, err := executorTree.Execute()
	assert.Nil(t, err)
	assert.Equal(t, fakedb.Result3, qr)
}
