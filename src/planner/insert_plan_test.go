/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"fmt"
	"testing"
	"time"

	"router"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestInsertPlan(t *testing.T) {
	results := []string{
		`{
	"RawQuery": "insert into A(id, b, c) values(1,2,3) on duplicate key update c=11",
	"Partitions": [
		{
			"Query": "insert into sbtest.A6(id, b, c) values (1, 2, 3) on duplicate key update c = 11",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "insert into A(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "insert into sbtest.A5(id, b, c) values (65536, 3, 4)",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "insert into sbtest.A6(id, b, c) values (1, 2, 3), (23, 4, 5)",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "insert into sbtest.A(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "insert into sbtest.A5(id, b, c) values (65536, 3, 4)",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "insert into sbtest.A6(id, b, c) values (1, 2, 3), (23, 4, 5)",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
	}
	querys := []string{
		"insert into A(id, b, c) values(1,2,3) on duplicate key update c=11",
		"insert into A(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
		"insert into sbtest.A(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewInsertPlan(log, database, query, node.(*sqlparser.Insert), route)

		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			got := plan.JSON()
			log.Info(got)
			want := results[i]
			assert.Equal(t, want, got)
			plan.Type()
			plan.Size()
		}
	}
}

func TestInsertUnsupportedPlan(t *testing.T) {
	querys := []string{
		"insert into sbtest.A(b, c, id) values(1,2)",
		"insert into sbtest.A(b, c, d) values(1,2, 3)",
		"insert into sbtest.A select * from sbtest.B",
		"insert into sbtest.A(b, c, id) values(1,2,3) on duplicate key update id=1",
		"insert into sbtest.A(b, c, id) values(1, floor(3), floor(3))",
		"insert into sbtest.A(b,c,id) select id,b,c from sbtest.A",
	}

	results := []string{
		"unsupported: shardkey[id].out.of.index:[2]",
		"unsupported: shardkey.column[id].missing",
		"unsupported: shardkey.column[id].missing",
		"unsupported: cannot.update.shard.key",
		"unsupported: shardkey[id].type.canot.be[*sqlparser.FuncExpr]",
		"unsupported: rows.can.not.be.subquery[*sqlparser.Select]",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewInsertPlan(log, database, query, node.(*sqlparser.Insert), route)

		// plan build
		{
			err := plan.Build()
			want := results[i]
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}

func TestInsertPlanBench(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	query := "insert into sbtest.A(id, b, c) values(1,2,3),(23,4,5), (117,3,4),(1,2,3),(23,4,5), (117,3,4)"
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig())
	assert.Nil(t, err)

	{
		N := 100000
		now := time.Now()
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		for i := 0; i < N; i++ {
			plan := NewInsertPlan(log, database, query, node.(*sqlparser.Insert), route)
			err := plan.Build()
			assert.Nil(t, err)
		}

		took := time.Since(now)
		fmt.Printf(" LOOP\t%v COST %v, avg:%v/s\n", N, took, (int64(N)/(took.Nanoseconds()/1e6))*1000)
	}
}

func TestReplacePlan(t *testing.T) {
	results := []string{`{
	"RawQuery": "replace into A(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "replace into sbtest.A5(id, b, c) values (65536, 3, 4)",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "replace into sbtest.A6(id, b, c) values (1, 2, 3), (23, 4, 5)",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "replace into sbtest.A(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "replace into sbtest.A5(id, b, c) values (65536, 3, 4)",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "replace into sbtest.A6(id, b, c) values (1, 2, 3), (23, 4, 5)",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
	}
	querys := []string{
		"replace into A(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
		"replace into sbtest.A(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewInsertPlan(log, database, query, node.(*sqlparser.Insert), route)

		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			got := plan.JSON()
			log.Info(got)
			want := results[i]
			assert.Equal(t, want, got)
			plan.Type()
		}
	}
}

func TestReplaceUnsupportedPlan(t *testing.T) {
	querys := []string{
		"replace into sbtest.A(b, c, id) values(1,2)",
		"replace into sbtest.A(b, c, d) values(1,2, 3)",
		"replace into sbtest.A select * from sbtest.B",
	}

	results := []string{
		"unsupported: shardkey[id].out.of.index:[2]",
		"unsupported: shardkey.column[id].missing",
		"unsupported: shardkey.column[id].missing",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewInsertPlan(log, database, query, node.(*sqlparser.Insert), route)

		// plan build
		{
			err := plan.Build()
			want := results[i]
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}

func TestReplacePlanBench(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	query := "replace into sbtest.A(id, b, c) values(1,2,3),(23,4,5), (117,3,4),(1,2,3),(23,4,5), (117,3,4)"
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig())
	assert.Nil(t, err)

	{
		N := 100000
		now := time.Now()
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		for i := 0; i < N; i++ {
			plan := NewInsertPlan(log, database, query, node.(*sqlparser.Insert), route)
			err := plan.Build()
			assert.Nil(t, err)
		}

		took := time.Since(now)
		fmt.Printf(" LOOP\t%v COST %v, avg:%v/s\n", N, took, (int64(N)/(took.Nanoseconds()/1e6))*1000)
	}
}

func TestInsertPlanError(t *testing.T) {
	query := "insert into A(b, c, id) values(1, 2, 3)"

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig())
	assert.Nil(t, err)
	databaseNull := ""
	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	plan := NewInsertPlan(log, databaseNull, query, node.(*sqlparser.Insert), route)

	// plan build
	{
		err := plan.Build()
		assert.NotNil(t, err)
	}
}
