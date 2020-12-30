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
		`{
	"RawQuery": "insert into sbtest.A(ID, B, C) values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "insert into sbtest.A5(ID, B, C) values (65536, 3, 4)",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "insert into sbtest.A6(ID, B, C) values (1, 2, 3), (23, 4, 5)",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "insert into sbtest.A(ID, B, C) values(1,2,3),(23,4,5), (65536,3,4) on duplicate key update B = 11",
	"Partitions": [
		{
			"Query": "insert into sbtest.A5(ID, B, C) values (65536, 3, 4) on duplicate key update B = 11",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "insert into sbtest.A6(ID, B, C) values (1, 2, 3), (23, 4, 5) on duplicate key update B = 11",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "insert HIGH_PRIORITY iGNORE into A set id = 1, b = 2, c = 3 on duplicate key update c=11",
	"Partitions": [
		{
			"Query": "insert high_priority ignore into sbtest.A6(id, b, c) values (1, 2, 3) on duplicate key update c = 11",
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
		"insert into sbtest.A(ID, B, C) values(1,2,3),(23,4,5), (65536,3,4)",
		"insert into sbtest.A(ID, B, C) values(1,2,3),(23,4,5), (65536,3,4) on duplicate key update B = 11",
		"insert HIGH_PRIORITY iGNORE into A set id = 1, b = 2, c = 3 on duplicate key update c=11",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig())
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

func TestInsertPlanSort(t *testing.T) {
	results := []string{
		`{
	"RawQuery": "insert into sbtest.A(id, b, c) values(1,2,3), (23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "insert into sbtest.A5(id, b, c) values (65536, 3, 4)",
			"Backend": "backend6",
			"Range": "[0-512)"
		},
		{
			"Query": "insert into sbtest.A6(id, b, c) values (1, 2, 3), (23, 4, 5)",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "insert into sbtest.A(id, b, c) values(65536,3,4), (23,4,5), (1,2,3)",
	"Partitions": [
		{
			"Query": "insert into sbtest.A5(id, b, c) values (65536, 3, 4)",
			"Backend": "backend6",
			"Range": "[0-512)"
		},
		{
			"Query": "insert into sbtest.A6(id, b, c) values (23, 4, 5), (1, 2, 3)",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
	}
	querys := []string{
		"insert into sbtest.A(id, b, c) values(1,2,3), (23,4,5), (65536,3,4)",
		"insert into sbtest.A(id, b, c) values(65536,3,4), (23,4,5), (1,2,3)",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableDeadLockConfig())
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
		"insert into sbtest.A(b, c, id) values(1,2,3) on duplicate key update id=1",
		"insert into sbtest.A(b, c, id) values(1, floor(3), floor(3))",
		"insert into sbtest.A select * from sbtest.B",
		"insert into sbtest.A(b,c,id) select id,b,c from sbtest.A",
		"insert into sbtest.G(b, c, id) select * from sbtest.A",
		"insert into sbtest.G select * from sbtest.A",
		"insert /* simple */ high_priority into a partition (col_1) values (1)",
	}

	results := []string{
		"unsupported: shardkey[id].out.of.index:[2]",
		"unsupported: shardkey.column[id].missing",
		"unsupported: cannot.update.shard.key",
		"unsupported: shardkey[id].type.canot.be[*sqlparser.FuncExpr]",
		"unsupported: rows.can.not.be.subquery[*sqlparser.Select]",
		"unsupported: rows.can.not.be.subquery[*sqlparser.Select]",
		"unsupported: rows.can.not.be.subquery[*sqlparser.Select]",
		"unsupported: rows.can.not.be.subquery[*sqlparser.Select]",
		"unsupported: radon.now.not.support.insert.with.partition.",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableGConfig())
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

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig())
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
		`{
	"RawQuery": "replace into sbtest.A set id = 1, b = 2, c = 3",
	"Partitions": [
		{
			"Query": "replace into sbtest.A6(id, b, c) values (1, 2, 3)",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
	}
	querys := []string{
		"replace into A(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
		"replace into sbtest.A(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
		"replace into sbtest.A set id = 1, b = 2, c = 3",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig())
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
		"replace into sbtest.A(b,c,id) select id,b,c from sbtest.A",
		"replace into sbtest.G(b, c, id) select * from sbtest.A",
		"replace into sbtest.G select * from sbtest.A",
	}

	results := []string{
		"unsupported: shardkey[id].out.of.index:[2]",
		"unsupported: shardkey.column[id].missing",
		"unsupported: rows.can.not.be.subquery[*sqlparser.Select]",
		"unsupported: rows.can.not.be.subquery[*sqlparser.Select]",
		"unsupported: rows.can.not.be.subquery[*sqlparser.Select]",
		"unsupported: rows.can.not.be.subquery[*sqlparser.Select]",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableGConfig())
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

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig())
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
	querys := []string{
		"insert into A(b, c, id) values(1, 2, 3)",
		"insert into G(b, c, id) values(1, 2, 3)",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableGConfig())
	assert.Nil(t, err)
	databaseNull := ""
	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewInsertPlan(log, databaseNull, query, node.(*sqlparser.Insert), route)

		// plan build
		{
			err := plan.Build()
			assert.NotNil(t, err)
		}
	}
}

func TestInsertPlanGlobal(t *testing.T) {
	results := []string{
		`{
	"RawQuery": "insert into G(id, b, c) values(1,2,3) on duplicate key update c=11",
	"Partitions": [
		{
			"Query": "insert into sbtest.G(id, b, c) values (1, 2, 3) on duplicate key update c = 11",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "insert into sbtest.G(id, b, c) values (1, 2, 3) on duplicate key update c = 11",
			"Backend": "backend2",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "insert into G(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "insert into sbtest.G(id, b, c) values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "insert into sbtest.G(id, b, c) values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend2",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "insert into G values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "insert into sbtest.G values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "insert into sbtest.G values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend2",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "insert into sbtest.G(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "insert into sbtest.G(id, b, c) values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "insert into sbtest.G(id, b, c) values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend2",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "insert /*test insert without columns*/ into sbtest.G values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "insert /*test insert without columns*/ into sbtest.G values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "insert /*test insert without columns*/ into sbtest.G values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend2",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "insert /*test insert ... set ... */ HIGH_PRIORITY iGNORE into G set id = 1, b = 2, c = 3 on duplicate key update c=11",
	"Partitions": [
		{
			"Query": "insert /*test insert ... set ... */ high_priority ignore into sbtest.G(id, b, c) values (1, 2, 3) on duplicate key update c = 11",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "insert /*test insert ... set ... */ high_priority ignore into sbtest.G(id, b, c) values (1, 2, 3) on duplicate key update c = 11",
			"Backend": "backend2",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "insert /*test insert ... set ... */ LOW_PRIORITY iGNORE into sbtest.G set id = 1, b = 2, c = 3",
	"Partitions": [
		{
			"Query": "insert /*test insert ... set ... */ low_priority ignore into sbtest.G(id, b, c) values (1, 2, 3)",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "insert /*test insert ... set ... */ low_priority ignore into sbtest.G(id, b, c) values (1, 2, 3)",
			"Backend": "backend2",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "insert /*test insert ... set ... */ LOW_PRIORITY iGNORE into sbtest.G set id = 1, b = 2, c = 3",
	"Partitions": [
		{
			"Query": "insert /*test insert ... set ... */ low_priority ignore into sbtest.G(id, b, c) values (1, 2, 3)",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "insert /*test insert ... set ... */ low_priority ignore into sbtest.G(id, b, c) values (1, 2, 3)",
			"Backend": "backend2",
			"Range": ""
		}
	]
}`,
	}
	querys := []string{
		"insert into G(id, b, c) values(1,2,3) on duplicate key update c=11",
		"insert into G(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
		"insert into G values(1,2,3),(23,4,5), (65536,3,4)",
		"insert into sbtest.G(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
		"insert /*test insert without columns*/ into sbtest.G values(1,2,3),(23,4,5), (65536,3,4)",
		"insert /*test insert ... set ... */ HIGH_PRIORITY iGNORE into G set id = 1, b = 2, c = 3 on duplicate key update c=11",
		"insert /*test insert ... set ... */ LOW_PRIORITY iGNORE into sbtest.G set id = 1, b = 2, c = 3",
		"insert /*test insert ... set ... */ LOW_PRIORITY iGNORE into sbtest.G set id = 1, b = 2, c = 3",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableGConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		// database is nil
		if i == 7 {
			database = ""
		}
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

func TestReplacePlanGlobal(t *testing.T) {
	results := []string{
		`{
	"RawQuery": "replace into G(id, b, c) values(1,2,3)",
	"Partitions": [
		{
			"Query": "replace into sbtest.G(id, b, c) values (1, 2, 3)",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "replace into sbtest.G(id, b, c) values (1, 2, 3)",
			"Backend": "backend2",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "replace into G values(1,2,3)",
	"Partitions": [
		{
			"Query": "replace into sbtest.G values (1, 2, 3)",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "replace into sbtest.G values (1, 2, 3)",
			"Backend": "backend2",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "replace into G(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "replace into sbtest.G(id, b, c) values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "replace into sbtest.G(id, b, c) values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend2",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "replace into sbtest.G(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "replace into sbtest.G(id, b, c) values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "replace into sbtest.G(id, b, c) values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend2",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "replace into sbtest.G values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "replace into sbtest.G values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "replace into sbtest.G values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend2",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "replace /*test replace ... set ... */ DELAYED iGNORE into G set id = 1, b = 2, c = 3",
	"Partitions": [
		{
			"Query": "replace /*test replace ... set ... */ delayed ignore into sbtest.G(id, b, c) values (1, 2, 3)",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "replace /*test replace ... set ... */ delayed ignore into sbtest.G(id, b, c) values (1, 2, 3)",
			"Backend": "backend2",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "replace /*test replace ... set ... */ LOW_PRIORITY iGNORE into sbtest.G set id = 1, b = 2, c = 3",
	"Partitions": [
		{
			"Query": "replace /*test replace ... set ... */ low_priority ignore into sbtest.G(id, b, c) values (1, 2, 3)",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "replace /*test replace ... set ... */ low_priority ignore into sbtest.G(id, b, c) values (1, 2, 3)",
			"Backend": "backend2",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "replace /*test database is empty*/ LOW_PRIORITY iGNORE into sbtest.G set id = 1, b = 2, c = 3",
	"Partitions": [
		{
			"Query": "replace /*test database is empty*/ low_priority ignore into sbtest.G(id, b, c) values (1, 2, 3)",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "replace /*test database is empty*/ low_priority ignore into sbtest.G(id, b, c) values (1, 2, 3)",
			"Backend": "backend2",
			"Range": ""
		}
	]
}`,
	}
	querys := []string{
		"replace into G(id, b, c) values(1,2,3)",
		"replace into G values(1,2,3)",
		"replace into G(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
		"replace into sbtest.G(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
		"replace into sbtest.G values(1,2,3),(23,4,5), (65536,3,4)",
		"replace /*test replace ... set ... */ DELAYED iGNORE into G set id = 1, b = 2, c = 3",
		"replace /*test replace ... set ... */ LOW_PRIORITY iGNORE into sbtest.G set id = 1, b = 2, c = 3",
		"replace /*test database is empty*/ LOW_PRIORITY iGNORE into sbtest.G set id = 1, b = 2, c = 3",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableGConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		// database is nil
		if i == 6 {
			database = ""
		}
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

func TestInsertPlanSingle(t *testing.T) {
	results := []string{
		`{
	"RawQuery": "insert into S(id, b, c) values(1,2,3) on duplicate key update c=11",
	"Partitions": [
		{
			"Query": "insert into sbtest.S(id, b, c) values (1, 2, 3) on duplicate key update c = 11",
			"Backend": "backend1",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "insert into S(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "insert into sbtest.S(id, b, c) values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend1",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "insert into S values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "insert into sbtest.S values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend1",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "insert /*test insert ... set ... */ HIGH_PRIORITY iGNORE into S set id = 1, b = 2, c = 3 on duplicate key update c=11",
	"Partitions": [
		{
			"Query": "insert /*test insert ... set ... */ high_priority ignore into sbtest.S(id, b, c) values (1, 2, 3) on duplicate key update c = 11",
			"Backend": "backend1",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "insert /*test insert ... set ... */ LOW_PRIORITY iGNORE into sbtest.S set id = 1, b = 2, c = 3",
	"Partitions": [
		{
			"Query": "insert /*test insert ... set ... */ low_priority ignore into sbtest.S(id, b, c) values (1, 2, 3)",
			"Backend": "backend1",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "insert into sbtest.S(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "insert into sbtest.S(id, b, c) values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend1",
			"Range": ""
		}
	]
}`,
	}
	querys := []string{
		"insert into S(id, b, c) values(1,2,3) on duplicate key update c=11",
		"insert into S(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
		"insert into S values(1,2,3),(23,4,5), (65536,3,4)",
		"insert /*test insert ... set ... */ HIGH_PRIORITY iGNORE into S set id = 1, b = 2, c = 3 on duplicate key update c=11",
		"insert /*test insert ... set ... */ LOW_PRIORITY iGNORE into sbtest.S set id = 1, b = 2, c = 3",
		"insert into sbtest.S(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableSConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		// database is nil
		if i == 8 {
			database = ""
		}
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

func TestReplacePlanSingle(t *testing.T) {
	results := []string{
		`{
	"RawQuery": "replace into S(id, b, c) values(1,2,3)",
	"Partitions": [
		{
			"Query": "replace into sbtest.S(id, b, c) values (1, 2, 3)",
			"Backend": "backend1",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "replace into S(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "replace into sbtest.S(id, b, c) values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend1",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "replace into sbtest.S(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "replace into sbtest.S(id, b, c) values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend1",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "replace /*test replace ... set ... */ LOW_PRIORITY iGNORE into sbtest.S set id = 1, b = 2, c = 3",
	"Partitions": [
		{
			"Query": "replace /*test replace ... set ... */ low_priority ignore into sbtest.S(id, b, c) values (1, 2, 3)",
			"Backend": "backend1",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "replace into sbtest.S(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	"Partitions": [
		{
			"Query": "replace into sbtest.S(id, b, c) values (1, 2, 3), (23, 4, 5), (65536, 3, 4)",
			"Backend": "backend1",
			"Range": ""
		}
	]
}`,
	}
	querys := []string{
		"replace into S(id, b, c) values(1,2,3)",
		"replace into S(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
		"replace into sbtest.S(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
		"replace /*test replace ... set ... */ LOW_PRIORITY iGNORE into sbtest.S set id = 1, b = 2, c = 3",
		"replace into sbtest.S(id, b, c) values(1,2,3),(23,4,5), (65536,3,4)",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableSConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		// database is nil
		if i == 4 {
			database = ""
		}
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
