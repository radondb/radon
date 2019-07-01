/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"testing"

	"router"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestUnionPlan(t *testing.T) {
	results := []string{
		`{
	"RawQuery": "select a,b from G union select a,b from A where id=1 order by a limit 10",
	"Project": "a, b",
	"Partitions": [
		{
			"Query": "select a, b from sbtest.G union select a, b from sbtest.A6 as A where id = 1 order by a asc limit 10",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "select a,b from A where id=1 union select a,b from B where id=0 order by a limit 10",
	"Project": "a, b",
	"Partitions": [
		{
			"Query": "select a, b from sbtest.A6 as A where id = 1",
			"Backend": "backend6",
			"Range": "[512-4096)"
		},
		{
			"Query": "select a, b from sbtest.B0 as B where id = 0",
			"Backend": "backend1",
			"Range": "[0-512)"
		}
	],
	"UnionType": "union",
	"GatherMerge": [
		"a"
	],
	"Limit": {
		"Offset": 0,
		"Limit": 10
	}
}`,
		`{
	"RawQuery": "select a,b from S union (select a,b from G order by a) limit 10",
	"Project": "a, b",
	"Partitions": [
		{
			"Query": "select a, b from sbtest.S union (select a, b from sbtest.G) limit 10",
			"Backend": "backend1",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "select a,b from S union all (select a,b from A where id=1 union select a,b from B where id=0 order by a limit 10) order by b",
	"Project": "a, b",
	"Partitions": [
		{
			"Query": "select a, b from sbtest.S",
			"Backend": "backend1",
			"Range": ""
		},
		{
			"Query": "select a, b from sbtest.A6 as A where id = 1",
			"Backend": "backend6",
			"Range": "[512-4096)"
		},
		{
			"Query": "select a, b from sbtest.B0 as B where id = 0",
			"Backend": "backend1",
			"Range": "[0-512)"
		}
	],
	"UnionType": "union all",
	"GatherMerge": [
		"b"
	]
}`,
		`{
	"RawQuery": "select 1 union select a from A where id=1 order by 1 limit 10",
	"Project": "1",
	"Partitions": [
		{
			"Query": "select 1 from dual union select a from sbtest.A6 as A where id = 1 order by 1 asc limit 10",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "select a as tmp,b from B union distinct (select a,b from S union select 1,'a') order by a limit 10",
	"Project": "tmp, b",
	"Partitions": [
		{
			"Query": "select a as tmp, b from sbtest.B0 as B",
			"Backend": "backend1",
			"Range": "[0-512)"
		},
		{
			"Query": "select a as tmp, b from sbtest.B1 as B",
			"Backend": "backend2",
			"Range": "[512-4096)"
		},
		{
			"Query": "select a, b from sbtest.S union select 1, 'a' from dual",
			"Backend": "backend1",
			"Range": ""
		}
	],
	"UnionType": "union distinct",
	"GatherMerge": [
		"tmp"
	],
	"Limit": {
		"Offset": 0,
		"Limit": 10
	}
}`,
	}
	querys := []string{
		"select a,b from G union select a,b from A where id=1 order by a limit 10",
		"select a,b from A where id=1 union select a,b from B where id=0 order by a limit 10",
		"select a,b from S union (select a,b from G order by a) limit 10",
		"select a,b from S union all (select a,b from A where id=1 union select a,b from B where id=0 order by a limit 10) order by b",
		"select 1 union select a from A where id=1 order by 1 limit 10",
		"select a as tmp,b from B union distinct (select a,b from S union select 1,'a') order by a limit 10",
	}
	wants := []int{
		2, 2, 2, 2, 1, 2,
	}
	// Database not null.
	{
		log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
		database := "sbtest"

		route, cleanup := router.MockNewRouter(log)
		defer cleanup()

		err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableSConfig(), router.MockTableGConfig())
		assert.Nil(t, err)
		for i, query := range querys {
			node, err := sqlparser.Parse(query)
			assert.Nil(t, err)
			plan := NewUnionPlan(log, database, query, node.(*sqlparser.Union), route)

			// plan build
			{
				log.Info("--select.query:%+v", query)
				err := plan.Build()
				assert.Equal(t, len(plan.Root.getFields()), wants[i])
				assert.Nil(t, err)
				got := plan.JSON()
				want := results[i]
				assert.Equal(t, want, got)
				assert.Equal(t, PlanTypeUnion, plan.Type())
				assert.NotNil(t, plan.Children())
				plan.Size()
			}
		}
	}
}

func TestUnionUnsupportedPlan(t *testing.T) {
	querys := []string{
		"select a from A union select a,b from B",
		"select a from A union select b from B order by b",
		"select a from A union select b from B order by a limit x",
		"select a from C union select b from A limit 1",
		"select a from A union select b from C",
	}
	results := []string{
		"unsupported: the.used.'select'.statements.have.a.different.number.of.columns",
		"unsupported: orderby[b].should.in.select.list",
		"unsupported: limit.offset.or.counts.must.be.IntVal",
		"Table 'C' doesn't exist (errno 1146) (sqlstate 42S02)",
		"Table 'C' doesn't exist (errno 1146) (sqlstate 42S02)",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewUnionPlan(log, database, query, node.(*sqlparser.Union), route)

		// plan build
		{
			err := plan.Build()
			want := results[i]
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}
