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
		"select a as tmp,b from B union distinct (select a,b from S union select 1,'a') order by a limit 10",
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

				assert.Nil(t, err)
				got := plan.JSON()
				want := results[i]
				assert.Equal(t, want, got)
				assert.Equal(t, PlanTypeUnion, plan.Type())
				plan.Size()
			}
		}
	}
}
