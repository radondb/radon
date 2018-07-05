/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
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

func TestDeletePlan(t *testing.T) {
	results := []string{
		`{
	"RawQuery": "delete from sbtest.A where id=1",
	"Partitions": [
		{
			"Query": "delete from sbtest.A6 where id = 1",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "delete from sbtest.A where id=1 order by xx",
	"Partitions": [
		{
			"Query": "delete from sbtest.A6 where id = 1 order by xx asc",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "delete from sbtest.A where name='xx'",
	"Partitions": [
		{
			"Query": "delete from sbtest.A1 where name = 'xx'",
			"Backend": "backend1",
			"Range": "[0-32)"
		},
		{
			"Query": "delete from sbtest.A2 where name = 'xx'",
			"Backend": "backend2",
			"Range": "[32-64)"
		},
		{
			"Query": "delete from sbtest.A3 where name = 'xx'",
			"Backend": "backend3",
			"Range": "[64-96)"
		},
		{
			"Query": "delete from sbtest.A4 where name = 'xx'",
			"Backend": "backend4",
			"Range": "[96-256)"
		},
		{
			"Query": "delete from sbtest.A5 where name = 'xx'",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "delete from sbtest.A6 where name = 'xx'",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "delete from sbtest.A where id in (1, 2,3)",
	"Partitions": [
		{
			"Query": "delete from sbtest.A1 where id in (1, 2, 3)",
			"Backend": "backend1",
			"Range": "[0-32)"
		},
		{
			"Query": "delete from sbtest.A2 where id in (1, 2, 3)",
			"Backend": "backend2",
			"Range": "[32-64)"
		},
		{
			"Query": "delete from sbtest.A3 where id in (1, 2, 3)",
			"Backend": "backend3",
			"Range": "[64-96)"
		},
		{
			"Query": "delete from sbtest.A4 where id in (1, 2, 3)",
			"Backend": "backend4",
			"Range": "[96-256)"
		},
		{
			"Query": "delete from sbtest.A5 where id in (1, 2, 3)",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "delete from sbtest.A6 where id in (1, 2, 3)",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
	}
	querys := []string{
		"delete from sbtest.A where id=1",
		"delete from sbtest.A where id=1 order by xx",
		"delete from sbtest.A where name='xx'",
		"delete from sbtest.A where id in (1, 2,3)",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig())
	assert.Nil(t, err)
	planTree := NewPlanTree()
	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewDeletePlan(log, database, query, node.(*sqlparser.Delete), route)

		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			{
				err := planTree.Add(plan)
				assert.Nil(t, err)
			}
			got := plan.JSON()
			log.Debug(got)
			want := results[i]
			assert.Equal(t, want, got)
			assert.Equal(t, PlanTypeDelete, plan.Type())
			assert.Nil(t, plan.Children())
		}
	}
}

func TestDeleteUnsupportedPlan(t *testing.T) {
	querys := []string{
		"delete from sbtest.A",
		"delete from sbtest.A where id in (select id from t1)",
	}

	results := []string{
		"unsupported: missing.where.clause.in.DML",
		"unsupported: subqueries.in.delete",
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
		plan := NewDeletePlan(log, database, query, node.(*sqlparser.Delete), route)

		// plan build
		{
			err := plan.Build()
			want := results[i]
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}

func TestDeleteErrorPlan(t *testing.T) {
	query := "delete from A where id=1"

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig())
	assert.Nil(t, err)
	databaseNull := ""
	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	plan := NewDeletePlan(log, databaseNull, query, node.(*sqlparser.Delete), route)

	// plan build
	{
		planTree := NewPlanTree()
		{
			err := planTree.Add(plan)
			assert.Nil(t, err)
		}

		{
			err := planTree.Build()
			assert.NotNil(t, err)
		}
	}
}
