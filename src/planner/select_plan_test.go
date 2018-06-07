/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"router"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestSelectPlan(t *testing.T) {
	results := []string{
		`{
	"RawQuery": "select 1, sum(a),avg(a),a,b from sbtest.A where id\u003e1 group by a,b order by a desc limit 10 offset 100",
	"Project": "1, sum(a), avg(a), sum(a), count(a), a, b",
	"Partitions": [
		{
			"Query": "select 1, sum(a), avg(a), sum(a), count(a), a, b from sbtest.A1 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend1",
			"Range": "[0-32)"
		},
		{
			"Query": "select 1, sum(a), avg(a), sum(a), count(a), a, b from sbtest.A2 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend2",
			"Range": "[32-64)"
		},
		{
			"Query": "select 1, sum(a), avg(a), sum(a), count(a), a, b from sbtest.A3 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend3",
			"Range": "[64-96)"
		},
		{
			"Query": "select 1, sum(a), avg(a), sum(a), count(a), a, b from sbtest.A4 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend4",
			"Range": "[96-256)"
		},
		{
			"Query": "select 1, sum(a), avg(a), sum(a), count(a), a, b from sbtest.A5 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "select 1, sum(a), avg(a), sum(a), count(a), a, b from sbtest.A6 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	],
	"Aggregate": [
		"sum(a)",
		"avg(a)",
		"sum(a)",
		"count(a)"
	],
	"GatherMerge": [
		"a"
	],
	"HashGroupBy": [
		"a",
		"b"
	],
	"Limit": {
		"Offset": 100,
		"Limit": 10
	}
}`,
		`{
	"RawQuery": "select id, sum(a) as A from A group by id having A\u003e1000",
	"Project": "id, sum(a) as A",
	"Partitions": [
		{
			"Query": "select id, sum(a) as A from sbtest.A1 as A group by id having A \u003e 1000",
			"Backend": "backend1",
			"Range": "[0-32)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A2 as A group by id having A \u003e 1000",
			"Backend": "backend2",
			"Range": "[32-64)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A3 as A group by id having A \u003e 1000",
			"Backend": "backend3",
			"Range": "[64-96)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A4 as A group by id having A \u003e 1000",
			"Backend": "backend4",
			"Range": "[96-256)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A5 as A group by id having A \u003e 1000",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A6 as A group by id having A \u003e 1000",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	],
	"Aggregate": [
		"A"
	],
	"HashGroupBy": [
		"id"
	]
}`,
	}
	querys := []string{
		"select 1, sum(a),avg(a),a,b from sbtest.A where id>1 group by a,b order by a desc limit 10 offset 100",
		"select id, sum(a) as A from A group by id having A>1000",
	}

	// Database not null.
	{
		log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
		database := "sbtest"

		route, cleanup := router.MockNewRouter(log)
		defer cleanup()

		err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
		assert.Nil(t, err)
		for i, query := range querys {
			node, err := sqlparser.Parse(query)
			assert.Nil(t, err)
			plan := NewSelectPlan(log, database, query, node.(*sqlparser.Select), route)

			// plan build
			{
				log.Info("--select.query:%+v", query)
				err := plan.Build()
				assert.Nil(t, err)
				got := plan.JSON()
				want := results[i]
				assert.Equal(t, want, got)
				assert.Equal(t, PlanTypeSelect, plan.Type())
				assert.NotNil(t, plan.Children())
			}
		}
	}
}

func TestSelectPlanDatabaseIsNull(t *testing.T) {
	results := []string{
		`{
	"RawQuery": "select 1, sum(a),avg(a),a,b from sbtest.A where id\u003e1 group by a,b order by a desc limit 10 offset 100",
	"Project": "1, sum(a), avg(a), sum(a), count(a), a, b",
	"Partitions": [
		{
			"Query": "select 1, sum(a), avg(a), sum(a), count(a), a, b from sbtest.A1 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend1",
			"Range": "[0-32)"
		},
		{
			"Query": "select 1, sum(a), avg(a), sum(a), count(a), a, b from sbtest.A2 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend2",
			"Range": "[32-64)"
		},
		{
			"Query": "select 1, sum(a), avg(a), sum(a), count(a), a, b from sbtest.A3 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend3",
			"Range": "[64-96)"
		},
		{
			"Query": "select 1, sum(a), avg(a), sum(a), count(a), a, b from sbtest.A4 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend4",
			"Range": "[96-256)"
		},
		{
			"Query": "select 1, sum(a), avg(a), sum(a), count(a), a, b from sbtest.A5 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "select 1, sum(a), avg(a), sum(a), count(a), a, b from sbtest.A6 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	],
	"Aggregate": [
		"sum(a)",
		"avg(a)",
		"sum(a)",
		"count(a)"
	],
	"GatherMerge": [
		"a"
	],
	"HashGroupBy": [
		"a",
		"b"
	],
	"Limit": {
		"Offset": 100,
		"Limit": 10
	}
}`,
		`{
	"RawQuery": "select id, sum(a) as A from sbtest.A group by id having A\u003e1000",
	"Project": "id, sum(a) as A",
	"Partitions": [
		{
			"Query": "select id, sum(a) as A from sbtest.A1 as A group by id having A \u003e 1000",
			"Backend": "backend1",
			"Range": "[0-32)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A2 as A group by id having A \u003e 1000",
			"Backend": "backend2",
			"Range": "[32-64)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A3 as A group by id having A \u003e 1000",
			"Backend": "backend3",
			"Range": "[64-96)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A4 as A group by id having A \u003e 1000",
			"Backend": "backend4",
			"Range": "[96-256)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A5 as A group by id having A \u003e 1000",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A6 as A group by id having A \u003e 1000",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	],
	"Aggregate": [
		"A"
	],
	"HashGroupBy": [
		"id"
	]
}`,
	}
	querys := []string{
		"select 1, sum(a),avg(a),a,b from sbtest.A where id>1 group by a,b order by a desc limit 10 offset 100",
		"select id, sum(a) as A from sbtest.A group by id having A>1000",
	}

	// Database is null.
	{
		log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

		route, cleanup := router.MockNewRouter(log)
		defer cleanup()

		err := route.AddForTest("sbtest", router.MockTableMConfig(), router.MockTableBConfig())
		assert.Nil(t, err)
		for i, query := range querys {
			node, err := sqlparser.Parse(query)
			assert.Nil(t, err)
			plan := NewSelectPlan(log, "", query, node.(*sqlparser.Select), route)

			// plan build
			{
				log.Info("--select.query:%+v", query)
				err := plan.Build()
				assert.Nil(t, err)
				got := plan.JSON()
				want := results[i]
				assert.Equal(t, want, got)
				assert.Equal(t, PlanTypeSelect, plan.Type())
				assert.NotNil(t, plan.Children())
			}
		}
	}
}

func TestSelectUnsupportedPlan(t *testing.T) {
	querys := []string{
		"select * from A as A1 where id in (select id from B)",
		"select distinct(b) from A",
		"select A.id from A join B on B.id=A.id",
		"select id, rand(id) from A",
		"select id from A order by b",
		"select id from A limit x",
		"select age,count(*) from A group by age having count(*) >=2",
		"select id from A,b limit x",
	}
	results := []string{
		"unsupported: subqueries.in.select",
		"unsupported: distinct",
		"unsupported: JOIN.expression",
		"unsupported: function:rand",
		"unsupported: orderby[b].should.in.select.list",
		"unsupported: limit.offset.or.counts.must.be.IntVal",
		"unsupported: expr[count(*)].in.having.clause",
		"unsupported: subqueries.in.select",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewSelectPlan(log, database, query, node.(*sqlparser.Select), route)

		// plan build
		{
			err := plan.Build()
			want := results[i]
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}

func TestSelectPlanAs(t *testing.T) {
	results := []string{
		`{
	"RawQuery": "select a1.id  from A as a1 where a1.id\u003e1000",
	"Project": "a1.id",
	"Partitions": [
		{
			"Query": "select a1.id from sbtest.A1 as a1 where a1.id \u003e 1000",
			"Backend": "backend1",
			"Range": "[0-32)"
		},
		{
			"Query": "select a1.id from sbtest.A2 as a1 where a1.id \u003e 1000",
			"Backend": "backend2",
			"Range": "[32-64)"
		},
		{
			"Query": "select a1.id from sbtest.A3 as a1 where a1.id \u003e 1000",
			"Backend": "backend3",
			"Range": "[64-96)"
		},
		{
			"Query": "select a1.id from sbtest.A4 as a1 where a1.id \u003e 1000",
			"Backend": "backend4",
			"Range": "[96-256)"
		},
		{
			"Query": "select a1.id from sbtest.A5 as a1 where a1.id \u003e 1000",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "select a1.id from sbtest.A6 as a1 where a1.id \u003e 1000",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "select A.id  from A where A.id\u003e1000",
	"Project": "A.id",
	"Partitions": [
		{
			"Query": "select A.id from sbtest.A1 as A where A.id \u003e 1000",
			"Backend": "backend1",
			"Range": "[0-32)"
		},
		{
			"Query": "select A.id from sbtest.A2 as A where A.id \u003e 1000",
			"Backend": "backend2",
			"Range": "[32-64)"
		},
		{
			"Query": "select A.id from sbtest.A3 as A where A.id \u003e 1000",
			"Backend": "backend3",
			"Range": "[64-96)"
		},
		{
			"Query": "select A.id from sbtest.A4 as A where A.id \u003e 1000",
			"Backend": "backend4",
			"Range": "[96-256)"
		},
		{
			"Query": "select A.id from sbtest.A5 as A where A.id \u003e 1000",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "select A.id from sbtest.A6 as A where A.id \u003e 1000",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
	}
	querys := []string{
		"select a1.id  from A as a1 where a1.id>1000",
		"select A.id  from A where A.id>1000", // alias table is alse 'A'
	}

	// Database not null.
	{
		log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
		database := "sbtest"

		route, cleanup := router.MockNewRouter(log)
		defer cleanup()

		err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
		assert.Nil(t, err)
		for i, query := range querys {
			node, err := sqlparser.Parse(query)
			assert.Nil(t, err)
			plan := NewSelectPlan(log, database, query, node.(*sqlparser.Select), route)

			// plan build
			{
				log.Info("--select.query:%+v", query)
				err := plan.Build()
				assert.Nil(t, err)
				got := plan.JSON()
				log.Debug("---%+v", got)
				want := results[i]
				assert.Equal(t, want, got)
				assert.Equal(t, PlanTypeSelect, plan.Type())
				assert.NotNil(t, plan.Children())
			}
		}
	}
}
