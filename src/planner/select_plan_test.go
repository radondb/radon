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

func TestSelectPlan(t *testing.T) {
	results := []string{
		`{
	"RawQuery": "select 1, sum(a),avg(a),a,b from sbtest.A where id\u003e1 group by a,b order by a desc limit 10 offset 100",
	"Project": "1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b",
	"Partitions": [
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A1 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend1",
			"Range": "[0-32)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A2 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend2",
			"Range": "[32-64)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A3 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend3",
			"Range": "[64-96)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A4 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend4",
			"Range": "[96-256)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A5 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A6 as A where id \u003e 1 group by a, b order by a desc limit 110",
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
			"Query": "select id, sum(a) as A from sbtest.A1 as A group by id having A \u003e 1000 order by id asc",
			"Backend": "backend1",
			"Range": "[0-32)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A2 as A group by id having A \u003e 1000 order by id asc",
			"Backend": "backend2",
			"Range": "[32-64)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A3 as A group by id having A \u003e 1000 order by id asc",
			"Backend": "backend3",
			"Range": "[64-96)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A4 as A group by id having A \u003e 1000 order by id asc",
			"Backend": "backend4",
			"Range": "[96-256)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A5 as A group by id having A \u003e 1000 order by id asc",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A6 as A group by id having A \u003e 1000 order by id asc",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	],
	"Aggregate": [
		"A"
	],
	"GatherMerge": [
		"id"
	]
}`,
		`{
	"RawQuery": "select id,a from sbtest.A where (a\u003e1 and (id=1))",
	"Project": "id, a",
	"Partitions": [
		{
			"Query": "select id, a from sbtest.A6 as A where a \u003e 1 and id = 1",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "select A.id,B.id from A join B on A.id=B.id where A.id=1",
	"Project": "A.id, B.id",
	"Partitions": [
		{
			"Query": "select A.id from sbtest.A6 as A where A.id = 1",
			"Backend": "backend6",
			"Range": "[512-4096)"
		},
		{
			"Query": "select B.id from sbtest.B0 as B",
			"Backend": "backend1",
			"Range": "[0-512)"
		},
		{
			"Query": "select B.id from sbtest.B1 as B",
			"Backend": "backend2",
			"Range": "[512-4096)"
		}
	]
}`,
	}
	querys := []string{
		"select 1, sum(a),avg(a),a,b from sbtest.A where id>1 group by a,b order by a desc limit 10 offset 100",
		"select id, sum(a) as A from A group by id having A>1000",
		"select id,a from sbtest.A where (a>1 and (id=1))",
		"select A.id,B.id from A join B on A.id=B.id where A.id=1",
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
	"Project": "1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b",
	"Partitions": [
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A1 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend1",
			"Range": "[0-32)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A2 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend2",
			"Range": "[32-64)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A3 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend3",
			"Range": "[64-96)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A4 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend4",
			"Range": "[96-256)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A5 as A where id \u003e 1 group by a, b order by a desc limit 110",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A6 as A where id \u003e 1 group by a, b order by a desc limit 110",
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
			"Query": "select id, sum(a) as A from sbtest.A1 as A group by id having A \u003e 1000 order by id asc",
			"Backend": "backend1",
			"Range": "[0-32)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A2 as A group by id having A \u003e 1000 order by id asc",
			"Backend": "backend2",
			"Range": "[32-64)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A3 as A group by id having A \u003e 1000 order by id asc",
			"Backend": "backend3",
			"Range": "[64-96)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A4 as A group by id having A \u003e 1000 order by id asc",
			"Backend": "backend4",
			"Range": "[96-256)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A5 as A group by id having A \u003e 1000 order by id asc",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "select id, sum(a) as A from sbtest.A6 as A group by id having A \u003e 1000 order by id asc",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	],
	"Aggregate": [
		"A"
	],
	"GatherMerge": [
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
		"select * from A join B on B.id=A.id",
		"select id from A order by b",
		"select id from A limit x",
		"select age,count(*) from A group by age having count(*) >=2",
		"select * from A where B.a >1",
		"select count() from A",
		"select round(avg(id)) from A",
		"select id,group_concat(distinct name) from A group by id",
		"select next value for A",
		"select A.*,(select b.str from b where A.id=B.id) str from A",
		"select avg(id)*1000 from A",
		"select avg(*) from A",
		"select B.* from A",
		"select * from A where a>1 having count(a) >3",
		"select * from A join B on A.id=B.id join G on G.id=A.id where A.a>B.a",
		"select a,b from A group by B.a",
		"select A.id,G.a as a, concat(B.str,G.str), 1 from A,B, A as G group by a",
		"select *,avg(a) from A",
	}
	results := []string{
		"unsupported: subqueries.in.select",
		"unsupported: distinct",
		"unsupported: '*'.expression.in.cross-shard.query",
		"unsupported: orderby[b].should.in.select.list",
		"unsupported: limit.offset.or.counts.must.be.IntVal",
		"unsupported: expr[count(*)].in.having.clause",
		"unsupported: unknown.table.'B'.in.clause",
		"unsupported: invalid.use.of.group.function[count]",
		"unsupported: 'round(avg(id))'.contain.aggregate.in.select.exprs",
		"unsupported: group_concat.in.select.exprs",
		"unsupported: nextval.in.select.exprs",
		"unsupported: subqueries.in.select",
		"unsupported: 'avg(id) * 1000'.contain.aggregate.in.select.exprs",
		"unsupported: syntax.error.at.'avg(*)'",
		"unsupported:  unknown.table.'B'.in.field.list",
		"unsupported: expr[count(a)].in.having.clause",
		"unsupported: where.clause.in.cross-shard.join",
		"unsupported: unknow.table.in.group.by.field[B.a]",
		"unsupported: select.expr.in.cross-shard.join",
		"unsupported: exists.aggregate.and.'*'.select.exprs",
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

func TestSelectSupportedPlan(t *testing.T) {
	querys := []string{
		"select id,rand(id) from A",
		"select now() as time, count(1), avg(id), sum(b) from A",
		"select avg(id + 1) from A",
		"select concat(str1,str2) from A",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig())
	assert.Nil(t, err)
	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewSelectPlan(log, database, query, node.(*sqlparser.Select), route)

		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)

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

func TestSelectPlanDatabaseNotFound(t *testing.T) {
	querys := []string{
		"select * from A as A1 where id = 10",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	databaseNull := ""
	planTree := NewPlanTree()
	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		plan := NewSelectPlan(log, databaseNull, query, node.(*sqlparser.Select), route)
		{
			err := planTree.Add(plan)
			assert.Nil(t, err)
		}

		// plan build
		{
			err := planTree.Build()
			want := "No database selected (errno 1046) (sqlstate 3D000)"
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}

func TestSelectPlanGlobal(t *testing.T) {
	querys := []string{
		"select 1, sum(a),avg(a),a,b from sbtest.G where id>1 group by a,b order by a desc limit 10 offset 100",
		"select G.a, G.b from G join G1 on G.a = G1.a where G1.id=1",
		"select G.a, G.b from G, G1 where G.a = G1.a and G1.id=1",
	}

	{
		log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
		database := "sbtest"

		route, cleanup := router.MockNewRouter(log)
		defer cleanup()

		err := route.AddForTest(database, router.MockTableGConfig(), router.MockTableG1Config())
		assert.Nil(t, err)
		for _, query := range querys {
			node, err := sqlparser.Parse(query)
			assert.Nil(t, err)
			plan := NewSelectPlan(log, database, query, node.(*sqlparser.Select), route)

			// plan build
			{
				log.Info("--select.query:%+v", query)
				err := plan.Build()
				assert.Nil(t, err)
				want := 1
				assert.Equal(t, want, len(plan.Root.GetQuery()))
				assert.Equal(t, PlanTypeSelect, plan.Type())
				assert.NotNil(t, plan.Children())
			}
		}
	}
}

func TestSelectPlanJoin(t *testing.T) {
	results := []string{
		`{
	"RawQuery": "select G.a, G.b from G join B on G.a = B.a where B.id=1",
	"Project": "G.a, G.b",
	"Partitions": [
		{
			"Query": "select G.a, G.b from sbtest.G join sbtest.B1 as B on G.a = B.a where B.id = 1",
			"Backend": "backend2",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "select G.a, G.b from G join B on G.a = B.a join G1 on G1.a = B.a where B.id=1",
	"Project": "G.a, G.b",
	"Partitions": [
		{
			"Query": "select G.a, G.b from sbtest.G join sbtest.B1 as B on G.a = B.a join sbtest.G1 on G1.a = B.a where B.id = 1",
			"Backend": "backend2",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "select G.a, G.b from G, B where B.id=1",
	"Project": "G.a, G.b",
	"Partitions": [
		{
			"Query": "select G.a, G.b from sbtest.G, sbtest.B1 as B where B.id = 1",
			"Backend": "backend2",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "select G.a, B.a from G join B on G.a = B.a order by B.a",
	"Project": "G.a, B.a",
	"Partitions": [
		{
			"Query": "select G.a, B.a from sbtest.G join sbtest.B0 as B on G.a = B.a order by B.a asc",
			"Backend": "backend1",
			"Range": "[0-512)"
		},
		{
			"Query": "select G.a, B.a from sbtest.G join sbtest.B1 as B on G.a = B.a order by B.a asc",
			"Backend": "backend2",
			"Range": "[512-4096)"
		}
	],
	"GatherMerge": [
		"B.a"
	]
}`,
	}
	querys := []string{
		"select G.a, G.b from G join B on G.a = B.a where B.id=1",
		"select G.a, G.b from G join B on G.a = B.a join G1 on G1.a = B.a where B.id=1",
		"select G.a, G.b from G, B where B.id=1",
		"select G.a, B.a from G join B on G.a = B.a order by B.a",
	}

	{
		log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
		database := "sbtest"

		route, cleanup := router.MockNewRouter(log)
		defer cleanup()

		err := route.AddForTest(database, router.MockTableGConfig(), router.MockTableBConfig(), router.MockTableG1Config())
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

func TestSelectPlanJoinErr(t *testing.T) {
	querys := []string{
		"select C.a, C.b from sbtest.C join sbtest.G on G.id = C.id where C.id=1",
		"select G1.a, G1.b from sbtest.G1 join sbtest.B on G1.id = B.id where B.id=1",
		"select G1.a, G1.b from sbtest.G1 join sbtest.C on G1.id = C.id where C.id=1",
		"select * from B, (G join A on G.a=A.a) where A.a=1",
		"select * from B, A where A.id=1 and B.a=A.a",
	}
	results := []string{
		"Table 'C' doesn't exist (errno 1146) (sqlstate 42S02)",
		"Table 'G1' doesn't exist (errno 1146) (sqlstate 42S02)",
		"Table 'G1' doesn't exist (errno 1146) (sqlstate 42S02)",
		"unsupported: '*'.expression.in.cross-shard.query",
		"unsupported: '*'.expression.in.cross-shard.query",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableGConfig(), router.MockTableMConfig(), router.MockTableBConfig())
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
