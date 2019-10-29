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
	"Project": "1, sum(a), avg(a), a, b",
	"Partitions": [
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A1 as A where id \u003e 1 group by a, b order by a desc",
			"Backend": "backend1",
			"Range": "[0-32)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A2 as A where id \u003e 1 group by a, b order by a desc",
			"Backend": "backend2",
			"Range": "[32-64)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A3 as A where id \u003e 1 group by a, b order by a desc",
			"Backend": "backend3",
			"Range": "[64-96)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A4 as A where id \u003e 1 group by a, b order by a desc",
			"Backend": "backend4",
			"Range": "[96-256)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A5 as A where id \u003e 1 group by a, b order by a desc",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A6 as A where id \u003e 1 group by a, b order by a desc",
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
	"Project": "id, A",
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
	"Project": "id, id",
	"Partitions": [
		{
			"Query": "select A.id from sbtest.A6 as A where A.id = 1 order by A.id asc",
			"Backend": "backend6",
			"Range": "[512-4096)"
		},
		{
			"Query": "select B.id from sbtest.B1 as B where B.id = 1 order by B.id asc",
			"Backend": "backend2",
			"Range": "[512-4096)"
		}
	],
	"Join": {
		"Type": "INNER JOIN",
		"Strategy": "Sort Merge Join"
	}
}`,
		`{
	"RawQuery": "select A.id from A join B where A.id=1",
	"Project": "id",
	"Partitions": [
		{
			"Query": "select A.id from sbtest.A6 as A where A.id = 1",
			"Backend": "backend6",
			"Range": "[512-4096)"
		},
		{
			"Query": "select 1 from sbtest.B0 as B",
			"Backend": "backend1",
			"Range": "[0-512)"
		},
		{
			"Query": "select 1 from sbtest.B1 as B",
			"Backend": "backend2",
			"Range": "[512-4096)"
		}
	],
	"Join": {
		"Type": "CROSS JOIN",
		"Strategy": "Cartesian Join"
	}
}`,
		`{
	"RawQuery": "select A.id from A left join B on A.id=B.id and A.a=1 and B.b=2 and 1=1 where B.id=1",
	"Project": "id",
	"Partitions": [
		{
			"Query": "select A.id, A.a = 1 as tmpc_0 from sbtest.A6 as A where A.id = 1 order by A.id asc",
			"Backend": "backend6",
			"Range": "[512-4096)"
		},
		{
			"Query": "select B.id from sbtest.B1 as B where 1 = 1 and B.b = 2 and B.id = 1 order by B.id asc",
			"Backend": "backend2",
			"Range": "[512-4096)"
		}
	],
	"Join": {
		"Type": "LEFT JOIN",
		"Strategy": "Sort Merge Join"
	}
}`,
		`{
	"RawQuery": "select /*+nested+*/ A.id from A join B on A.id = B.id where A.id = 1",
	"Project": "id",
	"Partitions": [
		{
			"Query": "select /*+nested+*/ A.id from sbtest.A6 as A where A.id = 1",
			"Backend": "backend6",
			"Range": "[512-4096)"
		},
		{
			"Query": "select /*+nested+*/ 1 from sbtest.B1 as B where :A_id = B.id and B.id = 1",
			"Backend": "backend2",
			"Range": "[512-4096)"
		}
	],
	"Join": {
		"Type": "INNER JOIN",
		"Strategy": "Nested Loop Join"
	}
}`,
		`{
	"RawQuery": "select A.id from A left join B on A.a+1=B.a where A.id=1",
	"Project": "id",
	"Partitions": [
		{
			"Query": "select A.id, A.a + 1 as tmpo_0 from sbtest.A6 as A where A.id = 1 order by tmpo_0 asc",
			"Backend": "backend6",
			"Range": "[512-4096)"
		},
		{
			"Query": "select B.a from sbtest.B0 as B order by B.a asc",
			"Backend": "backend1",
			"Range": "[0-512)"
		},
		{
			"Query": "select B.a from sbtest.B1 as B order by B.a asc",
			"Backend": "backend2",
			"Range": "[512-4096)"
		}
	],
	"Join": {
		"Type": "LEFT JOIN",
		"Strategy": "Sort Merge Join"
	}
}`,
		`{
	"RawQuery": "select B.id as a from B group by a",
	"Project": "a",
	"Partitions": [
		{
			"Query": "select B.id as a from sbtest.B0 as B group by a order by a asc",
			"Backend": "backend1",
			"Range": "[0-512)"
		},
		{
			"Query": "select B.id as a from sbtest.B1 as B group by a order by a asc",
			"Backend": "backend2",
			"Range": "[512-4096)"
		}
	],
	"GatherMerge": [
		"a"
	]
}`,
		`{
	"RawQuery": "select avg(distinct id) as tmp,b,sum(id),count(id) from B group by b",
	"Project": "tmp, b, sum(id), count(id)",
	"Partitions": [
		{
			"Query": "select id as tmp, b, id as ` + "`sum(id)`" + `, id as ` + "`count(id)`" + ` from sbtest.B0 as B group by b order by b asc",
			"Backend": "backend1",
			"Range": "[0-512)"
		},
		{
			"Query": "select id as tmp, b, id as ` + "`sum(id)`" + `, id as ` + "`count(id)`" + ` from sbtest.B1 as B group by b order by b asc",
			"Backend": "backend2",
			"Range": "[512-4096)"
		}
	],
	"Aggregate": [
		"avg(distinct id)",
		"sum(id)",
		"count(id)"
	],
	"HashGroupBy": [
		"b"
	]
}`,
		`{
	"RawQuery": "select sum(A.a), B.b from A join B on A.id=B.id where A.id=1 group by B.b",
	"Project": "sum(A.a), b",
	"Partitions": [
		{
			"Query": "select A.a as ` + "`sum(A.a)`" + `, A.id from sbtest.A6 as A where A.id = 1 order by A.id asc",
			"Backend": "backend6",
			"Range": "[512-4096)"
		},
		{
			"Query": "select B.b, B.id from sbtest.B1 as B where B.id = 1 order by B.id asc",
			"Backend": "backend2",
			"Range": "[512-4096)"
		}
	],
	"Join": {
		"Type": "INNER JOIN",
		"Strategy": "Sort Merge Join"
	},
	"Aggregate": [
		"sum(A.a)"
	],
	"HashGroupBy": [
		"b"
	]
}`,
		`{
	"RawQuery": "select 1, sum(a),avg(a),a,b from sbtest.S where id\u003e1 group by a,b order by a desc limit 10 offset 100",
	"Project": "1, sum(a), avg(a), a, b",
	"Partitions": [
		{
			"Query": "select 1, sum(a), avg(a), a, b from sbtest.S where id \u003e 1 group by a, b order by a desc limit 100, 10",
			"Backend": "backend1",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "select sum(G.a), S.b from G join S on G.id=S.id where G.id\u003e1 group by S.b",
	"Project": "sum(G.a), b",
	"Partitions": [
		{
			"Query": "select sum(G.a), S.b from sbtest.G join sbtest.S on G.id = S.id where G.id \u003e 1 group by S.b",
			"Backend": "backend1",
			"Range": ""
		}
	]
}`,
		`{
	"RawQuery": "select sum(A.a), S.b from A join S on A.id=S.id where A.id=0 group by S.b",
	"Project": "sum(A.a), b",
	"Partitions": [
		{
			"Query": "select sum(A.a), S.b from sbtest.A1 as A join sbtest.S on A.id = S.id where A.id = 0 group by S.b",
			"Backend": "backend1",
			"Range": "[0-32)"
		}
	]
}`,
		`{
	"RawQuery": "select sum(A.a), S.b from A join S on A.id=S.id where A.id=1 group by S.b",
	"Project": "sum(A.a), b",
	"Partitions": [
		{
			"Query": "select A.a as ` + "`sum(A.a)`" + `, A.id from sbtest.A6 as A where A.id = 1 order by A.id asc",
			"Backend": "backend6",
			"Range": "[512-4096)"
		},
		{
			"Query": "select S.b, S.id from sbtest.S where S.id = 1 order by S.id asc",
			"Backend": "backend1",
			"Range": ""
		}
	],
	"Join": {
		"Type": "INNER JOIN",
		"Strategy": "Sort Merge Join"
	},
	"Aggregate": [
		"sum(A.a)"
	],
	"HashGroupBy": [
		"b"
	]
}`,
		`{
	"RawQuery": "select * from A where id=1 or 2=id",
	"Project": "*",
	"Partitions": [
		{
			"Query": "select * from sbtest.A6 as A where id in (1, 2)",
			"Backend": "backend6",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "select * from B where B.id=1 or B.id=2 or (B.id=0 and B.name='a')",
	"Project": "*",
	"Partitions": [
		{
			"Query": "select * from sbtest.B0 as B where (B.id = 0 and B.name = 'a' or B.id in (1, 2))",
			"Backend": "backend1",
			"Range": "[0-512)"
		},
		{
			"Query": "select * from sbtest.B1 as B where (B.id = 0 and B.name = 'a' or B.id in (1, 2))",
			"Backend": "backend2",
			"Range": "[512-4096)"
		}
	]
}`,
		`{
	"RawQuery": "select A.id,B.id from A join B on A.id=B.id where A.id=0 or A.id=1 or A.id=2",
	"Project": "id, id",
	"Partitions": [
		{
			"Query": "select A.id from sbtest.A1 as A where A.id in (0, 1, 2) order by A.id asc",
			"Backend": "backend1",
			"Range": "[0-32)"
		},
		{
			"Query": "select A.id from sbtest.A6 as A where A.id in (0, 1, 2) order by A.id asc",
			"Backend": "backend6",
			"Range": "[512-4096)"
		},
		{
			"Query": "select B.id from sbtest.B0 as B where B.id in (0, 1, 2) order by B.id asc",
			"Backend": "backend1",
			"Range": "[0-512)"
		},
		{
			"Query": "select B.id from sbtest.B1 as B where B.id in (0, 1, 2) order by B.id asc",
			"Backend": "backend2",
			"Range": "[512-4096)"
		}
	],
	"Join": {
		"Type": "INNER JOIN",
		"Strategy": "Sort Merge Join"
	}
}`,
	}
	querys := []string{
		"select 1, sum(a),avg(a),a,b from sbtest.A where id>1 group by a,b order by a desc limit 10 offset 100",
		"select id, sum(a) as A from A group by id having A>1000",
		"select id,a from sbtest.A where (a>1 and (id=1))",
		"select A.id,B.id from A join B on A.id=B.id where A.id=1",
		"select A.id from A join B where A.id=1",
		"select A.id from A left join B on A.id=B.id and A.a=1 and B.b=2 and 1=1 where B.id=1",
		"select /*+nested+*/ A.id from A join B on A.id = B.id where A.id = 1",
		"select A.id from A left join B on A.a+1=B.a where A.id=1",
		"select B.id as a from B group by a",
		"select avg(distinct id) as tmp,b,sum(id),count(id) from B group by b",
		"select sum(A.a), B.b from A join B on A.id=B.id where A.id=1 group by B.b",
		"select 1, sum(a),avg(a),a,b from sbtest.S where id>1 group by a,b order by a desc limit 10 offset 100",
		"select sum(G.a), S.b from G join S on G.id=S.id where G.id>1 group by S.b",
		"select sum(A.a), S.b from A join S on A.id=S.id where A.id=0 group by S.b",
		"select sum(A.a), S.b from A join S on A.id=S.id where A.id=1 group by S.b",
		"select * from A where id=1 or 2=id",
		"select * from B where B.id=1 or B.id=2 or (B.id=0 and B.name='a')",
		"select A.id,B.id from A join B on A.id=B.id where A.id=0 or A.id=1 or A.id=2",
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
	"Project": "1, sum(a), avg(a), a, b",
	"Partitions": [
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A1 as A where id \u003e 1 group by a, b order by a desc",
			"Backend": "backend1",
			"Range": "[0-32)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A2 as A where id \u003e 1 group by a, b order by a desc",
			"Backend": "backend2",
			"Range": "[32-64)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A3 as A where id \u003e 1 group by a, b order by a desc",
			"Backend": "backend3",
			"Range": "[64-96)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A4 as A where id \u003e 1 group by a, b order by a desc",
			"Backend": "backend4",
			"Range": "[96-256)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A5 as A where id \u003e 1 group by a, b order by a desc",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A6 as A where id \u003e 1 group by a, b order by a desc",
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
	"Project": "id, A",
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
		"select a,b from A group by B.a",
		"select A.id,G.a as a, concat(B.str,G.str), 1 from A,B, A as G group by a",
		"select *,avg(a) from A",
		"select A.id from A join B on A.id=B.id right join G on G.id=A.id and A.a>B.a",
		"select A.id from (A,B) left join G on A.id =G.id and A.a>B.a",
		"select A.id from A join B on A.id=B.id right join G on G.id=A.id where concat(B.str,A.str) is null",
		"select A.id from A join B on A.id >= B.id join G on G.id<=A.id where concat(B.str,A.str) is null",
		"select A.id from A join B on A.id = B.id join G on G.id<=A.id+B.id",
		"select A.id from A join B on A.id = B.id join G on A.id+B.id<=G.id",
		"select A.id from G join (A,B) on A.id+B.id<=G.id",
		"select A.id from G join (A,B) on G.id<=A.id+B.id",
		"select A.id as tmp, B.id from A,B having tmp=1",
		"select COALESCE(B.b, ''), IF(B.b IS NULL, FALSE, TRUE) AS spent from A left join B on A.a=B.a",
		"select A.a as b from A order by A.b",
		"select a+1 from A order by a+1",
		"select b as a from A group by A.a",
		"select a+1 from A group by a+1",
		"select count(distinct *) from A",
		"select t1.a from G",
		"select A.id from A join B on A.id=B.id where A.id in (1,2) or B.a=1",
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
		"unsupported: unknow.table.in.group.by.field[B.a]",
		"unsupported: expr.'concat(B.str, G.str)'.in.cross-shard.join",
		"unsupported: exists.aggregate.and.'*'.select.exprs",
		"unsupported: on.clause.'A.a > B.a'.in.cross-shard.join",
		"unsupported: expr.'A.a > B.a'.in.cross-shard.join",
		"unsupported: expr.'concat(B.str, A.str)'.in.cross-shard.join",
		"unsupported: clause.'concat(B.str, A.str) is null'.in.cross-shard.join",
		"unsupported: expr.'A.id + B.id'.in.cross-shard.join",
		"unsupported: expr.'A.id + B.id'.in.cross-shard.join",
		"unsupported: expr.'A.id + B.id'.in.cross-shard.join",
		"unsupported: expr.'A.id + B.id'.in.cross-shard.join",
		"unsupported: unknown.column.'tmp'.in.having.clause",
		"unsupported: expr.'COALESCE(B.b, '')'.in.cross-shard.left.join",
		"unsupported: orderby[A.b].should.in.select.list",
		"unsupported: orderby:[a + 1].type.should.be.colname",
		"unsupported: group.by.field[A.a].should.be.in.select.list",
		"unsupported: group.by.[a + 1].type.should.be.colname",
		"unsupported: syntax.error.at.'count(distinct *)'",
		"unsupported: unknown.column.'t1.a'.in.exprs",
		"unsupported: clause.'A.id in (1, 2) or B.a in (1)'.in.cross-shard.join",
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
		"select * from A join B on A.id=B.id where A.id=0",
		"select A.id from A,B,B as C where B.id = 0 and A.id=C.id and A.id=0",
		"select A.id from A,A as B where A.id=B.id and A.a=1",
		"select A.id from A join B on A.id = B.id join G on G.id=A.id and A.id>1 and G.id=3",
		"select A.id from A left join B on A.id=B.id where B.str is null",
		"select A.id from A left join B on A.id=B.id where B.str<=>null",
		"select A.id from A left join B on A.id=B.id where null<=>B.str",
		"select A.id from A join B on A.id >= B.id join G on G.id<=A.id",
		"select /*+nested+*/ A.id from A join B on A.id=B.id right join G on G.id=A.id and A.a>B.a",
		"select /*+nested+*/ A.id from (A,B) left join G on A.id =G.id and A.a>B.a",
		"select /*+nested+*/ A.id from A join B on A.id=B.id right join G on G.id=A.id where concat(B.str,A.str) is null",
		"select /*+nested+*/ A.id from A join B on A.id >= B.id join G on G.id<=A.id where concat(B.str,A.str) is null",
		"select /*+nested+*/ A.id from A join B on A.id = B.id join G on G.id<=A.id+B.id",
		"select /*+nested+*/ A.id from A join B on A.id = B.id join G on A.id+B.id<=G.id",
		"select /*+nested+*/ A.id from G join (A,B) on A.id+B.id<=G.id",
		"select /*+nested+*/ A.id from G join (A,B) on G.id<=A.id+B.id",
		"select /*+nested+*/ sum(A.id) from A join B on A.id=B.id",
		"select /*+nested+*/ A.id from G,A,B where A.id=B.id having G.id=B.id and B.a=1 and 1=1",
		"select COALESCE(A.b, ''), IF(A.b IS NULL, FALSE, TRUE) AS spent from A left join B on A.a=B.a",
		"select COALESCE(B.b, ''), IF(B.b IS NULL, FALSE, TRUE) AS spent from A join B on A.a=B.a",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
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
	"Project": "id",
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
	"Project": "id",
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
	"Project": "a, b",
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
	"Project": "a, b",
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
	"Project": "a, b",
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
	"Project": "a, a",
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
		`{
	"RawQuery": "select * from B join B as A where A.a=B.a and A.id=B.id",
	"Project": "*",
	"Partitions": [
		{
			"Query": "select * from sbtest.B0 as B, sbtest.B0 as A where A.a = B.a and A.id = B.id",
			"Backend": "backend1",
			"Range": "[0-512)"
		},
		{
			"Query": "select * from sbtest.B1 as B, sbtest.B1 as A where A.a = B.a and A.id = B.id",
			"Backend": "backend2",
			"Range": "[512-4096)"
		}
	]
}`,
	}
	querys := []string{
		"select G.a, G.b from G join B on G.a = B.a where B.id=1",
		"select G.a, G.b from G join B on G.a = B.a join G1 on G1.a = B.a where B.id=1",
		"select G.a, G.b from G, B where B.id=1",
		"select G.a, B.a from G join B on G.a = B.a order by B.a",
		"select * from B join B as A where A.a=B.a and A.id=B.id",
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

func TestGenerateFieldQuery(t *testing.T) {
	query := "select /*+nested+*/ A.id+B.id from A join B on A.name=B.name"
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	plan := NewSelectPlan(log, database, query, node.(*sqlparser.Select), route)
	err = plan.Build()
	assert.Nil(t, err)

	got := plan.Root.(*JoinNode).Right.(*MergeNode).GenerateFieldQuery().Query
	want := "select :A_id + B.id from sbtest.B1 as B where 1 != 1"
	assert.Equal(t, want, got)
}
