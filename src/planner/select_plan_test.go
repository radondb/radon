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
	"RawQuery": "select 1, sum(a),avg(a),a,b from sbtest.A where id\u003e1 group by a,b order by A.a desc limit 10 offset 100",
	"Project": "1, sum(a), avg(a), a, b",
	"Partitions": [
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A1 as A where id \u003e 1 group by a, b order by A.a desc",
			"Backend": "backend1",
			"Range": "[0-32)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A2 as A where id \u003e 1 group by a, b order by A.a desc",
			"Backend": "backend2",
			"Range": "[32-64)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A3 as A where id \u003e 1 group by a, b order by A.a desc",
			"Backend": "backend3",
			"Range": "[64-96)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A4 as A where id \u003e 1 group by a, b order by A.a desc",
			"Backend": "backend4",
			"Range": "[96-256)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A5 as A where id \u003e 1 group by a, b order by A.a desc",
			"Backend": "backend5",
			"Range": "[256-512)"
		},
		{
			"Query": "select 1, sum(a), sum(a) as ` + "`avg(a)`" + `, count(a), a, b from sbtest.A6 as A where id \u003e 1 group by a, b order by A.a desc",
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
		"A.a"
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
			"Query": "select B.id from sbtest.B1 as B where B.id = 1 and 1 = 1 and B.b = 2 order by B.id asc",
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
			"Query": "select /*+nested+*/ 1 from sbtest.B1 as B where B.id = 1 and :A_id = B.id",
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
			"Query": "select A.id, A.a + 1 as tmpc_0 from sbtest.A6 as A where A.id = 1 order by tmpc_0 asc",
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
			"Query": "select sum(A.a), S.b from sbtest.A1 as A join sbtest.S on A.id = S.id where A.id = 0 and S.id = 0 group by S.b",
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
		"select 1, sum(a),avg(a),a,b from sbtest.A where id>1 group by a,b order by A.a desc limit 10 offset 100",
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
				plan.Size()
			}
		}
	}
}

func TestSelectUnsupportedPlan(t *testing.T) {
	querys := []string{
		"select * from A as A1 where id in (select id from B)",
		"select A.*,(select b.str from b where A.id=B.id) str from A",
	}
	results := []string{
		"unsupported: subqueries.in.select",
		"unsupported: subqueries.in.select",
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
