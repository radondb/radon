/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"backend"
	"testing"

	"router"
	"xcontext"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	descResult = &sqltypes.Result{
		RowsAffected: 2,
		Fields: []*querypb.Field{
			{
				Name: "Field",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "type",
				Type: querypb.Type_INT24,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("id")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("int(11)")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("name")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("int(11)")),
			},
		},
	}
)

func TestProcessSelect(t *testing.T) {
	tcases := []struct {
		query   string
		project string
		out     []xcontext.QueryTuple
	}{
		{
			query:   "select 1, sum(a),avg(a),a,b from sbtest.A where id > 1 group by a,b order by a desc limit 10 offset 100",
			project: "1, sum(a), avg(a), a, b",
			out: []xcontext.QueryTuple{
				{
					Query:   "select 1, sum(a), sum(a) as `avg(a)`, count(a), a, b from sbtest.A1 as A where id > 1 group by a, b order by a desc",
					Backend: "backend1",
					Range:   "[0-32)",
				},
				{
					Query:   "select 1, sum(a), sum(a) as `avg(a)`, count(a), a, b from sbtest.A2 as A where id > 1 group by a, b order by a desc",
					Backend: "backend2",
					Range:   "[32-64)",
				},
				{
					Query:   "select 1, sum(a), sum(a) as `avg(a)`, count(a), a, b from sbtest.A3 as A where id > 1 group by a, b order by a desc",
					Backend: "backend3",
					Range:   "[64-96)",
				},
				{
					Query:   "select 1, sum(a), sum(a) as `avg(a)`, count(a), a, b from sbtest.A4 as A where id > 1 group by a, b order by a desc",
					Backend: "backend4",
					Range:   "[96-256)",
				},
				{
					Query:   "select 1, sum(a), sum(a) as `avg(a)`, count(a), a, b from sbtest.A5 as A where id > 1 group by a, b order by a desc",
					Backend: "backend5",
					Range:   "[256-512)",
				},
				{
					Query:   "select 1, sum(a), sum(a) as `avg(a)`, count(a), a, b from sbtest.A6 as A where id > 1 group by a, b order by a desc",
					Backend: "backend6",
					Range:   "[512-4096)",
				}},
		},
		{
			query:   "select id, sum(a) as A from A group by id having id>1000",
			project: "id, A",
			out: []xcontext.QueryTuple{
				{
					Query:   "select id, sum(a) as A from sbtest.A1 as A group by id having id > 1000 order by id asc",
					Backend: "backend1",
					Range:   "[0-32)",
				},
				{
					Query:   "select id, sum(a) as A from sbtest.A2 as A group by id having id > 1000 order by id asc",
					Backend: "backend2",
					Range:   "[32-64)",
				},
				{
					Query:   "select id, sum(a) as A from sbtest.A3 as A group by id having id > 1000 order by id asc",
					Backend: "backend3",
					Range:   "[64-96)",
				},
				{
					Query:   "select id, sum(a) as A from sbtest.A4 as A group by id having id > 1000 order by id asc",
					Backend: "backend4",
					Range:   "[96-256)",
				},
				{
					Query:   "select id, sum(a) as A from sbtest.A5 as A group by id having id > 1000 order by id asc",
					Backend: "backend5",
					Range:   "[256-512)",
				},
				{
					Query:   "select id, sum(a) as A from sbtest.A6 as A group by id having id > 1000 order by id asc",
					Backend: "backend6",
					Range:   "[512-4096)",
				}},
		},
		{
			query:   "select id,a from sbtest.A where (a>1 and (id=1))",
			project: "id, a",
			out: []xcontext.QueryTuple{
				{
					Query:   "select id, a from sbtest.A6 as A where a > 1 and id = 1",
					Backend: "backend6",
					Range:   "[512-4096)",
				}},
		},
		{
			query:   "select A.id,B.id from A join B on A.id=B.id where A.id=1",
			project: "id, id",
			out: []xcontext.QueryTuple{
				{
					Query:   "select A.id from sbtest.A6 as A where A.id = 1 order by A.id asc",
					Backend: "backend6",
					Range:   "[512-4096)",
				},
				{
					Query:   "select B.id from sbtest.B1 as B where B.id = 1 order by B.id asc",
					Backend: "backend2",
					Range:   "[512-4096)",
				}},
		},
		{
			query:   "select A.id from A join B where A.id=1",
			project: "id",
			out: []xcontext.QueryTuple{
				{
					Query:   "select A.id from sbtest.A6 as A where A.id = 1",
					Backend: "backend6",
					Range:   "[512-4096)",
				},
				{
					Query:   "select 1 from sbtest.B0 as B",
					Backend: "backend1",
					Range:   "[0-512)",
				},
				{
					Query:   "select 1 from sbtest.B1 as B",
					Backend: "backend2",
					Range:   "[512-4096)",
				}},
		},
		{
			query:   "select A.id from A left join B on A.id=B.id and A.a=1 and B.b=2 and 1=1 where B.id=1",
			project: "id",
			out: []xcontext.QueryTuple{
				{
					Query:   "select A.id, A.a = 1 as tmpc_0 from sbtest.A6 as A where A.id = 1 order by A.id asc",
					Backend: "backend6",
					Range:   "[512-4096)",
				},
				{
					Query:   "select B.id from sbtest.B1 as B where B.id = 1 and 1 = 1 and B.b = 2 order by B.id asc",
					Backend: "backend2",
					Range:   "[512-4096)",
				}},
		},
		{
			query:   "select A.id from A join B on A.id = B.id and concat(A.str,B.str) = 'golang' where A.id = 1",
			project: "id",
			out: []xcontext.QueryTuple{
				{
					Query:   "select A.id, A.str from sbtest.A6 as A where A.id = 1",
					Backend: "backend6",
					Range:   "[512-4096)",
				},
				{
					Query:   "select 1 from sbtest.B1 as B where B.id = 1 and concat(:A_str, B.str) = 'golang' and :A_id = B.id",
					Backend: "backend2",
					Range:   "[512-4096)",
				}},
		},
		{
			query:   "select A.id from A left join B on A.a+1=B.a where A.id=1",
			project: "id",
			out: []xcontext.QueryTuple{
				{
					Query:   "select A.id, A.a + 1 as tmpc_0 from sbtest.A6 as A where A.id = 1 order by tmpc_0 asc",
					Backend: "backend6",
					Range:   "[512-4096)",
				},
				{
					Query:   "select B.a from sbtest.B0 as B order by B.a asc",
					Backend: "backend1",
					Range:   "[0-512)",
				},
				{
					Query:   "select B.a from sbtest.B1 as B order by B.a asc",
					Backend: "backend2",
					Range:   "[512-4096)",
				}},
		},
		{
			query:   "select B.id as a from B group by a",
			project: "a",
			out: []xcontext.QueryTuple{
				{
					Query:   "select B.id as a from sbtest.B0 as B group by a order by a asc",
					Backend: "backend1",
					Range:   "[0-512)",
				},
				{
					Query:   "select B.id as a from sbtest.B1 as B group by a order by a asc",
					Backend: "backend2",
					Range:   "[512-4096)",
				}},
		},
		{
			query:   "select avg(distinct id) as tmp,b,sum(id),count(id) from B group by b",
			project: "tmp, b, sum(id), count(id)",
			out: []xcontext.QueryTuple{
				{
					Query:   "select id as tmp, b, id as `sum(id)`, id as `count(id)` from sbtest.B0 as B group by b order by b asc",
					Backend: "backend1",
					Range:   "[0-512)",
				},
				{
					Query:   "select id as tmp, b, id as `sum(id)`, id as `count(id)` from sbtest.B1 as B group by b order by b asc",
					Backend: "backend2",
					Range:   "[512-4096)",
				}},
		},
		{
			query:   "select sum(A.a), B.b from A join B on A.id=B.id where A.id=1 group by B.b",
			project: "sum(A.a), b",
			out: []xcontext.QueryTuple{
				{
					Query:   "select A.a as `sum(A.a)`, A.id from sbtest.A6 as A where A.id = 1 order by A.id asc",
					Backend: "backend6",
					Range:   "[512-4096)",
				},
				{
					Query:   "select B.b, B.id from sbtest.B1 as B where B.id = 1 order by B.id asc",
					Backend: "backend2",
					Range:   "[512-4096)",
				}},
		},
		{
			query:   "select 1, sum(a),avg(a),a,b from sbtest.S where id>1 group by a,b order by a desc limit 10 offset 100",
			project: "1, sum(a), avg(a), a, b",
			out: []xcontext.QueryTuple{
				{
					Query:   "select 1, sum(a), avg(a), a, b from sbtest.S where id > 1 group by a, b order by a desc limit 100, 10",
					Backend: "backend1",
					Range:   "",
				}},
		},
		{
			query:   "select sum(G.a), S.b from G join S on G.id=S.id where G.id>1 group by S.b",
			project: "sum(G.a), b",
			out: []xcontext.QueryTuple{
				{
					Query:   "select sum(G.a), S.b from sbtest.G join sbtest.S on G.id = S.id where G.id > 1 group by S.b",
					Backend: "backend1",
					Range:   "",
				}},
		},
		{
			query:   "select sum(A.a), S.b from A join S on A.id=S.id where A.id=0 group by S.b",
			project: "sum(A.a), b",
			out: []xcontext.QueryTuple{
				{
					Query:   "select sum(A.a), S.b from sbtest.A1 as A join sbtest.S on A.id = S.id where A.id = 0 and S.id = 0 group by S.b",
					Backend: "backend1",
					Range:   "[0-32)",
				}},
		},

		{
			query:   "select sum(A.a), S.b from A join S on A.id=S.id where A.id=1 group by S.b",
			project: "sum(A.a), b",
			out: []xcontext.QueryTuple{
				{
					Query:   "select A.a as `sum(A.a)`, A.id from sbtest.A6 as A where A.id = 1 order by A.id asc",
					Backend: "backend6",
					Range:   "[512-4096)",
				},
				{
					Query:   "select S.b, S.id from sbtest.S where S.id = 1 order by S.id asc",
					Backend: "backend1",
					Range:   "",
				}},
		},

		{
			query:   "select * from A where id=1 or 2=id",
			project: "id, name",
			out: []xcontext.QueryTuple{
				{
					Query:   "select * from sbtest.A6 as A where id in (1, 2)",
					Backend: "backend6",
					Range:   "[512-4096)",
				}},
		},
		{
			query:   "select * from B where B.id=1 or B.id=2 or (B.id=0 and B.name='a')",
			project: "id, name",
			out: []xcontext.QueryTuple{
				{
					Query:   "select B.id, B.name from sbtest.B0 as B where (B.id = 0 and B.name = 'a' or B.id in (1, 2))",
					Backend: "backend1",
					Range:   "[0-512)",
				},
				{
					Query:   "select B.id, B.name from sbtest.B1 as B where (B.id = 0 and B.name = 'a' or B.id in (1, 2))",
					Backend: "backend2",
					Range:   "[512-4096)",
				}},
		},

		{
			query:   "select A.id,B.id from A join B on A.id=B.id where A.id=0 or A.id=1 or A.id=2",
			project: "id, id",
			out: []xcontext.QueryTuple{
				{
					Query:   "select A.id from sbtest.A1 as A where A.id in (0, 1, 2) order by A.id asc",
					Backend: "backend1",
					Range:   "[0-32)",
				},
				{
					Query:   "select A.id from sbtest.A6 as A where A.id in (0, 1, 2) order by A.id asc",
					Backend: "backend6",
					Range:   "[512-4096)",
				},
				{
					Query:   "select B.id from sbtest.B0 as B where B.id in (0, 1, 2) order by B.id asc",
					Backend: "backend1",
					Range:   "[0-512)",
				},
				{
					Query:   "select B.id from sbtest.B1 as B where B.id in (0, 1, 2) order by B.id asc",
					Backend: "backend2",
					Range:   "[512-4096)",
				}},
		},
		{
			query:   "select S.a from A join B on A.id=B.id join S on A.a+B.a>S.a where A.id=1",
			project: "a",
			out: []xcontext.QueryTuple{
				{
					Query:   "select A.a, A.id from sbtest.A6 as A where A.id = 1",
					Backend: "backend6",
					Range:   "[512-4096)",
				},
				{
					Query:   "select B.a from sbtest.B1 as B where B.id = 1 and :A_id = B.id",
					Backend: "backend2",
					Range:   "[512-4096)",
				},
				{
					Query:   "select S.a from sbtest.S where :A_a + :B_a > S.a",
					Backend: "backend1",
					Range:   "",
				}},
		},
	}

	// Database not null.
	{
		log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
		database := "sbtest"

		scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
		defer cleanup()
		fakedbs.AddQueryPattern("desc .*", descResult)

		route, cleanup := router.MockNewRouter(log)
		defer cleanup()

		err := route.CreateDatabase(database)
		assert.Nil(t, err)
		err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableSConfig(), router.MockTableGConfig())
		assert.Nil(t, err)
		for _, tcase := range tcases {
			node, err := sqlparser.Parse(tcase.query)
			assert.Nil(t, err)

			// plan build
			{
				log.Info("--select.query:%+v", tcase.query)
				plan, err := BuildNode(log, route, scatter, database, node.(sqlparser.SelectStatement))
				assert.Nil(t, err)
				q := plan.GetQuery()
				assert.Equal(t, tcase.out, q)
				plan.Children()
				assert.Equal(t, tcase.project, GetProject(plan))
			}
		}
	}
}

func TestSelectDatabaseIsNull(t *testing.T) {
	tcases := []struct {
		query string
		out   []xcontext.QueryTuple
	}{
		{
			query: "select 1, sum(a),avg(a),a,b from sbtest.A where id > 1 group by a,b order by a desc limit 10 offset 100",
			out: []xcontext.QueryTuple{
				{
					Query:   "select 1, sum(a), sum(a) as `avg(a)`, count(a), a, b from sbtest.A1 as A where id > 1 group by a, b order by a desc",
					Backend: "backend1",
					Range:   "[0-32)",
				},
				{
					Query:   "select 1, sum(a), sum(a) as `avg(a)`, count(a), a, b from sbtest.A2 as A where id > 1 group by a, b order by a desc",
					Backend: "backend2",
					Range:   "[32-64)",
				},
				{
					Query:   "select 1, sum(a), sum(a) as `avg(a)`, count(a), a, b from sbtest.A3 as A where id > 1 group by a, b order by a desc",
					Backend: "backend3",
					Range:   "[64-96)",
				},
				{
					Query:   "select 1, sum(a), sum(a) as `avg(a)`, count(a), a, b from sbtest.A4 as A where id > 1 group by a, b order by a desc",
					Backend: "backend4",
					Range:   "[96-256)",
				},
				{
					Query:   "select 1, sum(a), sum(a) as `avg(a)`, count(a), a, b from sbtest.A5 as A where id > 1 group by a, b order by a desc",
					Backend: "backend5",
					Range:   "[256-512)",
				},
				{
					Query:   "select 1, sum(a), sum(a) as `avg(a)`, count(a), a, b from sbtest.A6 as A where id > 1 group by a, b order by a desc",
					Backend: "backend6",
					Range:   "[512-4096)",
				}},
		},
		{
			query: "select id, sum(a) as A from sbtest.A group by id having id>1000",
			out: []xcontext.QueryTuple{
				{
					Query:   "select id, sum(a) as A from sbtest.A1 as A group by id having id > 1000 order by id asc",
					Backend: "backend1",
					Range:   "[0-32)",
				},
				{
					Query:   "select id, sum(a) as A from sbtest.A2 as A group by id having id > 1000 order by id asc",
					Backend: "backend2",
					Range:   "[32-64)",
				},
				{
					Query:   "select id, sum(a) as A from sbtest.A3 as A group by id having id > 1000 order by id asc",
					Backend: "backend3",
					Range:   "[64-96)",
				},
				{
					Query:   "select id, sum(a) as A from sbtest.A4 as A group by id having id > 1000 order by id asc",
					Backend: "backend4",
					Range:   "[96-256)",
				},
				{
					Query:   "select id, sum(a) as A from sbtest.A5 as A group by id having id > 1000 order by id asc",
					Backend: "backend5",
					Range:   "[256-512)",
				},
				{
					Query:   "select id, sum(a) as A from sbtest.A6 as A group by id having id > 1000 order by id asc",
					Backend: "backend6",
					Range:   "[512-4096)",
				}},
		},
	}

	// Database is null.
	{
		log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

		route, cleanup := router.MockNewRouter(log)
		defer cleanup()

		err := route.CreateDatabase("sbtest")
		assert.Nil(t, err)
		err = route.AddForTest("sbtest", router.MockTableMConfig())
		assert.Nil(t, err)
		for _, tcase := range tcases {
			node, err := sqlparser.Parse(tcase.query)
			assert.Nil(t, err)

			// plan build
			{
				log.Info("--select.query:%+v", tcase.query)
				plan, err := BuildNode(log, route, nil, "", node.(sqlparser.SelectStatement))
				assert.Nil(t, err)
				q := plan.GetQuery()
				assert.Equal(t, tcase.out, q)
				plan.Children()
			}
		}
	}
}

func TestSelectUnsupported(t *testing.T) {
	querys := []string{
		"select * from A as A1 where id in (select id from B)",
		"select distinct(b) from A",
		"select id from A limit x",
		"select age,count(*) from A group by age having count(*) >=2",
		"select * from A where B.a >1",
		"select count() from A",
		"select round(avg(id)) from A",
		"select id,group_concat(distinct name) from A group by id",
		"select next value for A",
		"select A.*,(select b.str from b where A.id=B.id) str from A",
		"select avg(*) from A",
		"select B.* from A",
		"select * from D,A",
		"select * from A where a>1 having count(a) >3",
		"select a,b from A group by B.a",
		"select sum(A.id) as tmp, B.id from A,B having tmp=1",
		"select COALESCE(B.b, ''), IF(B.b IS NULL, FALSE, TRUE) AS spent from A left join B on A.a=B.a",
		"select abs(B.a) AS spent,G.a from A left join B on A.a=B.a,G",
		"select abs(B.a) AS spent,G.a from G,A left join B on A.a=B.a",
		"select A.id from A left join B on A.id=B.id right join G on A.id = G.id where length(B.str) is null",
		"select A.id from A left join B on A.id=B.id left join G on A.id = G.id and abs(B.a)",
		"select A.id from A left join B on A.id=B.id join G on A.id = G.id and abs(B.a) > G.a",
		"select A.id from A left join B on A.id=B.id join G on A.id = G.id and G.a < abs(B.a)",
		"select A.id from A left join B on A.id=B.id left join G on A.id = G.id and G.a < abs(B.a)",
		"select A.id from G, A left join B on A.id=B.id where G.a < abs(B.a)",
		"select A.id from G, A left join B on A.id=B.id where abs(B.a) > G.a",
		"select A.id from (G, A left join B on A.id=B.id),C where abs(B.a) > G.a",
		"select A.id from C,(G, A left join B on A.id=B.id) where abs(B.a+B.b) > G.a",
		"select a+1 from A order by a+1",
		"select b as a from A group by A.a",
		"select a+1 from A group by a+1",
		"select count(distinct *) from A",
		"select t1.a from G",
		"select S.id from A join B on B.id=A.id",
		"select eeeee from A join B on B.id=A.id",
	}
	results := []string{
		"unsupported: subqueries.in.select",
		"unsupported: distinct",
		"unsupported: limit.offset.or.counts.must.be.IntVal",
		"unsupported: expr[count(*)].in.having.clause",
		"unsupported: unknown.column.'B.a'.in.clause",
		"unsupported: invalid.use.of.group.function[count]",
		"Unsupported Expression:round",
		"unsupported: group_concat.in.select.exprs",
		"unsupported: nextval.in.select.exprs",
		"unsupported: subqueries.in.select.exprs",
		"unsupported: syntax.error.at.'avg'",
		"unsupported: unknown.table.'B'.in.field.list",
		"Table 'D' doesn't exist (errno 1146) (sqlstate 42S02)",
		"unsupported: expr[count(a)].in.having.clause",
		"unsupported: unknow.table.in.group.by.field[B.a]",
		"unsupported: aggregation.in.having.clause",
		"unsupported: expr.'COALESCE(B.b, '')'.in.cross-shard.left.join",
		"unsupported: expr.'abs(B.a)'.in.cross-shard.left.join",
		"unsupported: expr.'abs(B.a)'.in.cross-shard.left.join",
		"unsupported: expr.'length(B.str)'.in.cross-shard.left.join",
		"unsupported: expr.'abs(B.a)'.in.cross-shard.left.join",
		"unsupported: expr.'abs(B.a)'.in.cross-shard.left.join",
		"unsupported: expr.'abs(B.a)'.in.cross-shard.left.join",
		"unsupported: expr.'abs(B.a)'.in.cross-shard.left.join",
		"unsupported: expr.'abs(B.a)'.in.cross-shard.left.join",
		"unsupported: expr.'abs(B.a)'.in.cross-shard.left.join",
		"unsupported: expr.'abs(B.a)'.in.cross-shard.left.join",
		"unsupported: expr.'abs(B.a + B.b)'.in.cross-shard.left.join",
		"unsupported: orderby:[a + 1].type.should.be.colname",
		"unsupported: group.by.field[A.a].should.be.in.select.list",
		"unsupported: group.by.[a + 1].type.should.be.colname",
		"unsupported: syntax.error.at.'count'",
		"unsupported: unknown.column.'t1.a'.in.exprs",
		"unsupported: unknown.column.'S.id'.in.field.list",
		"unsupported: unknown.column.'eeeee'.in.select.exprs",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	fakedbs.AddQueryPattern("desc .*", descResult)

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase("sbtest")
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig(), router.MockTableCConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		// plan build
		{
			log.Info("--select.query:%+v", query)
			_, err := BuildNode(log, route, scatter, database, node.(sqlparser.SelectStatement))
			want := results[i]
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}

func TestSelectSupported(t *testing.T) {
	querys := []string{
		"select id,rand(id),str1,str2 from A having concat(str1,str2) is null",
		"select now() as time, count(1), avg(id), sum(b) from A",
		"select avg(id + 1) from A",
		"select concat(str1,str2) from A",
		"select * from A join B on A.id=B.id where A.id=0",
		"select * from A join B on A.name=B.name",
		"select * from B join B as A where A.id=B.id and A.name=B.name",
		"select A.id from A,B,B as C where B.id = 0 and A.id=C.id and A.id=0",
		"select A.id from A,A as B where A.id=B.id and A.a=1",
		"select A.id from A join B on A.id = B.id join G on G.id=A.id and A.id>1 and G.id=3",
		"select A.id from A left join B on A.id=B.id where B.str is null",
		"select A.id from A left join B on A.id=B.id where B.str<=>null",
		"select A.id from A left join B on A.id=B.id where null<=>B.str",
		"select A.id from A join B on A.id >= B.id join G on G.id<=A.id",
		"select A.id from A join B on A.id=B.id right join G on G.id=A.id and A.a>B.a",
		"select A.id from (A,B) left join G on A.id =G.id and A.a>B.a",
		"select A.id from A join B on A.id=B.id right join G on G.id=A.id where concat(B.str,A.str) is null",
		"select A.id from A join B on A.id >= B.id join G on G.id<=A.id where concat(B.str,A.str) is null",
		"select A.id from A join B on A.id = B.id join G on G.id<=A.id+B.id",
		"select A.id from A join B on A.id = B.id join G on A.id+B.id<=G.id",
		"select A.id from G join (A,B) on A.id+B.id<=G.id",
		"select A.id from G join (A,B) on G.id<=A.id+B.id",
		"select sum(A.id) from A join B on A.id=B.id",
		"select B.id,G.id,B.a from G,A,B where G.a+B.a>5 having G.id=B.id and B.a=1 and 1=1",
		"select COALESCE(A.b, ''), IF(A.b IS NULL, FALSE, TRUE) AS spent from A left join B on A.a=B.a",
		"select COALESCE(B.b, ''), IF(B.b IS NULL, FALSE, TRUE) AS spent from A join B on A.a=B.a",
		"select A.id from A left join B on B.id+1=A.id where B.str1+B.str2 is null",
		"select A.id from A join B on A.id=B.id where A.id in (1,2) or B.a=1",
		"select A.id from A join B on A.id = B.id join G on A.id+B.id<=G.id where A.str + B.str is null",
		"select * from B, (G join A on G.a=A.a) where A.a=1",
		"select * from B, A where A.id=1 and B.a=A.a",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	fakedbs.AddQueryPattern("desc .*", descResult)

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase("sbtest")
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)
	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		// plan build
		{
			log.Info("--select.query:%+v", query)
			_, err := BuildNode(log, route, scatter, database, node.(sqlparser.SelectStatement))
			assert.Nil(t, err)
		}
	}
}

func TestSelectPlanAs(t *testing.T) {
	tcases := []struct {
		query string
		out   []xcontext.QueryTuple
	}{
		{
			query: "select a1.id  from A as a1 where a1.id>1000",
			out: []xcontext.QueryTuple{
				{
					Query:   "select a1.id from sbtest.A1 as a1 where a1.id > 1000",
					Backend: "backend1",
					Range:   "[0-32)",
				},
				{
					Query:   "select a1.id from sbtest.A2 as a1 where a1.id > 1000",
					Backend: "backend2",
					Range:   "[32-64)",
				},
				{
					Query:   "select a1.id from sbtest.A3 as a1 where a1.id > 1000",
					Backend: "backend3",
					Range:   "[64-96)",
				},
				{
					Query:   "select a1.id from sbtest.A4 as a1 where a1.id > 1000",
					Backend: "backend4",
					Range:   "[96-256)",
				},
				{
					Query:   "select a1.id from sbtest.A5 as a1 where a1.id > 1000",
					Backend: "backend5",
					Range:   "[256-512)",
				},
				{
					Query:   "select a1.id from sbtest.A6 as a1 where a1.id > 1000",
					Backend: "backend6",
					Range:   "[512-4096)",
				}},
		},
		{
			query: "select A.id  from A where A.id>1000",
			out: []xcontext.QueryTuple{
				{
					Query:   "select A.id from sbtest.A1 as A where A.id > 1000",
					Backend: "backend1",
					Range:   "[0-32)",
				},
				{
					Query:   "select A.id from sbtest.A2 as A where A.id > 1000",
					Backend: "backend2",
					Range:   "[32-64)",
				},
				{
					Query:   "select A.id from sbtest.A3 as A where A.id > 1000",
					Backend: "backend3",
					Range:   "[64-96)",
				},
				{
					Query:   "select A.id from sbtest.A4 as A where A.id > 1000",
					Backend: "backend4",
					Range:   "[96-256)",
				},
				{
					Query:   "select A.id from sbtest.A5 as A where A.id > 1000",
					Backend: "backend5",
					Range:   "[256-512)",
				},
				{
					Query:   "select A.id from sbtest.A6 as A where A.id > 1000",
					Backend: "backend6",
					Range:   "[512-4096)",
				}},
		},
	}

	// Database not null.
	{
		log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
		database := "sbtest"

		route, cleanup := router.MockNewRouter(log)
		defer cleanup()

		err := route.CreateDatabase("sbtest")
		assert.Nil(t, err)
		err = route.AddForTest(database, router.MockTableMConfig())
		assert.Nil(t, err)
		for _, tcase := range tcases {
			node, err := sqlparser.Parse(tcase.query)
			assert.Nil(t, err)

			// plan build
			{
				log.Info("--select.query:%+v", tcase.query)
				plan, err := BuildNode(log, route, nil, database, node.(sqlparser.SelectStatement))
				assert.Nil(t, err)
				q := plan.GetQuery()
				assert.Equal(t, tcase.out, q)
				plan.Children()
			}
		}
	}
}

func TestSelectDatabaseNotFound(t *testing.T) {
	query := "select * from A as A1 where id = 10"

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase("sbtest")
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig())
	assert.Nil(t, err)

	databaseNull := ""
	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	_, err = BuildNode(log, route, nil, databaseNull, node.(sqlparser.SelectStatement))
	want := "No database selected (errno 1046) (sqlstate 3D000)"
	got := err.Error()
	assert.Equal(t, want, got)
}

func TestUnsportStatement(t *testing.T) {
	query := "select a from A where id = 10 union (select a from B where id=3)"

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase("sbtest")
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	databaseNull := ""
	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	_, err = BuildNode(log, route, nil, databaseNull, node.(*sqlparser.Union).Right)
	want := "unsupported: unknown.select.statement"
	got := err.Error()
	assert.Equal(t, want, got)
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

		err := route.CreateDatabase("sbtest")
		assert.Nil(t, err)
		err = route.AddForTest(database, router.MockTableGConfig(), router.MockTableG1Config())
		assert.Nil(t, err)
		for _, query := range querys {
			node, err := sqlparser.Parse(query)
			assert.Nil(t, err)
			// plan build
			{
				log.Info("--select.query:%+v", query)
				plan, err := BuildNode(log, route, nil, database, node.(sqlparser.SelectStatement))
				assert.Nil(t, err)
				want := 1
				assert.Equal(t, want, len(plan.GetQuery()))
			}
		}
	}
}

func TestSelectPlanJoin(t *testing.T) {
	tcases := []struct {
		query string
		out   []xcontext.QueryTuple
	}{
		{
			query: "select G.a, G.b from G join B on G.a = B.a where B.id=1",
			out: []xcontext.QueryTuple{
				{
					Query:   "select G.a, G.b from sbtest.G join sbtest.B1 as B on G.a = B.a where B.id = 1",
					Backend: "backend2",
					Range:   "[512-4096)",
				}},
		},
		{
			query: "select G.a, G.b from G join B on G.a = B.a join G1 on G1.a = B.a where B.id=1",
			out: []xcontext.QueryTuple{
				{
					Query:   "select G.a, G.b from sbtest.G join sbtest.B1 as B on G.a = B.a join sbtest.G1 on G1.a = B.a where B.id = 1",
					Backend: "backend2",
					Range:   "[512-4096)",
				}},
		},
		{
			query: "select G.a, G.b from G, B where B.id=1",
			out: []xcontext.QueryTuple{
				{
					Query:   "select G.a, G.b from sbtest.G, sbtest.B1 as B where B.id = 1",
					Backend: "backend2",
					Range:   "[512-4096)",
				}},
		},
		{
			query: "select G.a, B.a from G join B on G.a = B.a order by B.a",
			out: []xcontext.QueryTuple{
				{
					Query:   "select G.a, B.a from sbtest.G join sbtest.B0 as B on G.a = B.a order by B.a asc",
					Backend: "backend1",
					Range:   "[0-512)",
				},
				{
					Query:   "select G.a, B.a from sbtest.G join sbtest.B1 as B on G.a = B.a order by B.a asc",
					Backend: "backend2",
					Range:   "[512-4096)",
				}},
		},
	}

	{
		log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
		database := "sbtest"

		route, cleanup := router.MockNewRouter(log)
		defer cleanup()

		err := route.CreateDatabase("sbtest")
		assert.Nil(t, err)
		err = route.AddForTest(database, router.MockTableGConfig(), router.MockTableBConfig(), router.MockTableG1Config())
		assert.Nil(t, err)
		for _, tcase := range tcases {
			node, err := sqlparser.Parse(tcase.query)
			assert.Nil(t, err)

			// plan build
			{
				log.Info("--select.query:%+v", tcase.query)
				plan, err := BuildNode(log, route, nil, database, node.(sqlparser.SelectStatement))
				assert.Nil(t, err)
				q := plan.GetQuery()
				assert.Equal(t, tcase.out, q)
				plan.Children()
			}
		}
	}
}

func TestSelectPlanJoinErr(t *testing.T) {
	querys := []string{
		"select C.a, C.b from sbtest.C join sbtest.G on G.id = C.id where C.id=1",
		"select G1.a, G1.b from sbtest.G1 join sbtest.B on G1.id = B.id where B.id=1",
		"select G1.a, G1.b from sbtest.G1 join sbtest.C on G1.id = C.id where C.id=1",
	}
	results := []string{
		"Table 'C' doesn't exist (errno 1146) (sqlstate 42S02)",
		"Table 'G1' doesn't exist (errno 1146) (sqlstate 42S02)",
		"Table 'G1' doesn't exist (errno 1146) (sqlstate 42S02)",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase("sbtest")
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableGConfig(), router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		// plan build
		{
			log.Info("--select.query:%+v", query)
			_, err := BuildNode(log, route, nil, database, node.(sqlparser.SelectStatement))
			want := results[i]
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}

func TestProcessUnion(t *testing.T) {
	tcases := []struct {
		query string
		out   []xcontext.QueryTuple
	}{
		{
			query: "select a,b from G union select a,b from A where id=1 order by a limit 10",
			out: []xcontext.QueryTuple{{
				Query:   "select a, b from sbtest.G union select a, b from sbtest.A6 as A where id = 1 order by a asc limit 10",
				Backend: "backend6",
				Range:   "[512-4096)",
			}},
		},
		{
			query: "select a,b from A where id=1 union select a,b from B where id=0 order by a limit 10",
			out: []xcontext.QueryTuple{{
				Query:   "select a, b from sbtest.A6 as A where id = 1",
				Backend: "backend6",
				Range:   "[512-4096)",
			}, {
				Query:   "select a, b from sbtest.B0 as B where id = 0",
				Backend: "backend1",
				Range:   "[0-512)",
			}},
		},
		{
			query: "select a,b from S union (select a,b from G order by a) limit 10",
			out: []xcontext.QueryTuple{{
				Query:   "select a, b from sbtest.S union (select a, b from sbtest.G order by a asc) limit 10",
				Backend: "backend1",
				Range:   "",
			}},
		},
		{
			query: "select a,b from S union all (select a,b from A where id=1 union select a,b from B where id=0 order by a limit 10) order by b",
			out: []xcontext.QueryTuple{{
				Query:   "select a, b from sbtest.S",
				Backend: "backend1",
				Range:   "",
			}, {
				Query:   "select a, b from sbtest.A6 as A where id = 1",
				Backend: "backend6",
				Range:   "[512-4096)",
			}, {
				Query:   "select a, b from sbtest.B0 as B where id = 0",
				Backend: "backend1",
				Range:   "[0-512)",
			}},
		},
		{
			query: "select 1 union select a from A where id=1 order by 1 limit 10",
			out: []xcontext.QueryTuple{{
				Query:   "select 1 from dual union select a from sbtest.A6 as A where id = 1 order by 1 asc limit 10",
				Backend: "backend6",
				Range:   "[512-4096)",
			}},
		},
		{
			query: "select a as tmp,b from B union distinct (select a,b from S union select 1,'a') order by a limit 10",
			out: []xcontext.QueryTuple{{
				Query:   "select a as tmp, b from sbtest.B0 as B",
				Backend: "backend1",
				Range:   "[0-512)",
			}, {
				Query:   "select a as tmp, b from sbtest.B1 as B",
				Backend: "backend2",
				Range:   "[512-4096)",
			}, {
				Query:   "select a, b from sbtest.S union select 1, 'a' from dual",
				Backend: "backend1",
				Range:   "",
			}},
		},
	}

	// Database not null.
	{
		log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
		database := "sbtest"

		route, cleanup := router.MockNewRouter(log)
		defer cleanup()

		err := route.CreateDatabase("sbtest")
		assert.Nil(t, err)
		err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableSConfig(), router.MockTableGConfig())
		assert.Nil(t, err)
		for _, tcase := range tcases {
			node, err := sqlparser.Parse(tcase.query)
			assert.Nil(t, err)

			// plan build
			{
				log.Info("--union.query:%+v", tcase.query)
				plan, err := BuildNode(log, route, nil, database, node.(sqlparser.SelectStatement))
				assert.Nil(t, err)
				q := plan.GetQuery()
				assert.Equal(t, tcase.out, q)
				plan.Children()
				plan.getReferTables()
			}
		}
	}
}

func TestUnionUnsupported(t *testing.T) {
	querys := []string{
		"select * from A where id = 2 union select id from B where id = 0 order by id",
		"select a from A union select a,b from B",
		"select a from A union select b from B order by b",
		"select a from A union select b from B order by A.a",
		"select a from A union select b from B order by a limit x",
		"select a from C union select b from A limit 1",
		"select a from A union select b from C",
	}
	results := []string{
		"unsupported: the.used.'select'.statements.have.a.different.number.of.columns",
		"unsupported: the.used.'select'.statements.have.a.different.number.of.columns",
		"unsupported: unknown.column.'b'.in.'order.clause'",
		"unsupported: table.'A'.from.one.of.the.SELECTs.cannot.be.used.in.field.list",
		"unsupported: limit.offset.or.counts.must.be.IntVal",
		"Table 'C' doesn't exist (errno 1146) (sqlstate 42S02)",
		"Table 'C' doesn't exist (errno 1146) (sqlstate 42S02)",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	fakedbs.AddQueryPattern("desc .*", descResult)

	err := route.CreateDatabase("sbtest")
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		// plan build
		{
			log.Info("--union.query:%+v", query)
			_, err := BuildNode(log, route, scatter, database, node.(sqlparser.SelectStatement))
			want := results[i]
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}

func TestGenerateFieldQuery(t *testing.T) {
	query := "select A.id+B.id from A join B on A.name=B.name"
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase("sbtest")
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	plan, err := BuildNode(log, route, nil, database, node.(sqlparser.SelectStatement))
	assert.Nil(t, err)

	got := plan.(*JoinNode).Right.(*MergeNode).GenerateFieldQuery().Query
	want := "select :A_id + B.id as `A.id + B.id` from sbtest.B1 as B where 1 != 1"
	assert.Equal(t, want, got)
}

func TestSelectPlanList(t *testing.T) {
	querys := []string{
		"select 1, sum(a),avg(a),a,b from sbtest.L where id>1 group by a,b order by a desc limit 10 offset 100",
		"select L.a, L.b from L join L1 on L.a = L1.a where L1.id=1",
		"select L.a, L.b from L, L1 where L.a = L1.a and L1.id=1",
		"select L.a, L.b from L, L1 where L.a = L1.a and L1.id=1 and L.id=1",
	}

	wants := []int{
		3,
		4,
		4,
		1,
	}

	{
		log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
		database := "sbtest"

		route, cleanup := router.MockNewRouter(log)
		defer cleanup()

		err := route.CreateDatabase("sbtest")
		assert.Nil(t, err)
		err = route.AddForTest(database, router.MockTableListConfig(), router.MockTableList1Config())
		assert.Nil(t, err)
		for i, query := range querys {
			node, err := sqlparser.Parse(query)
			assert.Nil(t, err)

			// plan build
			{
				log.Info("--select.query:%+v", query)
				plan, err := BuildNode(log, route, nil, database, node.(sqlparser.SelectStatement))
				assert.Nil(t, err)
				want := wants[i]
				assert.Equal(t, want, len(plan.GetQuery()))
			}
		}
	}
}

func TestSelectSupportedPlanList(t *testing.T) {
	querys := []string{
		"select id,rand(id) from L",
		"select now() as time, count(1), avg(id), sum(b) from L",
		"select avg(id + 1) from L",
		"select concat(str1,str2) from L",
		"select A.id from A join L on A.id=L.id where A.id=1",
		"select A.id from A,L,L as C where L.id = 1 and A.id=C.id and A.id=1",
		"select L.id from L,L as B where L.id=B.id and L.a=1",
		"select L.id from L join B on L.id = B.id join G on G.id=L.id and L.id>1 and G.id=5",
		"select A.id from A left join L on A.id=L.id where L.str is null",
		"select A.id from A left join L on A.id=L.id where L.str<=>null",
		"select A.id from A left join L on A.id=L.id where null<=>L.str",
		"select A.id from A join L on A.id >= L.id join G on G.id<=A.id",
		"select L.id from L join B on L.id >= B.id join G on G.id<=L.id",
		"select COALESCE(L.b, ''), IF(L.b IS NULL, FALSE, TRUE) AS spent from L left join B on L.a=B.a",
		"select COALESCE(L.b, ''), IF(L.b IS NULL, FALSE, TRUE) AS spent from A join L on A.a=L.a",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase("sbtest")
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(),
		router.MockTableGConfig(), router.MockTableListConfig())
	assert.Nil(t, err)
	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		_, err = BuildNode(log, route, nil, database, node.(sqlparser.SelectStatement))
		assert.Nil(t, err)
	}
}
