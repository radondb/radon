/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package engine

import (
	"fmt"
	"testing"

	"backend"
	"planner"
	"router"
	"xcontext"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestJoinEngine(t *testing.T) {
	r1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:  "id",
				Type:  querypb.Type_INT32,
				Table: "A",
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("4")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("5")),
			},
		},
	}
	r11 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:  "id",
				Type:  querypb.Type_INT32,
				Table: "A",
			},
			{
				Name:  "name",
				Type:  querypb.Type_VARCHAR,
				Table: "A",
			},
			{
				Name:  "tmpc_0",
				Type:  querypb.Type_INT32,
				Table: "A",
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("go")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("4")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("lang")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("0")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("6")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("lang")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("5")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("6")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nil")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
			},
		},
	}
	r12 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:  "id",
				Type:  querypb.Type_INT32,
				Table: "A",
			},
			{
				Name:  "name",
				Type:  querypb.Type_VARCHAR,
				Table: "A",
			},
		},
		Rows: [][]sqltypes.Value{},
	}
	r13 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:  "id",
				Type:  querypb.Type_INT32,
				Table: "A",
			},
			{
				Name:  "name",
				Type:  querypb.Type_VARCHAR,
				Table: "A",
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("4")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("lang")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("6")),
				sqltypes.MakeTrusted(querypb.Type_NULL_TYPE, nil),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("5")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice")),
			},
		},
	}
	r14 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:  "id",
				Type:  querypb.Type_INT32,
				Table: "A",
			},
			{
				Name:  "name",
				Type:  querypb.Type_VARCHAR,
				Table: "A",
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("go")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("4")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("lang")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("5")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("niu")),
			},
		},
	}
	r2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:  "name",
				Type:  querypb.Type_VARCHAR,
				Table: "B",
			},
			{
				Name:  "id",
				Type:  querypb.Type_INT32,
				Table: "B",
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("go")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("lang")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("5")),
			},
		},
	}
	r21 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:  "name",
				Type:  querypb.Type_VARCHAR,
				Table: "B",
			},
			{
				Name:  "id",
				Type:  querypb.Type_INT32,
				Table: "B",
			},
		},
		Rows: [][]sqltypes.Value{},
	}
	r22 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:  "name",
				Type:  querypb.Type_VARCHAR,
				Table: "B",
			},
			{
				Name:  "id",
				Type:  querypb.Type_INT32,
				Table: "B",
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_NULL_TYPE, nil),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
			},
		},
	}
	r3 := &sqltypes.Result{}
	r4 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:  "a",
				Type:  querypb.Type_VARCHAR,
				Table: "G",
			},
		},
		Rows: [][]sqltypes.Value{},
	}
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	// desc
	fakedbs.AddQuery("select A.id from sbtest.A8 as A where A.id = 2", r1)
	fakedbs.AddQuery("select A.id from sbtest.A8 as A where A.id = 3", r12)
	fakedbs.AddQuery("select A.id, A.name from sbtest.A8 as A where A.id = 2", r13)
	fakedbs.AddQuery("select A.id, A.name from sbtest.A8 as A where A.id = 2 order by A.name asc", r14)
	fakedbs.AddQuery("select A.id, A.name from sbtest.A8 as A where A.id = 3 order by A.name asc", r13)
	fakedbs.AddQuery("select A.id from sbtest.A0 as A where A.id > 2 order by A.id asc", r1)
	fakedbs.AddQuery("select A.id from sbtest.A2 as A where A.id > 2 order by A.id asc", r12)
	fakedbs.AddQuery("select A.id from sbtest.A4 as A where A.id > 2 order by A.id asc", r12)
	fakedbs.AddQuery("select A.id from sbtest.A8 as A where A.id > 2 order by A.id asc", r12)
	fakedbs.AddQuery("select A.id, A.name, A.id > 2 as tmpc_0 from sbtest.A0 as A order by A.name asc", r11)
	fakedbs.AddQuery("select A.id, A.name, A.id > 2 as tmpc_0 from sbtest.A2 as A order by A.name asc", r3)
	fakedbs.AddQuery("select A.id, A.name, A.id > 2 as tmpc_0 from sbtest.A4 as A order by A.name asc", r3)
	fakedbs.AddQuery("select A.id, A.name, A.id > 2 as tmpc_0 from sbtest.A8 as A order by A.name asc", r3)
	fakedbs.AddQuery("select A.id, A.name from sbtest.A8 as A where A.id = 5", r14)
	fakedbs.AddQuery("select A.id, A.name from sbtest.A8 as A where 1 != 1", r12)
	fakedbs.AddQuery("select A.id, A.name from sbtest.A8 as A where A.id = 3", r12)
	fakedbs.AddQuery("select A.id, A.name, A.id > 2 as tmpc_0 from sbtest.A0 as A", r11)
	fakedbs.AddQuery("select A.id, A.name, A.id > 2 as tmpc_0 from sbtest.A2 as A", r3)
	fakedbs.AddQuery("select A.id, A.name, A.id > 2 as tmpc_0 from sbtest.A4 as A", r3)
	fakedbs.AddQuery("select A.id, A.name, A.id > 2 as tmpc_0 from sbtest.A8 as A", r3)
	fakedbs.AddQuery("select G.a from sbtest.G", r4)

	fakedbs.AddQuery("select B.name, B.id from sbtest.B0 as B", r21)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B1 as B", r22)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B0 as B order by B.name asc", r21)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B1 as B order by B.name asc", r2)
	fakedbs.AddQuery("select B.name from sbtest.B1 as B where B.id = 1 order by B.name asc", r22)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B0 as B where B.id = 0", r2)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B0 as B where B.id > 2 and B.name = 's' order by B.id asc", r21)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B1 as B where B.id > 2 and B.name = 's' order by B.id asc", r21)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B0 as B where B.id > 2 order by B.id asc", r21)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B1 as B where B.id > 2 order by B.id asc", r2)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B1 as B where B.id = 1 and 'go' + b.name = 'golang'", r21)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B1 as B where B.id = 1 and 'lang' + b.name = 'golang'", r2)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B1 as B where B.id = 1 and 'niu' + b.name = 'golang'", r21)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B1 as B where B.id = 1", r21)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B1 as B where B.id = 1 and 'nice' + b.name = 'golang'", r21)
	fakedbs.AddQuery("select B.name, B.id from sbtest.b1 as b where b.id = 1 and 'nil' + b.name = 'golang'", r21)
	fakedbs.AddQuery("select b.name, b.id from sbtest.b1 as b where 1 != 1", r21)
	fakedbs.AddQuery("select b.name, null + b.id as id from sbtest.b1 as b where 1 != 1", r21)

	querys := []string{
		"select A.id, B.name from A right join B on A.id=B.id where A.id > 2",
		"select A.id, B.name from A join B on A.id=B.id where A.id > 2 limit 1",
		"select A.id, B.name, B.id, A.name from A left join B on A.name=B.name and A.id > 2 order by A.id",
		"select A.id, B.name, B.id from A,B where A.id = 2",
		"select A.id, B.name, B.id from A,B where A.id = 3",
		"select A.id, B.name, B.id from B,A where A.id = 3",
		"select A.id, B.name from A left join B on A.id=B.id and B.name='s' where A.id > 2",
		"select A.id, B.name from B join A on A.name=B.name where A.id = 2 and B.id=1",
		"select A.id, B.name, B.id from B join A on A.name=B.name where A.id = 3",
		"select A.id, B.name from B join A on A.name=B.name where A.id = 3 and B.id=1",
		"select A.id, B.name, B.id, A.name from B left join A on A.id>B.id and A.name!=B.name where A.id = 2",
		"select A.id, B.name, B.id, A.name from A left join B on A.name=B.name and A.id>=B.id and A.name<=>B.name and A.id >2 where B.name is null order by A.id",
		"select A.id, B.name, B.id, A.name from A join B on A.id<=B.id and A.name<=>B.name and A.id=2 and B.id=0 order by A.id",
		"select A.id, B.name, B.id, A.name from A join B on A.id < B.id and A.name<=>B.name and A.id=2 and B.id=0 order by A.id",
		"select A.id, B.name, B.id, A.name, A.id > 2 as tmpc_0 from A join B on A.name=B.name and A.id>=B.id and A.id > B.id order by A.id",
		"select A.id, B.name, B.id from A join B on A.name+B.name='golang' where A.id = 5 and B.id = 1 order by A.id limit 2",
		"select A.id, A.name, B.name, B.id from B join A on A.name+B.name='golang' where A.id = 5 and B.id = 1 order by A.id limit 1",
		"select A.id, B.name, B.id, A.name from A left join B on A.name+B.name='golang' and A.id>2 where B.name is null and B.id = 1 order by A.id",
		"select A.id, A.name, B.name, B.id from G,A,B where G.a+A.a=1 and A.name=B.name and A.id=5 and B.id=1",
		"select B.name, A.id+B.id as id from A join B on A.name=B.name where A.id = 3",
	}
	results := []string{
		"[[3 go] [5 lang]]",
		"[[3 go]]",
		"[[3 go 3 go] [4   lang] [5   nice] [6   nil] [6 lang 5 lang]]",
		"[[3  3] [4  3] [5  3]]",
		"[]",
		"[]",
		"[[3 ] [4 ] [5 ]]",
		"[]",
		"[[4 lang 5]]",
		"[]",
		"[]",
		"[[4   lang] [5   nice] [6   nil]]",
		"[[4 lang 5 lang]]",
		"[[4 lang 5 lang]]",
		"[[6 lang 5 lang 1]]",
		"[[4 lang 5] [4 go 3]]",
		"[]",
		"[]",
		"[]",
		"[]",
	}

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		plan := planner.NewSelectPlan(log, database, query, node.(*sqlparser.Select), route)
		err = plan.Build()
		assert.Nil(t, err)
		log.Debug("plan:%+v", plan.JSON())

		txn, err := scatter.CreateTransaction()
		assert.Nil(t, err)
		defer txn.Finish()
		txn.SetMaxJoinRows(32768)
		planEngine := BuildEngine(log, plan.Root, txn)
		{
			ctx := xcontext.NewResultContext()
			err := planEngine.Execute(ctx)
			assert.Nil(t, err)
			want := results[i]
			got := fmt.Sprintf("%v", ctx.Results.Rows)
			assert.Equal(t, want, got)
			log.Debug("%+v", ctx.Results)
		}
	}
}

func TestMaxRowErr(t *testing.T) {
	r1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("go")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("4")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("lang")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("5")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("lang")),
			},
		},
	}

	r2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
		},
	}

	r3 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("go")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("lang")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("5")),
			},
		},
	}

	r4 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "tmpc_0",
				Type: querypb.Type_INT32,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("0")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("0")),
			},
		},
	}

	r5 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "tmpc_0",
				Type: querypb.Type_INT32,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("0")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("5")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("0")),
			},
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig(), router.MockTableBConfig(), router.MockTableSConfig())
	assert.Nil(t, err)

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	// desc
	fakedbs.AddQuery("select a.id, a.name from sbtest.a8 as a where a.id = 3 order by a.id asc", r1)
	fakedbs.AddQuery("select a.id, a.name from sbtest.a8 as a where a.id = 3", r1)
	fakedbs.AddQuery("select 3 + b.id as id, b.name from sbtest.b1 as b where b.id = 3 and 3 = b.id", r1)
	fakedbs.AddQueryPattern("select b.id, b.name from .*", r2)
	fakedbs.AddQueryPattern("select b.name, b.id from .*", r3)
	fakedbs.AddQueryPattern("select s.id, s.name from .*", r1)
	fakedbs.AddQueryPattern("select s.name = 'a' as tmpc_0, s.id from .*", r4)
	fakedbs.AddQueryPattern("select s.a = 'a' as tmpc_0, s.id from .*", r5)

	querys := []string{
		"select A.id, A.name, B.name, B.id from A join B on A.id = B.id where A.id = 3",
		"select S.id, S.name, B.id, B.name from S left join B on S.id = B.id and B.id = 2",
		"select A.id, A.name, A.id + B.id as id, B.name from A join B on A.id = B.id where A.id = 3",
		"select S.id, S.name, B.name, B.id from S, B where B.id = 2",
		"select S.id, S.name, B.name, B.id from S left join B on S.id > B.id",
		"select B.name, B.id from S left join B on B.id = S.id and S.name = 'a'",
		"select B.name, B.id from S left join B on B.id = S.id and S.a = 'a'",
	}
	wants := "unsupported: join.row.count.exceeded.allowed.limit.of.'1'"
	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		plan := planner.NewSelectPlan(log, database, query, node.(*sqlparser.Select), route)
		err = plan.Build()
		assert.Nil(t, err)
		log.Debug("plan:%+v", plan.JSON())

		txn, err := scatter.CreateTransaction()
		assert.Nil(t, err)
		defer txn.Finish()
		txn.SetMaxJoinRows(1)
		planEngine := BuildEngine(log, plan.Root, txn)
		{
			ctx := xcontext.NewResultContext()
			err := planEngine.Execute(ctx)
			assert.NotNil(t, err)
			got := err.Error()
			assert.Equal(t, wants, got)
		}
	}
}

func TestGetFieldErr(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	// desc
	fakedbs.AddQueryErrorPattern("select .*", errors.New("mock.execute.error"))

	querys := []string{
		"select A.id,A.name from A",
		"select A.id, A.name, B.name, B.id from G,A,B where G.a=A.a and A.name=B.name and A.id=2 and B.id=1",
	}

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		plan := planner.NewSelectPlan(log, database, query, node.(*sqlparser.Select), route)
		err = plan.Build()
		assert.Nil(t, err)
		log.Debug("plan:%+v", plan.JSON())

		txn, err := scatter.CreateTransaction()
		assert.Nil(t, err)
		defer txn.Finish()
		{
			ctx := xcontext.NewResultContext()
			planEngine := BuildEngine(log, plan.Root, txn)
			joinVars := make(map[string]*querypb.BindVariable)
			err := planEngine.getFields(ctx, joinVars)
			assert.NotNil(t, err)
		}
	}
}

func TestMockErr(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	// desc
	fakedbs.AddQueryErrorPattern("select .*", errors.New("mock.execute.error"))

	querys := []string{
		"select A.id, B.name from A right join B on A.id=B.id where A.id > 2 group by A.id",
		"select a from A where A > 1 group by a",
		"select A.id, B.name, B.id from A join B on A.id+B.id=3 where A.id = 2 and B.id = 1 group by A.id limit 1",
	}

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		plan := planner.NewSelectPlan(log, database, query, node.(*sqlparser.Select), route)
		err = plan.Build()
		assert.Nil(t, err)
		log.Debug("plan:%+v", plan.JSON())

		txn, err := scatter.CreateTransaction()
		assert.Nil(t, err)
		defer txn.Finish()
		planEngine := BuildEngine(log, plan.Root, txn)
		{
			ctx := xcontext.NewResultContext()
			err := planEngine.Execute(ctx)
			assert.NotNil(t, err)
		}
	}
}
