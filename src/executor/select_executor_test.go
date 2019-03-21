/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package executor

import (
	"fmt"
	"testing"

	"backend"
	"planner"
	"router"
	"xcontext"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func TestMergeExecutor(t *testing.T) {
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
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("z")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("x")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("5")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("g")),
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
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("go")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("51")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("lang")),
			},
		},
	}
	r3 := &sqltypes.Result{}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	// desc
	fakedbs.AddQuery("select id, name from sbtest.A0 as A where id > 8 order by id desc, name asc", r1)
	fakedbs.AddQuery("select id, name from sbtest.A2 as A where id > 8 order by id desc, name asc", r2)
	fakedbs.AddQuery("select id, name from sbtest.A4 as A where id > 8 order by id desc, name asc", r3)
	fakedbs.AddQuery("select id, name from sbtest.A8 as A where id > 8 order by id desc, name asc", r3)

	querys := []string{
		"select id, name from A where id>8 order by id desc, name asc",
	}
	results := []string{
		"[[51 lang] [5 g] [3 go] [3 z] [1 x]]",
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
		executor := NewSelectExecutor(log, plan, txn)
		{
			ctx := xcontext.NewResultContext()
			err := executor.Execute(ctx)
			assert.Nil(t, err)
			want := results[i]
			got := fmt.Sprintf("%v", ctx.Results.Rows)
			assert.Equal(t, want, got)
			log.Debug("%+v", ctx.Results)
		}
	}
}

func TestJoinExecutor(t *testing.T) {
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
				Name:  "tmpc_0",
				Type:  querypb.Type_INT32,
				Table: "A",
			},
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
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("go")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("0")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("4")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("lang")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("6")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("lang")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("5")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice")),
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
	fakedbs.AddQuery("select A.id from sbtest.A8 as A where A.id = 2", r1)
	fakedbs.AddQuery("select A.id from sbtest.A8 as A where A.id = 3", r12)

	fakedbs.AddQuery("select A.id, A.name from sbtest.A8 as A where A.id = 2 order by A.name asc", r1)
	fakedbs.AddQuery("select A.id, A.name from sbtest.A8 as A where A.id = 3 order by A.name asc", r13)

	fakedbs.AddQuery("select A.id from sbtest.A0 as A where A.id > 2 order by A.id asc", r1)
	fakedbs.AddQuery("select A.id from sbtest.A2 as A where A.id > 2 order by A.id asc", r12)
	fakedbs.AddQuery("select A.id from sbtest.A4 as A where A.id > 2 order by A.id asc", r12)
	fakedbs.AddQuery("select A.id from sbtest.A8 as A where A.id > 2 order by A.id asc", r12)

	fakedbs.AddQuery("select A.id > 2 as tmpc_0, A.id, A.name from sbtest.A0 as A order by A.name asc", r11)
	fakedbs.AddQuery("select A.id > 2 as tmpc_0, A.id, A.name from sbtest.A2 as A order by A.name asc", r3)
	fakedbs.AddQuery("select A.id > 2 as tmpc_0, A.id, A.name from sbtest.A4 as A order by A.name asc", r3)
	fakedbs.AddQuery("select A.id > 2 as tmpc_0, A.id, A.name from sbtest.A8 as A order by A.name asc", r3)

	fakedbs.AddQuery("select B.name, B.id from sbtest.B0 as B", r21)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B1 as B", r2)

	fakedbs.AddQuery("select B.name, B.id from sbtest.B0 as B order by B.name asc", r21)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B1 as B order by B.name asc", r2)

	fakedbs.AddQuery("select B.name from sbtest.B1 as B where B.id = 1 order by B.name asc", r22)

	fakedbs.AddQuery("select B.name, B.id from sbtest.B0 as B where B.name = 's' and B.id > 2 order by B.id asc", r21)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B1 as B where B.name = 's' and B.id > 2 order by B.id asc", r21)

	fakedbs.AddQuery("select B.name, B.id from sbtest.B0 as B where B.id > 2 order by B.id asc", r21)
	fakedbs.AddQuery("select B.name, B.id from sbtest.B1 as B where B.id > 2 order by B.id asc", r2)
	querys := []string{
		"select A.id, B.name from A join B on A.id=B.id where A.id > 2 group by A.id",
		"select A.id, B.name from A left join B on A.id=B.id where A.id > 2 group by A.id",
		"select A.id, B.name from A right join B on A.id=B.id where A.id > 2 group by A.id",
		"select A.id, B.name from A join B on A.id=B.id where A.id > 2 group by A.id limit 1",
		"select A.id, B.name, B.id from A left join B on A.name=B.name and A.id > 2 group by A.id",
		"select A.id, B.name, B.id from A,B where A.id = 2 group by A.id",
		"select A.id, B.name, B.id from A,B where A.id = 3 group by A.id",
		"select A.id, B.name, B.id from B,A where A.id = 3 group by A.id",
		"select A.id, B.name from A left join B on A.id=B.id and B.name='s' where A.id > 2 group by A.id",
		"select A.id, B.name from B join A on A.name=B.name where A.id = 2 and B.id=1 group by A.id",
		"select A.id, B.name, B.id from B join A on A.name=B.name where A.id = 3 group by A.id",
		"select A.id, B.name from B join A on A.id=B.id where A.id > 2 group by A.id",
	}
	results := []string{
		"[[3 go] [5 lang]]",
		"[[3 go] [4 ] [5 lang]]",
		"[[3 go] [5 lang]]",
		"[[3 go]]",
		"[[3 go 3] [4  ] [5  ] [6 lang 5]]",
		"[[3 go 3] [3 lang 5] [4 go 3] [4 lang 5] [5 go 3] [5 lang 5]]",
		"[]",
		"[]",
		"[[3 ] [4 ] [5 ]]",
		"[]",
		"[[4 lang 5]]",
		"[[3 go] [5 lang]]",
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
		executor := NewSelectExecutor(log, plan, txn)
		{
			ctx := xcontext.NewResultContext()
			err := executor.Execute(ctx)
			assert.Nil(t, err)
			want := results[i]
			got := fmt.Sprintf("%v", ctx.Results.Rows)
			assert.Equal(t, want, got)
			log.Debug("%+v", ctx.Results)
		}
	}
}
