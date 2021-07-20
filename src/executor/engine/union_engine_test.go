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

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
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

func TestUnionEngine(t *testing.T) {
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
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("go")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("5")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("lang")),
			},
		},
	}
	r3 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
		}}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableAConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	// desc
	fakedbs.AddQuery("select id, name from sbtest.A0 as A where id > 2", r2)
	fakedbs.AddQuery("select id, name from sbtest.A2 as A where id > 2", r3)
	fakedbs.AddQuery("select id, name from sbtest.A4 as A where id > 2", r3)
	fakedbs.AddQuery("select id, name from sbtest.A8 as A where id > 2", r3)
	fakedbs.AddQuery("select id, name from sbtest.A8 as A where id = 2", r3)
	fakedbs.AddQuery("select 5, 'lang' from dual", r1)
	fakedbs.AddQuery("select id, name from sbtest.B0 as B where id > 1", r3)
	fakedbs.AddQuery("select id, name from sbtest.B1 as B where id > 1", r3)

	querys := []string{
		"select id, name from A where id > 2 union select 5, 'lang' order by id",
		"select id, name from A where id > 2 union all select 5, 'lang' order by id",
		"select id, name from A where id = 2 union select id, name from B where id > 1 order by id",
		"select id, name from A where id > 2 union distinct select 5, 'lang' order by id limit 1",
	}
	results := []string{
		"[[3 go] [5 lang]]",
		"[[3 go] [5 lang] [5 lang]]",
		"[]",
		"[[3 go]]",
	}

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		plan := planner.NewUnionPlan(log, database, query, node.(*sqlparser.Union), route, scatter)
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
			assert.Nil(t, err)
			want := results[i]
			got := fmt.Sprintf("%v", ctx.Results.Rows)
			assert.Equal(t, want, got)
			log.Debug("%+v", ctx.Results)
		}
	}
}

func TestUnionEngineErr(t *testing.T) {
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
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("5")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("lang")),
			},
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableAConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	// desc
	fakedbs.AddQuery("select * from sbtest.A8 as A where id = 2", r1)
	fakedbs.AddQueryPattern("desc .*", descResult)

	querys := []string{
		"select * from A where id = 2 union select id, name from B where id = 1 order by id",
	}
	wants := []string{
		"mock.handler.query[select id, name from sbtest.b1 as b where id = 1].error[can.not.found.the.cond.please.set.first] (errno 1105) (sqlstate HY000)",
	}

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		plan := planner.NewUnionPlan(log, database, query, node.(*sqlparser.Union), route, scatter)
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
			got := err.Error()
			assert.Equal(t, wants[i], got)
		}
	}
}
