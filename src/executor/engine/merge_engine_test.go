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
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestMergeEngine(t *testing.T) {
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

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableAConfig())
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

		plan := planner.NewSelectPlan(log, database, query, node.(*sqlparser.Select), route, scatter)
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

func TestGenerateQueryErr(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableAConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	// Create scatter.
	scatter, _, cleanup := backend.MockScatter(log, 10)
	defer cleanup()

	query := "select B.name, A.id+B.id as id from A join B on A.name=B.name where A.id = 3"
	want := "missing bind var A_id"

	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)

	plan := planner.NewSelectPlan(log, database, query, node.(*sqlparser.Select), route, scatter)
	err = plan.Build()
	assert.Nil(t, err)
	log.Debug("plan:%+v", plan.JSON())

	txn, err := scatter.CreateTransaction()
	assert.Nil(t, err)
	defer txn.Finish()

	ctx := xcontext.NewResultContext()
	planEngine := BuildEngine(log, plan.Root, txn)
	{
		err = planEngine.(*JoinEngine).right.getFields(ctx, nil)
		assert.NotNil(t, err)
		got := err.Error()
		assert.Equal(t, want, got)
	}
	{
		err = planEngine.(*JoinEngine).right.execBindVars(ctx, nil, true)
		assert.NotNil(t, err)
		got := err.Error()
		assert.Equal(t, want, got)
	}
}
