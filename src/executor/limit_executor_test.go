/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package executor

import (
	"testing"

	"backend"
	"fmt"
	"planner"
	"router"
	"xcontext"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func TestLimitExecutor(t *testing.T) {
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
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("11")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name11")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("12")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name12")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("13")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name13")),
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
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("21")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name21")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("22")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name22")),
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
	// Add querys.
	fakedbs.AddQuery("select id, name from sbtest.A0 as A where id > 8 order by id asc limit 1", r1)
	fakedbs.AddQuery("select id, name from sbtest.A2 as A where id > 8 order by id asc limit 1", r2)
	fakedbs.AddQuery("select id, name from sbtest.A4 as A where id > 8 order by id asc limit 1", r3)
	fakedbs.AddQuery("select id, name from sbtest.A8 as A where id > 8 order by id asc limit 1", r3)

	querys := []string{
		"select id, name from A where id>8 order by id limit 1",
	}
	results := []string{
		"[[11 nice name11]]",
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
