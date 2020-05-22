/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package operator

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

func TestAggregateOperator(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)

	// Create scatter and query handler.
	scatter, _, cleanup := backend.MockScatter(log, 10)
	defer cleanup()

	querys := []string{
		"select id, sum(score) as score from A where id>1",
		"select id, count(score) as score from A where id>2",
		"select id, min(score) as score from A where id>1",
		"select id, max(score) as score from A where id>2",
		"select id, sum(distinct score) as score from A where id>2",
		"select id, count(distinct score) as score from A where id>2",
		"select id, min(distinct score) as score from A where id>2",
		"select id, max(distinct score) as score from A where id>2",
	}
	results := []string{
		"[[3 20]]",
		"[[3 20]]",
		"[[3 3]]",
		"[[3 7]]",
		"[[3 10]]",
		"[[3 2]]",
		"[[3 3]]",
		"[[3 7]]",
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
		{
			ctx := xcontext.NewResultContext()
			ctx.Results = &sqltypes.Result{}
			ctx.Results.Fields = []*querypb.Field{
				{
					Name: "id",
					Type: querypb.Type_INT32,
				}, {
					Name: "score",
					Type: sqltypes.Decimal,
				},
			}
			ctx.Results.Rows = [][]sqltypes.Value{
				{
					sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
					sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
				},
				{
					sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
					sqltypes.MakeTrusted(querypb.Type_INT32, []byte("7")),
				},
				{
					sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
					sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
				},
				{
					sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
					sqltypes.MakeTrusted(querypb.Type_INT32, []byte("7")),
				},
			}
			err = ExecSubPlan(log, plan.Root, ctx)
			assert.Nil(t, err)
			want := results[i]
			got := fmt.Sprintf("%v", ctx.Results.Rows)
			assert.Equal(t, want, got)
			log.Debug("%+v", ctx.Results)
		}
	}
}

func TestAggregateAvgOperator(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)

	// Create scatter and query handler.
	scatter, _, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	querys := []string{
		"select avg(score) as score from A where id>8",
		"select avg(distinct score) as score, count(score) from A where id>8",
	}
	results := []string{
		"[[3.666667]]",
		"[[8.000000 4]]",
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
		{
			ctx := xcontext.NewResultContext()
			ctx.Results = &sqltypes.Result{}
			ctx.Results.Fields = []*querypb.Field{
				{
					Name:     "score",
					Type:     querypb.Type_FLOAT32,
					Decimals: 2,
				},
				{
					Name: "count(score)",
					Type: querypb.Type_INT32,
				},
			}
			ctx.Results.Rows = [][]sqltypes.Value{
				{
					sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
					sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
				},
				{
					sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
					sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
				},
				{
					sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
					sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
				},
				{
					sqltypes.MakeTrusted(querypb.Type_INT32, []byte("13")),
					sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
				},
			}
			err = ExecSubPlan(log, plan.Root, ctx)
			assert.Nil(t, err)
			want := results[i]
			got := fmt.Sprintf("%v", ctx.Results.Rows)
			assert.Equal(t, want, got)
			log.Debug("%+v", ctx.Results)
		}
	}
}

func TestAggregateNotPush(t *testing.T) {
	r1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "a",
				Type: querypb.Type_FLOAT32,
			},
			{
				Name: "b",
				Type: querypb.Type_INT32,
			},
			{
				Name: "c",
				Type: querypb.Type_FLOAT32,
			},
			{
				Name: "d",
				Type: querypb.Type_INT32,
			},
			{
				Name: "e",
				Type: querypb.Type_INT32,
			},
		},
		Rows: [][]sqltypes.Value{},
	}

	r2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "a",
				Type: querypb.Type_FLOAT32,
			},
			{
				Name: "a",
				Type: querypb.Type_FLOAT32,
			},
			{
				Name: "b",
				Type: querypb.Type_INT32,
			},
			{
				Name: "c",
				Type: querypb.Type_FLOAT32,
			},
			{
				Name: "d",
				Type: querypb.Type_INT32,
			},
			{
				Name: "e",
				Type: querypb.Type_INT32,
			},
		},
		Rows: [][]sqltypes.Value{},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)

	// Create scatter and query handler.
	scatter, _, cleanup := backend.MockScatter(log, 10)
	defer cleanup()

	querys := []string{
		"select sum(distinct a) as a, count(b) as b, avg(c) as c, max(d) as d, min(e) as e from A",
		"select a, sum(distinct a) as a, count(b) as b, avg(c) as c, max(d) as d, min(e) as e from A",
	}
	rss := []*sqltypes.Result{
		r1,
		r2,
	}
	wantResults := []string{
		"[[ 0   ]]",
		"[[  0   ]]",
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
		{
			ctx := xcontext.NewResultContext()
			ctx.Results = rss[i]
			err = ExecSubPlan(log, plan.Root, ctx)
			assert.Nil(t, err)
			want := wantResults[i]
			got := fmt.Sprintf("%v", ctx.Results.Rows)
			assert.Equal(t, want, got)
			log.Debug("%+v", ctx.Results)
		}
	}
}

func TestAggregateGroup(t *testing.T) {
	r1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "a",
				Type: querypb.Type_INT32,
			},
			{
				Name: "sum(score)",
				Type: sqltypes.Decimal,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("11")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("2")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("22")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("11")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("2")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("22")),
			},
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)

	// Create scatter and query handler.
	scatter, _, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	querys := []string{
		"select a, sum(score) from A where id>8 group by a",
	}
	results := []string{
		"[[1 22] [2 44]]",
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
		{
			ctx := xcontext.NewResultContext()
			ctx.Results = &sqltypes.Result{}
			ctx.Results = r1
			err = ExecSubPlan(log, plan.Root, ctx)
			assert.Nil(t, err)
			want := results[i]
			got := fmt.Sprintf("%v", ctx.Results.Rows)
			assert.Equal(t, want, got)
			log.Debug("%+v", ctx.Results)
		}
	}
}
