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

func TestAggregateExecutor(t *testing.T) {
	r1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "score",
				Type: querypb.Type_INT32,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
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
				Name: "score",
				Type: querypb.Type_INT32,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("7")),
			},
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	// sum.
	fakedbs.AddQuery("select id, sum(score) as score from sbtest.A0 as A where id > 1", r1)
	fakedbs.AddQuery("select id, sum(score) as score from sbtest.A2 as A where id > 1", r2)
	fakedbs.AddQuery("select id, sum(score) as score from sbtest.A4 as A where id > 1", r1)
	fakedbs.AddQuery("select id, sum(score) as score from sbtest.A8 as A where id > 1", r2)

	// count.
	fakedbs.AddQuery("select id, count(score) as score from sbtest.A0 as A where id > 2", r1)
	fakedbs.AddQuery("select id, count(score) as score from sbtest.A2 as A where id > 2", r2)
	fakedbs.AddQuery("select id, count(score) as score from sbtest.A4 as A where id > 2", r1)
	fakedbs.AddQuery("select id, count(score) as score from sbtest.A8 as A where id > 2", r2)

	// min.
	fakedbs.AddQuery("select id, min(score) as score from sbtest.A0 as A where id > 1", r1)
	fakedbs.AddQuery("select id, min(score) as score from sbtest.A2 as A where id > 1", r2)
	fakedbs.AddQuery("select id, min(score) as score from sbtest.A4 as A where id > 1", r1)
	fakedbs.AddQuery("select id, min(score) as score from sbtest.A8 as A where id > 1", r2)

	// max.
	fakedbs.AddQuery("select id, max(score) as score from sbtest.A0 as A where id > 2", r1)
	fakedbs.AddQuery("select id, max(score) as score from sbtest.A2 as A where id > 2", r2)
	fakedbs.AddQuery("select id, max(score) as score from sbtest.A4 as A where id > 2", r1)
	fakedbs.AddQuery("select id, max(score) as score from sbtest.A8 as A where id > 2", r2)

	// distinct.
	fakedbs.AddQuery("select id, score as score from sbtest.A0 as A where id > 2", r1)
	fakedbs.AddQuery("select id, score as score from sbtest.A2 as A where id > 2", r2)
	fakedbs.AddQuery("select id, score as score from sbtest.A4 as A where id > 2", r1)
	fakedbs.AddQuery("select id, score as score from sbtest.A8 as A where id > 2", r2)

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

func TestAggregateAvgExecutor(t *testing.T) {
	r1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "score",
				Type: querypb.Type_FLOAT32,
			},
			{
				Name: "count(score)",
				Type: querypb.Type_INT32,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
			},
		},
	}

	r2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "score",
				Type: querypb.Type_FLOAT32,
			},
			{
				Name: "count(score)",
				Type: querypb.Type_INT32,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("13")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("3")),
			},
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	// avg.
	fakedbs.AddQuery("select sum(score) as score, count(score) from sbtest.A0 as A where id > 8", r1)
	fakedbs.AddQuery("select sum(score) as score, count(score) from sbtest.A2 as A where id > 8", r1)
	fakedbs.AddQuery("select sum(score) as score, count(score) from sbtest.A4 as A where id > 8", r1)
	fakedbs.AddQuery("select sum(score) as score, count(score) from sbtest.A8 as A where id > 8", r2)

	// avg distinct.
	fakedbs.AddQuery("select score as score, score as `count(score)` from sbtest.A0 as A where id > 8", r1)
	fakedbs.AddQuery("select score as score, score as `count(score)` from sbtest.A2 as A where id > 8", r1)
	fakedbs.AddQuery("select score as score, score as `count(score)` from sbtest.A4 as A where id > 8", r1)
	fakedbs.AddQuery("select score as score, score as `count(score)` from sbtest.A8 as A where id > 8", r2)
	querys := []string{
		"select avg(score) as score from A where id>8",
		"select avg(distinct score) as score, count(score) from A where id>8",
	}
	results := []string{
		"[[3.6666666666666665]]",
		"[[8 4]]",
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

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	// avg.
	fakedbs.AddQueryPattern("select.*", r1)
	querys := []string{
		"select sum(distinct a) as a, count(b) as b, avg(c) as c, max(d) as d, min(e) as e from A",
	}
	results := []string{
		"[[ 0   ]]",
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

func TestAggregateGroup(t *testing.T) {
	r1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "a",
				Type: querypb.Type_INT32,
			},
			{
				Name: "sum(score)",
				Type: querypb.Type_INT32,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("11")),
			},
		},
	}

	r2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "a",
				Type: querypb.Type_INT32,
			},
			{
				Name: "sum(score)",
				Type: querypb.Type_INT32,
			},
		},
		Rows: [][]sqltypes.Value{
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

	err := route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	// avg.
	fakedbs.AddQuery("select a, sum(score) from sbtest.A0 as A where id > 8 group by a order by a asc", r1)
	fakedbs.AddQuery("select a, sum(score) from sbtest.A2 as A where id > 8 group by a order by a asc", r1)
	fakedbs.AddQuery("select a, sum(score) from sbtest.A4 as A where id > 8 group by a order by a asc", r2)
	fakedbs.AddQuery("select a, sum(score) from sbtest.A8 as A where id > 8 group by a order by a asc", r2)

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
