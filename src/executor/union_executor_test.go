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
	"planner"
	"router"
	"xcontext"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

var (
	descResult = &sqltypes.Result{
		RowsAffected: 1,
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
		},
	}
)

func TestUnionExecutorErr(t *testing.T) {
	r1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("5")),
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
	fakedbs.AddQueryErrorPattern("select id from sbtest.B1 as B where id = 1", errors.New("mock.execute.error"))
	fakedbs.AddQueryPattern("desc .*", descResult)

	querys := []string{
		"select * from A where id = 2 union select id from B where id = 1 order by id",
	}
	wants := []string{
		"mock.execute.error (errno 1105) (sqlstate HY000)",
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
		executor := NewUnionExecutor(log, plan, txn)
		{
			ctx := xcontext.NewResultContext()
			err := executor.Execute(ctx)
			assert.NotNil(t, err)
			got := err.Error()
			assert.Equal(t, wants[i], got)
		}
	}
}
