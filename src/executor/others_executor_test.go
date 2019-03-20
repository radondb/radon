/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package executor

import (
	"testing"

	"backend"
	"fakedb"
	"planner"
	"router"
	"xcontext"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestOthersExecutor(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)

	// delete.
	querys := []string{
		"checksum table A",
		"checksum table sbtest.A",
	}
	// Add querys.
	fakedbs.AddQueryPattern("checksum table .*", fakedb.Result3)

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		plan := planner.NewOthersPlan(log, database, query, node, route)
		err = plan.Build()
		assert.Nil(t, err)

		txn, err := scatter.CreateTransaction()
		assert.Nil(t, err)
		defer txn.Finish()
		executor := NewOthersExecutor(log, plan, txn)
		{
			ctx := xcontext.NewResultContext()
			err := executor.Execute(ctx)
			assert.Nil(t, err)
		}
	}
}
