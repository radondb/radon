/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"testing"

	"router"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestOrderByPlan(t *testing.T) {
	querys := []string{
		"select a,b from A order by a",
		"select * from A order by a",
		"select a,*,c,d from A order by a asc",
		"select a as b,c,d from A order by b desc",
		"select A.* from A order by A.a",
		"select A.* from A order by a",
		"select * from A order by A.a",
		"select a from A order by A.a",
		"select A.a from A order by a",
		"select a as b from A order by a",
		"select a as b from A order by A.a",
		"select a,b from A order by c",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	route, cleanup := router.MockNewRouter(log)
	defer cleanup()
	err := route.AddForTest("sbtest", router.MockTableMConfig())
	assert.Nil(t, err)
	for _, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		p, err := scanTableExprs(log, route, "sbtest", node.From)
		assert.Nil(t, err)
		_, _, err = parseSelectExprs(node.SelectExprs, p)
		assert.Nil(t, err)
		plan := NewOrderByPlan(log, node.OrderBy, p)
		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			log.Debug("%v,%s", plan.Type(), plan.JSON())
		}
		log.Debug("\n")
	}
}

func TestOrderByPlanError(t *testing.T) {
	querys := []string{
		"select a,b from A order by rand()",
		"select A.* from A order by X.a",
		"select A.a from A join B on A.id=B.id order by b",
	}
	results := []string{
		"unsupported: orderby:[rand()].type.should.be.colname",
		"unsupported: unknow.table.in.order.by.field[X.a]",
		"unsupported: column.'b'.in.order.clause.is.ambiguous",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	route, cleanup := router.MockNewRouter(log)
	defer cleanup()
	err := route.AddForTest("sbtest", router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		p, err := scanTableExprs(log, route, "sbtest", node.From)
		assert.Nil(t, err)
		_, _, err = parseSelectExprs(node.SelectExprs, p)
		assert.Nil(t, err)
		plan := NewOrderByPlan(log, node.OrderBy, p)
		// plan build
		{
			err := plan.Build()
			want := results[i]
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}
